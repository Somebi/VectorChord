// This software is licensed under a dual license model:
//
// GNU Affero General Public License v3 (AGPLv3): You may use, modify, and
// distribute this software under the terms of the AGPLv3.
//
// Elastic License v2 (ELv2): You may also use, modify, and distribute this
// software under the Elastic License v2, which has specific restrictions.
//
// We welcome any commercial collaboration or support. For inquiries
// regarding the licenses, please contact us at:
// vectorchord-inquiry@tensorchord.ai
//
// Copyright (c) 2025 TensorChord Inc.

use crate::datatype::typmod::Typmod;
use crate::index::fetcher::*;
use crate::index::storage::{PostgresPage, PostgresRelation};
use crate::index::vchordrq::am::Reloption;
use crate::index::vchordrq::opclass::{Opfamily, opfamily};
use crate::index::vchordrq::types::*;
use algo::{
    Page, PageGuard, Relation, RelationRead, RelationReadTypes, RelationWrite, RelationWriteTypes,
};
use half::f16;
use pgrx::pg_sys::{Datum, ItemPointerData};
use rand::Rng;
use simd::Floating;
use std::ffi::CStr;
use std::ops::Deref;
use vchordrq::types::*;
use vector::VectorOwned;
use vector::vect::VectOwned;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum BuildPhaseCode {
    Initializing = 0,
    InternalBuild = 1,
    ExternalBuild = 2,
    Build = 3,
    Inserting = 4,
    Compacting = 5,
}

pub struct BuildPhase(BuildPhaseCode, u16);

impl BuildPhase {
    pub const fn new(code: BuildPhaseCode, k: u16) -> Option<Self> {
        match (code, k) {
            (BuildPhaseCode::Initializing, 0) => Some(BuildPhase(code, k)),
            (BuildPhaseCode::InternalBuild, 0..102) => Some(BuildPhase(code, k)),
            (BuildPhaseCode::ExternalBuild, 0) => Some(BuildPhase(code, k)),
            (BuildPhaseCode::Build, 0) => Some(BuildPhase(code, k)),
            (BuildPhaseCode::Inserting, 0) => Some(BuildPhase(code, k)),
            (BuildPhaseCode::Compacting, 0) => Some(BuildPhase(code, k)),
            _ => None,
        }
    }
    pub const fn name(self) -> &'static CStr {
        match self {
            BuildPhase(BuildPhaseCode::Initializing, k) => {
                static RAW: [&CStr; 1] = [c"initializing"];
                RAW[k as usize]
            }
            BuildPhase(BuildPhaseCode::InternalBuild, k) => {
                static RAWS: [&[&CStr]; 2] = [
                    &[c"initializing index, by internal build"],
                    seq_macro::seq!(
                        N in 0..=100 {
                            &[#(
                                const {
                                    let bytes = concat!("initializing index, by internal build (", N, " %)\0").as_bytes();
                                    if let Ok(cstr) = CStr::from_bytes_with_nul(bytes) {
                                        cstr
                                    } else {
                                        unreachable!()
                                    }
                                },
                            )*]
                        }
                    ),
                ];
                static RAW: [&CStr; 102] = {
                    let mut result = [c""; 102];
                    let mut offset = 0_usize;
                    let mut i = 0_usize;
                    while i < RAWS.len() {
                        let mut j = 0_usize;
                        while j < RAWS[i].len() {
                            {
                                result[offset] = RAWS[i][j];
                                offset += 1;
                            }
                            j += 1;
                        }
                        i += 1;
                    }
                    assert!(offset == result.len());
                    result
                };
                RAW[k as usize]
            }
            BuildPhase(BuildPhaseCode::ExternalBuild, k) => {
                static RAW: [&CStr; 1] = [c"initializing index, by external build"];
                RAW[k as usize]
            }
            BuildPhase(BuildPhaseCode::Build, k) => {
                static RAW: [&CStr; 1] = [c"initializing index"];
                RAW[k as usize]
            }
            BuildPhase(BuildPhaseCode::Inserting, k) => {
                static RAW: [&CStr; 1] = [c"inserting tuples from table to index"];
                RAW[k as usize]
            }
            BuildPhase(BuildPhaseCode::Compacting, k) => {
                static RAW: [&CStr; 1] = [c"compacting tuples in index"];
                RAW[k as usize]
            }
        }
    }
    pub const fn from_code(code: BuildPhaseCode) -> Self {
        Self(code, 0)
    }
    pub const fn from_value(value: u32) -> Option<Self> {
        const INITIALIZING: u16 = BuildPhaseCode::Initializing as _;
        const INTERNAL_BUILD: u16 = BuildPhaseCode::InternalBuild as _;
        const EXTERNAL_BUILD: u16 = BuildPhaseCode::ExternalBuild as _;
        const BUILD: u16 = BuildPhaseCode::Build as _;
        const INSERTING: u16 = BuildPhaseCode::Inserting as _;
        const COMPACTING: u16 = BuildPhaseCode::Compacting as _;
        let k = value as u16;
        match (value >> 16) as u16 {
            INITIALIZING => Self::new(BuildPhaseCode::Initializing, k),
            INTERNAL_BUILD => Self::new(BuildPhaseCode::InternalBuild, k),
            EXTERNAL_BUILD => Self::new(BuildPhaseCode::ExternalBuild, k),
            BUILD => Self::new(BuildPhaseCode::Build, k),
            INSERTING => Self::new(BuildPhaseCode::Inserting, k),
            COMPACTING => Self::new(BuildPhaseCode::Compacting, k),
            _ => None,
        }
    }
    pub const fn into_value(self) -> u32 {
        (self.0 as u32) << 16 | (self.1 as u32)
    }
}

#[pgrx::pg_guard]
pub extern "C-unwind" fn ambuildphasename(x: i64) -> *mut core::ffi::c_char {
    if let Ok(x) = u32::try_from(x.wrapping_sub(1)) {
        if let Some(x) = BuildPhase::from_value(x) {
            x.name().as_ptr().cast_mut()
        } else {
            std::ptr::null_mut()
        }
    } else {
        std::ptr::null_mut()
    }
}

#[derive(Debug, Clone)]
struct Heap {
    heap_relation: pgrx::pg_sys::Relation,
    index_relation: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
    opfamily: Opfamily,
    scan: *mut pgrx::pg_sys::TableScanDescData,
}

impl Heap {
    fn traverse<F: FnMut((ItemPointerData, Vec<(OwnedVector, u16)>))>(
        &self,
        progress: bool,
        callback: F,
    ) {
        use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
        pub struct State<'a, F> {
            pub this: &'a Heap,
            pub callback: F,
        }
        #[pgrx::pg_guard]
        unsafe extern "C-unwind" fn call<F>(
            _index_relation: pgrx::pg_sys::Relation,
            ctid: pgrx::pg_sys::ItemPointer,
            values: *mut Datum,
            is_null: *mut bool,
            _tuple_is_alive: bool,
            state: *mut core::ffi::c_void,
        ) where
            F: FnMut((ItemPointerData, Vec<(OwnedVector, u16)>)),
        {
            let state = unsafe { &mut *state.cast::<State<F>>() };
            let opfamily = state.this.opfamily;
            let datum = unsafe { (!is_null.add(0).read()).then_some(values.add(0).read()) };
            let ctid = unsafe { ctid.read() };
            if let Some(store) = unsafe { datum.and_then(|x| opfamily.store(x)) } {
                (state.callback)((ctid, store));
            }
        }
        let table_am = unsafe { &*(*self.heap_relation).rd_tableam };
        let mut state = State {
            this: self,
            callback,
        };
        let index_build_range_scan = table_am.index_build_range_scan.expect("bad table");
        unsafe {
            #[allow(ffi_unwind_calls, reason = "protected by pg_guard_ffi_boundary")]
            pg_guard_ffi_boundary(|| {
                index_build_range_scan(
                    self.heap_relation,
                    self.index_relation,
                    self.index_info,
                    true,
                    false,
                    progress,
                    0,
                    pgrx::pg_sys::InvalidBlockNumber,
                    Some(call::<F>),
                    (&mut state) as *mut State<F> as *mut _,
                    self.scan,
                )
            });
        }
    }
}

#[derive(Debug, Clone)]
struct PostgresReporter {}

impl PostgresReporter {
    fn phase(&mut self, phase: BuildPhase) {
        unsafe {
            pgrx::pg_sys::pgstat_progress_update_param(
                pgrx::pg_sys::PROGRESS_CREATEIDX_SUBPHASE as _,
                (phase.into_value() as i64) + 1,
            );
        }
    }
    fn tuples_total(&mut self, tuples_total: u64) {
        unsafe {
            pgrx::pg_sys::pgstat_progress_update_param(
                pgrx::pg_sys::PROGRESS_CREATEIDX_TUPLES_TOTAL as _,
                tuples_total as _,
            );
        }
    }
    fn tuples_done(&mut self, tuples_done: u64) {
        unsafe {
            pgrx::pg_sys::pgstat_progress_update_param(
                pgrx::pg_sys::PROGRESS_CREATEIDX_TUPLES_DONE as _,
                tuples_done as _,
            );
        }
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambuild(
    heap_relation: pgrx::pg_sys::Relation,
    index_relation: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    pgrx::info!("Debug: ambuild function called");
    use validator::Validate;
    let (vector_options, vchordrq_options) = unsafe { options(index_relation) };
    if let Err(errors) = Validate::validate(&vector_options) {
        pgrx::error!("error while validating options: {}", errors);
    }
    if let Err(errors) = Validate::validate(&vchordrq_options) {
        pgrx::error!("error while validating options: {}", errors);
    }
    let opfamily = unsafe { opfamily(index_relation) };
    let index = unsafe { PostgresRelation::new(index_relation) };
    let heap = Heap {
        heap_relation,
        index_relation,
        index_info,
        opfamily,
        scan: std::ptr::null_mut(),
    };
    let mut reporter = PostgresReporter {};

    let mut centroid_representatives_for_analysis: Option<Vec<Vec<CentroidRepresentative>>> = None;
    let mut tuples_total_for_analysis: Option<u64> = None;
    let structures = match vchordrq_options.build.source.clone() {
        VchordrqBuildSourceOptions::External(external_build) => {
            reporter.phase(BuildPhase::from_code(BuildPhaseCode::ExternalBuild));
            pgrx::info!("Debug: Using External build source");
            pgrx::info!("Debug: external_build = {:?}", external_build);
            let reltuples = unsafe { (*(*index_relation).rd_rel).reltuples };
            reporter.tuples_total(reltuples as u64);
            make_external_build(vector_options.clone(), opfamily, external_build.clone())
        }
        VchordrqBuildSourceOptions::Internal(internal_build) => {
            reporter.phase(BuildPhase::from_code(BuildPhaseCode::InternalBuild));
            pgrx::info!("Debug: Using Internal build source");
            pgrx::info!("Debug: internal_build = {:?}", internal_build);
            let mut tuples_total = 0_u64;
            let (samples, sample_origins) = 'a: {
                let mut rand = rand::rng();
                pgrx::info!("Debug: internal_build.lists = {:?}", internal_build.lists);
                pgrx::info!("Debug: internal_build.sampling_factor = {}", internal_build.sampling_factor);
                
                let max_number_of_samples = internal_build
                    .lists
                    .last()
                    .map(|x| x.saturating_mul(internal_build.sampling_factor));
                
                pgrx::info!("Debug: max_number_of_samples = {:?}", max_number_of_samples);
                
                let Some(max_number_of_samples) = max_number_of_samples else {
                    pgrx::info!("Debug: No max_number_of_samples, breaking with empty vectors");
                    break 'a (Vec::new(), Vec::new());
                };

                // Add detailed sampling log
                pgrx::info!(
                    "sampling: Using Reservoir Sampling, sampling {} vectors because {} = {} × {} (sampling_factor = {}, cluster_size = {})",
                    max_number_of_samples,
                    max_number_of_samples,
                    internal_build.sampling_factor,
                    internal_build.lists.last().unwrap(),
                    internal_build.sampling_factor,
                    internal_build.lists.last().unwrap()
                );

                let mut samples = Vec::new();
                let mut sample_origins = Vec::new(); // Track origin and resolved id
                let mut number_of_samples = 0_u32;
                
                // Build CTID to ID map for reliable ID resolution during sampling
                let heap_relid = unsafe { (*heap.heap_relation).rd_id };
                let id_map = build_ctid_id_map(heap_relid);
                
                heap.traverse(false, |(ctid, store)| {
                    for (vector, extra) in store {
                        let x = match vector {
                            OwnedVector::Vecf32(x) => VectOwned::build_to_vecf32(x.as_borrowed()),
                            OwnedVector::Vecf16(x) => VectOwned::build_to_vecf32(x.as_borrowed()),
                        };
                        assert_eq!(
                            vector_options.dims,
                            x.len() as u32,
                            "invalid vector dimensions"
                        );
                        
                        let [bi_hi, bi_lo, ip_posid] = ctid_to_key(ctid);
                        // Resolve document ID immediately during sampling for reliable tracking
                        let block_number = block_from_parts(bi_hi, bi_lo);
                        
                        // Debug logging for ID mapping lookup
                        if tuples_total % 1000 == 0 { // Log every 1000th tuple to avoid spam
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Raw CTID: {:?}", ctid);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ctid_to_key result: bi_hi={}, bi_lo={}, ip_posid={}", bi_hi, bi_lo, ip_posid);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: block_from_parts({}, {}) = {}", bi_hi, bi_lo, block_number);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Looking up key ({}, {}) in ID map", block_number, ip_posid);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map size: {}", id_map.len());
                            
                            // Show sample keys from the ID map to understand the coordinate system
                            if tuples_total == 1000 { // Only show once to avoid spam
                                let mut sample_keys: Vec<_> = id_map.keys().take(5).collect();
                                sample_keys.sort();
                                for (i, key) in sample_keys.iter().enumerate() {
                                    let (col_name, id_value) = &id_map[key];
                                    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map sample key {}: {:?} -> col='{}', id='{}'", i, key, col_name, id_value);
                                }
                                
                                // Try to find a key that might match our lookup
                                let mut found_keys = Vec::new();
                                for (key, value) in id_map.iter() {
                                    if key.0 == block_number || key.0 == block_number + 1 || key.0 == block_number - 1 {
                                        found_keys.push((*key, value.clone()));
                                        if found_keys.len() >= 3 {
                                            break;
                                        }
                                    }
                                }
                                if !found_keys.is_empty() {
                                    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Found nearby keys: {:?}", found_keys);
                                }
                            }
                        }
                        
                        // Show sample keys from the ID map earlier to understand the coordinate system
                        if tuples_total == 0 { // Show on first tuple
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: First tuple - showing ID map sample keys");
                            let mut sample_keys: Vec<_> = id_map.keys().take(5).collect();
                            sample_keys.sort();
                            for (i, key) in sample_keys.iter().enumerate() {
                                let (col_name, id_value) = &id_map[key];
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map sample key {}: {:?} -> col='{}', id='{}'", i, key, col_name, id_value);
                            }
                        }
                        
                        // Try to find the ID mapping using the same coordinate system as the ID map
                        let mut validated_doc_id_info = id_map.get(&(block_number, ip_posid)).cloned();

                        // Fallback: if the map lookup failed, resolve by querying the heap using CTID
                        if validated_doc_id_info.is_none() {
                            if let Some((col_name, id_value)) = fetch_heap_id_by_ctid(heap_relid, (bi_hi, bi_lo, ip_posid)) {
                                pgrx::info!(
                                    "METADATA_FIX_2024_DEBUG_XYZ789: Fallback CTID lookup succeeded: col_name={}, id_value={}",
                                    col_name,
                                    id_value
                                );
                                validated_doc_id_info = Some((col_name, id_value));
                            }
                        }
                        
                        // If not found, try to find nearby keys to understand the coordinate system
                        if validated_doc_id_info.is_none() && tuples_total == 1000 {
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: No ID mapping found for block_number={}, ip_posid={}", block_number, ip_posid);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map size: {}", id_map.len());
                            
                            // Show a few sample keys from the ID map to understand the coordinate system
                            let mut sample_keys: Vec<_> = id_map.keys().take(5).collect();
                            sample_keys.sort();
                            for (i, key) in sample_keys.iter().enumerate() {
                                let (col_name, id_value) = &id_map[key];
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map sample key {}: {:?} -> col='{}', id='{}'", i, key, col_name, id_value);
                            }
                            
                            // Try to find a key that might match our lookup
                            let mut found_keys = Vec::new();
                            for (key, value) in id_map.iter() {
                                if key.0 == block_number || key.0 == block_number + 1 || key.0 == block_number - 1 {
                                    found_keys.push((*key, value.clone()));
                                    if found_keys.len() >= 3 {
                                        break;
                                    }
                                }
                            }
                            if !found_keys.is_empty() {
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Found nearby keys: {:?}", found_keys);
                            }
                        }
                        
                        // Debug logging for lookup result
                        if tuples_total % 1000 == 0 {
                            match &validated_doc_id_info {
                                Some((col_name, id_value)) => {
                                    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Found ID mapping: col_name={}, id_value={}", col_name, id_value);
                                }
                                None => {
                                    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: No ID mapping found for block_number={}, ip_posid={}", block_number, ip_posid);
                                }
                            }
                        }
                        
                        if number_of_samples < max_number_of_samples {
                            samples.push(x);
                            sample_origins.push((bi_hi, bi_lo, ip_posid, extra, validated_doc_id_info));
                            number_of_samples += 1;
                        } else {
                            let index = rand.random_range(0..max_number_of_samples) as usize;
                            samples[index] = x;
                            sample_origins[index] = (bi_hi, bi_lo, ip_posid, extra, validated_doc_id_info);
                        }
                    }
                    tuples_total += 1;
                });

                pgrx::info!(
                    "sampling: Completed reservoir sampling of {} vectors from {} total vectors in table",
                    samples.len(),
                    tuples_total
                );

                // Save samples to file and exit
                pgrx::info!("saving {} samples to ann_samples.npy", samples.len());
                
                // Convert samples to ndarray for easy .npy writing
                use ndarray::Array2;
                use ndarray_npy::write_npy;
                
                // Flatten the samples into a single vector
                let dims = vector_options.dims as usize;
                let mut flat_samples = Vec::with_capacity(samples.len() * dims);
                for sample in &samples {
                    flat_samples.extend_from_slice(sample);
                }
                
                // Create ndarray from flattened data
                let samples_array = match Array2::from_shape_vec((samples.len(), dims), flat_samples) {
                    Ok(array) => array,
                    Err(e) => {
                        pgrx::error!("Failed to create ndarray from samples: {}", e);
                        // Since this function has a return type, we need to continue and let the error propagate
                        // The error logging above will help with debugging
                        // Note: This return is unreachable due to pgrx::error! behavior
                        return std::ptr::null_mut();
                    }
                };
                
                // Save metadata for analysis
                let mut metadata_rows = Vec::with_capacity(samples.len());
                
                // UNIQUE_IDENTIFIER: METADATA_FIX_2024_DEBUG_XYZ789 - This confirms latest code is running
                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Starting metadata generation with table name resolution");
                
                // Get the actual table name from the relation
                let actual_table_name = unsafe {
                    let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
                    if !rel.is_null() {
                        let relname = pgrx::pg_sys::get_rel_name(heap_relid);
                        if !relname.is_null() {
                            let table_name = CStr::from_ptr(relname).to_string_lossy().to_string();
                            pgrx::pg_sys::RelationClose(rel);
                            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Resolved table name: {}", table_name);
                            table_name
                        } else {
                            pgrx::pg_sys::RelationClose(rel);
                            pgrx::warning!("METADATA_FIX_2024_DEBUG_XYZ789: Failed to get relname, using unknown");
                            "unknown".to_string()
                        }
                    } else {
                        pgrx::warning!("METADATA_FIX_2024_DEBUG_XYZ789: Failed to get relation, using unknown");
                        "unknown".to_string()
                    }
                };
                
                for (bi_hi, bi_lo, ip_posid, extra, doc_id_info) in &sample_origins {
                    let doc_id = if let Some((_column_name, id_value)) = doc_id_info {
                        id_value.clone()
                    } else {
                        format!("ctid_{}_{}_{}", bi_hi, bi_lo, ip_posid)
                    };
                    // Always emit the resolved table name; fallback to "unknown" only if relation lookup failed
                    let table_name = actual_table_name.clone();
                    metadata_rows.push(format!("{}|{}|{}_{}_{}|{}", 
                        doc_id, 
                        table_name, 
                        bi_hi, bi_lo, ip_posid, 
                        extra
                    ));
                }
                
                // Get current working directory for debugging
                let current_dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("unknown"));
                pgrx::info!("Current working directory: {:?}", current_dir);
                
                // Write to the shared directory that's mounted from host
                let file_path = "/var/local/postgres_shared/ann_samples.npy";
                let metadata_file_path = "/var/local/postgres_shared/ann_samples_metadata.txt";
                pgrx::info!("Attempting to write to: {} and {}", file_path, metadata_file_path);
                
                // Try to write to the primary location first
                let write_result = write_npy(file_path, &samples_array);
                let metadata_write_result = std::fs::write(metadata_file_path, metadata_rows.join("\n"));
                
                if write_result.is_ok() && metadata_write_result.is_ok() {
                    pgrx::info!("Successfully saved {} samples to {}", samples.len(), file_path);
                    pgrx::info!("Successfully saved metadata to {}", metadata_file_path);
                    pgrx::info!("Metadata format: doc_id|table_name|ctid_bi_hi_bi_lo_posid|token_index");
                    
                    // Relax permissions so host can read
                    use std::os::unix::fs::PermissionsExt;
                    if let Err(e) = std::fs::set_permissions(file_path, std::fs::Permissions::from_mode(0o666)) {
                        pgrx::warning!("Failed to set file permissions: {}", e);
                    } else {
                        pgrx::info!("Set file permissions to 666 for host access");
                    }
                    if let Err(e) = std::fs::set_permissions(metadata_file_path, std::fs::Permissions::from_mode(0o666)) {
                        pgrx::warning!("Failed to set metadata file permissions: {}", e);
                    } else {
                        pgrx::info!("Set metadata file permissions to 666 for host access");
                    }
                    
                    // Best-effort chown (may not be available depending on container)
                    use std::process::Command;
                    let _ = Command::new("chown").args(&["1000:1000", file_path]).output();
                    let _ = Command::new("chown").args(&["1000:1000", metadata_file_path]).output();
                    
                    // Abort indexing after saving, as requested
                    pgrx::error!(
                        "ANN samples saved to {} and metadata to {} ({} vectors). Aborting index build as requested.",
                        file_path, metadata_file_path, samples.len()
                    );
                } else {
                    // If primary path failed, log error and abort
                    let err = write_result.unwrap_err();
                    let metadata_err = metadata_write_result.unwrap_err();
                    pgrx::error!("Failed to write {}: {}. Failed to write metadata: {}. Aborting index build.", file_path, err, metadata_err);
                }
            };
            
            // Samples have been saved and process has exited, so this code should never be reached
            // But if it somehow is, we'll continue with the index build
            pgrx::info!("Continuing with index build after saving samples");
            
            reporter.tuples_total(tuples_total);
            // Build once-per-build CTID -> ID map for reliable id resolution
            let heap_relid = unsafe { (*heap.heap_relation).rd_id };
            let id_map = build_ctid_id_map(heap_relid);

            let (structures, centroid_representatives) = make_internal_build_with_tracking(
                vector_options.clone(),
                internal_build.clone(),
                samples,
                sample_origins,
                &mut reporter,
                heap_relid,
                &id_map,
            );
            centroid_representatives_for_analysis = Some(centroid_representatives);
            tuples_total_for_analysis = Some(tuples_total);
            structures
        }
    };
    reporter.phase(BuildPhase::from_code(BuildPhaseCode::Build));
    // Build consumes `vector_options` and `structures`; analysis below uses clones
    let structures_for_analysis = structures
        .iter()
        .map(|s| Structure { centroids: s.centroids.clone(), children: s.children.clone() })
        .collect::<Vec<_>>();
    crate::index::vchordrq::algo::build(vector_options, vchordrq_options.index, &index, structures);
    reporter.phase(BuildPhase::from_code(BuildPhaseCode::Inserting));

    // Add cluster distribution analysis
    if let (VchordrqBuildSourceOptions::Internal(_), Some(centroid_representatives), Some(tuples_total)) = (&vchordrq_options.build.source, centroid_representatives_for_analysis, tuples_total_for_analysis) {
        let assignments = collect_cluster_assignments(&structures_for_analysis, &heap);
        let heap_relid = unsafe { (*heap.heap_relation).rd_id };
        // Build fresh CTID to ID map right before analysis to ensure current mappings
        // This prevents issues with stale CTIDs that may have changed due to VACUUM/REINDEX operations
        let id_map = build_ctid_id_map(heap_relid);
        analyze_and_report_cluster_distribution_with_centroids(
            &assignments,
            &structures_for_analysis,
            &centroid_representatives,
            tuples_total,
            heap_relid,
            &id_map,
        );
    }

    let cache = if vchordrq_options.build.pin {
        let mut trace = vchordrq::cache(&index);
        trace.sort();
        trace.dedup();
        if let Some(max) = trace.last().copied() {
            let mut mapping = vec![u32::MAX; 1 + max as usize];
            let mut pages = Vec::<Box<PostgresPage<vchordrq::Opaque>>>::with_capacity(trace.len());
            for id in trace {
                mapping[id as usize] = pages.len() as u32;
                pages.push(index.read(id).clone_into_boxed());
            }
            vchordrq_cached::VchordrqCached::_1 { mapping, pages }
        } else {
            vchordrq_cached::VchordrqCached::_0 {}
        }
    } else {
        vchordrq_cached::VchordrqCached::_0 {}
    };
    if let Some(leader) = unsafe {
        VchordrqLeader::enter(
            heap_relation,
            index_relation,
            (*index_info).ii_Concurrent,
            cache,
        )
    } {
        unsafe {
            parallel_build(
                index_relation,
                heap_relation,
                index_info,
                leader.tablescandesc,
                leader.vchordrqshared,
                leader.vchordrqcached,
                |indtuples| {
                    reporter.tuples_done(indtuples);
                },
            );
            leader.wait();
            let nparticipants = leader.nparticipants;
            loop {
                pgrx::pg_sys::SpinLockAcquire(&raw mut (*leader.vchordrqshared).mutex);
                if (*leader.vchordrqshared).nparticipantsdone == nparticipants {
                    pgrx::pg_sys::SpinLockRelease(&raw mut (*leader.vchordrqshared).mutex);
                    break;
                }
                pgrx::pg_sys::SpinLockRelease(&raw mut (*leader.vchordrqshared).mutex);
                pgrx::pg_sys::ConditionVariableSleep(
                    &raw mut (*leader.vchordrqshared).workersdonecv,
                    pgrx::pg_sys::WaitEventIPC::WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN as _,
                );
            }
            reporter.tuples_done((*leader.vchordrqshared).indtuples);
            reporter.tuples_total((*leader.vchordrqshared).indtuples);
            pgrx::pg_sys::ConditionVariableCancelSleep();
        }
    } else {
        let mut indtuples = 0;
        reporter.tuples_done(indtuples);
        heap.traverse(true, |(ctid, store)| {
            for (vector, extra) in store {
                let key = ctid_to_key(ctid);
                let payload = kv_to_pointer((key, extra));
                crate::index::vchordrq::algo::insert(opfamily, &index, payload, vector, true);
            }
            indtuples += 1;
            reporter.tuples_done(indtuples);
        });
        reporter.tuples_total(indtuples);
    }
    let check = || {
        pgrx::check_for_interrupts!();
    };
    reporter.phase(BuildPhase::from_code(BuildPhaseCode::Compacting));
    crate::index::vchordrq::algo::maintain(opfamily, &index, check);
    unsafe { pgrx::pgbox::PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc0().into_pg() }
}

struct VchordrqShared {
    /* Immutable state */
    heaprelid: pgrx::pg_sys::Oid,
    indexrelid: pgrx::pg_sys::Oid,
    isconcurrent: bool,

    /* Worker progress */
    workersdonecv: pgrx::pg_sys::ConditionVariable,

    /* Mutex for mutable state */
    mutex: pgrx::pg_sys::slock_t,

    /* Mutable state */
    nparticipantsdone: i32,
    indtuples: u64,
}

mod vchordrq_cached {
    pub const ALIGN: usize = 8;
    pub type Tag = u64;

    use crate::index::storage::PostgresPage;
    use algo::tuples::RefChecker;
    use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

    #[repr(C, align(8))]
    #[derive(Debug, Clone, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout)]
    struct VchordrqCachedHeader0 {}

    #[repr(C, align(8))]
    #[derive(Debug, Clone, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout)]
    struct VchordrqCachedHeader1 {
        mapping_s: usize,
        mapping_e: usize,
        pages_s: usize,
        pages_e: usize,
    }

    pub enum VchordrqCached {
        _0 {},
        _1 {
            mapping: Vec<u32>,
            pages: Vec<Box<PostgresPage<vchordrq::Opaque>>>,
        },
    }

    impl VchordrqCached {
        pub fn serialize(&self) -> Vec<u8> {
            let mut buffer = Vec::new();
            match self {
                VchordrqCached::_0 {} => {
                    buffer.extend((0 as Tag).to_ne_bytes());
                    buffer.extend(std::iter::repeat_n(0, size_of::<VchordrqCachedHeader0>()));
                    buffer[size_of::<Tag>()..][..size_of::<VchordrqCachedHeader0>()]
                        .copy_from_slice(VchordrqCachedHeader0 {}.as_bytes());
                }
                VchordrqCached::_1 { mapping, pages } => {
                    buffer.extend((1 as Tag).to_ne_bytes());
                    buffer.extend(std::iter::repeat_n(0, size_of::<VchordrqCachedHeader1>()));
                    let mapping_s = buffer.len();
                    buffer.extend(mapping.as_bytes());
                    let mapping_e = buffer.len();
                    while buffer.len() % ALIGN != 0 {
                        buffer.push(0u8);
                    }
                    let pages_s = buffer.len();
                    buffer.extend(pages.iter().flat_map(|x| unsafe {
                        std::mem::transmute::<&PostgresPage<vchordrq::Opaque>, &[u8; 8192]>(
                            x.as_ref(),
                        )
                    }));
                    let pages_e = buffer.len();
                    while buffer.len() % ALIGN != 0 {
                        buffer.push(0u8);
                    }
                    buffer[size_of::<Tag>()..][..size_of::<VchordrqCachedHeader1>()]
                        .copy_from_slice(
                            VchordrqCachedHeader1 {
                                mapping_s,
                                mapping_e,
                                pages_s,
                                pages_e,
                            }
                            .as_bytes(),
                        );
                }
            }
            buffer
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub enum VchordrqCachedReader<'a> {
        #[allow(dead_code)]
        _0(VchordrqCachedReader0<'a>),
        _1(VchordrqCachedReader1<'a>),
    }

    #[derive(Debug, Clone, Copy)]
    pub struct VchordrqCachedReader0<'a> {
        #[allow(dead_code)]
        header: &'a VchordrqCachedHeader0,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct VchordrqCachedReader1<'a> {
        #[allow(dead_code)]
        header: &'a VchordrqCachedHeader1,
        mapping: &'a [u32],
        pages: &'a [PostgresPage<vchordrq::Opaque>],
    }

    impl<'a> VchordrqCachedReader1<'a> {
        pub fn get(&self, id: u32) -> Option<&'a PostgresPage<vchordrq::Opaque>> {
            let index = *self.mapping.get(id as usize)?;
            if index == u32::MAX {
                return None;
            }
            Some(&self.pages[index as usize])
        }
    }

    impl<'a> VchordrqCachedReader<'a> {
        pub fn deserialize_ref(source: &'a [u8]) -> Self {
            let tag = u64::from_ne_bytes(std::array::from_fn(|i| source[i]));
            match tag {
                0 => {
                    let checker = RefChecker::new(source);
                    let header: &VchordrqCachedHeader0 = checker.prefix(size_of::<Tag>());
                    Self::_0(VchordrqCachedReader0 { header })
                }
                1 => {
                    let checker = RefChecker::new(source);
                    let header: &VchordrqCachedHeader1 = checker.prefix(size_of::<Tag>());
                    let mapping = checker.bytes(header.mapping_s, header.mapping_e);
                    let pages =
                        unsafe { checker.bytes_slice_unchecked(header.pages_s, header.pages_e) };
                    Self::_1(VchordrqCachedReader1 {
                        header,
                        mapping,
                        pages,
                    })
                }
                _ => panic!("bad bytes"),
            }
        }
    }
}

fn is_mvcc_snapshot(snapshot: *mut pgrx::pg_sys::SnapshotData) -> bool {
    matches!(
        unsafe { (*snapshot).snapshot_type },
        pgrx::pg_sys::SnapshotType::SNAPSHOT_MVCC
            | pgrx::pg_sys::SnapshotType::SNAPSHOT_HISTORIC_MVCC
    )
}

struct VchordrqLeader {
    pcxt: *mut pgrx::pg_sys::ParallelContext,
    nparticipants: i32,
    snapshot: pgrx::pg_sys::Snapshot,
    vchordrqshared: *mut VchordrqShared,
    tablescandesc: *mut pgrx::pg_sys::ParallelTableScanDescData,
    vchordrqcached: *const u8,
}

impl VchordrqLeader {
    pub unsafe fn enter(
        heap_relation: pgrx::pg_sys::Relation,
        index_relation: pgrx::pg_sys::Relation,
        isconcurrent: bool,
        cache: vchordrq_cached::VchordrqCached,
    ) -> Option<Self> {
        let _cache = cache.serialize();
        drop(cache);
        let cache = _cache;

        unsafe fn compute_parallel_workers(
            heap_relation: pgrx::pg_sys::Relation,
            index_relation: pgrx::pg_sys::Relation,
        ) -> i32 {
            unsafe {
                if pgrx::pg_sys::plan_create_index_workers(
                    (*heap_relation).rd_id,
                    (*index_relation).rd_id,
                ) == 0
                {
                    return 0;
                }
                if !(*heap_relation).rd_options.is_null() {
                    let std_options = (*heap_relation)
                        .rd_options
                        .cast::<pgrx::pg_sys::StdRdOptions>();
                    std::cmp::min(
                        (*std_options).parallel_workers,
                        pgrx::pg_sys::max_parallel_maintenance_workers,
                    )
                } else {
                    pgrx::pg_sys::max_parallel_maintenance_workers
                }
            }
        }

        let request = unsafe { compute_parallel_workers(heap_relation, index_relation) };
        if request <= 0 {
            return None;
        }

        unsafe {
            pgrx::pg_sys::EnterParallelMode();
        }
        let pcxt = unsafe {
            pgrx::pg_sys::CreateParallelContext(
                c"vchord".as_ptr(),
                c"vchordrq_parallel_build_main".as_ptr(),
                request,
            )
        };

        let snapshot = if isconcurrent {
            unsafe { pgrx::pg_sys::RegisterSnapshot(pgrx::pg_sys::GetTransactionSnapshot()) }
        } else {
            &raw mut pgrx::pg_sys::SnapshotAnyData
        };

        fn estimate_chunk(e: &mut pgrx::pg_sys::shm_toc_estimator, x: usize) {
            e.space_for_chunks += x.next_multiple_of(pgrx::pg_sys::ALIGNOF_BUFFER as _);
        }
        fn estimate_keys(e: &mut pgrx::pg_sys::shm_toc_estimator, x: usize) {
            e.number_of_keys += x;
        }
        let est_tablescandesc =
            unsafe { pgrx::pg_sys::table_parallelscan_estimate(heap_relation, snapshot) };
        unsafe {
            estimate_chunk(&mut (*pcxt).estimator, size_of::<VchordrqShared>());
            estimate_keys(&mut (*pcxt).estimator, 1);
            estimate_chunk(&mut (*pcxt).estimator, est_tablescandesc);
            estimate_keys(&mut (*pcxt).estimator, 1);
            estimate_chunk(&mut (*pcxt).estimator, 8 + cache.len());
            estimate_keys(&mut (*pcxt).estimator, 1);
        }

        unsafe {
            pgrx::pg_sys::InitializeParallelDSM(pcxt);
            if (*pcxt).seg.is_null() {
                if is_mvcc_snapshot(snapshot) {
                    pgrx::pg_sys::UnregisterSnapshot(snapshot);
                }
                pgrx::pg_sys::DestroyParallelContext(pcxt);
                pgrx::pg_sys::ExitParallelMode();
                return None;
            }
        }

        let vchordrqshared = unsafe {
            let vchordrqshared =
                pgrx::pg_sys::shm_toc_allocate((*pcxt).toc, size_of::<VchordrqShared>())
                    .cast::<VchordrqShared>();
            vchordrqshared.write(VchordrqShared {
                heaprelid: (*heap_relation).rd_id,
                indexrelid: (*index_relation).rd_id,
                isconcurrent,
                workersdonecv: std::mem::zeroed(),
                mutex: std::mem::zeroed(),
                nparticipantsdone: 0,
                indtuples: 0,
            });
            pgrx::pg_sys::ConditionVariableInit(&raw mut (*vchordrqshared).workersdonecv);
            pgrx::pg_sys::SpinLockInit(&raw mut (*vchordrqshared).mutex);
            vchordrqshared
        };

        let tablescandesc = unsafe {
            let tablescandesc = pgrx::pg_sys::shm_toc_allocate((*pcxt).toc, est_tablescandesc)
                .cast::<pgrx::pg_sys::ParallelTableScanDescData>();
            pgrx::pg_sys::table_parallelscan_initialize(heap_relation, tablescandesc, snapshot);
            tablescandesc
        };

        let vchordrqcached = unsafe {
            let x = pgrx::pg_sys::shm_toc_allocate((*pcxt).toc, 8 + cache.len()).cast::<u8>();
            (x as *mut u64).write_unaligned(cache.len() as _);
            std::ptr::copy(cache.as_ptr(), x.add(8), cache.len());
            x
        };

        unsafe {
            pgrx::pg_sys::shm_toc_insert((*pcxt).toc, 0xA000000000000001, vchordrqshared.cast());
            pgrx::pg_sys::shm_toc_insert((*pcxt).toc, 0xA000000000000002, tablescandesc.cast());
            pgrx::pg_sys::shm_toc_insert((*pcxt).toc, 0xA000000000000003, vchordrqcached.cast());
        }

        unsafe {
            pgrx::pg_sys::LaunchParallelWorkers(pcxt);
        }

        let nworkers_launched = unsafe { (*pcxt).nworkers_launched };

        unsafe {
            if nworkers_launched == 0 {
                pgrx::pg_sys::WaitForParallelWorkersToFinish(pcxt);
                if is_mvcc_snapshot(snapshot) {
                    pgrx::pg_sys::UnregisterSnapshot(snapshot);
                }
                pgrx::pg_sys::DestroyParallelContext(pcxt);
                pgrx::pg_sys::ExitParallelMode();
                return None;
            }
        }

        Some(Self {
            pcxt,
            nparticipants: nworkers_launched + 1,
            snapshot,
            vchordrqshared,
            tablescandesc,
            vchordrqcached,
        })
    }

    pub fn wait(&self) {
        unsafe {
            pgrx::pg_sys::WaitForParallelWorkersToAttach(self.pcxt);
        }
    }
}

impl Drop for VchordrqLeader {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            unsafe {
                pgrx::pg_sys::WaitForParallelWorkersToFinish(self.pcxt);
                if is_mvcc_snapshot(self.snapshot) {
                    pgrx::pg_sys::UnregisterSnapshot(self.snapshot);
                }
                pgrx::pg_sys::DestroyParallelContext(self.pcxt);
                pgrx::pg_sys::ExitParallelMode();
            }
        }
    }
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vchordrq_parallel_build_main(
    _seg: *mut pgrx::pg_sys::dsm_segment,
    toc: *mut pgrx::pg_sys::shm_toc,
) {
    let vchordrqshared = unsafe {
        pgrx::pg_sys::shm_toc_lookup(toc, 0xA000000000000001, false).cast::<VchordrqShared>()
    };
    let tablescandesc = unsafe {
        pgrx::pg_sys::shm_toc_lookup(toc, 0xA000000000000002, false)
            .cast::<pgrx::pg_sys::ParallelTableScanDescData>()
    };
    let vchordrqcached = unsafe {
        pgrx::pg_sys::shm_toc_lookup(toc, 0xA000000000000003, false)
            .cast::<u8>()
            .cast_const()
    };
    let heap_lockmode;
    let index_lockmode;
    if unsafe { !(*vchordrqshared).isconcurrent } {
        heap_lockmode = pgrx::pg_sys::ShareLock as pgrx::pg_sys::LOCKMODE;
        index_lockmode = pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE;
    } else {
        heap_lockmode = pgrx::pg_sys::ShareUpdateExclusiveLock as pgrx::pg_sys::LOCKMODE;
        index_lockmode = pgrx::pg_sys::RowExclusiveLock as pgrx::pg_sys::LOCKMODE;
    }
    let heap = unsafe { pgrx::pg_sys::table_open((*vchordrqshared).heaprelid, heap_lockmode) };
    let index = unsafe { pgrx::pg_sys::index_open((*vchordrqshared).indexrelid, index_lockmode) };
    let index_info = unsafe { pgrx::pg_sys::BuildIndexInfo(index) };
    unsafe {
        (*index_info).ii_Concurrent = (*vchordrqshared).isconcurrent;
    }

    unsafe {
        parallel_build(
            index,
            heap,
            index_info,
            tablescandesc,
            vchordrqshared,
            vchordrqcached,
            |_| (),
        );
    }

    unsafe {
        pgrx::pg_sys::index_close(index, index_lockmode);
        pgrx::pg_sys::table_close(heap, heap_lockmode);
    }
}

unsafe fn parallel_build(
    index_relation: pgrx::pg_sys::Relation,
    heap_relation: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
    tablescandesc: *mut pgrx::pg_sys::ParallelTableScanDescData,
    vchordrqshared: *mut VchordrqShared,
    vchordrqcached: *const u8,
    mut callback: impl FnMut(u64),
) {
    use vchordrq_cached::VchordrqCachedReader;
    let cached = VchordrqCachedReader::deserialize_ref(unsafe {
        let bytes = (vchordrqcached as *const u64).read_unaligned();
        std::slice::from_raw_parts(vchordrqcached.add(8), bytes as _)
    });

    let index = unsafe { PostgresRelation::new(index_relation) };

    let scan = unsafe { pgrx::pg_sys::table_beginscan_parallel(heap_relation, tablescandesc) };
    let opfamily = unsafe { opfamily(index_relation) };
    let heap = Heap {
        heap_relation,
        index_relation,
        index_info,
        opfamily,
        scan,
    };
    match cached {
        VchordrqCachedReader::_0(_) => {
            heap.traverse(true, |(ctid, store)| {
                for (vector, extra) in store {
                    let key = ctid_to_key(ctid);
                    let payload = kv_to_pointer((key, extra));
                    crate::index::vchordrq::algo::insert(opfamily, &index, payload, vector, true);
                }
                unsafe {
                    let indtuples;
                    {
                        pgrx::pg_sys::SpinLockAcquire(&raw mut (*vchordrqshared).mutex);
                        (*vchordrqshared).indtuples += 1;
                        indtuples = (*vchordrqshared).indtuples;
                        pgrx::pg_sys::SpinLockRelease(&raw mut (*vchordrqshared).mutex);
                    }
                    callback(indtuples);
                }
            });
        }
        VchordrqCachedReader::_1(cached) => {
            let index = CachingRelation {
                cache: cached,
                relation: index,
            };
            heap.traverse(true, |(ctid, store)| {
                for (vector, extra) in store {
                    let key = ctid_to_key(ctid);
                    let payload = kv_to_pointer((key, extra));
                    crate::index::vchordrq::algo::insert(opfamily, &index, payload, vector, true);
                }
                unsafe {
                    let indtuples;
                    {
                        pgrx::pg_sys::SpinLockAcquire(&raw mut (*vchordrqshared).mutex);
                        (*vchordrqshared).indtuples += 1;
                        indtuples = (*vchordrqshared).indtuples;
                        pgrx::pg_sys::SpinLockRelease(&raw mut (*vchordrqshared).mutex);
                    }
                    callback(indtuples);
                }
            });
        }
    }
    unsafe {
        pgrx::pg_sys::SpinLockAcquire(&raw mut (*vchordrqshared).mutex);
        (*vchordrqshared).nparticipantsdone += 1;
        pgrx::pg_sys::SpinLockRelease(&raw mut (*vchordrqshared).mutex);
        pgrx::pg_sys::ConditionVariableSignal(&raw mut (*vchordrqshared).workersdonecv);
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambuildempty(_index_relation: pgrx::pg_sys::Relation) {
    pgrx::error!("Unlogged indexes are not supported.");
}

unsafe fn options(
    index_relation: pgrx::pg_sys::Relation,
) -> (VectorOptions, VchordrqIndexingOptions) {
    let att = unsafe { &mut *(*index_relation).rd_att };
    #[cfg(any(
        feature = "pg13",
        feature = "pg14",
        feature = "pg15",
        feature = "pg16",
        feature = "pg17"
    ))]
    let atts = unsafe { att.attrs.as_slice(att.natts as _) };
    #[cfg(feature = "pg18")]
    let atts = unsafe {
        let ptr = att
            .compact_attrs
            .as_ptr()
            .add(att.natts as _)
            .cast::<pgrx::pg_sys::FormData_pg_attribute>();
        std::slice::from_raw_parts(ptr, att.natts as _)
    };
    if atts.is_empty() {
        pgrx::error!("indexing on no columns is not supported");
    }
    if atts.len() != 1 {
        pgrx::error!("multicolumn index is not supported");
    }
    // get dims
    let typmod = Typmod::parse_from_i32(atts[0].atttypmod).unwrap();
    let dims = if let Some(dims) = typmod.dims() {
        dims.get()
    } else {
        pgrx::error!(
            "Dimensions type modifier of a vector column is needed for building the index."
        );
    };
    // get v, d
    let opfamily = unsafe { opfamily(index_relation) };
    let vector = VectorOptions {
        dims,
        v: opfamily.vector_kind(),
        d: opfamily.distance_kind(),
    };
    // get indexing, segment, optimizing
    let rabitq = 'rabitq: {
        let reloption = unsafe { (*index_relation).rd_options as *const Reloption };
        if reloption.is_null() || unsafe { (*reloption).options == 0 } {
            break 'rabitq Default::default();
        }
        let s = unsafe { Reloption::options(reloption) }.to_string_lossy();
        match toml::from_str::<VchordrqIndexingOptions>(&s) {
            Ok(p) => p,
            Err(e) => pgrx::error!("failed to parse options: {}", e),
        }
    };
    (vector, rabitq)
}

// Add this structure to track centroid representatives
#[derive(Debug, Clone)]
struct CentroidRepresentative {
    doc_id: (u16, u16, u16), // ctid as [bi_hi, bi_lo, ip_posid]
    token_index: u16,        // extra field (token position in document)
    distance_to_centroid: f32, // how close this vector is to the computed centroid
    id_info: Option<(String, String)>, // (col_name, id_value)
}

// ---------- CTID -> ID helpers and map ----------

fn block_from_parts(hi: u16, lo: u16) -> u32 {
    ((hi as u32) << 16) | (lo as u32)
}

fn parse_tid_text(t: &str) -> Option<(u32, u16)> {
    let t = t.trim();
    let s = t.strip_prefix('(')?.strip_suffix(')')?;
    let mut it = s.split(',');
    let blk: u32 = it.next()?.trim().parse().ok()?;
    let off: u16 = it.next()?.trim().parse().ok()?;
    
    // Debug logging to understand the coordinate system
    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: parse_tid_text: '{}' -> blk={}, off={}", t, blk, off);
    
    // The CTID text from PostgreSQL shows the actual block number, not bi_hi/bi_lo
    // So we can use it directly as the block number
    Some((blk, off))
}

fn build_ctid_id_map(heap_relid: pgrx::pg_sys::Oid) -> HashMap<(u32, u16), (String, String)> {
    let mut m = HashMap::new();

    unsafe {
        let mut used_pk = false;
        let mut is_partition = false;
        let mut is_partitioned_parent = false;

        let _ = pgrx::spi::Spi::connect(|client| {
            let mut parent_relid = heap_relid;

            let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
            if !rel.is_null() {
                is_partition = (*(*rel).rd_rel).relispartition;
                is_partitioned_parent =
                    (*(*rel).rd_rel).relkind == pgrx::pg_sys::RELKIND_PARTITIONED_TABLE as i8;
                pgrx::pg_sys::RelationClose(rel);
            }

            if is_partition {
                let parent_query = format!("SELECT inhparent FROM pg_inherits WHERE inhrelid = {}", heap_relid);
                if let Ok(table) = client.select(&parent_query, Some(1), &[]) {
                    if let Some(row) = table.into_iter().next() {
                        if let Ok(Some(oid)) = row.get_by_name::<pgrx::pg_sys::Oid, _>("inhparent") {
                            parent_relid = oid;
                        }
                    }
                }
            }

            let tables_to_query = if is_partition || is_partitioned_parent {
                get_table_partitions(parent_relid, &client)
            } else {
                let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
                if rel.is_null() { return Ok::<_, pgrx::spi::Error>(()); }
                let nsp = (*(*rel).rd_rel).relnamespace;
                let relname = pgrx::pg_sys::get_rel_name(heap_relid);
                let nspname = pgrx::pg_sys::get_namespace_name(nsp);
                if relname.is_null() || nspname.is_null() {
                    pgrx::pg_sys::RelationClose(rel);
                    return Ok::<_, pgrx::spi::Error>(());
                }
                let schema = CStr::from_ptr(nspname).to_string_lossy().to_string();
                let table = CStr::from_ptr(relname).to_string_lossy().to_string();
                pgrx::pg_sys::RelationClose(rel);
                let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                vec![format!("{}.{}", quote_ident(&schema), quote_ident(&table))]
            };

            let mut query_and_populate = |col_name: &str| -> Result<bool, pgrx::spi::Error> {
                let mut populated = false;
                
                // First, let's test what the actual block numbers look like
                if m.is_empty() {
                    let test_sql = format!("SELECT ctid::text AS tid, {}::text AS id, ctid AS raw_ctid FROM {} LIMIT 5", col_name, tables_to_query[0]);
                    if let Ok(test_rows) = client.select(&test_sql, None, &[]) {
                        pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Testing CTID representation:");
                        for r in test_rows {
                            if let (Some(tid_txt), Some(id_txt), Some(raw_ctid)) = (
                                r.get_by_name::<String, _>("tid").unwrap_or(None),
                                r.get_by_name::<String, _>("id").unwrap_or(None),
                                r.get_by_name::<ItemPointerData, _>("raw_ctid").unwrap_or(None)
                            ) {
                                let [test_bi_hi, test_bi_lo, test_ip_posid] = ctid_to_key(raw_ctid);
                                let test_block_number = block_from_parts(test_bi_hi, test_bi_lo);
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: CTID text: '{}', ID: '{}', raw_ctid: {:?}", tid_txt, id_txt, raw_ctid);
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ctid_to_key: [bi_hi={}, bi_lo={}, ip_posid={}]", test_bi_hi, test_bi_lo, test_ip_posid);
                                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: block_from_parts({}, {}) = {}", test_bi_hi, test_bi_lo, test_block_number);
                            }
                        }
                    }
                }
                
                for t_name in &tables_to_query {
                    let sql = format!("SELECT ctid AS raw_ctid, {}::text AS id FROM {}", col_name, t_name);
                    if let Ok(rows) = client.select(&sql, None, &[]) {
                        for r in rows {
                            if let (Some(raw_ctid), Some(id_txt)) = (
                                r.get_by_name::<ItemPointerData, _>("raw_ctid").unwrap_or(None),
                                r.get_by_name::<String, _>("id").unwrap_or(None)
                            ) {
                                let [bi_hi, bi_lo, ip_posid] = ctid_to_key(raw_ctid);
                                let blk = block_from_parts(bi_hi, bi_lo);
                                if m.len() < 5 {
                                    pgrx::info!(
                                        "METADATA_FIX_2024_DEBUG_XYZ789: Insert map key (blk={}, off={}) -> col='{}', id='{}'",
                                        blk, ip_posid, col_name, id_txt
                                    );
                                }
                                m.insert((blk, ip_posid), (col_name.to_string(), id_txt));
                                populated = true;
                            }
                        }
                    }
                }
                Ok(populated)
            };

            if query_and_populate("id")? {
                // Populated with "id"
            } else {
                let pk_sql = format!(
                    "SELECT a.attname AS colname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = {} AND i.indisprimary AND a.attnum > 0 AND NOT a.attisdropped ORDER BY array_position(i.indkey, a.attnum) ASC LIMIT 1",
                    parent_relid // Use parent relid for PK lookup
                );
                if let Ok(pk_rows) = client.select(&pk_sql, None, &[]) {
                    if let Some(col) = pk_rows.first().get_by_name::<String, _>("colname").unwrap_or(None) {
                        used_pk = true;
                        if query_and_populate(&format!("\"{}\"", col.replace('"', "\"\"")))? {
                            // Populated with PK
                        }
                    }
                }
                if !used_pk {
                    let has_ad_id_sql = format!("SELECT 1 FROM pg_attribute WHERE attrelid = {} AND attname = 'ad_id' AND NOT attisdropped LIMIT 1", heap_relid);
                    if client.select(&has_ad_id_sql, None, &[]).map(|r| r.len() > 0).unwrap_or(false) {
                        if query_and_populate("ad_id")? {
                            // Populated with ad_id
                        }
                    }
                }
            }
            Ok::<_, pgrx::spi::Error>(())
        });

        pgrx::info!(
            "id-map: built {} entries (source: {}, partitioned: {})",
            m.len(),
            if used_pk { "primary key" } else { "id column" },
            is_partition || is_partitioned_parent,
        );
        
        // Debug logging to show sample keys in the ID map
        if !m.is_empty() {
            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: ID map built successfully with {} entries", m.len());
            let mut sample_keys: Vec<_> = m.keys().take(5).collect();
            sample_keys.sort();
            for (i, key) in sample_keys.iter().enumerate() {
                let (col_name, id_value) = &m[key];
                pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: Sample key {}: {:?} -> col='{}', id='{}'", i, key, col_name, id_value);
            }
        }
        
        if m.is_empty() {
            pgrx::warning!("id-map: empty map built; will rely on per-CTID fallback");
        }
    }

    m
}

fn id_from_map(
    id_map: &HashMap<(u32, u16), (String, String)>,
    doc_id: (u16, u16, u16),
) -> Option<(String, String)> {
    // The doc_id is in the format (bi_hi, bi_lo, ip_posid) from the internal CTID structure
    // But the ID map was built using CTID text which shows the actual block number directly
    // We need to convert the internal CTID to match the coordinate system used in the ID map
    
    // Option 1: Use block_from_parts (current approach - may be wrong)
    let blk_from_parts = block_from_parts(doc_id.0, doc_id.1);
    
    // Option 2: Use the bi_hi directly as the block number (if bi_lo is always 0 for small tables)
    let blk_direct = doc_id.0 as u32;
    
    // Option 3: Try both approaches to see which one works
    let off = doc_id.2;
    
    // Debug logging to understand the coordinate system conversion
    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: doc_id={:?} -> blk_from_parts={}, blk_direct={}, off={}", doc_id, blk_from_parts, blk_direct, off);
    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: looking up keys ({}, {}) and ({}, {}) in map with {} entries", blk_from_parts, off, blk_direct, off, id_map.len());
    
    // Log a few sample keys from the map for debugging
    if id_map.len() <= 10 {
        for (key, value) in id_map.iter().take(5) {
            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: map contains key {:?} -> {:?}", key, value);
        }
    } else {
        // For larger maps, show a few sample keys to understand the coordinate system
        let mut sample_keys: Vec<_> = id_map.keys().take(5).collect();
        sample_keys.sort();
        for (i, key) in sample_keys.iter().enumerate() {
            let (col_name, id_value) = &id_map[key];
            pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: sample key {}: {:?} -> col='{}', id='{}'", i, key, col_name, id_value);
        }
    }
    
    // Try both coordinate systems to see which one works
    if let Some(result) = id_map.get(&(blk_from_parts, off)) {
        pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: found using block_from_parts({}, {})", blk_from_parts, off);
        return Some(result.clone());
    }
    
    if let Some(result) = id_map.get(&(blk_direct, off)) {
        pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: found using blk_direct({}, {})", blk_direct, off);
        return Some(result.clone());
    }
    
    pgrx::info!("METADATA_FIX_2024_DEBUG_XYZ789: id_from_map: no match found for either coordinate system");
    None
}

// Add this structure to track cluster assignments with centroid info
#[derive(Debug, Clone)]
struct VectorAssignment {
    doc_id: (u16, u16, u16), // ctid as [bi_hi, bi_lo, ip_posid]
    token_index: u16,        // extra field (token position in document)
    cluster_path: Vec<u32>,  // which clusters at each level
    _distance: f32,           // distance to final centroid (unused for now)
}

// Add this structure to track detailed cluster info
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ClusterInfo {
    level: usize,
    cluster_id: u32,
    centroid_vector: Vec<f32>,
    representative: Option<CentroidRepresentative>,
    member_count: u64,
    sample_members: Vec<(u16, u16, u16, u16)>, // Sample doc_ids and token_indices
}

// ---------- Partition Discovery Helper ----------
fn get_table_partitions(heap_relid: pgrx::pg_sys::Oid, client: &pgrx::spi::SpiClient) -> Vec<String> {
    let mut partitions = Vec::new();
    let query = format!(
        "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = {}",
        heap_relid
    );
    if let Ok(table) = client.select(&query, None, &[]) {
        for row in table {
            if let Ok(Some(part_name)) = row.get_by_name::<String, _>("inhrelid") {
                partitions.push(part_name);
            }
        }
    }
    partitions
}

// Modified make_internal_build to track centroid representatives
fn make_internal_build_with_tracking(
    vector_options: VectorOptions,
    internal_build: VchordrqInternalBuildOptions,
    mut samples: Vec<Vec<f32>>,
    sample_origins: Vec<(u16, u16, u16, u16, Option<(String, String)>)>, // origin + pre-resolved id
    reporter: &mut PostgresReporter,
    heap_relid_for_ids: pgrx::pg_sys::Oid,
    id_map: &HashMap<(u32, u16), (String, String)>,
) -> (Vec<Structure<Vec<f32>>>, Vec<Vec<CentroidRepresentative>>) {
    use std::iter::once;
    
    k_means::preprocess(internal_build.build_threads as _, &mut samples, |sample| {
        *sample = rabitq::rotate::rotate(sample)
    });
    
    let mut result = Vec::<Structure<Vec<f32>>>::new();
    let mut centroid_representatives = Vec::<Vec<CentroidRepresentative>>::new();
    
    for w in internal_build.lists.iter().rev().copied().chain(once(1)) {
        let (input, input_origins) = if let Some(structure) = result.last() {
            // For subsequent levels, use previous centroids as input
            // Origins would be the representatives from previous level
            (&structure.centroids, centroid_representatives.last().unwrap().iter().map(|rep| (rep.doc_id.0, rep.doc_id.1, rep.doc_id.2, rep.token_index, rep.id_info.clone())).collect::<Vec<_>>())
        } else {
            // First level uses original samples
            (&samples, sample_origins.clone())
        };
        
        let num_threads = internal_build.build_threads as _;
        let num_points = input.len();
        let num_dims = vector_options.dims as usize;
        let num_lists = w as usize;
        let num_iterations = internal_build.kmeans_iterations as _;
        
        if result.is_empty() {
            let percentage = 0;
            let default = BuildPhase::from_code(BuildPhaseCode::InternalBuild);
            let phase = BuildPhase::new(BuildPhaseCode::InternalBuild, 1 + percentage).unwrap_or(default);
            reporter.phase(phase);
        }
        
        if num_lists > 1 {
            pgrx::info!(
                "clustering: startingggggggggg22222, using {num_threads} threads, clustering {num_points} vectors of {num_dims} dimension into {num_lists} clusters, in {num_iterations} iterations"
            );
            pgrx::info!(
                "clustering: training centroids using {} samples",
                num_points
            );
        }
        
        // Dump current input vectors and their corresponding doc/token ids to .npy, then exit
        use ndarray::Array2;
        use ndarray_npy::write_npy;
        let base_dir = std::env::var("VCHORD_DUMP_DIR").unwrap_or_else(|_| "/var/local/VectorChord".to_string());
        let _ = std::fs::create_dir_all(&base_dir);
        let vectors_path = format!(
            "{}/kmeans_input_vectors_w{}_points{}_dims{}.npy",
            base_dir, num_lists, num_points, num_dims
        );
        let ids_path = format!(
            "{}/kmeans_input_doc_ids_w{}_points{}.npy",
            base_dir, num_lists, num_points
        );
        let _metadata_path = format!(
            "{}/kmeans_input_metadata_w{}_points{}.npy",
            base_dir, num_lists, num_points
        );
        
        let mut flat_vectors = Vec::with_capacity(num_points * num_dims);
        for v in input.iter() {
            flat_vectors.extend_from_slice(v);
        }
        let vectors_array = match Array2::from_shape_vec((num_points, num_dims), flat_vectors) {
            Ok(a) => a,
            Err(e) => {
                pgrx::error!("failed to build ndarray for vectors: {}", e);
            }
        };
        
        // Save CTID components and token indices
        let mut flat_ids = Vec::with_capacity(num_points * 4);
        for (bi_hi, bi_lo, ip_posid, extra, _doc_id_info) in input_origins.iter() {
            flat_ids.push(*bi_hi as u32);
            flat_ids.push(*bi_lo as u32);
            flat_ids.push(*ip_posid as u32);
            flat_ids.push(*extra as u32);
        }
        let ids_array = match Array2::from_shape_vec((num_points, 4usize), flat_ids) {
            Ok(a) => a,
            Err(e) => {
                pgrx::error!("failed to build ndarray for ids: {}", e);
            }
        };
        
        // Save document IDs and metadata for analysis
        let mut doc_ids = Vec::with_capacity(num_points);
        let mut table_names = Vec::with_capacity(num_points);
        for (bi_hi, bi_lo, ip_posid, _extra, doc_id_info) in input_origins.iter() {
            if let Some((doc_id, table_name)) = doc_id_info {
                doc_ids.push(doc_id.clone());
                table_names.push(table_name.clone());
            } else {
                // Fallback to CTID-based identifier if document ID couldn't be resolved
                doc_ids.push(format!("ctid_{}_{}_{}", bi_hi, bi_lo, ip_posid));
                table_names.push("unknown".to_string());
            }
        }
        
        // Save metadata as structured data (doc_id, table_name, ctid_components, token_index)
        let mut metadata_rows = Vec::with_capacity(num_points);
        for (i, (bi_hi, bi_lo, ip_posid, extra, _doc_id_info)) in input_origins.iter().enumerate() {
            metadata_rows.push(format!("{}|{}|{}_{}_{}|{}", 
                doc_ids[i], 
                table_names[i], 
                bi_hi, bi_lo, ip_posid, 
                extra
            ));
        }
        
        // Write metadata to a separate text file for easy analysis
        let metadata_text_path = format!(
            "{}/kmeans_input_metadata_w{}_points{}.txt",
            base_dir, num_lists, num_points
        );
        if let Err(e) = std::fs::write(&metadata_text_path, metadata_rows.join("\n")) {
            pgrx::error!("failed to write metadata text file: {}", e);
        }
        if let Err(e) = write_npy(&vectors_path, &vectors_array) {
            pgrx::error!("failed to write vectors .npy: {}", e);
        }
        if let Err(e) = write_npy(&ids_path, &ids_array) {
            pgrx::error!("failed to write ids .npy: {}", e);
        }
        pgrx::info!("dumped numpy files: {} and {}", vectors_path, ids_path);
        pgrx::info!("dumped metadata file: {}", metadata_text_path);
        pgrx::info!("metadata format: doc_id|table_name|ctid_bi_hi_bi_lo_posid|token_index");
        pgrx::error!(
            "stopping after dumping k-means input as requested (set by instrumentation)"
        );
        
        // Note: The following code is unreachable due to pgrx::error! behavior
        // but is kept for completeness and potential future use

        let centroids = k_means::k_means(
            num_threads,
            |i| {
                pgrx::check_for_interrupts!();
                if result.is_empty() {
                    let percentage = ((i as f64 / num_iterations as f64) * 100.0).clamp(0.0, 100.0) as u16;
                    let default = BuildPhase::from_code(BuildPhaseCode::InternalBuild);
                    let phase = BuildPhase::new(BuildPhaseCode::InternalBuild, 1 + percentage).unwrap_or(default);
                    reporter.phase(phase);
                }
                if num_lists > 1 {
                    pgrx::info!("clustering: iteration {}", i + 1);
                }
            },
            num_lists,
            num_dims,
            input,
            internal_build.spherical_centroids,
            num_iterations,
        );
        
        // Find the closest original vector to each computed centroid
        let mut level_representatives = Vec::new();
        for (cluster_idx, centroid) in centroids.iter().enumerate() {
            let mut best_representative = None;
            let mut best_distance = f32::INFINITY;
            
            for (input_idx, input_vector) in input.iter().enumerate() {
                let distance = f32::reduce_sum_of_d2(input_vector, centroid);
                if distance < best_distance {
                    best_distance = distance;
                    let (bi_hi, bi_lo, ip_posid, extra, doc_id_info) = input_origins[input_idx].clone();
                    let doc_id = (bi_hi, bi_lo, ip_posid);
                    
                    // Resolve id from map first, then fallback
                    let id_info = doc_id_info
                        .clone()
                        .or_else(|| id_from_map(id_map, doc_id));
                    
                    // Try to refresh stale CTID if we have valid ID info
                    let refreshed_doc_id = if let Some(ref id_info) = id_info {
                        refresh_stale_ctid(heap_relid_for_ids, doc_id, &Some(id_info.clone())).unwrap_or(doc_id)
                    } else {
                        doc_id
                    };
                    
                    best_representative = Some(CentroidRepresentative {
                        doc_id: refreshed_doc_id,
                        token_index: extra,
                        distance_to_centroid: distance,
                        id_info,
                    });
                }
            }
            
            level_representatives.push(best_representative.unwrap_or(CentroidRepresentative {
                doc_id: (0, 0, 0),
                token_index: 0,
                distance_to_centroid: f32::INFINITY,
                id_info: None,
            }));
            
            if num_lists > 1 {
                let rep = &level_representatives[cluster_idx];
                pgrx::info!(
                    "clustering: centroid {cid}, representative document's token is doc(ctid_bi_hi={hi}, ctid_bi_lo={lo}, ip_posid={pos}, token_id={tok}) aka doc({hi},{lo},{pos})_token{tok} (token's distance from the centroid is: {dist:.4})",
                    cid = cluster_idx,
                    hi = rep.doc_id.0,
                    lo = rep.doc_id.1,
                    pos = rep.doc_id.2,
                    tok = rep.token_index,
                    dist = rep.distance_to_centroid
                );
            }
        }
        
        // Resolve and cache id_info for representatives now to avoid CTID drift later
        let resolved_level_reps = level_representatives
            .into_iter()
            .map(|r| r)
            .collect::<Vec<_>>();
        centroid_representatives.push(resolved_level_reps);
        
        if result.is_empty() {
            let percentage = 100;
            let default = BuildPhase::from_code(BuildPhaseCode::InternalBuild);
            let phase = BuildPhase::new(BuildPhaseCode::InternalBuild, 1 + percentage).unwrap_or(default);
            reporter.phase(phase);
        }
        
        if num_lists > 1 {
            pgrx::info!("clustering: finished");
        }
        
        if let Some(structure) = result.last() {
            let mut children = vec![Vec::new(); centroids.len()];
            for i in 0..structure.len() as u32 {
                let target = k_means::k_means_lookup(&structure.centroids[i as usize], &centroids);
                children[target].push(i);
            }
            let (centroids, children) = std::iter::zip(centroids, children)
                .filter(|(_, x)| !x.is_empty())
                .unzip::<_, _, Vec<_>, Vec<_>>();
            result.push(Structure {
                centroids,
                children,
            });
        } else {
            let children = vec![Vec::new(); centroids.len()];
            result.push(Structure {
                centroids,
                children,
            });
        }
    }
    
    (result, centroid_representatives)
}

#[allow(clippy::collapsible_else_if)]
fn make_external_build(
    vector_options: VectorOptions,
    _opfamily: Opfamily,
    external_build: VchordrqExternalBuildOptions,
) -> Vec<Structure<Vec<f32>>> {
    use std::collections::BTreeMap;
    let VchordrqExternalBuildOptions { table } = external_build;
    let mut parents = BTreeMap::new();
    let mut vectors = BTreeMap::new();
    pgrx::spi::Spi::connect(|client| {
        use crate::datatype::memory_vector::VectorOutput;
        use pgrx::pg_sys::panic::ErrorReportable;
        use vector::VectorBorrowed;
        let schema_query = "SELECT n.nspname::TEXT 
            FROM pg_catalog.pg_extension e
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname = 'vector';";
        let pgvector_schema: String = client
            .select(schema_query, None, &[])
            .unwrap_or_report()
            .first()
            .get_by_name("nspname")
            .expect("external build: cannot get schema of pgvector")
            .expect("external build: cannot get schema of pgvector");
        let dump_query =
            format!("SELECT id, parent, vector::{pgvector_schema}.vector FROM {table};");
        let centroids = client.select(&dump_query, None, &[]).unwrap_or_report();
        for row in centroids {
            let id: Option<i32> = row.get_by_name("id").unwrap();
            let parent: Option<i32> = row.get_by_name("parent").unwrap();
            let vector: Option<VectorOutput> = row.get_by_name("vector").unwrap();
            let id = id.expect("external build: id could not be NULL");
            let vector = vector.expect("external build: vector could not be NULL");
            let pop = parents.insert(id, parent);
            if pop.is_some() {
                pgrx::error!(
                    "external build: there are at least two lines have same id, id = {id}"
                );
            }
            if vector_options.dims != vector.as_borrowed().dims() {
                pgrx::error!("external build: incorrect dimension, id = {id}");
            }
            vectors.insert(id, rabitq::rotate::rotate(vector.as_borrowed().slice()));
        }
    });
    if parents.len() >= 2 && parents.values().all(|x| x.is_none()) {
        // if there are more than one vertex and no edges,
        // assume there is an implicit root
        let n = parents.len();
        let mut result = Vec::new();
        result.push(Structure {
            centroids: vectors.values().cloned().collect::<Vec<_>>(),
            children: vec![Vec::new(); n],
        });
        result.push(Structure {
            centroids: vec![{
                // compute the vector on root, without normalizing it
                let mut sum = vec![0.0f32; vector_options.dims as _];
                for vector in vectors.values() {
                    f32::vector_add_inplace(&mut sum, vector);
                }
                f32::vector_mul_scalar_inplace(&mut sum, 1.0 / n as f32);
                sum
            }],
            children: vec![(0..n as u32).collect()],
        });
        return result;
    }
    let mut children = parents
        .keys()
        .map(|x| (*x, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut root = None;
    for (&id, &parent) in parents.iter() {
        if let Some(parent) = parent {
            if let Some(parent) = children.get_mut(&parent) {
                parent.push(id);
            } else {
                pgrx::error!("external build: parent does not exist, id = {id}, parent = {parent}");
            }
        } else {
            if let Some(root) = root {
                pgrx::error!("external build: two root, id = {root}, id = {id}");
            } else {
                root = Some(id);
            }
        }
    }
    let Some(root) = root else {
        pgrx::error!("external build: there are no root");
    };
    let mut heights = BTreeMap::<_, _>::new();
    fn dfs_for_heights(
        heights: &mut BTreeMap<i32, Option<u32>>,
        children: &BTreeMap<i32, Vec<i32>>,
        u: i32,
    ) {
        if heights.contains_key(&u) {
            pgrx::error!("external build: detect a cycle, id = {u}");
        }
        heights.insert(u, None);
        let mut height = None;
        for &v in children[&u].iter() {
            dfs_for_heights(heights, children, v);
            let new = heights[&v].unwrap() + 1;
            if let Some(height) = height {
                if height != new {
                    pgrx::error!("external build: two heights, id = {u}");
                }
            } else {
                height = Some(new);
            }
        }
        if height.is_none() {
            height = Some(1);
        }
        heights.insert(u, height);
    }
    dfs_for_heights(&mut heights, &children, root);
    let heights = heights
        .into_iter()
        .map(|(k, v)| (k, v.expect("not a connected graph")))
        .collect::<BTreeMap<_, _>>();
    if !(1..=8).contains(&(heights[&root] - 1)) {
        pgrx::error!(
            "external build: unexpected tree height, height = {}",
            heights[&root]
        );
    }
    let mut cursors = vec![0_u32; 1 + heights[&root] as usize];
    let mut labels = BTreeMap::new();
    for id in parents.keys().copied() {
        let height = heights[&id];
        let cursor = cursors[height as usize];
        labels.insert(id, (height, cursor));
        cursors[height as usize] += 1;
    }
    fn extract(
        height: u32,
        labels: &BTreeMap<i32, (u32, u32)>,
        vectors: &BTreeMap<i32, Vec<f32>>,
        children: &BTreeMap<i32, Vec<i32>>,
    ) -> (Vec<Vec<f32>>, Vec<Vec<u32>>) {
        labels
            .iter()
            .filter(|(_, (h, _))| *h == height)
            .map(|(id, _)| {
                (
                    vectors[id].clone(),
                    children[id].iter().map(|id| labels[id].1).collect(),
                )
            })
            .unzip()
    }
    let mut result = Vec::new();
    for height in 1..=heights[&root] {
        let (centroids, children) = extract(height, &labels, &vectors, &children);
        result.push(Structure {
            centroids,
            children,
        });
    }
    result
}

pub trait InternalBuild: VectorOwned {
    fn build_to_vecf32(vector: Self::Borrowed<'_>) -> Vec<f32>;

    fn build_from_vecf32(x: &[f32]) -> Self;
}

impl InternalBuild for VectOwned<f32> {
    fn build_to_vecf32(vector: Self::Borrowed<'_>) -> Vec<f32> {
        vector.slice().to_vec()
    }

    fn build_from_vecf32(x: &[f32]) -> Self {
        Self::new(x.to_vec())
    }
}

impl InternalBuild for VectOwned<f16> {
    fn build_to_vecf32(vector: Self::Borrowed<'_>) -> Vec<f32> {
        f16::vector_to_f32(vector.slice())
    }

    fn build_from_vecf32(x: &[f32]) -> Self {
        Self::new(f16::vector_from_f32(x))
    }
}

struct CachingRelation<'a, R> {
    cache: vchordrq_cached::VchordrqCachedReader1<'a>,
    relation: R,
}

impl<R: Clone> Clone for CachingRelation<'_, R> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache,
            relation: self.relation.clone(),
        }
    }
}

enum CachingRelationReadGuard<'a, G: Deref> {
    Wrapping(G),
    Cached(u32, &'a G::Target),
}

impl<G: PageGuard + Deref> PageGuard for CachingRelationReadGuard<'_, G> {
    fn id(&self) -> u32 {
        match self {
            CachingRelationReadGuard::Wrapping(x) => x.id(),
            CachingRelationReadGuard::Cached(id, _) => *id,
        }
    }
}

impl<G: Deref> Deref for CachingRelationReadGuard<'_, G> {
    type Target = G::Target;

    fn deref(&self) -> &Self::Target {
        match self {
            CachingRelationReadGuard::Wrapping(x) => x,
            CachingRelationReadGuard::Cached(_, page) => page,
        }
    }
}

impl<R: Relation> Relation for CachingRelation<'_, R> {
    type Page = R::Page;
}

impl<R: RelationRead<Page = PostgresPage<vchordrq::Opaque>>> RelationReadTypes
    for CachingRelation<'_, R>
{
    type ReadGuard<'a> = CachingRelationReadGuard<'a, R::ReadGuard<'a>>;
}

impl<R: RelationRead<Page = PostgresPage<vchordrq::Opaque>>> RelationRead
    for CachingRelation<'_, R>
{
    fn read(&self, id: u32) -> Self::ReadGuard<'_> {
        if let Some(x) = self.cache.get(id) {
            CachingRelationReadGuard::Cached(id, x)
        } else {
            CachingRelationReadGuard::Wrapping(self.relation.read(id))
        }
    }
}

impl<R: RelationWrite<Page = PostgresPage<vchordrq::Opaque>>> RelationWriteTypes
    for CachingRelation<'_, R>
{
    type WriteGuard<'a> = R::WriteGuard<'a>;
}

impl<R: RelationWrite<Page = PostgresPage<vchordrq::Opaque>>> RelationWrite
    for CachingRelation<'_, R>
{
    fn write(&self, id: u32, tracking_freespace: bool) -> Self::WriteGuard<'_> {
        self.relation.write(id, tracking_freespace)
    }

    fn extend(
        &self,
        opaque: <Self::Page as Page>::Opaque,
        tracking_freespace: bool,
    ) -> Self::WriteGuard<'_> {
        self.relation.extend(opaque, tracking_freespace)
    }

    fn search(&self, freespace: usize) -> Option<Self::WriteGuard<'_>> {
        self.relation.search(freespace)
    }
}

// Add this structure to track cluster assignments
// NOTE: Defined earlier in the file; keep only one definition

// Add this function to collect cluster assignments during insertion
fn collect_cluster_assignments(
    structures: &[Structure<Vec<f32>>],
    heap: &Heap,
) -> Vec<VectorAssignment> {
    
    let mut assignments = Vec::new();
    let mut total_processed = 0u64;
    
    pgrx::info!("=== COLLECTING CLUSTER ASSIGNMENTS ===");
    
    heap.traverse(false, |(ctid, store)| {
        for (vector, extra) in store {
            let key = ctid_to_key(ctid);
            let doc_id = (key[0], key[1], key[2]);
            // Do NOT fetch id for all tokens; we fetch only for centroid representatives later
            let token_index = extra;
            
            // Convert to processing format
            let x = match vector {
                OwnedVector::Vecf32(x) => VectOwned::build_to_vecf32(x.as_borrowed()),
                OwnedVector::Vecf16(x) => VectOwned::build_to_vecf32(x.as_borrowed()),
            };
            
            // Find cluster assignment by traversing the hierarchy
            let mut cluster_path = Vec::new();
            let current_vec = &x;
            
            for structure in structures.iter() {
                let mut best_cluster = 0;
                let mut best_distance = f32::INFINITY;
                
                for (cluster_idx, centroid) in structure.centroids.iter().enumerate() {
                    let distance = f32::reduce_sum_of_d2(current_vec, centroid);
                    
                    if distance < best_distance {
                        best_distance = distance;
                        best_cluster = cluster_idx as u32;
                    }
                }
                
                cluster_path.push(best_cluster);
                
                // For next level, we'd use the centroid as reference
                // (simplified - actual implementation might differ)
            }
            
            let final_distance = if let Some(structure) = structures.last() {
                let final_cluster = cluster_path.last().copied().unwrap_or(0);
                f32::reduce_sum_of_d2(&x, &structure.centroids[final_cluster as usize])
            } else {
                0.0
            };
            assignments.push(VectorAssignment {
                doc_id,
                token_index,
                cluster_path,
                _distance: final_distance,
            });
        }
        total_processed += 1;
        
        if total_processed % 100000 == 0 {
            pgrx::info!("assignment: processed {} documents", total_processed);
        }
    });
    
    pgrx::info!("assignment: completed processing {} documents with {} total vectors", 
               total_processed, assignments.len());
    
    assignments
}

fn format_doc_hash(doc_id: (u16, u16, u16)) -> String {
    // Derive a stable 16-byte hex id from CTID using splitmix64-based mixing
    fn splitmix64(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E3779B97F4A7C15);
        let mut x = z;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
        x ^ (x >> 31)
    }
    let (hi, lo, pos) = doc_id;
    let seed = ((hi as u64) << 48) | ((lo as u64) << 32) | ((pos as u64) << 16) | 0x9E37u64;
    let h1 = splitmix64(seed);
    let h2 = splitmix64(seed ^ 0xDEADBEEFCAFEBABEu64);
    let mut s = String::with_capacity(32);
    for b in h1.to_be_bytes().into_iter().chain(h2.to_be_bytes()) {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{:02x}", b);
    }
    s
}

// Enhanced cluster distribution analysis with centroid representatives
fn analyze_and_report_cluster_distribution_with_centroids(
    assignments: &[VectorAssignment],
    structures: &[Structure<Vec<f32>>],
    centroid_representatives: &[Vec<CentroidRepresentative>],
    total_vectors: u64,
    heap_relid: pgrx::pg_sys::Oid,
    id_map: &HashMap<(u32, u16), (String, String)>,
) {
    use std::collections::HashMap;
    
    pgrx::info!("=== DETAILED CLUSTER DISTRIBUTION ANALYSIS WITH CENTROIDS ===");
    pgrx::info!("Total vectors analyzed: {}", assignments.len());
    pgrx::info!("Total documents processed: {}", total_vectors);
    pgrx::info!("Average vectors per document: {:.2}", assignments.len() as f64 / total_vectors as f64);
    
    for (level_idx, structure) in structures.iter().enumerate() {
        let mut cluster_stats: HashMap<u32, (u64, Vec<(u16, u16, u16, u16)>)> = HashMap::new();
        
        for assignment in assignments {
            if let Some(&cluster_id) = assignment.cluster_path.get(level_idx) {
                let entry = cluster_stats.entry(cluster_id).or_insert((0, Vec::new()));
                entry.0 += 1;
                
                if entry.1.len() < 5 {
                    entry.1.push((
                        assignment.doc_id.0,
                        assignment.doc_id.1, 
                        assignment.doc_id.2,
                        assignment.token_index
                    ));
                }
            }
        }
        
        pgrx::info!("--- Level {} Analysis ({} clusters) ---", level_idx, structure.len());
        
        // Build entries for all clusters (including those with 0 vectors), then sort by size desc
        let mut entries = Vec::with_capacity(structure.len());
        for cluster_id in 0..structure.len() as u32 {
            let (count, _sample_docs) = cluster_stats
                .get(&cluster_id)
                .cloned()
                .unwrap_or((0, Vec::new()));
            let percentage = (count as f64 / assignments.len() as f64) * 100.0;
            let centroid_rep = centroid_representatives
                .get(level_idx)
                .and_then(|level| level.get(cluster_id as usize));
            entries.push((cluster_id, count, percentage, centroid_rep));
        }
        entries.sort_by(|a, b| b.1.cmp(&a.1));

        for (cluster_id, count, percentage, centroid_rep) in entries {
            if let Some(rep) = centroid_rep {
                // Attempt to fetch id column from the heap table using CTID
                // First try to refresh stale CTID if we have valid ID info
                let refreshed_doc_id = if let Some(ref id_info) = rep.id_info {
                    refresh_stale_ctid(heap_relid, rep.doc_id, &Some(id_info.clone())).unwrap_or(rep.doc_id)
                } else {
                    rep.doc_id
                };
                
                let images_id_info = rep
                    .id_info
                    .clone()
                    .or_else(|| id_from_map(id_map, refreshed_doc_id));

                let verified_id = match images_id_info {
                    Some((col_name, id_value)) => {
                        // Verify the ID exists in the table
                        let is_real_id = unsafe {
                            pgrx::spi::Spi::connect(|client| {
                                let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
                                if rel.is_null() { return Ok(false); }
                                let nsp = (*(*rel).rd_rel).relnamespace;
                                let relname = pgrx::pg_sys::get_rel_name(heap_relid);
                                let nspname = pgrx::pg_sys::get_namespace_name(nsp);
                                let (schema, table) = if !relname.is_null() && !nspname.is_null() {
                                    (CStr::from_ptr(nspname).to_string_lossy().to_string(), CStr::from_ptr(relname).to_string_lossy().to_string())
                                } else {
                                    ("".to_string(), "".to_string())
                                };
                                pgrx::pg_sys::RelationClose(rel);
                                if schema.is_empty() || table.is_empty() { return Ok(false); }

                                let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                                let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

                                let sql = format!("SELECT 1 FROM {} WHERE {} = $1 LIMIT 1", qualified, quote_ident(&col_name));
                                client.select(&sql, Some(1), &[(&id_value).into()]).map(|t| t.len() > 0)
                            }).unwrap_or(false)
                        };

                        if is_real_id {
                            Some(id_value)
                        } else {
                            pgrx::warning!(
                                "Verification failed: Centroid representative's id '{}' (from column '{}') not found in table. CTID: ({},{},{}). Skipping this centroid.",
                                id_value, col_name, rep.doc_id.0, rep.doc_id.1, rep.doc_id.2
                            );
                            None
                        }
                    }
                    None => {
                        // Try to get more diagnostic information about the table
                        let table_info = unsafe {
                            pgrx::spi::Spi::connect(|client| {
                                let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
                                if rel.is_null() { return Ok("unknown".to_string()); }
                                let nsp = (*(*rel).rd_rel).relnamespace;
                                let relname = pgrx::pg_sys::get_rel_name(heap_relid);
                                let nspname = pgrx::pg_sys::get_namespace_name(nsp);
                                let (schema, table) = if !relname.is_null() && !nspname.is_null() {
                                    (CStr::from_ptr(nspname).to_string_lossy().to_string(), CStr::from_ptr(relname).to_string_lossy().to_string())
                                } else {
                                    ("unknown".to_string(), "unknown".to_string())
                                };
                                pgrx::pg_sys::RelationClose(rel);
                                
                                let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                                let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
                                
                                // Check if table is partitioned
                                let partition_check = "SELECT 1 FROM pg_catalog.pg_class WHERE oid = $1::regclass AND relispartition";
                                let is_partitioned = match client.select(partition_check, None, &[heap_relid.into()]) {
                                    Ok(rows) => rows.len() > 0,
                                    Err(_) => false,
                                };
                                
                                // Get table size info
                                let size_sql = "SELECT pg_size_pretty(pg_total_relation_size($1::regclass)) as size, pg_total_relation_size($1::regclass) as bytes";
                                let size_info = match client.select(size_sql, None, &[qualified.clone().into()]) {
                                    Ok(rows) if rows.len() > 0 => {
                                        if let Ok(Some(size)) = rows.first().get_by_name::<String, _>("size") {
                                            format!("size: {}", size)
                                        } else {
                                            "size: unknown".to_string()
                                        }
                                    },
                                    _ => "size: unknown".to_string(),
                                };
                                
                                Ok(format!("{}.{} (partitioned: {}, {})", schema, table, is_partitioned, size_info))
                            }).unwrap_or_else(|_: pgrx::spi::Error| "unknown".to_string())
                        };
                        
                        pgrx::warning!(
                            "Failed to fetch a real document ID for centroid representative. CTID: ({},{},{}).\n\
                            Table: {}\n\
                            This may indicate:\n\
                            1. The CTID refers to data that has been deleted or moved\n\
                            2. The table has been partitioned and the CTID refers to a partition that no longer exists\n\
                            3. The table has been reorganized (VACUUM, REINDEX, etc.) and the CTID is stale\n\
                            4. Data corruption in the index build process\n\
                            Skipping this centroid and continuing with analysis.",
                            rep.doc_id.0, rep.doc_id.1, rep.doc_id.2, table_info
                        );
                        None
                    }
                };

                // Only process centroids with valid IDs
                if let Some(id_value) = verified_id {
                    pgrx::info!(
                        "  Cluster L{lvl}_C{cid}: {cnt} vectors ({pct:.2}%) - Centroid rep: doc(ctid_bi_hi={hi}, ctid_bi_lo={lo}, ip_posid={pos}, token_id={tok}) aka doc({hi},{lo},{pos})_token{tok} (id: {img_id}, dist: {dist:.4})",
                        lvl = level_idx,
                        cid = cluster_id,
                        cnt = count,
                        pct = percentage,
                        hi = rep.doc_id.0,
                        lo = rep.doc_id.1,
                        pos = rep.doc_id.2,
                        tok = rep.token_index,
                        img_id = id_value,
                        dist = rep.distance_to_centroid
                    );
                } else {
                    pgrx::info!("  Cluster L{lvl}_C{cid}: {cnt} vectors ({pct:.2}%) - Skipped due to stale CTID", 
                        lvl = level_idx,
                        cid = cluster_id,
                        cnt = count,
                        pct = percentage
                    );
                }
            } else {
                pgrx::info!(
                    "  Cluster L{lvl}_C{cid}: {cnt} vectors ({pct:.2}%) - No centroid rep available",
                    lvl = level_idx,
                    cid = cluster_id,
                    cnt = count,
                    pct = percentage
                );
            }
        }
        
        // Show statistics
        let total_clusters_used = cluster_stats.len();
        let avg_vectors_per_cluster = assignments.len() as f64 / total_clusters_used as f64;
        let max_vectors = cluster_stats.values().map(|(count, _)| *count).max().unwrap_or(0);
        let min_vectors = cluster_stats.values().map(|(count, _)| *count).min().unwrap_or(0);
        
        pgrx::info!("  Level {} Summary:", level_idx);
        pgrx::info!("    - Clusters with vectors: {}/{}", total_clusters_used, structure.len());
        pgrx::info!("    - Avg vectors per cluster: {:.1}", avg_vectors_per_cluster);
        pgrx::info!("    - Max vectors in cluster: {}", max_vectors);
        pgrx::info!("    - Min vectors in cluster: {}", min_vectors);
    }
    
    // End of cluster distribution analysis (removed Multi-Vector Document Analysis per request)
    pgrx::info!("=== END CLUSTER DISTRIBUTION ANALYSIS ===");
}

fn fetch_heap_id_by_ctid(heap_relid: pgrx::pg_sys::Oid, doc_id: (u16, u16, u16)) -> Option<(String, String)> {
    pgrx::info!("DEBUG: Entering fetch_heap_id_by_ctid");
    // Resolve schema-qualified table name from relid
    unsafe {
        let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
        if rel.is_null() { return None; }
        let nsp = (*(*rel).rd_rel).relnamespace;
        let relname = pgrx::pg_sys::get_rel_name(heap_relid);
        let nspname = pgrx::pg_sys::get_namespace_name(nsp);
        let is_partition = (*(*rel).rd_rel).relispartition;
        let is_partitioned_parent = (*(*rel).rd_rel).relkind == pgrx::pg_sys::RELKIND_PARTITIONED_TABLE as i8;
        if relname.is_null() || nspname.is_null() {
            pgrx::pg_sys::RelationClose(rel);
            return None;
        }
        let schema = CStr::from_ptr(nspname).to_string_lossy().to_string();
        let table = CStr::from_ptr(relname).to_string_lossy().to_string();
        pgrx::pg_sys::RelationClose(rel);

        let block: u32 = ((doc_id.0 as u32) << 16) | (doc_id.1 as u32);
        let posid: u16 = doc_id.2;
        let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

        let mut value: Option<(String, String)> = None;
        let _ = pgrx::spi::Spi::connect(|client| {
            // Build list of tables to probe for CTID matches
            let tables_to_query: Vec<String> = if is_partitioned_parent {
                get_table_partitions(heap_relid, client)
            } else {
                vec![qualified.clone()]
            };

            let check_sql = "SELECT 1 FROM pg_catalog.pg_attribute\n             WHERE attrelid = $1::regclass AND attname = 'id' AND NOT attisdropped\n             LIMIT 1";
            let has_id = match client.select(check_sql, None, &[heap_relid.into()]) {
                Ok(rows) => {
                    pgrx::info!("id-lookup: check for 'id' column returned {} rows", rows.len());
                    rows.len() > 0
                },
                Err(e) => {
                    pgrx::warning!("id-lookup: check for 'id' column failed: {}", e);
                    false
                },
            };
            // Use a single text parameter and cast to tid: $1::tid
            let tid_param = format!("({}, {})", block, posid);
            if has_id {
                pgrx::info!("id-lookup: 'id' column found, querying with ctid...");
                // Probe all candidate tables (parent or partitions)
                for tname in &tables_to_query {
                    // Try with ONLY first (narrow scan)
                    let sql_only = format!(
                        "SELECT id::text AS id FROM ONLY {} WHERE ctid = $1::tid LIMIT 1",
                        tname
                    );
                    match client.select(&sql_only, None, &[tid_param.clone().into()]) {
                        Ok(rows) if rows.len() > 0 => {
                            if let Ok(Some(id)) = rows.first().get_by_name::<String, _>("id") {
                                pgrx::info!("id-lookup: success using 'id' column (ONLY) on {}. Found id: {}", tname, id);
                                value = Some(("id".to_string(), id));
                                return Ok::<_, pgrx::spi::Error>(());
                            }
                        }
                        Ok(_) => {
                            // If ONLY fails, try without ONLY
                            let sql_all = format!(
                                "SELECT id::text AS id FROM {} WHERE ctid = $1::tid LIMIT 1",
                                tname
                            );
                            match client.select(&sql_all, None, &[tid_param.clone().into()]) {
                                Ok(rows) if rows.len() > 0 => {
                                    if let Ok(Some(id)) = rows.first().get_by_name::<String, _>("id") {
                                        pgrx::info!("id-lookup: success using 'id' column (no ONLY) on {}. Found id: {}", tname, id);
                                        value = Some(("id".to_string(), id));
                                        return Ok::<_, pgrx::spi::Error>(());
                                    }
                                }
                                Ok(_) => {
                                    // keep trying other partitions
                                },
                                Err(e) => pgrx::warning!("id-lookup: query for 'id' with ctid failed on {}: {}", tname, e),
                            }
                        }
                        Err(e) => pgrx::warning!("id-lookup: query for 'id' with ctid failed (ONLY) on {}: {}", tname, e),
                    }
                }
                // If we reach here, not found in any candidate table
                if is_partitioned_parent {
                    pgrx::warning!("id-lookup: query for 'id' with ctid returned 0 rows on all partitions - CTID ({}, {}) may be stale or refer to deleted data", block, posid);
                } else {
                    pgrx::warning!("id-lookup: query for 'id' with ctid returned 0 rows - CTID ({}, {}) may be stale or refer to deleted data", block, posid);
                }
            } else {
                pgrx::info!("id-lookup: 'id' column not found. Checking for primary key...");
            }
            if value.is_some() { return Ok::<_, pgrx::spi::Error>(()); }

            // 2. Try with Primary Key
            let pk_sql = "SELECT a.attname AS colname\n               FROM pg_index i\n               JOIN pg_attribute a\n                 ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)\n              WHERE i.indrelid = $1::regclass AND i.indisprimary\n                AND a.attnum > 0 AND NOT a.attisdropped\n              ORDER BY array_position(i.indkey, a.attnum) ASC\n              LIMIT 1";
            match client.select(pk_sql, None, &[heap_relid.into()]) {
                Ok(pk_rows) if pk_rows.len() > 0 => {
                    if let Some(pk_col_name) = pk_rows.first().get_by_name::<String, _>("colname").unwrap_or(None) {
                        pgrx::info!("id-lookup: found primary key column: '{}'. Querying with ctid...", pk_col_name);
                        let col_quoted = quote_ident(&pk_col_name);
                        // Probe all candidate tables (parent or partitions)
                        for tname in &tables_to_query {
                            let sql_only = format!(
                                "SELECT ({}::text) AS id FROM ONLY {} WHERE ctid = $1::tid LIMIT 1",
                                col_quoted, tname
                            );
                            match client.select(&sql_only, None, &[tid_param.clone().into()]) {
                                Ok(rows) if rows.len() > 0 => {
                                    if let Ok(Some(id)) = rows.first().get_by_name::<String, _>("id") {
                                        pgrx::info!("id-lookup: success using PK column '{}' (ONLY) on {}. Found id: {}", pk_col_name, tname, id);
                                        value = Some((pk_col_name.clone(), id));
                                        return Ok::<_, pgrx::spi::Error>(());
                                    }
                                }
                                Ok(_) => {
                                    let sql_all = format!(
                                        "SELECT ({}::text) AS id FROM {} WHERE ctid = $1::tid LIMIT 1",
                                        col_quoted, tname
                                    );
                                    match client.select(&sql_all, None, &[tid_param.clone().into()]) {
                                        Ok(rows) if rows.len() > 0 => {
                                            if let Ok(Some(id)) = rows.first().get_by_name::<String, _>("id") {
                                                pgrx::info!("id-lookup: success using PK column '{}' (no ONLY) on {}. Found id: {}", pk_col_name, tname, id);
                                                value = Some((pk_col_name.clone(), id));
                                                return Ok::<_, pgrx::spi::Error>(());
                                            }
                                        }
                                        Ok(_) => {
                                            // continue with next partition
                                        },
                                        Err(e) => pgrx::warning!("id-lookup: query for PK '{}' with ctid failed on {}: {}", pk_col_name, tname, e),
                                    }
                                }
                                Err(e) => pgrx::warning!("id-lookup: query for PK '{}' with ctid failed (ONLY) on {}: {}", pk_col_name, tname, e),
                            }
                        }
                        if is_partitioned_parent {
                            pgrx::warning!("id-lookup: query for PK '{}' with ctid returned 0 rows on all partitions - CTID ({}, {}) may be stale or refer to deleted data", pk_col_name, block, posid);
                        } else {
                            pgrx::warning!("id-lookup: query for PK '{}' with ctid returned 0 rows - CTID ({}, {}) may be stale or refer to deleted data", pk_col_name, block, posid);
                        }
                    }
                }
                Ok(_) => pgrx::info!("id-lookup: no primary key found for table."),
                Err(e) => pgrx::warning!("id-lookup: primary key lookup failed: {}", e),
            }
            Ok::<_, pgrx::spi::Error>(())
        });
        return value;
    }
}

// Add this function after the fetch_heap_id_by_ctid function
fn refresh_stale_ctid(
    heap_relid: pgrx::pg_sys::Oid,
    stale_ctid: (u16, u16, u16),
    id_info: &Option<(String, String)>,
) -> Option<(u16, u16, u16)> {
    // If we have valid ID info, try to find the current CTID for this ID
    if let Some((_, id_value)) = id_info {
        unsafe {
            let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
            if rel.is_null() { return None; }
            let nsp = (*(*rel).rd_rel).relnamespace;
            let relname = pgrx::pg_sys::get_rel_name(heap_relid);
            let nspname = pgrx::pg_sys::get_namespace_name(nsp);
            if relname.is_null() || nspname.is_null() {
                pgrx::pg_sys::RelationClose(rel);
                return None;
            }
            let schema = CStr::from_ptr(nspname).to_string_lossy().to_string();
            let table = CStr::from_ptr(relname).to_string_lossy().to_string();
            pgrx::pg_sys::RelationClose(rel);

            let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
            let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

            let mut refreshed_ctid = None;
            let _ = pgrx::spi::Spi::connect(|client| {
                // Try to find current CTID for this ID
                let sql = format!(
                    "SELECT ctid::text AS tid FROM {} WHERE id::text = $1 LIMIT 1",
                    qualified
                );
                match client.select(&sql, None, &[id_value.into()]) {
                    Ok(rows) if rows.len() > 0 => {
                        if let Ok(Some(tid_txt)) = rows.first().get_by_name::<String, _>("tid") {
                            if let Some((blk, off)) = parse_tid_text(&tid_txt) {
                                let bi_hi = (blk >> 16) as u16;
                                let bi_lo = (blk & 0xFFFF) as u16;
                                refreshed_ctid = Some((bi_hi, bi_lo, off));
                                pgrx::info!("ctid-refresh: refreshed stale CTID ({}, {}, {}) to ({}, {}, {}) for id: {}", 
                                    stale_ctid.0, stale_ctid.1, stale_ctid.2, bi_hi, bi_lo, off, id_value);
                            }
                        }
                    }
                    Ok(_) => {
                        pgrx::warning!("ctid-refresh: could not find current CTID for id: {}", id_value);
                    }
                    Err(e) => {
                        pgrx::warning!("ctid-refresh: failed to query for current CTID: {}", e);
                    }
                }
                Ok::<_, pgrx::spi::Error>(())
            });
            refreshed_ctid
        }
    } else {
        None
    }
}



// Add this function after the refresh_stale_ctid function
fn batch_refresh_ctids(
    heap_relid: pgrx::pg_sys::Oid,
    ctids_with_ids: &[(u16, u16, u16, Option<(String, String)>)],
) -> Vec<(u16, u16, u16)> {
    let mut refreshed_ctids = Vec::new();
    
    // Group CTIDs by ID to batch the queries
    let mut id_to_ctids: HashMap<String, Vec<(u16, u16, u16)>> = HashMap::new();
    for (bi_hi, bi_lo, ip_posid, id_info) in ctids_with_ids {
        if let Some((id_value, _)) = id_info {
            id_to_ctids.entry(id_value.clone()).or_default().push((*bi_hi, *bi_lo, *ip_posid));
        }
    }
    
    if id_to_ctids.is_empty() {
        // No valid IDs to refresh, return original CTIDs
        return ctids_with_ids.iter().map(|(bi_hi, bi_lo, ip_posid, _)| (*bi_hi, *bi_lo, *ip_posid)).collect();
    }
    
    unsafe {
        let rel = pgrx::pg_sys::RelationIdGetRelation(heap_relid);
        if rel.is_null() { 
            return ctids_with_ids.iter().map(|(bi_hi, bi_lo, ip_posid, _)| (*bi_hi, *bi_lo, *ip_posid)).collect();
        }
        let nsp = (*(*rel).rd_rel).relnamespace;
        let relname = pgrx::pg_sys::get_rel_name(heap_relid);
        let nspname = pgrx::pg_sys::get_namespace_name(nsp);
        if relname.is_null() || nspname.is_null() {
            pgrx::pg_sys::RelationClose(rel);
            return ctids_with_ids.iter().map(|(bi_hi, bi_lo, ip_posid, _)| (*bi_hi, *bi_lo, *ip_posid)).collect();
        }
        let schema = CStr::from_ptr(nspname).to_string_lossy().to_string();
        let table = CStr::from_ptr(relname).to_string_lossy().to_string();
        pgrx::pg_sys::RelationClose(rel);

        let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

        let _ = pgrx::spi::Spi::connect(|client| {
            // Batch query to get current CTIDs for all IDs
            let id_values: Vec<&str> = id_to_ctids.keys().map(|s| s.as_str()).collect();
            let placeholders: Vec<String> = (1..=id_values.len()).map(|i| format!("${}", i)).collect();
            let sql = format!(
                "SELECT id::text AS id, ctid::text AS tid FROM {} WHERE id::text IN ({})",
                qualified,
                placeholders.join(",")
            );
            
            match client.select(&sql, None, &id_values.iter().map(|&s| s.into()).collect::<Vec<_>>()) {
                Ok(rows) => {
                    let mut id_to_current_ctid: HashMap<String, (u16, u16, u16)> = HashMap::new();
                    for row in rows {
                        if let (Some(id_txt), Some(tid_txt)) = (row.get_by_name::<String, _>("id").unwrap_or(None), row.get_by_name::<String, _>("tid").unwrap_or(None)) {
                            if let Some((blk, off)) = parse_tid_text(&tid_txt) {
                                let bi_hi = (blk >> 16) as u16;
                                let bi_lo = (blk & 0xFFFF) as u16;
                                id_to_current_ctid.insert(id_txt, (bi_hi, bi_lo, off));
                            }
                        }
                    }
                    
                    // Map back to original order with refreshed CTIDs
                    for (bi_hi, bi_lo, ip_posid, id_info) in ctids_with_ids {
                        if let Some((id_value, _)) = id_info {
                            if let Some(&refreshed_ctid) = id_to_current_ctid.get(id_value) {
                                if refreshed_ctid != (*bi_hi, *bi_lo, *ip_posid) {
                                    pgrx::info!("ctid-batch-refresh: refreshed CTID ({}, {}, {}) to ({}, {}, {}) for id: {}", 
                                        bi_hi, bi_lo, ip_posid, refreshed_ctid.0, refreshed_ctid.1, refreshed_ctid.2, id_value);
                                }
                                refreshed_ctids.push(refreshed_ctid);
                            } else {
                                // Keep original CTID if refresh failed
                                refreshed_ctids.push((*bi_hi, *bi_lo, *ip_posid));
                            }
                        } else {
                            // Keep original CTID if no ID info
                            refreshed_ctids.push((*bi_hi, *bi_lo, *ip_posid));
                        }
                    }
                }
                Err(e) => {
                    pgrx::warning!("ctid-batch-refresh: failed to batch refresh CTIDs: {}", e);
                    // Fall back to original CTIDs
                    refreshed_ctids = ctids_with_ids.iter().map(|(bi_hi, bi_lo, ip_posid, _)| (*bi_hi, *bi_lo, *ip_posid)).collect();
                }
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    }
    
    refreshed_ctids
}

