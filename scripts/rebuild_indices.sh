#!/usr/bin/env bash
# Script to rebuild VectorChord indices after extension installation

set -euo pipefail

# Configuration variables (override via environment variables)
CONTAINER_NAME="${CONTAINER_NAME:-vchord_listings}"
DB_NAME="${DB_NAME:-listings_db}"
DB_USER="${DB_USER:-listings_user}"
DB_PASSWORD="${DB_PASSWORD:-listings_somebi_731}"
DB_PORT="${DB_PORT:-5432}"
PG_PORT_IN_CONTAINER=5432
AUTO_REINDEX="${AUTO_REINDEX:-false}"

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    local container="$1"
    echo "Waiting for PostgreSQL to be ready in container: $container"
    
    local ready=false
    for i in {1..60}; do
        if docker exec "$container" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 1
    done
    
    if [ "$ready" != true ]; then
        echo "PostgreSQL did not become ready in time." >&2
        return 1
    fi
    
    echo "PostgreSQL is ready"
    return 0
}

# Function to check if we can connect to the database
check_db_connection() {
    local container="$1"
    
    if ! docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
        echo "Warning: unable to connect to Postgres as user '$DB_USER'. Cannot proceed with index operations." >&2
        return 1
    fi
    
    return 0
}

# Function to get current extension version
get_extension_version() {
    local container="$1"
    
    docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || echo ""
}

# Function to recreate VectorChord indexes
recreate_indexes() {
    local container="$1"
    local current_version="$2"
    local new_version="$3"
    
    echo "Auto-recreating VectorChord indexes (version changed from ${current_version:-none} to ${new_version})..."
    echo "Using DROP + CREATE INDEX approach to avoid stale CTID issues"
    
    # Get list of VectorChord indexes to recreate
    local indexes
    indexes=$(docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -At -c \
        "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname)
           FROM pg_index i
           JOIN pg_class c ON i.indexrelid = c.oid
           JOIN pg_namespace n ON c.relnamespace = n.oid
           JOIN pg_am am ON c.relam = am.oid
          WHERE am.amname IN ('vchordrq','vchordg')
          ORDER BY c.relname;")
    
    if [ -n "$indexes" ]; then
        for idx_name in $indexes; do
            echo "Recreating index: $idx_name"
            
            # Get the complete index definition
            local idx_def
            idx_def=$(docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -At -c \
                "SELECT pg_get_indexdef(i.indexrelid)
                   FROM pg_index i
                   JOIN pg_class c ON i.indexrelid = c.oid
                   JOIN pg_namespace n ON c.relnamespace = n.oid
                  WHERE quote_ident(n.nspname)||'.'||quote_ident(c.relname) = '$idx_name';")
            
            if [ -n "$idx_def" ]; then
                echo "  Dropping: $idx_name"
                docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "DROP INDEX IF EXISTS $idx_name;" || {
                    echo "Warning: drop failed for $idx_name; continuing" >&2
                }
                echo "  Creating: $idx_name"
                docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "$idx_def;" || {
                    echo "Warning: create failed for $idx_name; continuing" >&2
                }
            else
                echo "Warning: could not get definition for $idx_name; skipping" >&2
            fi
        done
    else
        echo "No VectorChord indexes found to recreate."
    fi
}

# Function to create optimized VectorChord index
create_optimized_index() {
    local container="$1"
    
    echo "Creating optimized VectorChord index..."
    
    local index_ddl="DROP INDEX IF EXISTS images_multi_embedding_maxsim_idx_optimized; CREATE INDEX images_multi_embedding_maxsim_idx_optimized ON images USING vchordrq (multi_embedding vector_maxsim_ops) WITH (options = \$\$residual_quantization = true
build.pin = true
rerank_in_table = false
[build.internal]
kmeans_iterations = 50
sampling_factor = 512
lists = [16, 128, 512]
spherical_centroids = true
build_threads = 32\$\$);"
    
    echo "Index DDL content:"
    echo "$index_ddl"
    echo ""
    
    if docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "$index_ddl"; then
        echo "Optimized VectorChord index created successfully."
    else
        echo "Warning: Index creation failed." >&2
        return 1
    fi
}

# Function to verify extension functionality
verify_extension() {
    local container="$1"
    
    echo "Verifying extension functionality..."
    
    # Test loading the extension
    if docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA -c "LOAD 'vchord'; SELECT 1;" >/dev/null 2>&1; then
        echo "Extension load test: ok"
    else
        echo "Extension load test: failed" >&2
        return 1
    fi
    
    # Test calling a function from the extension
    if docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA \
        -c "LOAD 'vchord'; SELECT \"_vchordg_support_vector_ip_ops\"();" >/dev/null 2>&1; then
        echo "Extension function call test: ok"
    else
        echo "Extension function call test: failed" >&2
        return 1
    fi
    
    return 0
}

# Main execution
main() {
    echo "VectorChord Index Rebuild Script"
    echo "================================"
    echo "Configuration:"
    echo "  CONTAINER_NAME: $CONTAINER_NAME"
    echo "  DB_NAME: $DB_NAME"
    echo "  DB_USER: $DB_USER"
    echo "  AUTO_REINDEX: $AUTO_REINDEX"
    echo ""
    
    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Error: required container '${CONTAINER_NAME}' is not running." >&2
        exit 1
    fi
    
    # Wait for PostgreSQL to be ready
    if ! wait_for_postgres "$CONTAINER_NAME"; then
        exit 1
    fi
    
    # Check database connection
    if ! check_db_connection "$CONTAINER_NAME"; then
        exit 1
    fi
    
    # Get current extension version
    local current_version
    current_version=$(get_extension_version "$CONTAINER_NAME")
    echo "Current vchord extension version: ${current_version:-not installed}"
    
    # Verify extension functionality
    if ! verify_extension "$CONTAINER_NAME"; then
        echo "Error: Extension verification failed" >&2
        exit 1
    fi
    
    # Optionally recreate indexes if AUTO_REINDEX is enabled
    if [ "$AUTO_REINDEX" = true ]; then
        # For now, we'll just create the optimized index
        # In a real scenario, you might want to check if version changed
        echo "AUTO_REINDEX enabled - proceeding with index operations..."
        
        if ! create_optimized_index "$CONTAINER_NAME"; then
            echo "Warning: Failed to create optimized index" >&2
        fi
    else
        echo "AUTO_REINDEX disabled - skipping index operations"
    fi
    
    echo "Index rebuild script completed successfully!"
}

# Run main function
main "$@"
