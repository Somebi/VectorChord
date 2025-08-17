#!/usr/bin/env bash
# Incremental build script for VectorChord with optimized caching

set -euo pipefail

# Configuration
BUILD_CONTAINER="${BUILD_CONTAINER:-vchord_build_persistent}"
BUILDER_IMAGE="${BUILDER_IMAGE:-vchord-builder:latest}"
CLEAN_BUILD="${CLEAN_BUILD:-false}"
FORCE_REBUILD="${FORCE_REBUILD:-false}"
USE_INCREMENTAL="${USE_INCREMENTAL:-true}"
PERSISTENT_BUILDER="${PERSISTENT_BUILDER:-true}"

# Function to check if dependencies have changed
check_dependencies_changed() {
    local deps_hash_file=".deps_hash"
    local current_hash=$(find crates/ Cargo.toml Cargo.lock -type f -exec sha256sum {} \; | sort | sha256sum)
    
    if [ -f "$deps_hash_file" ] && [ "$(cat "$deps_hash_file")" = "$current_hash" ]; then
        echo "Dependencies unchanged, using incremental build"
        return 1  # false - no change
    else
        echo "Dependencies changed, full rebuild needed"
        echo "$current_hash" > "$deps_hash_file"
        return 0  # true - changed
    fi
}

# Function to setup build container
setup_build_container() {
    if [ "$PERSISTENT_BUILDER" = true ]; then
        # Check if build container exists, create only if needed
        if ! docker ps -a --format '{{.Names}}' | grep -q "^${BUILD_CONTAINER}$"; then
            echo "Creating persistent build container: $BUILD_CONTAINER"
            docker run -d \
                --name "$BUILD_CONTAINER" \
                -v "$(pwd):/build" \
                -v cargo_cache:/home/builder/.cargo \
                -v target_cache:/build/target \
                -w /build \
                "$BUILDER_IMAGE" \
                tail -f /dev/null
        else
            echo "Using existing build container: $BUILD_CONTAINER"
            # Ensure container is running
            if ! docker ps --format '{{.Names}}' | grep -q "^${BUILD_CONTAINER}$"; then
                docker start "$BUILD_CONTAINER"
            fi
        fi
    else
        # Create temporary build container
        BUILD_CONTAINER="vchord_build_$$"
        echo "Creating temporary build container: $BUILD_CONTAINER"
        
        docker run -d \
            --name "$BUILD_CONTAINER" \
            -v "$(pwd):/build" \
            -w /build \
            "$BUILDER_IMAGE" \
            tail -f /dev/null
    fi
    
    # Wait for container to be ready
    sleep 2
}

# Function to cleanup build container
cleanup_build_container() {
    if [ "$PERSISTENT_BUILDER" = false ]; then
        echo "Cleaning up temporary build container..."
        docker rm -f "$BUILD_CONTAINER" >/dev/null 2>&1 || true
    else
        echo "Keeping persistent build container for future builds"
    fi
}

# Main build function
build_extension() {
    echo "Starting VectorChord extension build..."
    
    # Check if we need a full rebuild
    if [ "$CLEAN_BUILD" = true ] || [ "$FORCE_REBUILD" = true ] || check_dependencies_changed; then
        echo "Performing clean build..."
        docker exec "$BUILD_CONTAINER" bash -c "rm -rf target/ && mkdir -p target/"
        docker exec --user root "$BUILD_CONTAINER" bash -c "chown -R builder:builder /build/target"
    else
        echo "Using incremental build (keeping existing target directory)"
        # Ensure target directory has correct permissions
        docker exec --user root "$BUILD_CONTAINER" bash -c "chown -R builder:builder /build/target 2>/dev/null || true"
    fi
    
    # Build with optimized settings
    if docker exec "$BUILD_CONTAINER" bash -c "
        set -euo pipefail
        export PATH=\"/home/builder/.cargo/bin:\$PATH\"
        export CARGO_HOME=/home/builder/.cargo
        export PGRX_PG_CONFIG_PATH=pg_config
        
        # Use incremental compilation for faster rebuilds
        export CARGO_INCREMENTAL=1
        export CARGO_NET_GIT_FETCH_WITH_CLI=true
        
        # Build the make crate first
        echo 'Building make crate...'
        cargo build -p make --profile release
        
        # Build the main extension
        echo 'Building vchord extension...'
        ./target/release/make build --output ./build/raw --profile release --jobs \$(nproc)
    "; then
        echo "Extension built successfully!"
        return 0
    else
        echo "Error: Build failed!" >&2
        return 1
    fi
}

# Main execution
main() {
    echo "VectorChord Incremental Build Script"
    echo "==================================="
    echo "Configuration:"
    echo "  CLEAN_BUILD: $CLEAN_BUILD"
    echo "  FORCE_REBUILD: $FORCE_REBUILD"
    echo "  USE_INCREMENTAL: $USE_INCREMENTAL"
    echo "  PERSISTENT_BUILDER: $PERSISTENT_BUILDER"
    echo "  BUILD_CONTAINER: $BUILD_CONTAINER"
    echo ""
    
    # Check if builder image exists
    if ! docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${BUILDER_IMAGE}$"; then
        echo "Error: Builder image $BUILDER_IMAGE not found. Please run ./scripts/build_builder_image.sh first." >&2
        exit 1
    fi
    
    # Setup build container
    setup_build_container
    
    # Build the extension
    if build_extension; then
        echo "Build completed successfully!"
        cleanup_build_container
        exit 0
    else
        echo "Build failed!"
        cleanup_build_container
        exit 1
    fi
}

# Run main function
main "$@"
