#!/usr/bin/env bash
# Clean build script for VectorChord - always performs a fresh build

set -euo pipefail

# Configuration
BUILD_CONTAINER="${BUILD_CONTAINER:-vchord_build_clean}"
BUILDER_IMAGE="${BUILDER_IMAGE:-vchord-builder:latest}"
PERSISTENT_BUILDER="${PERSISTENT_BUILDER:-false}"

# Function to setup build container
setup_build_container() {
    # Always create a fresh container for clean builds
    BUILD_CONTAINER="vchord_build_clean_$$"
    echo "Creating clean build container: $BUILD_CONTAINER"
    
    docker run -d \
        --name "$BUILD_CONTAINER" \
        -v "$(pwd):/build" \
        -w /build \
        "$BUILDER_IMAGE" \
        tail -f /dev/null
    
    # Wait for container to be ready
    sleep 2
}

# Function to cleanup build container
cleanup_build_container() {
    echo "Cleaning up clean build container..."
    docker rm -f "$BUILD_CONTAINER" >/dev/null 2>&1 || true
}

# Main build function
build_extension() {
    echo "Starting VectorChord clean build..."
    
    # Always perform clean build
    echo "Performing clean build..."
    docker exec "$BUILD_CONTAINER" bash -c "rm -rf target/ && mkdir -p target/"
    docker exec --user root "$BUILD_CONTAINER" bash -c "chown -R builder:builder /build/target"
    
    # Build with optimized settings
    if docker exec "$BUILD_CONTAINER" bash -c "
        set -euo pipefail
        export PATH=\"/home/builder/.cargo/bin:\$PATH\"
        export CARGO_HOME=/home/builder/.cargo
        export PGRX_PG_CONFIG_PATH=pg_config
        
        # Disable incremental compilation for clean builds
        export CARGO_INCREMENTAL=0
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
    echo "VectorChord Clean Build Script"
    echo "============================="
    echo "Configuration:"
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
        echo "Clean build completed successfully!"
        cleanup_build_container
        exit 0
    else
        echo "Clean build failed!"
        cleanup_build_container
        exit 1
    fi
}

# Run main function
main "$@"
