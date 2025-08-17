# VectorChord Builder Docker Image

This directory contains a Docker image for building the VectorChord PostgreSQL extension with compatible glibc 2.36.

## Files

- `Dockerfile.build` - Build container definition with Rust toolchain and PostgreSQL dev packages
- `../scripts/build_builder_image.sh` - Script to build the builder image

## Usage

### 1. Build the builder image

```bash
# From the VectorChord repository root
./scripts/build_builder_image.sh
```

This creates a `vchord-builder:latest` image with:
- Base: `ghcr.io/tensorchord/vchord-postgres:pg17-v0.5.0` (PostgreSQL 17 + glibc 2.36)
- Rust toolchain
- Build tools (build-essential, git, etc.)
- All build dependencies

### 2. Use in install_extension.sh

The existing `scripts/install_extension.sh` script has been updated to automatically use this builder image when `BUILD_IN_CONTAINER=true` is set.

```bash
# Set the flag to use containerized build
export BUILD_IN_CONTAINER=true

# Run the install script as usual
./scripts/install_extension.sh
```

## How it works

1. When `BUILD_IN_CONTAINER=true`, the install script checks if the builder image exists
2. If not found, it automatically builds it using `build_builder_image.sh`
3. Creates a temporary build container with source code mounted
4. Builds the extension inside the container with compatible glibc
5. Copies the built artifacts to the PostgreSQL container
6. Cleans up the temporary build container

## Benefits

- **GLIBC Compatibility**: Built extension works with target PostgreSQL containers
- **Clean Environment**: No host system dependencies required
- **Automatic**: Integrated into existing install workflow
- **Efficient**: Reuses builder image across builds
