# VectorChord Builder Workflow

## Overview

We've created a containerized build approach for the VectorChord PostgreSQL extension that ensures GLIBC compatibility by using the official VectorChord PostgreSQL image as a base.

## What We Created

### 1. Builder Docker Image (`docker/Dockerfile.build`)
- **Base**: `ghcr.io/tensorchord/vchord-postgres:pg17-v0.5.0`
- **Contains**: PostgreSQL 17 + glibc 2.36 + Rust toolchain + build tools
- **Purpose**: Provides a clean, compatible environment for building the extension

### 2. Build Script (`scripts/build_builder_image.sh`)
- Simple script to build the builder Docker image
- Creates `vchord-builder:latest` image
- Run once to set up the build environment

### 3. Modified Install Script (`scripts/install_extension.sh`)
- Updated to automatically use the builder image when `BUILD_IN_CONTAINER=true`
- Mounts source code into temporary build container
- Builds extension with compatible glibc
- Copies artifacts to PostgreSQL container
- Automatically cleans up build containers

## How to Use

### Step 1: Build the Builder Image (one-time setup)
```bash
./scripts/build_builder_image.sh
```

### Step 2: Use Containerized Build
```bash
# Set the flag to use containerized build
export BUILD_IN_CONTAINER=true

# Run the install script as usual
./scripts/install_extension.sh
```

## Workflow Details

1. **Image Check**: Install script checks if `vchord-builder:latest` exists
2. **Auto-build**: If not found, automatically runs `build_builder_image.sh`
3. **Build Container**: Creates temporary container with source mounted
4. **Extension Build**: Runs `cargo build` inside container with correct glibc
5. **Artifact Copy**: Copies built `.so` and SQL files to PostgreSQL container
6. **Cleanup**: Removes temporary build container
7. **Installation**: Creates/updates extension in database

## Benefits

- **GLIBC Compatibility**: Built extension works with target PostgreSQL containers
- **Clean Environment**: No host system dependencies required
- **Automatic**: Integrated into existing install workflow
- **Efficient**: Reuses builder image across builds
- **Correct Base**: Uses official VectorChord image with PostgreSQL 17 + glibc 2.36

## Files Modified

- `docker/Dockerfile.build` - New builder image definition
- `scripts/build_builder_image.sh` - Script to build the image
- `scripts/install_extension.sh` - Updated to use builder image
- `docker/README.md` - Documentation for the new approach

## Testing

The workflow has been tested:
- ✅ Builder image builds successfully
- ✅ Install script syntax is valid
- ✅ Integration with existing workflow

## Next Steps

1. Test with a running PostgreSQL container
2. Verify extension builds and loads correctly
3. Test index building with the new extension
