# VectorChord Scripts

This directory contains the reorganized build and installation scripts for VectorChord.

## Script Overview

### Build Scripts

#### `app.sh` - Main Application Script
- **Purpose**: Main entry point that chooses between incremental and clean builds
- **Usage**: `./scripts/app.sh`
- **Environment Variables**:
  - `CLEAN_BUILD`: Force clean build (default: false)
  - `FORCE_REBUILD`: Force rebuild even if unchanged (default: false)
  - `USE_INCREMENTAL`: Enable incremental builds (default: true)
  - `PERSISTENT_BUILDER`: Use persistent build container (default: true)

#### `build_incremental.sh` - Incremental Build Script
- **Purpose**: Performs incremental builds with dependency checking and caching
- **Usage**: `./scripts/build_incremental.sh`
- **Features**:
  - Dependency change detection
  - Persistent build containers
  - Cargo incremental compilation
  - Target directory reuse

#### `build_clean.sh` - Clean Build Script
- **Purpose**: Always performs a fresh build without caching
- **Usage**: `./scripts/build_clean.sh`
- **Features**:
  - Always cleans target directory
  - Disables incremental compilation
  - Fresh dependency resolution

### Installation and Management Scripts

#### `install_extension.sh` - Extension Installation
- **Purpose**: Installs the built extension into PostgreSQL containers
- **Usage**: `./scripts/install_extension.sh`
- **Features**:
  - Extension deployment
  - Container restart management
  - Version management
  - GLIBC compatibility checking

#### `rebuild_indices.sh` - Index Management
- **Purpose**: Rebuilds VectorChord indices after extension updates
- **Usage**: `./scripts/rebuild_indices.sh`
- **Features**:
  - Index recreation (DROP + CREATE)
  - Optimized index creation
  - Extension verification

#### `build_builder_image.sh` - Docker Image Builder
- **Purpose**: Creates the builder Docker image
- **Usage**: `./scripts/build_builder_image.sh`
- **Features**:
  - Rust toolchain installation
  - PostgreSQL development packages
  - Builder user setup

## Usage Examples

### Basic Incremental Build
```bash
./scripts/app.sh
```

### Force Clean Build
```bash
CLEAN_BUILD=true ./scripts/app.sh
```

### Force Rebuild
```bash
FORCE_REBUILD=true ./scripts/app.sh
```

### Disable Incremental Builds
```bash
USE_INCREMENTAL=false ./scripts/app.sh
```

### Use Temporary Build Containers
```bash
PERSISTENT_BUILDER=false ./scripts/app.sh
```

### Full Installation Workflow
```bash
# 1. Build the extension
./scripts/app.sh

# 2. Install into PostgreSQL container
./scripts/install_extension.sh

# 3. Rebuild indices (optional)
AUTO_REINDEX=true ./scripts/rebuild_indices.sh
```

## Script Dependencies

- **Docker**: Required for build containers
- **Builder Image**: Must be built first using `build_builder_image.sh`
- **PostgreSQL Container**: Must be running for installation scripts

## Environment Variables

### Build Configuration
- `CLEAN_BUILD`: Force clean build
- `FORCE_REBUILD`: Force rebuild regardless of changes
- `USE_INCREMENTAL`: Enable/disable incremental builds
- `PERSISTENT_BUILDER`: Use persistent vs temporary build containers

### Database Configuration
- `CONTAINER_NAME`: PostgreSQL container name
- `DB_NAME`: Database name
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password
- `DB_PORT`: Database port

### Build Container Configuration
- `BUILDER_IMAGE`: Docker image for building
- `BUILD_CONTAINER`: Build container name
- `JOBS`: Number of parallel build jobs

## Architecture

The new script structure separates concerns:

1. **Build Logic**: Handled by `app.sh`, `build_incremental.sh`, and `build_clean.sh`
2. **Installation Logic**: Handled by `install_extension.sh`
3. **Index Management**: Handled by `rebuild_indices.sh`
4. **Infrastructure**: Handled by `build_builder_image.sh`

This separation allows for:
- Independent testing of each component
- Easier maintenance and debugging
- Flexible workflow combinations
- Better error isolation

## Troubleshooting

### Build Issues
- Ensure builder image exists: `./scripts/build_builder_image.sh`
- Check Docker daemon is running
- Verify sufficient disk space for builds

### Installation Issues
- Ensure PostgreSQL container is running
- Check database connection credentials
- Verify extension binary compatibility

### Index Issues
- Ensure extension is properly installed
- Check database permissions
- Verify table structure