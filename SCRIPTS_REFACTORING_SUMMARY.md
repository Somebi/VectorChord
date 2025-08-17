# VectorChord Scripts Refactoring Summary

## Overview
The VectorChord scripts have been refactored to achieve strict responsibility separation. Each script now has a single, well-defined purpose as indicated by its filename.

## Script Responsibilities

### 1. `app.sh` - Main Entry Point and Complete Workflow Orchestrator
**Purpose**: Main entry point that orchestrates the complete VectorChord workflow
**Responsibilities**:
- Sets up build dependencies and Rust toolchain
- Manages extension versions and auto-bumping
- Builds the extension (incremental or clean)
- Calls `install_extension.sh` to handle artifact deployment and extension installation
- **Does NOT contain**: Extension installation logic, container restart logic, index rebuilding logic, or artifact deployment logic

**Workflow Steps**:
1. Setup dependencies and Rust toolchain
2. Manage extension versions and auto-bumping
3. Build the extension (incremental or clean)
4. Call `install_extension.sh` for artifact deployment and extension installation

### 2. `install_extension.sh` - Extension Installation and Artifact Deployment
**Purpose**: Handles artifact deployment and installs the VectorChord extension into PostgreSQL
**Responsibilities**:
- **Deploys built artifacts** into the container (GLIBC compatibility checks, digest verification, backup creation)
- Verifies container is running and PostgreSQL is ready
- Creates/updates the PostgreSQL extension
- Restarts container if needed (detected via .backup_path file)
- Optionally calls `rebuild_indices.sh` for index operations
- Verifies extension functionality

**What it NO LONGER does**:
- ❌ Dependency installation
- ❌ Rust toolchain setup
- ❌ Version management
- ❌ Building

### 3. `build_incremental.sh` - Incremental Builds
**Purpose**: Performs incremental builds with caching
**Responsibilities**:
- Manages persistent build containers
- Checks dependency changes
- Performs incremental compilation
- Optimizes build performance

### 4. `build_clean.sh` - Clean Builds
**Purpose**: Performs clean, fresh builds
**Responsibilities**:
- Creates temporary build containers
- Performs complete rebuilds
- Ensures clean build environment

### 5. `build_builder_image.sh` - Docker Builder Image
**Purpose**: Builds the Docker image used for compilation
**Responsibilities**:
- Builds the vchord-builder Docker image
- Uses Dockerfile.build from docker/ directory

### 6. `rebuild_indices.sh` - Index Management
**Purpose**: Rebuilds VectorChord indices
**Responsibilities**:
- Recreates VectorChord indexes
- Creates optimized indexes
- Handles index DROP + CREATE operations
- Verifies index functionality

## Benefits of the New Structure

### 1. **Single Responsibility Principle**
Each script has one clear purpose, making them easier to understand, maintain, and debug.

### 2. **Modularity**
Scripts can be run independently for specific tasks (e.g., just building, just installing extensions, just rebuilding indices).

### 3. **Reusability**
Specialized scripts can be called from different workflows or used in CI/CD pipelines.

### 4. **Maintainability**
Changes to specific functionality only affect the relevant script, reducing the risk of breaking other features.

### 5. **Testability**
Individual scripts can be tested in isolation, making debugging easier.

### 6. **Flexibility**
Users can run specific parts of the workflow without going through the entire process.

## Usage Examples

### Complete Workflow (Build + Deploy + Install)
```bash
./scripts/app.sh
```

### Just Build (Incremental)
```bash
./scripts/build_incremental.sh
```

### Just Build (Clean)
```bash
./scripts/build_clean.sh
```

### Just Install Extension (includes artifact deployment)
```bash
./scripts/install_extension.sh
```

### Just Rebuild Indices
```bash
./scripts/rebuild_indices.sh
```

### Just Build Builder Image
```bash
./scripts/build_builder_image.sh
```

## Environment Variables

Each script accepts relevant environment variables for configuration. The main configuration variables are:

- `CONTAINER_NAME`: PostgreSQL container name
- `DB_NAME`: Database name
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password
- `POSTGRES_VERSION`: PostgreSQL version
- `CLEAN_BUILD`: Force clean build
- `USE_INCREMENTAL`: Enable incremental builds
- `AUTO_BUMP_PATCH`: Auto-bump patch versions
- `AUTO_REINDEX`: Auto-rebuild indices
- `FORCE_COPY`: Force artifact copy
- `BUILD_IN_CONTAINER`: Build inside container

## Migration Notes

The refactoring maintains backward compatibility:
- `app.sh` is now the main entry point for the complete workflow
- `install_extension.sh` handles both artifact deployment and extension installation
- All existing functionality is preserved
- Environment variables are still supported
- The user experience remains the same

## Workflow Scenarios

### Scenario 1: Complete Development Cycle
```bash
# Build, deploy, and install everything
./scripts/app.sh
```

### Scenario 2: Just Rebuild After Code Changes
```bash
# Build only (incremental)
./scripts/build_incremental.sh

# Then install the new extension (includes deployment)
./scripts/install_extension.sh
```

### Scenario 3: Clean Build
```bash
# Force clean build
CLEAN_BUILD=true ./scripts/app.sh
```

### Scenario 4: Just Install Extension (with deployment)
```bash
# When extension is already built, deploy and install
./scripts/install_extension.sh
```

### Scenario 5: Just Rebuild Indices
```bash
# When you only need to recreate indexes
./scripts/rebuild_indices.sh
```

## Script Dependencies

### `app.sh` calls:
- `build_incremental.sh` or `build_clean.sh` (depending on strategy)
- `install_extension.sh`

### `install_extension.sh` can optionally call:
- `rebuild_indices.sh` (when AUTO_REINDEX=true)

### All scripts are independent and can be run standalone

## Future Enhancements

With this modular structure, future enhancements can be easily added:
- New build strategies
- Additional deployment methods
- Custom index optimization
- Integration with other tools
- CI/CD pipeline integration
