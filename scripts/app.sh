#!/usr/bin/env bash
# Main application script for VectorChord - orchestrates the complete workflow

set -euo pipefail

# Configuration variables (override via environment variables)
CLEAN_BUILD="${CLEAN_BUILD:-false}"
FORCE_REBUILD="${FORCE_REBUILD:-false}"
USE_INCREMENTAL="${USE_INCREMENTAL:-true}"
PERSISTENT_BUILDER="${PERSISTENT_BUILDER:-true}"
AUTO_BUMP_PATCH="${AUTO_BUMP_PATCH:-true}"
AUTO_REINDEX="${AUTO_REINDEX:-true}"
FORCE_COPY="${FORCE_COPY:-false}"
BUILD_IN_CONTAINER="${BUILD_IN_CONTAINER:-true}"
EXT_VERSION_OVERRIDE="${EXT_VERSION_OVERRIDE:-}"
POSTGRES_VERSION="${POSTGRES_VERSION:-17}"
CONTAINER_NAME="${CONTAINER_NAME:-vchord_listings}"
DB_NAME="${DB_NAME:-listings_db}"
DB_USER="${DB_USER:-listings_user}"
DB_PASSWORD="${DB_PASSWORD:-listings_somebi_731}"
DB_PORT="${DB_PORT:-5432}"
PG_PORT_IN_CONTAINER=5432

# Function to determine build strategy
determine_build_strategy() {
    if [ "$CLEAN_BUILD" = true ] || [ "$FORCE_REBUILD" = true ]; then
        echo "clean"
    elif [ "$USE_INCREMENTAL" = true ]; then
        echo "incremental"
    else
        echo "clean"
    fi
}

# Function to run the appropriate build script
run_build() {
    local build_strategy="$1"
    
    case "$build_strategy" in
        "incremental")
            echo "Running incremental build..."
            export PERSISTENT_BUILDER="$PERSISTENT_BUILDER"
            export FORCE_REBUILD="$FORCE_REBUILD"
            timeout 30 ./scripts/build_incremental.sh
            ;;
        "clean")
            echo "Running clean build..."
            export PERSISTENT_BUILDER="$PERSISTENT_BUILDER"
            timeout 30 ./scripts/build_clean.sh
            ;;
        *)
            echo "Error: Unknown build strategy: $build_strategy" >&2
            exit 1
            ;;
    esac
}

# Function to setup build dependencies
setup_dependencies() {
    echo "Setting up build dependencies..."
    
    # Install build dependencies (Postgres dev and toolchain prerequisites)
    PACKAGES=(build-essential libpq-dev postgresql-server-dev-${POSTGRES_VERSION})
    need_apt=false
    for pkg in "${PACKAGES[@]}"; do
      if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        need_apt=true
        break
      fi
    done
    
    if [ "$need_apt" = true ]; then
      echo "Installing APT packages: ${PACKAGES[*]}"
      sudo apt-get update
      sudo apt-get install -y "${PACKAGES[@]}"
      sudo apt-get autoremove -y
    else
      echo "APT packages already installed; skipping apt-get."
    fi
    
    # Ensure rustup-managed Cargo (supports Edition 2024) is used
    export PATH="$HOME/.cargo/bin:$PATH"
    if [ -f "$HOME/.cargo/env" ]; then
      . "$HOME/.cargo/env"
    fi
    
    if ! command -v rustup >/dev/null 2>&1; then
      echo "Installing Rust toolchain..."
      export RUSTUP_INIT_SKIP_PATH_CHECK=yes
      curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
      . "$HOME/.cargo/env"
    fi
    
    if ! rustup toolchain list | grep -q '^stable'; then
      echo "Installing stable Rust toolchain..."
      rustup toolchain install stable
    fi
    
    rustup override set stable
    echo "Rust toolchain version:"
    cargo --version
    
    echo "Dependencies setup completed successfully!"
}

# Function to manage versions
manage_versions() {
    echo "Managing extension versions..."
    
    # Check if container has a working extension first
    local existing_version=""
    if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
        existing_version=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || true)
    fi
    
    if [ -n "$existing_version" ] && [ "$AUTO_BUMP_PATCH" = true ]; then
        echo "Container already has working vchord extension version ${existing_version}, skipping auto-bump to preserve stability"
        echo "Set AUTO_BUMP_PATCH=false or manually remove the extension if you need to update"
        AUTO_BUMP_PATCH=false
    fi
    
    if [ "$AUTO_BUMP_PATCH" = true ]; then
        echo "Auto-bumping patch version..."
        
        local control_default_version
        control_default_version=$(sed -nE "s/^default_version = '([^']+)'/\1/p" vchord.control | head -n1 || true)
        
        if [ -n "$control_default_version" ]; then
            IFS='.' read -r MAJ MIN PAT <<<"$control_default_version"
            if [[ -n "$MAJ" && -n "$MIN" && -n "$PAT" ]]; then
                local next_version="${MAJ}.${MIN}.$((PAT+1))"
                local from_version_for_upgrade="${existing_version:-$control_default_version}"
                
                # Only proceed if we have a valid upgrade path
                if [ -n "$from_version_for_upgrade" ] && [ "$from_version_for_upgrade" != "$next_version" ]; then
                    # Create install SQL if missing by copying latest available install SQL
                    if [ ! -f "sql/install/vchord--${next_version}.sql" ]; then
                        local available_install_versions
                        mapfile -t available_install_versions < <(ls -1 sql/install/vchord--*.sql 2>/dev/null | sed -E "s#.*/vchord--(.*)\\.sql#\1#" | sort -V)
                        if [ ${#available_install_versions[@]} -gt 0 ]; then
                            local last_install="${available_install_versions[-1]}"
                            cp "sql/install/vchord--${last_install}.sql" "sql/install/vchord--${next_version}.sql"
                        else
                            echo "No install SQL templates found to copy from (sql/install)." >&2
                            exit 4
                        fi
                    fi
                    
                    # Create upgrade SQL if missing (with proper content, not just comments)
                    if [ ! -f "sql/upgrade/vchord--${from_version_for_upgrade}--${next_version}.sql" ]; then
                        {
                            echo "-- Upgrade from ${from_version_for_upgrade} to ${next_version}"
                            echo "-- Generated by app.sh with AUTO_BUMP_PATCH=true"
                            echo ""
                            echo "-- This is a version bump upgrade. No schema changes required."
                            echo "SELECT 1; -- Ensure the file has valid SQL content"
                        } > "sql/upgrade/vchord--${from_version_for_upgrade}--${next_version}.sql"
                    fi
                    
                    # Update control file default_version
                    sed -i -E "s/^(default_version = ')${control_default_version}(')/\\1${next_version}\\2/" vchord.control
                    echo "Auto-bumped vchord version: ${control_default_version} -> ${next_version}"
                else
                    echo "Skipping auto-bump: no valid upgrade path from ${from_version_for_upgrade} to ${next_version}"
                fi
            fi
        fi
    fi
    
    # Determine target version
    local target_version
    if [ -n "$EXT_VERSION_OVERRIDE" ]; then
        target_version="$EXT_VERSION_OVERRIDE"
    else
        local control_default_version
        control_default_version=$(sed -nE "s/^default_version = '([^']+)'/\1/p" vchord.control | head -n1 || true)
        
        if [ -n "$control_default_version" ]; then
            target_version="$control_default_version"
        else
            # Fall back to highest available install SQL version
            local available_install_versions
            mapfile -t available_install_versions < <(ls -1 sql/install/vchord--*.sql 2>/dev/null | sed -E "s#.*/vchord--(.*)\\.sql#\1#" | sort -V)
            
            if [ ${#available_install_versions[@]} -gt 0 ]; then
                target_version="${available_install_versions[-1]}"
            else
                echo "No install SQL files found and control default_version is empty." >&2
                exit 3
            fi
        fi
    fi
    
    echo "Target version determined: $target_version"
    echo "TARGET_VERSION=$target_version" > .target_version
}

# Function to install extension (includes artifact deployment)
install_extension() {
    echo "Installing VectorChord extension (including artifact deployment)..."
    
    # Export environment variables for install_extension.sh
    export CONTAINER_NAME="$CONTAINER_NAME"
    export DB_NAME="$DB_NAME"
    export DB_USER="$DB_USER"
    export DB_PASSWORD="$DB_PASSWORD"
    export DB_PORT="$DB_PORT"
    export PG_PORT_IN_CONTAINER="$PG_PORT_IN_CONTAINER"
    export AUTO_REINDEX="$AUTO_REINDEX"
    export FORCE_COPY="$FORCE_COPY"
    export BUILD_IN_CONTAINER="$BUILD_IN_CONTAINER"
    
    # Run the install_extension.sh script
    timeout 120 ./scripts/install_extension.sh
}

# Function to rebuild indices if requested
rebuild_indices_if_requested() {
    if [ "$AUTO_REINDEX" = true ]; then
        echo "Rebuilding VectorChord indices..."
        
        # Export environment variables for rebuild_indices.sh
        export CONTAINER_NAME="$CONTAINER_NAME"
        export DB_NAME="$DB_NAME"
        export DB_USER="$DB_USER"
        export DB_PASSWORD="$DB_PASSWORD"
        export DB_PORT="$DB_PORT"
        export PG_PORT_IN_CONTAINER="$PG_PORT_IN_CONTAINER"
        export AUTO_REINDEX="$AUTO_REINDEX"
        
        # Run the rebuild_indices.sh script
        timeout 60 ./scripts/rebuild_indices.sh
    else
        echo "AUTO_REINDEX disabled - skipping index rebuild"
    fi
}

# Main execution
main() {
    echo "VectorChord Application Script"
    echo "============================="
    echo "Configuration:"
    echo "  CLEAN_BUILD: $CLEAN_BUILD"
    echo "  FORCE_REBUILD: $FORCE_REBUILD"
    echo "  USE_INCREMENTAL: $USE_INCREMENTAL"
    echo "  PERSISTENT_BUILDER: $PERSISTENT_BUILDER"
    echo "  AUTO_BUMP_PATCH: $AUTO_BUMP_PATCH"
    echo "  AUTO_REINDEX: $AUTO_REINDEX"
    echo "  FORCE_COPY: $FORCE_COPY"
    echo "  BUILD_IN_CONTAINER: $BUILD_IN_CONTAINER"
    echo "  EXT_VERSION_OVERRIDE: ${EXT_VERSION_OVERRIDE:-not set}"
    echo "  CONTAINER_NAME: $CONTAINER_NAME"
    echo "  DB_NAME: $DB_NAME"
    echo "  DB_USER: $DB_USER"
    echo ""
    
    # Check if we're in the right directory
    if [ ! -f "scripts/build_incremental.sh" ] || [ ! -f "scripts/build_clean.sh" ]; then
        echo "Error: Build scripts not found. Please run this script from the VectorChord root directory." >&2
        exit 1
    fi
    
    # Setup dependencies
    setup_dependencies
    
    # Manage versions
    manage_versions
    
    # Determine build strategy
    local build_strategy
    build_strategy=$(determine_build_strategy)
    echo "Selected build strategy: $build_strategy"
    echo ""
    
    # Run the appropriate build
    if ! run_build "$build_strategy"; then
        echo "Error: Build failed!" >&2
        exit 1
    fi
    
    # Install extension (includes artifact deployment)
    if ! install_extension; then
        echo "Error: Extension installation failed!" >&2
        exit 1
    fi
    
    # Rebuild indices if requested
    if ! rebuild_indices_if_requested; then
        echo "Error: Index rebuild failed!" >&2
        exit 1
    fi
    
    echo "Complete VectorChord workflow completed successfully!"
}

# Run main function
main "$@"
