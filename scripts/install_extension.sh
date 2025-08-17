#!/usr/bin/env bash
# Install the VectorChord PostgreSQL extension into an already running Docker container.
# This script handles artifact deployment and extension installation.

set -euo pipefail

# Configuration variables (override via environment variables)
CONTAINER_NAME="${CONTAINER_NAME:-vchord_listings}"
DB_NAME="${DB_NAME:-listings_db}"
DB_USER="${DB_USER:-listings_user}"
DB_PASSWORD="${DB_PASSWORD:-listings_somebi_731}"
DB_PORT="${DB_PORT:-5432}"
PG_PORT_IN_CONTAINER=5432
AUTO_REINDEX="${AUTO_REINDEX:-false}"
FORCE_COPY="${FORCE_COPY:-false}"
BUILD_IN_CONTAINER="${BUILD_IN_CONTAINER:-true}"

# Function to check if container is running
check_container_running() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Error: required container '${CONTAINER_NAME}' is not running." >&2
        echo "This script does not create or manage containers. Please start the container externally." >&2
        exit 1
    fi
}

# Function to deploy artifacts
deploy_artifacts() {
    echo "Deploying built artifacts..."
    
    # Query installation paths from the container
    local pkglibdir
    pkglibdir=$(docker exec "$CONTAINER_NAME" pg_config --pkglibdir)
    local sharedir
    sharedir=$(docker exec "$CONTAINER_NAME" pg_config --sharedir)
    
    echo "Installation paths:"
    echo "  PKGLIBDIR: $pkglibdir"
    echo "  SHAREDIR: $sharedir"
    echo ""
    
    local local_so="build/raw/pkglibdir/vchord.so"
    local remote_so="$pkglibdir/vchord.so"
    
    if [ ! -f "$local_so" ]; then
        echo "Local artifact $local_so not found. Build may have failed." >&2
        exit 1
    fi
    
    # Check GLIBC compatibility
    local container_glibc_version
    container_glibc_version=$(docker exec "$CONTAINER_NAME" sh -lc "getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print \$2}'" 2>/dev/null || true)
    if [ -z "$container_glibc_version" ]; then
        container_glibc_version=$(docker exec "$CONTAINER_NAME" sh -lc "ldd --version 2>&1 | head -n1 | sed -E 's/.* ([0-9]+\.[0-9]+).*/\\1/'" 2>/dev/null || true)
    fi
    
    local required_glibc
    required_glibc=$(strings "$local_so" 2>/dev/null | grep -oE "GLIBC_[0-9]+\.[0-9]+" | sort -V | tail -n1 | sed -E "s/^GLIBC_//" || true)
    
    if [ -n "$required_glibc" ] && [ -n "$container_glibc_version" ]; then
        # If container glibc is older than required, block the copy to avoid breaking postgres
        if [ "$(printf '%s\n%s\n' "$container_glibc_version" "$required_glibc" | sort -V | head -n1)" != "$required_glibc" ]; then
            echo "GLIBC compatibility check: container=$container_glibc_version, required_by_local_so=$required_glibc"
            echo "Refusing to copy host-built vchord.so into the container to avoid GLIBC mismatch (would cause postgres to fail at startup)."
            echo "Hint: rebuild inside a matching environment (Debian bookworm) or set BUILD_IN_CONTAINER=true to build inside the container."
            exit 1
        fi
    fi
    
    # Check if copy is needed
    local remote_present
    remote_present=$(docker exec "$CONTAINER_NAME" sh -lc "[ -f '$remote_so' ] && echo yes || echo no" || echo no)
    
    local need_copy=false
    if [ "$remote_present" = "no" ]; then
        echo "No existing vchord.so in container."
        need_copy=true
    else
        # Prefer sha256 if available
        local local_digest
        local remote_digest
        if command -v sha256sum >/dev/null 2>&1; then
            local_digest=$(sha256sum "$local_so" | awk '{print $1}')
        else
            local_digest=$(md5sum "$local_so" | awk '{print $1}')
        fi
        
        remote_digest=$(docker exec "$CONTAINER_NAME" sh -lc "if command -v sha256sum >/dev/null 2>&1 && [ -f '$remote_so' ]; then sha256sum '$remote_so' | awk '{print \\\$1}'; elif [ -f '$remote_so' ]; then md5sum '$remote_so' | awk '{print \\\$1}'; else echo missing; fi" 2>/dev/null || echo missing)
        
        if [ "$local_digest" != "$remote_digest" ]; then
            echo "Local vchord.so digest ($local_digest) differs from remote ($remote_digest). Will copy."
            need_copy=true
        fi
    fi
    
    if [ "${FORCE_COPY:-false}" = true ]; then
        echo "FORCE_COPY=true set; will copy artifacts regardless of digest."
        need_copy=true
    fi
    
    if [ "$need_copy" = true ]; then
        echo "Copying built artifacts into the container..."
        
        # Backup existing .so for rollback if present
        local backup_path=""
        if [ "$remote_present" = "yes" ]; then
            backup_path="${remote_so}.bak.$(date +%s)"
            docker exec "$CONTAINER_NAME" sh -lc "cp '$remote_so' '$backup_path'" || true
        fi
        
        # Copy artifacts
        docker cp build/raw/pkglibdir/. "$CONTAINER_NAME:$pkglibdir/"
        docker cp build/raw/sharedir/. "$CONTAINER_NAME:$sharedir/"
        
        echo "Note: container restart is NOT performed by this script."
        echo "New backend sessions will load the updated library automatically when needed."
        
        # Store backup path for potential rollback
        echo "$backup_path" > .backup_path
    else
        echo "Container already has matching vchord.so; skipping copy."
    fi
    
    # Verify deployment
    local local_digest_alg="sha256"
    local local_digest
    local remote_digest_alg
    local remote_digest
    
    if command -v sha256sum >/dev/null 2>&1; then
        local_digest=$(sha256sum "$local_so" | awk '{print $1}')
    else
        local_digest_alg="md5"
        local_digest=$(md5sum "$local_so" | awk '{print $1}')
    fi
    
    remote_digest_alg=$(docker exec "$CONTAINER_NAME" sh -lc 'if command -v sha256sum >/dev/null 2>&1; then echo sha256; else echo md5; fi')
    if [ "$remote_digest_alg" = "sha256" ]; then
        remote_digest=$(docker exec "$CONTAINER_NAME" sha256sum "$remote_so" | awk '{print $1}')
    else
        remote_digest=$(docker exec "$CONTAINER_NAME" md5sum "$remote_so" | awk '{print $1}')
    fi
    
    local local_size
    local remote_size
    local_size=$(stat -c%s "$local_so")
    remote_size=$(docker exec "$CONTAINER_NAME" stat -c%s "$remote_so")
    
    if [ "$local_digest" = "$remote_digest" ] && [ "$local_digest_alg" = "$remote_digest_alg" ]; then
        local match_label="yes"
    else
        local match_label="no"
    fi
    
    echo "vchord binary deploy check: path=${remote_so}, digest match=${match_label} (local ${local_digest_alg}:${local_digest}, remote ${remote_digest_alg}:${remote_digest}), size local=${local_size}B, remote=${remote_size}B"
    
    if [ "$match_label" = "yes" ]; then
        echo "Artifact deployment completed successfully!"
    else
        echo "Warning: Deployment verification failed" >&2
        exit 1
    fi
}

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
        exit 1
    fi
    
    echo "PostgreSQL is ready"
}

# Function to check database connection
check_db_connection() {
    local container="$1"
    
    echo "Testing database connection..."
    if ! docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
        echo "Warning: unable to connect to Postgres as user '$DB_USER'. Cannot proceed with extension operations." >&2
        exit 1
    fi
    
    echo "Database connection successful"
}

# Function to get current extension version
get_current_version() {
    local container="$1"
    
    docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || echo ""
}

# Function to determine target version
determine_target_version() {
    # Try to read from .target_version file (set by app.sh)
    if [ -f ".target_version" ]; then
        local target_version
        target_version=$(grep "^TARGET_VERSION=" .target_version | cut -d'=' -f2)
        if [ -n "$target_version" ]; then
            echo "$target_version"
            return 0
        fi
    fi
    
    # Fall back to control file default_version
    local control_default_version
    control_default_version=$(sed -nE "s/^default_version = '([^']+)'/\1/p" vchord.control | head -n1 || true)
    
    if [ -n "$control_default_version" ]; then
        echo "$control_default_version"
        return 0
    fi
    
    # Fall back to highest available install SQL version
    local available_install_versions
    mapfile -t available_install_versions < <(ls -1 sql/install/vchord--*.sql 2>/dev/null | sed -E "s#.*/vchord--(.*)\\.sql#\1#" | sort -V)
    
    if [ ${#available_install_versions[@]} -gt 0 ]; then
        echo "${available_install_versions[-1]}"
        return 0
    fi
    
    echo "No install SQL files found and control default_version is empty." >&2
    return 1
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

# Function to manage extension
manage_extension() {
    local container="$1"
    local current_version="$2"
    local target_version="$3"
    
    if [ -z "$current_version" ]; then
        # Not installed: install at target version
        echo "Installing vchord extension at version $target_version..."
        docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" \
            -v ON_ERROR_STOP=1 \
            -c "CREATE EXTENSION IF NOT EXISTS vchord;" \
            -c "ALTER EXTENSION vchord UPDATE TO '${target_version}';" \
            -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
    elif [ "$current_version" = "$target_version" ]; then
        echo "vchord is already at version $current_version; nothing to do."
    elif [ "$(printf '%s\n%s\n' "$current_version" "$target_version" | sort -V | head -n1)" = "$current_version" ] && [ "$current_version" != "$target_version" ]; then
        # Upgrade to target version
        echo "Upgrading vchord from version $current_version to $target_version..."
        docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" \
            -v ON_ERROR_STOP=1 \
            -c "ALTER EXTENSION vchord UPDATE TO '${target_version}';" \
            -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
    else
        echo "Installed vchord version ($current_version) is newer than target ($target_version). Skipping downgrade."
        docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
    fi
}

# Function to print version summary
print_version_summary() {
    local container="$1"
    local current_version="$2"
    local target_version="$3"
    
    local new_version
    new_version=$(get_current_version "$container")
    
    if [ -z "$current_version" ]; then
        local prev_label="not installed"
    else
        local prev_label="$current_version"
    fi
    
    echo "vchord extension version summary: previous=${prev_label}, target=${target_version}, now=${new_version:-not installed}"
}

# Function to restart container if needed
restart_container_if_needed() {
    local container="$1"
    
    # Check if we need to restart (i.e., if .backup_path exists from deploy_artifacts)
    if [ -f ".backup_path" ]; then
        echo "New extension binary deployed. Restarting container to load the updated extension..."
        echo "Container restart is required because vchord is loaded as shared_preload_libraries at startup."
        
        echo "Restarting container: $container"
        docker restart "$container"
        
        # Wait for PostgreSQL to be ready again
        echo "Waiting for PostgreSQL to become ready after restart..."
        local ready=false
        for i in {1..60}; do
            if docker exec "$container" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
                ready=true
                break
            fi
            sleep 1
        done
        
        if [ "$ready" != true ]; then
            echo "PostgreSQL did not become ready after restart." >&2
            exit 2
        fi
        
        echo "Container restarted successfully. New extension is now loaded."
        
        # Verify extension functionality after restart
        echo "Verifying extension functionality after restart..."
        
        # Test loading the extension
        if docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA -c "LOAD 'vchord'; SELECT 1;" >/dev/null 2>&1; then
            echo "Extension load test after restart: ok"
        else
            echo "Extension load test after restart: failed" >&2
            exit 1
        fi
        
        # Test calling a function from the extension
        if docker exec "$container" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA \
            -c "LOAD 'vchord'; SELECT \"_vchordg_support_vector_ip_ops\"();" >/dev/null 2>&1; then
            echo "Extension function call test after restart: ok"
        else
            echo "Extension function call test after restart: failed" >&2
            exit 1
        fi
    else
        echo "No container restart needed."
    fi
}



# Main execution
main() {
    echo "VectorChord Extension Installation Script"
    echo "========================================"
    echo "Configuration:"
    echo "  CONTAINER_NAME: $CONTAINER_NAME"
    echo "  DB_NAME: $DB_NAME"
    echo "  DB_USER: $DB_USER"
    echo "  AUTO_REINDEX: $AUTO_REINDEX"
    echo "  FORCE_COPY: $FORCE_COPY"
    echo "  BUILD_IN_CONTAINER: $BUILD_IN_CONTAINER"
    echo ""
    
    echo "This script will handle the VectorChord extension using the following workflow:"
    echo "1. Deploy built artifacts into the container"
    echo "2. Verify container is running and PostgreSQL is ready"
    echo "3. Create/update the PostgreSQL extension"
    echo "4. Restart container if needed (for shared_preload_libraries)"
    echo ""
    
    # Check if container is running
    check_container_running
    
    # Deploy artifacts first
    deploy_artifacts
    
    # Wait for PostgreSQL to be ready
    wait_for_postgres "$CONTAINER_NAME"
    
    # Check database connection
    check_db_connection "$CONTAINER_NAME"
    
    # Get current extension version
    local current_version
    current_version=$(get_current_version "$CONTAINER_NAME")
    echo "Current vchord extension version: ${current_version:-not installed}"
    
    # Determine target version
    local target_version
    if ! target_version=$(determine_target_version); then
        echo "Error: Could not determine target version" >&2
        exit 1
    fi
    echo "Target version: $target_version"
    echo ""
    
    # Manage the extension
    if ! manage_extension "$CONTAINER_NAME" "$current_version" "$target_version"; then
        echo "Error: Extension management failed" >&2
        exit 1
    fi
    
    # Print version summary
    print_version_summary "$CONTAINER_NAME" "$current_version" "$target_version"
    
    # Verify extension functionality
    if ! verify_extension "$CONTAINER_NAME"; then
        echo "Error: Extension verification failed" >&2
        exit 1
    fi
    
    # Restart container if needed
    restart_container_if_needed "$CONTAINER_NAME"
    
    echo ""
    echo "VectorChord extension installation completed successfully!"
    echo ""
    echo "The extension is now installed and ready to use."
    echo "You can verify the installation by connecting to the database and running:"
    echo "  SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
    echo ""
    echo "Note: If you need to rebuild indices manually, run:"
    echo "  ./scripts/rebuild_indices.sh"
    echo ""
}

# Run main function
main "$@"

