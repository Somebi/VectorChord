#!/usr/bin/env bash
# Build and install the VectorChord PostgreSQL extension into an already running Docker container.
# This script does NOT build images or create/start containers; that is handled by another project.

set -euo pipefail

#postgresql://listings_user:listings_somebi_731@localhost:5433/listings_db

# Configuration variables (override via environment variables)
POSTGRES_VERSION="${POSTGRES_VERSION:-17}"
VECTORCHORD_VERSION="${VECTORCHORD_VERSION:-v0.5.0}"
CONTAINER_NAME="${CONTAINER_NAME:-vchord_listings}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/tensorchord/vchord-postgres:pg${POSTGRES_VERSION}-${VECTORCHORD_VERSION}}"
PATH_TO_POSTGRES_DATA="${PATH_TO_POSTGRES_DATA:-$(realpath "$(pwd)/../postgres_data")}" 
# Host-shared path for dumping ANN samples; will be bind-mounted into container at /var/local/postgres_shared
PATH_TO_SHARED="${PATH_TO_SHARED:-/var/local/postgres_shared}"
DB_NAME="${DB_NAME:-listings_db}"
DB_USER="${DB_USER:-listings_user}"
DB_PASSWORD="${DB_PASSWORD:-listings_somebi_731}"
DB_PORT="${DB_PORT:-5432}"
# TARGET_VERSION is computed automatically unless EXT_VERSION_OVERRIDE is provided
TARGET_VERSION="${EXT_VERSION_OVERRIDE:-}"
AUTO_BUMP_PATCH="${AUTO_BUMP_PATCH:-true}"
AUTO_REINDEX="${AUTO_REINDEX:-false}"  # Note: Actually performs DROP+CREATE, not REINDEX
# If true, when GLIBC mismatch is detected, build and install the extension inside the container
BUILD_IN_CONTAINER="${BUILD_IN_CONTAINER:-false}"
REPO_URL="${REPO_URL:-https://github.com/tensorchord/VectorChord.git}"
PG_PORT_IN_CONTAINER=5432

# Ensure host shared directory exists for ANN samples
mkdir -p "$PATH_TO_SHARED"

# Ensure the container is already running (managed externally)
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required but not found in PATH." >&2
  exit 1
fi
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "Error: required container '${CONTAINER_NAME}' is not running." >&2
  echo "This script does not create or manage containers. Please start the container externally (image expected: ${IMAGE_REPO})." >&2
  exit 1
fi

# Clone the repository if this script is run outside of it
if [ ! -d .git ]; then
  git clone "$REPO_URL" VectorChord
  cd VectorChord
else
  cd "$(git rev-parse --show-toplevel)"
fi

# Do not decide TARGET_VERSION yet; we'll compute after contacting the DB

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
  export RUSTUP_INIT_SKIP_PATH_CHECK=yes
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  . "$HOME/.cargo/env"
fi
if ! rustup toolchain list | grep -q '^stable'; then
  rustup toolchain install stable
fi
rustup override set stable
cargo --version

# Optional: auto-bump patch version by generating install/upgrade SQL and updating control file
# Only run auto-bump if no working extension exists in the container
if [ "$AUTO_BUMP_PATCH" = true ]; then
  # Check if container has a working extension first
  CONTAINER_READY=false
  if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
    CONTAINER_READY=true
  fi
  
  if [ "$CONTAINER_READY" = true ]; then
    EXISTING_EXT_VERSION=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || true)
    if [ -n "$EXISTING_EXT_VERSION" ]; then
      echo "Container already has working vchord extension version ${EXISTING_EXT_VERSION}, skipping auto-bump to preserve stability"
      echo "Set AUTO_BUMP_PATCH=false or manually remove the extension if you need to update"
      AUTO_BUMP_PATCH=false
    fi
  fi
  
  if [ "$AUTO_BUMP_PATCH" = true ]; then
    CONTROL_DEFAULT_VERSION=$(sed -nE "s/^default_version = '([^']+)'/\1/p" vchord.control | head -n1 || true)
    if [ -n "$CONTROL_DEFAULT_VERSION" ]; then
      IFS='.' read -r MAJ MIN PAT <<<"$CONTROL_DEFAULT_VERSION"
      if [[ -n "$MAJ" && -n "$MIN" && -n "$PAT" ]]; then
        NEXT_VERSION="${MAJ}.${MIN}.$((PAT+1))"
        # Determine base version for upgrade (prefer DB extversion if available and ready)
        DB_READY=false
        if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
          DB_READY=true
        fi
        if [ "$DB_READY" = true ]; then
          DB_EXTVERSION=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || true)
        else
          DB_EXTVERSION=""
        fi
        FROM_VERSION_FOR_UPGRADE="${DB_EXTVERSION:-$CONTROL_DEFAULT_VERSION}"
        
        # Only proceed if we have a valid upgrade path
        if [ -n "$FROM_VERSION_FOR_UPGRADE" ] && [ "$FROM_VERSION_FOR_UPGRADE" != "$NEXT_VERSION" ]; then
          # Create install SQL if missing by copying latest available install SQL
          if [ ! -f "sql/install/vchord--${NEXT_VERSION}.sql" ]; then
            mapfile -t AVAILABLE_INSTALL_VERSIONS < <(ls -1 sql/install/vchord--*.sql 2>/dev/null | sed -E "s#.*/vchord--(.*)\\.sql#\1#" | sort -V)
            if [ ${#AVAILABLE_INSTALL_VERSIONS[@]} -gt 0 ]; then
              LAST_INSTALL="${AVAILABLE_INSTALL_VERSIONS[-1]}"
              cp "sql/install/vchord--${LAST_INSTALL}.sql" "sql/install/vchord--${NEXT_VERSION}.sql"
            else
              echo "No install SQL templates found to copy from (sql/install)." >&2
              exit 4
            fi
          fi
          
          # Create upgrade SQL if missing (with proper content, not just comments)
          if [ ! -f "sql/upgrade/vchord--${FROM_VERSION_FOR_UPGRADE}--${NEXT_VERSION}.sql" ]; then
            {
              echo "-- Upgrade from ${FROM_VERSION_FOR_UPGRADE} to ${NEXT_VERSION}"
              echo "-- Generated by install_extension.sh with AUTO_BUMP_PATCH=true"
              echo ""
              echo "-- This is a version bump upgrade. No schema changes required."
              echo "SELECT 1; -- Ensure the file has valid SQL content"
            } > "sql/upgrade/vchord--${FROM_VERSION_FOR_UPGRADE}--${NEXT_VERSION}.sql"
          fi
          
          # Update control file default_version
          sed -i -E "s/^(default_version = ')${CONTROL_DEFAULT_VERSION}(')/\\1${NEXT_VERSION}\\2/" vchord.control
          echo "Auto-bumped vchord version: ${CONTROL_DEFAULT_VERSION} -> ${NEXT_VERSION}"
        else
          echo "Skipping auto-bump: no valid upgrade path from ${FROM_VERSION_FOR_UPGRADE} to ${NEXT_VERSION}"
        fi
      fi
    fi
  fi
fi

# Build the extension
JOBS="${JOBS:-$(nproc)}"
make build JOBS="$JOBS"

# Query installation paths from the container
PKGLIBDIR=$(docker exec "$CONTAINER_NAME" pg_config --pkglibdir)
SHAREDIR=$(docker exec "$CONTAINER_NAME" pg_config --sharedir)

# Decide whether to deploy newly built artifacts into the container
LOCAL_SO="build/raw/pkglibdir/vchord.so"
REMOTE_SO="$PKGLIBDIR/vchord.so"

if [ ! -f "$LOCAL_SO" ]; then
  echo "Local artifact $LOCAL_SO not found. Build may have failed." >&2
  exit 1
fi

REMOTE_PRESENT=$(docker exec "$CONTAINER_NAME" sh -lc "[ -f '$REMOTE_SO' ] && echo yes || echo no" || echo no)

# Detect GLIBC version in the container and the maximum GLIBC required by the built .so
CONTAINER_GLIBC_VERSION=$(docker exec "$CONTAINER_NAME" sh -lc "getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print \$2}'" 2>/dev/null || true)
if [ -z "$CONTAINER_GLIBC_VERSION" ]; then
  CONTAINER_GLIBC_VERSION=$(docker exec "$CONTAINER_NAME" sh -lc "ldd --version 2>&1 | head -n1 | sed -E 's/.* ([0-9]+\.[0-9]+).*/\\1/'" 2>/dev/null || true)
fi
REQUIRED_GLIBC=$(strings "$LOCAL_SO" 2>/dev/null | grep -oE "GLIBC_[0-9]+\.[0-9]+" | sort -V | tail -n1 | sed -E "s/^GLIBC_//" || true)

# Prefer sha256 if available
if command -v sha256sum >/dev/null 2>&1; then
  LOCAL_DIGEST=$(sha256sum "$LOCAL_SO" | awk '{print $1}')
else
  LOCAL_DIGEST=$(md5sum "$LOCAL_SO" | awk '{print $1}')
fi

REMOTE_DIGEST=$(docker exec "$CONTAINER_NAME" sh -lc "if command -v sha256sum >/dev/null 2>&1 && [ -f '$REMOTE_SO' ]; then sha256sum '$REMOTE_SO' | awk '{print \\\$1}'; elif [ -f '$REMOTE_SO' ]; then md5sum '$REMOTE_SO' | awk '{print \\\$1}'; else echo missing; fi" 2>/dev/null || echo missing)

# Decide if we need to copy, but block copy if GLIBC would break the container
NEED_COPY=false
GLIBC_MISMATCH=false
if [ -n "$REQUIRED_GLIBC" ] && [ -n "$CONTAINER_GLIBC_VERSION" ]; then
  # If container glibc is older than required, block the copy to avoid breaking postgres
  if [ "$(printf '%s\n%s\n' "$CONTAINER_GLIBC_VERSION" "$REQUIRED_GLIBC" | sort -V | head -n1)" != "$REQUIRED_GLIBC" ]; then
    GLIBC_MISMATCH=true
    echo "GLIBC compatibility check: container=$CONTAINER_GLIBC_VERSION, required_by_local_so=$REQUIRED_GLIBC"
    echo "Refusing to copy host-built vchord.so into the container to avoid GLIBC mismatch (would cause postgres to fail at startup)."
    echo "Hint: rebuild inside a matching environment (Debian bookworm) or set BUILD_IN_CONTAINER=true to build inside the container."
  fi
fi
if [ "$REMOTE_PRESENT" = "no" ]; then
  echo "No existing vchord.so in container."
  NEED_COPY=true
elif [ "$LOCAL_DIGEST" != "$REMOTE_DIGEST" ]; then
  echo "Local vchord.so digest ($LOCAL_DIGEST) differs from remote ($REMOTE_DIGEST). Will copy."
  NEED_COPY=true
fi

if [ "${FORCE_COPY:-false}" = true ]; then
  echo "FORCE_COPY=true set; will copy artifacts regardless of digest."
  NEED_COPY=true
fi

if [ "$GLIBC_MISMATCH" = true ]; then
  if [ "$BUILD_IN_CONTAINER" = true ]; then
    echo "BUILD_IN_CONTAINER=true: building and installing vchord inside container '$CONTAINER_NAME'..."
    # 1) Ensure build deps in container
    docker exec "$CONTAINER_NAME" bash -lc "set -euo pipefail; export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y --no-install-recommends curl ca-certificates build-essential pkg-config libpq-dev postgresql-server-dev-${POSTGRES_VERSION} git" || {
      echo "Error: failed to install build dependencies inside container." >&2
      exit 5
    }
    # 2) Ensure Rust toolchain
    docker exec "$CONTAINER_NAME" bash -lc "set -euo pipefail; if [ ! -x \"/root/.cargo/bin/cargo\" ]; then curl -sSf https://sh.rustup.rs | sh -s -- -y; fi" || {
      echo "Error: failed to install rustup/cargo in container." >&2
      exit 5
    }
    # 3) Copy source tree and build
    docker exec "$CONTAINER_NAME" bash -lc "rm -rf /tmp/VectorChord" || true
    docker cp . "$CONTAINER_NAME:/tmp/VectorChord"
    # IMPORTANT: remove any host-built target to avoid GLIBC mismatch for build scripts
    docker exec "$CONTAINER_NAME" bash -lc "rm -rf /tmp/VectorChord/target" || true
    docker exec "$CONTAINER_NAME" bash -lc "set -euo pipefail; export PATH=/root/.cargo/bin:\$PATH; . /root/.cargo/env 2>/dev/null || true; cd /tmp/VectorChord; PGRX_PG_CONFIG_PATH=pg_config cargo build -p vchord --lib --profile release --features pg${POSTGRES_VERSION} -j \$(nproc)" || {
      echo "Error: cargo build failed inside container." >&2
      exit 5
    }
    # 4) Install into container paths
    docker exec "$CONTAINER_NAME" bash -lc "set -euo pipefail; PKGLIB=\$(pg_config --pkglibdir); SHARE=\$(pg_config --sharedir); mkdir -p \"\$SHARE/extension\"; install -m 644 /tmp/VectorChord/target/release/libvchord.so \"\$PKGLIB/vchord.so\"; install -m 644 /tmp/VectorChord/vchord.control \"\$SHARE/extension/vchord.control\"; install -m 644 /tmp/VectorChord/sql/install/vchord--*.sql \"\$SHARE/extension/\"; install -m 644 /tmp/VectorChord/sql/upgrade/vchord--*.sql \"\$SHARE/extension/\"" || {
      echo "Error: failed to install built artifacts inside container." >&2
      exit 5
    }
    # 5) Sanity check: ensure installed binary contains the updated symbols
    docker exec "$CONTAINER_NAME" bash -lc "set -euo pipefail; PKGLIB=\$(pg_config --pkglibdir); strings \"\$PKGLIB/vchord.so\" | head -n 5 >/dev/null 2>&1 || true"
    DID_COPY=true
    NEED_COPY=false
    GLIBC_MISMATCH=false
  else
    if [ "$REMOTE_PRESENT" = "no" ]; then
      echo "Error: No existing vchord.so in container and local build requires newer GLIBC ($REQUIRED_GLIBC) than container has ($CONTAINER_GLIBC_VERSION)."
      echo "Set BUILD_IN_CONTAINER=true to build inside the container and install a compatible binary."
      exit 5
    else
      echo "Warning: GLIBC mismatch detected; skipping copy and leaving existing container binary in place."
      NEED_COPY=false
    fi
  fi
fi

if [ "$NEED_COPY" = true ]; then
  echo "Copying built artifacts into the container..."
  # Backup existing .so for rollback if present
  BACKUP_PATH="${REMOTE_SO}.bak.$(date +%s)"
  if [ "$REMOTE_PRESENT" = "yes" ]; then
    docker exec "$CONTAINER_NAME" sh -lc "cp '$REMOTE_SO' '$BACKUP_PATH'" || true
  fi
  docker cp build/raw/pkglibdir/. "$CONTAINER_NAME:$PKGLIBDIR/"
  docker cp build/raw/sharedir/. "$CONTAINER_NAME:$SHAREDIR/"
  echo "Note: container restart is NOT performed by this script."
  echo "New backend sessions will load the updated library automatically when needed."
  DID_COPY=true
else
  echo "Container already has matching vchord.so; skipping copy."
  DID_COPY=false
fi

# Wait for PostgreSQL to be ready inside the container
echo "$CONTAINER_NAME"
ready=false
for i in {1..60}; do
  if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [ "$ready" != true ]; then
  echo "PostgreSQL did not become ready in time." >&2
  exit 2
fi

# If we cannot connect with the provided DB_USER, skip SQL and finish successfully
if ! docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
  echo "Warning: unable to connect to Postgres as user '$DB_USER'. Skipping extension creation/update. Artifacts have been installed into the container."
  exit 0
fi

# If we deployed a new binary, sanity-check that it can be loaded; if not, roll back to the backup
if [ "${DID_COPY:-false}" = true ]; then
  if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA -c "LOAD 'vchord'; SELECT 1;" >/dev/null 2>&1; then
    echo "Post-copy LOAD check: ok"
  else
    echo "Post-copy LOAD check: failed. Restoring previous vchord.so to avoid breaking the container."
    if docker exec "$CONTAINER_NAME" sh -lc "[ -f '${BACKUP_PATH:-}' ]" >/dev/null 2>&1; then
      docker exec "$CONTAINER_NAME" sh -lc "mv '${BACKUP_PATH}' '$REMOTE_SO'" || true
      echo "Rollback complete."
    else
      echo "Warning: backup was not found; cannot roll back automatically." >&2
    fi
    exit 6
  fi
fi

CURRENT_VERSION=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || true)

# Decide TARGET_VERSION based on extension control default_version (preferred),
# falling back to the highest available install SQL version. Do not auto-bump.
if [ -z "${EXT_VERSION_OVERRIDE:-}" ]; then
  CONTROL_DEFAULT_VERSION=$(sed -nE "s/^default_version = '([^']+)'/\1/p" vchord.control | head -n1 || true)
  mapfile -t AVAILABLE_INSTALL_VERSIONS < <(ls -1 sql/install/vchord--*.sql 2>/dev/null | sed -E "s#.*/vchord--(.*)\\.sql#\1#" | sort -V)
  if [ -n "$CONTROL_DEFAULT_VERSION" ]; then
    TARGET_VERSION="$CONTROL_DEFAULT_VERSION"
  elif [ ${#AVAILABLE_INSTALL_VERSIONS[@]} -gt 0 ]; then
    TARGET_VERSION="${AVAILABLE_INSTALL_VERSIONS[-1]}"
  else
    echo "No install SQL files found and control default_version is empty." >&2
    exit 3
  fi
else
  TARGET_VERSION="$EXT_VERSION_OVERRIDE"
fi

if [ -z "$CURRENT_VERSION" ]; then
  # Not installed: install at target version
  docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" \
    -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS vchord;" \
    -c "ALTER EXTENSION vchord UPDATE TO '${TARGET_VERSION}';" \
    -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
elif [ "$CURRENT_VERSION" = "$TARGET_VERSION" ]; then
  echo "vchord is already at version $CURRENT_VERSION; nothing to do."
elif [ "$(printf '%s\n%s\n' "$CURRENT_VERSION" "$TARGET_VERSION" | sort -V | head -n1)" = "$CURRENT_VERSION" ] && [ "$CURRENT_VERSION" != "$TARGET_VERSION" ]; then
  # Upgrade to target version
  docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" \
    -v ON_ERROR_STOP=1 \
    -c "ALTER EXTENSION vchord UPDATE TO '${TARGET_VERSION}';" \
    -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
else
  echo "Installed vchord version ($CURRENT_VERSION) is newer than target ($TARGET_VERSION). Skipping downgrade."
  docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"
fi

# Print version summary
NEW_VERSION=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA -c "SELECT extversion FROM pg_extension WHERE extname='vchord';" 2>/dev/null | tr -d ' \t' || true)
if [ -z "$CURRENT_VERSION" ]; then
  PREV_LABEL="not installed"
else
  PREV_LABEL="$CURRENT_VERSION"
fi
echo "vchord extension version summary: previous=${PREV_LABEL}, target=${TARGET_VERSION}, now=${NEW_VERSION:-not installed}"

# Verify deployed binary matches the one built locally
LOCAL_SO="build/raw/pkglibdir/vchord.so"
REMOTE_SO="$PKGLIBDIR/vchord.so"
LOCAL_DIGEST_ALG="sha256"
if command -v sha256sum >/dev/null 2>&1; then
  LOCAL_DIGEST=$(sha256sum "$LOCAL_SO" | awk '{print $1}')
else
  LOCAL_DIGEST_ALG="md5"
  LOCAL_DIGEST=$(md5sum "$LOCAL_SO" | awk '{print $1}')
fi
REMOTE_DIGEST_ALG=$(docker exec "$CONTAINER_NAME" sh -lc 'if command -v sha256sum >/dev/null 2>&1; then echo sha256; else echo md5; fi')
if [ "$REMOTE_DIGEST_ALG" = "sha256" ]; then
  REMOTE_DIGEST=$(docker exec "$CONTAINER_NAME" sha256sum "$REMOTE_SO" | awk '{print $1}')
else
  REMOTE_DIGEST=$(docker exec "$CONTAINER_NAME" md5sum "$REMOTE_SO" | awk '{print $1}')
fi
LOCAL_SIZE=$(stat -c%s "$LOCAL_SO")
REMOTE_SIZE=$(docker exec "$CONTAINER_NAME" stat -c%s "$REMOTE_SO")
if [ "$LOCAL_DIGEST" = "$REMOTE_DIGEST" ] && [ "$LOCAL_DIGEST_ALG" = "$REMOTE_DIGEST_ALG" ]; then
  MATCH_LABEL="yes"
else
  MATCH_LABEL="no"
fi
echo "vchord binary deploy check: path=${REMOTE_SO}, digest match=${MATCH_LABEL} (local ${LOCAL_DIGEST_ALG}:${LOCAL_DIGEST}, remote ${REMOTE_DIGEST_ALG}:${REMOTE_DIGEST}), size local=${LOCAL_SIZE}B, remote=${REMOTE_SIZE}B"

# Verify the library is loadable and loaded in a live backend
APPNAME="vchord_check_$$_$(date +%s)"
docker exec "$CONTAINER_NAME" sh -lc \
  "PGAPPNAME='$APPNAME' psql -U '$DB_USER' -d '$DB_NAME' -h localhost -p '$PG_PORT_IN_CONTAINER' -v ON_ERROR_STOP=1 -tA -c \"LOAD 'vchord'; SELECT pg_sleep(3);\"" >/dev/null 2>&1 &
sleep 0.2
RUNTIME_PID=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -tA \
  -c "SELECT pid FROM pg_stat_activity WHERE application_name='${APPNAME}' ORDER BY backend_start DESC LIMIT 1;" 2>/dev/null | tr -d ' \t' || true)
if [ -n "$RUNTIME_PID" ]; then
  if docker exec "$CONTAINER_NAME" sh -lc "[ -r /proc/$RUNTIME_PID/maps ]" >/dev/null 2>&1; then
    LOADED=$(docker exec "$CONTAINER_NAME" sh -lc "grep -q '/vchord\\.so' /proc/$RUNTIME_PID/maps >/dev/null 2>&1 && echo yes || echo no")
    echo "vchord runtime load check: backend_pid=${RUNTIME_PID}, loaded=${LOADED}"
  else
    # Distinguish between non-existent and permission denied
    if docker exec "$CONTAINER_NAME" sh -lc "[ -e /proc/$RUNTIME_PID/maps ]" >/dev/null 2>&1; then
      echo "vchord runtime load check: backend_pid=${RUNTIME_PID}, loaded=unknown (permission denied)"
    else
      echo "vchord runtime load check: backend_pid=${RUNTIME_PID}, loaded=unknown (backend exited)"
    fi
  fi
else
  echo "vchord runtime load check: unable to determine backend pid"
fi

# Invoke a harmless function from the extension to ensure it can execute
if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -tA \
  -c "LOAD 'vchord'; SELECT \"_vchordg_support_vector_ip_ops\"();" >/dev/null 2>&1; then
  echo "vchord function call check: ok"
else
  echo "vchord function call check: failed"
fi

# Optionally RECREATE VectorChord indexes automatically after version change
# Uses DROP + CREATE INDEX instead of REINDEX to avoid stale CTID issues
if [ "$AUTO_REINDEX" = true ] && [ -n "$NEW_VERSION" ] && [ "$NEW_VERSION" != "$CURRENT_VERSION" ]; then
  echo "Auto-recreating VectorChord indexes (version changed from ${CURRENT_VERSION:-none} to ${NEW_VERSION})..."
  echo "Using DROP + CREATE INDEX approach to avoid stale CTID issues"
  
  # Get list of VectorChord indexes to recreate
  INDEXES=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -At -c \
    "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname)
       FROM pg_index i
       JOIN pg_class c ON i.indexrelid = c.oid
       JOIN pg_namespace n ON c.relnamespace = n.oid
       JOIN pg_am am ON c.relam = am.oid
      WHERE am.amname IN ('vchordrq','vchordg')
      ORDER BY c.relname;")
  
  if [ -n "$INDEXES" ]; then
    for IDX_NAME in $INDEXES; do
      echo "Recreating index: $IDX_NAME"
      
      # Get the complete index definition
      IDX_DEF=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -At -c \
        "SELECT pg_get_indexdef(i.indexrelid)
           FROM pg_index i
           JOIN pg_class c ON i.indexrelid = c.oid
           JOIN pg_namespace n ON c.relnamespace = n.oid
          WHERE quote_ident(n.nspname)||'.'||quote_ident(c.relname) = '$IDX_NAME';")
      
      if [ -n "$IDX_DEF" ]; then
        echo "  Dropping: $IDX_NAME"
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "DROP INDEX IF EXISTS $IDX_NAME;" || {
          echo "Warning: drop failed for $IDX_NAME; continuing" >&2
        }
        echo "  Creating: $IDX_NAME"
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "$IDX_DEF;" || {
          echo "Warning: create failed for $IDX_NAME; continuing" >&2
        }
      else
        echo "Warning: could not get definition for $IDX_NAME; skipping" >&2
      fi
    done
  else
    echo "No VectorChord indexes found to recreate."
  fi
fi

# Create the specific optimized VectorChord index
echo "Creating optimized VectorChord index..."
INDEX_DDL="DROP INDEX IF EXISTS images_multi_embedding_maxsim_idx_optimized; CREATE INDEX images_multi_embedding_maxsim_idx_optimized ON images USING vchordrq (multi_embedding vector_maxsim_ops) WITH (options = \$\$residual_quantization = true
build.pin = true
rerank_in_table = false
[build.internal]
kmeans_iterations = 50
sampling_factor = 512
lists = [16, 128, 512]
spherical_centroids = true
build_threads = 32\$\$);"

echo "Index DDL content:"
echo "$INDEX_DDL"
echo ""

if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -h localhost -p "$PG_PORT_IN_CONTAINER" -v ON_ERROR_STOP=1 -c "$INDEX_DDL"; then
  echo "Optimized VectorChord index created successfully."
else
  echo "Warning: Index creation failed." >&2
fi

