#!/usr/bin/env bash
# Build and install the VectorChord PostgreSQL extension inside a Docker container.
# The script is idempotent and may be run multiple times safely.

set -euo pipefail

# Configuration variables (override via environment variables)
POSTGRES_VERSION="${POSTGRES_VERSION:-17}"
VECTORCHORD_VERSION="${VECTORCHORD_VERSION:-v0.4.3}"
CONTAINER_NAME="${CONTAINER_NAME:-vchord_listings}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/tensorchord/vchord-postgres:pg${POSTGRES_VERSION}-${VECTORCHORD_VERSION}}"
PATH_TO_POSTGRES_DATA="${PATH_TO_POSTGRES_DATA:-$(realpath "$(pwd)/../postgres_data")}" 
DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_PORT="${DB_PORT:-5432}"
REPO_URL="${REPO_URL:-https://github.com/tensorchord/VectorChord.git}"

# Ensure the container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  docker run -d \
    --name "$CONTAINER_NAME" \
    --cpus=32 --cpuset-cpus=0-31 \
    -e POSTGRES_DB="$DB_NAME" \
    -e POSTGRES_USER="$DB_USER" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -p "$DB_PORT":5432 \
    -v "$PATH_TO_POSTGRES_DATA":/var/lib/postgresql/data \
    --restart unless-stopped \
    --memory=28g \
    --shm-size=4g \
    "$IMAGE_REPO"
else
  docker start "$CONTAINER_NAME" >/dev/null
fi

# Clone the repository if this script is run outside of it
if [ ! -d .git ]; then
  git clone "$REPO_URL" VectorChord
  cd VectorChord
else
  cd "$(git rev-parse --show-toplevel)"
fi

# Optionally override extension version (e.g., EXT_VERSION_OVERRIDE=1.0.1)
if [ -n "${EXT_VERSION_OVERRIDE:-}" ]; then
  sed -i "s/^default_version = .*/default_version = '${EXT_VERSION_OVERRIDE}'/" vchord.control
fi

# Install build dependencies
sudo apt-get update
sudo apt-get install -y build-essential libpq-dev postgresql-server-dev-${POSTGRES_VERSION} rustc cargo

# Build the extension
make build

# Query installation paths from the container
PKGLIBDIR=$(docker exec "$CONTAINER_NAME" pg_config --pkglibdir)
SHAREDIR=$(docker exec "$CONTAINER_NAME" pg_config --sharedir)

# Copy built artifacts into the container
docker cp build/raw/pkglibdir/. "$CONTAINER_NAME:$PKGLIBDIR/"
docker cp build/raw/sharedir/. "$CONTAINER_NAME:$SHAREDIR/"

# Restart the container
docker restart "$CONTAINER_NAME"

# Create or update the extension and show its version
docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" \
  -c "CREATE EXTENSION IF NOT EXISTS vchord;" \
  -c "ALTER EXTENSION vchord UPDATE;" \
  -c "SELECT extname, extversion FROM pg_extension WHERE extname='vchord';"

