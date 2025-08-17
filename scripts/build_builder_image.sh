#!/usr/bin/env bash
# Simple script to build the VectorChord builder Docker image

set -euo pipefail

# Configuration
BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-vchord-builder:latest}"
DOCKERFILE_PATH="docker/Dockerfile.build"

echo "Building VectorChord builder Docker image..."

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE_PATH" ]; then
    echo "Error: Dockerfile not found at $DOCKERFILE_PATH" >&2
    exit 1
fi

# Build the image
docker build -f "$DOCKERFILE_PATH" -t "$BUILD_IMAGE_NAME" docker/

if [ $? -eq 0 ]; then
    echo "Successfully built builder image: $BUILD_IMAGE_NAME"
else
    echo "Error: Failed to build builder image" >&2
    exit 1
fi
