#!/bin/bash
set -euo pipefail

IMAGE="ghcr.io/charles-001/dolphie"
VERSION=$(sed -n 's/^version = "\([^"]*\)"/\1/p' pyproject.toml)

if [ -z "$VERSION" ]; then
    echo "Error: Could not read version from pyproject.toml"
    exit 1
fi

echo "Building and pushing $IMAGE:$VERSION"

docker buildx build \
    --platform=linux/amd64 \
    --no-cache \
    --push \
    -t "$IMAGE:$VERSION" \
    -t "$IMAGE:latest" \
    .

echo "Done! Pushed $IMAGE:$VERSION and $IMAGE:latest"
