#!/usr/bin/env bash
# Build the AeroStream controller + broker images and the native client from a source tree.
#   usage: build-aerostream.sh <tag> <repo-dir>      e.g.  build-aerostream.sh main /path/to/checkout-of-main
set -euo pipefail
TAG="${1:?tag (main|integration)}"; SRC="${2:?repo dir}"
docker build -f "$SRC/Dockerfile.controller" -t "aerostream-controller:$TAG" "$SRC"
docker build -f "$SRC/Dockerfile.broker" -t "aerostream-broker:$TAG" "$SRC"
mkdir -p "$SRC/client/bin" && (cd "$SRC/client" && go build -o bin/client .)
echo "built aerostream-{controller,broker}:$TAG and $SRC/client/bin/client"
