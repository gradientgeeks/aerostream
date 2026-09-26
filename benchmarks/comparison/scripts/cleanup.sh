#!/usr/bin/env bash
# Remove every benchmark container.
docker rm -f bench-kafka bench-redpanda bench-aerostream bench-aerostream-controller >/dev/null 2>&1 || true
echo "benchmark containers removed"
