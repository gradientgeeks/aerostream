#!/usr/bin/env bash
# Shared settings for the comparison benchmark. Source this file; do not run it.

# Identical hardware limits for every broker container.
export LIMITS="${LIMITS:---cpus=2.0 --memory=2g}"

export KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:latest}"
export REDPANDA_IMAGE="${REDPANDA_IMAGE:-redpandadata/redpanda:latest}"

# AeroStream images are built from the code under test (see COMMANDS.md): TAG is "main" or "integration".
export AERO_TAG="${AERO_TAG:-quay}"
# AERO_TAG=quay uses the published all-in-one image; any other tag uses a local all-in-one build `aerostream:<tag>`
# (docker build -t aerostream:<tag> .). Both binaries are in that image; start-aerostream.sh selects them by entrypoint.
if [ "$AERO_TAG" = quay ]; then _AERO_IMG=quay.io/gradientgeeks/aerostream:latest; else _AERO_IMG="aerostream:${AERO_TAG}"; fi
export AERO_BROKER_IMAGE="${AERO_BROKER_IMAGE:-$_AERO_IMG}"
export AERO_CONTROLLER_IMAGE="${AERO_CONTROLLER_IMAGE:-$_AERO_IMG}"
export AERO_CLIENT="${AERO_CLIENT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)/client/bin/client}"

# Container names.
export KAFKA_NAME=bench-kafka REDPANDA_NAME=bench-redpanda AERO_NAME=bench-aerostream AERO_CTL_NAME=bench-aerostream-controller

# Every container uses host networking (no docker-proxy in the data path). Ports are chosen not to clash
# with a locally running AeroStream (7001-7003, 8001-8003, 9001-9003, 9091-9093).
export KAFKA_BOOTSTRAP=localhost:9094            # external listener used by the load generator
export KAFKA_ADMIN_BOOTSTRAP=localhost:19192     # internal listener used by kafka-topics.sh
export REDPANDA_BOOTSTRAP=localhost:19092
export AERO_CONTROLLER_GRPC=127.0.0.1:28001      # dedicated benchmark controller (NOT the one on :8001)
export AERO_CONTROLLER_HTTP=127.0.0.1:29001
export AERO_KAFKA_BOOTSTRAP=localhost:9096        # AeroStream Kafka-protocol port (driven with kafka-producer-perf-test)

# 64 MiB message limit so every payload up to 50 MiB fits.
export MAX_MSG=67108864

# Workloads: label size_bytes kafka_records aero_producers aero_messages_per_producer kafka_extra_props
# Typical production sizes (1 KB .. 1 MB, Kafka's default max message size) plus 10 MB. 10 KB and up move 500 MB per run; 1 KB moves 50 MB.
# Large-message tests move 500 MB in total; small-message tests match the earlier benchmark (100k x 100 B, 50k x 1 KB).
export WORKLOADS=(
  "1KB 1024 50000 10 5000 "
  "10KB 10240 50000 10 5000 "
  "50KB 51200 10000 10 1000 "
  "100KB 102400 5000 10 500 "
  "250KB 256000 2000 10 200 "
  "500KB 512000 1000 10 100 "
  "1MB 1048576 500 10 50 send.buffer.bytes=4194304 receive.buffer.bytes=4194304"
  "10MB 10485760 50 10 5 buffer.memory=268435456 send.buffer.bytes=4194304 receive.buffer.bytes=4194304"
)
export RUNS="${RUNS:-3}"
