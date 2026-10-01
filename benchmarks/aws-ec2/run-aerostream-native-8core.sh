#!/usr/bin/env bash
# AeroStream NATIVE protocol (port 9091), OpenMessaging Benchmark on ONE 8 vCPU / 16 GiB machine (c6id.2xlarge, 4 physical cores):
#   physical cores 0-1 -> broker container (8 GiB), physical cores 2-3 -> OMB load generator (pinned with taskset)
#   8 producers x 8 consumers x 32 partitions, 1 KB messages: max-rate + fixed 100k and 200k msg/s, 2 rounds
# Uses the driver in benchmarks/omb-driver-aerostream (built into OMB on the machine; no fork or pull request needed).
# Collects metrics, copies everything back with scp and destroys the machine.
#   ./run-aerostream-native-8core.sh          plan + estimate only        ./run-aerostream-native-8core.sh --yes     run it
#   AERO_IMAGE=<local image tag>              exact image to test (default: aerostream:append-entries, the one pushed as :latest)
export TOPOLOGY=single PROFILE=native8 SYSTEMS=aerostream-native AERO_IMAGE="${AERO_IMAGE:-aerostream:append-entries}"
exec "$(cd "$(dirname "$0")" && pwd)/run-e2e.sh" "$@"
