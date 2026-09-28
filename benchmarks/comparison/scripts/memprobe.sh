#!/usr/bin/env bash
# Diagnose throughput stalls: run the native 1 MB workload N times against a broker under a given memory limit and print,
# per run, throughput, max latency and the broker cgroup's reclaim / dirty-page counters (memory.stat, memory.events).
#   usage: memprobe.sh <memory-limit e.g. 2g|8g> [runs=4] [size=1048576] [messages-per-producer=50]
set -uo pipefail; D="$(cd "$(dirname "$0")" && pwd)"; source "$D/env.sh"
MEM="${1:?memory limit}"; N="${2:-4}"; SIZE="${3:-1048576}"; MSGS="${4:-50}"
export LIMITS="--cpus=2.0 --memory=$MEM"
"$D/cleanup.sh" >/dev/null; "$D/start-aerostream.sh" >/dev/null || exit 1
stat() { docker exec "$AERO_NAME" cat /sys/fs/cgroup/memory.stat /sys/fs/cgroup/memory.events 2>/dev/null \
  | awk '$1~/^(file|file_dirty|file_writeback|pgscan_direct|pgsteal_direct|pgscan_kswapd|high|max)$/{printf "%s=%s ",$1,$2}'; }
printf 'limit %s | before: %s\n' "$MEM" "$(stat)"
for i in $(seq 1 "$N"); do
  T="probe-$i"; "$AERO_CLIENT" -controller "$AERO_CONTROLLER_GRPC" create-topic "$T" 1 1 >/dev/null
  OUT=$("$AERO_CLIENT" -controller "$AERO_CONTROLLER_GRPC" bench -topic "$T" -partition 0 -size "$SIZE" -producers 10 -messages "$MSGS" 2>&1)
  mb=$(echo "$OUT" | grep -m1 "Data Throughput" | sed -E 's/.*: *([0-9.,]+) MB.*/\1/'); mx=$(echo "$OUT" | grep -m1 "Max Latency" | sed -E 's/.*: *//')
  printf 'run %s: %8s MB/s  max %-10s | %s\n' "$i" "$mb" "$mx" "$(stat)"
done
"$D/cleanup.sh" >/dev/null
