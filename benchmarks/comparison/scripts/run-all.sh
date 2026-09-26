#!/usr/bin/env bash
# One full session: start each system in turn, run all workloads while sampling docker stats, store everything under
# results/<label>/.   usage: run-all.sh <label> [system ...]     (systems default to: kafka redpanda aerostream; also: aerostream-kafka)
#   ONLY="1KB 50MB" limits the workloads; RUNS=n sets runs per workload.
#   AERO_TAG=main|integration selects which AeroStream images are used.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"; source "$D/env.sh"
LABEL="${1:?result label, e.g. 2026-09-26-integration}"; shift || true
SYSTEMS=("${@:-kafka redpanda aerostream}"); read -ra SYSTEMS <<<"${SYSTEMS[*]}"
OUT="$D/../results/$LABEL"; mkdir -p "$OUT"
for SYS in "${SYSTEMS[@]}"; do
  "$D/cleanup.sh" >/dev/null
  case "$SYS" in
    kafka)      "$D/start-kafka.sh";      CONTAINER=$KAFKA_NAME ;;
    redpanda)   "$D/start-redpanda.sh";   CONTAINER=$REDPANDA_NAME ;;
    aerostream|aerostream-kafka) "$D/start-aerostream.sh"; CONTAINER=$AERO_NAME ;;
  esac
  sleep 10                                                    # let start-up work settle
  docker stats --no-stream --format '{{.MemUsage}} pids={{.PIDs}}' "$CONTAINER" | sed 's/^/idle: /' | tee "$OUT/$SYS-idle.txt"
  "$D/stats.sh" "$CONTAINER" "$OUT/$SYS-stats.csv" & STATS=$!
  "$D/run-workloads.sh" "$SYS" "$OUT"
  touch "$OUT/$SYS-stats.csv.stop"; wait "$STATS" || true
  "$D/peaks.sh" "$OUT/$SYS-stats.csv" | tee "$OUT/$SYS-peaks.txt"
  "$D/summarize.sh" "$OUT" "$SYS" >/dev/null
done
"$D/cleanup.sh"
echo "results in $OUT"
