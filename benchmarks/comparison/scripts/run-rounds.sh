#!/usr/bin/env bash
# Fair size-by-size comparison: 8 sizes (100 B .. 1 MB) x 4 systems = 32 tests.
#
# For each size, every system is tested once (Apache Kafka, Redpanda, AeroStream native, AeroStream Kafka port). The order
# of systems rotates from size to size so no system is always first or last. Every test:
#   1. waits until the disk has recovered: at least MIN_SLEEP seconds, then the disk writes < IDLE_MBPS for IDLE_SECS
#      in a row, and the NVMe temperature <= MAX_TEMP (SSD write caches refill and heat drains while the disk is idle),
#   2. starts a fresh broker container (fresh data volume),
#   3. runs the workload RUNS times, each on a new topic,
#   4. removes the broker and its data volume.
#
# usage:  scripts/run-rounds.sh <result-label>
# env:    RUNS=3  RUN_MB=200  MIN_SLEEP=30  IDLE_SECS=60  IDLE_MBPS=5  MAX_TEMP=45  DISK=nvme0n1  AERO_TAG=wb
#         SIZES="100B 1KB 10KB 50KB 100KB 250KB 500KB 1MB"  SYSTEMS="kafka redpanda aerostream aerostream-kafka"
#         FSTRIM=1   runs `sudo fstrim -v /` once before starting (asks for your password)
# output: results/<label>/  same files as run-all.sh (raw logs, *-summary.tsv, *-disk-per-run.tsv) + report.md + rounds.log
set -uo pipefail
D="$(cd "$(dirname "$0")" && pwd)"; source "$D/env.sh"
LABEL="${1:?result label, e.g. 2026-09-27-rounds}"
RUNS="${RUNS:-3}"; RUN_MB="${RUN_MB:-200}"; MIN_SLEEP="${MIN_SLEEP:-30}"; IDLE_SECS="${IDLE_SECS:-60}"
IDLE_MBPS="${IDLE_MBPS:-5}"; MAX_TEMP="${MAX_TEMP:-45}"; DISK="${DISK:-nvme0n1}"
read -ra SIZES <<<"${SIZES:-100B 1KB 10KB 50KB 100KB 250KB 500KB 1MB}"
read -ra SYSTEMS <<<"${SYSTEMS:-kafka redpanda aerostream aerostream-kafka}"
OUT="$D/../results/$LABEL"; mkdir -p "$OUT"
LOG="$OUT/rounds.log"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

bytes_of() { case "$1" in *MB) echo $(( ${1%MB} * 1048576 ));; *KB) echo $(( ${1%KB} * 1024 ));; *B) echo "${1%B}";; esac; }
# extra kafka-producer-perf-test properties per size, from env.sh WORKLOADS (e.g. socket buffers for 1 MB)
extra_of() { for w in "${WORKLOADS[@]}"; do read -r l _ _ _ _ e <<<"$w"; [ "$l" = "$1" ] && { echo "$e"; return; }; done; }
nvme_temp() { for h in /sys/class/hwmon/hwmon*; do [ "$(cat "$h/name" 2>/dev/null)" = nvme ] && { echo $(( $(cat "$h/temp1_input") / 1000 )); return; }; done; echo 0; }
disk_sectors() { awk -v d="$DISK" '$3==d {print $10}' /proc/diskstats; }

wait_for_disk() {
  sync
  log "  waiting: min ${MIN_SLEEP}s, then disk < ${IDLE_MBPS} MB/s for ${IDLE_SECS}s and SSD <= ${MAX_TEMP}C"
  sleep "$MIN_SLEEP"
  local quiet=0 prev cur mbps t waited=$MIN_SLEEP
  prev=$(disk_sectors)
  while :; do
    sleep 5; waited=$((waited + 5))
    cur=$(disk_sectors); mbps=$(( (cur - prev) * 512 / 1048576 / 5 )); prev=$cur; t=$(nvme_temp)
    if [ "$mbps" -lt "$IDLE_MBPS" ] && [ "$t" -le "$MAX_TEMP" ]; then quiet=$((quiet + 5)); else quiet=0; fi
    [ "$quiet" -ge "$IDLE_SECS" ] && break
    [ $((waited % 60)) -eq 0 ] && log "    ...still waiting (${waited}s): disk ${mbps} MB/s, SSD ${t}C"
  done
  log "  disk recovered after ${waited}s (SSD $(nvme_temp)C)"
}

start_system() {
  "$D/cleanup.sh" >/dev/null
  case "$1" in
    kafka)    "$D/start-kafka.sh" ;;
    redpanda) "$D/start-redpanda.sh" ;;
    aerostream|aerostream-kafka) "$D/start-aerostream.sh" ;;
  esac
}
container_of() { case "$1" in kafka) echo "$KAFKA_NAME";; redpanda) echo "$REDPANDA_NAME";; *) echo "$AERO_NAME";; esac; }

run_test() {  # $1 system  $2 size label
  local SYS=$1 L=$2 SIZE RECORDS EXTRA MSGS BOOT
  SIZE=$(bytes_of "$L")
  RECORDS=$(( RUN_MB * 1048576 / SIZE )); [ "$RECORDS" -gt 200000 ] && RECORDS=200000   # small sizes: cap message count
  MSGS=$(( (RECORDS + 9) / 10 ))                                                         # native client: 10 producers
  EXTRA=$(extra_of "$L")
  start_system "$SYS" > >(sed 's/^/    /' | tee -a "$LOG") 2>&1 || { log "  $SYS failed to start"; return; }
  if [ ! -f "$OUT/$SYS-idle.txt" ]; then   # idle footprint, once per system (first test)
    sleep 10; docker stats --no-stream --format '{{.MemUsage}} pids={{.PIDs}}' "$(container_of "$SYS")" | sed 's/^/idle: /' > "$OUT/$SYS-idle.txt"
  fi
  "$D/stats.sh" "$(container_of "$SYS")" "$OUT/$SYS-$L-stats.csv" & local STATS=$!
  for r in $(seq 1 "$RUNS"); do
    local T="bench-${SYS}-${L,,}-r$r" LOGF="$OUT/$SYS-$L-run$r.log"
    "$D/create-topic.sh" "$SYS" "$T" >/dev/null 2>&1 || { log "  topic create failed: $T"; continue; }
    local T0; T0=$(date +%s.%N)
    case "$SYS" in
      kafka|redpanda|aerostream-kafka)
        BOOT=$KAFKA_BOOTSTRAP; [ "$SYS" = redpanda ] && BOOT=$REDPANDA_BOOTSTRAP; [ "$SYS" = aerostream-kafka ] && BOOT=$AERO_KAFKA_BOOTSTRAP
        docker run --rm --network host -e KAFKA_HEAP_OPTS="-Xmx2g" "$KAFKA_IMAGE" /opt/kafka/bin/kafka-producer-perf-test.sh \
          --topic "$T" --num-records "$RECORDS" --record-size "$SIZE" --throughput -1 \
          --producer-props bootstrap.servers="$BOOT" acks=1 max.request.size=$MAX_MSG $EXTRA >"$LOGF" 2>&1 ;;
      aerostream)
        "$AERO_CLIENT" -controller "$AERO_CONTROLLER_GRPC" bench -topic "$T" -partition 0 -size "$SIZE" -producers 10 -messages "$MSGS" >"$LOGF" 2>&1 ;;
    esac
    printf '%s\trun%s\t%s\t%s\n' "$L" "$r" "$T0" "$(date +%s.%N)" >>"$OUT/$SYS-timeline.tsv"
    log "  $SYS $L run $r: $(grep -hE 'records sent|Data Throughput' "$LOGF" | tail -1 | sed -E 's/.*\(([0-9.]+ MB\/sec)\).*/\1/; s/.*: *([0-9.,]+ MB\/sec).*/\1/')"
  done
  touch "$OUT/$SYS-$L-stats.csv.stop"; wait "$STATS" 2>/dev/null; rm -f "$OUT/$SYS-$L-stats.csv.stop"
  "$D/cleanup.sh" >/dev/null
}

# ------------------------------------------------------------------------------------------------------------------------
[ "${FSTRIM:-0}" = 1 ] && { log "fstrim (sudo)"; sudo fstrim -v / | tee -a "$LOG"; }
TOTAL=$(( ${#SIZES[@]} * ${#SYSTEMS[@]} )); N=0
log "start: ${#SIZES[@]} sizes x ${#SYSTEMS[@]} systems = $TOTAL tests, $RUNS runs each, ~${RUN_MB} MB per run; results -> $OUT"
"$D/diskstats.sh" "$DISK" "$OUT/session-disk.csv" & DISKP=$!
trap 'touch "$OUT/session-disk.csv.stop"; "$D/cleanup.sh" >/dev/null; log "interrupted"; exit 130' INT TERM

for i in "${!SIZES[@]}"; do
  L=${SIZES[$i]}
  for j in "${!SYSTEMS[@]}"; do
    SYS=${SYSTEMS[$(( (i + j) % ${#SYSTEMS[@]} ))]}     # rotate the start system every size
    N=$((N + 1))
    log "=== test $N/$TOTAL: $L on $SYS"
    wait_for_disk
    run_test "$SYS" "$L"
  done
done

touch "$OUT/session-disk.csv.stop"; wait "$DISKP" 2>/dev/null; rm -f "$OUT/session-disk.csv.stop"
for SYS in "${SYSTEMS[@]}"; do
  ln -sf session-disk.csv "$OUT/$SYS-disk.csv"
  "$D/disk-report.sh" "$OUT" "$SYS" > "$OUT/$SYS-disk-per-run.tsv"
  "$D/summarize.sh" "$OUT" "$SYS" >/dev/null
  cat "$OUT"/$SYS-*-stats.csv 2>/dev/null | awk 'NR==1 || !/^time/' > "$OUT/$SYS-stats.csv"
  "$D/peaks.sh" "$OUT/$SYS-stats.csv" > "$OUT/$SYS-peaks.txt"
done
"$D/report.sh" "$OUT" > "$OUT/report.md" 2>/dev/null
log "done: $TOTAL tests. Report: $OUT/report.md"
cat "$OUT/report.md"
