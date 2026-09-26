#!/usr/bin/env bash
# Run every workload RUNS times against one system and keep the raw tool output.
#   usage: run-workloads.sh <kafka|redpanda|aerostream|aerostream-kafka> <log-dir>
# Kafka / Redpanda : kafka-producer-perf-test.sh (apache/kafka image), acks=1
# AeroStream       : the repo's `client bench` (native data-plane protocol)
# aerostream-kafka : kafka-producer-perf-test.sh against AeroStream's Kafka port (what Kafka clients use)
set -euo pipefail; source "$(dirname "$0")/env.sh"
SYS="$1"; OUT="$2"; mkdir -p "$OUT"
D="$(dirname "$0")"
for w in "${WORKLOADS[@]}"; do
  read -r LABEL SIZE RECORDS PRODUCERS MSGS EXTRA <<<"$w"
  [[ -z "${ONLY:-}" || " $ONLY " == *" $LABEL "* ]] || continue     # ONLY="1KB 50MB" limits the workloads
  for run in $(seq 1 "$RUNS"); do
    TOPIC="bench-${SYS}-${LABEL,,}-r${run}"
    LOG="$OUT/${SYS}-${LABEL}-run${run}.log"
    "$D/create-topic.sh" "$SYS" "$TOPIC" >/dev/null 2>&1 || { echo "topic create failed for $TOPIC" | tee "$LOG"; continue; }
    echo "[$SYS] $LABEL run $run" >&2
    case "$SYS" in
      kafka|redpanda|aerostream-kafka)
        BOOT=$KAFKA_BOOTSTRAP; [ "$SYS" = redpanda ] && BOOT=$REDPANDA_BOOTSTRAP; [ "$SYS" = aerostream-kafka ] && BOOT=$AERO_KAFKA_BOOTSTRAP
        docker run --rm --network host "$KAFKA_IMAGE" /opt/kafka/bin/kafka-producer-perf-test.sh \
          --topic "$TOPIC" --num-records "$RECORDS" --record-size "$SIZE" --throughput -1 \
          --producer-props bootstrap.servers="$BOOT" acks=1 max.request.size=$MAX_MSG $EXTRA >"$LOG" 2>&1 || true ;;
      aerostream)
        "$AERO_CLIENT" -controller "$AERO_CONTROLLER_GRPC" bench \
          -topic "$TOPIC" -partition 0 -size "$SIZE" -producers "$PRODUCERS" -messages "$MSGS" >"$LOG" 2>&1 || true ;;
    esac
  done
done
