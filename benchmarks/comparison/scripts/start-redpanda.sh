#!/usr/bin/env bash
# Redpanda (single node) under the benchmark limits.
set -euo pipefail; source "$(dirname "$0")/env.sh"
docker rm -f "$REDPANDA_NAME" >/dev/null 2>&1 || true
docker run -d --name "$REDPANDA_NAME" $LIMITS --network host "$REDPANDA_IMAGE" \
  redpanda start \
  --smp 2 --memory 1500M --reserve-memory 0M --overprovisioned \
  --node-id 0 \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092 \
  --set redpanda.kafka_batch_max_bytes=$MAX_MSG \
  --set redpanda.kafka_request_max_bytes=104857600 >/dev/null
for i in $(seq 1 60); do
  docker exec "$REDPANDA_NAME" rpk cluster info -X brokers=127.0.0.1:19092 >/dev/null 2>&1 && { echo "redpanda ready"; exit 0; }
  sleep 1
done
echo "redpanda did not become ready" >&2; docker logs --tail 20 "$REDPANDA_NAME" >&2; exit 1
