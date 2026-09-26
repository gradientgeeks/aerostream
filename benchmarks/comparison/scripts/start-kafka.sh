#!/usr/bin/env bash
# Apache Kafka (KRaft, single node) under the benchmark limits.
set -euo pipefail; source "$(dirname "$0")/env.sh"
docker rm -f "$KAFKA_NAME" >/dev/null 2>&1 || true
docker run -d --name "$KAFKA_NAME" $LIMITS --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:19192,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:19093 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19192,EXTERNAL://127.0.0.1:9094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_MESSAGE_MAX_BYTES=$MAX_MSG \
  -e KAFKA_REPLICA_FETCH_MAX_BYTES=$MAX_MSG \
  -e KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600 \
  "$KAFKA_IMAGE" >/dev/null
for i in $(seq 1 60); do
  docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_ADMIN_BOOTSTRAP" --list >/dev/null 2>&1 && { echo "kafka ready"; exit 0; }
  sleep 1
done
echo "kafka did not become ready" >&2; docker logs --tail 20 "$KAFKA_NAME" >&2; exit 1
