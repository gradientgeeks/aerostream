#!/usr/bin/env bash
# usage: create-topic.sh <kafka|redpanda|aerostream|aerostream-kafka> <topic>   (1 partition, replication factor 1, 64 MiB max message)
set -euo pipefail; source "$(dirname "$0")/env.sh"
SYS="$1"; TOPIC="$2"
case "$SYS" in
  kafka)      docker exec "$KAFKA_NAME" /opt/kafka/bin/kafka-topics.sh --create --topic "$TOPIC" --partitions 1 --replication-factor 1 \
                --bootstrap-server "$KAFKA_ADMIN_BOOTSTRAP" --config max.message.bytes=$MAX_MSG ;;
  redpanda)   docker exec "$REDPANDA_NAME" rpk topic create "$TOPIC" -p 1 -r 1 -c max.message.bytes=$MAX_MSG -X brokers=127.0.0.1:19092 ;;
  aerostream|aerostream-kafka) "$AERO_CLIENT" -controller "$AERO_CONTROLLER_GRPC" create-topic "$TOPIC" 1 1 ;;
  *) echo "unknown system $SYS" >&2; exit 2 ;;
esac
