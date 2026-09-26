#!/usr/bin/env bash
# Can a current Kafka client talk to AeroStream's Kafka port?  Starts the AeroStream stack for AERO_TAG and runs
# Kafka's own kafka-broker-api-versions.sh (ApiVersions + Metadata) against it, plus a raw ApiVersions v3 request.
#   usage: AERO_TAG=main|integration check-kafka-port.sh <output-file>
set -uo pipefail; source "$(dirname "$0")/env.sh"
OUT="${1:?output file}"; mkdir -p "$(dirname "$OUT")"
"$(dirname "$0")/start-aerostream.sh" >/dev/null
{
  echo "== AeroStream image: $AERO_BROKER_IMAGE"
  echo "== kafka-broker-api-versions.sh (Kafka 4.x client) against localhost:9096 (25 s timeout)"
  docker run --rm --network host "$KAFKA_IMAGE" timeout 25 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$AERO_KAFKA_BOOTSTRAP" 2>&1 \
    | grep -vE '^\s+at ' | head -12
  echo "exit status: ${PIPESTATUS[0]}"
  echo "== raw ApiVersions v3 (flexible) request; a correct reply has a compact-array length varint after the error code"
  # 4-byte length + header v2 (api 18, version 3, corr id 7, client id "probe") + body (client name "java", version "1")
  exec 3<>/dev/tcp/127.0.0.1/9096
  printf '\x00\x00\x00\x18\x00\x12\x00\x03\x00\x00\x00\x07\x00\x05probe\x00\x05java\x021\x00' >&3
  RESP=$(timeout 3 head -c 24 <&3 | od -An -tx1 | tr -d ' \n')
  exec 3>&-
  echo "response (first 24 bytes, hex): $RESP"
  echo "layout: [4-byte length][4-byte correlation id][2-byte error code][array...]. A flexible (v3) reply continues with a 1-byte"
  echo "compact array length; a classic (wrong for v3) reply continues with a 4-byte int32 count such as 00000006."
} 2>&1 | tee "$OUT"
"$(dirname "$0")/cleanup.sh" >/dev/null
