#!/usr/bin/env bash
# AeroStream: a dedicated benchmark controller (so a locally running cluster is not touched) and one broker
# container under the benchmark limits. Images come from build-aerostream.sh (AERO_TAG=main|integration).
set -euo pipefail; source "$(dirname "$0")/env.sh"
docker rm -f "$AERO_NAME" "$AERO_CTL_NAME" >/dev/null 2>&1 || true
docker run -d --name "$AERO_CTL_NAME" --network host "$AERO_CONTROLLER_IMAGE" \
  -id bench -raft-addr 127.0.0.1:27001 -grpc-addr "$AERO_CONTROLLER_GRPC" -http-addr "$AERO_CONTROLLER_HTTP" \
  -data-dir /data -bootstrap >/dev/null
for i in $(seq 1 30); do curl -sf "http://$AERO_CONTROLLER_HTTP/status" >/dev/null 2>&1 && break; sleep 1; done
docker run -d --name "$AERO_NAME" $LIMITS --network host "$AERO_BROKER_IMAGE" \
  --id 10 --host 127.0.0.1 --data-port 9095 --kafka-port 9096 \
  --controller "http://$AERO_CONTROLLER_GRPC" --storage-dir /data >/dev/null
for i in $(seq 1 60); do
  curl -sf "http://$AERO_CONTROLLER_HTTP/api/brokers" 2>/dev/null | grep -q '"active": *true' && { echo "aerostream ($AERO_TAG) ready"; exit 0; }
  sleep 1
done
echo "aerostream did not become ready" >&2; docker logs --tail 20 "$AERO_NAME" >&2; exit 1
