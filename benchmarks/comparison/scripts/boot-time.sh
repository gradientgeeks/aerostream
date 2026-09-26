#!/usr/bin/env bash
# Time from `docker run` until each system is usable, N runs each (default 3). Polls every 20 ms.
#   kafka / redpanda : the Kafka listener answers a raw ApiVersions v0 request
#   aerostream       : the broker has registered with the controller (REST /api/brokers shows it active)
#   usage: boot-time.sh [runs] [system ...]      (AERO_TAG=main|integration)
set -uo pipefail; source "$(dirname "$0")/env.sh"
RUNS="${1:-3}"; shift || true
SYSTEMS=("${@:-kafka redpanda aerostream}"); read -ra SYSTEMS <<<"${SYSTEMS[*]}"
now() { date +%s.%N; }
api_versions_ok() {   # $1 = port; ApiVersions v0: 4-byte length, api 18, version 0, correlation id 1, client id "boot"
  (
    exec 3<>"/dev/tcp/127.0.0.1/$1" || exit 1
    printf '\x00\x00\x00\x0e\x00\x12\x00\x00\x00\x00\x00\x01\x00\x04boot' >&3
    n=$(timeout 1 head -c 4 <&3 | od -An -tu1 | tr -d ' \n')
    [ -n "$n" ] && [ "$n" != "0000" ]
  ) 2>/dev/null
}
for SYS in "${SYSTEMS[@]}"; do
  for i in $(seq 1 "$RUNS"); do
    "$(dirname "$0")/cleanup.sh" >/dev/null; T0=$(now)
    case "$SYS" in
      kafka)      "$(dirname "$0")/start-kafka.sh"      >/dev/null 2>&1 & ;;
      redpanda)   "$(dirname "$0")/start-redpanda.sh"   >/dev/null 2>&1 & ;;
      aerostream) "$(dirname "$0")/start-aerostream.sh" >/dev/null 2>&1 & ;;
    esac
    # the start scripts also wait for readiness (slowly); we time our own fast probe instead
    while :; do
      case "$SYS" in
        kafka)      api_versions_ok 9094 && break ;;
        redpanda)   api_versions_ok 19092 && break ;;
        aerostream) curl -sf "http://$AERO_CONTROLLER_HTTP/api/brokers" 2>/dev/null | grep -q '"active": *true' && break ;;
      esac
      sleep 0.02
    done
    T1=$(now); printf '%s run%s  %.2f s\n' "$SYS" "$i" "$(echo "$T1 $T0" | awk '{print $1-$2}')"
    wait 2>/dev/null
  done
done
"$(dirname "$0")/cleanup.sh" >/dev/null
