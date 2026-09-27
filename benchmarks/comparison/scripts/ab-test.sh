#!/usr/bin/env bash
# Interleaved A/B comparison of two AeroStream broker setups: A, B, A, B, ... so machine drift (thermal, frequency
# scaling, background load) hits both sides equally. Sequential sessions on a laptop are too noisy to resolve +-20%.
#   usage: ab-test.sh <sideA> <sideB> <rounds> <system: aerostream|aerostream-kafka>
#   a side is  <image-tag>[:<broker-config.toml>]   e.g.  integration   or   integration:/tmp/nocompact.toml
#   env:   ONLY="100B 1KB"  RUNS=3   (passed through to run-all.sh); controller image defaults to aerostream-controller:integration
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
SA="$1"; SB="$2"; ROUNDS="$3"; SYS="${4:-aerostream}"; STAMP=$(date +%Y%m%d-%H%M%S)
export AERO_CONTROLLER_IMAGE="${AERO_CONTROLLER_IMAGE:-aerostream-controller:integration}"
label() { local t="${1%%:*}" c=""; [[ "$1" == *:* ]] && c="+$(basename "${1#*:}" .toml)"; echo "$t$c"; }
LA=$(label "$SA"); LB=$(label "$SB"); [ "$LA" = "$LB" ] && { echo "sides have the same label"; exit 2; }
for r in $(seq 1 "$ROUNDS"); do
  for SIDE in "$SA" "$SB"; do
    T="${SIDE%%:*}"; CFG=""; [[ "$SIDE" == *:* ]] && CFG="${SIDE#*:}"
    AERO_TAG="$T" AERO_BROKER_CONFIG="$CFG" "$D/run-all.sh" "ab-$STAMP/$(label "$SIDE")-round$r" "$SYS" >/dev/null 2>&1
  done
done
echo "workload  side  median-of-rounds (MB/s)   per-round medians"
for w in ${ONLY:-100B 1KB 1MB 10MB 50MB}; do
  for L in "$LA" "$LB"; do
    vals=$(for r in $(seq 1 "$ROUNDS"); do
      "$D/report.sh" "$D/../results/ab-$STAMP/$L-round$r" | awk -F'|' -v w="$w" -v s="$SYS" '$2 ~ "^ "w" $" && $3 ~ s {gsub(/ /,"",$4); print $4}' | head -1
    done | tr '\n' ' ')
    med=$(echo "$vals" | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
    printf '%-9s %-28s %-10s %s\n' "$w" "$L" "$med" "$vals"
  done
done
echo "raw results: results/ab-$STAMP/"
