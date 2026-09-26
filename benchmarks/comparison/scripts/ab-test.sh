#!/usr/bin/env bash
# Interleaved A/B comparison of two AeroStream broker images: A, B, A, B, ... so machine drift (thermal, frequency
# scaling, background load) hits both sides equally. Sequential sessions on a laptop are too noisy to resolve +-20%.
#   usage: ab-test.sh <tagA> <tagB> <rounds> <system: aerostream|aerostream-kafka>
#   env:   ONLY="100B 1KB"  RUNS=3   (passed through to run-all.sh); controller image defaults to aerostream-controller:integration
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
A="$1"; B="$2"; ROUNDS="$3"; SYS="${4:-aerostream}"; STAMP=$(date +%Y%m%d-%H%M%S)
export AERO_CONTROLLER_IMAGE="${AERO_CONTROLLER_IMAGE:-aerostream-controller:integration}"
for r in $(seq 1 "$ROUNDS"); do
  for T in "$A" "$B"; do
    AERO_TAG="$T" "$D/run-all.sh" "ab-$STAMP/$T-round$r" "$SYS" >/dev/null 2>&1
  done
done
echo "workload  side  median-of-rounds (MB/s)   per-round medians"
for w in ${ONLY:-100B 1KB 1MB 10MB 50MB}; do
  for T in "$A" "$B"; do
    vals=$(for r in $(seq 1 "$ROUNDS"); do
      "$D/report.sh" "$D/../results/ab-$STAMP/$T-round$r" | awk -F'|' -v w="$w" -v s="$SYS" '$2 ~ "^ "w" $" && $3 ~ s {gsub(/ /,"",$4); print $4}' | head -1
    done | tr '\n' ' ')
    med=$(echo "$vals" | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
    printf '%-9s %-9s %-10s %s\n' "$w" "$T" "$med" "$vals"
  done
done
echo "raw results: results/ab-$STAMP/"
