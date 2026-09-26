#!/usr/bin/env bash
# Markdown tables (median run per workload) from the summary files of one session.
#   usage: report.sh <results/label-dir>
set -euo pipefail
DIR="$1"
median_row() {  # $1 = system, $2 = workload -> "MB/s msgs/s avg p50 p95 p99 max" of the run with the median MB/s
  awk -F'\t' -v w="$2" '$1==w && $3!="FAILED" {print $3"\t"$0}' "$DIR/$1-summary.tsv" | sort -n | \
    awk -F'\t' '{r[NR]=$0} END{ if(NR==0){print "FAILED"; exit} split(r[int((NR+1)/2)],f,"\t"); print f[4]"\t"f[5]"\t"f[6]"\t"f[7]"\t"f[8]"\t"f[9]"\t"f[10] }'
}
SYSTEMS=(kafka redpanda aerostream aerostream-kafka)
echo "| Workload | System | MB/s (median) | MB/s (min-max of runs) | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |"
echo "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
for w in 100B 1KB 1MB 10MB 50MB; do
  for s in "${SYSTEMS[@]}"; do
    [ -f "$DIR/$s-summary.tsv" ] || continue
    row=$(median_row "$s" "$w")
    if [ "$row" = FAILED ]; then echo "| $w | $s | failed | | | | | | |"; continue; fi
    range=$(awk -F'\t' -v w="$w" '$1==w && $3!="FAILED" {v=$3+0; if(min==""||v<min)min=v; if(v>max)max=v} END{printf "%.0f-%.0f", min, max}' "$DIR/$s-summary.tsv")
    echo "$row" | awk -F'\t' -v w="$w" -v s="$s" -v r="$range" '{printf "| %s | %s | %.1f | %s | %.0f | %.2f | %.2f | %.2f | %.2f | %.2f |\n", w, s, $1, r, $2, $3, $4, $5, $6, $7}'
  done
done
echo
echo "| System | idle memory | peak memory (MiB) | peak CPU % | peak PIDs |"
echo "| :--- | :--- | ---: | ---: | ---: |"
for s in "${SYSTEMS[@]}"; do
  [ -f "$DIR/$s-peaks.txt" ] || continue
  idle=$(sed 's/^idle: //; s/ pids=.*//; s| / .*||' "$DIR/$s-idle.txt")
  eval "$(sed 's/ /; /g' "$DIR/$s-peaks.txt")"
  echo "| $s | $idle | $peak_mem_mib | $peak_cpu_pct | $peak_pids |"
done
