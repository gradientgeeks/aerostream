#!/usr/bin/env bash
# Turn the raw logs of one system into a tab-separated summary (one line per run) plus the median run per workload.
#   usage: summarize.sh <log-dir> <system>
set -euo pipefail
DIR="$1"; SYS="$2"
tomb() { awk -v v="$1" -v u="$2" 'BEGIN{ m=(u=="ns")?1e-6:(u=="µs"||u=="us")?1e-3:(u=="ms")?1:(u=="s")?1000:1; printf "%.3f", v*m }'; }
for f in $(ls "$DIR"/${SYS}-*-run*.log | sort -V); do
  base=$(basename "$f" .log); label=${base#${SYS}-}; label=${label%-run*}; run=${base##*-run}
  if [ "$SYS" = aerostream ]; then
    dur=$(grep -m1 "Total Duration" "$f" | sed -E 's/.*: *([0-9.]+)(ns|µs|us|ms|s).*/\1 \2/')
    ok=$(grep -m1 "Successful Writes" "$f" | sed -E 's/.*: *([0-9,]+).*/\1/' | tr -d ,)
    mb=$(grep -m1 "Data Throughput" "$f" | sed -E 's/.*: *([0-9.,]+) MB.*/\1/' | tr -d ,)
    mps=$(grep -m1 "Messages / Second" "$f" | sed -E 's/.*: *([0-9.,]+) msgs.*/\1/' | tr -d ,)
    lat() { grep -m1 -E "$1" "$f" | sed -E 's/.*: *([0-9.]+) *(ns|µs|us|ms|s).*/\1 \2/'; }
    [ -z "$mb" ] && { printf "%s\trun%s\tFAILED\n" "$label" "$run"; continue; }
    printf "%s\trun%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$label" "$run" "$mb" "$mps" \
      "$(tomb $(lat 'Avg Latency'))" "$(tomb $(lat 'p50'))" "$(tomb $(lat 'p95'))" "$(tomb $(lat 'p99 '))" "$(tomb $(lat 'Max Latency'))"
  else
    line=$(grep -E "records sent" "$f" | tail -1)
    if [ -z "$line" ]; then printf "%s\trun%s\tFAILED\n" "$label" "$run"; continue; fi
    echo "$line" | sed -E 's/.* ([0-9.]+) records\/sec \(([0-9.]+) MB\/sec\), ([0-9.]+) ms avg latency, ([0-9.]+) ms max latency, ([0-9.]+) ms 50th, ([0-9.]+) ms 95th, ([0-9.]+) ms 99th.*/\2\t\1\t\3\t\5\t\6\t\7\t\4/' \
      | sed "s/^/$label\trun$run\t/"
  fi
done | tee "$DIR/${SYS}-summary.tsv"
echo "# columns: workload run MB/s msgs/s avg_ms p50_ms p95_ms p99_ms max_ms"
