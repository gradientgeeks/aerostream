#!/usr/bin/env bash
# Sample `docker stats` once per second into a CSV until the file <csv>.stop appears.
#   usage: stats.sh <container> <csv>      (run in the background around a workload)
CONTAINER="$1"; CSV="$2"; rm -f "$CSV.stop"
echo "time,mem_usage,cpu_percent,pids" >"$CSV"
while [ ! -e "$CSV.stop" ]; do
  docker stats --no-stream --format '{{.MemUsage}},{{.CPUPerc}},{{.PIDs}}' "$CONTAINER" 2>/dev/null | sed "s/^/$(date +%s),/" >>"$CSV"
done
