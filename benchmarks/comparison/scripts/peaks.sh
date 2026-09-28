#!/usr/bin/env bash
# Peak memory (MiB), peak CPU (%) and peak PIDs from a stats.sh CSV.   usage: peaks.sh <csv>
awk -F, 'NR>1 {
  split($2, a, " / "); v=a[1]; n=v+0;
  if (v ~ /GiB/) n*=1024; else if (v ~ /KiB|kB/) n/=1024; else if (v ~ /B$/ && v !~ /iB/) n/=1048576;
  if (n>m) m=n; c=$3+0; if (c>cp) cp=c; if ($4+0>p) p=$4+0 }
  END { printf "peak_mem_mib=%.1f peak_cpu_pct=%.0f peak_pids=%d\n", m, cp, p }' "$1"
