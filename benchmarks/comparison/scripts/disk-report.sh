#!/usr/bin/env bash
# Per-run disk activity: for each run window in <sys>-timeline.tsv, average write MB/s and % of time the disk was busy,
# from <sys>-disk.csv.   usage: disk-report.sh <session-dir> <system>
DIR="$1"; SYS="$2"
echo -e "workload\trun\tseconds\tdisk_write_MB_s\tdisk_busy_pct\tmax_temp_c"
awk -F'[,\t]' 'FNR==1 && FILENAME ~ /disk.csv/ {next}
  FILENAME ~ /disk.csv/ {n++; t[n]=$1; w[n]=$2; b[n]=$3; c[n]=$4; next}
  { s=$3; e=$4; i0=0; i1=0
    for (i=1;i<=n;i++) { if (t[i]<=s) i0=i; if (t[i]<=e) i1=i }
    if (i1<=i0) { i1=i0+1 } if (i1>n) i1=n
    dt=t[i1]-t[i0]; if (dt<=0) dt=1
    mx=0; for (i=i0;i<=i1;i++) if (c[i]+0>mx) mx=c[i]+0
    printf "%s\t%s\t%.1f\t%.0f\t%.0f\t%s\n", $1, $2, e-s, (w[i1]-w[i0])*512/1048576/dt, (b[i1]-b[i0])/(dt*10), mx }' \
  "$DIR/$SYS-disk.csv" "$DIR/$SYS-timeline.tsv"
