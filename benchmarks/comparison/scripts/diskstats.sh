#!/usr/bin/env bash
# Sample a host block device every 0.2 s until <csv>.stop exists.
#   usage: diskstats.sh <device e.g. nvme0n1> <csv>
# Columns: epoch, sectors_written (512 B units), io_ticks_ms (time the device had I/O in flight) -- from /proc/diskstats --
# and temp_c, the NVMe composite temperature (empty if no nvme hwmon sensor).
DEV="$1"; CSV="$2"; rm -f "$CSV.stop"; HW=""; for h in /sys/class/hwmon/hwmon*; do [ "$(cat "$h/name" 2>/dev/null)" = nvme ] && HW="$h/temp1_input" && break; done
echo "epoch,sectors_written,io_ticks_ms,temp_c" >"$CSV"
while [ ! -e "$CSV.stop" ]; do
  TEMP=""; [ -n "$HW" ] && TEMP=$(( $(cat "$HW") / 1000 ))
  awk -v d="$DEV" -v t="$(date +%s.%N)" -v c="$TEMP" '$3==d {print t","$10","$13","c}' /proc/diskstats >>"$CSV"
  sleep 0.2
done
