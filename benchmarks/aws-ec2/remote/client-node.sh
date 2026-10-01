#!/usr/bin/env bash
# Runs ON THE CLIENT NODE.   usage: client-node.sh <system> <workload> <label> <broker-private-ip> <test-min> <warm-min>
# Runs one OMB workload against the broker on the other node and leaves the result JSON + logs in ~/bench/results/<label>/.
set -uo pipefail
SYSTEM="$1"; WL="$2"; LABEL="$3"; BROKER_IP="$4"; TEST="$5"; WARM="$6"
BENCH=/home/ec2-user/bench; DIST=$BENCH/omb/dist; OUT=$BENCH/results/$LABEL; mkdir -p "$OUT"
sed "s/__BROKER__/$BROKER_IP/" "$BENCH/drivers/$SYSTEM.yaml" > "$OUT/driver.yaml"
# workload: either one of ours (templated durations) or an upstream OMB workload passed as workloads/<file>.yaml
if [ -f "$BENCH/workloads/$WL.yaml" ]; then sed "s/__TEST__/$TEST/; s/__WARM__/$WARM/" "$BENCH/workloads/$WL.yaml" > "$OUT/workload.yaml"
else cp "$DIST/workloads/$WL" "$OUT/workload.yaml"; fi
cd "$DIST" || exit 1
rm -f ./*.json
# Single-machine topology (PIN=1): OMB runs only on CLIENT_CPUSET; the broker owns BROKER_CPUSET (see split-cpus.py).
TASKSET=""; CLIENT_CPUSET=""; BROKER_CPUSET=""
if [ "${PIN:-0}" = 1 ] && [ -f "$BENCH/cpusets.env" ]; then source "$BENCH/cpusets.env"; TASKSET="taskset -c $CLIENT_CPUSET"; fi
nohup sar -P ALL -u -r -n DEV 5 -o "$OUT/sar.bin" >/dev/null 2>&1 &
LIMIT=$(( (TEST + WARM + 12) * 60 ))
# GC log of the load generator: stalls here show up as publish latency, so keep it next to the OMB result
export HEAP_OPTS="${HEAP_OPTS:--Xms4G -Xmx4G} -Xlog:gc,safepoint:file=$OUT/gc.log:time,uptime"
timeout "$LIMIT" $TASKSET ./bin/benchmark --drivers "$OUT/driver.yaml" "$OUT/workload.yaml" > "$OUT/omb.log" 2>&1 &
BPID=$!
# OMB sometimes keeps running after writing its result (lingering Kafka client thread): stop it once the JSON exists.
while kill -0 "$BPID" 2>/dev/null && ! ls ./*.json >/dev/null 2>&1; do sleep 3; done
sleep 8
pkill -f 'io.openmessaging.benchmark.Benchmark' 2>/dev/null; kill "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null
pkill -x sar 2>/dev/null; sleep 1
# CPU busy for a set of cpus, as one "Average: all ... <idle>" line (the format summarize.py reads)
avgidle() { sar -P ALL -u -f "$OUT/sar.bin" 2>/dev/null | awk -v set="$1" 'BEGIN{n=split(set,a,",");for(i=1;i<=n;i++)m[a[i]]=1} /^Average:/ && ($2 in m) {s+=$NF;c++} END{if(c) printf "Average:        all      0.00      0.00      0.00      0.00      0.00 %8.2f\n", s/c}'; }
if [ -n "$TASKSET" ]; then avgidle "$CLIENT_CPUSET" > "$OUT/sar-cpu.txt"; avgidle "$BROKER_CPUSET" > "$OUT/sar-cpu-broker.txt"
else sar -u -f "$OUT/sar.bin" > "$OUT/sar-cpu.txt" 2>/dev/null; fi
if ls ./*.json >/dev/null 2>&1; then cp ./*.json "$OUT/result.json"; echo "OK $LABEL"; else echo "NO RESULT $LABEL"; tail -20 "$OUT/omb.log"; exit 1; fi
