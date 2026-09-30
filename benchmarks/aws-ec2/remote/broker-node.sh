#!/usr/bin/env bash
# Runs ON THE BROKER NODE.   usage: broker-node.sh start <aerostream|kafka|redpanda> <label>   |   broker-node.sh stop <label>
# One broker container at a time, host networking (no docker-proxy in the data path), data on local NVMe, fresh data dir
# per run. Limits: MODE=parity -> 2 CPU / 2 GiB (same as omb-run.sh); MODE=fullnode -> 4 CPU / 7 GiB.
set -uo pipefail
ACTION="${1:?start|stop}"; BENCH=/home/ec2-user/bench; C=bench-broker
MODE="${MODE:-parity}"; DATA=/mnt/nvme/data
[ -d "$DATA" ] || DATA=/home/ec2-user/bench/data      # fall back to the root volume if no local NVMe was found
imds() { local t; t=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60'); curl -s -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/$1"; }

if [ "$ACTION" = start ]; then
  SYSTEM="${2:?system}"; LABEL="${3:?label}"; OUT="$BENCH/broker/$LABEL"; mkdir -p "$OUT"
  IP=$(imds local-ipv4)
  # Kafka runs a JVM with a fixed heap (default 1 GiB) that does not grow with the container, so in fullnode mode give it
  # a real heap (leaving the rest for the page cache); in parity mode keep the default, as omb-run.sh does.
  if [ "$MODE" = fullnode ]; then CPUS=4; MEM=7g; SMP=4; RPMEM=5G; KHEAP="-Xms4g -Xmx4g"; else CPUS=2; MEM=2g; SMP=2; RPMEM=1500M; KHEAP=""; fi
  # Single-machine topology (PIN=1): the broker gets its own physical cores (BROKER_CPUSET from split-cpus.py) and BROKER_MEM.
  CPUARG="--cpus=$CPUS"
  if [ "${PIN:-0}" = 1 ] && [ -f "$BENCH/cpusets.env" ]; then
    source "$BENCH/cpusets.env"; CPUARG="--cpuset-cpus=$BROKER_CPUSET"; CPUS=$(awk -F, '{print NF}' <<< "$BROKER_CPUSET"); SMP=$CPUS
    MEM="${BROKER_MEM:-8g}"; RPMEM=5G; KHEAP="-Xms4g -Xmx4g"
  fi
  docker rm -f -v "$C" >/dev/null 2>&1; sudo rm -rf "$DATA/$SYSTEM"; sudo mkdir -p "$DATA/$SYSTEM"; sudo chmod 777 "$DATA/$SYSTEM"
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null      # same cold page cache for every system
  case "$SYSTEM" in
    aerostream)
      IMG=$(cat "$BENCH/aerostream.image")
      docker run -d --name "$C" $CPUARG --memory=$MEM --network host -e ADVERTISED_HOST="$IP" -e DATA_DIR=/data \
        -v "$DATA/$SYSTEM:/data" "$IMG" >/dev/null
      for i in $(seq 1 90); do curl -sf http://127.0.0.1:9001/api/brokers 2>/dev/null | grep -q '"active": *true' && break; sleep 1; done ;;
    kafka)
      docker run -d --name "$C" $CPUARG --memory=$MEM --network host -v "$DATA/$SYSTEM:/data/kafka" \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LOG_DIRS=/data/kafka \
        -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$IP:9092 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 ${KHEAP:+-e KAFKA_HEAP_OPTS="$KHEAP"} "$KAFKA_IMAGE" >/dev/null
      for i in $(seq 1 90); do docker exec "$C" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 1; done ;;
    redpanda)
      docker run -d --name "$C" $CPUARG --memory=$MEM --network host -v "$DATA/$SYSTEM:/var/lib/redpanda/data" "$REDPANDA_IMAGE" \
        redpanda start --smp $SMP --memory $RPMEM --reserve-memory 0M --overprovisioned --node-id 0 --check=false \
        --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://$IP:9092 >/dev/null
      for i in $(seq 1 90); do docker exec "$C" rpk cluster info >/dev/null 2>&1 && break; sleep 1; done ;;
    *) echo "unknown system $SYSTEM" >&2; exit 2 ;;
  esac
  docker ps --filter name=$C --format '{{.Status}}' | grep -q Up || { echo "broker container not running"; docker logs "$C" 2>&1 | tail -20; exit 1; }
  echo "$SYSTEM ready on $IP:9092 (mode=$MODE, cpus=$CPUS ${CPUARG#--}, mem=$MEM)" | tee "$OUT/started.txt"
  # samplers: container stats (as in omb-run.sh) + node-level cpu/mem/disk
  ( while [ ! -f "$OUT/.stop" ]; do docker stats --no-stream --format '{{.MemUsage}},{{.CPUPerc}},{{.PIDs}}' "$C" >> "$OUT/broker-stats.csv" 2>/dev/null; done ) >/dev/null 2>&1 &
  nohup sar -u -r -n DEV 5 -o "$OUT/sar.bin" >/dev/null 2>&1 &
  nohup iostat -x -t 5 > "$OUT/iostat.txt" 2>&1 &
  echo "$IP" > "$BENCH/broker.ip"
else
  LABEL="${2:?label}"; OUT="$BENCH/broker/$LABEL"; mkdir -p "$OUT"
  touch "$OUT/.stop"; pkill -x sar 2>/dev/null; pkill -x iostat 2>/dev/null; sleep 6
  [ -f "$OUT/sar.bin" ] && sar -u -f "$OUT/sar.bin" > "$OUT/sar-cpu.txt" 2>/dev/null; [ -f "$OUT/sar.bin" ] && sar -r -f "$OUT/sar.bin" > "$OUT/sar-mem.txt" 2>/dev/null
  docker logs "$C" > "$OUT/broker.log" 2>&1
  du -sh "$DATA"/* > "$OUT/disk-usage.txt" 2>/dev/null
  docker rm -f -v "$C" >/dev/null 2>&1; rm -f "$OUT/.stop"; echo stopped
fi
