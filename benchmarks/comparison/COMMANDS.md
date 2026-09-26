# Commands

Everything below is plain `docker` / `bash`. The scripts in `scripts/` are just these commands wrapped up;
read `scripts/env.sh` for every setting in one place.

## 0. Prerequisites
Docker, Go (to build the AeroStream client), `curl`. Images pulled on first use: `apache/kafka:latest`, `redpandadata/redpanda:latest`.

## 1. Resource limits (identical for every broker)
```
--cpus=2.0 --memory=2g
```
Every container also uses `--network host`, so no Docker userland proxy sits in the data path. The load generators run
in their own unconstrained containers/processes.

## 2. Build the AeroStream images and client from the code under test
```bash
# integration branch (this checkout)
benchmarks/comparison/scripts/build-aerostream.sh integration "$(git rev-parse --show-toplevel)"

# main branch, from a separate checkout so the working tree is untouched
git worktree add --detach /tmp/aero-main main
benchmarks/comparison/scripts/build-aerostream.sh main /tmp/aero-main
```
This runs `docker build -f Dockerfile.controller -t aerostream-controller:<tag> .`,
`docker build -f Dockerfile.broker -t aerostream-broker:<tag> .` and `go build -o client/bin/client ./client`.

## 3. Start the brokers
### Apache Kafka (KRaft, single node) - `scripts/start-kafka.sh`
```bash
docker run -d --name bench-kafka --cpus=2.0 --memory=2g --network host \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:19192,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:19093 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19192,EXTERNAL://127.0.0.1:9094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_MESSAGE_MAX_BYTES=67108864 -e KAFKA_REPLICA_FETCH_MAX_BYTES=67108864 \
  -e KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600 \
  apache/kafka:latest
```
### Redpanda (single node) - `scripts/start-redpanda.sh`
```bash
docker run -d --name bench-redpanda --cpus=2.0 --memory=2g --network host redpandadata/redpanda:latest \
  redpanda start --smp 2 --memory 1500M --reserve-memory 0M --overprovisioned --node-id 0 \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092 \
  --set redpanda.kafka_batch_max_bytes=67108864 --set redpanda.kafka_request_max_bytes=104857600
```
### AeroStream - `scripts/start-aerostream.sh` (`AERO_TAG=main|integration`)
```bash
# dedicated controller (a locally running cluster on :8001 is not touched)
docker run -d --name bench-aerostream-controller --network host aerostream-controller:$AERO_TAG \
  -id bench -raft-addr 127.0.0.1:27001 -grpc-addr 127.0.0.1:28001 -http-addr 127.0.0.1:29001 -data-dir /data -bootstrap
# broker under the limits
docker run -d --name bench-aerostream --cpus=2.0 --memory=2g --network host aerostream-broker:$AERO_TAG \
  --id 10 --host 127.0.0.1 --data-port 9095 --kafka-port 9096 --controller http://127.0.0.1:28001 --storage-dir /data
```

## 4. Create topics (1 partition, replication factor 1, up to 64 MiB messages) - `scripts/create-topic.sh`
```bash
docker exec bench-kafka /opt/kafka/bin/kafka-topics.sh --create --topic T --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:19192 --config max.message.bytes=67108864
docker exec bench-redpanda rpk topic create T -p 1 -r 1 -c max.message.bytes=67108864 -X brokers=127.0.0.1:19092
client/bin/client -controller 127.0.0.1:28001 create-topic T 1 1
```

## 5. Workloads - `scripts/run-workloads.sh`
Each workload is run 3 times, each on a fresh topic. Large-message workloads push 500 MB in total.

| Workload | Kafka / Redpanda: `--num-records` x `--record-size` | AeroStream: `-producers` x `-messages` x `-size` | extra Kafka producer props |
| :--- | :--- | :--- | :--- |
| 100 B | 100000 x 100 | 10 x 10000 x 100 | |
| 1 KB | 50000 x 1024 | 10 x 5000 x 1024 | |
| 1 MB | 500 x 1048576 | 10 x 50 x 1048576 | |
| 10 MB | 50 x 10485760 | 10 x 5 x 10485760 | `buffer.memory=134217728` |
| 50 MB | 10 x 52428800 | 5 x 2 x 52428800 | `buffer.memory=268435456` |

```bash
# Apache Kafka (port 9094) and Redpanda (port 19092): same tool, same properties
docker run --rm --network host apache/kafka:latest /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic T --num-records 500 --record-size 1048576 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9094 acks=1 max.request.size=67108864

# AeroStream (native data-plane protocol)
client/bin/client -controller 127.0.0.1:28001 bench -topic T -partition 0 -size 1048576 -producers 10 -messages 50
```

## 6. Resource statistics - `scripts/stats.sh`, `scripts/peaks.sh`
```bash
docker stats --no-stream --format '{{.MemUsage}},{{.CPUPerc}},{{.PIDs}}' bench-kafka     # sampled once per second
```
Idle memory is one sample taken 10 s after the broker is ready; peaks are taken over the whole workload set.

## 7. Everything in one go - `scripts/run-all.sh`
```bash
cd benchmarks/comparison
AERO_TAG=integration scripts/run-all.sh 2026-09-26-integration              # kafka, redpanda, aerostream
AERO_TAG=main AERO_CLIENT=/tmp/aero-main/client/bin/client scripts/run-all.sh 2026-09-26-main
scripts/run-all.sh mytest aerostream                                        # a single system
ONLY="1KB 50MB" RUNS=1 scripts/run-all.sh quick                             # quick check
scripts/cleanup.sh                                                          # remove all benchmark containers
```
Output per session in `results/<label>/`: raw tool output `<system>-<workload>-run<n>.log`, `<system>-summary.tsv`
(`scripts/summarize.sh`), `<system>-stats.csv`, `<system>-peaks.txt`, `<system>-idle.txt`.

## 8. Other scripts
```bash
scripts/check-kafka-port.sh <out-file>            # can a current Kafka client talk to the Kafka port? (AERO_TAG=main|integration)
scripts/boot-time.sh 3 aerostream kafka redpanda  # time until usable, 3 runs each (fast bash probe, 20 ms polling)
ONLY="100B 1KB" RUNS=3 scripts/ab-test.sh <tagA> <tagB> 3 aerostream   # interleaved A/B of two AeroStream broker images
scripts/report.sh results/<session>                # markdown tables: median run per workload, min-max, resource peaks
```
Use `ab-test.sh` (not two sequential sessions) to compare code changes: on this laptop the machine shifts between performance modes and
sequential sessions differ by more than most code changes. Profiling recipe: `results/2026-09-26-kafka-port-profile/README.md`.
