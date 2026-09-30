# Commands

Everything below is plain `docker` / `bash`. The scripts in `scripts/` are just these commands wrapped up;
read `scripts/env.sh` for every setting in one place. Always name the system to run (`aerostream` or `aerostream-kafka`); the
scripts are generic and run every system they know about when none is given.

## 0. Prerequisites
Docker, Go (to build the AeroStream client), `curl`. The `apache/kafka` image is pulled on first use for its `kafka-producer-perf-test.sh` tool, which drives the Kafka port.

## 1. Resource limits
```
--cpus=2.0 --memory=2g
```
The broker container also uses `--network host`, so no Docker userland proxy sits in the data path. The load generators run
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

## 3. Start AeroStream - `scripts/start-aerostream.sh` (`AERO_TAG=main|integration`)
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
client/bin/client -controller 127.0.0.1:28001 create-topic T 1 1
```

## 5. Workloads - `scripts/run-workloads.sh`
Each workload is run 3 times, each on a fresh topic. Large-message workloads push 500 MB in total.

| Workload | Kafka port: `--num-records` x `--record-size` | Native port: `-producers` x `-messages` x `-size` | extra Kafka producer props |
| :--- | :--- | :--- | :--- |
| 100 B | 100000 x 100 | 10 x 10000 x 100 | |
| 1 KB | 50000 x 1024 | 10 x 5000 x 1024 | |
| 1 MB | 500 x 1048576 | 10 x 50 x 1048576 | |
| 10 MB | 50 x 10485760 | 10 x 5 x 10485760 | `buffer.memory=134217728` |
| 50 MB | 10 x 52428800 | 5 x 2 x 52428800 | `buffer.memory=268435456` |

```bash
# Kafka port (AeroStream broker on 9096), driven by Kafka's producer performance tool
docker run --rm --network host apache/kafka:latest /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic T --num-records 500 --record-size 1048576 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9096 acks=1 max.request.size=67108864

# Native data-plane protocol
client/bin/client -controller 127.0.0.1:28001 bench -topic T -partition 0 -size 1048576 -producers 10 -messages 50
```

## 6. Resource statistics - `scripts/stats.sh`, `scripts/peaks.sh`
```bash
docker stats --no-stream --format '{{.MemUsage}},{{.CPUPerc}},{{.PIDs}}' bench-aerostream     # sampled once per second
```
Idle memory is one sample taken 10 s after the broker is ready; peaks are taken over the whole workload set.

## 7. Everything in one go - `scripts/run-all.sh`
```bash
cd benchmarks/comparison
AERO_TAG=integration scripts/run-all.sh 2026-09-26-integration aerostream          # native port
AERO_TAG=integration scripts/run-all.sh 2026-09-26-integration-kafka aerostream-kafka   # Kafka port
AERO_TAG=main AERO_CLIENT=/tmp/aero-main/client/bin/client scripts/run-all.sh 2026-09-26-main aerostream
ONLY="1KB 50MB" RUNS=1 scripts/run-all.sh quick aerostream                          # quick check
scripts/cleanup.sh                                                                  # remove all benchmark containers
```
Output per session in `results/<label>/` (the raw `*.log` files and `*-stats.csv` samples are git-ignored, see `benchmarks/.gitignore`): raw tool output `<system>-<workload>-run<n>.log`, `<system>-summary.tsv`
(`scripts/summarize.sh`), `<system>-stats.csv`, `<system>-peaks.txt`, `<system>-idle.txt`.

## 8. Other scripts
```bash
scripts/check-kafka-port.sh <out-file>            # can a current Kafka client talk to the Kafka port? (AERO_TAG=main|integration)
scripts/boot-time.sh 3 aerostream                 # time until usable, 3 runs (fast bash probe, 20 ms polling)
ONLY="100B 1KB" RUNS=3 scripts/ab-test.sh <tagA> <tagB> 3 aerostream   # interleaved A/B of two AeroStream broker images
scripts/report.sh results/<session>                # markdown tables: median run per workload, min-max, resource peaks
```
Use `ab-test.sh` (not two sequential sessions) to compare code changes: on a laptop the machine shifts between performance modes and
sequential sessions differ by more than most code changes. Profiling recipe: `results/2026-09-26-kafka-port-profile/README.md`.

For multi-partition, consume-path and fixed-rate latency measurements on dedicated cloud hardware, use the OpenMessaging Benchmark
scripts in [`../aws-ec2/`](../aws-ec2/README.md).
