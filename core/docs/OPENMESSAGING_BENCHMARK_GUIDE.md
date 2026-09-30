# OpenMessaging Benchmark (OMB) Execution Guide

This document details the exact, reproducible procedure for compiling, configuring, and executing the **OpenMessaging Benchmark (OMB)** against **AeroStream**'s Kafka wire protocol engine.

The OpenMessaging Benchmark is the Linux Foundation’s vendor-neutral benchmarking framework used to measure sustained throughput, end-to-end latency, and tail latencies ($p_{50}, p_{95}, p_{99}, p_{99.9}$).

---

## 1. Prerequisites & Environment

| Component | Requirement | Note |
| :--- | :--- | :--- |
| **Operating System** | Linux (Ubuntu / Debian / RHEL) | Kernel 5.x+ with `sendfile(2)` and `epoll` |
| **Container Engine**| Docker 24.0+ with BuildKit | Used for isolated benchmarking sandbox |
| **Java Runtime** | Java 17 LTS | Required by OMB (Java 21+ breaks older Lombok plugins) |
| **Maven** | Apache Maven 3.9+ | Built via JDK 17 container |
| **Python** | Python 3.9+ | For parsing JSON test outputs |

---

## 2. Step 1: Clone and Compile OpenMessaging Benchmark

The upstream OpenMessaging Benchmark repository requires Java 17 to compile its Kafka driver and packaging modules. If your host has a newer OpenJDK (e.g., OpenJDK 25), compile using the official Eclipse Temurin container.

### A. Clone the Repository
```bash
cd benchmarks
git clone --depth 1 https://github.com/openmessaging/benchmark.git openmessaging-benchmark
cd openmessaging-benchmark
```

### B. Compile with JDK 17 Container
Run the build inside a `maven:3.9-eclipse-temurin-17` container. Skip tests, spotless checks, and license verification (since shallow git clones omit git history needed by license plugins):

```bash
docker run --rm \
  -v $(pwd):/app \
  -v ~/.m2:/root/.m2 \
  -w /app \
  maven:3.9-eclipse-temurin-17 \
  mvn clean install -DskipTests -Dlicense.skip=true -Dspotless.check.skip=true -Dspotbugs.skip=true \
  -pl benchmark-framework,driver-kafka,package -am
```

### C. Extract Distribution
Once `BUILD SUCCESS` is reported, unpack the generated binary tarball into a `dist/` directory:

```bash
mkdir -p dist
tar -xzf package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz -C dist --strip-components=1
cd dist
```

---

## 3. Step 2: Configure the AeroStream Kafka Driver

Create the driver configuration file in `dist/driver-kafka/aerostream.yaml`:

```yaml
name: AeroStream-Kafka
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

# Single-broker topology for standalone container evaluation
replicationFactor: 1

topicConfig: ""

commonConfig: |
  bootstrap.servers=localhost:9092
  default.api.timeout.ms=60000
  request.timeout.ms=60000

producerConfig: |
  acks=1
  linger.ms=1
  batch.size=131072
  max.in.flight.requests.per.connection=5

consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=true
  auto.commit.interval.ms=5000
  max.partition.fetch.bytes=1048576
```

> [!TIP]
> **Consumer Commit Tuning**: Setting `enable.auto.commit=true` with a 5-second interval mirrors production consumer behavior and avoids flooding synchronous offset commit requests into the consensus engine on every poll.

---

## 4. Step 3: Define the Workload Configuration

Create the workload profile in `dist/workloads/aerostream-16p-1kb.yaml`:

```yaml
name: AeroStream OMB 16 Partitions 1KB Max Rate

topics: 1
partitionsPerTopic: 16
messageSize: 1024
payloadFile: "payload/payload-1Kb.data"
subscriptionsPerTopic: 1
consumerPerSubscription: 2
producersPerTopic: 2

# 0 = Discover maximum sustainable rate under current hardware limits
producerRate: 0

consumerBacklogSizeGB: 0
testDurationMinutes: 1
```

---

## 5. Step 4: Launch the AeroStream Container

Start AeroStream with strict container resource constraints (`2.0 CPUs, 2 GiB RAM`) to match standard benchmark sandbox parameters:

```bash
docker rm -f aerostream 2>/dev/null || true

docker run -d --name aerostream \
  --cpus=2.0 --memory=2g \
  -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
  quay.io/gradientgeeks/aerostream:latest

# Wait for Raft election and broker registration
sleep 3
curl -s http://localhost:9001/api/cluster
```

Ensure the output confirms broker status:
```json
{"brokers":[{"active":true,"host":"0.0.0.0","id":1,"port":9091}],"brokers_count":1,"node_id":"node1","raft_state":"Leader"}
```

---

## 6. Step 5: Execute OpenMessaging Benchmark

From within `benchmarks/openmessaging-benchmark/dist`, run:

```bash
./bin/benchmark --drivers driver-kafka/aerostream.yaml workloads/aerostream-16p-1kb.yaml
```

The benchmark will:
1. Connect via Kafka `AdminClient` and create `test-topic-xxxx` with 16 partitions.
2. Initialize 2 producers and 2 consumers.
3. Run a **1-minute warm-up** period.
4. Execute the **1-minute measurement** phase across all partitions.
5. Generate an official JSON result: `aerostream-16p-1kb-AeroStream-Kafka-<timestamp>.json`.

---

## 7. Step 6: Parse and Analyze Results

Use Python to parse the generated JSON file and compute latency percentiles:

```python
import json
import glob

# Load latest OMB result JSON
result_files = sorted(glob.glob("aerostream-16p-1kb-AeroStream-Kafka-*.json"))
with open(result_files[-1]) as f:
    d = json.load(f)

avg_pub = sum(d['publishRate']) / len(d['publishRate'])
max_pub = max(d['publishRate'])
avg_cons = sum(d['consumeRate']) / len(d['consumeRate'])

print("=== OPENMESSAGING BENCHMARK OFFICIAL RESULTS ===")
print(f"Workload:     {d['workload']}")
print(f"Driver:       {d['driver']}")
print(f"Partitions:   {d['partitions']}")
print(f"Message Size: {d['messageSize']} bytes")
print(f"Publish Rate: Avg = {avg_pub:,.1f} msg/s ({avg_pub*d['messageSize']/(1024*1024):.2f} MB/s) | Peak = {max_pub:,.1f} msg/s")
print(f"Consume Rate: Avg = {avg_cons:,.1f} msg/s ({avg_cons*d['messageSize']/(1024*1024):.2f} MB/s)")
print(f"Error Rate:   {sum(d['publishErrorRate']):.2f} err/s")
print("")
print("--- Publish Latency (ms) ---")
print(f"  Avg:    {d['aggregatedPublishLatencyAvg']:.2f} ms")
print(f"  p50:    {d['aggregatedPublishLatency50pct']:.2f} ms")
print(f"  p75:    {d['aggregatedPublishLatency75pct']:.2f} ms")
print(f"  p95:    {d['aggregatedPublishLatency95pct']:.2f} ms")
print(f"  p99:    {d['aggregatedPublishLatency99pct']:.2f} ms")
print(f"  p99.9:  {d['aggregatedPublishLatency999pct']:.2f} ms")
print(f"  Max:    {d['aggregatedPublishLatencyMax']:.2f} ms")
```

---

## 8. Verified Test Results

Latest measured results (30 September 2026): one AeroStream broker on an AWS `c6id.2xlarge` (8 vCPU, 16 GiB), broker pinned to two physical cores and the load generator to the other two, 1 topic with 32 partitions, 1 KB messages, 8 producers, 8 consumers, `acks=1`, 2-minute warm-up plus 5-minute measurement, two rounds per workload (median shown):

| Offered load | Publish rate | Publish $p_{50}$ | $p_{99}$ | $p_{99.9}$ | End-to-end $p_{99}$ | Errors |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| 100,000 msg/s (fixed) | 100,082 msg/s (97.7 MB/s) | 0.7 ms | 1.4 ms | 2.3 ms | 2.0 ms | 0 |
| 200,000 msg/s (fixed) | 200,175 msg/s (195.5 MB/s) | 0.7 ms | 1.7 ms | 3.0 ms | 2.0 ms | 0 |
| Maximum rate | 271,350 msg/s (265.0 MB/s) | 105 ms | 1,104 ms | 1,376 ms | 1,119 ms | 0 |

Latency at the maximum rate reflects queueing at the saturation point; use the fixed-rate rows to judge latency. The full per-run results, method and raw data are in [`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md) and `benchmarks/omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/`.

To reproduce the EC2 run end to end (create the machine, run OMB, copy results back with `scp`, destroy the machine), use `benchmarks/aws-ec2/run-aerostream-8core.sh`; see [`benchmarks/aws-ec2/README.md`](../../benchmarks/aws-ec2/README.md).

---

## 9. Key Architectural Insights & Troubleshooting

1. **Synchronous Offset Commits vs. Continuous Pacing**:
   In older driver configs where `enable.auto.commit=false` was paired with per-record `commitAsync()`, thousands of commits were submitted to the broker per second. When offsets are mirrored to a Raft control plane, synchronous consensus snapshots can throttle throughput. Using periodic auto-commit (`auto.commit.interval.ms=5000`) avoids synthetic consensus contention.

2. **Zero-Copy Page Cache Retention**:
   Because consumers read via Linux `sendfile(2)`, recent batches remain hot in the kernel page cache. As seen in the results, **consume rate tracked publish rate 1:1** in every run, with an end-to-end median latency of about 1 ms at 100,000 to 200,000 msg/s.
