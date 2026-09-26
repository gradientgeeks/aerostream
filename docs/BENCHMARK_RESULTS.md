# Benchmark: AeroStream vs. Apache Kafka vs. Redpanda vs. Apache Pulsar
**Hardware limits (enforced per broker container)**: `--cpus=2.0 --memory=2g --memory-swap=2g`
**Host**: 12th Gen Intel Core i5-1235U (12 threads, laptop-class, frequency-scaled), Debian 13 (Linux 6.12, x86_64), Docker 29.8.1
**Date**: September 26, 2026
**Versions**: Apache Kafka 4.3.1 (KRaft), Redpanda v26.2.3, Apache Pulsar 4.2.4 (standalone), AeroStream `integration/feature-gaps` @ `b9ea11e` (controller with persistent Raft log, Rust broker with compression / transactions / share groups / admin API)

> This document **replaces** the earlier 3-way benchmark. Its headline numbers (1.8 ms cold boot, 1.5 MiB RAM,
> 13x at 50 MB, 5-15,000x lower latency) do not reproduce under the methodology below; see section 6.
> Everything here comes from `deploy/benchmark/` and the raw data is in `deploy/benchmark/results-2026-09-26.json`.

---

## 1. Summary

* **Large messages over AeroStream's native data-plane port are the strongest result.** At 1 MB it moves
  **1,018 MB/s** (3.0x Redpanda, 3.5x Kafka, 8.2x Pulsar). At 50 MB it moves **719 MB/s**, about 13x Kafka and Redpanda (both ~53-55 MB/s) and 2.5x Pulsar
  (284 MB/s, using Pulsar's message chunking). At 10 MB the lead is narrow (299 vs 248 MB/s for Pulsar) and run-to-run spread is wide (124-299 MB/s).
* **Over the Kafka wire protocol - the port real Kafka clients use - AeroStream is not competitive yet.** With the same Kafka 4.3.1 producer
  it reaches 0.46x Kafka's message rate at 100 B and 1 KB, roughly parity at 1 MB, and 0.67x / 0.46x at 10 MB / 50 MB.
  Latency under saturation is 0.6-0.7 s p50 for small messages (the broker cannot keep up, so requests queue).
* **Small messages: Kafka, Redpanda and Pulsar lead.** Redpanda reaches 534K msg/s at 100 B, Kafka 487K, Pulsar 254K, AeroStream (Kafka port) 223K.
  The native-port figure for small messages (97K msg/s) is a closed-loop, single-producer number bounded by a ~10 us round trip; it is not a saturation throughput (section 4).
* **Footprint is where AeroStream wins clearly.** Idle **~9 MiB** vs 213-708 MiB; peak under load 145-345 MiB vs 909-1,798 MiB (Kafka reached 88% of its 2 GiB limit).
* **Cold start is not a win.** AeroStream's port opens in 0.24 s, but the broker is usable only after Raft election and broker registration: **3.4 s**.
  Redpanda is ready in 0.7 s, Kafka in 2.7 s, Pulsar in 6.3 s.
* **Durability settings differ** between systems (section 2.3): AeroStream and Kafka acknowledge from the OS page cache; Redpanda and Pulsar acknowledge after an fsync by default.
  That favours the first two and must be kept in mind when reading throughput.

---

## 2. Methodology

### 2.1 Setup
* One broker per fresh container, single node, **1 partition, replication factor 1**, `acks=1` semantics. Data on the container's overlay filesystem (host SSD).
* The load generator runs in a **separate unconstrained container that shares the broker's network namespace** (no NAT, no advertised-listener effects).
* Each workload is run **3 times** on fresh topics; tables show the **median run** (by MB/s). Per-run values are in the JSON.
* Payload sweep: 100 B x 500,000, 1 KB x 300,000, and 1 MB x 500, 10 MB x 50, 50 MB x 10 (500 MB each for the large-message tests). Random compressibility is irrelevant: no compression.
* Memory / threads / CPU are sampled with `docker stats` during the runs; idle memory is taken 13 s after start-up.

| System | Broker configuration | Load generator |
| :--- | :--- | :--- |
| Apache Kafka | KRaft single node, heap `-Xms1g -Xmx1g`, `message.max.bytes` = 100 MiB | `kafka-producer-perf-test.sh` (Kafka 4.3.1 client) |
| Redpanda | `--smp 2 --memory 1G --overprovisioned`, `kafka_batch_max_bytes` = 100 MiB | same Kafka perf tool |
| Apache Pulsar | standalone (broker + bookie + ZK in one JVM), heap 512 MiB + 1.1 GiB direct, no functions worker | `PulsarGen.java`: pipelined `sendAsync`, batching 1 ms / 1000 msgs |
| AeroStream, **Kafka port** (9092) | all-in-one image (Go controller + Rust broker + UI) | same Kafka perf tool |
| AeroStream, **native port** (9091) | same container | the repo's Go `bench` client: **1 producer, closed loop** |

Kafka-API producer properties (identical for Kafka, Redpanda and AeroStream's Kafka port): `acks=1 linger.ms=1 batch.size=262144 max.request.size=100MiB buffer.memory=256MiB`, unthrottled.
`PulsarGen` reproduces the Kafka tool's semantics (unthrottled async sends, latency = send to ack, exact wall time) because `pulsar-perf` only reports throughput at 1 s resolution.
Cross-check: `PulsarGen` reaches ~170K msg/s at 1 KB on a warm broker vs ~150K msg/s from `pulsar-perf`.

### 2.2 Large messages
Kafka and Redpanda need their message-size limits raised (done). Pulsar's bookie rejected single 50 MB entries and ran out of direct memory in 2 GiB,
so payloads over 4 MiB use **Pulsar's message chunking**, its supported mechanism. The 10 MB and 50 MB results are therefore chunked (Pulsar's default max message size is 5 MiB); 1 MB is sent unchunked.

### 2.3 Durability of an acknowledged write (not equalised)
| System | What an ack means in this test |
| :--- | :--- |
| Apache Kafka | `acks=1`: written to the OS page cache, no fsync |
| AeroStream | `write` + `flush` (no fsync): in the OS page cache |
| Redpanda | default settings: flushed (fsync) before ack unless write caching is enabled |
| Apache Pulsar | BookKeeper journal with fsync (group commit) |

### 2.4 Caveats
* Laptop-class CPU with frequency scaling and other background load (host load average ~1.4 at idle): expect run-to-run variance (see the JSON; e.g. AeroStream native 10 MB ranged 124-299 MB/s, Kafka 1 KB 145-213 MB/s).
* Produce path only. Consume throughput, multiple partitions / producers, and replication (RF>1) were **not** measured.

---

## 3. Results

### Throughput summary (MB/s, median of runs)

| Payload | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| 100B | 46.5 | 50.9 | 24.2 | 21.2 | 9.3 |
| 1KB | 174.4 | 120.0 | 175.2 | 80.5 | 70.9 |
| 1MB | 287.2 | 343.2 | 124.4 | 299.9 | 1,018.3 |
| 10MB | 153.6 | 216.0 | 247.6 | 102.2 | 298.5 |
| 50MB | 54.7 | 52.6 | 284.1 | 25.3 | 719.4 |

#### 100B payload

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Data throughput (MB/s)** | 46.5 | 50.9 | 24.2 | 21.2 | 9.3 |
| **Messages / s** | 487,329 | 533,618 | 254,160 | 222,618 | 97,125 |
| **Avg latency (ms)** | 36.3 | 1.8 | 3.4 | 654 | 0.010 |
| **p50 latency (ms)** | 30.0 | 2.0 | 3.4 | 682 | 0.010 |
| **p95 latency (ms)** | 80.0 | 3.0 | 4.6 | 1,028 | 0.011 |
| **p99 latency (ms)** | 88.0 | 5.0 | 6.0 | 1,091 | 0.014 |
| **Max latency (ms)** | 248 | 224 | 21.3 | 1,097 | 0.297 |
| **Failed runs** | 0 | 0 | 0 | 0 | 0 |

#### 1KB payload

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Data throughput (MB/s)** | 174.4 | 120.0 | 175.2 | 80.5 | 70.9 |
| **Messages / s** | 178,571 | 122,850 | 179,413 | 82,440 | 72,569 |
| **Avg latency (ms)** | 26.5 | 3.2 | 5.0 | 723 | 0.014 |
| **p50 latency (ms)** | 18.0 | 2.0 | 3.0 | 672 | 0.014 |
| **p95 latency (ms)** | 67.0 | 9.0 | 8.5 | 1,634 | 0.017 |
| **p99 latency (ms)** | 72.0 | 22.0 | 47.8 | 1,701 | 0.023 |
| **Max latency (ms)** | 228 | 508 | 52.2 | 1,761 | 54.9 |
| **Failed runs** | 0 | 0 | 0 | 0 | 0 |

#### 1MB payload

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Data throughput (MB/s)** | 287.2 | 343.2 | 124.4 | 299.9 | 1,018.3 |
| **Messages / s** | 287 | 343 | 124 | 300 | 1,018 |
| **Avg latency (ms)** | 155 | 9.5 | 267 | 28.1 | 0.982 |
| **p50 latency (ms)** | 50.0 | 4.0 | 288 | 26.0 | 0.653 |
| **p95 latency (ms)** | 395 | 38.0 | 398 | 68.0 | 0.905 |
| **p99 latency (ms)** | 414 | 65.0 | 483 | 93.0 | 2.1 |
| **Max latency (ms)** | 414 | 238 | 584 | 355 | 49.6 |
| **Failed runs** | 0 | 0 | 0 | 0 | 0 |

#### 10MB payload

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Data throughput (MB/s)** | 153.6 | 216.0 | 247.6 | 102.2 | 298.5 |
| **Messages / s** | 15 | 22 | 25 | 10 | 30 |
| **Avg latency (ms)** | 985 | 509 | 46.3 | 1,563 | 33.5 |
| **p50 latency (ms)** | 712 | 555 | 34.7 | 1,953 | 9.7 |
| **p95 latency (ms)** | 1,810 | 784 | 43.9 | 2,267 | 54.0 |
| **p99 latency (ms)** | 1,814 | 809 | 580 | 2,338 | 1,020 |
| **Max latency (ms)** | 1,814 | 809 | 580 | 2,338 | 1,020 |
| **Failed runs** | 0 | 0 | 0 | 0 | 0 |

#### 50MB payload

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Data throughput (MB/s)** | 54.7 | 52.6 | 284.1 | 25.3 | 719.4 |
| **Messages / s** | 1 | 1 | 6 | 1 | 14 |
| **Avg latency (ms)** | 3,848 | 4,006 | 186 | 9,991 | 69.5 |
| **p50 latency (ms)** | 4,900 | 5,097 | 148 | 14,925 | 62.1 |
| **p95 latency (ms)** | 5,127 | 5,470 | 467 | 15,111 | 102 |
| **p99 latency (ms)** | 5,127 | 5,470 | 467 | 15,111 | 102 |
| **Max latency (ms)** | 5,127 | 5,470 | 467 | 15,111 | 102 |
| **Failed runs** | 0 | 0 | 0 | 0 | 0 |

#### Resource footprint and cold boot (container limit: 2 CPU / 2 GiB)

| Metric | Apache Kafka (KRaft) | Redpanda | Apache Pulsar (standalone) | AeroStream (Kafka port) | AeroStream (native port) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Idle memory (MiB)** | 369.0 | 213.0 | 707.9 | 9.1 | 8.8 |
| **Peak memory under load (MiB)** | 1,798.1 | 908.6 | 979.9 | 344.9 | 144.7 |
| **Peak memory (% of 2 GiB)** | 87.8 | 44.4 | 47.8 | 16.8 | 7.1 |
| **Peak OS threads / PIDs** | 105 | 10 | 172 | 13 | 15 |
| **Peak CPU (% of one core, limit 200)** | 153 | 73 | 204 | 112 | 81 |
| **Cold boot to ready (s, median of 3)** | 2.68 | 0.73 | 6.31 | 3.41 | 3.41 |


---

## 4. Reading the AeroStream numbers

* **Native port vs Kafka port.** The native port (`sendfile` data plane, one record = one frame) reaches 1 GB/s at 1 MB. The Kafka port runs the same storage engine but adds Kafka framing and record-batch handling
  and is 3.4x slower at 1 MB and 28x slower at 50 MB. Kafka clients only ever use the Kafka port.
* **Why the Kafka port is slow is not yet profiled.** Candidates visible in the code: every record becomes its own log entry (needed so each record keeps its own offset), and each `PartitionLog::append` does several syscalls
  (file metadata, write, flush, two index writes, retention check) per entry; large payloads are also parsed/validated and copied before being appended. These are hypotheses, not measurements.
* **Native small-message numbers are closed-loop.** The Go `bench` client sends one message, waits for its ack, then sends the next: throughput is `1 / round-trip`, so 10 us latency gives ~100K msg/s.
  The Kafka/Pulsar tools pipeline thousands of requests, so their latency includes queueing under saturation. Native and pipelined latencies must not be ranked against each other;
  compare the Kafka-port row against Kafka, Redpanda and Pulsar.
* **Redpanda and Pulsar acknowledge after fsync**, AeroStream and Kafka do not (section 2.3). Enabling Redpanda write caching or relaxing Pulsar journal sync would raise their throughput.

---

## 5. Where each system stands (from this data)

| Workload | Best result | Notes |
| :--- | :--- | :--- |
| 100 B / 1 KB, pipelined | Redpanda (100 B), Kafka / Pulsar (1 KB) | AeroStream Kafka port ~0.5x of the best |
| 1 MB | AeroStream native (1,018 MB/s) | Kafka port ~ Kafka (300 vs 287 MB/s) |
| 10 MB | AeroStream native (299 MB/s) and Pulsar (248 MB/s), close | AeroStream Kafka port is the slowest (102 MB/s) |
| 50 MB | AeroStream native (719 MB/s) | Pulsar 284 MB/s with chunking; Kafka / Redpanda ~54 MB/s; AeroStream Kafka port 25 MB/s |
| Memory | AeroStream (9 MiB idle, <=345 MiB peak) | Kafka reached 1.8 GiB |
| Time to usable | Redpanda 0.7 s | AeroStream 3.4 s (port open at 0.24 s), Kafka 2.7 s, Pulsar 6.3 s |

---

## 6. What changed from the previous benchmark

The earlier document compared AeroStream's native protocol (closed-loop Go client) against Kafka / Redpanda driven with default producer settings, and reported a "cold boot" of 1.8 ms and 1.5 MiB of RAM. Re-running with the tooling above:

* Cold boot: the port opens in 0.24 s, the cluster is usable in **3.4 s**; 1.8 ms did not measure a usable broker.
* Memory: **8.8 MiB idle, 145-345 MiB under load** for the whole all-in-one container (controller + broker), not 1.5 MiB.
* Small-message ratios flipped: with tuned batching (`linger.ms=1`, 256 KiB batches) Kafka reaches ~490K msg/s at 100 B instead of ~34K, so AeroStream's small-message "advantage" came from comparing a closed-loop client with an untuned open-loop one.
* Large-message advantage **does hold** for the native port (1 MB: 1,018 vs 287-343 MB/s; 50 MB: 719 vs ~54 MB/s), but not for the Kafka port.

---

## 7. Reproduce

```bash
# build the images / client, then (Docker required; ~30 min)
docker build -t aerostream:bench .
(cd client && CGO_ENABLED=0 go build -o /tmp/aeroclient .)
python3 deploy/benchmark/run_bench.py --aero-image aerostream:bench --aero-client /tmp/aeroclient --runs 3 --out results.json
python3 deploy/benchmark/report.py results.json     # markdown tables
```
`--systems kafka redpanda pulsar aerostream-kafka aerostream-native` and `--workloads 1KB 50MB` select subsets.
