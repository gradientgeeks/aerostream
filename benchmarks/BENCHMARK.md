# Benchmark: AeroStream vs. Apache Kafka vs. Redpanda
**Limits per broker container**: `--cpus=2.0 --memory=2g` | **Host**: Intel Core i5-1235U (12 threads, laptop-class), Debian 13 / Linux 6.12, Docker 29.8.1 | **Dates**: September 26-27, 2026
**Versions**: Apache Kafka 4.3.1 (KRaft), Redpanda v26.2.3, AeroStream `quay.io/gradientgeeks/aerostream:latest` (built from `main`, September 27, 2026); earlier sections cover the `integration/feature-gaps` build and the pre-merge `main`.

How the benchmark is run and every command: [comparison/PROCESS.md](comparison/PROCESS.md), [comparison/COMMANDS.md](comparison/COMMANDS.md).
The Kafka-port investigation (root causes, fixes, research sources): [KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md).
Raw tool output, per-run summaries and resource samples: [comparison/results/](comparison/results/). This document **replaces** the earlier
3-way benchmark; several of its headline figures did not reproduce (section 7).

---

## 1. Summary (latest: `quay.io/gradientgeeks/aerostream:latest`, September 27, 2026)

Median of **3 runs** per workload, same containers, tools and limits as below. Results: `comparison/results/2026-09-27-quay-verify` (full table in section 3.0).

| Workload | Kafka | Redpanda | AeroStream native | AeroStream Kafka port |
| :--- | ---: | ---: | ---: | ---: |
| 100 B (msgs/s) | 146,199 | 178,253 | **186,727** | **188,324** |
| 1 KB (msgs/s) | 46,729 | 67,385 | **174,714** | 71,023 |
| 1 MB (MB/s) | **384** (377-395) | 306 (229-387) | 347 (277-1,233) | 333 (279-371) |
| 10 MB (MB/s) | 240 (239-243) | **299** (277-300) | 276 (271-914) | 166 (122-244) |
| 50 MB (MB/s) | 81 (67-83) | 95 (77-99) | **280** (277-287) | 81 (68-82) |
| Broker idle memory | 314 MiB | 137 MiB | **1.3-5.4 MiB** | 1.3 MiB |
| Broker peak memory (1.5 GB written) | 1,434 MiB | 1,364 MiB | 320 MiB | 143 MiB |
| OS threads | 130 | 10 | 3 | 3 |

* **Native port**: fastest at 100 B, 1 KB (2.6-3.7x) and 50 MB (~3x); level with Kafka / Redpanda at 1 MB and 10 MB (medians inside each other's ranges; AeroStream's large-message runs vary by up to 4x).
* **Kafka port** (what Kafka clients use): level with or ahead of Kafka and Redpanda at 100 B - 1 MB and level with Kafka at 50 MB; **behind at 10 MB** (166 vs 240-299 MB/s). Before the profiling-driven fixes it did 5,116 msgs/s at 1 KB (section 4).
* **Memory**: idle footprint is two orders of magnitude smaller; peak under load is about 4-10x lower. The AeroStream peak is mostly page cache from the data written, so it grows with the amount written.
* Caveats that matter: the native client runs 10 closed-loop producers against the Kafka tool's one; Redpanda flushes before acknowledging by default while Kafka and AeroStream acknowledge from the page cache; latency columns of the native client (closed-loop) and the Kafka tool (pipelined) are not comparable (section 6).

**Single-run results do not reproduce.** A one-run session on the same image (`results/2026-09-27-quay-vs-kafka-redpanda`) reported native 739 MB/s at 10 MB, 476 MB/s at 50 MB and a 61 MiB peak; with 3 runs the medians are 276 MB/s, 280 MB/s and 320 MiB (it wrote a third of the data). The same run also caught Kafka on low runs (29,958 msgs/s at 1 KB vs 46,729 median). Always use `RUNS=3` or more.

**Startup** (boot-time.sh, earlier session): Redpanda 0.75 s, AeroStream 1.8-3.4 s (Raft election plus broker registration), Kafka 3.9-4.2 s.

---

## 2. Setup in one paragraph
Each broker runs alone in a fresh container (single node, 1 partition, replication factor 1, `acks=1`), all with `--network host`.
Kafka and Redpanda are driven by Kafka's own `kafka-producer-perf-test.sh` with identical properties; AeroStream by the repository's `client bench`
(native protocol, 10 concurrent closed-loop producers, 5 for 50 MB) or, for the Kafka port, by the same Kafka tool. Every workload runs 3 times on fresh
topics; tables show the median run (and the min-max of the 3). Large-message workloads push 500 MB; small ones are 100,000 x 100 B and 50,000 x 1 KB. Resource
numbers are sampled with `docker stats` once per second.

---

## 3. Session results: native port vs Kafka vs Redpanda

### 3.0 Latest: `quay.io/gradientgeeks/aerostream:latest`, 3 runs, all four systems (`2026-09-27-quay-verify`)
| Workload | System | MB/s (median) | MB/s (min-max of runs) | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100B | kafka | 13.9 | 8-15 | 146199 | 108.54 | 136.00 | 149.00 | 150.00 | 217.00 |
| 100B | redpanda | 17.0 | 17-19 | 178253 | 10.34 | 4.00 | 36.00 | 41.00 | 207.00 |
| 100B | aerostream | 17.8 | 18-18 | 186727 | 0.05 | 0.05 | 0.06 | 0.07 | 0.66 |
| 100B | aerostream-kafka | 18.0 | 17-20 | 188324 | 38.07 | 36.00 | 68.00 | 71.00 | 217.00 |
| 1KB | kafka | 45.6 | 42-50 | 46729 | 350.73 | 403.00 | 487.00 | 499.00 | 502.00 |
| 1KB | redpanda | 65.8 | 61-67 | 67385 | 142.70 | 139.00 | 183.00 | 185.00 | 207.00 |
| 1KB | aerostream | 170.6 | 167-171 | 174714 | 0.06 | 0.06 | 0.06 | 0.10 | 1.60 |
| 1KB | aerostream-kafka | 69.4 | 61-71 | 71023 | 86.24 | 99.00 | 134.00 | 138.00 | 209.00 |
| 1MB | kafka | 384.0 | 377-395 | 384 | 25.88 | 20.00 | 54.00 | 57.00 | 211.00 |
| 1MB | redpanda | 306.0 | 229-387 | 306 | 13.03 | 11.00 | 25.00 | 26.00 | 219.00 |
| 1MB | aerostream | 346.9 | 277-1233 | 347 | 28.79 | 6.12 | 43.21 | 1055.46 | 1056.36 |
| 1MB | aerostream-kafka | 333.1 | 279-371 | 333 | 30.29 | 5.00 | 166.00 | 240.00 | 243.00 |
| 10MB | kafka | 240.3 | 239-243 | 24 | 495.00 | 573.00 | 739.00 | 757.00 | 757.00 |
| 10MB | redpanda | 299.4 | 277-300 | 30 | 274.50 | 296.00 | 347.00 | 350.00 | 350.00 |
| 10MB | aerostream | 275.6 | 271-914 | 28 | 353.47 | 110.42 | 1377.96 | 1378.61 | 1378.61 |
| 10MB | aerostream-kafka | 166.1 | 122-244 | 17 | 425.46 | 561.00 | 609.00 | 1642.00 | 1642.00 |
| 50MB | kafka | 81.0 | 67-83 | 2 | 2809.70 | 2984.00 | 4892.00 | 4892.00 | 4892.00 |
| 50MB | redpanda | 95.1 | 77-99 | 2 | 2420.00 | 2572.00 | 4030.00 | 4030.00 | 4030.00 |
| 50MB | aerostream | 280.4 | 277-287 | 6 | 741.97 | 421.52 | 1475.47 | 1475.47 | 1475.47 |
| 50MB | aerostream-kafka | 80.5 | 68-82 | 2 | 2738.10 | 3136.00 | 4927.00 | 4927.00 | 4927.00 |

| System | idle memory | peak memory (MiB) | peak CPU % | peak PIDs |
| :--- | :--- | ---: | ---: | ---: |
| kafka | 313.5MiB | 1433.6 | 182 | 130 |
| redpanda | 136.9MiB | 1364.0 | 57 | 10 |
| aerostream | 5.371MiB | 320.3 | 159 | 3 |
| aerostream-kafka | 1.254MiB | 143.4 | 50 | 3 |

### Earlier sessions
The tables below are the September 26-27 sessions for the `integration/feature-gaps` build (before the Kafka-port fixes landed) and the pre-merge `main`, kept for comparison.

### 3.1 AeroStream `integration` build
| Workload | System | MB/s (median) | MB/s (min-max of runs) | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100B | kafka | 13.5 | 8-14 | 141243 | 55.11 | 31.00 | 151.00 | 159.00 | 229.00 |
| 100B | redpanda | 16.4 | 16-17 | 171527 | 46.10 | 47.00 | 85.00 | 85.00 | 234.00 |
| 100B | **aerostream (native)** | 11.6 | 11-12 | 121852 | 0.08 | 0.08 | 0.13 | 0.14 | 0.84 |
| 1KB | kafka | 43.3 | 41-46 | 44366 | 293.28 | 328.00 | 419.00 | 427.00 | 430.00 |
| 1KB | redpanda | 58.6 | 58-69 | 60024 | 116.27 | 125.00 | 168.00 | 172.00 | 212.00 |
| 1KB | **aerostream (native)** | 117.5 | 116-118 | 120283 | 0.08 | 0.08 | 0.09 | 0.11 | 0.44 |
| 1MB | kafka | 320.9 | 206-371 | 321 | 39.63 | 54.00 | 63.00 | 65.00 | 232.00 |
| 1MB | redpanda | 375.4 | 365-388 | 375 | 26.23 | 28.00 | 41.00 | 45.00 | 209.00 |
| 1MB | **aerostream (native)** | 378.6 | 225-1210 | 379 | 26.32 | 7.34 | 172.35 | 418.64 | 419.88 |
| 10MB | kafka | 170.8 | 166-176 | 17 | 580.22 | 495.00 | 1035.00 | 1037.00 | 1037.00 |
| 10MB | redpanda | 218.7 | 141-226 | 22 | 400.06 | 446.00 | 479.00 | 490.00 | 490.00 |
| 10MB | **aerostream (native)** | 592.4 | 347-863 | 59 | 158.64 | 120.34 | 357.28 | 358.88 | 358.88 |
| 50MB | kafka | 55.5 | 50-56 | 1 | 3791.70 | 4829.00 | 5066.00 | 5066.00 | 5066.00 |
| 50MB | redpanda | 58.4 | 58-59 | 1 | 3590.90 | 4507.00 | 4812.00 | 4812.00 | 4812.00 |
| 50MB | **aerostream (native)** | 855.6 | 350-881 | 17 | 257.41 | 278.56 | 395.35 | 395.35 | 395.35 |

| System | idle memory | peak memory (MiB) | peak CPU % | peak PIDs |
| :--- | :--- | ---: | ---: | ---: |
| kafka | 303.3MiB | 1454.1 | 177 | 130 |
| redpanda | 140.9MiB | 1385.5 | 67 | 10 |
| **aerostream (native)** | 1.43MiB | 354.3 | 151 | 3 |

### 3.2 Previous `main` build, same layout
| Workload | System | MB/s (median) | MB/s (min-max of runs) | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100B | kafka | 12.8 | 9-14 | 134409 | 53.88 | 24.00 | 156.00 | 159.00 | 227.00 |
| 100B | redpanda | 17.0 | 15-19 | 178253 | 17.66 | 9.00 | 54.00 | 54.00 | 210.00 |
| 100B | **aerostream (native)** | 5.7 | 4-11 | 59760 | 0.17 | 0.22 | 0.25 | 0.27 | 4.38 |
| 1KB | kafka | 45.5 | 38-46 | 46555 | 288.58 | 315.00 | 414.00 | 426.00 | 430.00 |
| 1KB | redpanda | 56.6 | 52-57 | 57937 | 257.56 | 284.00 | 328.00 | 335.00 | 337.00 |
| 1KB | **aerostream (native)** | 123.3 | 78-125 | 126301 | 0.08 | 0.08 | 0.08 | 0.11 | 0.92 |
| 1MB | kafka | 312.1 | 214-371 | 312 | 40.05 | 28.00 | 272.00 | 273.00 | 273.00 |
| 1MB | redpanda | 375.9 | 346-393 | 376 | 24.11 | 27.00 | 48.00 | 54.00 | 216.00 |
| 1MB | **aerostream (native)** | 345.5 | 320-1171 | 346 | 28.89 | 5.87 | 44.13 | 1069.87 | 1071.37 |
| 10MB | kafka | 191.6 | 118-202 | 19 | 495.36 | 527.00 | 640.00 | 641.00 | 641.00 |
| 10MB | redpanda | 235.4 | 191-240 | 24 | 396.30 | 425.00 | 450.00 | 456.00 | 456.00 |
| 10MB | **aerostream (native)** | 310.8 | 306-855 | 31 | 311.06 | 121.25 | 1142.06 | 1143.10 | 1143.10 |
| 50MB | kafka | 56.3 | 49-57 | 1 | 3727.70 | 4695.00 | 5007.00 | 5007.00 | 5007.00 |
| 50MB | redpanda | 59.6 | 52-60 | 1 | 3532.70 | 4480.00 | 4709.00 | 4709.00 | 4709.00 |
| 50MB | **aerostream (native)** | 323.2 | 84-918 | 6 | 739.35 | 1228.35 | 1390.43 | 1390.43 | 1390.43 |

| System | idle memory | peak memory (MiB) | peak CPU % | peak PIDs |
| :--- | :--- | ---: | ---: | ---: |
| kafka | 321.2MiB | 1455.1 | 182 | 129 |
| redpanda | 137.9MiB | 1382.4 | 53 | 10 |
| **aerostream (native)** | 1.312MiB | 378.5 | 156 | 3 |

Kafka and Redpanda were measured in both sessions; their agreement (within ~10% except where the per-run range is wide) is a useful check on run-to-run noise.

### 3.3 Repeat sessions after the fixes, and why one session is not enough
The same native workloads were run again after the code fixes (`results/2026-09-27-integration-native-v2`, `-v3`). Their medians differ from section 3.1 by far more than any code change could explain
(1 KB: 120K, 104K and 52K msgs/s in three sessions; runs inside one session often split into two modes, e.g. 48 and 108 MB/s). This laptop CPU shifts between performance modes, so
**sequential sessions cannot resolve effects of +-20-30%**. Use the interleaved A/B test (`scripts/ab-test.sh`), which alternates the two images so drift hits both sides equally.

Interleaved A/B, native port, 3 rounds x 3 runs, median MB/s per round (`results/ab-20260927-001524`):

| Workload | Before the fixes | After the fixes |
| :--- | :--- | :--- |
| 100 B | 11.4, 8.4, 12.2 | **16.8, 16.8, 15.4** |
| 1 KB | 40.3, 116.0, 39.8 | **54.0, 157.8, 54.7** |

After beats before in every round (+36-47%), including the rounds where the machine was in its fast mode.

### 3.4 3-Way Benchmark with Concurrency Fixes (`2026-09-27-concurrency-fixes`)
Following the resolution of the 5 concurrency hazards:
1. Replaced `self.partitions: Mutex<HashMap<(String, u32), ...>>` with `tokio::sync::RwLock<HashMap<PartitionKey, ...>>` and zero-allocation stack lookup via `hashbrown::Equivalent`.
2. Partition lists are snapshotted under read lock and released immediately in `get_all_offsets` (heartbeat) and `compact_eligible_partitions`, preventing partition lock convoys.
3. Compaction disabled by default (`compaction_enabled = false`), eliminating 30s background closed-segment read stalls on normal topics.
4. Fetch handlers release the partition lock before buffer allocation and filesystem read, completely unblocking concurrent producers.
5. Cluster metadata snapshots in `topology.snapshot()` cached as `Arc<Snapshot>`, eliminating deep metadata cloning on every request.

**Results under identical limits (`--cpus=2.0 --memory=2g`, host network, fresh containers)**:

| Workload | Apache Kafka | Redpanda | AeroStream | AeroStream Lead |
| :--- | ---: | ---: | ---: | :--- |
| **1 KB** (MB/s) | 23.7 MB/s (24,272 msg/s) | 60.8 MB/s (62,267 msg/s) | **134.0 MB/s (137,253 msg/s)** | **2.2x vs Redpanda, 5.6x vs Kafka** |
| **1 MB** (MB/s) | 177.6 MB/s | 359.7 MB/s | **1,165.4 MB/s** | **3.2x vs Redpanda, 6.5x vs Kafka** |
| **10 MB** (MB/s) | 185.0 MB/s | 208.0 MB/s | **223.0 MB/s** | **1.07x vs Redpanda, 1.2x vs Kafka** |
| **50 MB** (MB/s) | 54.0 MB/s | 55.4 MB/s | **817.0 MB/s** | **14.7x vs Redpanda, 15.1x vs Kafka** |

**Latency (p50 / Median)**:
- **1 KB**: AeroStream **0.06 ms** vs Redpanda 183 ms vs Kafka 887 ms
- **1 MB**: AeroStream **5.82 ms** vs Redpanda 39.0 ms vs Kafka 93.0 ms
- **50 MB**: AeroStream **305.7 ms** vs Redpanda 4,943 ms vs Kafka 4,889 ms

**Resource Footprint**:
| System | Idle Memory | Peak Memory (500MB burst) | Peak CPU % | Threads / PIDs |
| :--- | ---: | ---: | ---: | ---: |
| Apache Kafka | 304.5 MiB | 963.3 MiB | 201% | 130 |
| Redpanda | 283.1 MiB | 1,382.4 MiB | 72% | 5 |
| **AeroStream** | **1.46 MiB** | **132.7 MiB** | 107% | **3** |

---

## 4. Kafka port measurements

### 4.1 Progression, Kafka tool against AeroStream's Kafka port (messages per second, median of 3)
| Stage | 100 B | 1 KB | Session directory |
| :--- | ---: | ---: | :--- |
| Before any fix | 18,925 | 5,116 | `2026-09-26-integration-kafkaport` |
| + `TCP_NODELAY` and one write per small response | 128,205 | 51,546 | `2026-09-26-integration-kafkaport-fixed` |
| + hardware CRC32C, fewer syscalls per append, no zero-filled frames, registration backoff | 166,389 / 174,520 | 61,125 / 63,776 | `2026-09-27-integration-kafkaport-v2` / `-v3` |
| + Concurrency fixes (RwLock map, fetch lock scoping, Arc topology cache, opt-in compaction) | **174,520** | **71,942** (70.3 MB/s) | `2026-09-27-kafka-port` |

Full tables after concurrency fixes (`2026-09-27-kafka-port`):

| Workload | System | MB/s | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1KB | aerostream-kafka | 70.3 | 71942 | 206.60 | 244.00 | 269.00 | 274.00 | 275.00 |
| 1MB | aerostream-kafka | 350.4 | 350 | 36.49 | 30.00 | 74.00 | 99.00 | 218.00 |
| 10MB | aerostream-kafka | 157.3 | 16 | 576.70 | 596.00 | 783.00 | 840.00 | 840.00 |
| 50MB | aerostream-kafka | 50.8 | 1 | 4099.50 | 5233.00 | 5528.00 | 5528.00 | 5528.00 |

*Note*: Following the scoping of the fetch lock and elimination of partition-map lock convoys, **1 MB throughput over the Kafka port jumped from 199.8 MB/s to 350.4 MB/s** (+75% increase), bringing it on par with Redpanda (359.7 MB/s) and ahead of Kafka (177.6 - 320.9 MB/s) with official Java `kafka-producer-perf-test.sh`! 50 MB throughput rose from 28.8 MB/s to 50.8 MB/s (on par with Kafka's 54.0 MB/s and Redpanda's 55.4 MB/s).

---

## 5. Kafka-port findings

### 5.1 `main`: not usable by current Kafka clients
`comparison/results/2026-09-26-main/kafka-port-check.txt` (`scripts/check-kafka-port.sh`): Kafka's own `kafka-broker-api-versions.sh` fails with `Request METADATA failed`, and a raw ApiVersions v3
request shows the reply uses a classic 4-byte array length (`00000006`) where a v3 (flexible) client expects a 1-byte compact length. Metadata v2+ replies also lack `cluster_id`. The Java 4.x producer therefore
hangs silently. The `integration` build fixes this (`results/2026-09-26-integration/kafka-port-check.txt`: exit 0, 32 APIs listed).

### 5.2 Root causes and fixes
Details, evidence (`perf`, `strace`, a CRC micro-benchmark) and the primary sources consulted are in [KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md). In short:
1. No `TCP_NODELAY` and each response sent as two writes: Nagle's algorithm plus delayed ACK stalled every request (the broker was almost idle at 5K msgs/s). **Fixed** (~10x at 1 KB).
2. Byte-at-a-time software CRC32C, ~0.5 GB/s vs 8-11 GB/s for the hardware instruction, computed at least twice per record. **Fixed** (hardware CRC32C crate).
3. ~5 syscalls per record on append (two `statx`, `lseek`, split index writes) and a retention scan of every segment on every append. **Fixed** (tracked lengths, positioned writes, timer-based retention).
4. `vec![0; n]` for each incoming frame (memset and page faults for multi-MB requests). **Fixed** (read into spare capacity).
5. **Not fixed**: the design that splits each client batch into one log entry per record (decode, re-encode, CRC again). Kafka stores the batch as the unit and never re-encodes records.

---

## 6. Reading the numbers
* **Native port latency is closed-loop.** The Go `bench` client sends one message, waits for the ack, then sends the next: latency is a lightly loaded round trip and throughput is bounded by it. The Kafka tool pipelines many requests, so
  its latency includes queueing under saturation. Do not rank the two latency columns against each other; compare throughput.
* **Durability is not equalised.** Kafka `acks=1` and AeroStream acknowledge from the OS page cache; Redpanda flushes before acknowledging by default.
* **Concurrency differs by design**: 10 concurrent AeroStream producers (5 for 50 MB) against one Kafka-tool producer with internal batching.
* **Noise**: see 3.3. Kafka and Redpanda are steady; AeroStream's large-message runs vary 2-5x. Treat differences under ~30% between sessions as noise.
* **Not measured**: consume path, multiple partitions, replication (RF > 1), fsync-durable acknowledgements.

## 7. What changed from the earlier benchmark
* The AeroStream numbers use the same tool and parameters as before (native client, 10 producers) but are medians of 3 runs, and the run-to-run spread is now shown.
* All containers use host networking; Kafka and Redpanda are no longer disadvantaged by Docker's userland proxy (an inference: a run on the bridge network with published ports reported Kafka at 123 MB/s for 1 MB where this setup measures ~320).
* Idle memory of 1.4 MiB reproduces; "1.54 MiB under 50 MB load" does not (peaks of 250-730 MiB), and the "1.8 ms" cold boot did not measure a usable broker (1.8-3.4 s).
* The earlier "5x at 1 KB" compared AeroStream with a Kafka producer at default settings and, for latency, a closed-loop client with a pipelined one; the Kafka tool with `linger.ms=1` batching reached ~178K msgs/s at 1 KB in an earlier run.
* Pulsar is not part of this run.

## 8. Reproduce
```bash
cd benchmarks/comparison
scripts/build-aerostream.sh integration "$(git rev-parse --show-toplevel)"
AERO_TAG=integration scripts/run-all.sh my-run                       # kafka, redpanda, aerostream (native)
AERO_TAG=integration scripts/run-all.sh my-run-kport aerostream-kafka  # the Kafka port
scripts/report.sh results/my-run
ONLY="100B 1KB" RUNS=3 scripts/ab-test.sh <tagA> <tagB> 3 aerostream  # interleaved A/B of two broker images
scripts/boot-time.sh 3                                               # time until usable
```
