# Benchmark: AeroStream vs. Apache Kafka vs. Redpanda
**Limits per broker container**: `--cpus=2.0 --memory=2g` | **Host**: Intel Core i5-1235U (12 threads, laptop-class), Debian 13 / Linux 6.12, Docker 29.8.1 | **Dates**: September 26-27, 2026
**Versions**: Apache Kafka 4.3.1 (KRaft), Redpanda v26.2.3, AeroStream `integration/feature-gaps` and, for comparison, the previous `main`.

How the benchmark is run and every command: [comparison/PROCESS.md](comparison/PROCESS.md), [comparison/COMMANDS.md](comparison/COMMANDS.md).
The Kafka-port investigation (root causes, fixes, research sources): [KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md).
Raw tool output, per-run summaries and resource samples: [comparison/results/](comparison/results/). This document **replaces** the earlier
3-way benchmark; several of its headline figures did not reproduce (section 7).

---

## 1. Summary

**AeroStream's native data-plane port** (its own protocol; Kafka clients cannot use it), median of 3 runs, 10 concurrent closed-loop producers:

| Workload | AeroStream native | Kafka | Redpanda | Result |
| :--- | ---: | ---: | ---: | :--- |
| 50 MB (MB/s) | 856 (350-881) | 56 | 58 | wins by ~15x |
| 10 MB | 592 (347-863) | 171 | 219 | wins by ~3x |
| 1 KB (msgs/s) | 120K | 44K | 60K | wins by 2-2.7x |
| 1 MB | 379 (225-1,210) | 321 | 375 | tie |
| 100 B (msgs/s) | 122K | 141K | 172K | loses |

Caveats that matter: AeroStream drives 10 producers where the Kafka tool drives one; Redpanda flushes before acknowledging by default while Kafka and AeroStream acknowledge from the
page cache; AeroStream's large-message runs are very noisy (ranges above); its latency is closed-loop and not comparable to the pipelined tool's (section 6).

**AeroStream's Kafka port** (what Kafka clients use), driven by the same Kafka producer tool as Kafka and Redpanda, after the fixes described in
[KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md):

| Workload | AeroStream Kafka port | Kafka | Redpanda | Was (before any fix) |
| :--- | ---: | ---: | ---: | ---: |
| 100 B (msgs/s) | **174,520** | 141,243 | 171,527 | 18,925 |
| 1 KB (msgs/s) | **63,776** | 44,366 | 60,024 | 5,116 |
| 1 MB (MB/s) | 200 (165-209) | 321 | 375 | 201 |
| 10 MB (MB/s) | 144 (118-164) | 171 | 219 | 152 |
| 50 MB (MB/s) | 29 (27-47) | 56 | 58 | 51 (26-51) |

* Small messages went from 9-12x slower than Kafka to on par or ahead: a missing `TCP_NODELAY` (one fix took 1 KB from 5.1K to 51.5K msgs/s) plus hardware CRC32C, fewer syscalls per append and no zero-filled frame buffers (to 63.8K).
* **Large messages over the Kafka port are still behind** (1 MB 200 vs 321-375, 10 MB 144 vs 171-219, 50 MB 29 vs 56-58). These sizes did not change with the fixes; the cause is not isolated.
* On the previous `main` the Kafka port cannot be used by current Kafka clients at all (malformed ApiVersions v3 / Metadata v2+ replies), section 5.1.

**Footprint and startup**
* Idle memory of the AeroStream broker container: **1.4 MiB** (Kafka 303 MiB, Redpanda 141 MiB). Peak under 500 MB writes: 250-730 MiB depending on the session (mostly page cache) vs ~1.4 GiB for Kafka and Redpanda.
* Time until usable (boot-time.sh, 3 runs): Redpanda **0.75 s**, AeroStream **1.8-3.4 s** (Raft election plus broker registration; bimodal), Kafka **3.9-4.2 s**.

---

## 2. Setup in one paragraph
Each broker runs alone in a fresh container (single node, 1 partition, replication factor 1, `acks=1`), all with `--network host`.
Kafka and Redpanda are driven by Kafka's own `kafka-producer-perf-test.sh` with identical properties; AeroStream by the repository's `client bench`
(native protocol, 10 concurrent closed-loop producers, 5 for 50 MB) or, for the Kafka port, by the same Kafka tool. Every workload runs 3 times on fresh
topics; tables show the median run (and the min-max of the 3). Large-message workloads push 500 MB; small ones are 100,000 x 100 B and 50,000 x 1 KB. Resource
numbers are sampled with `docker stats` once per second.

---

## 3. Session results: native port vs Kafka vs Redpanda

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

---

## 4. Kafka port measurements

### 4.1 Progression, Kafka tool against AeroStream's Kafka port (messages per second, median of 3)
| Stage | 100 B | 1 KB | Session directory |
| :--- | ---: | ---: | :--- |
| Before any fix | 18,925 | 5,116 | `2026-09-26-integration-kafkaport` |
| + `TCP_NODELAY` and one write per small response | 128,205 | 51,546 | `2026-09-26-integration-kafkaport-fixed` |
| + hardware CRC32C, fewer syscalls per append, no zero-filled frames, registration backoff | 166,389 / **174,520** | 61,125 / **63,776** | `2026-09-27-integration-kafkaport-v2` / `-v3` |

Full final tables (`2026-09-27-integration-kafkaport-v3`):

| Workload | System | MB/s (median) | MB/s (min-max of runs) | msgs/s | avg ms | p50 ms | p95 ms | p99 ms | max ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100B | aerostream-kafka | 16.6 | 16-17 | 174520 | 16.03 | 6.00 | 44.00 | 45.00 | 205.00 |
| 1KB | aerostream-kafka | 62.3 | 61-64 | 63776 | 186.26 | 185.00 | 253.00 | 261.00 | 262.00 |
| 1MB | aerostream-kafka | 199.8 | 165-209 | 200 | 85.49 | 8.00 | 1018.00 | 1053.00 | 1056.00 |
| 10MB | aerostream-kafka | 144.0 | 118-164 | 14 | 698.26 | 585.00 | 1338.00 | 1390.00 | 1390.00 |
| 50MB | aerostream-kafka | 28.8 | 27-47 | 1 | 8343.30 | 8817.00 | 11849.00 | 11849.00 | 11849.00 |

Sizes from 1 MB up did not move with any fix (1 MB 201 -> 169 -> 200 MB/s, 10 MB 152 -> 158 -> 144, 50 MB 51 -> 26 -> 29; all within run-to-run spread). They remain the Kafka port's weak spot (section 1).

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
