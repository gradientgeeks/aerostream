# AeroStream Performance Benchmarks (OpenMessaging Benchmark)

**Benchmark standard**: [Linux Foundation OpenMessaging Benchmark (OMB)](https://github.com/openmessaging/benchmark)  
**Protocol under test**: Kafka wire protocol (`:9092`) in sections 1.1 and 3, AeroStream native protocol (`:9091`) in section 1.2; single broker, `acks=1`, no replication  
**Raw datasets**: [`benchmarks/omb-results/`](omb-results/)  
**Execution guides**: [`core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md`](../core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md), [`aws-ec2/README.md`](aws-ec2/README.md)  

The OpenMessaging Benchmark measures sustained throughput, backpressure and latency percentiles for messaging systems. This document reports AeroStream's results on two setups: a dedicated AWS EC2 machine (section 1) and a resource-capped container on a laptop (section 3). Section 4 explains the design choices behind the numbers, and section 5 reports storage-layer results for large partition counts.

---

## 1. Results on AWS EC2 (`c6id.2xlarge`, 8 vCPU, 16 GiB)

**Dataset**: [`omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/`](omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/) (30 September 2026)  
**Image (this dataset)**: `quay.io/gradientgeeks/aerostream:latest` at that date, digest `sha256:1fd1a44a7c8c` (before the native-path and writeback-thread work in 1.2)

One AeroStream broker, 1 topic, 32 partitions, 1,024-byte messages, 8 producers, 8 consumers. Each run is a 2-minute warm-up (excluded) plus a 5-minute measurement (30 ten-second samples). Two rounds per workload; the table shows the median of the rounds.

### 1.1 Kafka Wire Protocol (:9092)

| Offered load | Publish rate | Publish $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | Broker cores busy | Errors |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 100,000 msg/s (fixed) | 100,000 msg/s (97.7 MB/s) | 0.7 ms | 1.2 ms | 1.4 ms | 2.3 ms | 14% | 0 |
| 200,000 msg/s (fixed) | 200,000 msg/s (195.5 MB/s) | 0.7 ms | 1.3 ms | 1.7 ms | 3.0 ms | 22% | 0 |
| Maximum rate | **271,350 msg/s** (265.0 MB/s) | — | — | 1,104 ms | — | 55% | 0 |

### 1.2 Native Protocol (:9091): Original Baseline vs Final (Writeback Fix)

**Protocol**: AeroStream native protocol (port 9091), custom OMB driver in `benchmarks/omb-driver-aerostream`, not the Kafka port  
**Image (final)**: `quay.io/gradientgeeks/aerostream:2026-10-01-writeback-thread` (digest `sha256:ad92b677…`), reproduced in a second run on 4 October 2026 (100k p99 1.2 ms, 200k p99 1.4 ms, max rate 287,459 msg/s)  
**Profile**: `c6id.2xlarge`, local NVMe, same machine and matrix as 1.1  
Figures are the median of 2 rounds, 1 KB messages, 32 partitions, 8 producers and 8 consumers:

| Workload | Metric | Original Baseline | Final (Writeback Fix) | Improvement |
| :--- | :--- | :---: | :---: | :---: |
| **100,000 msg/s** | $p_{50}$ latency | 1.2 ms | **0.7 ms** | **1.7× lower median latency** |
| (fixed offered load) | $p_{95}$ latency | 2.7 ms | **1.2 ms** | **2.3× lower tail latency** |
| | $p_{99}$ latency | 63.2 ms | **1.3 ms** | **48× lower tail latency ($p_{99}$)** |
| | $p_{99.9}$ latency | 94.2 ms | **1.8 ms** | **52× lower tail latency ($p_{99.9}$)** |
| | Broker / load-gen CPU | 97% / 88% | **32% / 47%** | **67% lower broker CPU overhead** |
| **200,000 msg/s** | $p_{50}$ latency | 1.8 ms | **0.8 ms** | **2.3× lower median latency** |
| (fixed offered load) | $p_{95}$ latency | 70.7 ms | **1.3 ms** | **54× lower tail latency** |
| | $p_{99}$ latency | 109.9 ms | **1.5 ms** | **73× lower tail latency ($p_{99}$)** |
| | $p_{99.9}$ latency | 145.8 ms | **3.8 ms** | **38× lower tail latency ($p_{99.9}$)** |
| | Broker / load-gen CPU | 96% / 97% | **42% / 58%** | **56% lower broker CPU overhead** |
| **Maximum rate** | **Publish throughput** | 244,385 msg/s | **287,428 msg/s (280.7 MB/s)** | **+18% higher throughput** |
| (unthrottled) | Publish $p_{99}$ | 1,009 ms | **149 ms** | **85% lower queueing tail** |
| | Broker / load-gen CPU | 95% / 96% | **67% / 51%** | **29% lower broker CPU at saturation** |

*Key Findings*: Paced page-cache writeback (`sync_file_range` / `posix_fadvise`) prevents kernel background flusher contention from blocking Tokio worker threads. At 200,000 msg/s, $p_{99}$ latency drops from **109.9 ms to 1.5 ms** while reducing broker CPU usage from **96% to 42%**, and maximum throughput reaches **287,428 msg/s (280.7 MB/s)**.

### Per-run results

| Workload | Round | Publish rate | Consume rate | $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | Max | End-to-end $p_{99}$ | Peak 10 s rate |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100k msg/s | 1 | 100,084 | 100,084 | 0.69 ms | 1.23 ms | 1.43 ms | 2.33 ms | 11.5 ms | 2.00 ms | 102,527 |
| 100k msg/s | 2 | 100,080 | 100,080 | 0.68 ms | 1.21 ms | 1.40 ms | 2.29 ms | 10.5 ms | 2.00 ms | 102,395 |
| 200k msg/s | 1 | 200,157 | 200,157 | 0.75 ms | 1.34 ms | 1.76 ms | 3.09 ms | 12.2 ms | 2.00 ms | 204,694 |
| 200k msg/s | 2 | 200,194 | 200,194 | 0.73 ms | 1.31 ms | 1.72 ms | 2.98 ms | 14.1 ms | 2.00 ms | 205,832 |
| Max rate | 1 | 271,231 | 271,231 | 170.6 ms | 1,028 ms | 1,290 ms | 1,543 ms | 2,059 ms | 1,303 ms | 285,971 |
| Max rate | 2 | 271,469 | 271,473 | 39.7 ms | 597 ms | 918 ms | 1,210 ms | 1,779 ms | 934 ms | 287,703 |

### What the results show

1. **Flat latency up to 200,000 msg/s.** On the Kafka port, publish $p_{99}$ is 1.4 ms at 100k msg/s and 1.7 ms at 200k msg/s, with the broker's cores only 14% and 22% busy.
2. **A saturation point at about 271,350 msg/s (265.0 MB/s).** Latency at that rate is queueing delay (p99 about 1.1 s), so compare latency at the fixed rates. The native protocol saturates higher: 287,428 msg/s (section 1.2).
3. **Consumers keep pace.** Consume rate equals publish rate in every run, with no growing backlog.
4. **Zero errors** in all six runs.
5. **CPU utilization at saturation.** At the maximum rate the broker's cores were 55% busy and the load generator's 62%.

### Test environment

| Component | Specification |
| :--- | :--- |
| Machine | AWS EC2 `c6id.2xlarge`, us-east-1a, one machine for broker and load generator |
| Processor | Intel Xeon Platinum 8375C @ 2.90 GHz, 8 vCPUs = 4 physical cores x 2 threads (AWS Nitro, KVM) |
| Memory / storage | 16 GiB RAM, 474 GB local NVMe (broker data directory) |
| Operating system | Amazon Linux 2023, Linux 6.18, Docker |
| Broker placement | Container with host networking, pinned to vCPUs `0,1,4,5` (two physical cores), 8 GiB limit; empty data directory and dropped page cache before every run |
| Load generator | OpenMessaging Benchmark (commit `5b1fa70`), pinned to vCPUs `2,3,6,7` (the other two physical cores) |
| Producer settings | `acks=1`, `linger.ms=1`, `batch.size=131072`, `max.in.flight.requests.per.connection=5` |
| Consumer settings | `auto.offset.reset=earliest`, auto-commit every 5 s, `max.partition.fetch.bytes=1048576` |

---

## 2. Reproducing the EC2 Benchmark

```bash
cd benchmarks/aws-ec2
./run-aerostream-8core.sh          # prints the plan and estimate; creates nothing
./run-aerostream-8core.sh --yes    # runs it: about 80 minutes, roughly $0.55 of EC2
```

The script creates one tagged EC2 machine, runs the workloads, copies the results to `benchmarks/aws-ec2/results/<run-id>/` with `scp`, destroys the machine and verifies that nothing is left. See [`aws-ec2/README.md`](aws-ec2/README.md).

---

## 3. Results in a Resource-Capped Container (Laptop)

**Hardware & environment**: Intel Core i5-1235U (12 threads, laptop class), Debian 13 / Linux 6.12, Docker 29.8.1  
**Resource constraints**: `--cpus=2.0 --memory=2g` for the broker container  
**Workload**: 1 topic, 16 partitions, 1 KB payload, 2 producers and 2 consumers (`aerostream-16p-1kb.yaml`)

These runs measure AeroStream at successive stages of development and under a small resource budget.

| Metric | Shard-per-Core | Async Archive | Producers Only |
| :--- | :---: | :---: | :---: |
| **Wire protocol** | Kafka (`:9092`) | Kafka (`:9092`) | Kafka (`:9092`) |
| **Workload profile** | 16p 1KB max rate | 16p 1KB max rate | 16p 1KB 300k rate |
| **Publish avg rate** | **217,100 msg/s** | 190,245 msg/s | 205,356 msg/s |
| **Publish avg throughput** | 212.0 MB/s | 185.8 MB/s | 200.5 MB/s |
| **Publish peak rate** | 243,460 msg/s | 207,881 msg/s | 207,642 msg/s |
| **Consume avg rate** | 217,143 msg/s | 190,341 msg/s | n/a (producers only) |
| **Publish latency $p_{50}$** | 5.3 ms | 1.6 ms | 330.0 ms |
| **Publish latency $p_{99}$** | 460.2 ms | 531.3 ms | 570.6 ms |
| **Publish latency $p_{99.9}$** | 504.9 ms | 1,368.1 ms | 746.4 ms |
| **Publish latency max** | 591.4 ms | 1,596.9 ms | 846.8 ms |
| **End-to-end latency $p_{50}$** | 14.0 ms | 4.0 ms | n/a |
| **End-to-end latency $p_{99}$** | 492.0 ms | 565.0 ms | n/a |
| **Broker peak memory** | 513 MiB | 494 MiB | 188 MiB |
| **Broker peak CPU** | 203% (of 200%) | 204% | 200% |
| **Errors** | 0 | 0 | 0 |
| **Dataset** | [`shard-kafka-wire-timeseries`](omb-results/shard-kafka-wire-timeseries/) | [`archive-async-timeseries`](omb-results/archive-async-timeseries/) | [`base-producers-only`](omb-results/base-producers-only/) |

Consumers kept pace in real time (217,143 consume msg/s against 217,100 publish msg/s), helped by a lazy per-partition `tokio::sync::Notify` and out-of-lock file reads. These were single runs on a machine with background desktop load, so expect run-to-run variation of roughly 20%.

---

## 4. Design Choices Behind the Numbers

### 4.1 Shard-per-Core avoids lock contention across partitions
AeroStream's Shard-per-Core engine assigns each partition to a worker thread by deterministic hashing:
$$\text{shard\_id} = \text{DefaultHasher}(topic, partition) \pmod{N}$$

Each shard runs as an isolated actor pinned to a CPU core with `libc::sched_setaffinity`. An append on partition 0 never acquires or waits on a lock for partition 1, so all partitions progress without cross-core cache-line invalidation.

### 4.2 Paced page-cache writeback
Under high ingress (> 200 MB/s) in a memory-limited container, dirty pages can accumulate until the kernel blocks writers for a synchronous flush. AeroStream starts writeback in the background with `libc::sync_file_range(SYNC_FILE_RANGE_WRITE)` every 8 MiB (`storage.writeback_bytes`). In the 16-partition capped-container run this sustained 212 MB/s with a maximum publish latency of 591 ms.

### 4.3 In-place base-offset patching
Instead of building a new buffer per record batch, AeroStream writes the assigned 8-byte `base_offset` directly into the log with positioned writes:
```rust
let log_file = self.active_log.get()?;
log_file.write_all_at(&off_bytes, pos)?;
if batch.len() > 8 {
    log_file.write_all_at(&batch[8..], pos + 8)?;
}
```
This zero-copy path kept broker memory at 513 MiB while processing 217,100 msg/s in the capped-container run.

### 4.4 Asynchronous zero-copy segment rollover
On rollover, closed segments are archived with POSIX hard links (`fs::hard_link`) on a background thread, sharing disk inodes with no copy and no memory overhead. In the `archive-async-timeseries` run the broker sustained 190,245 msg/s while continuously archiving sealed segments.

### 4.5 Batched appends for multi-record produce requests
A produce batch with several records is stored as one log entry per record. `PartitionLog::append_entries` writes all of a batch's entries with one log write and one index write per segment (instead of two syscalls per record) and runs retention, high-watermark and waiter wake-ups once per batch. The log and index bytes it produces are identical to appending record by record; a test compares them byte for byte across many segment rolls. Measured with a 128-record, 1 KB batch on an in-memory filesystem, the append step drops from about 850 ns to about 235 ns per record (3.4x to 3.6x faster over six rounds).

---

## 5. Partition Density

The active segment's file handles are held in a shared LRU pool (default 2,048 handles) instead of two descriptors per partition, offsets are found through a two-level sparse index (a 4-byte in-memory sample per 128 index entries plus one positioned read of the on-disk index), and idle partitions release their descriptors and index state (`storage.max_open_segment_files`, `storage.partition_idle_secs`).

A storage-layer probe with 10,000 partitions in one process (release build, single machine):

| Measurement | Result |
| :--- | :--- |
| Open file descriptors with 10,000 partitions active | about 2,050 (the pool limit) |
| Open file descriptors after all partitions go idle | 4 |
| Memory added by 10,000 partitions | about 15 MB (about 1.5 KB per partition) |
| Creating 10,000 partitions | 0.4 to 0.7 s |
| Appends spread evenly across all 10,000 partitions (worst case for the pool) | 81,000 to 86,000 appends/s |
| First fetch on each of 10,000 partitions | about 14 microseconds each |

This probe measures the storage layer only (no network, replication or consumer groups) and uses one append per partition per pass, which reopens files far more often than typical skewed traffic does.

---

## 6. Reproducing the Container Benchmark

```bash
# 1. Start AeroStream with container limits
docker run -d --name aerostream-broker \
  --cpus=2.0 --memory=2g \
  --network host \
  quay.io/gradientgeeks/aerostream:latest

# 2. Run the OMB workload
cd benchmarks/openmessaging-benchmark
./omb-run.sh quay.io/gradientgeeks/aerostream:latest workloads/aerostream-16p-1kb.yaml aerostream-kafkawire
```

Full configuration details and driver configurations are in [`core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md`](../core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md).
