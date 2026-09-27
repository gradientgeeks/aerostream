# Kafka-port performance: what was wrong, what was fixed, what is left

Investigation of why AeroStream's Kafka-protocol port was much slower than Kafka and Redpanda with the same producer tool.
Numbers are from [BENCHMARK.md](BENCHMARK.md); raw evidence is in `comparison/results/2026-09-26-kafka-port-profile/`.

## 1. Root causes found (with evidence)

| # | Cause | Evidence | Effect |
| :-- | :-- | :-- | :-- |
| 1 | **No `TCP_NODELAY`, and each response sent as two writes** (4-byte length, then body) | `grep set_nodelay` finds nothing in the broker; at 5K msgs/s the broker was almost idle (`perf`: 9 samples in 8 s) | Nagle waits for the first segment's ACK while the client delays it. Fix alone: 1 KB 5,116 -> 51,546 msgs/s, 100 B 18,925 -> 128,205 |
| 2 | **Byte-at-a-time software CRC32C** | micro-benchmark: 474-504 MB/s vs 8,089-10,946 MB/s for the SSE4.2 instruction (17-22x, identical result); `perf`: `parse_records` 41.8% + `encode_idempotent_records_batch` 9.0% of CPU | ~2 us of CRC per 1 KiB record, paid at least twice per record |
| 3 | **~5 syscalls per record** on append (`statx` x2, `lseek`, split index writes) and a **retention scan of every segment on every append** | `strace -c`: 67,770 `statx`, 67,770 `lseek`, 101,756 `write` for 50,000 records; ~8% of CPU in kernel path lookups | CPU and latency per record |
| 4 | **`vec![0; n]` for every incoming frame** | `perf` on 50 MB messages: page-fault and page-clearing symbols (`do_anonymous_page`, `clear_page_erms`) | memset plus page faults of the whole frame |
| 5 | **Each client batch is split into one log entry per record** (decode, re-encode, CRC again) so that every record has its own offset | `txn::batch::to_entries` | costs 2 and 3 multiplied by the record count; also prevents storing batches compressed |
| 6 | **Registration retried on a fixed 3 s sleep** | `grpc/mod.rs`; the broker retried 3 s after "not the cluster leader" | startup 3.4 s |

## 2. Fixes applied and measured
Kafka port, messages per second, median of 3 (Kafka tool, same properties as Kafka/Redpanda):

| Stage | 100 B | 1 KB |
| :--- | ---: | ---: |
| Before | 18,925 | 5,116 |
| 1: `TCP_NODELAY` + single write for responses <= 64 KiB | 128,205 | 51,546 |
| 2-4, 6: hardware CRC32C (`crc32c` crate), tracked lengths + positioned writes (`write_all_at`) + timer-based retention, read frames into spare capacity, registration backoff 200 ms -> 3 s | 174,520 | 63,776 |

For reference (same tool): Kafka 141,243 / 44,366, Redpanda 171,527 / 60,024. Interleaved A/B on the native port (which shares the append path): +47% at 100 B and +36% at 1 KB.
Startup improved from 3.4 s to 1.8-3.4 s (bimodal, see 3.3). Sizes from 1 MB up did not change.

An attempt to also stop pinning worker threads under a CPU quota was **reverted**: the small-message results got worse and the run-to-run variance did not improve, so the hypothesis was not supported by the data.

## 2a. Write stalls under a memory limit (fixed September 27)
Large-message noise and the collapse from 50 KB upward came from dirty page cache filling the container's memory limit plus a synchronous segment copy on every roll.
Fixed with paced `sync_file_range` writeback and hard-linked cold archives; see [BENCHMARK.md](BENCHMARK.md) section 0 for evidence and before/after numbers.
Kafka-port medians after the fix: 1 KB 67.7, 10 KB 160.5, 50 KB 215.5, 100 KB 246.0, 250 KB 380.6, 500 KB 380.0, 1 MB 346.3, 10 MB 203.8 MB/s.

## 3. Still open

### 3.1 Store the client batch as the unit (largest remaining item)
Kafka's own documentation says a record batch is the unit of storage and offset assignment (`baseOffset` + `lastOffsetDelta`), that the broker does not recompute the CRC on append (the partition leader epoch
is excluded from the CRC precisely for that), and that records are not individually re-encoded. AeroStream splits every batch into one entry per record. Following Kafka means: append the batch bytes after patching the base offset,
assign `next_offset += record_count`, and locate offsets with a **sparse index** (Kafka adds one index entry per `log.index.interval.bytes`, default 4096, each entry 4 bytes relative offset + 4 bytes position, and finds an offset with a floor lookup
then a short scan). This would remove most of the per-record CPU, allow compressed batches to stay compressed on disk, and is the likely fix for the remaining large-message gap. It touches the log, index, compaction, share groups, the transaction index,
replication and Iceberg, so it needs its own design and test pass.

### 3.2 Large messages (1-50 MB) over the Kafka port
**Update (September 27, quay image, 3 runs):** the Kafka port now measures 333 MB/s at 1 MB (Kafka 384, Redpanda 306) and 81 MB/s at 50 MB (Kafka 81, Redpanda 95); only 10 MB remains behind (166 vs 240-299). The earlier numbers below are kept for reference.

1 MB 200 vs 321-375 MB/s, 10 MB 144 vs 171-219, 50 MB 29 vs 56-58. Profile at 50 MB: broker mostly idle, time in kernel page-cache copies; Kafka and Redpanda hit a ~55 MB/s wall with the same single-producer Java client, so part of it is client-bound.
Ideas: reuse a per-connection frame buffer (avoid re-faulting fresh pages per request), `posix_fallocate` segments, one write per batch rather than per record (3.1).

### 3.3 Startup
The Go controller uses hashicorp/raft defaults (HeartbeatTimeout 1000 ms, ElectionTimeout 1000 ms; minimum allowed 5 ms). A node with no leader waits about a heartbeat timeout with random staggering before its first election, which is the bimodal 1.8 s vs 3.4 s.
For a single bootstrapped node, lowering `heartbeat_timeout_ms` / `election_timeout_ms` in the controller config (constraints: `LeaderLeaseTimeout` <= `HeartbeatTimeout`, `ElectionTimeout` >= `HeartbeatTimeout`) should cut this.
The controller keeps its Raft log in BoltDB; hashicorp/raft-wal needs one fsync per append instead of two and avoids BoltDB free-space slowdowns after truncation, but is documented as experimental, so it is not adopted.

### 3.4 Thread pinning
`on_thread_start` pins every started thread (including Tokio's blocking pool) to core `tid % available_parallelism`. `available_parallelism()` is cgroup-quota aware (it returned 2 for `--cpus=2` on this host) but the pinned cores are chosen by index, not from the process's allowed set. The documented downside of CPU limits with many
runnable threads is CFS throttling and tail latency. Worth revisiting with a proper A/B (`scripts/ab-test.sh`); not changed.

## 4. Sources consulted
* Apache Kafka, message format (batches, base offset, CRC scope): <https://kafka.apache.org/39/implementation/message-format/>
* Apache Kafka, log (offset lookup, flush parameters): <https://kafka.apache.org/42/implementation/log/>
* Strimzi, Kafka segments and the sparse offset index: <https://strimzi.io/blog/2021/12/17/kafka-segment-retention/>
* `crc32c` crate (hardware SSE4.2 with runtime detection, software fallback): <https://docs.rs/crc32c/latest/crc32c/>; alternatives `turbo_crc` <https://docs.rs/turbo_crc/latest/turbo_crc/>
* Rust `std::thread::available_parallelism` (cgroup caveats): <https://doc.rust-lang.org/stable/std/thread/fn.available_parallelism.html>
* Tokio runtime builder (`worker_threads`, `on_thread_start`, blocking pool): <https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html>
* Tokio `TcpStream` (`set_nodelay`, vectored writes): <https://docs.rs/tokio/latest/tokio/net/struct.TcpStream.html>; Red Hat, TCP_NODELAY and small buffer writes: <https://docs.redhat.com/en/documentation/red_hat_enterprise_linux_for_real_time/7/html/tuning_guide/tcp_nodelay_and_small_buffer_writes>
* Rust `vec![0; n]` zeroed allocation and read_buf: <https://darkcoding.net/software/rust-zeroed-vector-allocation/>, <https://rust-lang.github.io/rfcs/2930-read-buf.html>, <https://nnethercote.github.io/perf-book/heap-allocations.html>
* Linux CFS bandwidth control and throttling: <https://docs.kernel.org/scheduler/sched-bwc.html>, <https://danluu.com/cgroup-throttling/>
* hashicorp/raft configuration (timeouts, snapshots, bootstrap): <https://pkg.go.dev/github.com/hashicorp/raft>, defaults in <https://github.com/hashicorp/raft/blob/main/config.go>; `raft-wal`: <https://github.com/hashicorp/raft-wal>
