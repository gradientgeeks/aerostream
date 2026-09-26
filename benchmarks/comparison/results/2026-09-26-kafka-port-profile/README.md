# Kafka-port profile (integration build, 1 KB records, 2 CPU / 2 GB broker container)

Method: start the broker container with `--privileged` (or `--cap-add SYS_PTRACE`), `apt-get install linux-perf strace` inside it,
drive the Kafka port with `kafka-producer-perf-test.sh` (`acks=1 linger.ms=1 batch.size=262144`) and sample the broker
(`perf record -F 499 -g -p 1 -- sleep 8`, `strace -c -f -p 1`).

## CPU (perf, 300k x 1 KiB records)
| Share of broker CPU | Symbol |
| ---: | :--- |
| 41.8% | `rust_broker::kafka::handlers::parse_records` (includes the inlined batch CRC32C check) |
| 9.0% | `rust_broker::kafka::handlers::encode_idempotent_records_batch` (re-encodes every record as its own batch, CRC32C again) |
| ~8% | kernel file-metadata path: `statx`, `__d_lookup_rcu`, `link_path_walk`, `generic_fillattr`, ... |
| rest | socket / page-cache copies |

## Syscalls (strace -c, 50,000 records; absolute speed is distorted ~10x by strace, counts are real)
`write` 101,756 (about 2 per record) | `statx` 67,770 | `lseek` 67,770 | `recvfrom` 5,020 | `sendto` 4,528 | `epoll_wait` 1,214.
About 5 syscalls per record on the append path, while network syscalls are amortised over ~100 records per batch.

## CRC32C (`crcbench.rs`, `crc-microbench.txt`)
The broker's `crc32c` is a byte-at-a-time table lookup. On this CPU it runs at **~0.5 GB/s** (504 MB/s on a quiet machine,
474 MB/s in the saved run), while the SSE4.2 hardware instruction reaches **8-11 GB/s** (17-22x) with an identical result.
That is ~2 us of CRC per 1 KiB record for the table version, paid at least twice per record (once to verify the incoming
batch, once when the record is re-encoded as a single-record batch).

## 50 MB messages
The broker is mostly idle (892 samples in 12 s at 499 Hz); the time goes to kernel page-cache copies. Kafka and Redpanda reach the
same ~55 MB/s wall with this single-producer Java client, so the 50 MB Kafka-port figure is largely client-bound.
