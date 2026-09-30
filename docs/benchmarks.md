# Performance Benchmarks

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">OMB Benchmark</span>
<span class="md-tag">5 min read</span>
<span class="md-tag">Kafka Wire Protocol</span>
</div>

AeroStream was benchmarked through its Kafka wire protocol port (`9092`) with the **Linux Foundation OpenMessaging Benchmark (OMB)** framework, on a dedicated AWS EC2 machine. This page reports what AeroStream delivers on that hardware: sustained throughput, latency percentiles, and resource use. The raw data is in [`benchmarks/omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/`](https://github.com/gradientgeeks/aerostream/tree/main/benchmarks/omb-results/aws-c6id-2xlarge-aerostream-2026-09-30).

---

## Results at a Glance

Test machine: AWS `c6id.2xlarge` (8 vCPU, 16 GiB). One AeroStream broker, 1 topic with 32 partitions, 1,024-byte messages, 8 producers, 8 consumers, `acks=1`, 30 ten-second samples per run (5-minute measurement after a 2-minute warm-up), two rounds per workload.

| Offered load | Publish rate | Publish $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | End-to-end $p_{99}$ | Broker cores busy | Errors |
|---|---|---|---|---|---|---|---|---|
| **100,000 msg/s** (fixed) | 100,082 msg/s (97.7 MB/s) | **0.7 ms** | 1.2 ms | **1.4 ms** | 2.3 ms | 2.0 ms | 14% | 0 |
| **200,000 msg/s** (fixed) | 200,175 msg/s (195.5 MB/s) | **0.7 ms** | 1.3 ms | **1.7 ms** | 3.0 ms | 2.0 ms | 22% | 0 |
| **Maximum rate** (unthrottled) | **271,350 msg/s** (265.0 MB/s) | 105 ms | 813 ms | 1,104 ms | 1,376 ms | 1,119 ms | 55% | 0 |

Values are the median of the two rounds. Consumers kept pace with producers in every run (consume rate equal to publish rate, no growing backlog).

!!! tip "How to read this"
    * **Up to at least 200,000 msg/s, latency stays flat.** Publish $p_{99}$ is under 2 ms and $p_{99.9}$ under 3.1 ms, with the broker's cores at most 22% busy.
    * **The saturation point is about 271,000 msg/s** (265 MB/s of 1 KB messages). Above it, requests queue, which is why latency at the maximum rate is measured in hundreds of milliseconds. Use the fixed-rate rows to judge latency and the maximum-rate row to judge capacity.
    * **Throughput is repeatable:** the two maximum-rate rounds measured 271,231 and 271,469 msg/s (within 0.1%).

### Every Run

| Workload | Round | Publish rate | $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | End-to-end $p_{99}$ | Peak 10 s rate |
|---|---|---|---|---|---|---|---|---|
| 100k msg/s | 1 | 100,084 | 0.69 ms | 1.23 ms | 1.43 ms | 2.33 ms | 2.00 ms | 102,527 |
| 100k msg/s | 2 | 100,080 | 0.68 ms | 1.21 ms | 1.40 ms | 2.29 ms | 2.00 ms | 102,395 |
| 200k msg/s | 1 | 200,157 | 0.75 ms | 1.34 ms | 1.76 ms | 3.09 ms | 2.00 ms | 204,694 |
| 200k msg/s | 2 | 200,194 | 0.73 ms | 1.31 ms | 1.72 ms | 2.98 ms | 2.00 ms | 205,832 |
| Max rate | 1 | 271,231 | 170.6 ms | 1,028 ms | 1,290 ms | 1,543 ms | 1,303 ms | 285,971 |
| Max rate | 2 | 271,469 | 39.7 ms | 597 ms | 918 ms | 1,210 ms | 934 ms | 287,703 |

At the maximum rate, latency varies from run to run (the queue depth at saturation is not stable) while throughput does not.

---

## CPU Use

The machine's four physical cores were split so that the broker and the load generator never share a core: the broker container ran on vCPUs `0,1,4,5` (two physical cores) and OMB on vCPUs `2,3,6,7`. CPU was sampled every 5 seconds with `sar` on each set.

| Workload | Broker cores busy (4 vCPUs) | Load-generator cores busy (4 vCPUs) |
|---|---|---|
| 100,000 msg/s | 14% | 48% |
| 200,000 msg/s | 22% | 59% |
| Maximum rate | 55% | 62% |

Neither side was fully busy at the maximum rate, so the 271,000 msg/s limit is not raw CPU on the broker's cores.

---

## Test Environment & Methodology

| Component | Specification |
|---|---|
| **Machine** | AWS EC2 `c6id.2xlarge`, us-east-1a, single machine for broker and load generator |
| **Processor** | Intel Xeon Platinum 8375C @ 2.90 GHz: 8 vCPUs = 4 physical cores x 2 threads (AWS Nitro, KVM) |
| **Memory / storage** | 16 GiB RAM, 474 GB local NVMe (broker data directory) |
| **Operating system** | Amazon Linux 2023, Linux 6.18, Docker |
| **Broker placement** | Docker container, host networking, pinned to vCPUs `0,1,4,5`, 8 GiB memory limit; fresh data directory and dropped page cache before every run |
| **Load generator** | OpenMessaging Benchmark (commit `5b1fa70`), pinned to vCPUs `2,3,6,7` |
| **Workload** | 1 topic, 32 partitions, 1,024-byte payloads, 8 producers, 8 consumers (one subscription) |
| **Producer settings** | `acks=1`, `linger.ms=1`, `batch.size=131072`, `max.in.flight.requests.per.connection=5` |
| **Consumer settings** | `auto.offset.reset=earliest`, auto-commit every 5 s, `max.partition.fetch.bytes=1048576` |
| **Broker build** | `quay.io/gradientgeeks/aerostream:latest` (digest `sha256:1fd1a44a7c8c`), one broker, no replication |
| **Run structure** | 2-minute warm-up (excluded) + 5-minute measurement, 2 rounds per workload, 3 workloads |
| **Date** | 30 September 2026 |

---

## Earlier Test: Constrained Container on a Laptop

An earlier run measured the broker under a strict resource cap: 2 CPUs and 2 GiB of RAM, on a laptop-class machine, 16 partitions, 2 producers and 2 consumers at the maximum rate for 60 seconds.

| Metric | AeroStream (Port 9092) |
|---|---|
| **Publish throughput (avg)** | 202,395 msg/s (197.7 MB/s) |
| **Publish throughput (peak 10 s interval)** | 216,680 msg/s |
| **Consume throughput (avg)** | 202,418 msg/s |
| **Publish latency $p_{50}$ / $p_{99}$ / $p_{99.9}$ / max** | 1.5 ms / 452 ms / 494 ms / 565 ms |
| **End-to-end latency $p_{50}$ / $p_{99}$** | 4.0 ms / 485 ms |
| **Average CPU (steady state)** | 134% of the 200% cap (about 1.34 cores) |
| **Peak container memory** | 533 MiB (from 41 MiB at start) |

| Interval | 0-10 s | 10-20 s | 20-30 s | 30-40 s | 40-50 s | 50-60 s |
|---|---|---|---|---|---|---|
| **Throughput (msg/s)** | 209,910 | 181,919 | 197,393 | 216,680 | 207,052 | 201,414 |

This single-iteration run used a laptop (Intel Core i5-1235U, 15 GiB, Debian 13) with desktop background processes active, so run-to-run variation of about 20% is possible. It is included as a data point for small, resource-limited deployments.

---

## Reproducing the Benchmark

=== "AWS EC2 (the results above)"

    One command creates the machine, runs OMB, copies the results back and destroys the machine:

    ```bash
    cd benchmarks/aws-ec2
    ./run-aerostream-8core.sh          # prints the plan, time and cost estimate; creates nothing
    ./run-aerostream-8core.sh --yes    # runs it (about 80 minutes)
    ```

    See [`benchmarks/aws-ec2/README.md`](https://github.com/gradientgeeks/aerostream/blob/main/benchmarks/aws-ec2/README.md) for the method and safety mechanisms.

=== "Local container"

    ```bash
    cd benchmarks/openmessaging-benchmark
    ./omb-run.sh quay.io/gradientgeeks/aerostream:latest workloads/aerostream-16p-1kb.yaml aerostream-kafkawire
    ```

---

## Caveats & Methodology Notes

!!! warning "Benchmark Considerations"
    * **Single broker, `acks=1`, no replication.** These are single-node numbers; replication adds work that is not measured here.
    * **Latency at the maximum rate is queueing.** Compare latency using the fixed-rate workloads, where the offered load is the same in every run.
    * **Page cache.** Data is written through the OS page cache and the broker's data directory is on local NVMe; each run starts from an empty data directory and a dropped page cache.
    * **Hardware dependent.** Results describe this machine (4 physical cores) and 1 KB messages on the Kafka wire protocol; other hardware and message sizes will differ. The native AeroStream TCP protocol is not covered here.
    * **What limits the maximum rate is open.** At 271,000 msg/s neither the broker's nor the load generator's cores were fully busy; the limit has not been isolated yet.
