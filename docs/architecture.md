---
icon: material/layers-triple-outline
---
# Dual-Engine Architecture Deep-Dive

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Deep Dive</span>
<span class="md-tag">10 min read</span>
<span class="md-tag">Go 1.26 & Rust 1.98.1</span>
<span class="md-tag">Edition 2024</span>
</div>

AeroStream's core architectural principle is the **strict physical and operational separation of distributed consensus from log storage and network I/O**. Rather than running everything inside a monolithic runtime, AeroStream combines two purpose-built engines:

1. A **Go 1.26 Control Plane** handling cluster orchestration, distributed Raft consensus, dynamic metadata coordination, schema governance, enterprise RBAC/ACLs, and REST/Admin APIs.
2. A **Rust 1.98.1 (Edition 2024) Data Plane** running a shard-per-core storage engine, lock-free append-only log segments, zero-copy network sockets, and hardware-accelerated batch verification.

![Dual Engine Architecture](images/dual_engine_architecture.png)

---

## 1. Control Plane Architecture (Go 1.26 & Raft Quorum)

The AeroStream Control Plane runs as a lightweight, resilient microservice dedicated to maintaining the cluster's ground truth.

![Controller and Broker Orchestration](images/controller_broker_orchestration.png)

### Quorum Elections & Health Heartbeats

* **Sub-150ms Leader Elections**: The control plane utilizes HashiCorp Raft. In the event of a leader controller failure, remaining voter nodes detect heartbeat timeout and elect a new leader in under 150 milliseconds.
* **Continuous Broker Heartbeats**: Storage brokers transmit UDP/gRPC heartbeats every 2 seconds to the active controller on port `8001`. If a broker fails to heartbeat within the grace threshold (default 6–8 seconds), the controller marks it unreachable and automatically promotes in-sync replicas (ISR) to partition leaders.
* **Piggybacked Dynamic Configuration**: Controller responses to broker heartbeats piggyback partition assignment deltas, dynamically updating broker routing, client quotas, and compression policies without restart.

### Partition Consensus & High Watermark ($HW$) Tracking

Replication safety across multi-node clusters is governed by the High Watermark ($HW$) consensus invariant. The controller and broker track the Log End Offset ($\text{LEO}$) for every in-sync replica ($r \in \text{ISR}$):

$$HW = \min_{r \in \text{ISR}} \text{LEO}_r$$

The High Watermark strictly bounds the offset visibility for consumer fetch requests:

$$0 \le \text{CommittedOffset} \le HW \le \text{LEO}_{\text{leader}}$$

When partition leaders append batches locally ($\text{LEO}_{\text{leader}} \gets \text{LEO}_{\text{leader}} + \Delta$), the updated base offset is committed and made available to consumers only after all followers in the active ISR acknowledge replication up to that offset.

### Green Tea Garbage Collector (Go 1.26)

The Go 1.26 runtime defaults to the **Green Tea Garbage Collector**, which fundamentally restructures heap marking and generational scavenging. This brings:

* **Sub-millisecond GC Pauses**: $p_{99.9}$ GC pauses remain strictly under **1 ms**, even when managing tens of thousands of topic partitions and active client metadata connections.
* **10%–40% Reduction in Allocator Overhead**: Drastically decreases memory churn during heavy burst periods of administrative topic creation and schema registration.
* **Generational Scavenging**: Short-lived allocations from JSON request serialization and transient RPC buffers are reclaimed before escalating to mature heap arenas.

### Swiss Tables SIMD Hash Maps

Go 1.24+ and Go 1.26 replace legacy bucket-based hash tables with an internal implementation based on **Swiss Tables**—utilizing 16-way SIMD vector probing inspired by Google Abseil:

* Topic-to-partition lookup speed is improved by **30%**.
* Wildcard ACL rule matching (`orders.*`, `finance.eu.*`) executes in constant-time SIMD instructions, ensuring authorization checks never add measurable latency.
* Metadata state queries execute with minimal CPU cache misses.

---

## 2. Data Plane Kernel (Rust 1.98.1 & Tokio)

The Storage Broker is built entirely in Rust (compiled with `rustc 1.98.1`, targeting Edition 2024). It operates with complete mechanical sympathy directly against Linux kernel primitives.

![Shard Per Core Architecture](images/shard_per_core_architecture.png)

### Shard-per-Core Zero-Contention Architecture

To eliminate cross-thread locking on high-throughput paths, AeroStream adopts a **Shard-per-Core** model:

* **Deterministic Shard Assignment**: Topic partitions are deterministically mapped to dedicated worker threads via the `ShardRouter` hash partition mapping function:

    $$S = \text{hash}(\text{topic}, \text{partition}) \pmod N$$

    where $N$ denotes the total number of pinned shard execution threads ($N = N_{\text{shards}}$). For a topic name $T$ and partition index $p$:

    $$S = \left( \mathcal{H}_{\text{64}}(T) \oplus p \right) \pmod N$$

    This ensures uniform partition spreading across available CPU execution units without global coordinator locks or thread migrations.

* **Thread-to-Core Affinity**: Worker threads are pinned to dedicated physical CPU cores using Linux `libc::sched_setaffinity`:

    ```rust
    let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
    libc::CPU_SET(shard_id % num_cpus, &mut cpuset);
    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
    ```

* **Lock-Free Actor Channels**: Ingress network connections dispatch record batches to the designated core worker via `flume::unbounded` lock-free ring-buffers—eliminating cross-core mutexes, atomic CAS loops, and CPU cache-line bouncing.

### Positioned Writes Without Lock Races

In conventional file I/O, concurrent appends to an open file handle require acquiring a file-pointer lock (`lseek` + `write`).

AeroStream completely bypasses file-pointer mutex contention using Linux positioned writes (`pwrite(2)`):

```rust
// Positioned write directly at the calculated segment byte offset
file.write_all_at(record_bytes, current_segment_offset)?;
```

Because each write supplies its absolute byte position directly:

1. Multiple worker threads can append to non-overlapping partition segments concurrently.
2. Active segment lengths are tracked purely in-memory via monotonic atomics, eliminating `statx(2)` and `lseek(2)` system call overheads from the hot produce path.

### In-Place Base-Offset Patching ($\mathcal{O}(1)$)

In the Kafka wire protocol, producers submit `RecordBatch` payloads with `base_offset = 0`. Traditional proxies decompress batches, iterate over records, adjust offsets, recompress, and recompute checksums, incurring $\mathcal{O}(B)$ memory copies:

$$T_{\text{naive}}(\text{Bridge}) = \mathcal{O}(B) + \mathcal{O}(K \cdot \text{decompress})$$

Because Kafka RecordBatch CRC32C covers **only bytes 21 through end-of-batch**, the 8-byte `base_offset` at bytes 0..8 is outside the checksum domain. AeroStream patches the offset in $\mathcal{O}(1)$ time directly on disk:

$$T_{\text{patch}}(\text{AeroStream}) = \mathcal{O}(1) \ll T_{\text{naive}}(\text{Bridge})$$

```rust
let base_offset_bytes = next_offset.to_be_bytes();
segment_file.write_all_at(&base_offset_bytes, batch_start_pos)?;
```

---

## 3. Zero-Copy `sendfile(2)` Pipeline

When consumers request message batches via Port 9092 (Kafka Fetch) or Port 9091 (Native Streaming), AeroStream executes **true zero-copy transfers**:

![Produce and Fetch Pipeline](images/produce_fetch_pipeline.png)

### Fetch Path Comparison

| Step | Traditional Monolithic JVM Broker | AeroStream Rust Data Plane |
|---|---|---|
| **Read path** | Disk $\to$ Page Cache $\to$ JVM Heap $\to$ Socket Buffer | Disk $\to$ Page Cache $\to$ Network Socket (`sendfile(2)`) |
| **Userspace copies** | 1–2 heap buffer allocations | **0** (kernel zero-copy transfer) |
| **CPU Cache footprint** | Pollutes L1/L2 data cache with multi-MB payloads | Direct NIC DMA without CPU cache eviction |
| **Index search** | Java object hierarchy traversal | $\mathcal{O}(\log N)$ binary search over contiguous `.idx` mmap |

### Memory-Mapped Indexing (`.idx`)

AeroStream stores sparse index files alongside `.log` data segments. Each index entry occupies exactly 8 bytes (4 bytes relative offset, 4 bytes physical position).

The broker memory-maps these `.idx` files using `mmap(2)`. Finding an offset within a 128 MB segment requires $\mathcal{O}(\log N)$ binary search over a contiguous slice of memory, resolved directly by hardware cache lines.

### Paced Page-Cache Writeback

In memory-constrained container environments (e.g., Kubernetes limits of 2 GiB RAM), aggressive OS dirty page accumulation can trigger sudden, seconds-long Linux writeback pauses.

AeroStream protects against writeback pauses by actively pacing kernel writeback, enforcing an upper bound on dirty buffer buildup:

$$M_{\text{dirty}}(t) \le \Delta_{\text{pace}} = 8 \text{ MiB}$$

* Every **8 MiB** of appended data, the storage engine invokes `sync_file_range(2)` with `SYNC_FILE_RANGE_WRITE`:
  $$\text{sync\_file\_range}(fd, \text{offset} - 8\text{MB}, 8\text{MB}, \text{SYNC\_FILE\_RANGE\_WRITE})$$
* It advises the kernel with `posix_fadvise(POSIX_FADV_DONTNEED)` for historical segments, preventing cold consumer reads from evicting hot active ingestion buffers.

---

## 4. Hardware CRC32C Acceleration

Every Kafka RecordBatch framing standard mandates a 32-bit Castagnoli polynomial checksum (`CRC32C`) covering the record batch header and payload.

The Castagnoli generator polynomial is defined as:

$$P(x) = x^{32} + x^{28} + x^{27} + x^{26} + x^{25} + x^{23} + x^{22} + x^{20} + x^{19} + x^{18} + x^{14} + x^{13} + x^{11} + x^{10} + x^9 + x^8 + x^6 + 1$$

Represented in hexadecimal notation as `0x1EDC6F41`. For a binary message polynomial $M(x)$ of length $k$, the 32-bit CRC checksum $R(x)$ is computed as the polynomial remainder:

$$R(x) = M(x) \cdot x^{32} \pmod{P(x)}$$

AeroStream leverages specialized CPU hardware instructions:

* **x86_64**: `CRC32` instruction (part of SSE 4.2 / AVX-512)
* **AArch64 (ARMv8)**: Hardware CRC extension instructions

```rust
#[inline(always)]
pub fn compute_crc32c_hardware(data: &[u8]) -> u32 {
    let mut crc = crc32c::Hasher::new();
    crc.update(data);
    crc.finalize()
}
```

This hardware pipeline delivers **10.9 GB/s checksum throughput** on modern CPUs, removing checksum verification as a bottleneck on 100 Gbps network interfaces:

$$\text{Latency}_{\text{CRC}} = \frac{\text{BatchSize}}{10.9 \times 10^9 \text{ B/s}} \approx 91.7 \text{ ns per 1 KB batch}$$

---

## 5. Native Binary Protocol (Port 9091) vs. Kafka Protocol (Port 9092)

AeroStream provides dual listener ingress ports:

* **Port 9092 (Kafka Wire Protocol)**: Drop-in replacement for standard Kafka clients (`kafka-clients`, `confluent-kafka`, `kafkajs`, `franz-go`).
* **Port 9091 (AeroStream Native Protocol)**: 7-byte ultra-lightweight header framing (`0xAE 0x01`), direct memory layout, zero Kafka envelope overhead, designed for sub-millisecond tail latency and maximum hardware efficiency.

![Native and Kafka Dual Protocol](images/native_and_kafka_dual_protocol.png)

### Feature & Efficiency Matrix

| Dimension | Native Binary Protocol (`:9091`) | Apache Kafka Protocol (`:9092`) |
|---|---|---|
| **Framing Overhead** | **7 bytes total** (`[0xAE, 0x01, cmd, len]`) | 14–48+ bytes (Length, ApiKey, Version, CorrelationID, ClientID, Tags) |
| **Parsing Complexity** | Fixed-offset slice parsing ($\mathcal{O}(1)$) | Varint decoding, tagged fields, flexible headers ($\mathcal{O}(K)$) |
| **Client Ecosystem** | Official `aerostream-sdk` (Go, Rust, Java, .NET, Node.js) | Universal Kafka ecosystem (`confluent-kafka`, `librdkafka`, `spring-kafka`) |
| **Throughput (1 KB msgs)** | **174,714 msg/s** (resource-capped container) | **100,000–271,350 msg/s** (OMB benchmark on EC2) |
| **Median Latency ($p_{50}$)** | **< 0.5 ms** | **0.7 ms** |
| **Tail Latency ($p_{99}$)** | **< 1.0 ms** | **1.4 – 1.7 ms** |
| **Authentication** | Command 0 Bearer Token Handshake | ApiKey 17/36 SASL PLAIN / SCRAM-SHA-256 / mTLS |

### Native Protocol Command Set

* **Command 0 (Auth Handshake)**: Authenticates client token (`[token: UTF-8]`), responding with `[0xAE, 0x01, status: u8]`.
* **Command 1 (Produce / Append)**: Appends payload to topic/partition, returning assigned 64-bit log offset.
* **Command 2 (Consumer Fetch)**: Fetches record bytes bounded by partition High Watermark ($HW$).
* **Command 3 (Replica Fetch)**: Inter-broker replication stream advancing replica LEOs.
* **Command 4 (Multi-Entry Long-Poll)**: Multi-record batch fetch with server-side suspension until data arrives or `max_wait_ms` expires.

---

## 6. Queuing & Saturation Dynamics (Little's Law)

Under heavy load, broker latency follows queuing theory dynamics governed by Little's Law:

$$L = \lambda W$$

where $L$ is the average number of requests in the system, $\lambda$ is arrival rate, and $W$ is mean residence time.

When offered load $\lambda < \mu$ (where $\mu \approx 271,000 \text{ msg/s}$ is saturation service capacity):

* Requests process immediately without queuing: $W \approx \frac{1}{\mu} \approx 0.7 \text{ ms}$.
* Tail latency remains flat ($p_{99} < 1.7 \text{ ms}$ at 200,000 msg/s).

When offered load approaches or exceeds service capacity ($\lambda \to \mu$):

$$W = \frac{1}{\mu - \lambda}$$

Requests queue in network and channel buffers, causing latency to elevate to hundreds of milliseconds while throughput remains pinned at physical saturation capacity (271,350 msg/s, 265 MB/s on an 8-vCPU instance).

---

## 7. Multi-Cloud Tiered Storage & Zero-Downtime Draining

AeroStream completes its architecture with two enterprise production features:

### Multi-Cloud Tiered Storage Pipeline

Sealed 128 MB log segments are asynchronously offloaded to cloud object storage (AWS S3, MinIO, Google Cloud Storage, Azure Blob Storage) via zero-copy hard links (`fs::hard_link`), decoupling long-term data retention from expensive local NVMe capacity.

![Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

### Graceful Cluster Draining

During rolling node maintenance or scale-down events, operators trigger partition draining via `POST /api/brokers/{id}/drain`. The Go Controller automatically migrates partition leadership to surviving ISR members with zero message loss and zero downtime.

![Cluster Topology Scale Down](images/cluster_topology_scale_down.png)
