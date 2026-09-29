# Dual-Engine Architecture Deep-Dive

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Deep Dive</span>
<span class="md-tag">9 min read</span>
<span class="md-tag">Go 1.26 & Rust 1.98.1</span>
</div>

AeroStream's core architectural principle is the **strict separation of distributed consensus from log storage and network I/O**. Rather than running everything inside a monolithic runtime, AeroStream combines two purpose-built engines:

1. A **Go 1.26 Control Plane** handling cluster orchestration, distributed Raft consensus, dynamic metadata coordination, schema governance, and REST/Admin APIs.
2. A **Rust 1.98.1 Data Plane** running a shard-per-core storage engine, lock-free append-only log segments, zero-copy network sockets, and hardware-accelerated batch verification.

![Controller and Broker Orchestration](images/controller_broker_orchestration.png)

---

## Control Plane (Go 1.26 & Raft Quorum)

The AeroStream Control Plane runs as a lightweight, resilient microservice dedicated to maintaining the cluster's ground truth.

```mermaid
flowchart TD
    subgraph ControlPlane["Go 1.26 Control Plane"]
        RAFT["HashiCorp Raft Consensus Engine<br/>Quorum Election & Log Replication"]
        FSM["Partition & Topology State Machine<br/>Atomic BoltDB Snapshots"]
        HB["Broker Heartbeat Monitor<br/>2-second interval, Piggybacked Config"]
        SR["Confluent-Compatible Schema Registry<br/>Avro / JSON / Protobuf"]
        ACL["Enterprise RBAC & ACL Policy Evaluator<br/>SIMD Swiss Tables"]
        REST["HTTP REST / Web Console Server<br/>Port 9001"]
    end

    subgraph DataPlane["Rust Data Plane Brokers"]
        B1["Broker 1 (Leader P0, P1)"]
        B2["Broker 2 (Leader P2)"]
        B3["Broker 3 (Follower P0, P1, P2)"]
    end

    REST --> FSM
    SR --> FSM
    ACL --> FSM
    FSM --> RAFT
    RAFT -->|Linearizable Log Entries| FSM
    HB <-->|Heartbeats & Metadata Sync - gRPC 8001| B1
    HB <-->|Heartbeats & Metadata Sync - gRPC 8001| B2
    HB <-->|Heartbeats & Metadata Sync - gRPC 8001| B3
```

### Quorum Elections & Health Heartbeats

* **Sub-150ms Leader Elections**: The control plane utilizes HashiCorp Raft. In the event of a leader controller failure, remaining voter nodes detect heartbeat timeout and elect a new leader in under 150 milliseconds.
* **Continuous Broker Heartbeats**: Storage brokers transmit UDP/gRPC heartbeats every 2 seconds to the active controller. If a broker fails to heartbeat within the grace threshold (default 6 seconds), the controller marks it unreachable and automatically promotes in-sync replicas (ISR) to partition leaders.
* **Piggybacked Dynamic Configuration**: Controller responses to broker heartbeats piggyback partition assignment deltas, dynamically updating broker routing without restart.

### Green Tea Garbage Collector (Go 1.26)

The Go 1.26 runtime defaults to the **Green Tea Garbage Collector**, which fundamentally restructures heap marking and generational scavenging. This brings:

* **Sub-millisecond GC Pauses**: $p_{99.9}$ GC pauses remain strictly under **1 ms**, even when managing tens of thousands of topic partitions and active client metadata connections.
* **10%–40% Reduction in Allocator Overhead**: Drastically decreases memory churn during heavy burst periods of administrative topic creation and schema registration.

### Swiss Tables SIMD Hash Maps

Go 1.24/1.26 replaces legacy bucket-based hash tables with **Swiss Tables**—utilizing 16-way SIMD vector probing. In AeroStream's control plane:

* Topic-to-partition lookup speed is improved by **30%**.
* Wildcard ACL rule matching (`orders.*`, `finance.eu.*`) executes in constant-time SIMD instructions, ensuring authorization checks never add measurable latency.

---

## Data Plane Kernel (Rust 1.98.1 & Tokio)

The Storage Broker is built entirely in Rust (compiled with `rustc 1.98.1`, targeting Edition 2024). It operates with complete mechanical sympathy directly against Linux kernel primitives.

![Shard Per Core Architecture](images/shard_per_core_architecture.png)

### Shard-per-Core Zero-Contention Architecture

To eliminate cross-thread locking on high-throughput paths, AeroStream adopts a **Shard-per-Core** model:

* **Deterministic Shard Assignment**: Topic partitions are deterministically hashed to CPU cores:
  $$\text{shard\_id} = \text{partition\_id} \pmod{N_{\text{shards}}}$$
* **Thread-to-Core Affinity**: Worker threads are pinned to dedicated physical CPU cores using Linux `libc::sched_setaffinity`.
* **Lock-Free Actor Channels**: Ingress network connections dispatch record batches to the designated core worker via `flume::unbounded` lock-free ring-buffers—eliminating cross-core mutexes, atomic CAS loops, and CPU cache-line bouncing.

### Positioned Writes Without Lock Races

In conventional file I/O, concurrent appends to an open file handle require acquiring a file-pointer lock (`lseek` + `write`).

AeroStream completely bypasses file-pointer mutex contention using Linux positioned writes:

```rust
// Positioned write directly at the calculated segment byte offset
file.write_all_at(record_bytes, current_segment_offset)?;
```

Because each write supplies its absolute byte position directly:

1. Multiple worker threads can append to non-overlapping partition segments concurrently.
2. Active segment lengths are tracked purely in-memory via monotonic atomics, eliminating `statx(2)` and `lseek(2)` system call overheads from the hot produce path.

---

## Zero-Copy `sendfile(2)` Pipeline

When consumers request message batches via Port 9092 (Kafka Fetch) or Port 9091 (Native Streaming), AeroStream executes **true zero-copy transfers**:

![Produce and Fetch Pipeline](images/produce_fetch_pipeline.png)

```text
┌────────────────────────┐
│ Linux Disk Page Cache  │
└───────────┬────────────┘
            │
            │  Direct Kernel DMA (sendfile system call)
            ▼
┌────────────────────────┐
│  Network NIC Ring Buff │  ===> Client Socket TCP Stream
└────────────────────────┘
  (Zero Userspace Memory Copying / Zero CPU Cache Eviction!)
```

### Contrast with Traditional JVM Brokers

| Feature | Standard JVM Streaming Broker | AeroStream Rust Data Plane |
|---|---|---|
| **Fetch Path** | Disk $\to$ Page Cache $\to$ JVM Heap $\to$ Socket Buffer | Disk $\to$ Page Cache $\to$ Network Socket (`sendfile(2)`) |
| **Userspace Copies** | 2 copies (heap byte buffer alloc) | **0 copies** |
| **CPU Cache Thrashing** | High (GC and object allocations) | **Zero** (Data never enters CPU L1/L2 cache) |
| **Index Search** | JVM object deserialization | Binary search in memory-mapped (`mmap`) `.idx` |

### Memory-Mapped Indexing (`.idx`)

AeroStream stores sparse index files alongside `.log` data segments. Each index entry occupies exactly 8 bytes (4 bytes relative offset, 4 bytes physical position).

The broker memory-maps these `.idx` files using `mmap(2)`. Finding an offset within a 128 MB segment requires $\mathcal{O}(\log N)$ binary search over a contiguous slice of memory, resolved directly by hardware cache lines.

### Paced Page-Cache Writeback

In memory-constrained container environments (e.g., Kubernetes limits of 2 GiB RAM), aggressive OS dirty page accumulation can trigger sudden, seconds-long Linux writeback pauses.

AeroStream protects against this by actively pacing kernel writeback:

* Every **8 MiB** of appended data, the storage engine invokes `sync_file_range(2)` with `SYNC_FILE_RANGE_WRITE`.
* It advises the kernel with `posix_fadvise(POSIX_FADV_DONTNEED)` for historical segments, preventing cold consumer reads from evicting hot active ingestion buffers.

---

## Hardware CRC32C Acceleration

Every Kafka RecordBatch framing standard mandates a 32-bit Castagnoli polynomial checksum (`CRC32C`) covering the record batch header and payload.

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

This hardware pipeline delivers **10.9 GB/s checksum throughput** on modern CPUs, removing checksum verification as a bottleneck on 100 Gbps network interfaces.
