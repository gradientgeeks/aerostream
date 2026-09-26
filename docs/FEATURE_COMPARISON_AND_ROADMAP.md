# AeroStream vs. Apache Kafka vs. Redpanda: Architectural Comparison & Feature Analysis

---

## 1. Why "AeroStream" Instead of "AeroMQ"?

To understand the fundamental identity of **AeroStream**, it helps to contrast traditional Message Queues with Distributed Event Streaming Logs:

| Dimension | Traditional Message Queue (e.g., RabbitMQ, ActiveMQ, SQS) | Distributed Event Streaming Log (AeroStream, Kafka, Redpanda) |
| :--- | :--- | :--- |
| **Data Storage Model** | **Ephemeral Queue**: Messages are discarded immediately once an acknowledgment (ACK) is received from a consumer. | **Append-Only Sequential Log**: Messages are written to persistent, segmented disk logs (`.log` and `.idx`) and retained regardless of consumption. |
| **Message Replay** | **Impossible**: Once removed from the queue, a message cannot be re-read. | **Supported**: Consumers track their own offsets and can rewind to offset `0` or seek arbitrarily to replay historical streams. |
| **Consumption Model** | **Push**: The broker maintains consumer state and pushes messages to connected workers. | **Pull**: Consumers poll partitions independently at their own pace without broker-side message leasing locks. |
| **Scalability & Routing** | Uses complex routing keys and exchange bindings. Degrades significantly under massive backpressure or large queue depths. | Uses **partitioning** to split topic logs horizontally across nodes, achieving linear scale and massive parallel processing. |
| **Data Transfer Engine** | Heap buffers and userspace message framing. | **Kernel DMA Zero-Copy**: Data is dispatched from OS page cache directly to network sockets (`sendfile(2)`). |

**Conclusion**: Calling the project "MQ" was a historical misnomer. AeroStream is fundamentally a **Distributed Append-Only Event Streaming Platform**.

---

## 2. Retention Policies in AeroStream

AeroStream implements a deterministic, multi-tiered retention policy engine inside its Rust storage kernel (`rust-broker/src/log/manager.rs` and `config/broker.example.toml`):

| Policy Metric | Config Setting | Default Value | Mechanism & Behavior |
| :--- | :--- | :--- | :--- |
| **Time-Based (Age)** | `max_retention_age_secs` | `604800` (**7 days**) | Matches Kafka's default (`log.retention.hours=168`). Closed segments whose last modified timestamp exceeds this limit are evicted. |
| **Size-Based** | `max_retention_size` | `1073741824` (**1 GiB**) | Matches Kafka's `log.retention.bytes`. If partition disk usage exceeds this threshold, oldest sealed segments are evicted first. |
| **Segment Rolling** | `max_segment_size` | `134217728` (**128 MiB**) | Matches `log.segment.bytes`. When the active appending log hits this threshold, it is sealed into immutable `.log` and `.idx` files. |
| **Tiered Cold Storage** | `cold_storage_dir` | Path on block storage | Sealed segments can be archived into secondary cold storage before local NVMe eviction, allowing long-term replay. |
| **Active Head Protection**| Hardcoded Safety | Head Segment Protected | The retention engine will **never delete the currently active appending segment**, guaranteeing write continuity even under full disk quotas. |

---

## 3. Comprehensive Feature Comparison Matrix

| Capability | Apache Kafka (v3.9 / 4.0 KRaft) | Redpanda (C++/Seastar) | AeroStream (Current) | Status in AeroStream |
| :--- | :---: | :---: | :---: | :--- |
| **Storage Architecture** | Append-Only Log (Java Heap + Page Cache) | Append-Only Log (Thread-Per-Core C++) | **Append-Only Log (Rust `mmap` + Zero-Copy DMA)** | **Implemented** |
| **Consensus & Metadata** | Native KRaft (Metadata quorum) | Native Raft (Embedded C++) | **Native Raft (Go HashiCorp Raft)** | **Implemented** |
| **Kernel Zero-Copy** | `FileChannel.transferTo()` | Direct I/O via Seastar | **Linux `sendfile(2)` + CPU-affinity pinning** | **Implemented** |
| **Web UI Console** | External (AKHQ, Conduktor, Provectus) | External / Cloud Console | **Embedded Native Console (`/aerostream/console`)** | **Implemented** |
| **All-In-One Container** | Complex (multiple containers) | Single binary | **Full-Stack Container (Broker + Controller + UI)** | **Implemented** |
| **Kafka Wire Protocol** | Native | **100% Wire Compatible** | Custom TCP framing + gRPC | **Not Yet Supported** |
| **Log Compaction** | `cleanup.policy=compact` | Supported | Delete Only (Age/Size) | **Not Yet Supported** |
| **Exactly-Once Semantics** | Idempotent Producer + 2PC Coordinator | Idempotent Producer + 2PC | At-least-once (`acks=1`) | **Not Yet Supported** |
| **Cloud Object Storage Tier** | KIP-405 (S3 / GCS / Azure) | Native Shadow Indexing (S3 / GCS) | Local Disk Cold Tier | **Not Yet Supported** |
| **Built-in Schema Registry** | External (Confluent / Karapace) | **Built-in Schema Registry (Avro/Proto/JSON)** | None (Opaque byte slices) | **Not Yet Supported** |
| **In-Broker Stream Transforms**| External (Flink / Kafka Streams) | **Native WASM Data Transforms** | None | **Not Yet Supported** |
| **Enterprise RBAC / ACLs** | SASL/SCRAM, Kerberos, Granular ACLs | SASL/SCRAM, OIDC, RBAC | Shared Bearer Token + TLS | **Not Yet Supported** |
| **Consumer Rebalancing** | Cooperative Sticky (KIP-848) | Cooperative Sticky (KIP-848) | Custom Coordinator + Lag Tracking | **Partial** |
| **Connectors Ecosystem** | 300+ Kafka Connect plugins | Compatible with Kafka Connect | Custom Client SDKs | **Not Yet Supported** |

---

## 4. Deep-Dive: Features in Kafka & Redpanda Not Yet in AeroStream

### 1. Apache Kafka Wire Protocol Compatibility (The #1 Ecosystem Enabler)
* **What Kafka & Redpanda Have**: Full binary protocol support for standard Kafka ApiKeys (`Produce` 0, `Fetch` 1, `ListOffsets` 2, `Metadata` 3, `OffsetCommit` 8, `JoinGroup` 11, etc.). Because Redpanda implements this protocol, any application written in Java (`kafka-clients`), Python (`confluent-kafka`), Go (`sarama`), or C# (`Confluent.Kafka`) connects with zero modifications.
* **AeroStream Today**: Uses an ultra-lean custom binary framing protocol over TCP (`rust-broker/src/net/mod.rs`) and gRPC for controller interactions.
* **Engineering Impact**: AeroStream cannot currently leverage existing enterprise tooling, Kafka Connect, or standard client SDKs without a wire-protocol compatibility shim.

### 2. Log Compaction (`cleanup.policy=compact`)
* **What Kafka & Redpanda Have**: Instead of discarding records when time or size limits expire, a compacted topic preserves the **latest record for every unique key**. Deletions are performed by producing a "tombstone" (key with a `null` payload).
* **Why It Matters**: Log compaction is essential for Change Data Capture (CDC via Debezium), table caching, database replication, and stream processing state backends (like Kafka Streams or Flink `KTable`).
* **AeroStream Today**: Only supports age-based and size-based segment deletion.

### 3. Exactly-Once Semantics (EOS) & Idempotent Transactions
* **What Kafka & Redpanda Have**:
  1. **Idempotent Producers**: Every producer is assigned a unique Producer ID (`PID`) and sequence numbers per partition. During network timeouts or retries, duplicate messages are detected and rejected by the broker.
  2. **Transaction Coordinator**: Allows applications to write atomically across multiple topics and commit consumer offsets within a single two-phase commit boundary (`InitProducerId`, `AddPartitionsToTxn`, `EndTxn`).
* **AeroStream Today**: Implements reliable at-least-once delivery with explicit persistence ACK confirmation (`acks=1`), but does not feature cross-partition atomic transactions or sequence de-duplication.

### 4. Cloud Object Storage Tiered Storage (Shadow Indexing)
* **What Redpanda Has**: Redpanda's "Shadow Indexing" automatically offloads sealed log segments to cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or MinIO). Consumers can fetch messages from 2 years ago, and Redpanda transparently streams the data from S3 without the client realizing it is no longer on local NVMe disk.
* **AeroStream Today**: Implements a local filesystem-based cold archive directory (`cold_storage_dir`), but does not yet feature direct S3 multipart uploading or remote virtual index management.

### 5. Built-in Schema Registry
* **What Redpanda Has**: Redpanda embeds a Confluent-compatible Schema Registry directly into the broker binary, enforcing Avro, Protobuf, and JSON schema evolution rules (backward/forward compatibility) upon write.
* **AeroStream Today**: Treats message payloads as raw, opaque binary byte buffers (`Vec<u8>`).

### 6. In-Broker Serverless Stream Processing (WASM Data Transforms)
* **What Redpanda Has**: Developers can deploy WebAssembly (Wasm) functions directly onto the broker. Redpanda executes these transforms on the CPU core holding the partition leader, allowing inline masking of PII, filtering, or JSON transcoding before records are committed to disk.
* **AeroStream Today**: Storage and ingestion plane only; transformation logic must be performed by client consumers.

### 7. Fine-Grained Role-Based Access Control (RBAC) & ACLs
* **What Kafka & Redpanda Have**:
  * SASL mechanisms: `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI` (Kerberos), `OAUTHBEARER`.
  * Granular Access Control Lists: *"Allow Principal `payments-svc` WRITE on `orders-*`, but DENY READ on `audit-logs`"*.
* **AeroStream Today**: Supports TLS data plane encryption and shared bearer token authentication (`auth.token`), but does not yet have per-user or per-topic ACL tables.

---

## 5. Strategic Roadmap for AeroStream

```
+-----------------------------------------------------------------------------------+
|                        AeroStream Evolution Roadmap                               |
+-----------------------------------------------------------------------------------+
| Phase 1: Kafka Wire Protocol Shim  -> Produce / Fetch / Metadata API compatibility|
| Phase 2: Log Compaction Cleaner    -> Key-hash indexing & Tombstone garbage coll. |
| Phase 3: S3 Cloud Tiered Storage   -> Async segment offloading to AWS S3 & MinIO  |
| Phase 4: Idempotent Producer EOS   -> Producer ID (PID) & sequence de-duplication |
| Phase 5: Built-in Schema Registry  -> Avro / Protobuf validation in Go Controller |
+-----------------------------------------------------------------------------------+
```

1. **Phase 1: Kafka Protocol Wire Compatibility Shim (Top Priority)**:
   * Build a protocol translation layer in the Rust broker handling `Produce` (ApiKey 0), `Fetch` (ApiKey 1), and `Metadata` (ApiKey 3). This will immediately unlock all standard Kafka client libraries and integrations.
2. **Phase 2: Log Compaction Cleaner**:
   * Add a background compaction cleaner thread in `rust-broker/src/log/manager.rs` to build key-offset hash indexes and deduplicate older closed segments.
3. **Phase 3: S3 Cloud Tiered Storage**:
   * Extend the current cold storage pipeline to upload rolled segments asynchronously to AWS S3 / MinIO via `aws-sdk-s3`.
4. **Phase 4: Idempotent Producer ID & Sequence De-duplication**:
   * Track producer sequence numbers in memory per active partition head to guarantee zero duplicate messages during network retries.

---

*For detailed benchmark metrics and performance test logs across 1 MB, 10 MB, and 50 MB payloads, see [`docs/BENCHMARK_RESULTS.md`](./BENCHMARK_RESULTS.md).*
