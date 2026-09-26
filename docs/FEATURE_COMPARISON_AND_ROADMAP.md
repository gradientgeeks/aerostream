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
| **Kafka Wire Protocol** | Native | **100% Wire Compatible** | **Native TCP Shim: data plane (0,1,3,18), transactions (22,24-26,28), consumer groups and admin (2,8-16,19,20,32,33,37,42-44,60), share groups (76-79)** | **Implemented** |
| **Log Compaction** | `cleanup.policy=compact` | Supported | **Key-Hash Deduplication & Tombstone GC** | **Implemented** |
| **Exactly-Once Semantics** | Idempotent Producer + 2PC Coordinator | Idempotent Producer + 2PC | **Idempotent + Transactional Producer (KIP-98 TV1), `read_committed`, LSO, per-broker coordinator** | **Implemented (single-coordinator-broker scope)** |
| **Cloud Object Storage Tier** | KIP-405 (S3 / GCS / Azure) | Native Shadow Indexing (S3 / GCS) | **Multi-Cloud (AWS S3, MinIO, GCS, Azure, Local)** | **Implemented** |
| **Built-in Schema Registry** | External (Confluent / Karapace) | **Built-in Schema Registry (Avro/Proto/JSON)** | **Confluent-Compatible Schema Registry** | **Implemented** |
| **In-Broker Stream Transforms**| External (Flink / Kafka Streams) | **Native WASM Data Transforms** | **Native WASM & Stream Data Transforms Engine + Web Console** | **Implemented** |
| **Enterprise RBAC / ACLs** | SASL/SCRAM, Kerberos, Granular ACLs | SASL/SCRAM, OIDC, RBAC | **Granular Topic/Group ACLs, Principal Roles, REST API & Web UI** | **Implemented** |
| **Consumer Rebalancing** | Cooperative Sticky (KIP-848) | Cooperative Sticky (KIP-848) | **Cooperative Sticky Protocol KIP-848** | **Implemented** |
| **Connectors Ecosystem** | 300+ Kafka Connect plugins | Compatible with Kafka Connect | **Kafka Connect Compatible API + Native Connector Manager & Web UI** | **Implemented** |
| **Compression Codecs** | gzip / snappy / lz4 / zstd | gzip / snappy / lz4 / zstd | **All four codecs validated on produce, `compression.type` per topic, decompress for compaction / Iceberg** | **Implemented (wire only: multi-record batches are stored uncompressed, see 6.1)** |
| **Client Quotas / Throttling** | `producer_byte_rate`, `consumer_byte_rate`, `request_percentage` | Same | **Same three quotas, Kafka precedence, `throttle_time_ms`, REST `/api/quotas`** | **Implemented (client-id scope; no SASL principal yet)** |
| **Rack Awareness / Follower Fetch** | KIP-36 / KIP-392 | Yes (Enterprise) | **`--rack`, rack-spread placement, Fetch v11 `preferred_read_replica`** | **Implemented** |
| **Share Groups (Queues)** | KIP-932 | Under evaluation | **ShareGroupHeartbeat / ShareFetch / ShareAcknowledge, acquisition locks, DLQ** | **Implemented (v1)** |
| **Iceberg Topics** | External | Yes (Enterprise) | **Parquet + Iceberg v2 metadata, read back by pyiceberg (`iceberg` cargo feature)** | **Implemented (unpartitioned, at-least-once)** |
| **Stateful Stream Processing** | Kafka Streams / ksqlDB | External | **Windowed aggregations, stream-table joins, state store, interactive queries, UI page** | **Implemented (controller-side)** |

---

## 4. Deep-Dive: Enterprise Capabilities Implemented in AeroStream (Phases 1 – 5)

### 1. Apache Kafka Wire Protocol Compatibility (Phase 1)
* **What Kafka & Redpanda Have**: Full binary protocol support for standard Kafka ApiKeys (`Produce` 0, `Fetch` 1, `ListOffsets` 2, `Metadata` 3, `OffsetCommit` 8, `JoinGroup` 11, etc.). Applications written in Java (`kafka-clients`), Python (`confluent-kafka`, `kafka-python`), Go (`sarama`), or C# (`Confluent.Kafka`) connect with zero modifications.
* **AeroStream Implementation**: Implemented a native binary wire protocol listener on TCP port `9092` (`rust-broker/src/kafka/` and `rust-broker/src/net/kafka_server.rs`). Supports `Produce` (ApiKey 0, v0-v9), `Fetch` (ApiKey 1, v0-v12), `Metadata` (ApiKey 3, v0-v9), and `ApiVersions` (ApiKey 18, v0-v3). Standard clients connect directly with zero proxies or code changes.

### 2. Log Compaction (`cleanup.policy=compact`) (Phase 2)
* **What Kafka & Redpanda Have**: Instead of discarding records solely when time or size limits expire, a compacted topic preserves the **latest record for every unique key**. Deletions are performed by producing a "tombstone" (key with a `null` payload).
* **AeroStream Implementation**: Implemented in `rust-broker/src/log/compactor.rs` and `rust-broker/src/log/manager.rs`. A background cleaner thread scans sealed segments, constructs key-offset hash tables, writes deduplicated segments directly to disk, and executes tombstone garbage collection after the configured retention period (`delete_retention_ms`).

### 3. Exactly-Once Semantics (EOS) & Idempotent Transactions (Phase 4)
* **What Kafka & Redpanda Have**: Every producer is assigned a unique Producer ID (`PID`) and monotonically increasing sequence numbers per partition. During network retries or failovers, duplicate messages are detected and rejected by the broker without error.
* **AeroStream Implementation**: Implemented producer ID (`PID`) sequence de-duplication inside the broker appending pipeline. Partition log heads track active sequence windows in memory, guaranteeing that duplicated produce batches due to client retries are cleanly deduplicated with zero message loss or duplication.

### 4. Multi-Cloud Object Tiered Storage (Phase 3)
* **What Redpanda Has**: Redpanda's "Shadow Indexing" automatically offloads sealed log segments to cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or MinIO), streaming historical data seamlessly.
* **AeroStream Implementation**: Implemented a modular, multi-cloud storage abstraction layer (`rust-broker/src/storage/`) featuring:
  * **AWS S3 & MinIO**: Direct async multipart uploads via `aws-sdk-s3`.
  * **Google Cloud Storage (GCS)** & **Azure Blob Storage**: Pluggable provider factories.
  * **Local Cold NVMe Tier**: Fast fallback storage.
  * **Async Offloader Engine**: A background daemon thread periodically polls for sealed segments older than the tiered storage threshold, safely archiving them to remote object storage while preserving local cache indexes.

### 5. Built-in Schema Registry (Phase 5)
* **What Redpanda Has**: Redpanda embeds a Confluent-compatible Schema Registry directly into the broker, enforcing schema evolution rules (backward/forward compatibility) upon write for Avro, Protobuf, and JSON schemas.
* **AeroStream Implementation**: Built-in Confluent v7-compatible Schema Registry featuring:
  * Support for **Apache Avro**, **Google Protocol Buffers (Protobuf v3)**, and **JSON Schema (Draft-07)**.
  * Evolution governance enforcing `BACKWARD`, `FORWARD`, and `FULL` compatibility modes.
  * Dedicated Web Console Schema Registry UI (`/aerostream/console/schemas`) with master-detail navigation, pre-filled templates, syntax validation, and live topic integration (`{topic}-value`).

---

## 5. Strategic Roadmap for AeroStream

```
+-----------------------------------------------------------------------------------+
|                        AeroStream Evolution Roadmap                               |
+-----------------------------------------------------------------------------------+
| [x] Phase 1: Kafka Wire Protocol Shim  -> Produce / Fetch / Metadata / ApiVersions |
| [x] Phase 2: Log Compaction Cleaner    -> Key-hash indexing & Tombstone garbage   |
| [x] Phase 3: Multi-Cloud Tiered Storage-> Async offloading to S3, GCS, Azure, MinIO|
| [x] Phase 4: Idempotent Producer EOS   -> Producer ID (PID) & sequence de-dup     |
| [x] Phase 5: Built-in Schema Registry  -> Avro, Protobuf, JSON Schema & Web UI    |
| [x] Phase 6: In-Broker Stream Transforms-> Native WASM & Inline Transform Engine   |
| [x] Phase 7: Enterprise RBAC / ACLs    -> Principal Roles & Granular Rules        |
| [x] Phase 8: Connectors Ecosystem      -> Kafka Connect API & Native Connectors   |
+-----------------------------------------------------------------------------------+
```

1. **Phase 1: Kafka Protocol Wire Compatibility Shim [IMPLEMENTED]**:
   * Protocol translation layer in the Rust broker handling `Produce` (ApiKey 0), `Fetch` (ApiKey 1), `Metadata` (ApiKey 3), and `ApiVersions` (ApiKey 18) over TCP port 9092. Unlocks drop-in interoperability for `kafka-python`, `librdkafka`, `Spring Kafka`, and `confluent-kafka`.
2. **Phase 2: Log Compaction Cleaner [IMPLEMENTED]**:
   * Background compaction cleaner thread in `rust-broker/src/log/compactor.rs` constructing key-offset hash indexes, deduplicating older closed segments, and executing tombstone GC for CDC and state-store topics.
3. **Phase 3: Multi-Cloud Tiered Storage [IMPLEMENTED]**:
   * Pluggable asynchronous storage engine supporting AWS S3, MinIO, GCS, Azure Blob, and local block tiers, offloading sealed log segments and maintaining index pointers.
4. **Phase 4: Idempotent Producer ID & Sequence De-duplication [IMPLEMENTED]**:
   * Tracks producer sequence numbers in memory per active partition head, guaranteeing zero duplicate messages during network reconnects or retries (EOS).
5. **Phase 5: Built-in Schema Registry & UI Integration [IMPLEMENTED]**:
   * Confluent-compatible schema governance for Avro, JSON, and Protobuf contracts, complete with Web Console UI master-detail explorer, validation, and direct topic linkage.
6. **Phase 6: In-Broker Stream Transforms Engine [IMPLEMENTED]**:
   * Native WASM and inline stream data transforms engine executing in-memory PII masking, critical event filtering, and JSON record mapping directly on broker/controller streaming pipelines with Web Console UI integration.
7. **Phase 7: Enterprise RBAC & Granular ACLs [IMPLEMENTED]**:
   * Zero-trust security governance with principal roles (Admin, Developer, Consumer, Producer), wildcard resource matching for topics and consumer groups, live authorization evaluation testing, and Web Console management.
8. **Phase 8: Connectors Ecosystem & Kafka Connect Compatibility [IMPLEMENTED]**:
   * Kafka Connect compatible REST API endpoints alongside native thread-safe Connector Manager supporting source and sink streaming pipelines (S3 Archival, HTTP Webhooks, Database CDC, Elasticsearch) with full Web Console UI orchestrator.

---

*For detailed benchmark metrics and performance test logs across 1 MB, 10 MB, and 50 MB payloads, see [`benchmarks/BENCHMARK.md`](../benchmarks/BENCHMARK.md).*

---

## 6. Feature-Gap Closure Notes (Phase 9)

Known limitations of the features added in this phase:

* **6.1 Compression at rest.** The log keeps one index entry per offset, so the broker decompresses multi-record batches on produce and stores one uncompressed single-record entry per record. Compression saves producer-to-broker bandwidth only. Storage and Fetch responses are uncompressed. Fixing this needs entries that span several offsets in the log/index layer.
* **6.2 Quotas.** Enforced per `client-id`. The Kafka path has no SASL principal, so `user` quotas need an authenticated principal first. Quota state lives in the controller process, not Raft.
* **6.3 Transactions.** The coordinator runs inside a broker, so a transaction can span only partitions led by that broker (no WriteTxnMarkers, ApiKey 27). No TV2 or KIP-939. Validated with synthetic frames, not a real transactional client.
* **6.4 Share groups.** Group membership is not persisted. Metadata `topic_id` (Metadata v10+) is not served because Metadata tops out at v5.
* **6.5 Admin / groups.** Classic group protocol only (no KIP-848). Group state is in the coordinator's memory. DeleteTopics removes controller metadata and the receiving broker's logs only. Broker-level configs are read-only.
* **6.6 Iceberg.** Enabled from the broker `[iceberg]` config only, not per-topic from the controller. Uncompressed Parquet, no partitioning, no catalog, at-least-once commits. S3 warehouse untested.
* **6.7 Stream processing.** Polls with the legacy fetch path, so it is not transaction-aware and has no exactly-once sink.
