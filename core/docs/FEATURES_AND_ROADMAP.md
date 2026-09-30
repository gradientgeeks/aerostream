# AeroStream: Architecture, Features & Roadmap

---

## 1. Why "AeroStream" Instead of "AeroMQ"?

To understand the fundamental identity of **AeroStream**, it helps to contrast traditional Message Queues with Distributed Event Streaming Logs:

| Dimension | Traditional Message Queue (e.g., RabbitMQ, ActiveMQ, SQS) | Distributed Event Streaming Log (AeroStream) |
| :--- | :--- | :--- |
| **Data Storage Model** | **Ephemeral Queue**: Messages are discarded immediately once an acknowledgment (ACK) is received from a consumer. | **Append-Only Sequential Log**: Messages are written to persistent, segmented disk logs (`.log` and `.idx`) and retained regardless of consumption. |
| **Message Replay** | **Impossible**: Once removed from the queue, a message cannot be re-read. | **Supported**: Consumers track their own offsets and can rewind to offset `0` or seek arbitrarily to replay historical streams. |
| **Consumption Model** | **Push**: The broker maintains consumer state and pushes messages to connected workers. | **Pull**: Consumers poll partitions independently at their own pace without broker-side message leasing locks. |
| **Scalability & Routing** | Uses complex routing keys and exchange bindings. Degrades significantly under massive backpressure or large queue depths. | Uses **partitioning** to split topic logs horizontally across nodes, achieving linear scale and massive parallel processing. |
| **Data Transfer Engine** | Heap buffers and userspace message framing. | **Kernel DMA Zero-Copy**: Data is dispatched from OS page cache directly to network sockets (`sendfile(2)`). |

**Conclusion**: Calling the project "MQ" was a historical misnomer. AeroStream is fundamentally a **Distributed Append-Only Event Streaming Platform**.

![AeroStream Zero-Copy Produce & Fetch Pipeline](images/produce_fetch_pipeline.png)

---

## 2. Retention Policies in AeroStream

AeroStream implements a deterministic, multi-tiered retention policy engine inside its Rust storage kernel (`rust-broker/src/log/manager.rs` and `config/broker.example.toml`):

![AeroStream Multi-Cloud Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

| Policy Metric | Config Setting | Default Value | Mechanism & Behavior |
| :--- | :--- | :--- | :--- |
| **Time-Based (Age)** | `max_retention_age_secs` | `604800` (**7 days**) | Same semantics as Kafka's `log.retention.hours` (default 168 h). Closed segments whose last modified timestamp exceeds this limit are evicted. |
| **Size-Based** | `max_retention_size` | `1073741824` (**1 GiB**) | Same semantics as Kafka's `log.retention.bytes`. If partition disk usage exceeds this threshold, oldest sealed segments are evicted first. |
| **Segment Rolling** | `max_segment_size` | `134217728` (**128 MiB**) | Same semantics as Kafka's `log.segment.bytes`. When the active appending log hits this threshold, it is sealed into immutable `.log` and `.idx` files. |
| **Tiered Cold Storage** | `cold_storage_dir` | Path on block storage | Sealed segments can be archived into secondary cold storage before local NVMe eviction, allowing long-term replay. |
| **Active Head Protection**| Hardcoded Safety | Head Segment Protected | The retention engine will **never delete the currently active appending segment**, guaranteeing write continuity even under full disk quotas. |

---

## 3. Feature Status Matrix

![AeroStream Cluster Topology & Zero-Downtime Scale-Down](images/cluster_topology_scale_down.png)

| Capability | AeroStream Implementation | Status |
| :--- | :--- | :--- |
| **Storage Architecture** | **Append-Only Log (Rust `mmap` + Zero-Copy DMA)** | **Implemented** |
| **Consensus & Metadata** | **Native Raft (Go HashiCorp Raft)** | **Implemented** |
| **Kernel Zero-Copy** | **Linux `sendfile(2)` + CPU-affinity pinning** | **Implemented** |
| **Web UI Console** | **Embedded Native Console (`/aerostream/console`)** | **Implemented** |
| **All-In-One Container** | **Full-Stack Container (Broker + Controller + UI)** | **Implemented** |
| **Kafka Wire Protocol** | **Native TCP Shim: data plane (0,1,3,18), transactions (22,24-26,28), SASL (17,36), consumer groups & admin (2,8-16,19,20,32,33,37,42-44,60), share groups (76-79)** | **Implemented** |
| **Log Compaction** | **Key-Hash Deduplication & Tombstone GC** | **Implemented** |
| **Exactly-Once Semantics** | **Idempotent + Transactional Producer (KIP-98 TV1), `read_committed`, LSO, per-broker coordinator** | **Implemented** |
| **Cloud Object Storage Tier** | **Multi-Cloud (AWS S3, MinIO, GCS, Azure, Local)** | **Implemented** |
| **Built-in Schema Registry** | **Confluent-Compatible Schema Registry** | **Implemented** |
| **In-Broker Stream Transforms** | **Native WASM & Stream Data Transforms Engine + Web Console** | **Implemented** |
| **Enterprise RBAC / ACLs** | **Granular Topic/Group ACLs, Principal Roles, REST API & Web UI** | **Implemented** |
| **Consumer Rebalancing** | **Classic consumer-group protocol (JoinGroup / SyncGroup / Heartbeat) with client-side assignors, including cooperative-sticky; the KIP-848 server-side protocol is not implemented (see 6.5)** | **Implemented (classic protocol)** |
| **Connectors Ecosystem** | **Kafka Connect Compatible API + Native Connector Manager & Web UI** | **Implemented** |
| **Multi-Partition Transactions** | **Transactional producer, `read_committed`, LSO isolation, commit/abort control batch markers, for partitions led by one broker** | **Implemented (single-broker scope, see 6.3)** |
| **10k+ Partition Density** | **LRU file-descriptor pool, two-level sparse index, dormant-partition eviction (10,000 partitions measured at the storage layer with about 2,050 open descriptors)** | *Phase 10 (core implemented; broker-level scale test pending)* |
| **Wire Security (SASL / mTLS)** | **Wire SASL (PLAIN & SCRAM-SHA-256 ApiKey 17/36) + Mutual TLS (mTLS) with Subject CN Principal Extraction + REST RBAC** | **Implemented** |
| **Cross-Datacenter Geo-Replication** | Multi-Cloud S3/GCS/Azure Offload (WAN in dev) | *Phase 13 (Planned)* |
| **Chaos & Production Hardening** | Comprehensive Unit, Integration & Benchmarks | *Phase 14 (Planned)* |
| **Compression Codecs** | **All four codecs validated on produce, `compression.type` per topic, decompress for compaction / Iceberg** | **Implemented (wire only: multi-record batches are stored uncompressed, see 6.1)** |
| **Client Quotas / Throttling** | **Same three quotas, Kafka precedence, `throttle_time_ms`, REST `/api/quotas`** | **Implemented (client-id scope; no SASL principal yet)** |
| **Rack Awareness / Follower Fetch** | **`--rack`, rack-spread placement, Fetch v11 `preferred_read_replica`** | **Implemented** |
| **Share Groups (Queues)** | **ShareGroupHeartbeat / ShareFetch / ShareAcknowledge, acquisition locks, DLQ** | **Implemented (v1)** |
| **Iceberg Topics** | **Parquet + Iceberg v2 metadata, read back by pyiceberg (`iceberg` cargo feature)** | **Implemented (unpartitioned, at-least-once)** |
| **Stateful Stream Processing** | **Windowed aggregations, stream-table joins, state store, interactive queries, UI page** | **Implemented (controller-side)** |

---

## 4. Deep-Dive: Enterprise Capabilities Implemented in AeroStream (Phases 1 – 8)

### 1. Apache Kafka Wire Protocol Compatibility (Phase 1)
* **Protocol coverage**: Full binary protocol support for standard Kafka ApiKeys (`Produce` 0, `Fetch` 1, `ListOffsets` 2, `Metadata` 3, `OffsetCommit` 8, `JoinGroup` 11, etc.). Applications written in Java (`kafka-clients`), Python (`confluent-kafka`, `kafka-python`), Go (`sarama`), or C# (`Confluent.Kafka`) connect with zero modifications.
* **AeroStream Implementation**: Implemented a native binary wire protocol listener on TCP port `9092` (`rust-broker/src/kafka/` and `rust-broker/src/net/kafka_server.rs`). Supports `Produce` (ApiKey 0, v0-v9), `Fetch` (ApiKey 1, v0-v12), `Metadata` (ApiKey 3, v0-v9), and `ApiVersions` (ApiKey 18, v0-v3). Standard clients connect directly with zero proxies or code changes.

### 2. Log Compaction (`cleanup.policy=compact`) (Phase 2)
* **Concept**: Instead of discarding records solely when time or size limits expire, a compacted topic preserves the **latest record for every unique key**. Deletions are performed by producing a "tombstone" (key with a `null` payload).
* **AeroStream Implementation**: Implemented in `rust-broker/src/log/compactor.rs` and `rust-broker/src/log/manager.rs`. A background cleaner thread scans sealed segments, constructs key-offset hash tables, writes deduplicated segments directly to disk, and executes tombstone garbage collection after the configured retention period (`delete_retention_ms`).

### 3. Exactly-Once Semantics (EOS) & Idempotent Transactions (Phase 4)
* **Concept**: Every producer is assigned a unique Producer ID (`PID`) and monotonically increasing sequence numbers per partition. During network retries or failovers, duplicate messages are detected and rejected by the broker without error.
* **AeroStream Implementation**: Implemented producer ID (`PID`) sequence de-duplication inside the broker appending pipeline. Partition log heads track active sequence windows in memory, guaranteeing that duplicated produce batches due to client retries are cleanly deduplicated with zero message loss or duplication.

### 4. Multi-Cloud Object Tiered Storage (Phase 3)
* **Concept**: Sealed log segments are automatically offloaded to cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or MinIO), streaming historical data seamlessly.
* **AeroStream Implementation**: Implemented a modular, multi-cloud storage abstraction layer (`rust-broker/src/storage/`) featuring:
  * **AWS S3 & MinIO**: Direct async multipart uploads via `aws-sdk-s3`.
  * **Google Cloud Storage (GCS)** & **Azure Blob Storage**: Pluggable provider factories.
  * **Local Cold NVMe Tier**: Fast fallback storage.
  * **Async Offloader Engine**: A background daemon thread periodically polls for sealed segments older than the tiered storage threshold, safely archiving them to remote object storage while preserving local cache indexes.

### 5. Built-in Schema Registry (Phase 5)
* **Concept**: A Confluent-compatible Schema Registry embedded in the broker enforces schema evolution rules (backward/forward compatibility) upon write for Avro, Protobuf, and JSON schemas.
* **AeroStream Implementation**: Built-in Confluent v7-compatible Schema Registry featuring:
  * Support for **Apache Avro**, **Google Protocol Buffers (Protobuf v3)**, and **JSON Schema (Draft-07)**.
  * Evolution governance enforcing `BACKWARD`, `FORWARD`, and `FULL` compatibility modes.
  * Dedicated Web Console Schema Registry UI (`/aerostream/console/schemas`) with master-detail navigation, pre-filled templates, syntax validation, and live topic integration (`{topic}-value`).

### 6. In-Broker Stream Transforms Engine (Phase 6)
* **AeroStream Implementation**: Native in-broker stream data transformation engine (`go-controller/pkg/transform/engine.go`) executing WASM bytecodes, PII masking, critical status filtering, and JSON mapping directly within streaming pipelines. Includes an interactive live dry-run tester and Web Console UI (`/aerostream/console/transforms`).

### 7. Enterprise RBAC & Granular ACLs (Phase 7)
* **AeroStream Implementation**: Zero-trust security governance engine (`go-controller/pkg/auth/acls.go`) with principal roles (`SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`), wildcard resource matching (`orders-*`), allow vs. deny precedence rules, and live authorization simulation in the Web Console.

### 8. Connectors Ecosystem & Kafka Connect API (Phase 8)
* **AeroStream Implementation**: Thread-safe Connector Manager (`go-controller/pkg/connect/manager.go`) exposing Kafka Connect-compatible REST API endpoints alongside built-in connectors (AWS S3 Archival, HTTP Webhooks, Database CDC, Elasticsearch) and a visual deployment dashboard (`/aerostream/console/connectors`).

---

## 5. Strategic Roadmap for AeroStream

| Phase | Capability | Focus Area | Status |
|:---|:---|:---|:---|
| **Phase 1** | Kafka Wire Protocol Shim | Produce, Fetch, Metadata, ApiVersions | **Completed** |
| **Phase 2** | Log Compaction Cleaner | Key-hash indexing & tombstone GC | **Completed** |
| **Phase 3** | Multi-Cloud Tiered Storage | S3, MinIO, GCS, Azure Blob offload | **Completed** |
| **Phase 4** | Idempotent Producer EOS | PID & sequence deduplication | **Completed** |
| **Phase 5** | Built-in Schema Registry | Avro, Protobuf, JSON Schema | **Completed** |
| **Phase 6** | Stream Transforms | Inline filtering, masking & WASM | **Completed** |
| **Phase 7** | Enterprise RBAC & ACLs | Principal roles & Swiss Tables lookup | **Completed** |
| **Phase 8** | Connectors Ecosystem | Kafka Connect REST API & native tasks | **Completed** |
| **Phase 9** | Distributed 2PC Transactions | Multi-topic atomic commits & WAL | **Completed** |
| **Phase 10** | Massive Partition Density | Shard-per-core partition routing | **Active / In Progress** |
| **Phase 11** | Enterprise Wire Security | SASL/PLAIN, SASL/SCRAM-SHA-256 | **Completed** |
| **Phase 12** | Stateful Stream Processing | Windows & KTable state stores | Planned |
| **Phase 13** | Geo-Replication | Active-Active cross-DC mirroring | Planned |
| **Phase 14** | Chaos & Jepsen Hardening | Failure injection & soak verification | Planned |

---

### Implemented Capabilities (Phases 1 – 8)

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

### Next-Generation Enterprise Horizon (Phases 9 – 14)

> **Status (September 2026):** the core of Phase 10 (descriptor pool, two-level index, dormant partitions) has landed; parts of Phase 9 (transactional producer with `read_committed`, single-coordinator scope) and Phase 12 (windowed aggregations and stream-table joins in the controller) have landed; see Section 6 for their limits. The rest of the plan below is unchanged.

#### Phase 9: End-to-End Exactly-Once Distributed Transactions (2PC)
* **Goal**: full two-phase commit (2PC) distributed transactions (`InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `WriteTxnMarkers`) along with zombie fencing across multiple topics and partitions.
* **AeroStream Horizon**:
  * Implement the **Transaction Coordinator** module within the Go Controller and Rust broker storage engine.
  * Wire Kafka Transactional ApiKeys (`AddPartitionsToTxn` ApiKey 24, `AddOffsetsToTxn` ApiKey 25, `EndTxn` ApiKey 26, `WriteTxnMarkers` ApiKey 27).
  * Introduce transactional log markers (`COMMIT` / `ABORT`) into segment files, ensuring atomic commits across multiple topics and partitions with consumer isolation level `read_committed`.
  * Support transactional consumer offset commits (`read-process-write` cycles) with zombie fencing.

#### Phase 10: Massive Partition Density (10,000+ Partitions per Broker)
* **Goal**: host 10,000+ active partitions per broker node via hierarchical index caching, compact in-memory state models, and lazy file descriptor pooling.
* **Implemented (September 2026):**
  * **LRU file-descriptor pool** (`storage.max_open_segment_files`, default 2,048): partitions no longer pin two descriptors each; evicted files reopen on demand.
  * **Two-level sparse index**: a 4-byte in-memory sample per 128 index entries narrows a lookup to one positioned read of the on-disk index.
  * **Dormant partitions** (`storage.partition_idle_secs`): idle partitions release descriptors and index state.
  * **Batched appends**: a multi-record produce batch is written with one log write and one index write.
  * Measured at the storage layer with 10,000 partitions in one process: about 2,050 open descriptors (4 when idle) and about 15 MB of extra memory. See [`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md) section 5.
* **Remaining**: a broker-level scale test with network traffic at 10,000+ partitions, partition admission control against the descriptor and memory budget, and slab-based eviction of idle partition state (which needs persisted producer and transaction state).
* **Original plan**:
  * Replace static open file descriptors with an **LRU File Descriptor & Mmap Cache Pool**. Inactive partition segments will release open handles back to the OS pool, removing `ulimit -n` bottlenecks.
  * Implement **Sparse Two-Level Indexing**: Keep primary segment indexes compact in memory (~4 bytes per entry) and load detailed byte offsets dynamically on demand.
  * Benchmark and validate dense partition scaling up to 25,000 active partitions per broker on modest hardware without OS file descriptor exhaustion.

#### Phase 11: Enterprise Security & Authentication Protocols
* **Goal**: SASL/SCRAM, Kerberos / GSSAPI, OAuth2 / OIDC token authentication, and mutual TLS (mTLS) with dynamic certificate rotation directly over the wire protocol.
* **Implemented Capabilities (September 2026)**:
  * **Wire SASL Authentication**: Full support for `SaslHandshake` (ApiKey 17) and `SaslAuthenticate` (ApiKey 36) implementing `PLAIN` and `SCRAM-SHA-256` mechanisms with direct integration into the `AclManager` RBAC engine.
  * **Mutual TLS (mTLS) & Client Certificate Authentication**: Integrated `tokio-rustls` engine supporting:
    * Server and mutual TLS on both the native data-plane listener (`net::server`) and Kafka wire-protocol listener (`net::kafka_server`).
    * Client certificate verification via `WebPkiClientVerifier` backed by a trusted root CA (`tls.ca_file`).
    * Subject Common Name (`CN`) principal extraction (`tls.client_cert_principal: true`), directly authenticating clients and mapping them to RBAC/ACL principals without requiring SASL.
    * Outbound TLS/mTLS client credentials (`tls.client_cert_file`, `tls.client_key_file`) for broker-to-controller gRPC synchronization and peer broker replica fetch.
* **AeroStream Horizon (Next Steps)**:
  * Support `SCRAM-SHA-512` and Kerberos / GSSAPI SASL mechanisms.
  * Support OAuth2 / OIDC token bearer authentication over the wire protocol (KIP-255).
  * Implement dynamic, zero-downtime certificate reloading (ACME / Let's Encrypt / HashiCorp Vault integration) on both broker data ports and controller gRPC/REST listeners.

#### Phase 12: Ecosystem & Stateful Stream Processing Frameworks
* **Goal**: interoperability with stream-processing frameworks (Kafka Streams, ksqlDB, Apache Flink, Apache Spark Structured Streaming) and stateful processing on top of AeroStream topics.
* **AeroStream Horizon**:
  * Expand the built-in Stream Transforms Engine from stateless inline mapping to a **Distributed Stateful Stream Processing Framework**.
  * Integrate embedded local key-value state stores (e.g. Sled / RocksDB) backed by AeroStream changelog topics.
  * Support stateful streaming abstractions: event-time windowing (tumbling, hopping, sliding, session), stateful joins, and aggregation topologies.
  * Publish certified native connectors for Apache Flink, Apache Spark, and Debezium CDC.

#### Phase 13: Cross-Datacenter Active-Active Geo-Replication
* **Goal**: continuous cross-cluster replication and multi-cluster shadow indexing.
* **AeroStream Horizon**:
  * Implement the **AeroStream Mirroring & Georeplication Engine**: An asynchronous, high-throughput replication service that continuously mirrors topics and partitions across geographic regions.
  * Support active-active bidirectional replication with cyclic loop detection (cluster provenance header tags) and deterministic conflict resolution.
  * Automated cross-cluster consumer group offset translation for seamless disaster recovery (DR) failovers.

#### Phase 14: Battle-Testing, Chaos Engineering & Production Hardening
* **Goal**: production hardening: surviving split-brain scenarios, disk failures, network partitions, and hardware corruption at scale.
* **AeroStream Horizon**:
  * Deploy automated **Chaos Mesh** and **Jepsen testing suites** into CI/CD, rigorously validating:
    * Raft leader election during sudden controller kills and network partitioning.
    * Storage data plane integrity under power-loss simulations (`kill -9`, sudden host crash, power-cut fsync verification).
    * Split-brain recovery, partition reassignment races, and disk I/O stall handling.
  * Execute long-running multi-day soak tests pushing continuous 1 GB/s ingestion under varying failure conditions.
  * Publish production runbooks, automated disaster recovery scripts, and Grafana / Prometheus dashboards.

---

*For detailed benchmark metrics and performance test logs across 1 MB, 10 MB, and 50 MB payloads, see [`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md).*


## 6. Feature-Gap Closure Notes (Phase 9)

Known limitations of the features added in this phase:

* **6.1 Compression at rest.** The log keeps one index entry per offset, so the broker decompresses multi-record batches on produce and stores one uncompressed single-record entry per record. Compression saves producer-to-broker bandwidth only. Storage and Fetch responses are uncompressed. Fixing this needs entries that span several offsets in the log/index layer.
* **6.2 Quotas.** Enforced per `client-id`. The Kafka path has no SASL principal, so `user` quotas need an authenticated principal first. Quota state lives in the controller process, not Raft.
* **6.3 Transactions.** The coordinator runs inside a broker, so a transaction can span only partitions led by that broker (no WriteTxnMarkers, ApiKey 27). No TV2 or KIP-939. Validated with synthetic frames, not a real transactional client.
* **6.4 Share groups.** Group membership is not persisted. Metadata `topic_id` (Metadata v10+) is not served because Metadata tops out at v5.
* **6.5 Admin / groups.** Classic group protocol only (no KIP-848). Group state is in the coordinator's memory. DeleteTopics removes controller metadata and the receiving broker's logs only. Broker-level configs are read-only.
* **6.6 Iceberg.** Enabled from the broker `[iceberg]` config only, not per-topic from the controller. Uncompressed Parquet, no partitioning, no catalog, at-least-once commits. S3 warehouse untested.
* **6.7 Stream processing.** Polls with the legacy fetch path, so it is not transaction-aware and has no exactly-once sink.
