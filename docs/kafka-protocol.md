# Apache Kafka Compatibility (Port 9092)

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Drop-In</span>
<span class="md-tag">7 min read</span>
<span class="md-tag">34+ ApiKeys</span>
<span class="md-tag">Magic v2 & Zero-Copy</span>
</div>

AeroStream provides **native, drop-in compatibility with the Apache Kafka wire protocol** over TCP port **`9092`**. It is not a proxy, sidecar, or translation gateway—the Rust data plane storage kernel speaks the native Kafka binary serialization protocol directly on the wire.

Existing enterprise software developed with Kafka SDKs can switch to AeroStream simply by updating the bootstrap server endpoint.

---

## 1. Port 9092 Kafka Wire Protocol Architecture

![AeroStream Dual-Protocol Engine: Native vs. Kafka Wire Protocol](images/native_and_kafka_dual_protocol.png)
*Figure 1: Architectural comparison between AeroStream Native Protocol (Port 9091, 7-byte framing) and Apache Kafka Wire Protocol (Port 9092, RequestHeader v2 framing), with the 5 official client SDKs ([Vector SVG](diagrams/native_and_kafka_dual_protocol.svg) • [Excalidraw Source](diagrams/native_and_kafka_dual_protocol.excalidraw)).*

When a Kafka client connects to `localhost:9092`, AeroStream negotiates protocol capabilities, manages connection framing, handles SASL authentication, and executes produce/fetch cycles adhering strictly to the official Kafka protocol specification.


1. **Protocol Negotiation (`ApiVersions` ApiKey 18)**: Client requests advertised API keys and versions supported by AeroStream.
2. **Metadata Resolution (`Metadata` ApiKey 3)**: Broker queries the Go Controller via gRPC to retrieve the active partition leader and in-sync replicas (ISR).
3. **Optimized Record Ingestion (`Produce` ApiKey 0)**: In-place base offset patching directly writes raw batches via positioned I/O without memory reallocation.

![Produce & Fetch Zero-Copy Pipeline](images/produce_fetch_pipeline.png)

---

## 2. Supported API Keys & Versions Matrix

AeroStream currently implements full wire support for **34+ API Keys**, covering high-speed message delivery, consumer group coordination, transactions, security, and administrative operations:

| ApiKey | Name | Supported Versions | Subsystem & Description |
|---|---|---|---|
| **`0`** | `Produce` | v0 – v8 | Message ingestion with `acks=0`, `acks=1`, `acks=-1`/`all`, compression codecs (`None`, `Gzip`, `Snappy`, `LZ4`, `Zstd`), in-place base offset patching. |
| **`1`** | `Fetch` | v0 – v11 | High watermark ($HW$) tracking, zero-copy `sendfile(2)` streaming, rack-aware replica selection (KIP-392), and min/max byte polling. |
| **`2`** | `ListOffsets` | v0 – v7 | Earliest (`-2`), Latest (`-1`), and timestamp-based index lookups. |
| **`3`** | `Metadata` | v0 – v9 | Cluster topology discovery, active broker addresses, topic partition layouts, leader epochs, and rack IDs. |
| **`8`** | `OffsetCommit` | v0 – v8 | Consumer group offset persistence with epoch validation and metadata payloads. |
| **`9`** | `OffsetFetch` | v0 – v7 | Consumer group partition offset queries with topic filtering. |
| **`10`** | `FindCoordinator` | v0 – v4 | Group coordinator discovery (KeyType: Group or Transaction). |
| **`11`** | `JoinGroup` | v0 – v9 | Consumer group membership management and dynamic protocol negotiation. |
| **`12`** | `Heartbeat` | v0 – v4 | Consumer session liveness verification. |
| **`13`** | `LeaveGroup` | v0 – v5 | Clean consumer group member deregistration. |
| **`14`** | `SyncGroup` | v0 – v5 | Distribution of partition assignment plans computed by group leader. |
| **`15`** | `DescribeGroups` | v0 – v5 | Group state inspection, member list, and assigned partitions. |
| **`16`** | `ListGroups` | v0 – v4 | Enumeration of all registered consumer groups. |
| **`17`** | `SaslHandshake` | v0 – v1 | Negotiating SASL authentication mechanisms (`PLAIN`, `SCRAM-SHA-256`). |
| **`18`** | `ApiVersions` | v0 – v3 | Protocol negotiation, version ranges, and flexible header format resolution. |
| **`19`** | `CreateTopics` | v0 – v7 | Dynamic topic provisioning with partition counts, replication factors, and configs. |
| **`20`** | `DeleteTopics` | v0 – v6 | Topic deletion and partition resource reclamation. |
| **`22`** | `InitProducerId` | v0 – v4 | Idempotent & transactional producer ID allocation and epoch sequencing (KIP-98). |
| **`24`** | `AddPartitionsToTxn` | v0 – v3 | Registering topic partitions within an active transaction. |
| **`25`** | `AddOffsetsToTxn` | v0 – v3 | Binding consumer group offsets to transaction boundaries. |
| **`26`** | `EndTxn` | v0 – v3 | Two-Phase Commit (`COMMIT` or `ABORT`) transaction resolution. |
| **`28`** | `TxnOffsetCommit` | v0 – v3 | Committing consumer offsets within an atomic transaction. |
| **`32`** | `DescribeConfigs` | v0 – v4 | Querying broker, topic, and client quota configurations. |
| **`33`** | `AlterConfigs` | v0 – v2 | Updating cluster and topic dynamic configurations. |
| **`36`** | `SaslAuthenticate` | v0 – v2 | Transporting client SASL authentication tokens. |
| **`37`** | `CreatePartitions` | v0 – v3 | Expanding partition count on existing topics. |
| **`42`** | `DeleteGroups` | v0 – v2 | Deleting empty or dead consumer groups. |
| **`43`** | `ElectLeaders` | v0 – v2 | Triggering partition leader elections (Preferred or Unclean). |
| **`44`** | `IncrementalAlterConfigs` | v0 – v1 | Granular incremental alteration of configurations. |
| **`60`** | `DescribeCluster` | v0 – v1 | Cluster identification, controller ID, and authorized operations. |
| **`76`** | `ShareGroupHeartbeat` | v0 | KIP-932 Share Group heartbeat and membership coordination. |
| **`77`** | `ShareGroupDescribe` | v0 | KIP-932 Share Group state and subscriber inspection. |
| **`78`** | `ShareFetch` | v0 | KIP-932 Cooperative record acquisition and lock leasing. |
| **`79`** | `ShareAcknowledge` | v0 | KIP-932 Per-record ACK / REJECT / RELEASE delivery acknowledgments. |

---

## 3. Zero-Copy `sendfile(2)` Engine

When serving Kafka Fetch requests (`ApiKey 1`), AeroStream bypasses userspace data copying:

![Produce and Fetch Pipeline](images/produce_fetch_pipeline.png)

```text
Conventional Broker (2 Heap Copies + CPU Cache Thrashing):
[ Disk Page Cache ] ──(read syscall)──► [ Userspace Heap ] ──(write syscall)──► [ Socket Ring Buffer ]

AeroStream Zero-Copy (Direct Kernel DMA):
[ Disk Page Cache ] ──────────────────(sendfile syscall)────────────────────► [ Socket Ring Buffer ]
```

1. **Header Framing**: The Kafka `FetchResponse` header (correlation ID, partition headers, high watermark, error codes) is written to the TCP socket buffer.
2. **Kernel DMA**: The payload bytes from the segment file are transferred directly from the Linux Page Cache to the socket file descriptor via `libc::sendfile(2)`.
3. **Cache Preservation**: Multi-megabyte record batches never touch L1/L2 CPU caches, preventing cache eviction of critical metadata indices.

---

## 4. Transaction Isolation & Exactly-Once Semantics (EOS)

AeroStream supports transactional producing and consuming adhering to Apache Kafka's KIP-98 protocol:

1. **`InitProducerId` (ApiKey 22)**: Producer requests PID and epoch allocation from the broker transaction coordinator.
2. **`AddPartitionsToTxn` (ApiKey 24)**: Registers target topic-partitions with the transaction coordinator.
3. **Transactional Produce (`Produce` ApiKey 0)**: Ingests record batches tagged with producer ID, epoch, and sequence numbers.
4. **`EndTxn` (ApiKey 26)**: Coordinates two-phase commit (`COMMIT` or `ABORT`), appending transactional control markers to partition segments and advancing the Last Stable Offset (LSO).

### Isolation Levels

Consumers specify their isolation level in the `Fetch` request:

* **`read_uncommitted` (Isolation Level 0)**: Consumers read all appended records up to the High Watermark ($HW$), including records from in-flight or subsequently aborted transactions.
* **`read_committed` (Isolation Level 1)**: Consumers read only up to the **Last Stable Offset (LSO)**:
  $$\text{LSO} = \min(\text{Offset of first in-flight transaction}, HW)$$
  AeroStream filters out aborted transactions and control batch markers automatically, presenting a clean, linearizable event stream to the application.

---

## 5. In-Place Base-Offset Patching

In Apache Kafka's **Magic v2 RecordBatch** format, records within a batch have delta offsets relative to the batch's `base_offset`. When a producer creates a batch, it sets `base_offset = 0`. The broker must assign a globally monotonic log offset to the batch upon ingestion.

### The Problem with Traditional Proxies

Traditional Kafka bridges decompress the entire record batch in userspace memory, iterate through each individual record, rewrite offsets, recalculate batch metadata, recompress, and re-encode. This incurs $\mathcal{O}(B)$ memory allocation overhead:

$$T_{\text{reencode}} = \mathcal{O}(B) + \mathcal{O}(K \cdot \text{decompress})$$

### AeroStream's $\mathcal{O}(1)$ In-Place Solution

The 32-bit CRC32C checksum in a Kafka RecordBatch covers **only bytes 21 through end-of-batch**. Bytes 0 through 8 contain the 64-bit `base_offset`, which is excluded from the checksum calculation.

AeroStream exploits this property to patch the offset in $\mathcal{O}(1)$ constant time directly on disk using positioned writes (`pwrite(2)` / `FileExt::write_all_at`):

```rust
let base_offset_bytes = next_offset.to_be_bytes();
segment_file.write_all_at(&base_offset_bytes, batch_start_pos)?;
```

No decompression, no memory allocations, and zero CPU cycles spent recomputing checksums.

---

## 6. Client Ecosystem Compatibility

AeroStream has been verified against all major official Kafka client libraries:

| Language | Client Library | Supported Features |
|---|---|---|
| **Java** | `org.apache.kafka:kafka-clients`, `Spring Kafka` | Full producer, consumer, transactions, SASL, TLS |
| **Python** | `confluent-kafka` (librdkafka), `kafka-python` | Full produce, consume, consumer groups, SASL |
| **Go** | `github.com/segmentio/kafka-go`, `github.com/twmb/franz-go` | High-throughput async batching, consumer groups |
| **.NET (C#)** | `Confluent.Kafka` | Produce, consume, transactions, async Task APIs |
| **Node.js** | `kafkajs` | Produce, consume, group rebalancing |
| **Rust** | `rdkafka` (librdkafka binding) | Zero-copy produce/consume, futures-based async |
| **CLI Tools** | `kcat` (`kafkacat`), official Kafka shell scripts | Metadata exploration, console producer/consumer |

### Migration Example

Switching from Apache Kafka or Redpanda to AeroStream requires zero code alterations:

=== "Spring Boot (`application.yml`)"

    ```yaml
    spring:
      kafka:
        bootstrap-servers: aerostream.internal:9092
        producer:
          acks: all
          retries: 3
    ```

=== "Python (`confluent-kafka`)"

    ```python
    from confluent_kafka import Producer

    producer = Producer({'bootstrap.servers': 'aerostream.internal:9092'})
    producer.produce('telemetry', key='sensor-1', value=b'{"temp": 24.2}')
    producer.flush()
    ```

=== "`kcat` CLI"

    ```bash
    # Query cluster metadata
    kcat -b localhost:9092 -L

    # Produce test record
    echo "hello aerostream" | kcat -b localhost:9092 -t telemetry -P

    # Consume from earliest offset
    kcat -b localhost:9092 -t telemetry -C -o beginning
    ```
