# AeroStream

[![GitHub License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-quay.io%2Fgradientgeeks%2Faerostream-blue?logo=docker)](https://quay.io/repository/gradientgeeks/aerostream)
[![Go Report Card](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](go-controller/)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange?logo=rust)](rust-broker/)
[![Architecture](https://img.shields.io/badge/arch-amd64%20%7C%20arm64-brightgreen)](#-quick-start-with-docker)

**AeroStream** is a high-performance, distributed event-streaming and messaging engine designed for extreme throughput, microsecond latencies, and modern multi-cloud workloads. 

Built with a **Dual-Engine Architecture**—pairing a resilient **Go-based Raft control plane** with a zero-copy **Rust-based storage and networking data plane**—AeroStream delivers next-generation event streaming with full Kafka wire-protocol compatibility, built-in tiered storage, schema governance, stream transforms, and an integrated Web Console UI.

---

## ⚡ Key Highlights & Benchmark Comparison (OpenMessaging Benchmark)

Benchmarked using the vendor-neutral **[Linux Foundation OpenMessaging Benchmark (OMB)](https://github.com/openmessaging/benchmark)** framework under strict resource constraints (`--cpus=2.0 --memory=2g`), 1 topic, 16 partitions, 1 KB payloads, and saturated max-rate producers and consumers:

| Metric | AeroStream (Shard-per-Core) | Redpanda (v26.2.3) | AeroStream Advantage |
| :--- | :---: | :---: | :---: |
| **Sustained Publish Rate** | **217,100 msg/s** (212.0 MB/s) | 145,649 msg/s (142.2 MB/s) | **+49.1% Higher Throughput** |
| **Peak Publish Rate** | **243,460 msg/s** | 271,033 msg/s | Consistent throughput floor |
| **Consume Rate** | **217,143 msg/s** | 145,805 msg/s | **Zero Consumer Lag** |
| **Publish Latency ($p_{50}$)** | 5.3 ms | **1.1 ms** | Single-digit millisecond latency |
| **Publish Latency ($p_{99}$)** | **460.2 ms** | 1,678.5 ms | **3.6x Lower Tail Latency** |
| **Publish Latency (Max)** | **591.4 ms** | 2,841.9 ms | **4.8x Lower Max Latency** |
| **Peak Container Memory** | **513 MiB** | 1,514 MiB | **66% Lower RAM Footprint** |
| **Broker Idle Memory** | **1.3 MiB** | 137 MiB | Two orders of magnitude lower |
| **Benchmark Errors** | **0** | **0** | Clean run under max load |

* **Full Benchmark Report & Methodology**: [`benchmarks/BENCHMARK.md`](benchmarks/BENCHMARK.md)
* **Raw OMB Test Runs & Datasets**: [`benchmarks/omb-results/`](benchmarks/omb-results/)
* **Execution & Reproduction Guide**: [`docs/OPENMESSAGING_BENCHMARK_GUIDE.md`](docs/OPENMESSAGING_BENCHMARK_GUIDE.md)

---

## 🚀 Key Features

* **Shard-per-Core Zero-Contention Engine**:
  * Deterministic partition-to-shard mapping (`partition_id % num_shards`) with thread-to-CPU affinity pinning via `libc::sched_setaffinity`.
  * Lock-free actor message passing via `flume::unbounded` channels—zero cross-core mutex locks, atomics, or thread migrations on hot produce/consume paths.
* **Extreme Memory & Storage Optimizations**:
  * In-place base offset patching directly on disk (`write_all_at`), completely bypassing multi-megabyte heap reallocations.
  * Paced page-cache writeback via Linux `sync_file_range(2)` and `posix_fadvise(2)` (pacing dirty flushes every 8 MiB), eliminating Linux kernel writeback stalls in memory-constrained environments.
  * Zero-copy cold tiering via hard links (`fs::hard_link`), decoupling hot partition log rollover from object store network latency.
* **Comprehensive Kafka Wire Protocol Compatibility**:
  * Native listener on port `9092` supporting 34+ Kafka API keys across produce, fetch, metadata, consumer groups, schemas, and ACLs.
  * High-performance Fetch long polling with lazy `tokio::sync::Notify` registration and lockless out-of-lock disk I/O.
  * Enterprise SASL authentication (`PLAIN` and `SCRAM-SHA-256`) and Two-Phase Commit (2PC) Transactions.
  * Drop-in compatibility with standard Kafka client ecosystems (`kafka-python`, `librdkafka`, `kafka-go`, `Confluent.Kafka`, Java / Spring Kafka).
* **Dual-Engine Decoupled Architecture**:
  * **Control Plane (Go)**: Distributed Raft consensus, automated partition leadership elections, dynamic cluster membership, 2-second broker heartbeats with dynamic config piggybacking, and gRPC coordination.
  * **Data Plane (Rust)**: Tokio async runtime, CPU core pinning, kernel zero-copy `sendfile(2)` socket transfers, and memory-mapped (`mmap`) offset indexing.
* **Tiered Multi-Cloud Storage**:
  * Hot partition segments on fast local NVMe/SSD.
  * Transparent, non-blocking background offload to **AWS S3 / MinIO**, **Google Cloud Storage (GCS)**, **Azure Blob Storage**, or network filesystem mounts.
* **Iceberg-Native Topics**:
  * Topics can write directly into Apache Iceberg tables (Parquet/Avro), for lakehouse-native analytics without a separate sink connector.
* **Share Groups (KIP-932 Queue Semantics)**:
  * Cooperative, queue-like consumption where multiple consumers acquire/acknowledge individual records from the same partition without exclusive assignment — ahead of Apache Kafka's own GA timeline for this KIP.
  * Per-record delivery-attempt limits, lock timeouts, and dead-letter-queue (DLQ) forwarding for records that exhaust retries.
* **Built-in Schema Registry**:
  * Confluent-compatible REST API on `/subjects`, `/schemas`, and `/compatibility`.
  * First-class support for **Avro**, **Protobuf**, and **JSON Schema** with `BACKWARD`, `FORWARD`, and `FULL` compatibility validation.
* **In-Broker Stream Processing & Transforms**:
  * Dynamic Stream Processing Engine (`/api/streams`), inline real-time filtering, PII data masking (`MASK_PII`), JSON schema transformation, and WASM runtime.
* **Enterprise Security & Granular RBAC**:
  * Role-based access control (`SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`).
  * Granular topic, consumer group, and cluster ACLs with prefix and wildcard pattern matching.
* **Cooperative Sticky Rebalance Protocol**:
  * KIP-848 style non-blocking cooperative rebalancing avoiding stop-the-world partition revocations.
* **Connectors Ecosystem**:
  * Kafka Connect compatible management API with native connectors: S3 Archival Sink, Webhook REST Sink, Database CDC Source, and Elasticsearch Sink.
* **Modern Web Console UI & Dynamic Control Plane**:
  * Sleek Angular management console with dark/light themes, dynamic cluster topology visualizer, live message inspector, real-time consumer lag monitoring (`/api/lag`), schema registry browser, and policy simulators.

---

## 🐳 Quick Start with Docker

You can run the full AeroStream stack (Controller, Zero-Copy Broker, and Web Console) using the official multi-architecture container image:

```bash
docker run -d --name aerostream \
  -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
  -v aerostream_data:/data \
  quay.io/gradientgeeks/aerostream:latest
```

> 📖 **Deployment Quickstart Guides**:
> * 🐳 **[Docker Quickstart Guide](docs/DOCKER_QUICKSTART.md)**: 30-second local setup with single all-in-one container (`quay.io/gradientgeeks/aerostream:latest`), port mapping, and client samples.
> * ☸️ **[Kubernetes Quickstart Guide](docs/K8S_QUICKSTART.md)**: Production deployment using standard `kubectl` manifests, headless services, StatefulSets, and automated zero-downtime draining.
> * ⎈ **[Helm Quickstart Guide](docs/HELM_QUICKSTART.md)**: Official Helm v3 chart installation, values customization, S3 tiered storage, and rack-aware zone placement.

### Accessing Endpoints:
* **Web Console UI**: [http://localhost:9001/aerostream/console](http://localhost:9001/aerostream/console)
* **Kafka Wire Protocol**: `localhost:9092` (point any Kafka producer/consumer here)
* **Native TCP Data Plane**: `localhost:9091`
* **REST Management API & Schema Registry**: `http://localhost:9001`
* **gRPC Control Plane**: `localhost:8001`

---

## 🏗 Architecture Overview

![AeroStream Dual-Engine Architecture](docs/images/dual_engine_architecture.png)

AeroStream achieves its performance through strict architectural decoupling and hardware-aligned execution:
* **Go Control Plane (Ports 9001 & 8001)**: Drives distributed consensus via HashiCorp Raft, serves the dynamic Schema Registry, manages enterprise RBAC / ACL policies, Stream Processing Engine, and connector runtimes. Brokers maintain a 2-second heartbeat loop with dynamic configuration piggybacking and cluster topology synchronization.
* **Rust Shard-per-Core Data Plane (Ports 9091 & 9092)**: Partitions are mapped deterministically across dedicated shard worker threads pinned to specific CPU cores via `libc::sched_setaffinity`. Each shard runs an isolated event loop processing requests via lock-free actor message passing (`flume::unbounded`), bypassing cross-core locks and atomics.
* **Kernel & Memory Pacing**: Utilizes Linux kernel zero-copy `sendfile(2)` DMA transfers, memory-mapped (`mmap`) sparse index lookup, in-place base offset patching directly on disk (`write_all_at`), and paced dirty page writeback (`sync_file_range(2)` + `posix_fadvise(2)`).
* **Multi-Cloud Tiered Storage**: Automatically rolls sealed 128 MB log segments into an asynchronous offloader queue via zero-copy hard links (`fs::hard_link`), persisting them to AWS S3, MinIO, Google Cloud Storage, or Azure Blob without blocking producer ingestion.

> 📖 **Deep-Dive Architecture Specifications & Diagrams**:
> * ⚡ **[Shard-per-Core Architecture Whitepaper](docs/SHARD_PER_CORE.md)**: Thread-to-core pinning, lock-free actor messaging, paced writeback, and zero-allocation log append.
> * 🧩 **[Shard-per-Core Architecture Diagram](docs/images/shard_per_core_architecture.png)**: Visual guide to core pinning, lock-free channels, and memory pacing.
> * 🛰️ **[Controller-Broker Orchestration Diagram](docs/images/controller_broker_orchestration.png)**: Heartbeat piggybacking, Raft consensus, LEO reporting, and drain workflows.
> * 🚀 **[Zero-Copy Produce & Fetch Pipelines](docs/images/produce_fetch_pipeline.png)**: Step-by-step kernel DMA and mmap write paths.
> * ☁️ **[Multi-Cloud Tiered Storage Pipeline](docs/images/tiered_storage_pipeline.png)**: Non-blocking offloading and safe local eviction.
> * 🔄 **[Cluster Topology & Scale-Down Protocol](docs/images/cluster_topology_scale_down.png)**: 3-Node Raft consensus and graceful broker draining.


---

## 🛠 Local Development & Building from Source

### Prerequisites
* **Go**: 1.26 or higher
* **Rust**: 2024 edition (Cargo & Rustc)
* **Node.js**: 20+ and npm (for Web Console)
* **Make**

### 1. Build all components
```bash
make build
```
This builds:
* `go-controller/bin/controller`: Go cluster controller and Raft consensus daemon.
* `rust-broker/target/release/rust-broker`: Rust zero-copy storage broker.
* `client/bin/client`: Go CLI client and benchmark utility.

### 2. Start a Local 5-Node Cluster
Start 3 Go Raft controllers and 2 Rust storage brokers locally:
```bash
make start
```

### 3. Check Cluster Health
```bash
make status
```

### 4. Interactive CLI Usage
```bash
# Create a topic with 3 partitions and replication factor 2
./client/bin/client create-topic orders 3 2

# Produce messages using native protocol
./client/bin/client produce orders 0 '{"order_id": "ORD-1001", "amount": 99.50}'

# Consume messages
./client/bin/client consume orders 0 0 --follow
```

### 5. Standard Kafka Client Usage
Produce and consume via standard Kafka tools or Python:
```python
from kafka import KafkaProducer, KafkaConsumer

# Produce via standard Kafka protocol to port 9092
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('orders', b'{"order_id": "ORD-1002", "status": "CONFIRMED"}')
producer.flush()

# Consume via standard Kafka protocol
consumer = KafkaConsumer('orders', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
for message in consumer:
    print(f"Received: offset={message.offset} value={message.value.decode('utf-8')}")
    break
```

### 6. Official .NET / C# Client Usage (`Confluent.Kafka`)
AeroStream is fully compatible with .NET 8 / 9 via official `Confluent.Kafka`:
```csharp
using Confluent.Kafka;

var config = new ProducerConfig {
    BootstrapServers = "localhost:9092",
    EnableIdempotence = true // KIP-98 exactly-once semantics
};
using var producer = new ProducerBuilder<string, string>(config).Build();
var deliveryReport = await producer.ProduceAsync("orders", new Message<string, string> {
    Key = "order-1002",
    Value = "{\"status\": \"CONFIRMED\"}"
});
Console.WriteLine($"Delivered to {deliveryReport.TopicPartitionOffset}");
```
*Full 4-test .NET test suite located in [`examples/dotnet-app/`](examples/dotnet-app/).*

---

## 🧪 Testing

Run the test suite across all sub-systems:

```bash
# Test Rust Storage Engine & Kafka Protocol
cd rust-broker && cargo test

# Test Go Controller, Consensus & Schema Registry
cd go-controller && go test -v ./...

# Test CLI Client
cd client && go test -v ./...
```

---

## 📜 Documentation & Guides

* **[Documentation Portal (Index & Overview)](docs/README.md)**: Centralized knowledge base, role-based reading paths, and complete document catalog.
* **[Architecture Deep-Dive](docs/ARCHITECTURE.md)**: Dual-Engine internals, memory-mapping, zero-copy `sendfile(2)`, Raft FSM, and threading models.
* **[Shard-per-Core Architecture Whitepaper](docs/SHARD_PER_CORE.md)**: Technical deep-dive on CPU core affinity, lock-free actor channels, paced writeback, and zero-allocation log append.
* **[Operator & Deployment Guide](docs/OPERATOR_GUIDE.md)**: Cluster bootstrapping, Kubernetes StatefulSets, automated scale-down, broker draining, and monitoring.
* **[REST & Wire Protocol API Reference](docs/API_REFERENCE.md)**: Complete endpoint schemas, Schema Registry, Stream Transforms, ACLs, and Kafka wire framing.
* **[Feature Comparison & Evolution Roadmap](docs/FEATURE_COMPARISON_AND_ROADMAP.md)**: Detailed breakdown vs. Apache Kafka and Redpanda, and Next-Gen Enterprise Roadmap (Phases 9–14).
* **[Benchmark Results & Process](benchmarks/BENCHMARK.md)**: Kafka / Redpanda / AeroStream under strict container limits, with scripts, methodology and the Kafka-port investigation ([details](benchmarks/KAFKA_PORT_PERFORMANCE.md)).
* **[Python FastAPI Integration Example](examples/fastapi-app/README.md)**: Full-featured sample backend showcasing dual Kafka-wire and HTTP-REST streaming.
* **[Kubernetes Deployment Manifests](deploy/k8s/)**: Production-ready StatefulSet and Service definitions with preStop hooks.
* **[Helm Chart](deploy/helm/aerostream/README.md)**: Controller and broker StatefulSets with PVC-backed Raft state, rack awareness, and `helm test`.

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
