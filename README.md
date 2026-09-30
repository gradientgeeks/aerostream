# AeroStream

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-quay.io%2Fgradientgeeks%2Faerostream-blue?logo=docker&logoColor=white)](https://quay.io/repository/gradientgeeks/aerostream)
[![Go](https://img.shields.io/badge/Go-1.26_Control_Plane-00ADD8?logo=go&logoColor=white)](go-controller/)
[![Rust](https://img.shields.io/badge/Rust-1.98.1_Data_Plane-orange?logo=rust&logoColor=white)](rust-broker/)
[![Protocol](https://img.shields.io/badge/protocol-Kafka_100%25_Wire_Compatible-black?logo=apachekafka&logoColor=white)](docs/kafka-protocol.md)
[![UI](https://img.shields.io/badge/UI-Angular_21-DD0031?logo=angular&logoColor=white)](console/)
[![Docs](https://img.shields.io/badge/docs-MkDocs_Material-526CFE?logo=materialformkdocs&logoColor=white)](https://aerostream.gradientgeeks.com/docs/)
[![Status](https://img.shields.io/badge/status-preview-orange)](https://aerostream.gradientgeeks.com/)

**AeroStream** is an ultra-high-performance, distributed event-streaming and messaging engine engineered for extreme throughput, microsecond latencies, and modern multi-cloud workloads.

Built with a **Dual-Engine Architecture**—pairing a resilient **Go-based Raft control plane** with a zero-copy **Rust-based storage and networking data plane**—AeroStream delivers next-generation event streaming with 100% Kafka wire-protocol compatibility, built-in multi-cloud tiered storage, schema governance, stream transforms, and an integrated Web Console UI.

---

## ⚡ Benchmark Summary (OpenMessaging Benchmark)

AeroStream was measured with the vendor-neutral **[Linux Foundation OpenMessaging Benchmark (OMB)](https://github.com/openmessaging/benchmark)** framework through its Kafka wire port (`9092`), on an AWS `c6id.2xlarge` (8 vCPU, 16 GiB). One broker, 1 topic, 32 partitions, 1,024-byte messages, 8 producers, 8 consumers, `acks=1`, two rounds per workload:

| Offered load | Publish rate | Publish $p_{50}$ | $p_{99}$ | $p_{99.9}$ | End-to-end $p_{99}$ | Errors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **100,000 msg/s** (fixed) | 100,082 msg/s (97.7 MB/s) | 0.7 ms | 1.4 ms | 2.3 ms | 2.0 ms | 0 |
| **200,000 msg/s** (fixed) | 200,175 msg/s (195.5 MB/s) | 0.7 ms | 1.7 ms | 3.0 ms | 2.0 ms | 0 |
| **Maximum rate** | **271,350 msg/s** (265.0 MB/s) | 105 ms | 1,104 ms | 1,376 ms | 1,119 ms | 0 |

Latency stays under 2 ms at $p_{99}$ up to at least 200,000 msg/s; the saturation point is about 271,000 msg/s, where latency reflects queueing. The two rounds agree within 0.1% on throughput.

> 📊 **Explore Full Benchmark Reports & Reproduction**:
> * 📈 **[Website Benchmark Page](docs/benchmarks.md)** ([Online Portal](https://aerostream.gradientgeeks.com/docs/benchmarks/)): per-run results, CPU use, test environment and caveats.
> * 📑 **[OMB Benchmark Execution Guide](core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md)**: Step-by-step reproduction instructions using the official OpenMessaging Benchmark suite.
> * 🔬 **[Benchmark Report](benchmarks/BENCHMARK.md)**: EC2 results, resource-capped container runs, design notes and partition-density measurements.
> * ☁️ **[EC2 Benchmark Scripts](benchmarks/aws-ec2/README.md)**: one command creates the machine, runs OMB, copies the results back and destroys the machine.

---

## 🚀 Key Features

* **Shard-per-Core Zero-Contention Engine**:
  * Deterministic partition-to-shard mapping ($S = \text{hash}(\text{topic}, \text{partition}) \pmod N$) with thread-to-CPU affinity pinning via `libc::sched_setaffinity`.
  * Lock-free actor message passing via `flume::unbounded` channels—eliminating cross-core mutex locks, atomics, and thread migrations on hot produce/consume paths.
* **Extreme Memory & Storage Optimizations**:
  * In-place base offset patching directly on disk (`write_all_at`), completely bypassing multi-megabyte heap reallocations in $\mathcal{O}(1)$ time.
  * Paced page-cache writeback via Linux `sync_file_range(2)` and `posix_fadvise(2)` (pacing dirty flushes every 8 MiB), eliminating OS writeback stalls in memory-constrained containers.
  * Zero-copy cold tiering via hard links (`fs::hard_link`), decoupling hot partition log rollover from object store network latency.
* **100% Kafka Wire Protocol Compatibility**:
  * Native listener on port `9092` supporting 34+ Kafka API keys across produce, fetch, metadata, consumer groups, schemas, and ACLs.
  * High-performance Fetch long polling with lazy `tokio::sync::Notify` registration and lockless out-of-lock disk I/O.
  * Enterprise SASL authentication (`PLAIN` and `SCRAM-SHA-256`) and Two-Phase Commit (2PC) Transactions.
  * Drop-in compatibility with standard Kafka client ecosystems (`kafka-python`, `librdkafka`, `kafka-go`, `Confluent.Kafka`, Java / Spring Kafka).
* **Dual-Engine Decoupled Architecture**:
  * **Control Plane (Go)**: Distributed Raft consensus, automated partition leadership elections, dynamic cluster membership, 2-second broker heartbeats with dynamic config piggybacking, and gRPC coordination.
  * **Data Plane (Rust)**: Tokio async runtime, CPU core pinning, kernel zero-copy `sendfile(2)` socket transfers, and memory-mapped (`mmap`) offset indexing.
* **Multi-Cloud Tiered Storage**:
  * Hot partition segments on fast local NVMe/SSD.
  * Transparent, non-blocking background offload to **AWS S3 / MinIO**, **Google Cloud Storage (GCS)**, **Azure Blob Storage**, or network filesystem mounts.
* **Iceberg-Native Topics**:
  * Topics can write directly into Apache Iceberg tables (Parquet/Avro) for lakehouse-native analytics without a separate sink connector.
* **Share Groups (KIP-932 Queue Semantics)**:
  * Cooperative, queue-like consumption where multiple consumers acquire/acknowledge individual records from the same partition without exclusive assignment.
  * Per-record delivery-attempt limits, lock timeouts, and dead-letter-queue (DLQ) forwarding for records that exhaust retries.
* **Built-in Schema Registry**:
  * Confluent-compatible REST API on `/subjects`, `/schemas`, and `/compatibility`.
  * First-class support for **Avro**, **Protobuf**, and **JSON Schema** with `BACKWARD`, `FORWARD`, and `FULL` compatibility validation.
* **In-Broker Stream Processing & Transforms**:
  * Dynamic Stream Processing Engine (`/api/streams`), inline real-time filtering, PII data masking (`MASK_PII`), JSON schema transformation, and WASM runtime.
* **Enterprise Security & Granular RBAC**:
  * Role-based access control (`SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`).
  * Granular topic, consumer group, and cluster ACLs with prefix and wildcard pattern matching.
* **Modern Web Console UI**:
  * Sleek Angular management console with dark/light themes, dynamic cluster topology visualizer, live message inspector, real-time consumer lag monitoring (`/api/lag`), schema registry browser, and policy simulators.

---

## 🐳 Quick Start with Docker

Run the full AeroStream stack (Controller, Zero-Copy Broker, and Web Console) using the official container image:

```bash
docker run -d --name aerostream \
  -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
  -v aerostream_data:/data \
  quay.io/gradientgeeks/aerostream:latest
```

> 📖 **Deployment Quickstart Guides**:
> * 🐳 **[Docker Quickstart Guide](core/docs/DOCKER_QUICKSTART.md)** ([Web Guide](https://aerostream.gradientgeeks.com/docs/operations/#single-node-docker-deployment)): 30-second local setup with single all-in-one container, port mapping, and client samples.
> * ☸️ **[Kubernetes Quickstart Guide](core/docs/K8S_QUICKSTART.md)** ([Web Guide](https://aerostream.gradientgeeks.com/docs/operations/#production-kubernetes-statefulset)): Production deployment using standard `kubectl` manifests, headless services, StatefulSets, and automated zero-downtime draining.
> * ⎈ **[Helm Quickstart Guide](core/docs/HELM_QUICKSTART.md)** ([Web Guide](https://aerostream.gradientgeeks.com/docs/operations/#helm-chart-deployment)): Official Helm v3 chart installation, values customization, S3 tiered storage, and rack-aware zone placement.

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
> * ⚡ **[Shard-per-Core Architecture Whitepaper](core/docs/SHARD_PER_CORE.md)**: Thread-to-core pinning, lock-free actor messaging, paced writeback, and zero-allocation log append.
> * 🏛️ **[Distributed Systems Design Whitepaper](core/docs/DISTRIBUTED_SYSTEMS_DESIGN.md)**: In-depth engineering treatise on dual-engine mechanics, lock-free RCU, and hardware acceleration.
> * 📘 **[Architecture Web Portal](docs/architecture.md)** ([Online Docs](https://aerostream.gradientgeeks.com/docs/architecture/)): Interactive diagrams, Raft quorum consensus, and zero-copy pipeline details.
> * 🧩 **[Shard-per-Core Architecture Diagram](docs/images/shard_per_core_architecture.png)**: Visual guide to core pinning, lock-free channels, and memory pacing.
> * 🛰️ **[Controller-Broker Orchestration Diagram](docs/images/controller_broker_orchestration.png)**: Heartbeat piggybacking, Raft consensus, LEO reporting, and drain workflows.
> * 🚀 **[Zero-Copy Produce & Fetch Pipelines](docs/images/produce_fetch_pipeline.png)**: Step-by-step kernel DMA and mmap write paths.
> * ☁️ **[Multi-Cloud Tiered Storage Pipeline](docs/images/tiered_storage_pipeline.png)**: Non-blocking offloading and safe local eviction.
> * 🔄 **[Cluster Topology & Scale-Down Protocol](docs/images/cluster_topology_scale_down.png)**: 3-Node Raft consensus and graceful broker draining.

---

## 🛠 Local Development & Building from Source

### Prerequisites
* **Go**: 1.24 or higher
* **Rust**: 2021 or 2024 edition (Cargo & Rustc)
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
*Full .NET test suite located in [`examples/dotnet-app/`](examples/dotnet-app/).*

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

AeroStream provides dual documentation surfaces: interactive web documentation built with **MkDocs Material** (hosted at `https://aerostream.gradientgeeks.com/docs/`) and publication-grade technical whitepapers in `core/docs/`:

### 🌐 Interactive Web Documentation (`docs/`)
* **[Platform Overview & Quickstart](docs/index.md)** ([Web Portal](https://aerostream.gradientgeeks.com/docs/)): Architecture summary, 30-second Docker setup, and multi-language client examples.
* **[Dual-Engine Architecture Deep-Dive](docs/architecture.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/architecture/)): Raft quorum, Shard-per-Core storage, memory-mapped indexes, and hardware CRC32C.
* **[Apache Kafka Compatibility (Port 9092)](docs/kafka-protocol.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/kafka-protocol/)): Complete API key mapping (ApiKey 0–36), High Watermark semantics, and in-place base offset patching.
* **[Built-in Schema Registry](docs/schema-registry.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/schema-registry/)): Confluent REST compatibility, Avro/Protobuf/JSON Schema validation, and compatibility rules.
* **[In-Broker Stream Transforms](docs/transforms.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/transforms/)): Inline event routing, PII data masking, JSON filtering, and WASM runtime.
* **[Enterprise Security & RBAC](docs/security-rbac.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/security-rbac/)): Role-based access control, SASL authentication (`PLAIN`, `SCRAM`), and fine-grained ACLs.
* **[Multi-Cloud Tiered Storage](docs/tiered-storage.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/tiered-storage/)): Hot NVMe caching, transparent cloud offloading to S3/GCS/Azure, and historical replay.
* **[Cluster Operations & Lifecycle](docs/operations.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/operations/)): Production Kubernetes StatefulSets, automated broker draining, and scale-down procedures.
* **[Performance Benchmarks](docs/benchmarks.md)** ([Web Page](https://aerostream.gradientgeeks.com/docs/benchmarks/)): OpenMessaging Benchmark results, mathematical speedup derivations, and CPU efficiency charts.

### 🏛️ Core Technical Whitepapers (`core/docs/`)
* **[Shard-per-Core Architecture Whitepaper](core/docs/SHARD_PER_CORE.md)**: Hardware CPU core affinity, lock-free actor channels, paced writeback, and zero-allocation log append.
* **[Distributed Systems Engineering Treatise](core/docs/DISTRIBUTED_SYSTEMS_DESIGN.md)**: High-efficiency systems programming in Go and Rust, cache line mechanical sympathy, and 2PC WAL.
* **[Architecture Deep-Dive Specification](core/docs/ARCHITECTURE.md)**: Exhaustive 75 KB engineering specification of the dual-engine platform.
* **[REST & Wire Protocol API Reference](core/docs/API_REFERENCE.md)**: Exhaustive endpoint schemas, binary Kafka frame structures, and payload specifications.
* **[Features & Evolution Roadmap](core/docs/FEATURES_AND_ROADMAP.md)**: Feature status matrix, implemented capabilities (Phases 1–8) and the Next-Gen Enterprise Roadmap (Phases 9–14).
* **[OpenMessaging Benchmark Execution Guide](core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md)**: Official OMB benchmark compilation, driver configuration, and test execution runbook.
* **[Host & Multi-Payload Benchmark Report](benchmarks/BENCHMARK.md)**: Comprehensive host benchmarks across 100B, 1KB, 1MB, 10MB, and 50MB message sizes.
* **[Kafka Port Optimization Research](core/docs/KAFKA_PORT_OPTIMIZATION_RESEARCH.md)**: Low-level profiling, system call tracing, and optimization notes on Kafka protocol handling.
* **[Operator & Production Runbook](core/docs/OPERATOR_GUIDE.md)**: Bare-metal, Docker, and Kubernetes deployment runbook with systemd templates and sysctl tuning.

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
