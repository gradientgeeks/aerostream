# AeroStream

[![GitHub License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-quay.io%2Fgradientgeeks%2Faerostream-blue?logo=docker)](https://quay.io/repository/gradientgeeks/aerostream)
[![Go Report Card](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](go-controller/)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange?logo=rust)](rust-broker/)
[![Architecture](https://img.shields.io/badge/arch-amd64%20%7C%20arm64-brightgreen)](#-quick-start-with-docker)

**AeroStream** is a high-performance, distributed event-streaming and messaging engine designed for extreme throughput, microsecond latencies, and modern multi-cloud workloads. 

Built with a **Dual-Engine Architecture**—pairing a resilient **Go-based Raft control plane** with a zero-copy **Rust-based storage and networking data plane**—AeroStream delivers next-generation event streaming with full Kafka wire-protocol compatibility, built-in tiered storage, schema governance, stream transforms, and an integrated Web Console UI.

---

## ⚡ Key Highlights & Benchmark Comparison

Single node, `--cpus=2.0 --memory=2g` per broker, **median of 3 runs**, produce path only, host networking, message sizes from 1 KB to 10 MB (min-max of the 3 runs in brackets).
Kafka, Redpanda and AeroStream's Kafka port are driven by `kafka-producer-perf-test`; AeroStream's native port by its own client (10 closed-loop producers). Values in MB/s.

| Size | Kafka | Redpanda | AeroStream native | AeroStream Kafka port |
| :--- | ---: | ---: | ---: | ---: |
| 1KB | 42.6 (26-50) | 57.2 (56-73) | **168.1** (166-169) | 67.7 (65-85) |
| 10KB | 91.1 (75-112) | 145.2 (92-178) | **718.9** (684-803) | 160.5 (158-166) |
| 50KB | 228.4 (214-237) | 257.4 (224-276) | **949.1** (904-967) | 215.5 (211-224) |
| 100KB | 202.1 (200-278) | 331.7 (325-357) | **1010.9** (987-1073) | 246.0 (136-294) |
| 250KB | 239.0 (215-246) | 367.7 (259-377) | **1041.0** (1010-1071) | 380.6 (341-381) |
| 500KB | 379.4 (225-394) | 341.0 (322-400) | **1027.8** (1000-1106) | 380.0 (191-400) |
| 1MB | **382.9** (229-403) | 364.4 (239-386) | 283.7 (267-301) | 346.3 (305-377) |
| 10MB | 232.7 (176-244) | **295.5** (289-302) | 286.0 (282-297) | 203.8 (94-229) |

From 10 KB to 500 KB AeroStream's native port sustains 0.7-1.0 GB/s (3-5x Kafka and Redpanda); its Kafka port leads Kafka up to 250 KB and is behind at 1 MB and 10 MB.
Kafka and AeroStream acknowledge from the OS page cache while Redpanda flushes before acknowledging by default. The broker container idles at about 1.3 MiB (Kafka ~377 MiB, Redpanda ~271 MiB).
Methodology, caveats and the write-path fix behind these numbers: [BENCHMARK.md](benchmarks/BENCHMARK.md). For official Linux Foundation OpenMessaging Benchmark (OMB) methodology and results, see [OpenMessaging Benchmark Guide](docs/OPENMESSAGING_BENCHMARK_GUIDE.md).

---

## 🚀 Key Features

* **Kafka Wire Protocol Compatibility**:
  * Native listener on port `9092` supporting standard Kafka APIs (`ApiVersions`, `Metadata`, `Produce`, `Fetch`, `InitProducerId`).
  * Drop-in compatibility with existing Kafka client libraries (`kafka-python`, `librdkafka`, `kafka-go`, Java / Spring Kafka).
* **Dual-Engine Zero-Copy Architecture**:
  * **Control Plane (Go)**: Distributed Raft consensus, partition leadership elections, dynamic cluster membership, and gRPC coordination.
  * **Data Plane (Rust)**: Tokio async runtime with CPU affinity, kernel `sendfile(2)` zero-copy socket transfers, and memory-mapped (`mmap`) offset indexing.
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
* **In-Broker Stream Transforms & WASM Engine**:
  * Inline real-time filtering, PII data masking (`MASK_PII`), JSON schema transformation, and WASM runtime.
* **Enterprise Security & Granular RBAC**:
  * Role-based access control (`SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`).
  * Granular topic, consumer group, and cluster ACLs with prefix and wildcard pattern matching.
* **Cooperative Sticky Rebalance Protocol**:
  * KIP-848 style non-blocking cooperative rebalancing avoiding stop-the-world partition revocations.
* **Connectors Ecosystem**:
  * Kafka Connect compatible management API with native connectors: S3 Archival Sink, Webhook REST Sink, Database CDC Source, and Elasticsearch Sink.
* **Modern Web Console UI**:
  * Sleek Angular management console with dark/light themes, cluster topology visualizer, live message inspector, consumer group lag monitor, schema registry browser, and policy simulators.

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

AeroStream achieves its performance through strict decoupling:
* **Go Control Plane (Ports 9001 & 8001)**: Drives distributed consensus via HashiCorp Raft, serves the Confluent-compatible Schema Registry, manages enterprise RBAC / ACL policies, stream transforms, and connector runtimes.
* **Rust Data Plane (Ports 9091 & 9092)**: Handles high-throughput binary Kafka wire traffic and native streaming via Tokio async event loops, memory-mapped (`mmap`) sparse index search, and Linux kernel zero-copy `sendfile(2)` DMA transfers.
* **Multi-Cloud Tiered Storage**: Automatically rolls sealed 128 MB log segments into an asynchronous offloader queue, persisting them to AWS S3, MinIO, Google Cloud Storage, or Azure Blob without blocking producer ingestion.

> 📖 **Deep-Dive Diagrams**:
> * **[Zero-Copy Produce & Fetch Pipelines](docs/images/produce_fetch_pipeline.png)**: Step-by-step kernel DMA and mmap write paths.
> * **[Multi-Cloud Tiered Storage Pipeline](docs/images/tiered_storage_pipeline.png)**: Non-blocking offloading and safe local eviction.
> * **[Cluster Topology & Scale-Down Protocol](docs/images/cluster_topology_scale_down.png)**: 3-Node Raft consensus and graceful broker draining.


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
* **[Operator & Deployment Guide](docs/OPERATOR_GUIDE.md)**: Cluster bootstrapping, Kubernetes StatefulSets, automated scale-down, broker draining, and monitoring.
* **[REST & Wire Protocol API Reference](docs/API_REFERENCE.md)**: Complete endpoint schemas, Schema Registry, Stream Transforms, ACLs, and Kafka wire framing.
* **[Feature Comparison & Evolution Roadmap](docs/FEATURE_COMPARISON_AND_ROADMAP.md)**: Detailed breakdown vs. Apache Kafka and Redpanda, and Next-Gen Enterprise Roadmap (Phases 9–14).
* **[Benchmark Results & Process](benchmarks/BENCHMARK.md)**: Kafka / Redpanda / AeroStream under strict container limits, with scripts, methodology and the Kafka-port investigation ([details](benchmarks/KAFKA_PORT_PERFORMANCE.md)).
* **[Python FastAPI Integration Example](examples/fastapi-app/README.md)**: Full-featured sample backend showcasing dual Kafka-wire and HTTP-REST streaming.
* **[Kubernetes Deployment Manifests](deploy/k8s/)**: Production-ready StatefulSet and Service definitions with preStop hooks.
* **[Helm Chart](deploy/helm/aerostream/README.md)**: Controller and broker StatefulSets with PVC-backed Raft state, rack awareness, and `helm test`.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
