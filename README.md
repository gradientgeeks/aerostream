# AeroStream

[![GitHub License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fgradientgeeks%2Faerostream-blue?logo=docker)](https://github.com/orgs/gradientgeeks/packages/container/package/aerostream)
[![Go Report Card](https://img.shields.io/badge/go-1.22-00ADD8?logo=go)](go-controller/)
[![Rust](https://img.shields.io/badge/rust-2021_edition-orange?logo=rust)](rust-broker/)
[![Architecture](https://img.shields.io/badge/arch-amd64%20%7C%20arm64-brightgreen)](#-quick-start-with-docker)

**AeroStream** is a high-performance, distributed event-streaming and messaging engine designed for extreme throughput, microsecond latencies, and modern multi-cloud workloads. 

Built with a **Dual-Engine Architecture**—pairing a resilient **Go-based Raft control plane** with a zero-copy **Rust-based storage and networking data plane**—AeroStream delivers next-generation event streaming with full Kafka wire-protocol compatibility, built-in tiered storage, schema governance, stream transforms, and an integrated Web Console UI.

---

## ⚡ Key Highlights & Benchmark Comparison

Single node, `--cpus=2.0 --memory=2g` per broker, median of 3 runs, produce path only, all containers on host networking.
AeroStream is shown on its **native data-plane port** (10 concurrent closed-loop producers); Kafka and Redpanda are driven by `kafka-producer-perf-test.sh`.

| Benchmark scenario | Apache Kafka 4.3.1 | Redpanda 26.2 | AeroStream (native port) |
| :--- | ---: | ---: | ---: |
| **50 MB messages (MB/s)** | 55.5 | 58.4 | **855.6** (runs 350-881) |
| **10 MB messages (MB/s)** | 170.8 | 218.7 | **592.4** (347-863) |
| **1 MB messages (MB/s)** | 320.9 | 375.4 | 378.6 (225-1,210) |
| **1 KB messages (msgs/s)** | 44,366 | 60,024 | **120,283** |
| **100 B messages (msgs/s)** | 141,243 | **171,527** | 121,852 |
| **Broker idle memory** | 303 MiB | 141 MiB | **1.4 MiB** |
| **Broker peak memory under load** | 1,454 MiB | 1,386 MiB | **354 MiB** |

AeroStream leads on large messages (native port) and on memory footprint. On its **Kafka port** (what Kafka clients use) it now reaches 174,520 msgs/s at 100 B and 63,776 at 1 KB
(Kafka 141,243 / 44,366; Redpanda 171,527 / 60,024; it was 18,925 / 5,116 before profiling-driven fixes), but is still behind Kafka and Redpanda at 1-50 MB (about 200 / 144 / 29 MB/s). Kafka and AeroStream acknowledge
from the OS page cache while Redpanda flushes before acknowledging by default. Read the caveats in [BENCHMARK.md](benchmarks/BENCHMARK.md) (methodology, closed-loop native client, durability differences, run-to-run noise)
and [KAFKA_PORT_PERFORMANCE.md](benchmarks/KAFKA_PORT_PERFORMANCE.md) before drawing conclusions.

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
docker run -d \
  --name aerostream \
  -p 9091:9091 \
  -p 9092:9092 \
  -p 9001:9001 \
  -p 8001:8001 \
  -p 7001:7001 \
  -v aerostream_data:/data \
  ghcr.io/gradientgeeks/aerostream:latest
```

### Accessing Endpoints:
* **Web Console UI**: [http://localhost:9001/aerostream/console](http://localhost:9001/aerostream/console)
* **Kafka Wire Protocol**: `localhost:9092`
* **Native TCP Data Plane**: `localhost:9091`
* **REST Management API & Schema Registry**: `http://localhost:9001`
* **gRPC Control Plane**: `localhost:8001`

---

## 🏗 Architecture Overview

```
                      +---------------------------------------+
                      |         Clients & Applications        |
                      |  (Kafka Clients, Native CLI, Web UI)  |
                      +-------------------+-------------------+
                                          |
                      +-------------------+-------------------+
                      |      AeroStream Unified Ingress       |
                      +-------------------+-------------------+
                               |                     |
             (Kafka Protocol / TCP)        (Metadata / REST / gRPC)
             Port 9092 & 9091              Port 9001 & 8001
                               |                     |
                               v                     v
              +--------------------------------+   +-------------------------------+
              |        Rust Data Plane         |   |       Go Control Plane        |
              |       (Storage Engine)         |   |       (Cluster Manager)       |
              +--------------------------------+   +-------------------------------+
              | * Tokio Async I/O (sendfile)   |   | * Raft Consensus Quorum       |
              | * Memory-Mapped Indices (mmap) |   | * Confluent Schema Registry   |
              | * Compaction & Key Dedup       |   | * RBAC & Granular ACL Engine  |
              | * Idempotence Tracker (EOS)    |   | * Cooperative Sticky Rebalance|
              | * Tiered Storage Offloader     |   | * Stream Transforms (WASM/PII)|
              +----------------+---------------+   +---------------+---------------+
                               |                                   |
                               +-----------------+-----------------+
                                                 |
                                                 v
                               +-----------------------------------+
                               |     Tiered Multi-Cloud Storage    |
                               | (Local Disk | S3 | GCS | Azure)   |
                               +-----------------------------------+
```

---

## 🛠 Local Development & Building from Source

### Prerequisites
* **Go**: 1.22 or higher
* **Rust**: 1.75+ / 2021 edition (Cargo & Rustc)
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

## 📜 Documentation

* [Benchmark results and process](benchmarks/BENCHMARK.md)
* [Feature Comparison & Architectural Roadmap](docs/FEATURE_COMPARISON_AND_ROADMAP.md)
* [Kubernetes Deployment Guide](deploy/k8s/)

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
