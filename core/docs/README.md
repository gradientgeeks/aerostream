# AeroStream Documentation Portal

Welcome to the **AeroStream** documentation suite. AeroStream is a next-generation distributed event-streaming engine built on a **Dual-Engine Architecture**—pairing a resilient **Go 1.26 Raft control plane** with a zero-copy **Rust 1.98.1 (Edition 2024) storage and networking data plane**, accompanied by an **Angular 21 Web Console UI**.

This directory serves as the centralized technical knowledge base for application developers, system architects, and platform operators.

---

## 🗺️ Documentation Directory & Reading Paths

Choose your path based on your role:

| Role | Focus Area | Recommended Reading Path |
|:---|:---|:---|
| **System Architects & Evaluators** | Engine architecture, mechanical sympathy, Raft consensus, benchmarking | • [Architecture Deep-Dive](ARCHITECTURE.md)<br/>• [Shard-per-Core Whitepaper](SHARD_PER_CORE.md)<br/>• [Systems Programming Treatise](DISTRIBUTED_SYSTEMS_DESIGN.md)<br/>• [Strategic Roadmap](FEATURES_AND_ROADMAP.md)<br/>• [OMB Benchmarks](OPENMESSAGING_BENCHMARK_GUIDE.md) |
| **Application Developers & Integrators** | Client SDKs, native framing, Kafka compatibility, Schema Registry | • [Official Client SDKs (`aerostream-sdk`)](../../sdks/README.md)<br/>• [REST & Wire API Reference](API_REFERENCE.md)<br/>• [Python & Kafka Quickstart](PYTHON_KAFKA_CLIENT_GUIDE.md)<br/>• [Kafka Port Optimization Research](KAFKA_PORT_OPTIMIZATION_RESEARCH.md) |
| **Platform Operators & SRE Teams** | Production Kubernetes deployment, zero-downtime draining, Linux kernel tuning | • [Operator & Deployment Guide](OPERATOR_GUIDE.md)<br/>• [Kubernetes Quickstart](K8S_QUICKSTART.md)<br/>• [Broker Scale-Down & Partition Drain](DRAIN_AND_SCALE_DOWN.md) |

### 1. 🏛️ System Architects & Technical Evaluators
* **[Shard-per-Core Architecture Whitepaper](SHARD_PER_CORE.md)**: Technical whitepaper on AeroStream's Shared-Nothing Thread-per-Core architecture, hardware core pinning (`libc::sched_setaffinity`), lock-free actor model via `flume` channels, in-place base offset patching, paced page-cache writeback (`sync_file_range`), and NUMA affinity.
* **[Distributed Systems Engineering & High-Efficiency Systems Programming Guide](DISTRIBUTED_SYSTEMS_DESIGN.md)**: Publication-grade engineering treatise covering dual-engine separation, mechanical sympathy (L1/L2/L3 cache lines, false sharing, page faults), Go 1.26 runtime patterns (Green Tea GC, Swiss Tables SIMD, `sync.Pool`, Raft FSM snapshotting, lock-free RCU registry), Rust 1.98.1 Edition 2024 high-performance patterns (`hashbrown::Equivalent`, scoped locking, `write_all_at`, hardware SSE4.2 CRC32C, 2PC WAL), and 4-way container benchmark analysis.
* **[Architecture Deep-Dive](ARCHITECTURE.md)**: Exhaustive technical analysis of the dual-engine design, HashiCorp Raft consensus, Rust zero-copy `sendfile(2)` kernel dispatch, memory-mapped (`mmap`) append-only logs, Shard-per-Core engine, and controller-broker orchestration.
* **[Features & Evolution Roadmap](FEATURES_AND_ROADMAP.md)**: Feature status matrix, implemented capabilities (Phases 1–8), the Next-Generation Enterprise Horizon (Phases 9–14), and official client SDKs.
* **[Benchmark Results & Process](../../benchmarks/BENCHMARK.md)**: AeroStream's OpenMessaging Benchmark results on AWS EC2 and in resource-capped containers (271,350 msg/s at the maximum rate; publish $p_{99}$ of 1.4 ms at 100,000 msg/s), with scripts, methodology, and caveats.
* **[OpenMessaging Benchmark (OMB) Execution Guide](OPENMESSAGING_BENCHMARK_GUIDE.md)**: Step-by-step procedure for compiling, configuring, and running the official Linux Foundation OpenMessaging Benchmark against AeroStream's Kafka port (32 partitions, 1 KB payloads, sub-2ms $p_{99}$ tail latency).

### 2. 🚀 Application Developers & Integrators
* **[Official Client SDKs (`github.com/gradientgeeks/aerostream-sdk`)](../../sdks/README.md)**:
  * 🦫 **Go**: [`github.com/gradientgeeks/aerostream-sdk/go`](../../sdks/go/) (v0.1.0-preview)
  * 🦀 **Rust**: [`aerostream-client`](../../sdks/rust/) (v0.1.0-preview)
  * ☕ **Java**: [`org.gradientgeeks.aerostream:aerostream-client`](../../sdks/java/) (v0.1.0-preview)
  * 🔷 **.NET (C#)**: [`GradientGeeks.AeroStream.Client`](../../sdks/dotnet/) (v0.1.0-preview)
  * 🟩 **Node.js / TypeScript**: [`@gradientgeeks/aerostream-client`](../../sdks/nodejs/) (v0.1.0-preview)
* **[Docker Quickstart Guide](DOCKER_QUICKSTART.md)**: 30-second local setup with single all-in-one container (`quay.io/gradientgeeks/aerostream:latest`), port mapping, and client samples.
* **[Kubernetes Quickstart Guide](K8S_QUICKSTART.md)**: Production deployment using standard `kubectl` manifests, headless services, StatefulSets, and automated zero-downtime draining.
* **[Helm Quickstart Guide](HELM_QUICKSTART.md)**: Official Helm v3 chart installation, values customization, S3 tiered storage, and rack-aware zone placement.
* **[REST & Wire Protocol API Reference](API_REFERENCE.md)**: Complete HTTP REST API schemas, request/response payloads, and binary Kafka Wire Protocol frame structures (`Produce`, `Fetch`, `Metadata`, `ApiVersions`, `InitProducerId`).
* **[Python FastAPI Microservice Example](../examples/fastapi-app/README.md)**: Production-ready sample backend demonstrating asynchronous dual-protocol streaming (Kafka wire framing over TCP + HTTP REST) and background consumer workers.
* **Schema Governance & Built-in Registry**: Confluent-compatible schema evolution for Avro, Protobuf, and JSON Schema contracts (see [API Reference](API_REFERENCE.md#5-schema-registry-api)).
* **In-Broker Stream Transforms**: Inline WASM, PII data masking, and real-time JSON filtering (see [API Reference](API_REFERENCE.md#6-in-broker-stream-transforms-api)).

### 3. 🛠️ Platform Engineers & DevOps (SRE)
* **[Operator & Deployment Guide](OPERATOR_GUIDE.md)**: Production deployment on bare-metal / VMs, systemd unit templates, kernel tuning (`sysctl`), and multi-node cluster configuration.
* **[Kubernetes StatefulSets Manifests](../deploy/k8s/)** & **[K8s Quickstart](K8S_QUICKSTART.md)**: Helm-free, production-ready StatefulSet manifests with headless services, persistent volume claims, and automated `preStop` scale-down hooks.
* **[Helm Chart](../deploy/helm/aerostream/)** & **[Helm Quickstart](HELM_QUICKSTART.md)**: Multi-replica HA deployments with automated testing and value profiles.
* **Cluster Lifecycle & Zero-Downtime Operations**: Step-by-step procedures for bootstrapping, graceful broker partition draining (`POST /api/brokers/{id}/drain`), and Raft consensus node removal (`/leave`) (see [Operator Guide](OPERATOR_GUIDE.md#3-cluster-lifecycle-management)).
* **[Troubleshooting & Operational Diagnostics](OPERATOR_GUIDE.md#6-troubleshooting--operational-diagnostics)**: Common error signatures, dual-listener port routing, and socket connection isolation.

---

## 🖼️ Architecture & Dataflow Gallery

All architecture diagrams are rendered in high-resolution (3200 × 2160) using clean Excalidraw-style typography:

### 1. Dual-Engine Core Architecture
Visualizes the decoupled Go 1.26 control plane and zero-copy Rust 1.98.1 storage engine with unified client ingress.

![Dual Engine Core Architecture](images/dual_engine_architecture.png)

---

### 2. Zero-Copy Produce & Fetch Pipeline
Shows how write requests bypass heap allocations via memory-mapped buffers and in-place base offset patching, while consumer fetch requests leverage Linux `sendfile(2)` for direct pagecache-to-socket DMA transfers.

![Produce and Fetch Zero-Copy Pipeline](images/produce_fetch_pipeline.png)

---

### 3. Multi-Cloud Tiered Storage Pipeline
Details the lifecycle of partition logs from fast local NVMe segments through automated 128 MB rolling to asynchronous multi-cloud offloading (AWS S3, MinIO, GCS, Azure Blob, Local NFS).

![Multi-Cloud Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

---

### 4. Cluster Topology, Quorum & Graceful Scale-Down
Illustrates the 3-node Raft controller quorum, broker replication sets, and automated zero-downtime broker draining.

![Cluster Topology and Scale Down](images/cluster_topology_scale_down.png)

---

### 5. Shard-per-Core Architecture
Visualizes the hardware core pinning (`libc::sched_setaffinity`), lock-free actor model via `flume` channels, zero-contention PartitionLog maps, in-place base offset patching, and paced page-cache writeback.

![Shard-per-Core Architecture](images/shard_per_core_architecture.png)

---

### 6. Controller-Broker Control Plane Orchestration
Shows the bidirectional gRPC control stream between Go Controller and Rust Broker, 2s heartbeats with LEO progress, piggybacked dynamic configs (`client_quotas`, `topic_compression`), 8s failure detection, and graceful broker draining.

![Controller-Broker Orchestration](images/controller_broker_orchestration.png)

---

### 7. Dual-Protocol Engine: Native vs. Kafka Wire Protocol
Compares the ultra-compact 7-byte native frame (`[0xAE, 0x01][cmd: u8][body_len: u32 BE]`) on port 9091 with standard Kafka RequestHeader v2 framing on port 9092, and showcases the 5 official client SDKs (Go, Rust, Java, .NET, Node.js).

![Native and Kafka Dual-Protocol Engine](images/native_and_kafka_dual_protocol.png)

---

## ⚡ Default Ports & Protocol Cheat-Sheet

| Port | Protocol / Transport | Component | Purpose |
|---|---|---|---|
| **`9001`** | `HTTP / REST` | Go Controller | Angular 21 Web Console UI (`/aerostream/console`), REST Management API, Schema Registry, Stream Transforms, and ACLs |
| **`8001`** | `gRPC` | Go Controller | Internal cluster metadata synchronization and broker heartbeat registration |
| **`7001`** | `TCP (Raft)` | Go Controller | HashiCorp Raft consensus quorum communication between controllers |
| **`9092`** | `TCP (Kafka Wire)` | Rust Broker | Standard Kafka binary wire protocol listener (supports `kafka-clients`, `librdkafka`, `confluent-kafka`) |
| **`9091`** | `TCP (Native)` | Rust Broker | Native binary zero-copy stream protocol (`0xAE 0x01` framing, `aerostream-sdk`) |

---

## 📚 Complete Document Catalog

| Document | Description |
|---|---|
| **[`README.md`](../README.md)** | Root repository overview, quickstart, Docker run instructions, and benchmark highlights |
| **[`docs/README.md`](README.md)** | *This document* — Central documentation portal and index |
| **[`docs/SHARD_PER_CORE.md`](SHARD_PER_CORE.md)** | Complete technical whitepaper on the Shard-per-Core architecture & I/O optimizations |
| **[`docs/ARCHITECTURE.md`](ARCHITECTURE.md)** | Comprehensive technical architecture deep-dive (75+ KB) |
| **[`docs/DISTRIBUTED_SYSTEMS_DESIGN.md`](DISTRIBUTED_SYSTEMS_DESIGN.md)** | Engineering treatise on systems programming in Go 1.26 and Rust 1.98.1 |
| **[`docs/OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md)** | Bare-metal, Docker, and Kubernetes deployment & operational runbook |
| **[`docs/API_REFERENCE.md`](API_REFERENCE.md)** | Complete REST API schemas and Kafka wire protocol specification |
| **[`docs/FEATURES_AND_ROADMAP.md`](FEATURES_AND_ROADMAP.md)** | Feature status matrix, client SDKs, & Next-Gen Enterprise Roadmap |
| **[`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md)** | Host and container performance benchmarks across 100B, 1KB, 1MB, 10MB, and 50MB messages |
| **[`benchmarks/KAFKA_PORT_PERFORMANCE.md`](../../benchmarks/KAFKA_PORT_PERFORMANCE.md)** | Kafka-port profiling, fixes and sources; isolated 3-way container benchmark data tables |
| **[`examples/fastapi-app/README.md`](../examples/fastapi-app/README.md)** | Python FastAPI microservice integration guide and automated test suite |
| **[`docs/DOCKER_QUICKSTART.md`](DOCKER_QUICKSTART.md)** | Developer Docker container guide with multi-language code snippets and Compose |
| **[`deploy/k8s/`](../deploy/k8s/)** | Kubernetes StatefulSet manifests and headless service configurations |
