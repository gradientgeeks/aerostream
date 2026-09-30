# AeroStream Architecture Diagrams & Vector Assets

This directory contains the official architecture diagrams and Excalidraw vector source files for the **AeroStream Distributed Streaming Platform**.

## 🎨 Diagram Directory Index

| # | Diagram Identifier | Formats Available | Description |
|---|---|---|---|
| **1** | [`dual_engine_architecture`](dual_engine_architecture.svg) | [`.excalidraw`](dual_engine_architecture.excalidraw) • [`.svg`](dual_engine_architecture.svg) • [`.png`](../images/dual_engine_architecture.png) | Decoupled Go 1.26 Control Plane (Raft 7001, gRPC 8001, REST 9001, BoltDB, Schema Registry, RBAC Swiss Tables) and Rust 1.98.1 Data Plane (Kafka 9092, Native 9091, Shard-per-Core storage, mmap append, sendfile DMA). |
| **2** | [`shard_per_core_architecture`](shard_per_core_architecture.svg) | [`.excalidraw`](shard_per_core_architecture.excalidraw) • [`.svg`](shard_per_core_architecture.svg) • [`.png`](../images/shard_per_core_architecture.png) | Physical CPU socket & NUMA nodes, core pinning (`libc::sched_setaffinity`), lock-free actor model via `flume` bounded channels, deterministic partition routing `S = (Hash64(topic) ^ partition) % N_shards`, zero-contention PartitionLog maps, in-place base offset patching, paced writeback (`sync_file_range`). |
| **3** | [`produce_fetch_pipeline`](produce_fetch_pipeline.svg) | [`.excalidraw`](produce_fetch_pipeline.excalidraw) • [`.svg`](produce_fetch_pipeline.svg) • [`.png`](../images/produce_fetch_pipeline.png) | Write pipeline (Ingress -> CRC32C SSE4.2 -> in-place patching -> mmap append -> paced writeback -> High Watermark) & Read pipeline (Consumer fetch -> sparse index binary search -> segment lookup -> Linux `sendfile(2)` zero-copy kernel DMA transfer). |
| **4** | [`controller_broker_orchestration`](controller_broker_orchestration.svg) | [`.excalidraw`](controller_broker_orchestration.excalidraw) • [`.svg`](controller_broker_orchestration.svg) • [`.png`](../images/controller_broker_orchestration.png) | Bidirectional gRPC stream on port 8001, 2s periodic heartbeats with LEO and lag telemetry, piggybacked dynamic configs (quotas, compression), 8s lease expiry, automated ISR updates, and partition leader re-election. |
| **5** | [`cluster_topology_scale_down`](cluster_topology_scale_down.svg) | [`.excalidraw`](cluster_topology_scale_down.excalidraw) • [`.svg`](cluster_topology_scale_down.svg) • [`.png`](../images/cluster_topology_scale_down.png) | 3-node Raft controller quorum (port 7001), 3+ Rust brokers with ISR sets, graceful broker scale-down and automated partition draining (`POST /api/brokers/{id}/drain`), and zero message loss handoff. |
| **6** | [`tiered_storage_pipeline`](tiered_storage_pipeline.svg) | [`.excalidraw`](tiered_storage_pipeline.excalidraw) • [`.svg`](tiered_storage_pipeline.svg) • [`.png`](../images/tiered_storage_pipeline.png) | Active hot segment on local NVMe -> sealed segment roll -> async cold segment freeze and compression -> multi-cloud tiered offload (AWS S3, MinIO, GCS, Azure Blob) -> transparent remote read fetch and local disk cache eviction. |
| **7** | [`native_and_kafka_dual_protocol`](native_and_kafka_dual_protocol.svg) | [`.excalidraw`](native_and_kafka_dual_protocol.excalidraw) • [`.svg`](native_and_kafka_dual_protocol.svg) • [`.png`](../images/native_and_kafka_dual_protocol.png) | Side-by-side protocol comparison showing 7-byte native frame (`[0xAE, 0x01][cmd: u8][body_len: u32 BE]`) on port 9091 vs Kafka RequestHeader v2 framing on port 9092, and the 5 official client SDKs (Go, Rust, Java, .NET, Node.js). |

---

## 🛠️ Regenerating Diagrams

To regenerate all Excalidraw JSON source files, SVG vector diagrams, and rendered high-resolution 3200 × 2160 PNGs:

```bash
python3 scripts/generate_architecture_diagrams.py
```

### Technical Specifications
* **Schema**: Valid Excalidraw v2 JSON schema (`https://excalidraw.com`).
* **Canvas Resolution**: 3200 × 2160 pixels (Ultra-High-Definition 16:9).
* **Color Palette**: Dark Slate (`#0b0f19`, `#0f172a`), Cyan (`#38bdf8`), Emerald (`#34d399`), Amber (`#fbbf24`), Purple (`#a855f7`), Rose (`#fb7185`).
* **Vector Renderer**: Direct SVG export + `rsvg-convert` with anti-aliasing.
