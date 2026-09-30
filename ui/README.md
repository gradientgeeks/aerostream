# AeroStream Web Console

[![GitHub Repository](https://img.shields.io/badge/GitHub-gradientgeeks%2Faerostream-blue?logo=github)](https://github.com/gradientgeeks/aerostream)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Enterprise Web Administration Console for **AeroStream** ([gradientgeeks/aerostream](https://github.com/gradientgeeks/aerostream)) — the high-performance, distributed event streaming platform built with a dual-engine architecture:
- **Go**: Raft metadata consensus, controller coordination, Kafka protocol negotiation, and HTTP management APIs.
- **Rust**: Zero-copy partition commit log, lockless I/O ring buffers, and high-throughput TCP socket server.

---

## Features

- **Cluster Overview**: Live node topology, Raft consensus state (Leader/Follower/Candidate), broker heartbeat telemetry, and log storage volume.
- **Topics & Partition Management**: Topic CRUD, partition distributions, ISR health, high-watermark metrics, and cleanup policies (`compact` vs `delete`).
- **Message Explorer**: Real-time partition log viewer with offset seeking, timestamp decoding, hex dump preview, and structured JSON payloads.
- **Web Producer**: Interactive message publishing console with single or burst event simulation and latency tracking.
- **Schema Registry**: Full governance with Avro, JSON Schema, and Protobuf contracts, version evolution, and compatibility verification.
- **Stream Transforms**: In-broker WASM filters, PII data masking, and JSON enrichment pipelines.
- **Connectors Ecosystem**: Native Kafka Connect interface for S3 archival sinks, HTTP webhooks, and database CDC sources.
- **Consumer Groups & Lag Monitor**: KIP-848 cooperative sticky rebalancing status, active member partition allocations, and committed lag telemetry.
- **Security & RBAC ACLs**: Role-based access control policies (Allow/Deny) and live policy evaluation simulator.

---

## Development Server

To start a local development server on port 4200:

```bash
npm install
npm run start
```

Navigate to `http://localhost:4200/`. The console connects to the AeroStream Controller REST API at `http://localhost:9001` by default.

## Production Build

To build the production-ready distribution:

```bash
npm run build
```

Compiled assets will be output to `dist/ui/browser/`.

---

## Repository & Community

- **Project Repository**: [https://github.com/gradientgeeks/aerostream](https://github.com/gradientgeeks/aerostream)
- **Issue Tracker**: [https://github.com/gradientgeeks/aerostream/issues](https://github.com/gradientgeeks/aerostream/issues)
- **Organization**: [Gradient Geeks](https://github.com/gradientgeeks)
