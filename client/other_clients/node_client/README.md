# AeroStream Node.js KafkaJS Client & End-to-End Test Suite

Comprehensive KafkaJS integration test runner verifying wire-protocol compatibility against the AeroStream broker (`127.0.0.1:9092`).

## Requirements
- Node.js >= 18 (Tested on v24.17.0)
- npm >= 9 (Tested on v11.13.0)
- AeroStream Broker running at `127.0.0.1:9092`

## Installation
```bash
npm install
```

## Running Tests
Run the complete E2E test suite:
```bash
npm test
# or
node kafkajs_runner.js
```

## Verification Coverage & Feature Matrix

| # | Test Area | Protocol Features Validated | Status |
|---|---|---|---|
| 1 | **AdminClient Operations** | `admin.connect()`, `admin.createTopics()`, `admin.listTopics()`, `admin.fetchTopicMetadata()`, dynamic leader/ISR inspection | **PASS** |
| 2 | **Producer Batches** | Keys, partition routing across 3 partitions, custom headers (`traceId`, `source: kafkajs`), uncompressed, GZIP compression, Snappy compression | **PASS** |
| 3 | **Consumer Group Operations** | `consumer.connect()`, `consumer.subscribe()`, dynamic partition assignment rebalancing, `eachMessage` handler, header decoding, manual offset commit (`commitOffsets`), `admin.fetchOffsets()` validation | **PASS** |
| 4 | **Transactional Producer & Isolation** | `producer.transaction()`, transactional `send()`, `abort()`, `commit()`, `read_committed` consumer isolation (aborts filtered), `read_uncommitted` consumer isolation (all seen) | **PASS** |
| 5 | **Payload Checksum Verification** | 60+ variable payloads (256B, 4KB, 16KB, 64KB), SHA-256 byte-for-byte integrity check, high-throughput batching, zero data corruption | **PASS** |
