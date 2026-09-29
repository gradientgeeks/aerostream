# AeroStream .NET (C#) Kafka Client & End-to-End Test Suite

Comprehensive .NET C# Kafka integration test runner verifying wire-protocol compatibility against AeroStream's native Kafka wire protocol listener (`127.0.0.1:9092`) using the official `Confluent.Kafka` client library.

---

## Technical Overview

- **Framework**: .NET 8.0 (`net8.0`)
- **Client Library**: `Confluent.Kafka` v2.6.0 (`librdkafka` v2.6.0 native engine)
- **Target Broker**: `127.0.0.1:9092` (AeroStream Native Wire Protocol)
- **Execution Environment**: Containerized execution inside `mcr.microsoft.com/dotnet/sdk:8.0-alpine` (no host .NET SDK required)

---

## Execution Guide

Run the complete E2E test suite inside the pre-cached .NET 8 Alpine Docker container:

```bash
docker run --rm --network host \
  -v /home/uttam/projects/AeroMQ/client/other_clients/dotnet_client:/app \
  -w /app \
  mcr.microsoft.com/dotnet/sdk:8.0-alpine \
  dotnet run -c Release
```

---

## Verification Coverage & Feature Matrix

| # | Test Suite | Protocol & Engine Features Validated | Status |
|---|---|---|---|
| **1** | **AdminClient Operations** | `CreateTopicsAsync` (3 partitions, replication 1), `GetMetadata` cluster inspection, broker endpoint discovery (`0.0.0.0:9092`), partition leader and ISR verification | **PASS** |
| **2** | **Producer Operations** | Codec matrix (`None`, `Gzip`, `Snappy`), custom record headers (`X-Trace-Id`, `X-Source`, `X-Codec`), Murmur2 key-based partition routing across 3 partitions, delivery report verification | **PASS** |
| **3** | **Consumer Group Operations** | Dynamic partition assignment callbacks (`SetPartitionsAssignedHandler`), balanced multi-partition consumption, manual offset commit (`Commit`), committed offset resumption verification | **PASS** |
| **4** | **Payload Checksum Validation** | 50 records across 5 distinct size tiers (128B, 1KB, 8KB, 32KB, 64KB totaling 1.05 MB), SHA-256 digest validation, 100% byte-for-byte data integrity | **PASS** |
| **5** | **Benchmark** | High-throughput batch produce of 2,000 messages (512 bytes each, Snappy compression) across 3 partitions, measuring message throughput (msg/s), data rate (MB/s), and latency percentiles | **PASS** |

---

## Benchmark Metrics

| Metric | Result |
|---|---|
| **Messages Produced** | 2,000 records |
| **Payload Size** | 512 bytes / message |
| **Batch Duration** | 26.56 ms |
| **Throughput (msg/s)** | **75,297 msg/s** |
| **Throughput (MB/s)** | **36.77 MB/s** |
| **P50 Latency** | 12.62 ms |
| **P90 Latency** | 14.57 ms |
| **P95 Latency** | 14.95 ms |
| **P99 Latency** | 15.50 ms |
| **Min / Mean / Max Latency** | 7.59 ms / 12.33 ms / 15.94 ms |
