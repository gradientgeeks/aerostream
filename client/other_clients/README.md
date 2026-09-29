# AeroStream Multi-Client Kafka Wire Protocol Verification Suite

This directory contains complete end-to-end verification suites for alternative Kafka client libraries running against **AeroStream's** native Kafka wire protocol listener (port `9092`).

---

## Client Ecosystem Coverage

| Client Library | Language | Version | Protocol Features Tested | Test Runner | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **KafkaJS** | Node.js | `v2.2.4` | AdminClient, RoundRobin Consumer Group, RecordBatch v2 Headers, GZIP/Snappy, Manual Offset Commits, Transactional EOS (`read_committed` vs `read_uncommitted`), SHA-256 Checksums | [`node_client/kafkajs_runner.js`](file:///home/uttam/projects/AeroMQ/client/other_clients/node_client/kafkajs_runner.js) | **PASSED (5/5)** |
| **IBM/sarama** | Go | `v1.43+` | Metadata Discovery, Sync/Async Producer, Multi-codec Compression (`none`, `gzip`, `snappy`, `lz4`, `zstd`), Header Arrays, 2-Member Dynamic Group Rebalance, High-Throughput (138k msg/s) | [`go_clients/cmd/sarama_runner/main.go`](file:///home/uttam/projects/AeroMQ/client/other_clients/go_clients/cmd/sarama_runner/main.go) | **PASSED (5/5)** |
| **twmb/franz-go** | Go | `v1.17+` | Pure-Go `kgo.Client`, `kadm` Admin, RecordBatch v2 Headers, 4 Codecs (`snappy`, `gzip`, `lz4`, `zstd`), KIP-98 Idempotent Producer, Cooperative Sticky Rebalancing, High-Throughput (211k msg/s) | [`go_clients/cmd/franz_runner/main.go`](file:///home/uttam/projects/AeroMQ/client/other_clients/go_clients/cmd/franz_runner/main.go) | **PASSED (5/5)** |
| **Confluent.Kafka** | .NET (C#) | `v2.6.0` | AdminClient (3 Partitions & Metadata), Codec Matrix (None/Gzip/Snappy), Headers (`X-Trace-Id`, `X-Source`, `X-Codec`), Dynamic Assignment, Manual Offset Commit & Resumption, SHA-256 Checksums, Benchmark (75k msg/s) | [`dotnet_client/Program.cs`](file:///home/uttam/projects/AeroMQ/client/other_clients/dotnet_client/Program.cs) | **PASSED (5/5)** |

---

## Directory Structure

```tree
client/other_clients/
├── README.md                          # Master verification documentation
├── node_client/
│   ├── package.json                   # kafkajs, kafkajs-snappy dependencies
│   ├── kafkajs_runner.js              # Comprehensive E2E test runner (5 test suites)
│   ├── index.js                       # Convenience CLI entrypoint
│   └── README.md                      # Node.js specific instructions & results
├── go_clients/
│   ├── go.mod                         # IBM/sarama & twmb/franz-go dependencies
│   ├── go.sum
│   ├── common/
│   │   └── helpers.go                 # Shared ANSI color formatting and helpers
│   ├── cmd/
│   │   ├── sarama_runner/
│   │   │   └── main.go                # Sarama E2E test runner (5 test suites + benchmark)
│   │   └── franz_runner/
│   │       └── main.go                # franz-go E2E test runner (5 test suites + cooperative rebalance)
└── dotnet_client/
    ├── DotnetClient.csproj            # .NET 8 project targeting Confluent.Kafka 2.6.0
    ├── Program.cs                     # Comprehensive .NET E2E test runner (5 test suites + benchmark)
    └── README.md                      # .NET Docker run instructions & verification matrix
```

---

## Prerequisites

1. Running AeroStream cluster with Kafka wire listener enabled:
   ```bash
   docker run -d --name aerostream-test-cluster \
     --ulimit nofile=65536:65536 \
     -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
     -v aerostream_data:/data \
     quay.io/gradientgeeks/aerostream:latest
   ```

2. Node.js `>= 18.0.0` (for KafkaJS)
3. Go `>= 1.21.0` (for Sarama and franz-go)
4. Docker / .NET 8.0 SDK (for Confluent.Kafka .NET client)

---

## Running the Suites

### 1. KafkaJS (Node.js)

```bash
cd client/other_clients/node_client
npm install
npm test
# or: node kafkajs_runner.js
```

### 2. IBM/sarama (Go)

```bash
cd client/other_clients/go_clients
go run ./cmd/sarama_runner/main.go
```

### 3. twmb/franz-go (Go)

```bash
cd client/other_clients/go_clients
go run ./cmd/franz_runner/main.go
```

### 4. Confluent.Kafka (.NET / C#)

Run containerized without requiring the host .NET SDK:
```bash
docker run --rm --network host \
  -v /home/uttam/projects/AeroMQ/client/other_clients/dotnet_client:/app \
  -w /app \
  mcr.microsoft.com/dotnet/sdk:8.0-alpine \
  dotnet run -c Release
```

---

## Benchmark Highlights

| Metric | twmb/franz-go (Go) | IBM/sarama (Go) | Confluent.Kafka (.NET) | KafkaJS (Node.js) |
| :--- | :--- | :--- | :--- | :--- |
| **Max Throughput** | **211,561 msg/s** (103.30 MB/s) | **138,684 msg/s** (67.72 MB/s) | **75,297 msg/s** (36.77 MB/s) | **1,500 msg/s** (49.7 MB/s) |
| **P50 Latency** | 12.0 ms | 9.3 ms | 12.6 ms | 14.2 ms |
| **P99 Latency** | 13.9 ms | 13.8 ms | 15.5 ms | 22.1 ms |
| **Rebalance Protocol** | Cooperative Sticky | Eager Range Rebalance | Eager Range Rebalance | Eager RoundRobin |
| **Compression Tested** | GZIP, Snappy, LZ4, ZSTD | GZIP, Snappy, LZ4, ZSTD | None, GZIP, Snappy | GZIP, Snappy |
| **Data Integrity** | 100% SHA-256 Match | 100% SHA-256 Match | 100% SHA-256 Match | 100% SHA-256 Match |

