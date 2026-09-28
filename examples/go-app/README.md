# AeroStream Go Application Example

This project demonstrates streaming data into and out of **AeroStream** using both the **Apache Kafka Wire Protocol** (pure Go, zero-CGO dependencies) and AeroStream's ultra-fast **Native Zero-Copy Binary Protocol**.

## Features Demonstrated
1. **Kafka Protocol Compatibility (`:9093`)**:
   - `ApiVersions` (ApiKey 18): Negotiation of Kafka protocol versions and supported capabilities.
   - `Metadata` (ApiKey 3): Cluster metadata discovery, broker identification, and partition leader mapping.
   - `Produce` (ApiKey 0): Record batch framing, CRC32 verification, and acknowledgment processing.
   - `Fetch` (ApiKey 1): High-throughput streaming record retrieval.
2. **Native Zero-Copy Binary Protocol (`:9091`)**:
   - Fast binary framing (`[0xAE, 0x01, Cmd, BodyLen]`).
   - Command 1 (Produce): Direct memory append with sub-millisecond ACK.
   - Command 2 (Fetch): Direct kernel page-cache retrieval via non-blocking zero-copy streaming.

## Prerequisites
- Go 1.24+ (tested on Go 1.26)
- Running AeroStream Cluster (`make start`)

## Building and Running

```bash
# 1. Run the test suite
cd examples/go-app
go run main.go

# 2. Or build a standalone binary
go build -o bin/go-demo main.go
./bin/go-demo 127.0.0.1:9093
```
