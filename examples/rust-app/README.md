# AeroStream Rust Application Example

This project demonstrates streaming data into and out of **AeroStream** using pure **Tokio asynchronous Rust**, showcasing both the **Apache Kafka Wire Protocol** and AeroStream's ultra-low-latency **Native Zero-Copy Binary Protocol**.

## Features Demonstrated
1. **Kafka Wire Protocol Compatibility (`:9093`)**:
   - `ApiVersions` (ApiKey 18): Dynamic negotiation of Kafka protocol versions and supported capabilities.
   - `Metadata` (ApiKey 3): Broker node discovery, cluster topology, and partition leader routing.
   - `Produce` (ApiKey 0): Kafka message wrapping, IEEE CRC32 checksum generation, and broker ACK parsing.
   - `Fetch` (ApiKey 1): Partition record stream retrieval and high-watermark verification.
2. **Native Zero-Copy Binary Protocol (`:9091`)**:
   - High-performance binary protocol framing (`[0xAE, 0x01, Cmd, BodyLen]`).
   - Command 1 (Produce): Direct memory-mapped log append with sub-millisecond ACK.
   - Command 2 (Fetch): Direct zero-copy kernel page-cache retrieval without serialization overhead.

## Prerequisites
- Rust 1.80+ (tested on Rust 1.98)
- Running AeroStream Cluster (`make start`)

## Building and Running

```bash
# 1. Run the application suite directly
cd examples/rust-app
cargo run

# 2. Or build an optimized release binary
cargo build --release
./target/release/aerostream-rust-demo
```

## Environment Variables

- `AEROSTREAM_KAFKA_BROKER`: Kafka wire listener (default: `127.0.0.1:9093`)
- `AEROSTREAM_NATIVE_BROKER`: Native protocol listener (default: `127.0.0.1:9091`)
