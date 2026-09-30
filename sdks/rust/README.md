# AeroStream Rust Client SDK (`aerostream-client`)

The official high-performance, production-grade Rust client SDK for the AeroStream native binary protocol (`0xAE 0x01`).

## Features

- **Native Binary Protocol**: Fast zero-copy framing (`0xAE 0x01`), Big-Endian wire layout.
- **Asynchronous & Non-Blocking**: Built on Tokio runtime (`TCP_NODELAY`, custom framing).
- **Cancellation Safety**: Defused operation guards protect connection streams from corrupted state during timeout or cancellation.
- **Resilient Producer**: Configurable exponential backoff retry policy with randomized jitter.
- **Flexible Consumer**: Multi-entry long-polling fetch (`Command 4`) and continuous async `Stream` consumption.
- **Connection Management**: High-level `AeroClient` with connection pooling and resource reuse.
- **TLS Support**: Optional encrypted transport via `tokio-rustls`.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
aerostream-client = { path = "../rust" }
```

Or with TLS enabled:

```toml
[dependencies]
aerostream-client = { path = "../rust", features = ["tls"] }
```

## Quick Start

### Producer

```rust
use aerostream_client::{AeroProducer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ProducerConfig::builder()
        .bootstrap_server("127.0.0.1:9091")
        .max_retries(3)
        .build()?;

    let producer = AeroProducer::new(config);
    let offset = producer.send("orders", 0, b"order_payload_bytes").await?;
    println!("Message published at offset: {offset}");
    Ok(())
}
```

### Consumer (Batch & Continuous Stream)

```rust
use aerostream_client::{AeroConsumer, ConsumerConfig};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConsumerConfig::builder()
        .bootstrap_server("127.0.0.1:9091")
        .topic("orders")
        .partition(0)
        .initial_offset(0)
        .build()?;

    let consumer = AeroConsumer::new(config);

    // 1. Batch fetch
    let records = consumer.fetch_multi("orders", 0, 0, 1024 * 1024, 500).await?;
    for record in records {
        println!("Offset {}: {:?}", record.offset, record.payload);
    }

    // 2. Continuous stream
    let mut stream = consumer.stream();
    while let Some(item) = stream.next().await {
        let record = item?;
        println!("Streamed: {} -> {:?}", record.offset, record.payload);
    }

    Ok(())
}
```

## Protocol Wire Format Reference

- **Header (7B)**: `[magic: 0xAE, 0x01 (2B)][cmd: u8 (1B)][body_len: u32 BE (4B)]`
- **Command 0 (Auth)**:
  - Request: `[token: UTF-8]`
  - Response: `[0xAE, 0x01, status: u8]` (0 = OK, 3 = Auth Failed)
- **Command 1 (Produce)**:
  - Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][payload_len: u32 BE][payload: raw bytes]`
  - Response: `[0xAE, 0x01, status: u8][offset: u64 BE]` (status 0 = OK, 45 = OutOfOrder)
- **Command 2 (Consumer Fetch)**:
  - Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE]`
  - Response: `[0xAE, 0x01, status: u8]` (1 = Empty, 2 = Data followed by `[len: u32 BE][stream]`)
- **Command 4 (Multi-Entry Fetch)**:
  - Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE][max_wait_ms: u32 BE]`
  - Response: `[0xAE, 0x01, status: u8]` (1 = Empty, 2 = Data followed by `[count: u32 BE][(offset: u64, len: u32)...][payloads]`)
