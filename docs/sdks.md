# AeroStream Client SDKs

[![SDK Repo](https://img.shields.io/badge/SDK_Repo-gradientgeeks%2Faerostream--sdk-blue?logo=github)](https://github.com/gradientgeeks/aerostream-sdk)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://github.com/gradientgeeks/aerostream-sdk/blob/main/LICENSE)
[![Version](https://img.shields.io/badge/status-v0.1.0--preview-orange)](https://github.com/gradientgeeks/aerostream-sdk)

Official production-grade client SDKs for AeroStream's ultra-low-latency native binary protocol (`0xAE 0x01` on TCP port `9091`).

Source: [github.com/gradientgeeks/aerostream-sdk](https://github.com/gradientgeeks/aerostream-sdk)

---

## Native Protocol vs Kafka Protocol

| | Port 9091 (Native) | Port 9092 (Kafka Wire) |
|:--|:--|:--|
| **Use these SDKs** | ✅ Yes | ❌ Use standard Kafka clients |
| **Overhead** | 7-byte header only | Full Kafka envelope |
| **Tail latency** | Sub-millisecond | ~1–5 ms |
| **Auth** | Bearer token (Cmd 0) | SASL mechanisms |

### Protocol Framing (`0xAE 0x01`)

```
Header (7 bytes):
  [0xAE][0x01]  — magic bytes
  [cmd: u8]     — command id
  [body_len: u32 BE] — payload length
```

| Cmd | Name | Description |
|:---:|:--|:--|
| `0` | Auth | Bearer token authentication handshake |
| `1` | Produce | Append record to topic-partition, returns `u64` offset |
| `2` | Fetch | Bounded consumer fetch up to High Watermark |
| `4` | LongPoll Fetch | Multi-record batch with server-side suspension |

---

## SDK Matrix

| Language | Package | Install | Status |
|:--|:--|:--|:---:|
| 🦫 Go | `github.com/gradientgeeks/aerostream-sdk/go` | `go get github.com/gradientgeeks/aerostream-sdk/go` | ✅ Preview |
| 🦀 Rust | `aerostream-client` | `cargo add aerostream-client` | ✅ Preview |
| ☕ Java | `org.gradientgeeks.aerostream:aerostream-client` | Maven / Gradle | ✅ Preview |
| 🔷 .NET | `GradientGeeks.AeroStream.Client` | `dotnet add package GradientGeeks.AeroStream.Client` | ✅ Preview |
| 🟩 Node.js | `@gradientgeeks/aerostream-client` | `npm install @gradientgeeks/aerostream-client` | ✅ Preview |

---

## Go SDK

**Module**: `github.com/gradientgeeks/aerostream-sdk/go`  
**Requires**: Go 1.21+

### Install

```bash
go get github.com/gradientgeeks/aerostream-sdk/go
```

### Produce

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/gradientgeeks/aerostream-sdk/go/client"
)

func main() {
    c, err := client.NewClient("127.0.0.1:9091",
        client.WithAuthToken("secret-token"),
        client.WithTimeout(5*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    producer := c.NewProducer()
    offset, err := producer.Produce(context.Background(), "telemetry", 0, []byte("sensor-payload"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Produced at offset %d\n", offset)
}
```

### Consume

```go
consumer := c.NewConsumer()
for {
    records, err := consumer.FetchMulti(ctx, "telemetry", 0, startOffset, 1<<20, 500)
    if err != nil {
        log.Fatal(err)
    }
    for _, r := range records {
        fmt.Printf("offset=%d payload=%s\n", r.Offset, r.Payload)
        startOffset = r.Offset + 1
    }
}
```

### API Reference

| Type | Method | Description |
|:--|:--|:--|
| `Client` | `NewClient(addr, ...Option)` | Create connection with options |
| `Client` | `NewProducer()` | Create producer |
| `Client` | `NewConsumer()` | Create consumer |
| `Client` | `Close()` | Close connection |
| `Producer` | `Produce(ctx, topic, partition, payload)` | Append record, returns offset |
| `Consumer` | `Fetch(ctx, topic, partition, startOffset, maxBytes)` | Single fetch |
| `Consumer` | `FetchMulti(ctx, topic, partition, startOffset, maxBytes, maxWaitMs)` | Long-poll batch fetch |
| `Consumer` | `Messages(ctx)` | Returns `<-chan Record` streaming channel |

**Options**: `WithAuthToken(string)`, `WithTLS(*tls.Config)`, `WithTimeout(time.Duration)`, `WithRetry(n int)`

---

## Rust SDK

**Crate**: `aerostream-client` v0.1.0-preview  
**Requires**: Rust 1.75+ (Edition 2021)

### Install

```bash
cargo add aerostream-client
```

Or in `Cargo.toml`:

```toml
[dependencies]
aerostream-client = "0.1.0-preview"
tokio = { version = "1", features = ["full"] }
```

### Produce

```rust
use aerostream_client::{AeroClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = AeroClient::connect(
        ClientConfig::new("127.0.0.1:9091")
            .with_auth_token("secret-token")
    ).await?;

    let producer = client.producer();
    let offset = producer.send("telemetry", 0, b"sensor-payload").await?;
    println!("Produced at offset {offset}");
    Ok(())
}
```

### Consume

```rust
let consumer = client.consumer();
let mut stream = consumer.stream("telemetry", 0, 0).await?;

while let Some(record) = stream.next().await {
    let r = record?;
    println!("offset={} payload={:?}", r.offset, r.payload);
}
```

### API Reference

| Type | Method | Description |
|:--|:--|:--|
| `AeroClient` | `connect(ClientConfig)` | Async connect with auth |
| `AeroClient` | `producer()` | Get producer handle |
| `AeroClient` | `consumer()` | Get consumer handle |
| `AeroProducer` | `send(topic, partition, payload)` | Produce record, returns offset |
| `AeroConsumer` | `fetch(topic, partition, start, max_bytes)` | Bounded fetch |
| `AeroConsumer` | `fetch_multi(topic, partition, start, max_bytes, wait_ms)` | Long-poll batch fetch |
| `AeroConsumer` | `stream(topic, partition, start)` | Async stream |

**Error types**: `AeroError::AuthFailed`, `AeroError::ProtocolError`, `AeroError::Io`, `AeroError::Timeout`, `AeroError::OutOfOrder`

---

## Java SDK

**Group**: `org.gradientgeeks.aerostream`  
**Artifact**: `aerostream-client`  
**Version**: `0.1.0-preview`  
**Requires**: Java 17+

### Install (Maven)

```xml
<dependency>
  <groupId>org.gradientgeeks.aerostream</groupId>
  <artifactId>aerostream-client</artifactId>
  <version>0.1.0-preview</version>
</dependency>
```

### Install (Gradle)

```groovy
implementation 'org.gradientgeeks.aerostream:aerostream-client:0.1.0-preview'
```

### Produce

```java
import org.gradientgeeks.aerostream.client.AeroClient;
import org.gradientgeeks.aerostream.client.AeroProducer;
import java.nio.charset.StandardCharsets;

public class ProduceExample {
    public static void main(String[] args) throws Exception {
        try (AeroClient client = AeroClient.connect("127.0.0.1:9091", "secret-token");
             AeroProducer producer = client.producer()) {

            long offset = producer.send("telemetry", 0,
                "sensor-payload".getBytes(StandardCharsets.UTF_8));
            System.out.printf("Produced at offset %d%n", offset);
        }
    }
}
```

### Consume

```java
try (AeroClient client = AeroClient.connect("127.0.0.1:9091", "secret-token");
     AeroConsumer consumer = client.consumer()) {

    List<AeroRecord> records = consumer.fetchMulti("telemetry", 0, 0L, 1048576, 500);
    for (AeroRecord r : records) {
        System.out.printf("offset=%d payload=%s%n",
            r.getOffset(), new String(r.getPayload()));
    }
}
```

### API Reference

| Class | Method | Description |
|:--|:--|:--|
| `AeroClient` | `connect(host, token)` | Synchronous connect with auth |
| `AeroClient` | `producer()` | Create `AeroProducer` |
| `AeroClient` | `consumer()` | Create `AeroConsumer` |
| `AeroProducer` | `send(topic, partition, payload)` | Synchronous produce |
| `AeroProducer` | `sendAsync(topic, partition, payload)` | `CompletableFuture<Long>` produce |
| `AeroConsumer` | `fetch(topic, partition, startOffset, maxBytes)` | Bounded fetch |
| `AeroConsumer` | `fetchMulti(topic, partition, startOffset, maxBytes, waitMs)` | Long-poll batch |
| `AeroConsumer` | `stream()` | `Stream<AeroRecord>` iterator |

**Exceptions**: `AeroException`, `AuthenticationException`, `OutOfOrderException`, `ConnectionException`

---

## .NET (C#) SDK

**Package**: `GradientGeeks.AeroStream.Client`  
**Version**: `0.1.0-preview`  
**Requires**: .NET 8.0+

### Install

```bash
dotnet add package GradientGeeks.AeroStream.Client --version 0.1.0-preview
```

### Produce

```csharp
using System.Text;
using GradientGeeks.AeroStream.Client;

await using var client = await AeroClient.ConnectAsync(new AeroClientOptions {
    BootstrapServers = ["127.0.0.1:9091"],
    AuthToken = "secret-token"
});

var producer = client.CreateProducer();
long offset = await producer.SendAsync(
    "telemetry", 0,
    Encoding.UTF8.GetBytes("sensor-payload"));
Console.WriteLine($"Produced at offset {offset}");
```

### Consume

```csharp
var consumer = client.CreateConsumer();
await foreach (var record in consumer.StreamAsync("telemetry", 0, startOffset: 0))
{
    Console.WriteLine($"offset={record.Offset} payload={Encoding.UTF8.GetString(record.Payload.Span)}");
}
```

### API Reference

| Type | Member | Description |
|:--|:--|:--|
| `AeroClient` | `ConnectAsync(options)` | Async connect with TLS/auth |
| `AeroClient` | `CreateProducer()` | Get producer |
| `AeroClient` | `CreateConsumer()` | Get consumer |
| `AeroProducer` | `SendAsync(topic, partition, payload, ct)` | `ValueTask<long>` produce |
| `AeroConsumer` | `FetchAsync(topic, partition, start, maxBytes, ct)` | Bounded fetch |
| `AeroConsumer` | `FetchMultiAsync(topic, partition, start, maxBytes, waitMs, ct)` | Long-poll batch |
| `AeroConsumer` | `StreamAsync(topic, partition, start, ct)` | `IAsyncEnumerable<AeroRecord>` |

**Exceptions**: `AeroStreamException`, `AuthenticationException`, `OutOfOrderSequenceException`

---

## Node.js SDK

**Package**: `@gradientgeeks/aerostream-client`  
**Version**: `0.1.0-preview`  
**Requires**: Node.js 18+ / TypeScript 5+

### Install

```bash
npm install @gradientgeeks/aerostream-client
```

### Produce

```typescript
import { AeroClient } from '@gradientgeeks/aerostream-client';

const client = await AeroClient.connect('127.0.0.1:9091', 'secret-token');
const producer = client.producer();

const offset = await producer.send('telemetry', 0, Buffer.from('sensor-payload'));
console.log(`Produced at offset ${offset}`);

await client.close();
```

### Consume

```typescript
const consumer = client.consumer();
const records = await consumer.fetchMulti('telemetry', 0, 0n, 1048576, 500);
for (const record of records) {
    console.log(`offset=${record.offset} payload=${record.payload.toString()}`);
}
```

### Streaming

```typescript
for await (const record of consumer.stream('telemetry', 0, 0n)) {
    console.log(`offset=${record.offset}`);
}
```

### API Reference

| Class | Method | Description |
|:--|:--|:--|
| `AeroClient` | `connect(addr, token)` | Static async connect |
| `AeroClient` | `producer()` | Get producer |
| `AeroClient` | `consumer()` | Get consumer |
| `AeroClient` | `close()` | Disconnect |
| `AeroProducer` | `send(topic, partition, payload)` | Produce, returns `Promise<bigint>` |
| `AeroConsumer` | `fetch(topic, partition, start, maxBytes)` | `Promise<AeroRecord[]>` |
| `AeroConsumer` | `fetchMulti(topic, partition, start, maxBytes, waitMs)` | Long-poll batch |
| `AeroConsumer` | `stream(topic, partition, start)` | `AsyncIterable<AeroRecord>` |

---

## Example Applications

The following example applications are included in the SDK repository at [github.com/gradientgeeks/aerostream-sdk](https://github.com/gradientgeeks/aerostream-sdk):

| Example | Language | Source |
|:--|:--|:--|
| `produce_consume` | Go | [`go/examples/main.go`](https://github.com/gradientgeeks/aerostream-sdk/blob/main/go/examples/main.go) |
| `produce_consume` | Rust | [`rust/examples/produce_consume.rs`](https://github.com/gradientgeeks/aerostream-sdk/blob/main/rust/examples/produce_consume.rs) |
| `ExampleApp` | Java | [`java/src/test/java/io/aerostream/example/ExampleApp.java`](https://github.com/gradientgeeks/aerostream-sdk/blob/main/java/src/test/java/io/aerostream/example/ExampleApp.java) |
| `AeroStream.Example` | .NET | [`dotnet/examples/AeroStream.Example/Program.cs`](https://github.com/gradientgeeks/aerostream-sdk/blob/main/dotnet/examples/AeroStream.Example/Program.cs) |
| `example.ts` | Node.js | [`nodejs/examples/example.ts`](https://github.com/gradientgeeks/aerostream-sdk/blob/main/nodejs/examples/example.ts) |

---

## Kafka-Compatible Clients (Port 9092)

For Kafka wire protocol (port 9092), use any standard Kafka client:

=== "Python"
    ```bash
    pip install kafka-python-ng confluent-kafka
    ```
    ```python
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    producer.send('orders', b'payload')
    ```

=== "Go (franz-go)"
    ```bash
    go get github.com/twmb/franz-go/pkg/kgo
    ```
    ```go
    cl, _ := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
    cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("payload")})
    ```

=== "Java (kafka-clients)"
    ```xml
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>3.7.0</version>
    </dependency>
    ```

=== ".NET (Confluent.Kafka)"
    ```bash
    dotnet add package Confluent.Kafka
    ```

=== "Node.js (KafkaJS)"
    ```bash
    npm install kafkajs
    ```

---

## SDK Repository

The standalone public SDK repository is at:  
**[github.com/gradientgeeks/aerostream-sdk](https://github.com/gradientgeeks/aerostream-sdk)**

It is published separately from the main AeroStream server and can be imported independently by application developers.
