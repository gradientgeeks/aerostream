# AeroStream Client SDKs (`aerostream-sdk`)

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-v0.1.0--preview-orange)](https://aerostream.gradientgeeks.com)
[![Protocol](https://img.shields.io/badge/protocol-AeroStream_Native_0xAE01-black)](https://aerostream.gradientgeeks.com/docs/)

Official production-grade client SDKs for **AeroStream**'s ultra-low-latency native binary protocol (`0xAE 0x01` on TCP port `9091`).

---

## 📦 Supported Languages & Packages

| Language | Directory / Package | Module / Package Identifier | Status |
| :--- | :--- | :--- | :---: |
| 🦫 **Golang** | [`go/`](go/) | `github.com/gradientgeeks/aerostream-sdk/go` | ✅ **v0.1.0-preview** |
| 🦀 **Rust** | [`rust/`](rust/) | `aerostream-client` | ✅ **v0.1.0-preview** |
| ☕ **Java** | [`java/`](java/) | `org.gradientgeeks.aerostream:aerostream-client` | ✅ **v0.1.0-preview** |
| 🔷 **.NET (C#)** | [`dotnet/`](dotnet/) | `GradientGeeks.AeroStream.Client` | ✅ **v0.1.0-preview** |
| 🟩 **Node.js** | [`nodejs/`](nodejs/) | `@gradientgeeks/aerostream-client` | ✅ **v0.1.0-preview** |

---

## ⚡ Native Protocol vs Kafka Protocol (Port 9091 vs 9092)

* **Port 9092 (Kafka Wire Protocol)**: Drop-in replacement for standard Kafka SDKs (`kafka-clients`, `confluent-kafka`, `kafkajs`, `franz-go`).
* **Port 9091 (AeroStream Native Protocol)**: 7-byte ultra-lightweight header, direct memory layout, zero Kafka envelope overhead, sub-millisecond tail latency.

### Protocol Framing (`0xAE 0x01`)
```
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
| Magic Bytes (2B)    | Command (1B) | Body Length (4B BE)  | Variable Payload (N B)   |
| 0xAE 0x01           | uint8 (0..4) | uint32               | Command-Specific Bytes   |
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
```

* **Command 0**: Authentication Handshake (Bearer token)
* **Command 1**: High-Speed Produce / Append (assigned 64-bit log offset)
* **Command 2**: Consumer Fetch (bounded by partition High Watermark)
* **Command 4**: Multi-Entry Long-Polling Fetch (multi-record batches with server-side suspension)

---

## 🚀 Quickstarts

### Golang
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gradientgeeks/aerostream-sdk/go/client"
)

func main() {
	c, err := client.NewClient("127.0.0.1:9091", client.WithAuthToken("secret-token"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	producer := c.NewProducer()
	offset, err := producer.Produce(context.Background(), "telemetry", 0, []byte("sensor-payload"))
	fmt.Printf("Produced record at offset %d\n", offset)
}
```

### Rust
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
    println!("Produced record at offset {offset}");

    Ok(())
}
```

### Java
```java
import org.gradientgeeks.aerostream.client.AeroClient;
import org.gradientgeeks.aerostream.client.AeroProducer;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) {
        try (AeroClient client = AeroClient.connect("127.0.0.1:9091", "secret-token");
             AeroProducer producer = client.producer()) {
            long offset = producer.send("telemetry", 0, "sensor-payload".getBytes(StandardCharsets.UTF_8));
            System.out.printf("Produced record at offset %d%n", offset);
        }
    }
}
```

### .NET (C#)
```csharp
using System.Text;
using GradientGeeks.AeroStream.Client;

await using var client = await AeroClient.ConnectAsync(new AeroClientOptions {
    BootstrapServers = ["127.0.0.1:9091"],
    AuthToken = "secret-token"
});

var producer = client.CreateProducer();
long offset = await producer.SendAsync("telemetry", 0, Encoding.UTF_8.GetBytes("sensor-payload"));
Console.WriteLine($"Produced record at offset {offset}");
```

### Node.js / TypeScript
```typescript
import { AeroClient } from '@gradientgeeks/aerostream-client';

const client = await AeroClient.connect('127.0.0.1:9091', 'secret-token');
const producer = client.producer();

const offset = await producer.send('telemetry', 0, 'sensor-payload');
console.log(`Produced record at offset ${offset}`);

await client.close();
```

---

## 📄 License
Licensed under Apache License, Version 2.0 ([LICENSE](LICENSE)).
