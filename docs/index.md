---
icon: material/rocket-launch-outline
---
# Platform Overview & Quickstart

<div class="doc-badge-row" markdown>
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://github.com/gradientgeeks/aerostream/blob/main/LICENSE)
[![Go](https://img.shields.io/badge/Go-1.26_Control_Plane-00ADD8?logo=go&logoColor=white)](architecture.md#1-control-plane-architecture-go-126-raft-quorum)
[![Rust](https://img.shields.io/badge/Rust-1.98.1_Data_Plane-orange?logo=rust&logoColor=white)](architecture.md#2-data-plane-kernel-rust-1981-tokio)
[![UI](https://img.shields.io/badge/UI-Angular_21-DD0031?logo=angular&logoColor=white)](#accessing-the-web-console)
[![Protocol](https://img.shields.io/badge/protocol-Kafka_Wire_9092-black?logo=apachekafka&logoColor=white)](kafka-protocol.md)
[![Native Protocol](https://img.shields.io/badge/native-0xAE_0x01_9091-cyan)](architecture.md#5-native-binary-protocol-port-9091-vs-kafka-protocol-port-9092)
[![Status](https://img.shields.io/badge/status-preview-orange)](https://aerostream.gradientgeeks.com/)
</div>

## What is AeroStream?

**AeroStream** is an open-source, ultra-high-throughput, cloud-native distributed event streaming platform engineered around a **Dual-Engine Architecture**. It combines the distributed consensus stability and operational agility of a **Go Raft Control Plane** with the zero-copy performance and mechanical sympathy of a **Rust Shard-per-Core Storage Kernel**.

![AeroStream Architecture](images/dual_engine_architecture.png)

!!! tip "Drop-In Apache Kafka Compatibility & Ultra-Fast Native Streaming"
    AeroStream natively provides **dual ingress protocols**:
    
    * **Port `9092` (Kafka Wire Protocol)**: Drop-in replacement for standard Kafka SDKs (`kafka-clients`, `confluent-kafka`, `kafkajs`, `franz-go`). Point existing apps directly to AeroStream without modifying code or schemas.
    * **Port `9091` (AeroStream Native Protocol `0xAE 0x01`)**: Ultra-lightweight 7-byte binary header with zero Kafka framing overhead, delivering sub-millisecond tail latencies via official SDKs in Go, Rust, Java, .NET, and Node.js.

---

## Dual-Engine Architectural Rationale

AeroStream pairs two purpose-built runtimes, each used where it excels: **Go 1.26** for the control plane (HashiCorp Raft consensus, REST APIs, Schema Registry, RBAC policies, and stream transforms) and **Rust 1.98.1 (Edition 2024)** for the storage data plane (zero-copy `sendfile(2)`, hardware CRC32C, memory-mapped indices, and no garbage collection).

| Subsystem | Engine Runtime | Core Responsibilities | Performance Highlights |
|---|---|---|---|
| **Control Plane** | **Go 1.26** (Alpine) | HashiCorp Raft Quorum, Schema Registry, RBAC ACLs, Stream Transforms, Connectors, Web Console REST API | Green Tea GC (<1 ms pause), SIMD Swiss Tables hash maps, native Kubernetes cgroup auto-tuning |
| **Data Plane** | **Rust 1.98.1** (Edition 2024) | TCP Listeners (Ports 9091/9092), Zero-Copy Segmented Commit Log, Hardware CRC32C, Tiered Storage | Zero-copy `sendfile(2)` I/O, 0.7 ms median publish latency at 100k-200k msg/s, lock-free Shard-per-Core |
| **Console UI** | **Angular 21** | Cluster Topology Visualizer, Live Message Inspector, Schema Browser, ACL Simulators | Single Page Application, dark/light themes, reactive WebSocket & REST updates |

![Controller-Broker Orchestration](images/controller_broker_orchestration.png)

---

## Verified Benchmark Results (OpenMessaging Benchmark)

AeroStream was evaluated using the official **[Linux Foundation OpenMessaging Benchmark (OMB)](https://github.com/openmessaging/benchmark)** suite through its Kafka wire protocol port (`9092`) on an AWS `c6id.2xlarge` instance (8 vCPUs, 16 GiB RAM, local NVMe SSD):

| Workload Target | Actual Publish Rate | Publish $p_{50}$ | Publish $p_{99}$ | Publish $p_{99.9}$ | End-to-End $p_{99}$ | Broker Cores Busy | Errors |
|---|---|---|---|---|---|---|---|
| **100,000 msg/s** (fixed) | 100,082 msg/s (97.7 MB/s) | **0.7 ms** | **1.4 ms** | **2.3 ms** | 2.0 ms | 14% | 0 |
| **200,000 msg/s** (fixed) | 200,175 msg/s (195.5 MB/s) | **0.7 ms** | **1.7 ms** | **3.0 ms** | 2.0 ms | 22% | 0 |
| **Maximum Rate** (unthrottled) | **271,350 msg/s** (265.0 MB/s) | 105 ms | 1,104 ms | 1,376 ms | 1,119 ms | 55% | 0 |

* **Zero Tail Latency Spikes**: $p_{99}$ latency stays strictly under **1.7 ms** at sustained 200,000 msg/s with consumers keeping pace in real time.
* **Low CPU Utilization**: At 200,000 msg/s, the broker consumes only **22%** of its assigned execution cores.
* **Hardware-Accelerated Throughput**: Delivers **10.9 GB/s** CRC32C verification and true zero-copy `sendfile(2)` transmission.

---

## Quickstart: Launch AeroStream in 30 Seconds

Launch the complete AeroStream stack (Go Controller, Rust Storage Broker, and Angular 21 Web Console) using the official container image:

```bash
docker run -d --name aerostream \
  -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
  -v aerostream_data:/data \
  quay.io/gradientgeeks/aerostream:latest
```

### Port Mapping Reference

| Port | Protocol | Subsystem | Purpose |
|---|---|---|---|
| **`9092`** | `TCP (Kafka Wire)` | Rust Broker | **Standard Kafka Client Ingress** (`spring-kafka`, `confluent-kafka`, `librdkafka`, `kcat`) |
| **`9091`** | `TCP (Native)` | Rust Broker | **AeroStream Native High-Speed Protocol** (`aerostream-sdk` `0xAE 0x01` framing) |
| **`9001`** | `HTTP / REST` | Go Controller | **Angular 21 Web Console**, Schema Registry, REST Produce/Fetch, Management API |
| **`8001`** | `gRPC` | Go Controller | Internal cluster metadata synchronization and broker registration |
| **`7001`** | `TCP (Raft)` | Go Controller | HashiCorp Raft consensus quorum transport |

### Docker Compose Configuration

Create a `docker-compose.yml` for local orchestration:

```yaml
version: '3.8'

services:
  aerostream:
    image: quay.io/gradientgeeks/aerostream:latest
    container_name: aerostream
    ports:
      - "9091:9091"   # Ultra High-Speed Native TCP Protocol (0xAE 0x01)
      - "9092:9092"   # Apache Kafka Wire Protocol
      - "9001:9001"   # HTTP REST, Angular 21 Console & Schema Registry
      - "8001:8001"   # Internal gRPC Broker Control
      - "7001:7001"   # Raft Consensus Transport
    volumes:
      - aerostream_data:/data
    restart: unless-stopped

volumes:
  aerostream_data:
```

Launch with:

```bash
docker compose up -d
```

### Verifying Cluster Health

Verify cluster health via the REST API:

```bash
curl -s http://localhost:9001/api/cluster | jq
```

*Expected JSON output:*

```json
{
  "node_id": "node1",
  "raft_state": "Leader",
  "raft_leader": "127.0.0.1:7001",
  "brokers_count": 1,
  "brokers": [
    {
      "id": 1,
      "host": "0.0.0.0",
      "port": 9091,
      "kafka_port": 9092,
      "active": true
    }
  ],
  "topics_count": 0,
  "groups_count": 0
}
```

### Accessing the Web Console

Open your browser to:

[http://localhost:9001/aerostream/console](http://localhost:9001/aerostream/console)

The integrated **Angular 21 Web Console** allows real-time inspection of cluster topology, partition states, live consumer lag, schema registry contracts, and broker performance metrics.

---

## Client Quickstarts

### 1. Official AeroStream Native SDKs (Port 9091, `0xAE 0x01`)

For the lowest possible latency and overhead, use the official `aerostream-sdk` packages:

=== "Go (`github.com/gradientgeeks/aerostream-sdk/go`)"

    ```bash
    go get github.com/gradientgeeks/aerostream-sdk/go
    ```

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
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Produced record at offset %d\n", offset)
    }
    ```

=== "Rust (`aerostream-client`)"

    ```toml
    [dependencies]
    aerostream-client = "0.1.0-preview"
    tokio = { version = "1", features = ["full"] }
    ```

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

=== "Java (`org.gradientgeeks.aerostream`)"

    ```xml
    <dependency>
        <groupId>org.gradientgeeks.aerostream</groupId>
        <artifactId>aerostream-client</artifactId>
        <version>0.1.0-preview</version>
    </dependency>
    ```

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

=== ".NET C# (`GradientGeeks.AeroStream.Client`)"

    ```bash
    dotnet add package GradientGeeks.AeroStream.Client --version 0.1.0-preview
    ```

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

=== "Node.js / TypeScript (`@gradientgeeks/aerostream-client`)"

    ```bash
    npm install @gradientgeeks/aerostream-client@0.1.0-preview
    ```

    ```typescript
    import { AeroClient } from '@gradientgeeks/aerostream-client';

    const client = await AeroClient.connect('127.0.0.1:9091', 'secret-token');
    const producer = client.producer();

    const offset = await producer.send('telemetry', 0, 'sensor-payload');
    console.log(`Produced record at offset ${offset}`);

    await client.close();
    ```

---

### 2. Standard Apache Kafka Clients (Port 9092)

Point standard Kafka client libraries directly at `localhost:9092`:

=== "Python (`confluent-kafka`)"

    ```python
    from confluent_kafka import Producer, Consumer

    # Produce records to AeroStream
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    producer.produce('orders', key='ORD-101', value=b'{"amount": 149.50}')
    producer.flush()

    # Consume records
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'orders-analytics',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['orders'])
    msg = consumer.poll(1.0)
    if msg:
        print(f"Received: {msg.value().decode('utf-8')}")
    consumer.close()
    ```

=== "Java / Spring Kafka"

    Add to `application.yml`:

    ```yaml
    spring:
      kafka:
        bootstrap-servers: localhost:9092
        producer:
          key-serializer: org.apache.kafka.common.serialization.StringSerializer
          value-serializer: org.apache.kafka.common.serialization.StringSerializer
          acks: 1
        consumer:
          group-id: inventory-service
          auto-offset-reset: earliest
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    ```

=== "Kafka CLI (Bash)"

    ```bash
    # 1. Create a 3-partition topic via Controller REST API
    curl -X POST http://localhost:9001/api/topics \
      -H "Content-Type: application/json" \
      -d '{"name": "orders", "partitions": 3, "replication_factor": 1}'

    # 2. Produce records via standard Kafka CLI tools
    echo "order-101: {\"amount\": 89.50}" | kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic orders

    # 3. Consume records
    kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic orders \
      --from-beginning
    ```

=== "Go (`segmentio/kafka-go`)"

    ```go
    package main

    import (
        "context"
        "fmt"
        "github.com/segmentio/kafka-go"
    )

    func main() {
        w := &kafka.Writer{
            Addr:     kafka.TCP("localhost:9092"),
            Topic:    "orders",
            Balancer: &kafka.LeastBytes{},
        }
        defer w.Close()

        err := w.WriteMessages(context.Background(),
            kafka.Message{
                Key:   []byte("ORD-101"),
                Value: []byte(`{"amount": 89.50}`),
            },
        )
        if err != nil {
            panic(err)
        }
        fmt.Println("Produced message successfully to AeroStream!")
    }
    ```
