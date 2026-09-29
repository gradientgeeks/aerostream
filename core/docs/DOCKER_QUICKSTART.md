# AeroStream Docker Quickstart Guide

This guide explains how developers can run and interact with the official **AeroStream** full-stack container (`quay.io/gradientgeeks/aerostream:latest`) locally or in CI/CD pipelines.

The image contains everything required for a production-grade streaming environment:
- **Go Controller**: Raft consensus, Confluent-compatible Schema Registry, In-broker Stream Transforms (WASM/Filter/PII), Kafka Connect REST API, and Granular RBAC.
- **Rust Zero-Copy Broker**: Native TCP engine, standard Kafka Wire Protocol engine (ApiKey 0–36), KIP-98 Idempotent Producer, SASL authentication (`PLAIN` / `SCRAM-SHA-256`), and 2PC distributed transactions.
- **Embedded Web Console**: Single-page Angular management console served on port `9001` at `/aerostream/console`.

---

## 1. Quickstart: Run in Docker

Run the full-stack container with persistent storage:

```bash
docker run -d --name aerostream \
  -p 9091:9091 -p 9092:9092 -p 9001:9001 -p 8001:8001 -p 7001:7001 \
  -v aerostream_data:/data \
  quay.io/gradientgeeks/aerostream:latest
```

### Port Mapping Reference

| Port | Protocol | Component | Purpose |
|---|---|---|---|
| **`9092`** | `TCP (Kafka Wire)` | Rust Broker | **Standard Kafka Client Ingress** (`confluent-kafka`, `librdkafka`, `spring-kafka`, `kafkajs`, `.NET`) |
| **`9001`** | `HTTP / REST` | Go Controller | **Web Console UI** (`http://localhost:9001/aerostream/console`), REST Produce/Fetch, Schema Registry, and Management API |
| **`9091`** | `TCP (Native)` | Rust Broker | Ultra-low latency native binary stream protocol (`0xAE 0x51` magic header) |
| **`8001`** | `gRPC` | Go Controller | Internal cluster metadata synchronization and broker registration |
| **`7001`** | `TCP (Raft)` | Go Controller | Raft consensus quorum transport |

---

## 2. Verify Container Health

### Check Container Status & Logs
```bash
docker ps --filter name=aerostream
docker logs -f aerostream
```

### Inspect Cluster Health via REST
```bash
curl -s http://localhost:9001/api/cluster | jq
```
*Expected response:*
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
      "active": true
    }
  ],
  "topics_count": 0,
  "groups_count": 0
}
```

### Open the Web Console
Navigate to:
```text
http://localhost:9001/aerostream/console
```

---

## 3. Developer Client Integrations

Point your application to `localhost:9092` (standard Kafka protocol) or `localhost:9001` (HTTP REST).

### 3.1 Python (`confluent-kafka`)

```python
from confluent_kafka import Producer, Consumer

# 1. Produce Message
producer = Producer({'bootstrap.servers': 'localhost:9092'})
producer.produce('user-events', key='user_42', value='{"event": "LOGIN", "status": "SUCCESS"}')
producer.flush()
print("Delivered event to AeroStream!")

# 2. Consume Message
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'auth-monitoring-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['user-events'])
msg = consumer.poll(timeout=3.0)
if msg and not msg.error():
    print(f"Consumed from AeroStream: {msg.key().decode()} -> {msg.value().decode()}")
consumer.close()
```

---

### 3.2 .NET 8 / C# (`Confluent.Kafka`)

Install NuGet package:
```bash
dotnet add package Confluent.Kafka --version 2.15.1
```

```csharp
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main()
    {
        // 1. Idempotent Producer
        var pConfig = new ProducerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            EnableIdempotence = true // KIP-98 exactly-once semantics
        };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();
        var report = await producer.ProduceAsync("orders", new Message<string, string>
        {
            Key = "order-5501",
            Value = "{\"amount\": 89.90, \"currency\": \"USD\"}"
        });
        Console.WriteLine($"Delivered to {report.TopicPartitionOffset}");

        // 2. Consumer Group
        var cConfig = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            GroupId = "order-processing-service",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Subscribe("orders");
        var cr = consumer.Consume(TimeSpan.FromSeconds(5));
        if (cr != null)
        {
            Console.WriteLine($"Consumed: {cr.Message.Key} -> {cr.Message.Value}");
        }
    }
}
```

---

### 3.3 Node.js / TypeScript (`kafkajs`)

Install package:
```bash
npm install kafkajs
```

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'node-microservice',
  brokers: ['localhost:9092']
});

async function run() {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: 'telemetry',
    messages: [{ key: 'sensor_1', value: JSON.stringify({ temp: 22.4, status: 'OK' }) }]
  });
  console.log('Record produced!');

  const consumer = kafka.consumer({ groupId: 'telemetry-processors' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'telemetry', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received [${topic}:${partition}]: ${message.value.toString()}`);
    }
  });
}
run().catch(console.error);
```

---

### 3.4 Java / Spring Boot (`spring-kafka`)

`application.yml`:
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      group-id: inventory-group
      auto-offset-reset: earliest
```

Producer & Listener:
```java
@Service
public class OrderService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void createOrder(String orderId, String json) {
        kafkaTemplate.send("orders", orderId, json);
    }

    @KafkaListener(topics = "orders", groupId = "inventory-group")
    public void listen(String message) {
        System.out.println("Processing order: " + message);
    }
}
```

---

### 3.5 HTTP REST API & cURL (No Kafka SDK needed)

For serverless functions or lightweight scripts, ingest and retrieve directly via HTTP:

#### Produce a Record
```bash
curl -X POST http://localhost:9001/api/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "iot-sensor",
    "partition": 0,
    "message": "{\"voltage\": 12.6, \"uptime\": 86400}"
  }'
```

#### Fetch Records
```bash
curl "http://localhost:9001/api/messages?topic=iot-sensor&partition=0&offset=0&limit=10"
```

---

## 4. Docker Compose Setup

For team projects or local multi-service testing, define `docker-compose.yml`:

```yaml
version: '3.8'

services:
  aerostream:
    image: quay.io/gradientgeeks/aerostream:latest
    container_name: aerostream
    ports:
      - "9092:9092"   # Kafka Wire Protocol
      - "9091:9091"   # Native High-Throughput Data Plane
      - "9001:9001"   # Web Console & HTTP Management
      - "8001:8001"   # Controller gRPC
      - "7001:7001"   # Raft Consensus
    environment:
      - NODE_ID=node1
      - DATA_DIR=/data
    volumes:
      - aerostream_storage:/data
    restart: unless-stopped

volumes:
  aerostream_storage:
```

Start the service:
```bash
docker compose up -d
```

Stop the service:
```bash
docker compose down
```

---

## 5. Useful Administrative Commands

### Create a Topic via REST
```bash
curl -X POST http://localhost:9001/api/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "payment-events",
    "partitions": 3,
    "replication_factor": 1,
    "cleanup_policy": "delete",
    "retention_period": "7d"
  }'
```

### List Topics
```bash
curl -s http://localhost:9001/api/topics | jq
```

### Register an Avro Schema (Schema Registry)
```bash
curl -X POST http://localhost:9001/subjects/payment-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
  }'
```
