# AeroStream Example Applications

This directory contains production-ready example applications demonstrating how to build streaming and event-driven applications with **AeroStream** across multiple programming languages and frameworks.

Because AeroStream natively implements the **Apache Kafka Wire Protocol** alongside its own ultra-low-latency **Zero-Copy Binary Protocol**, you can connect using standard ecosystem client libraries as well as zero-overhead native sockets.

---

## Available Examples

| Application | Language / Platform | Client / Library | Protocol Used | Key Features |
| :--- | :--- | :--- | :--- | :--- |
| [**Java App**](file:///home/uttam/projects/AeroMQ/examples/java-app) | Java 17+ / Maven | `org.apache.kafka:kafka-clients` | Kafka Wire (`:9093`) | AdminClient, KafkaProducer, KafkaConsumer with consumer groups |
| [**Go App**](file:///home/uttam/projects/AeroMQ/examples/go-app) | Go 1.24+ | Pure Go (Zero-CGO) | Kafka Wire (`:9093`) & Native (`:9091`) | ApiVersions, Metadata, CRC32 Produce/Fetch, Native binary streaming |
| [**Rust App**](file:///home/uttam/projects/AeroMQ/examples/rust-app) | Rust 1.80+ / Tokio | Asynchronous Rust | Kafka Wire (`:9093`) & Native (`:9091`) | Tokio async framing, ApiVersions, Metadata, Produce, Fetch, Native ACK |
| [**.NET App**](file:///home/uttam/projects/AeroMQ/examples/dotnet-app) | C# / .NET 8.0 | `Confluent.Kafka` | Kafka Wire (`:9093`) | Enterprise event streaming in C# |
| [**FastAPI App**](file:///home/uttam/projects/AeroMQ/examples/fastapi-app) | Python 3.10+ | `confluent-kafka` | Kafka Wire (`:9093`) | Webhooks & HTTP-to-Kafka ingestion gateway |

---

## Quick Start: Testing the Examples

Make sure your AeroStream cluster is running:
```bash
make start
```

### 1. Java Application
```bash
cd examples/java-app
mvn compile exec:java
```

### 2. Go Application
```bash
cd examples/go-app
go run main.go
```

### 3. Rust Application
```bash
cd examples/rust-app
cargo run
```
