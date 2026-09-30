# AeroStream Java Client SDK

Official production-grade Java Client SDK for the **AeroStream** native binary protocol (`0xAE 0x01`).

[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Overview

The AeroStream Java SDK provides a high-throughput, low-latency client implementation for interacting with AeroStream brokers over TCP. Built with pure Java NIO, zero unnecessary runtime dependencies, and thread-safe abstractions, the SDK supports synchronous and asynchronous publishing, continuous record streaming, and automated fault-tolerant reconnection.

### Key Features

- **Native Binary Framing (`0xAE 0x01`)**: Zero-copy `ByteBuffer` codec for protocol commands 0 (Auth), 1 (Produce), 2 (Fetch), and 4 (MultiFetch).
- **Thread-Safe Transport**: Persistent socket transport with `TCP_NODELAY`, keepalive, and re-entrant request synchronization.
- **Resilience & Auto-Reconnect**: Automatic reconnect with exponential backoff and randomized jitter upon connection failure.
- **Asynchronous & Backpressure Control**: `CompletableFuture`-driven non-blocking publish guarded by configurable semaphore in-flight rate limiting.
- **High-Level Consumer API**: Position-based fetch, seek operations, and continuous lazy `java.util.stream.Stream<AeroRecord>`.
- **TLS / SSL Support**: Secure communications using standard `SSLContext` / `SSLSocketFactory`.

---

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>org.gradientgeeks.aerostream</groupId>
    <artifactId>aerostream-client</artifactId>
    <version>0.1.0-preview</version>
</dependency>
```

### Gradle

```groovy
implementation 'org.gradientgeeks.aerostream:aerostream-client:0.1.0-preview'
```

---

## Protocol Framing

AeroStream communicates over TCP (default port 9091) using binary framing:

```
+-------------------+-----------------+-----------------------+------------------------+
| Magic (2 Bytes)   | Command (1 Byte)| Body Length (4 Bytes) | Body (Variable Bytes)  |
| 0xAE, 0x01        | u8              | u32 Big-Endian        |                        |
+-------------------+-----------------+-----------------------+------------------------+
```

### Supported Commands

| ID | Command | Direction | Description |
|---|---|---|---|
| `0` | `CMD_AUTH` | Client &rarr; Broker | Token authentication handshake |
| `1` | `CMD_PRODUCE` | Client &rarr; Broker | Publish record to partition |
| `2` | `CMD_FETCH` | Client &rarr; Broker | Single-range data fetch |
| `4` | `CMD_FETCH_MULTI` | Client &rarr; Broker | Multi-entry long-polling fetch |

---

## Quickstart

### 1. Initializing the Client

```java
import org.gradientgeeks.aerostream.client.AeroClient;
import java.time.Duration;

AeroClient client = AeroClient.builder()
        .bootstrapServer("127.0.0.1:9091")
        .token("optional-auth-token")
        .connectTimeout(Duration.ofSeconds(5))
        .socketTimeout(Duration.ofSeconds(30))
        .build();
```

### 2. Publishing Messages (Producer)

#### Synchronous Send
```java
import org.gradientgeeks.aerostream.client.AeroProducer;
import java.nio.charset.StandardCharsets;

try (AeroProducer producer = client.producer()) {
    byte[] payload = "sensor-data-reading".getBytes(StandardCharsets.UTF_8);
    long offset = producer.send("sensors", 0, payload);
    System.out.println("Message committed at offset: " + offset);
}
```

#### Asynchronous Send with Backpressure
```java
CompletableFuture<Long> future = producer.sendAsync("sensors", 0, payload);
future.thenAccept(offset -> System.out.println("Async offset: " + offset))
      .exceptionally(ex -> {
          System.err.println("Produce failed: " + ex.getMessage());
          return null;
      });
```

#### Batch Send
```java
List<byte[]> batch = List.of(
    "payload-1".getBytes(StandardCharsets.UTF_8),
    "payload-2".getBytes(StandardCharsets.UTF_8)
);
long[] offsets = producer.sendBatch("sensors", 0, batch);
```

### 3. Consuming Messages (Consumer)

#### Polling Batch Fetch
```java
import org.gradientgeeks.aerostream.client.AeroConsumer;
import org.gradientgeeks.aerostream.common.AeroRecord;
import java.time.Duration;
import java.util.List;

try (AeroConsumer consumer = client.consumerBuilder()
        .topic("sensors")
        .partition(0)
        .initialOffset(0)
        .maxBytes(1048576) // 1 MB
        .maxWait(Duration.ofMillis(200))
        .build()) {

    List<AeroRecord> records = consumer.fetch();
    for (AeroRecord record : records) {
        System.out.printf("Offset %d: %s%n", record.offset(), record.payloadAsString());
    }
}
```

#### Continuous Stream Processing
```java
consumer.seek(0);
consumer.stream()
        .filter(record -> record.payload().length > 0)
        .forEach(record -> {
            System.out.printf("Streamed Record [%d]: %s%n", record.offset(), record.payloadAsString());
        });
```

---

## Error Handling

The client SDK maps protocol errors and network faults into clean Java exception hierarchies:

- **`AeroException`**: Base unchecked runtime exception.
- **`AuthenticationException`**: Broker rejected authentication token (`status 3`).
- **`OutOfOrderException`**: Partition sequence mismatch / idempotency check failed (`status 45`).
- **`ConnectionException`**: Network or socket I/O failure after retries exhausted.
- **`TimeoutException`**: Operation deadline exceeded.

```java
try {
    producer.send("audit-log", 0, payload);
} catch (AuthenticationException e) {
    System.err.println("Invalid credentials: " + e.getMessage());
} catch (OutOfOrderException e) {
    System.err.println("Sequence broken, resync required: " + e.getMessage());
} catch (ConnectionException e) {
    System.err.println("Broker unreachable: " + e.getMessage());
}
```

---

## Configuration Reference

### Client Configuration (`AeroClientBuilder`)

| Setting | Default | Description |
|---|---|---|
| `bootstrapServer(String)` | Required | Host and port of initial broker |
| `token(String)` | `null` | Authentication handshake secret |
| `tls(boolean)` | `false` | Enable TLS encrypted transport |
| `sslContext(SSLContext)` | `null` | Custom SSLContext for mTLS |
| `connectTimeout(Duration)` | `5s` | Socket connection timeout |
| `socketTimeout(Duration)` | `30s` | Read/write socket timeout |
| `maxReconnectAttempts(int)` | `5` | Max reconnection retries |
| `initialBackoff(Duration)` | `50ms` | Initial retry backoff interval |

### Producer Configuration (`ProducerBuilder`)

| Setting | Default | Description |
|---|---|---|
| `maxInFlight(int)` | `256` | Max concurrent in-flight requests semaphore |
| `maxRetries(int)` | `3` | Retries on transient connection errors |
| `retryBackoff(Duration)` | `50ms` | Base backoff delay for produce retries |
| `executor(ExecutorService)` | Internal Pool | Custom thread pool for async dispatches |

### Consumer Configuration (`ConsumerBuilder`)

| Setting | Default | Description |
|---|---|---|
| `topic(String)` | Required | Target topic name |
| `partition(int)` | `0` | Target partition ID |
| `initialOffset(long)` | `0` | Starting commit log offset |
| `maxBytes(int)` | `1048576` (1MB) | Maximum payload byte size to retrieve |
| `maxWait(Duration)` | `100ms` | Long-polling wait duration |

---

## Building & Testing

Requires JDK 17+ and Maven 3.8+.

```bash
cd sdks/java
mvn clean test
```

Run test suite with in-process mock broker:
```bash
mvn test -Dtest=MockBrokerIntegrationTest
```
