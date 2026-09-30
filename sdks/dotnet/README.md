# AeroStream .NET Client SDK (`GradientGeeks.AeroStream.Client`)

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![NuGet](https://img.shields.io/nuget/v/GradientGeeks.AeroStream.Client.svg)](https://www.nuget.org/packages/GradientGeeks.AeroStream.Client)
[![Framework](https://img.shields.io/badge/.NET-8.0-purple.svg)](https://dotnet.microsoft.com/)
[![Protocol](https://img.shields.io/badge/protocol-AeroStream_Native_0xAE01-black)](https://aerostream.gradientgeeks.com/docs/)

Official production-grade .NET C# client SDK for **AeroStream**'s ultra-low-latency native binary protocol (`0xAE 0x01` on TCP port `9091`).

---

## ⚡ Key Features

- **Zero-Allocation Binary Framing**: Custom codec using `Span<byte>`, `ReadOnlySpan<byte>`, and `BinaryPrimitives` for Big-Endian wire parsing.
- **High-Performance Async Sockets**: Direct `Socket` transport with `NoDelay = true`, OS-level KeepAlive, and half-duplex transaction serialization.
- **Resilient Auto-Reconnection**: Transparent connection recovery across bootstrap servers with exponential backoff and randomized full jitter.
- **Producer Backpressure**: Configurable `SemaphoreSlim` bounding maximum in-flight requests and preventing buffer bloat under burst loads.
- **Modern C# Streaming**: First-class support for `IAsyncEnumerable<AeroRecord>` streaming with automatic offset tracking and long-polling (`Command 4`).
- **TLS Encryption**: Native `SslStream` support for secure in-transit communication with optional custom certificate validation.

---

## 📦 Installation

Add the package via NuGet CLI:

```bash
dotnet add package GradientGeeks.AeroStream.Client
```

Or via the .NET CLI:

```xml
<PackageReference Include="GradientGeeks.AeroStream.Client" Version="0.1.0-preview" />
```

---

## 🚀 Quickstarts

### 1. Initializing Client

```csharp
using GradientGeeks.AeroStream.Client.Client;

var options = new AeroClientOptions
{
    BootstrapServers = new[] { "127.0.0.1:9091" },
    Token = "your-secret-token",
    RequestTimeout = TimeSpan.FromSeconds(5)
};

await using var client = new AeroClient(options);
```

### 2. Publishing Messages (Producer)

```csharp
using var producer = client.CreateProducer();

// Send string payload
long offset = await producer.SendAsync(
    topic: "telemetry",
    partition: 0,
    payload: "{\"sensor\": \"temp-1\", \"reading\": 24.5}"
);

Console.WriteLine($"Message published at log offset {offset}");

// Send raw byte memory with cancellation
byte[] rawBytes = new byte[] { 0x01, 0x02, 0x03, 0x04 };
long binaryOffset = await producer.SendAsync("telemetry", 0, rawBytes);
```

### 3. Continuous Asynchronous Streaming (`IAsyncEnumerable`)

```csharp
using var consumer = client.CreateConsumer();
using var cts = new CancellationTokenSource();

// Stream records continuously starting from offset 0
await foreach (AeroRecord record in consumer.StreamAsync("telemetry", partition: 0, startOffset: 0, cts.Token))
{
    Console.WriteLine($"[Offset: {record.Offset}] Payload: {record.GetPayloadString()}");
}
```

### 4. Batch Long-Polling Fetch

```csharp
using var consumer = client.CreateConsumer();

IReadOnlyList<AeroRecord> batch = await consumer.FetchMultiAsync(
    topic: "telemetry",
    partition: 0,
    startOffset: 0,
    maxBytes: 1048576,
    maxWaitMs: 500
);

foreach (var record in batch)
{
    Console.WriteLine($"Record at offset {record.Offset}: {record.Payload.Length} bytes");
}
```

---

## 📐 Protocol Framing Reference (`0xAE 0x01`)

```
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
| Magic Bytes (2B)    | Command (1B) | Body Length (4B BE)  | Variable Payload (N B)   |
| 0xAE 0x01           | uint8 (0..4) | uint32               | Command-Specific Bytes   |
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
```

| Command | Name | Request Body | Response Layout |
| :--- | :--- | :--- | :--- |
| **0** | **Auth Handshake** | `[token: UTF-8]` | `[0xAE, 0x01, status: u8]` (0 = Ok, 3 = Auth Failed) |
| **1** | **Produce / Append** | `[topic_len: u16][topic: UTF-8][partition: u32][payload_len: u32][payload: bytes]` | `[0xAE, 0x01, status: u8][offset: u64 BE]` (45 = OutOfOrder) |
| **2** | **Consumer Fetch** | `[topic_len: u16][topic: UTF-8][partition: u32][start_offset: u64][max_bytes: u32]` | `[0xAE, 0x01, 0x02][bytes_to_read: u32][payload]` or `[0xAE, 0x01, 0x01]` (Empty) |
| **4** | **Multi-Entry Long-Poll**| `[topic_len: u16][topic: UTF-8][partition: u32][start_offset: u64][max_bytes: u32][max_wait_ms: u32]` | `[0xAE, 0x01, 0x02][entry_count: u32][(offset: u64, len: u32)...][payloads]` |

---

## 🧪 Testing & Verification

Run the test suite using Docker:

```bash
docker run --rm -v $(pwd):/src -w /src mcr.microsoft.com/dotnet/sdk:8.0-alpine dotnet test
```

---

## 📄 License

Licensed under the Apache License, Version 2.0 ([LICENSE](../../LICENSE)).
