# AeroStream Node.js / TypeScript Client SDK

Official production-grade Node.js and TypeScript client SDK for **AeroStream**'s ultra-low-latency native binary protocol (`0xAE 0x01` on TCP port `9091`).

---

## 📦 Installation

```bash
npm install @gradientgeeks/aerostream-client
```

---

## 🚀 Quick Start

### Producer & Consumer

```typescript
import { AeroClient } from '@gradientgeeks/aerostream-client';

async function run() {
  // Connect to native TCP port 9091
  const client = await AeroClient.connect('127.0.0.1:9091', 'secret-auth-token');

  const producer = client.producer({ maxInFlight: 256 });
  const consumer = client.consumer();

  // 1. Produce
  const offset = await producer.send('orders', 0, 'order-payload');
  console.log(`Produced record at offset: ${offset}`);

  // 2. Fetch Batch
  const records = await consumer.fetch('orders', 0, 0n);
  for (const record of records) {
    console.log(`Record [${record.offset}]:`, record.payload.toString('utf-8'));
  }

  // 3. Continuous Reactive Streaming
  const controller = new AbortController();
  for await (const record of consumer.stream('orders', 0, { signal: controller.signal })) {
    console.log(`Streamed: [${record.offset}] ${record.payload.toString('utf-8')}`);
  }

  await client.close();
}

run().catch(console.error);
```

---

## ⚡ Architecture & Features

- **Zero-Allocation Binary Framing**: Custom Big-Endian buffer encoders and decoders for Command 0 (Auth), Command 1 (Produce), Command 2 (Fetch), and Command 4 (Fetch Multi).
- **Socket Transport**: Native Node.js `net.Socket` and `tls.TLSSocket` with `setNoDelay(true)` and `setKeepAlive(true)` for sub-millisecond latencies.
- **Auto-Reconnection**: Resilient error handling that transparently reconnects, re-authenticates token, and retries with jittered exponential backoff.
- **Async Iteration (`AsyncGenerator`)**: Continuous stream consumption with `for await (const record of consumer.stream())` and standard `AbortSignal` cancellation.
- **Concurrency Throttling**: Semaphore-based bounded in-flight requests to eliminate bufferbloat and backpressure overload.

---

## 🧪 Testing

```bash
npm test
```

Runs unit tests and in-process mock broker integration tests:
- Framing and magic byte validation
- Authentication handshake
- High-throughput produce and batching
- Multi-entry long polling fetch
- Streaming with AsyncGenerator
- Bounded concurrency backpressure
- Auto-reconnection upon socket disconnection

---

## 📄 License

Licensed under the Apache License, Version 2.0.
