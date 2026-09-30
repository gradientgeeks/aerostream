import { test, describe, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as net from 'node:net';
import { AeroClient } from '../src/client/client.js';
import { AuthenticationError } from '../src/errors.js';
import {
  MAGIC_0,
  MAGIC_1,
  CMD_AUTH,
  CMD_PRODUCE,
  CMD_FETCH,
  CMD_FETCH_MULTI,
  STATUS_OK,
  STATUS_EMPTY,
  STATUS_DATA,
  STATUS_AUTH_FAILED,
} from '../src/protocol/constants.js';

interface StoredRecord {
  offset: bigint;
  payload: Buffer;
}

class MockAeroBroker {
  private server: net.Server;
  public port = 0;
  private storage: Map<string, StoredRecord[]> = new Map();
  private nextOffset: Map<string, bigint> = new Map();
  private activeSockets: Set<net.Socket> = new Set();

  constructor() {
    this.server = net.createServer((socket) => {
      this.activeSockets.add(socket);
      socket.on('close', () => this.activeSockets.delete(socket));
      let buffer = Buffer.alloc(0);
      let authenticated = false;

      socket.on('data', (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);

        while (buffer.length >= 7) {
          if (buffer[0] !== MAGIC_0 || buffer[1] !== MAGIC_1) {
            socket.destroy();
            return;
          }

          const cmd = buffer[2];
          const bodyLen = buffer.readUInt32BE(3);
          const totalLen = 7 + bodyLen;

          if (buffer.length < totalLen) {
            break; // Wait for full frame
          }

          const body = buffer.subarray(7, totalLen);
          buffer = buffer.subarray(totalLen);

          this.handleCommand(socket, cmd, body, (auth) => {
            authenticated = auth;
          }, authenticated);
        }
      });
    });
  }

  private handleCommand(
    socket: net.Socket,
    cmd: number,
    body: Buffer,
    setAuth: (val: boolean) => void,
    authenticated: boolean
  ) {
    if (cmd === CMD_AUTH) {
      const token = body.toString('utf-8');
      if (token === 'valid-secret') {
        setAuth(true);
        socket.write(Buffer.from([MAGIC_0, MAGIC_1, STATUS_OK]));
      } else {
        socket.write(Buffer.from([MAGIC_0, MAGIC_1, STATUS_AUTH_FAILED]));
      }
      return;
    }

    if (cmd === CMD_PRODUCE) {
      let cursor = 0;
      const topicLen = body.readUInt16BE(cursor);
      cursor += 2;
      const topic = body.subarray(cursor, cursor + topicLen).toString('utf-8');
      cursor += topicLen;
      const partition = body.readUInt32BE(cursor);
      cursor += 4;
      const payloadLen = body.readUInt32BE(cursor);
      cursor += 4;
      const payload = Buffer.from(body.subarray(cursor, cursor + payloadLen));

      const key = `${topic}-${partition}`;
      if (!this.storage.has(key)) {
        this.storage.set(key, []);
        this.nextOffset.set(key, 0n);
      }

      const offset = this.nextOffset.get(key)!;
      this.nextOffset.set(key, offset + 1n);
      this.storage.get(key)!.push({ offset, payload });

      const resp = Buffer.alloc(11);
      resp[0] = MAGIC_0;
      resp[1] = MAGIC_1;
      resp[2] = STATUS_OK;
      resp.writeBigUInt64BE(offset, 3);
      socket.write(resp);
      return;
    }

    if (cmd === CMD_FETCH_MULTI) {
      let cursor = 0;
      const topicLen = body.readUInt16BE(cursor);
      cursor += 2;
      const topic = body.subarray(cursor, cursor + topicLen).toString('utf-8');
      cursor += topicLen;
      const partition = body.readUInt32BE(cursor);
      cursor += 4;
      const startOffset = body.readBigUInt64BE(cursor);
      cursor += 8;
      const maxBytes = body.readUInt32BE(cursor);

      const key = `${topic}-${partition}`;
      const records = this.storage.get(key) ?? [];
      const matched = records.filter((r) => r.offset >= startOffset);

      if (matched.length === 0) {
        socket.write(Buffer.from([MAGIC_0, MAGIC_1, STATUS_EMPTY]));
        return;
      }

      // Collect entries up to maxBytes
      let bytesCount = 0;
      const entries: StoredRecord[] = [];
      for (const rec of matched) {
        if (bytesCount + rec.payload.length > maxBytes && entries.length > 0) break;
        entries.push(rec);
        bytesCount += rec.payload.length;
      }

      const respLen = 3 + 4 + entries.length * 12 + bytesCount;
      const resp = Buffer.alloc(respLen);
      resp[0] = MAGIC_0;
      resp[1] = MAGIC_1;
      resp[2] = STATUS_DATA;
      resp.writeUInt32BE(entries.length, 3);

      let tableCursor = 7;
      let payloadCursor = 7 + entries.length * 12;

      for (const rec of entries) {
        resp.writeBigUInt64BE(rec.offset, tableCursor);
        resp.writeUInt32BE(rec.payload.length, tableCursor + 8);
        tableCursor += 12;

        rec.payload.copy(resp, payloadCursor);
        payloadCursor += rec.payload.length;
      }

      socket.write(resp);
      return;
    }
  }

  public disconnectAllClients(): void {
    for (const s of this.activeSockets) {
      s.destroy();
    }
    this.activeSockets.clear();
  }

  public async start(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        const addr = this.server.address() as net.AddressInfo;
        this.port = addr.port;
        resolve(this.port);
      });
    });
  }

  public async stop(): Promise<void> {
    return new Promise((resolve) => {
      this.server.close(() => resolve());
    });
  }
}

describe('AeroStream Node.js Client E2E Integration Suite', () => {
  let broker: MockAeroBroker;
  let brokerPort: number;

  before(async () => {
    broker = new MockAeroBroker();
    brokerPort = await broker.start();
  });

  after(async () => {
    await broker.stop();
  });

  test('Authentication handshake validation', async () => {
    // Failing authentication
    await assert.rejects(
      async () => {
        const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'invalid-secret');
        await client.close();
      },
      AuthenticationError
    );

    // Successful authentication
    const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'valid-secret');
    assert.ok(client);
    await client.close();
  });

  test('End-to-end produce and multi-fetch', async () => {
    const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'valid-secret');
    const producer = client.producer();
    const consumer = client.consumer();

    const topic = 'orders';
    const partition = 0;

    const off0 = await producer.send(topic, partition, 'order-alpha');
    const off1 = await producer.send(topic, partition, 'order-beta');
    const off2 = await producer.send(topic, partition, 'order-gamma');

    assert.equal(off0, 0n);
    assert.equal(off1, 1n);
    assert.equal(off2, 2n);

    // Fetch from offset 0
    const records = await consumer.fetch(topic, partition, 0n);
    assert.equal(records.length, 3);
    assert.equal(records[0].payload.toString('utf-8'), 'order-alpha');
    assert.equal(records[1].payload.toString('utf-8'), 'order-beta');
    assert.equal(records[2].payload.toString('utf-8'), 'order-gamma');

    // Slice fetch from offset 1
    const slice = await consumer.fetch(topic, partition, 1n);
    assert.equal(slice.length, 2);
    assert.equal(slice[0].offset, 1n);
    assert.equal(slice[0].payload.toString('utf-8'), 'order-beta');

    await client.close();
  });

  test('Batch produce records', async () => {
    const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'valid-secret');
    const producer = client.producer();

    const offsets = await producer.sendBatch('batch-test', 0, [
      'item-1',
      'item-2',
      'item-3',
      'item-4',
    ]);

    assert.equal(offsets.length, 4);
    assert.equal(offsets[0], 0n);
    assert.equal(offsets[3], 3n);

    await client.close();
  });

  test('Continuous streaming with async generator and AbortSignal', async () => {
    const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'valid-secret');
    const producer = client.producer();
    const consumer = client.consumer({ pollIntervalMs: 10 });

    const topic = 'sensor-stream';
    const partition = 0;

    for (let i = 0; i < 5; i++) {
      await producer.send(topic, partition, `sensor-val-${i}`);
    }

    const controller = new AbortController();
    const collected: string[] = [];

    const streamPromise = (async () => {
      for await (const record of consumer.stream(topic, partition, {
        signal: controller.signal,
        startOffset: 0n,
      })) {
        collected.push(record.payload.toString('utf-8'));
        if (collected.length === 5) {
          controller.abort();
        }
      }
    })();

    await streamPromise;
    assert.equal(collected.length, 5);
    assert.equal(collected[0], 'sensor-val-0');
    assert.equal(collected[4], 'sensor-val-4');

    await client.close();
  });

  test('Concurrent produce with in-flight backpressure semaphore', async () => {
    const client = await AeroClient.connect(`127.0.0.1:${brokerPort}`, 'valid-secret');
    const producer = client.producer({ maxInFlight: 4 });

    const topic = 'concurrency-topic';
    const promises: Promise<bigint>[] = [];

    for (let i = 0; i < 16; i++) {
      promises.push(producer.send(topic, 0, `msg-${i}`));
    }

    const results = await Promise.all(promises);
    assert.equal(results.length, 16);
    assert.equal(results[0], 0n);
    assert.equal(results[15], 15n);

    await client.close();
  });

  test('Auto-reconnect on socket disconnect', async () => {
    const client = await AeroClient.connect({
      host: '127.0.0.1',
      port: brokerPort,
      authToken: 'valid-secret',
      maxRetries: 3,
      initialRetryBackoffMs: 20,
    });

    const producer = client.producer();
    const off1 = await producer.send('reconnect-topic', 0, 'before-disconnect');
    assert.equal(off1, 0n);

    // Drop connection from broker
    broker.disconnectAllClients();

    // Next produce should automatically reconnect and authenticate
    const off2 = await producer.send('reconnect-topic', 0, 'after-reconnect');
    assert.equal(off2, 1n);

    await client.close();
  });
});
