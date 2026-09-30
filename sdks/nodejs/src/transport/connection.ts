import * as net from 'node:net';
import * as tls from 'node:tls';
import {
  MAGIC_0,
  MAGIC_1,
  STATUS_OK,
  STATUS_EMPTY,
  STATUS_DATA,
  STATUS_AUTH_FAILED,
  STATUS_OUT_OF_ORDER,
} from '../protocol/constants.js';
import {
  encodeAuthRequest,
  encodeProduceRequest,
  encodeFetchRequest,
  encodeMultiFetchRequest,
} from '../protocol/frame.js';
import { AeroRecord } from '../protocol/types.js';
import {
  AeroError,
  AuthenticationError,
  OutOfOrderError,
  ConnectionError,
  TimeoutError,
} from '../errors.js';
import { AeroClientOptions } from '../client/options.js';

export class AeroConnection {
  private socket: net.Socket | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly authToken?: string;
  private readonly connectTimeoutMs: number;
  private readonly requestTimeoutMs: number;
  private readonly maxRetries: number;
  private readonly initialBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly tlsOptions?: tls.ConnectionOptions;

  private buffer: Buffer = Buffer.alloc(0);
  private closed = false;
  private mutex: Promise<unknown> = Promise.resolve();

  private pendingRead: {
    requiredBytes: number;
    resolve: (buf: Buffer) => void;
    reject: (err: Error) => void;
    timer?: NodeJS.Timeout;
  } | null = null;

  constructor(options: AeroClientOptions = {}) {
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 9091;
    this.authToken = options.authToken;
    this.connectTimeoutMs = options.connectTimeoutMs ?? 5000;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 10000;
    this.maxRetries = options.maxRetries ?? 3;
    this.initialBackoffMs = options.initialRetryBackoffMs ?? 50;
    this.maxBackoffMs = options.maxRetryBackoffMs ?? 1000;
    this.tlsOptions = options.tlsOptions;
  }

  public async connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) {
      return;
    }
    await this.establishSocket();
  }

  private async establishSocket(): Promise<void> {
    this.cleanupSocket();

    return new Promise((resolve, reject) => {
      let isResolved = false;
      const timeoutTimer = setTimeout(() => {
        if (!isResolved) {
          isResolved = true;
          this.cleanupSocket();
          reject(new TimeoutError(`Connection timed out after ${this.connectTimeoutMs}ms`));
        }
      }, this.connectTimeoutMs);

      const onConnect = async () => {
        if (isResolved) return;
        clearTimeout(timeoutTimer);
        socket.setNoDelay(true);
        socket.setKeepAlive(true, 10000);

        try {
          if (this.authToken) {
            await this.authenticateDirect();
          }
          isResolved = true;
          resolve();
        } catch (err) {
          isResolved = true;
          this.cleanupSocket();
          reject(err);
        }
      };

      const onError = (err: Error) => {
        if (!isResolved) {
          isResolved = true;
          clearTimeout(timeoutTimer);
          this.cleanupSocket();
          reject(new ConnectionError(`Socket error: ${err.message}`, err));
        } else {
          this.onSocketFailure(err);
        }
      };

      const socket: net.Socket = this.tlsOptions
        ? tls.connect({
            host: this.host,
            port: this.port,
            ...this.tlsOptions,
          }, onConnect)
        : net.createConnection({ host: this.host, port: this.port }, onConnect);

      this.socket = socket;

      socket.on('data', (data: Buffer) => {
        this.buffer = Buffer.concat([this.buffer, data]);
        this.checkPendingRead();
      });

      socket.on('error', onError);
      socket.on('close', () => {
        this.onSocketFailure(new ConnectionError('Socket closed unexpectedly'));
      });
    });
  }

  private checkPendingRead(): void {
    if (!this.pendingRead) return;
    if (this.buffer.length >= this.pendingRead.requiredBytes) {
      const { requiredBytes, resolve, timer } = this.pendingRead;
      if (timer) clearTimeout(timer);
      this.pendingRead = null;

      const chunk = this.buffer.subarray(0, requiredBytes);
      this.buffer = this.buffer.subarray(requiredBytes);
      resolve(chunk);
    }
  }

  private readBytes(length: number, timeoutMs = this.requestTimeoutMs): Promise<Buffer> {
    if (this.buffer.length >= length) {
      const chunk = this.buffer.subarray(0, length);
      this.buffer = this.buffer.subarray(length);
      return Promise.resolve(chunk);
    }

    if (this.pendingRead) {
      return Promise.reject(new AeroError('Concurrent readBytes call detected'));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingRead) {
          this.pendingRead = null;
          reject(new TimeoutError(`Read operation timed out after ${timeoutMs}ms`));
        }
      }, timeoutMs);

      this.pendingRead = {
        requiredBytes: length,
        resolve,
        reject,
        timer,
      };
    });
  }

  private writeBuffer(buf: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socket || this.socket.destroyed) {
        return reject(new ConnectionError('Socket is not connected'));
      }
      this.socket.write(buf, (err) => {
        if (err) reject(new ConnectionError(err.message, err));
        else resolve();
      });
    });
  }

  private async authenticateDirect(): Promise<void> {
    if (!this.authToken) return;
    const req = encodeAuthRequest(this.authToken);
    await this.writeBuffer(req);

    const prefix = await this.readBytes(3);
    if (prefix[0] !== MAGIC_0 || prefix[1] !== MAGIC_1) {
      throw new AeroError(`Invalid magic bytes in auth response: 0x${prefix[0].toString(16)} 0x${prefix[1].toString(16)}`);
    }

    const status = prefix[2];
    if (status === STATUS_AUTH_FAILED) {
      throw new AuthenticationError('Broker rejected client authentication token');
    }
    if (status !== STATUS_OK) {
      throw new AeroError(`Unexpected auth response status: ${status}`);
    }
  }

  private onSocketFailure(err: Error): void {
    if (this.pendingRead) {
      const { reject, timer } = this.pendingRead;
      if (timer) clearTimeout(timer);
      this.pendingRead = null;
      reject(err);
    }
    this.buffer = Buffer.alloc(0);
  }

  private cleanupSocket(): void {
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.buffer = Buffer.alloc(0);
    if (this.pendingRead) {
      const { reject, timer } = this.pendingRead;
      if (timer) clearTimeout(timer);
      this.pendingRead = null;
      reject(new ConnectionError('Socket closed'));
    }
  }

  public async executeWithLock<T>(fn: () => Promise<T>): Promise<T> {
    if (this.closed) {
      throw new AeroError('Connection is closed');
    }

    const next = this.mutex.then(async () => {
      return await this.executeWithRetry(fn);
    });

    this.mutex = next.catch(() => {});
    return next;
  }

  private async executeWithRetry<T>(fn: () => Promise<T>): Promise<T> {
    let attempt = 0;
    while (true) {
      try {
        if (!this.socket || this.socket.destroyed) {
          await this.establishSocket();
        }
        return await fn();
      } catch (err: unknown) {
        attempt++;
        if (
          err instanceof AuthenticationError ||
          err instanceof OutOfOrderError ||
          attempt > this.maxRetries ||
          this.closed
        ) {
          throw err;
        }

        const backoff = Math.min(
          this.initialBackoffMs * Math.pow(2, attempt - 1),
          this.maxBackoffMs
        );
        const jitter = Math.floor(Math.random() * (backoff / 2));
        await new Promise((r) => setTimeout(r, backoff + jitter));
        this.cleanupSocket();
      }
    }
  }

  public async produce(topic: string, partition: number, payload: Uint8Array): Promise<bigint> {
    return this.executeWithLock(async () => {
      const req = encodeProduceRequest(topic, partition, payload);
      await this.writeBuffer(req);

      const prefix = await this.readBytes(3);
      if (prefix[0] !== MAGIC_0 || prefix[1] !== MAGIC_1) {
        throw new AeroError(`Invalid magic bytes: 0x${prefix[0].toString(16)} 0x${prefix[1].toString(16)}`);
      }

      const status = prefix[2];
      if (status === STATUS_OK) {
        const offsetBuf = await this.readBytes(8);
        return offsetBuf.readBigUInt64BE(0);
      } else if (status === STATUS_AUTH_FAILED) {
        throw new AuthenticationError();
      } else if (status === STATUS_OUT_OF_ORDER) {
        throw new OutOfOrderError();
      } else {
        throw new AeroError(`Produce rejected with status: ${status}`);
      }
    });
  }

  public async fetch(
    topic: string,
    partition: number,
    startOffset: bigint,
    maxBytes: number
  ): Promise<Buffer | null> {
    return this.executeWithLock(async () => {
      const req = encodeFetchRequest(topic, partition, startOffset, maxBytes);
      await this.writeBuffer(req);

      const prefix = await this.readBytes(3);
      if (prefix[0] !== MAGIC_0 || prefix[1] !== MAGIC_1) {
        throw new AeroError(`Invalid magic bytes: 0x${prefix[0].toString(16)} 0x${prefix[1].toString(16)}`);
      }

      const status = prefix[2];
      if (status === STATUS_EMPTY) {
        return null;
      } else if (status === STATUS_DATA) {
        const lenBuf = await this.readBytes(4);
        const bytesToRead = lenBuf.readUInt32BE(0);
        return await this.readBytes(bytesToRead);
      } else if (status === STATUS_AUTH_FAILED) {
        throw new AuthenticationError();
      } else {
        throw new AeroError(`Fetch rejected with status: ${status}`);
      }
    });
  }

  public async fetchMulti(
    topic: string,
    partition: number,
    startOffset: bigint,
    maxBytes: number,
    maxWaitMs: number
  ): Promise<AeroRecord[]> {
    return this.executeWithLock(async () => {
      const req = encodeMultiFetchRequest(topic, partition, startOffset, maxBytes, maxWaitMs);
      await this.writeBuffer(req);

      const prefix = await this.readBytes(3);
      if (prefix[0] !== MAGIC_0 || prefix[1] !== MAGIC_1) {
        throw new AeroError(`Invalid magic bytes: 0x${prefix[0].toString(16)} 0x${prefix[1].toString(16)}`);
      }

      const status = prefix[2];
      if (status === STATUS_EMPTY) {
        return [];
      } else if (status === STATUS_DATA) {
        const countBuf = await this.readBytes(4);
        const entryCount = countBuf.readUInt32BE(0);
        if (entryCount === 0) return [];

        const indexBuf = await this.readBytes(entryCount * 12);
        const entries: { offset: bigint; len: number }[] = [];
        let totalPayloadLen = 0;

        for (let i = 0; i < entryCount; i++) {
          const offset = indexBuf.readBigUInt64BE(i * 12);
          const len = indexBuf.readUInt32BE(i * 12 + 8);
          entries.push({ offset, len });
          totalPayloadLen += len;
        }

        const payloadBuf = await this.readBytes(totalPayloadLen);
        const records: AeroRecord[] = [];
        let cursor = 0;
        const now = new Date();

        for (const entry of entries) {
          const payload = Buffer.from(payloadBuf.subarray(cursor, cursor + entry.len));
          cursor += entry.len;
          records.push({
            offset: entry.offset,
            payload,
            topic,
            partition,
            timestamp: now,
          });
        }

        return records;
      } else if (status === STATUS_AUTH_FAILED) {
        throw new AuthenticationError();
      } else {
        throw new AeroError(`MultiFetch rejected with status: ${status}`);
      }
    });
  }

  public async close(): Promise<void> {
    this.closed = true;
    this.cleanupSocket();
  }
}
