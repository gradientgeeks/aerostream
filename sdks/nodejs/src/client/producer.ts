import { AeroConnection } from '../transport/connection.js';
import { ProducerOptions } from './options.js';

export class AeroProducer {
  private inFlight = 0;
  private readonly maxInFlight: number;
  private waitQueue: (() => void)[] = [];

  constructor(
    private readonly connection: AeroConnection,
    options: ProducerOptions = {}
  ) {
    this.maxInFlight = options.maxInFlight ?? 256;
  }

  private async acquireSemaphore(): Promise<void> {
    if (this.inFlight < this.maxInFlight) {
      this.inFlight++;
      return;
    }
    return new Promise<void>((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  private releaseSemaphore(): void {
    if (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift()!;
      next();
    } else {
      this.inFlight--;
    }
  }

  public async send(
    topic: string,
    partition: number,
    payload: Uint8Array | string
  ): Promise<bigint> {
    const rawPayload = typeof payload === 'string' ? Buffer.from(payload, 'utf-8') : payload;

    await this.acquireSemaphore();
    try {
      return await this.connection.produce(topic, partition, rawPayload);
    } finally {
      this.releaseSemaphore();
    }
  }

  public async sendBatch(
    topic: string,
    partition: number,
    payloads: (Uint8Array | string)[]
  ): Promise<bigint[]> {
    const results: bigint[] = [];
    for (const payload of payloads) {
      const offset = await this.send(topic, partition, payload);
      results.push(offset);
    }
    return results;
  }

  public async close(): Promise<void> {
    await this.connection.close();
  }
}
