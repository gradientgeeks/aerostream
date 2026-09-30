import { AeroConnection } from '../transport/connection.js';
import { AeroRecord } from '../protocol/types.js';
import { ConsumerOptions } from './options.js';

export class AeroConsumer {
  private currentOffset = 0n;
  private readonly maxBytes: number;
  private readonly maxWaitMs: number;
  private readonly pollIntervalMs: number;

  constructor(
    private readonly connection: AeroConnection,
    options: ConsumerOptions = {}
  ) {
    this.maxBytes = options.maxBytes ?? 1024 * 1024;
    this.maxWaitMs = options.maxWaitMs ?? 500;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
  }

  public seek(offset: bigint): void {
    this.currentOffset = offset;
  }

  public position(): bigint {
    return this.currentOffset;
  }

  public async fetch(
    topic: string,
    partition: number,
    startOffset = this.currentOffset
  ): Promise<AeroRecord[]> {
    const records = await this.connection.fetchMulti(
      topic,
      partition,
      startOffset,
      this.maxBytes,
      this.maxWaitMs
    );

    if (records.length > 0) {
      const highest = records[records.length - 1].offset;
      this.currentOffset = highest + 1n;
    }

    return records;
  }

  public async *stream(
    topic: string,
    partition: number,
    options: { signal?: AbortSignal; startOffset?: bigint } = {}
  ): AsyncGenerator<AeroRecord, void, unknown> {
    if (options.startOffset !== undefined) {
      this.currentOffset = options.startOffset;
    }

    const signal = options.signal;

    while (!signal?.aborted) {
      const records = await this.fetch(topic, partition, this.currentOffset);

      if (records.length === 0) {
        if (signal?.aborted) break;
        await new Promise((r) => setTimeout(r, this.pollIntervalMs));
        continue;
      }

      for (const record of records) {
        if (signal?.aborted) return;
        yield record;
      }
    }
  }

  public async close(): Promise<void> {
    await this.connection.close();
  }
}
