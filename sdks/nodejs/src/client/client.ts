import { AeroConnection } from '../transport/connection.js';
import { AeroProducer } from './producer.js';
import { AeroConsumer } from './consumer.js';
import { AeroClientOptions, ProducerOptions, ConsumerOptions } from './options.js';
import { AeroRecord } from '../protocol/types.js';

export class AeroClient {
  private readonly connection: AeroConnection;

  constructor(private readonly options: AeroClientOptions = {}) {
    this.connection = new AeroConnection(options);
  }

  public static async connect(
    endpointOrOptions: string | AeroClientOptions = '127.0.0.1:9091',
    authToken?: string
  ): Promise<AeroClient> {
    let opts: AeroClientOptions;
    if (typeof endpointOrOptions === 'string') {
      const [host, portStr] = endpointOrOptions.split(':');
      opts = {
        host: host || '127.0.0.1',
        port: portStr ? parseInt(portStr, 10) : 9091,
        authToken,
      };
    } else {
      opts = endpointOrOptions;
    }

    const client = new AeroClient(opts);
    await client.connection.connect();
    return client;
  }

  public producer(options?: ProducerOptions): AeroProducer {
    return new AeroProducer(this.connection, options);
  }

  public consumer(options?: ConsumerOptions): AeroConsumer {
    return new AeroConsumer(this.connection, options);
  }

  public async produce(
    topic: string,
    partition: number,
    payload: Uint8Array | string
  ): Promise<bigint> {
    const rawPayload = typeof payload === 'string' ? Buffer.from(payload, 'utf-8') : payload;
    return await this.connection.produce(topic, partition, rawPayload);
  }

  public async fetch(
    topic: string,
    partition: number,
    startOffset: bigint,
    maxBytes = 1024 * 1024,
    maxWaitMs = 500
  ): Promise<AeroRecord[]> {
    return await this.connection.fetchMulti(topic, partition, startOffset, maxBytes, maxWaitMs);
  }

  public async close(): Promise<void> {
    await this.connection.close();
  }
}
