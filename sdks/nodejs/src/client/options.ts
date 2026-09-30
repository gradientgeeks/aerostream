import * as tls from 'node:tls';

export interface AeroClientOptions {
  bootstrapServers?: string[];
  host?: string;
  port?: number;
  authToken?: string;
  connectTimeoutMs?: number;
  requestTimeoutMs?: number;
  maxRetries?: number;
  initialRetryBackoffMs?: number;
  maxRetryBackoffMs?: number;
  tlsOptions?: tls.ConnectionOptions;
}

export interface ProducerOptions {
  maxInFlight?: number;
  maxRetries?: number;
  retryBackoffMs?: number;
}

export interface ConsumerOptions {
  maxBytes?: number;
  maxWaitMs?: number;
  pollIntervalMs?: number;
}
