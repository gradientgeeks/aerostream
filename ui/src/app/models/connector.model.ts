export type ConnectorType = 'SOURCE' | 'SINK';
export type ConnectorState = 'RUNNING' | 'PAUSED' | 'FAILED';

export interface Connector {
  name: string;
  type: ConnectorType;
  class: string;
  topic: string;
  config: Record<string, string>;
  state: ConnectorState;
  tasks_count: number;
  records_processed: number;
  bytes_transferred: number;
  last_error?: string;
  created_at: string;
}

export interface ConnectorPlugin {
  class: string;
  type: ConnectorType;
  version: string;
  description: string;
}

export interface ConnectorStats {
  totalConnectors: number;
  activeSources: number;
  activeSinks: number;
  recordsStreamed: number;
}

export interface CreateConnectorRequest {
  name: string;
  type?: ConnectorType;
  class: string;
  topic: string;
  config?: Record<string, string>;
  tasks_count?: number;
}
