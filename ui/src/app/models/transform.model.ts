export type TransformType = 'WASM' | 'FILTER' | 'MASK_PII' | 'JSON_MAP';
export type TransformStatus = 'RUNNING' | 'PAUSED';

export interface StreamTransform {
  id: string;
  name: string;
  source_topic: string;
  target_topic: string;
  type: TransformType;
  config: Record<string, string>;
  code?: string;
  status: TransformStatus;
  messages_processed: number;
  messages_filtered: number;
  created_at: string;
}

export interface RegisterTransformRequest {
  name: string;
  source_topic: string;
  target_topic: string;
  type: TransformType;
  config?: Record<string, string>;
  code?: string;
}

export interface TestTransformRequest {
  transform_name?: string;
  transform?: Partial<StreamTransform>;
  payload: any;
}

export interface TestTransformResponse {
  status: string;
  drop: boolean;
  output: string;
  latency_ms: number;
}

export interface TransformMetrics {
  activeTransforms: number;
  filteredRecords: number;
  piiMaskedStreams: number;
  avgLatencyMs: number;
}
