export interface AeroRecord {
  offset: bigint;
  payload: Buffer;
  topic: string;
  partition: number;
  timestamp: Date;
}

export interface ProduceRequest {
  topic: string;
  partition: number;
  payload: Uint8Array;
}

export interface ProduceResponse {
  status: number;
  offset: bigint;
}

export interface FetchRequest {
  topic: string;
  partition: number;
  startOffset: bigint;
  maxBytes: number;
  maxWaitMs?: number;
}

export interface MultiFetchResponse {
  status: number;
  records: AeroRecord[];
}
