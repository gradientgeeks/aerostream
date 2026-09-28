export interface MessageRecord {
  offset: number;
  payload: string;
  length: number;
  key?: string;
  timestamp?: number;
  headers?: Record<string, string>;
}

export interface FetchMessagesResponse {
  topic: string;
  partition: number;
  messages: MessageRecord[];
  count: number;
}

export interface FetchMessagesParams {
  topic: string;
  partition: number;
  offset?: number;
  limit?: number;
}
