export interface MessageRecord {
  offset: number;
  payload: string;
  length: number;
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
