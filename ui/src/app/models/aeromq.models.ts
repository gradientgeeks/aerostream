export interface Broker {
  id: number;
  host: string;
  port: number;
  active: boolean;
  last_seen: number; // Unix timestamp in seconds
}

export interface ClusterInfo {
  node_id: string;
  raft_state: string; // 'Leader' | 'Follower' | 'Candidate'
  raft_leader: string;
  brokers_count: number;
  topics_count: number;
  groups_count: number;
  brokers: Broker[];
}

export interface Partition {
  partition_id: number;
  leader_id: number;
  replica_ids: number[];
  isr: number[];
  high_watermark: number;
  replica_offsets?: Record<string, number>;
}

export interface Topic {
  name: string;
  partitions: Partition[];
}

export interface ConsumerGroupMember {
  id: string;
  topics: string[];
  last_seen: number;
  assignments?: Record<string, number[]>;
}

export interface ConsumerGroup {
  group_id: string;
  generation: number;
  members: ConsumerGroupMember[];
  assignments?: Record<string, Record<string, number[]>>;
}

export interface PartitionLag {
  group_id: string;
  topic: string;
  partition: number;
  committed_offset: number;
  high_watermark: number;
  lag: number;
}

export interface ProduceRequest {
  topic: string;
  partition: number;
  message: string;
}

export interface ProduceResponse {
  success: boolean;
  topic: string;
  partition: number;
  offset: number;
}

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

export interface CreateTopicRequest {
  name: string;
  partitions: number;
  replication_factor?: number;
}
