export interface PartitionInfo {
  partition_id: number;
  leader_id: number;
  replica_ids: number[];
  isr: number[];
  high_watermark: number;
  replica_offsets?: { [brokerId: string]: number };
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
  partitionCount?: number;
  replicationFactor?: number;
  highWatermarkTotal?: number;
  cleanup_policy?: 'delete' | 'compact';
  retention_period?: string;
  retention_size?: string;
  segment_size?: string;
  tombstone_retention?: string;
  schema_subject?: string;
  schema_type?: string;
}

export interface CreateTopicRequest {
  name: string;
  partitions: number;
  replication_factor: number;
  cleanup_policy?: 'delete' | 'compact';
  retention_period?: string;
  retention_size?: string;
  segment_size?: string;
  tombstone_retention?: string;
}

export interface CreateTopicResponse {
  success: boolean;
  message: string;
}
