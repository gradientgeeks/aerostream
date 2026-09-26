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
}

export interface CreateTopicRequest {
  name: string;
  partitions: number;
  replication_factor: number;
}

export interface CreateTopicResponse {
  success: boolean;
  message: string;
}
