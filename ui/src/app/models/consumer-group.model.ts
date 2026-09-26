export interface PartitionAssignment {
  topic: string;
  partition: number;
}

export interface ConsumerGroupMember {
  id: string;
  topics: string[];
  last_seen: number | string | Date;
  client_host?: string;
  user_agent?: string;
  assigned_partitions?: PartitionAssignment[];
  revoking_partitions?: PartitionAssignment[];
  assignments?: Record<string, number[]>;
}

export interface ConsumerGroup {
  group_id: string;
  protocol?: string;
  state?: string;
  generation: number;
  leader_id?: string;
  rebalance_count?: number;
  last_rebalance_time?: string;
  members: ConsumerGroupMember[];
  assignments?: Record<string, any>;
}

export interface RebalanceResponse {
  success: boolean;
  message: string;
  group_id: string;
  protocol?: string;
  state?: string;
  generation?: number;
  leader_id?: string;
  rebalance_count?: number;
}
