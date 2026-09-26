export type Role = 'SUPER_ADMIN' | 'OPERATOR' | 'PRODUCER' | 'CONSUMER' | 'AUDITOR';

export type ResourceType = 'TOPIC' | 'GROUP' | 'CLUSTER';

export type Operation = 'READ' | 'WRITE' | 'DESCRIBE' | 'ALTER' | 'ALL';

export type Permission = 'ALLOW' | 'DENY';

export interface User {
  username: string;
  role: Role;
  created_at?: string;
}

export interface AclRule {
  id: string;
  principal: string;
  resource_type: ResourceType;
  resource_name: string;
  operation: Operation;
  permission: Permission;
  created_at?: string;
}

export interface CreateAclRequest {
  principal: string;
  resource_type: ResourceType;
  resource_name: string;
  operation: Operation;
  permission: Permission;
}

export interface CreateUserRequest {
  username: string;
  role: Role;
}

export interface TestAclRequest {
  principal: string;
  resource_type: ResourceType;
  resource_name: string;
  operation: Operation;
}

export interface TestAclResponse {
  allowed: boolean;
  reason: string;
}

export interface AclStats {
  totalRules: number;
  activePrincipals: number;
  superAdmins: number;
  denyPolicies: number;
}
