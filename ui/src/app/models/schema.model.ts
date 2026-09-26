export type SchemaType = 'AVRO' | 'PROTOBUF' | 'JSON';
export type CompatibilityMode =
  | 'BACKWARD'
  | 'BACKWARD_TRANSITIVE'
  | 'FORWARD'
  | 'FORWARD_TRANSITIVE'
  | 'FULL'
  | 'FULL_TRANSITIVE'
  | 'NONE';

export interface RegisteredSchema {
  id: number;
  subject: string;
  version: number;
  type: SchemaType;
  schema: string;
  compatibility: CompatibilityMode;
  created_at: string;
  description?: string;
  topic?: string;
}

export interface RegisterSchemaRequest {
  subject: string;
  type: SchemaType;
  schema: string;
  compatibility?: CompatibilityMode;
  description?: string;
}

export interface SchemaRegistryStats {
  totalSchemas: number;
  totalSubjects: number;
  compatibilityMode: CompatibilityMode;
  avroCount: number;
  protobufCount: number;
  jsonCount: number;
}
