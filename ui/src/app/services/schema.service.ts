import { Injectable, computed, signal } from '@angular/core';
import { Observable, of } from 'rxjs';
import {
  CompatibilityMode,
  RegisterSchemaRequest,
  RegisteredSchema,
  SchemaRegistryStats,
  SchemaType,
} from '../models/schema.model';

const STORAGE_KEY = 'aerostream_registered_schemas';

const INITIAL_SCHEMAS: RegisteredSchema[] = [
  {
    id: 1001,
    subject: 'orders-value',
    version: 2,
    type: 'AVRO',
    compatibility: 'BACKWARD',
    created_at: '2026-09-26T08:15:00Z',
    description: 'E-commerce order placement and lifecycle event schema with monetary amounts and status enums',
    topic: 'orders',
    schema: JSON.stringify(
      {
        type: 'record',
        name: 'OrderEvent',
        namespace: 'com.aerostream.ecommerce',
        doc: 'Core transactional order event stream payload',
        fields: [
          { name: 'order_id', type: 'string', doc: 'Unique UUID v4 order identifier' },
          { name: 'customer_id', type: 'string', doc: 'Customer account ID' },
          { name: 'amount', type: 'double', doc: 'Total order amount in designated currency' },
          { name: 'currency', type: 'string', default: 'USD' },
          {
            name: 'status',
            type: {
              type: 'enum',
              name: 'OrderStatus',
              symbols: ['PENDING', 'AUTHORIZED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'],
            },
          },
          { name: 'items_count', type: 'int', default: 1 },
          { name: 'timestamp_epoch_ms', type: 'long', doc: 'Unix epoch timestamp in milliseconds' },
        ],
      },
      null,
      2
    ),
  },
  {
    id: 1002,
    subject: 'telemetry-value',
    version: 1,
    type: 'JSON',
    compatibility: 'BACKWARD',
    created_at: '2026-09-26T08:20:00Z',
    description: 'IoT edge gateway environmental telemetry metrics stream (JSON Schema Draft-07)',
    topic: 'telemetry',
    schema: JSON.stringify(
      {
        $schema: 'http://json-schema.org/draft-07/schema#',
        title: 'EdgeTelemetryEvent',
        type: 'object',
        properties: {
          device_id: { type: 'string', description: 'Hardware MAC or serial number' },
          temperature_celsius: { type: 'number', minimum: -50, maximum: 120 },
          relative_humidity_pct: { type: 'number', minimum: 0, maximum: 100 },
          battery_pct: { type: 'integer', minimum: 0, maximum: 100 },
          firmware_version: { type: 'string' },
          reported_at: { type: 'string', format: 'date-time' },
        },
        required: ['device_id', 'temperature_celsius', 'battery_pct', 'reported_at'],
      },
      null,
      2
    ),
  },
  {
    id: 1003,
    subject: 'payment-records-value',
    version: 1,
    type: 'PROTOBUF',
    compatibility: 'BACKWARD',
    created_at: '2026-09-26T08:25:00Z',
    description: 'High-throughput payment gateway settlement events formatted in Google Protocol Buffers v3',
    topic: 'payment-records',
    schema: `syntax = "proto3";

package aerostream.payments.v1;

option java_package = "com.aerostream.payments.v1";
option java_multiple_files = true;

enum PaymentMethod {
  PAYMENT_METHOD_UNSPECIFIED = 0;
  CREDIT_CARD = 1;
  DEBIT_CARD = 2;
  APPLE_PAY = 3;
  GOOGLE_PAY = 4;
  CRYPTO_USDC = 5;
}

message PaymentRecord {
  string transaction_id = 1;
  string merchant_id = 2;
  int64 amount_cents = 3;
  string currency = 4;
  PaymentMethod method = 5;
  bool is_idempotent = 6;
  int64 created_at_unix_ms = 7;
  map<string, string> metadata = 8;
}`,
  },
  {
    id: 1004,
    subject: 'sensor-readings-value',
    version: 1,
    type: 'AVRO',
    compatibility: 'BACKWARD',
    created_at: '2026-09-26T08:30:00Z',
    description: 'Industrial vibration and acoustic sensor high-frequency timeseries data',
    topic: 'sensor-readings',
    schema: JSON.stringify(
      {
        type: 'record',
        name: 'SensorReading',
        namespace: 'com.aerostream.industrial',
        fields: [
          { name: 'sensor_uuid', type: 'string' },
          { name: 'station_id', type: 'int' },
          { name: 'rpm', type: 'double' },
          { name: 'vibration_rms_g', type: 'float' },
          { name: 'peak_acceleration', type: 'float' },
          { name: 'anomaly_flag', type: 'boolean', default: false },
          { name: 'sample_time_ns', type: 'long' },
        ],
      },
      null,
      2
    ),
  },
];

export const SCHEMA_TEMPLATES: Record<SchemaType, string> = {
  AVRO: JSON.stringify(
    {
      type: 'record',
      name: 'UserActivityEvent',
      namespace: 'com.aerostream.events',
      doc: 'Schema definition for user actions and page interactions',
      fields: [
        { name: 'event_id', type: 'string' },
        { name: 'user_id', type: 'string' },
        { name: 'action', type: 'string' },
        { name: 'source_ip', type: 'string', default: '127.0.0.1' },
        { name: 'session_duration_ms', type: 'long', default: 0 },
        { name: 'timestamp_epoch_ms', type: 'long' },
      ],
    },
    null,
    2
  ),
  JSON: JSON.stringify(
    {
      $schema: 'http://json-schema.org/draft-07/schema#',
      title: 'AuditLogEvent',
      type: 'object',
      properties: {
        log_id: { type: 'string' },
        principal: { type: 'string' },
        action: { type: 'string' },
        resource: { type: 'string' },
        outcome: { type: 'string', enum: ['SUCCESS', 'FAILURE', 'DENIED'] },
        timestamp: { type: 'string', format: 'date-time' },
      },
      required: ['log_id', 'principal', 'action', 'outcome', 'timestamp'],
    },
    null,
    2
  ),
  PROTOBUF: `syntax = "proto3";

package aerostream.telemetry.v2;

enum AlertSeverity {
  INFO = 0;
  WARNING = 1;
  CRITICAL = 2;
  FATAL = 3;
}

message DeviceAlert {
  string alert_id = 1;
  string device_uuid = 2;
  AlertSeverity severity = 3;
  string message = 4;
  int64 triggered_at_ms = 5;
  repeated string tags = 6;
}`,
};

@Injectable({
  providedIn: 'root',
})
export class SchemaService {
  private readonly _schemas = signal<RegisteredSchema[]>(this.loadSchemas());
  readonly schemas = this._schemas.asReadonly();

  readonly stats = computed<SchemaRegistryStats>(() => {
    const list = this._schemas();
    const avroCount = list.filter((s) => s.type === 'AVRO').length;
    const protobufCount = list.filter((s) => s.type === 'PROTOBUF').length;
    const jsonCount = list.filter((s) => s.type === 'JSON').length;
    const subjects = new Set(list.map((s) => s.subject));

    return {
      totalSchemas: list.length,
      totalSubjects: subjects.size,
      compatibilityMode: 'BACKWARD',
      avroCount,
      protobufCount,
      jsonCount,
    };
  });

  private loadSchemas(): RegisteredSchema[] {
    if (typeof localStorage === 'undefined') {
      return INITIAL_SCHEMAS;
    }
    const saved = localStorage.getItem(STORAGE_KEY);
    if (!saved) {
      this.persist(INITIAL_SCHEMAS);
      return INITIAL_SCHEMAS;
    }
    try {
      const parsed = JSON.parse(saved);
      if (Array.isArray(parsed) && parsed.length > 0) {
        return parsed;
      }
    } catch {
      // fallback
    }
    this.persist(INITIAL_SCHEMAS);
    return INITIAL_SCHEMAS;
  }

  private persist(schemas: RegisteredSchema[]): void {
    if (typeof localStorage !== 'undefined') {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(schemas));
      } catch (e) {
        console.warn('Failed to save schemas to localStorage', e);
      }
    }
  }

  getSchemas(): Observable<RegisteredSchema[]> {
    return of(this._schemas());
  }

  getSchemaBySubject(subject: string): RegisteredSchema | undefined {
    return this._schemas().find((s) => s.subject.toLowerCase() === subject.toLowerCase());
  }

  hasSubject(subject: string): boolean {
    return this._schemas().some(
      (s) =>
        s.subject.toLowerCase() === subject.toLowerCase() ||
        s.subject.toLowerCase() === `${subject.toLowerCase()}-value`
    );
  }

  getSchemaForTopic(topicName: string): RegisteredSchema | undefined {
    const directSubject = `${topicName}-value`.toLowerCase();
    const exact = this._schemas().find((s) => s.subject.toLowerCase() === directSubject);
    if (exact) return exact;

    const topicMatch = this._schemas().find(
      (s) => s.topic?.toLowerCase() === topicName.toLowerCase() || s.subject.toLowerCase() === topicName.toLowerCase()
    );
    return topicMatch;
  }

  registerSchema(req: RegisterSchemaRequest): Observable<RegisteredSchema> {
    const current = this._schemas();
    const existingIndex = current.findIndex((s) => s.subject.toLowerCase() === req.subject.trim().toLowerCase());

    let registered: RegisteredSchema;
    if (existingIndex >= 0) {
      // Register new version
      const existing = current[existingIndex];
      registered = {
        ...existing,
        version: existing.version + 1,
        type: req.type,
        schema: req.schema,
        compatibility: req.compatibility || existing.compatibility || 'BACKWARD',
        description: req.description || existing.description,
        created_at: new Date().toISOString(),
      };
      const updatedList = [...current];
      updatedList[existingIndex] = registered;
      this._schemas.set(updatedList);
      this.persist(updatedList);
    } else {
      const nextId = current.length > 0 ? Math.max(...current.map((s) => s.id)) + 1 : 1001;
      const topicName = req.subject.endsWith('-value')
        ? req.subject.slice(0, -6)
        : req.subject.endsWith('-key')
        ? req.subject.slice(0, -4)
        : req.subject;

      registered = {
        id: nextId,
        subject: req.subject.trim(),
        version: 1,
        type: req.type,
        schema: req.schema,
        compatibility: req.compatibility || 'BACKWARD',
        created_at: new Date().toISOString(),
        description: req.description || `Schema for topic ${topicName} events`,
        topic: topicName,
      };
      const updatedList = [registered, ...current];
      this._schemas.set(updatedList);
      this.persist(updatedList);
    }

    return of(registered);
  }

  deleteSchema(subject: string): Observable<boolean> {
    const filtered = this._schemas().filter((s) => s.subject.toLowerCase() !== subject.toLowerCase());
    this._schemas.set(filtered);
    this.persist(filtered);
    return of(true);
  }

  resetToDefaults(): void {
    this._schemas.set(INITIAL_SCHEMAS);
    this.persist(INITIAL_SCHEMAS);
  }

  getTemplate(type: SchemaType): string {
    return SCHEMA_TEMPLATES[type] || SCHEMA_TEMPLATES.AVRO;
  }

  validateSchemaDefinition(
    type: SchemaType,
    raw: string
  ): { valid: boolean; error?: string } {
    if (!raw || !raw.trim()) {
      return { valid: false, error: 'Schema definition cannot be empty' };
    }
    if (type === 'AVRO' || type === 'JSON') {
      try {
        const parsed = JSON.parse(raw);
        if (typeof parsed !== 'object' || parsed === null) {
          return { valid: false, error: 'Schema root must be a valid JSON object' };
        }
        if (type === 'AVRO' && !parsed.type) {
          return { valid: false, error: 'Avro schema must specify a top-level "type"' };
        }
        return { valid: true };
      } catch (e: any) {
        return { valid: false, error: `JSON Parse Error: ${e.message}` };
      }
    } else if (type === 'PROTOBUF') {
      if (!raw.includes('syntax') && !raw.includes('message')) {
        return { valid: false, error: 'Protobuf must contain syntax definition and message block' };
      }
      return { valid: true };
    }
    return { valid: true };
  }
}
