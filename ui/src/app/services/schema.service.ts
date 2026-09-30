import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, forkJoin, map, of, switchMap, tap } from 'rxjs';
import {
  CompatibilityMode,
  RegisterSchemaRequest,
  RegisteredSchema,
  SchemaRegistryStats,
  SchemaType,
} from '../models/schema.model';

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
  private readonly http = inject(HttpClient);

  private readonly _schemas = signal<RegisteredSchema[]>([]);
  readonly schemas = this._schemas.asReadonly();

  private readonly _loading = signal<boolean>(false);
  readonly loading = this._loading.asReadonly();

  private readonly _error = signal<string | null>(null);
  readonly error = this._error.asReadonly();

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

  constructor() {
    this.loadSchemas().subscribe({
      error: (err) => console.warn('Initial schema registry load failed:', err),
    });
  }

  /**
   * Resolves the API endpoint URL based on environment or localStorage overrides.
   */
  private getApiUrl(path: string): string {
    const custom =
      typeof localStorage !== 'undefined'
        ? localStorage.getItem('aerostream_api_url') ||
          localStorage.getItem('aeromq_api_url') ||
          localStorage.getItem('aerostream_api_base_url') ||
          localStorage.getItem('aeromq_api_base_url')
        : null;
    if (custom) {
      return `${custom.replace(/\/$/, '')}${path}`;
    }
    if (
      typeof window !== 'undefined' &&
      window.location.hostname === 'localhost' &&
      window.location.port === '4200'
    ) {
      return `http://localhost:9001${path}`;
    }
    return path;
  }

  /**
   * Loads registered subjects and schema definitions from Schema Registry REST API.
   */
  loadSchemas(): Observable<RegisteredSchema[]> {
    this._loading.set(true);
    this._error.set(null);

    const url = this.getApiUrl('/subjects');
    return this.http.get<string[]>(url).pipe(
      switchMap((subjects) => {
        if (!Array.isArray(subjects) || subjects.length === 0) {
          this._schemas.set([]);
          this._loading.set(false);
          return of([]);
        }

        const requests = subjects.map((subj) =>
          this.fetchSubjectLatest(subj).pipe(
            catchError((err) => {
              console.warn(`Failed to fetch schema for subject ${subj}`, err);
              return of(null);
            })
          )
        );

        return forkJoin(requests).pipe(
          map((results) => {
            const valid = results.filter((s): s is RegisteredSchema => s !== null);
            this._schemas.set(valid);
            this._loading.set(false);
            return valid;
          })
        );
      }),
      catchError((err) => {
        console.error('Failed to load subjects from Schema Registry', err);
        this._schemas.set([]);
        this._loading.set(false);
        this._error.set(err?.message || 'Failed to connect to Schema Registry');
        return of([]);
      })
    );
  }

  /**
   * Alias for loadSchemas().
   */
  fetchSchemas(): Observable<RegisteredSchema[]> {
    return this.loadSchemas();
  }

  /**
   * Fetches latest schema definition and compatibility setting for a single subject.
   */
  private fetchSubjectLatest(subject: string): Observable<RegisteredSchema> {
    const schemaUrl = this.getApiUrl(`/subjects/${encodeURIComponent(subject)}/versions/latest`);
    const configUrl = this.getApiUrl(`/config/${encodeURIComponent(subject)}`);

    const schema$ = this.http.get<any>(schemaUrl);
    const config$ = this.http.get<{ compatibilityLevel?: CompatibilityMode; compatibility?: CompatibilityMode }>(configUrl).pipe(
      catchError(() => of({ compatibilityLevel: 'BACKWARD' as CompatibilityMode }))
    );

    return forkJoin({ schemaResp: schema$, configResp: config$ }).pipe(
      map(({ schemaResp, configResp }) => {
        const rawType = (schemaResp.schemaType || schemaResp.type || 'AVRO').toUpperCase() as SchemaType;
        const type: SchemaType = (['AVRO', 'JSON', 'PROTOBUF'].includes(rawType) ? rawType : 'AVRO') as SchemaType;
        const topicName = subject.endsWith('-value')
          ? subject.slice(0, -6)
          : subject.endsWith('-key')
          ? subject.slice(0, -4)
          : subject;

        let schemaText = typeof schemaResp.schema === 'string' ? schemaResp.schema : JSON.stringify(schemaResp.schema, null, 2);
        let extractedDesc: string | undefined;

        if (type === 'AVRO' || type === 'JSON') {
          try {
            const parsed = typeof schemaResp.schema === 'string' ? JSON.parse(schemaResp.schema) : schemaResp.schema;
            schemaText = JSON.stringify(parsed, null, 2);
            extractedDesc = parsed.doc || parsed.description;
          } catch {
            // Keep raw schemaText
          }
        }

        const compatMode: CompatibilityMode =
          configResp?.compatibilityLevel || configResp?.compatibility || 'BACKWARD';

        const registered: RegisteredSchema = {
          id: Number(schemaResp.id) || 1,
          subject: schemaResp.subject || subject,
          version: Number(schemaResp.version) || 1,
          type,
          schema: schemaText,
          compatibility: compatMode,
          created_at: schemaResp.created_at || new Date().toISOString(),
          description: extractedDesc || `Governed schema contract for topic ${topicName}`,
          topic: topicName,
        };

        return registered;
      })
    );
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

    return this._schemas().find(
      (s) => s.topic?.toLowerCase() === topicName.toLowerCase() || s.subject.toLowerCase() === topicName.toLowerCase()
    );
  }

  /**
   * Registers a new schema or schema evolution version with the Schema Registry.
   */
  registerSchema(req: RegisterSchemaRequest): Observable<RegisteredSchema> {
    const trimmedSubject = req.subject.trim();
    const url = this.getApiUrl(`/subjects/${encodeURIComponent(trimmedSubject)}/versions`);
    const payload = {
      schema: req.schema,
      schemaType: req.type,
    };

    return this.http.post<{ id: number }>(url, payload).pipe(
      switchMap((res) => {
        // Optionally set compatibility mode if specified
        const setCompat$ = req.compatibility
          ? this.http
              .put(this.getApiUrl(`/config/${encodeURIComponent(trimmedSubject)}`), {
                compatibility: req.compatibility,
              })
              .pipe(catchError(() => of(null)))
          : of(null);

        return setCompat$.pipe(
          switchMap(() => this.fetchSchemas()),
          map((schemas) => {
            const found = schemas.find(
              (s) => s.subject.toLowerCase() === trimmedSubject.toLowerCase()
            );
            if (found) {
              return found;
            }
            const topicName = trimmedSubject.endsWith('-value')
              ? trimmedSubject.slice(0, -6)
              : trimmedSubject.endsWith('-key')
              ? trimmedSubject.slice(0, -4)
              : trimmedSubject;

            return {
              id: res.id || 1,
              subject: trimmedSubject,
              version: 1,
              type: req.type,
              schema: req.schema,
              compatibility: req.compatibility || 'BACKWARD',
              created_at: new Date().toISOString(),
              description: req.description || `Governed schema contract for topic ${topicName}`,
              topic: topicName,
            } as RegisteredSchema;
          })
        );
      })
    );
  }

  /**
   * Tests schema compatibility against a subject and version.
   */
  checkCompatibility(
    subject: string,
    version: number | string = 'latest',
    schema: string,
    schemaType: SchemaType = 'AVRO'
  ): Observable<{ is_compatible: boolean }> {
    const url = this.getApiUrl(
      `/compatibility/subjects/${encodeURIComponent(subject.trim())}/versions/${version}`
    );
    return this.http.post<{ is_compatible: boolean }>(url, {
      schema,
      schemaType,
    });
  }

  /**
   * Deletes a subject from the Schema Registry.
   */
  deleteSubject(subject: string): Observable<boolean> {
    const url = this.getApiUrl(`/subjects/${encodeURIComponent(subject.trim())}`);
    return this.http.delete<number[] | any>(url).pipe(
      switchMap(() => this.fetchSchemas()),
      map(() => true)
    );
  }

  /**
   * Backwards compatible alias for deleteSubject.
   */
  deleteSchema(subject: string): Observable<boolean> {
    return this.deleteSubject(subject);
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
