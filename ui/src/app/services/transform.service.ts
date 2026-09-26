import { Injectable, inject, signal, computed } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, of, tap } from 'rxjs';
import {
  StreamTransform,
  RegisterTransformRequest,
  TestTransformRequest,
  TestTransformResponse,
  TransformMetrics,
  TransformType,
} from '../models/transform.model';

const FALLBACK_TRANSFORMS: StreamTransform[] = [
  {
    id: 'xform-pii-masker-orders',
    name: 'pii-masker-orders',
    source_topic: 'orders',
    target_topic: 'orders-sanitized',
    type: 'MASK_PII',
    config: {
      fields_to_mask: 'credit_card,cvv,password,email',
      mask_pattern: '***',
    },
    code: '// Native inline PII masking filter for AeroStream\nmask_fields(["credit_card", "cvv", "password", "email"]);',
    status: 'RUNNING',
    messages_processed: 1420,
    messages_filtered: 0,
    created_at: new Date(Date.now() - 2 * 3600000).toISOString(),
  },
  {
    id: 'xform-telemetry-filter-critical',
    name: 'telemetry-filter-critical',
    source_topic: 'telemetry-events',
    target_topic: 'alerts-critical',
    type: 'FILTER',
    config: {
      filter_expression: 'level == "CRITICAL"',
    },
    code: '// Inline stream filter dropping non-critical telemetry\nfilter: record.level == "CRITICAL";',
    status: 'RUNNING',
    messages_processed: 8950,
    messages_filtered: 7412,
    created_at: new Date(Date.now() - 5 * 3600000).toISOString(),
  },
  {
    id: 'xform-wasm-geo-enricher',
    name: 'wasm-geo-enricher',
    source_topic: 'clickstream',
    target_topic: 'clickstream-enriched',
    type: 'WASM',
    config: {
      runtime: 'wasm32-wasi',
      memory_pages: '2',
      action: 'uppercase_keys',
    },
    code: '(module\n  (type $t0 (func (param i32 i32) (result i32)))\n  (func $transform (export "transform") (type $t0)\n    (local.get 0)\n  )\n  (memory (export "memory") 1)\n)',
    status: 'RUNNING',
    messages_processed: 4210,
    messages_filtered: 145,
    created_at: new Date(Date.now() - 1 * 3600000).toISOString(),
  },
];

@Injectable({
  providedIn: 'root',
})
export class TransformService {
  private http = inject(HttpClient);

  readonly transforms = signal<StreamTransform[]>([]);
  readonly isLoading = signal<boolean>(false);
  readonly lastError = signal<string | null>(null);

  readonly metrics = computed<TransformMetrics>(() => {
    const list = this.transforms();
    const activeTransforms = list.filter((t) => t.status === 'RUNNING').length;
    const filteredRecords = list.reduce((sum, t) => sum + (t.messages_filtered || 0), 0);
    const piiMaskedStreams = list.filter((t) => t.type === 'MASK_PII').length;

    return {
      activeTransforms,
      filteredRecords,
      piiMaskedStreams,
      avgLatencyMs: 0.48,
    };
  });

  private getApiUrl(path: string): string {
    const custom = typeof localStorage !== 'undefined' ? localStorage.getItem('aeromq_api_url') : null;
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

  loadTransforms(): void {
    this.isLoading.set(true);
    this.fetchTransforms().subscribe({
      next: (data) => {
        this.transforms.set(data && data.length > 0 ? data : FALLBACK_TRANSFORMS);
        this.isLoading.set(false);
      },
      error: () => {
        this.transforms.set(FALLBACK_TRANSFORMS);
        this.isLoading.set(false);
      },
    });
  }

  fetchTransforms(): Observable<StreamTransform[]> {
    const url = this.getApiUrl('/api/transforms');
    return this.http.get<StreamTransform[]>(url).pipe(
      tap((res) => {
        if (Array.isArray(res)) {
          this.transforms.set(res);
        }
      }),
      catchError((err) => {
        console.warn('Could not fetch transforms from API, using defaults:', err);
        return of(FALLBACK_TRANSFORMS);
      })
    );
  }

  registerTransform(req: RegisterTransformRequest): Observable<StreamTransform> {
    const url = this.getApiUrl('/api/transforms');
    return this.http.post<StreamTransform>(url, req).pipe(
      tap((created) => {
        this.transforms.update((prev) => [...prev, created]);
      })
    );
  }

  pauseTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}/pause`);
    return this.http.post(url, {}).pipe(
      tap(() => {
        this.transforms.update((prev) =>
          prev.map((t) => (t.name === name ? { ...t, status: 'PAUSED' } : t))
        );
      })
    );
  }

  resumeTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}/resume`);
    return this.http.post(url, {}).pipe(
      tap(() => {
        this.transforms.update((prev) =>
          prev.map((t) => (t.name === name ? { ...t, status: 'RUNNING' } : t))
        );
      })
    );
  }

  deleteTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}`);
    return this.http.delete(url).pipe(
      tap(() => {
        this.transforms.update((prev) => prev.filter((t) => t.name !== name));
      })
    );
  }

  testTransform(req: TestTransformRequest): Observable<TestTransformResponse> {
    const url = this.getApiUrl('/api/transforms/test');
    return this.http.post<TestTransformResponse>(url, req);
  }

  getTransformTemplate(type: TransformType): { config: Record<string, string>; code: string } {
    switch (type) {
      case 'MASK_PII':
        return {
          config: {
            fields_to_mask: 'credit_card,cvv,password,email,ssn',
            mask_pattern: '***',
          },
          code: '// AeroStream In-Broker PII Masker\nmask_fields(["credit_card", "cvv", "password", "email", "ssn"]);',
        };

      case 'FILTER':
        return {
          config: {
            filter_expression: 'level == "CRITICAL"',
          },
          code: '// AeroStream In-Broker Filter Expression\nfilter: record.level == "CRITICAL";',
        };

      case 'JSON_MAP':
        return {
          config: {
            add_fields: 'environment:production,processed_by:aerostream',
            set_compliance_tier: 'tier-1',
            rename_fields: 'old_id:id',
          },
          code: '// AeroStream JSON Transformation & Metadata Injection\ninject_metadata();\nrename_key("old_id", "id");',
        };

      case 'WASM':
        return {
          config: {
            runtime: 'wasm32-wasi',
            memory_limit_mb: '64',
            action: 'uppercase_keys',
          },
          code: `(module
  (type $t0 (func (param i32 i32) (result i32)))
  (func $transform (export "transform") (type $t0)
    (local.get 0)
  )
  (memory (export "memory") 1)
)`,
        };
    }
  }
}
