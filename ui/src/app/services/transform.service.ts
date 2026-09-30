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

@Injectable({
  providedIn: 'root',
})
export class TransformService {
  private http = inject(HttpClient);

  readonly transforms = signal<StreamTransform[]>([]);
  readonly isLoading = signal<boolean>(false);
  readonly lastError = signal<string | null>(null);
  private readonly executionLatencies = signal<number[]>([]);

  readonly metrics = computed<TransformMetrics>(() => {
    const list = this.transforms();
    const activeTransforms = list.filter((t) => t.status === 'RUNNING').length;
    const filteredRecords = list.reduce((sum, t) => sum + (t.messages_filtered || 0), 0);
    const piiMaskedStreams = list.filter((t) => t.type === 'MASK_PII').length;

    const latencies = this.executionLatencies();
    let avgLatencyMs = 0;
    if (latencies.length > 0) {
      const sum = latencies.reduce((acc, val) => acc + val, 0);
      avgLatencyMs = Number((sum / latencies.length).toFixed(3));
    }

    return {
      activeTransforms,
      filteredRecords,
      piiMaskedStreams,
      avgLatencyMs,
    };
  });

  /**
   * Records a measured test execution latency (in milliseconds) and updates the running average.
   */
  recordExecutionLatency(ms: number): void {
    if (typeof ms === 'number' && !isNaN(ms) && ms >= 0) {
      this.executionLatencies.update((prev) => [...prev.slice(-99), ms]);
    }
  }

  private getApiUrl(path: string): string {
    const custom = typeof localStorage !== 'undefined'
      ? (localStorage.getItem('aerostream_api_url') || localStorage.getItem('aeromq_api_url') || localStorage.getItem('aerostream_api_base_url') || localStorage.getItem('aeromq_api_base_url'))
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

  loadTransforms(): void {
    this.isLoading.set(true);
    this.lastError.set(null);
    this.fetchTransforms().subscribe({
      next: (data) => {
        this.transforms.set(data || []);
        this.isLoading.set(false);
      },
      error: (err) => {
        this.lastError.set(err?.message || 'Failed to fetch transforms');
        this.transforms.set([]);
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
        console.warn('Could not fetch transforms from API:', err);
        return of([]);
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
