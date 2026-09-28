import { Injectable, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';

export type StreamJobType = 'AGGREGATE' | 'JOIN';
export type WindowType = 'tumbling' | 'hopping' | 'sliding' | 'session';

export interface StreamJobConfig {
  name: string;
  type: StreamJobType;
  source_topic: string;
  target_topic?: string;
  table_topic?: string;
  key_field?: string;
  table_key_field?: string;
  value_field?: string;
  timestamp_field?: string;
  agg?: string;
  window?: { type: WindowType; size_ms?: number; advance_ms?: number; gap_ms?: number };
  grace_ms?: number;
  retention_ms?: number;
  join_type?: string;
}

export interface StreamJobMetrics {
  processed: number;
  late_dropped: number;
  skipped: number;
  emitted: number;
  unmatched: number;
  state_keys: number;
  stream_time_ms: number;
  table_rows: number;
}

export interface StreamJob extends StreamJobConfig {
  status: 'RUNNING' | 'PAUSED';
  metrics: StreamJobMetrics;
}

export interface WindowResult {
  key: string;
  window_start: number;
  window_end: number;
  count: number;
  sum: number;
  min: number;
  max: number;
  avg: number;
  value: number;
}

@Injectable({ providedIn: 'root' })
export class StreamsService {
  private http = inject(HttpClient);

  readonly jobs = signal<StreamJob[]>([]);
  readonly isLoading = signal<boolean>(false);
  readonly lastError = signal<string | null>(null);

  private url(path: string): string {
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

  load(): void {
    this.isLoading.set(true);
    this.http.get<StreamJob[]>(this.url('/api/streams')).subscribe({
      next: (list) => {
        this.jobs.set(list ?? []);
        this.lastError.set(null);
        this.isLoading.set(false);
      },
      error: (e) => {
        this.lastError.set(e?.message ?? 'Failed to load stream jobs');
        this.isLoading.set(false);
      },
    });
  }

  create(cfg: StreamJobConfig): Observable<StreamJob> {
    return this.http.post<StreamJob>(this.url('/api/streams'), cfg).pipe(tap(() => this.load()));
  }

  remove(name: string): Observable<unknown> {
    return this.http.delete(this.url(`/api/streams/${encodeURIComponent(name)}`)).pipe(tap(() => this.load()));
  }

  setPaused(name: string, paused: boolean): Observable<StreamJob> {
    const action = paused ? 'pause' : 'resume';
    return this.http
      .post<StreamJob>(this.url(`/api/streams/${encodeURIComponent(name)}/${action}`), {})
      .pipe(tap(() => this.load()));
  }

  ingest(name: string, value: unknown, topic?: string, key?: string, timestampMs?: number): Observable<StreamJob> {
    return this.http
      .post<StreamJob>(this.url(`/api/streams/${encodeURIComponent(name)}/ingest`), {
        topic,
        key,
        value,
        timestamp_ms: timestampMs,
      })
      .pipe(tap(() => this.load()));
  }

  results(name: string, limit = 50): Observable<unknown[]> {
    return this.http.get<unknown[]>(this.url(`/api/streams/${encodeURIComponent(name)}/results?limit=${limit}`));
  }

  keys(name: string): Observable<{ keys: string[] }> {
    return this.http.get<{ keys: string[] }>(this.url(`/api/streams/${encodeURIComponent(name)}/state`));
  }

  queryKey(name: string, key: string): Observable<{ key: string; windows?: WindowResult[]; table?: unknown }> {
    return this.http.get<{ key: string; windows?: WindowResult[]; table?: unknown }>(
      this.url(`/api/streams/${encodeURIComponent(name)}/state?key=${encodeURIComponent(key)}`)
    );
  }
}
