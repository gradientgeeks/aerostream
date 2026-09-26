import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, of, tap } from 'rxjs';
import {
  Connector,
  ConnectorPlugin,
  ConnectorStats,
  CreateConnectorRequest,
} from '../models/connector.model';

const STORAGE_KEY_CONNECTORS = 'aerostream_connectors';

export const INITIAL_PLUGINS: ConnectorPlugin[] = [
  {
    class: 'HttpWebhookSinkConnector',
    type: 'SINK',
    version: '1.0.0',
    description: 'Dispatches topic records to external HTTP REST endpoints.',
  },
  {
    class: 'S3ArchivalSinkConnector',
    type: 'SINK',
    version: '1.0.0',
    description: 'Streams records to AWS S3 / MinIO object storage.',
  },
  {
    class: 'DatabaseCdcSourceConnector',
    type: 'SOURCE',
    version: '1.0.0',
    description: 'Ingests simulated Change Data Capture (CDC) events from PostgreSQL/MySQL.',
  },
  {
    class: 'ElasticSearchSinkConnector',
    type: 'SINK',
    version: '1.0.0',
    description: 'Streams records to Elasticsearch / OpenSearch index.',
  },
];

export const PLUGIN_CONFIG_TEMPLATES: Record<string, Record<string, string>> = {
  HttpWebhookSinkConnector: {
    'http.url': 'https://api.internal/webhooks/payments',
    'http.method': 'POST',
    'http.headers': 'Content-Type: application/json',
    'batch.size': '100',
    'retry.backoff.ms': '1000',
  },
  S3ArchivalSinkConnector: {
    's3.bucket': 'aeromq-archives',
    's3.region': 'us-east-1',
    'flush.size': '1000',
    'storage.class': 'STANDARD_IA',
    'part.size.mb': '25',
  },
  DatabaseCdcSourceConnector: {
    'db.hostname': 'postgres-primary.internal',
    'db.port': '5432',
    'db.database': 'production_db',
    'db.table.whitelist': 'public.orders,public.users',
    'poll.interval.ms': '500',
  },
  ElasticSearchSinkConnector: {
    'es.host': 'http://elasticsearch:9200',
    'es.index': 'aerostream-events',
    'batch.size': '500',
    'key.ignore': 'true',
    'connection.timeout.ms': '5000',
  },
};

const DEFAULT_CONNECTORS: Connector[] = [
  {
    name: 's3-cold-storage-sink',
    type: 'SINK',
    class: 'S3ArchivalSinkConnector',
    topic: 'orders',
    config: {
      'connector.class': 'S3ArchivalSinkConnector',
      'tasks.max': '1',
      'topics': 'orders',
      's3.bucket': 'aeromq-archives',
      's3.region': 'us-east-1',
      'flush.size': '1000',
    },
    state: 'RUNNING',
    tasks_count: 1,
    records_processed: 142580,
    bytes_transferred: 52428800,
    created_at: new Date(Date.now() - 86400000).toISOString(),
  },
  {
    name: 'webhook-payment-notifier',
    type: 'SINK',
    class: 'HttpWebhookSinkConnector',
    topic: 'payment-transactions',
    config: {
      'connector.class': 'HttpWebhookSinkConnector',
      'tasks.max': '1',
      'topics': 'payment-transactions',
      'http.url': 'https://api.internal/webhooks/payments',
      'http.method': 'POST',
    },
    state: 'RUNNING',
    tasks_count: 1,
    records_processed: 89400,
    bytes_transferred: 18874368,
    created_at: new Date(Date.now() - 43200000).toISOString(),
  },
];

@Injectable({
  providedIn: 'root',
})
export class ConnectorService {
  private readonly http = inject(HttpClient);

  private readonly _connectors = signal<Connector[]>(this.loadInitialConnectors());
  readonly connectors = this._connectors.asReadonly();

  private readonly _plugins = signal<ConnectorPlugin[]>(INITIAL_PLUGINS);
  readonly plugins = this._plugins.asReadonly();

  private readonly _loading = signal<boolean>(false);
  readonly loading = this._loading.asReadonly();

  readonly stats = computed<ConnectorStats>(() => {
    const list = this._connectors();
    const activeSources = list.filter((c) => c.type === 'SOURCE' && c.state === 'RUNNING').length;
    const activeSinks = list.filter((c) => c.type === 'SINK' && c.state === 'RUNNING').length;
    const recordsStreamed = list.reduce((sum, c) => sum + (c.records_processed || 0), 0);

    return {
      totalConnectors: list.length,
      activeSources,
      activeSinks,
      recordsStreamed,
    };
  });

  constructor() {
    this.refresh();
  }

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

  private loadInitialConnectors(): Connector[] {
    if (typeof localStorage === 'undefined') {
      return DEFAULT_CONNECTORS;
    }
    const saved = localStorage.getItem(STORAGE_KEY_CONNECTORS);
    if (!saved) {
      this.persist(DEFAULT_CONNECTORS);
      return DEFAULT_CONNECTORS;
    }
    try {
      const parsed = JSON.parse(saved);
      if (Array.isArray(parsed) && parsed.length > 0) {
        return parsed;
      }
    } catch {
      // fallback
    }
    this.persist(DEFAULT_CONNECTORS);
    return DEFAULT_CONNECTORS;
  }

  private persist(connectors: Connector[]): void {
    if (typeof localStorage !== 'undefined') {
      try {
        localStorage.setItem(STORAGE_KEY_CONNECTORS, JSON.stringify(connectors));
      } catch (e) {
        console.warn('Failed to save connectors to localStorage', e);
      }
    }
  }

  refresh(): void {
    this._loading.set(true);

    // Fetch plugins
    this.http
      .get<ConnectorPlugin[]>(this.getApiUrl('/api/connector-plugins'))
      .pipe(
        catchError(() => of(INITIAL_PLUGINS)),
        tap((plugins) => {
          if (Array.isArray(plugins) && plugins.length > 0) {
            this._plugins.set(plugins);
          }
        })
      )
      .subscribe();

    // Fetch connectors detail
    this.http
      .get<Connector[]>(this.getApiUrl('/api/connectors-detail'))
      .pipe(
        catchError(() => of(this._connectors())),
        tap((list) => {
          this._loading.set(false);
          if (Array.isArray(list) && list.length > 0) {
            this._connectors.set(list);
            this.persist(list);
          }
        })
      )
      .subscribe({
        error: () => this._loading.set(false),
      });
  }

  createConnector(req: CreateConnectorRequest): Observable<Connector> {
    const url = this.getApiUrl('/api/connectors');
    return this.http.post<Connector>(url, req).pipe(
      catchError((err) => {
        // Fallback for offline/mock mode
        const plugin = this._plugins().find((p) => p.class === req.class);
        const inferredType = req.type || plugin?.type || 'SINK';
        const newConn: Connector = {
          name: req.name.trim(),
          type: inferredType,
          class: req.class,
          topic: req.topic,
          config: req.config || {},
          state: 'RUNNING',
          tasks_count: req.tasks_count || 1,
          records_processed: 0,
          bytes_transferred: 0,
          created_at: new Date().toISOString(),
        };
        const updated = [newConn, ...this._connectors().filter((c) => c.name !== newConn.name)];
        this._connectors.set(updated);
        this.persist(updated);
        return of(newConn);
      }),
      tap((created) => {
        if (created) {
          const current = this._connectors().filter((c) => c.name !== created.name);
          const updated = [created, ...current];
          this._connectors.set(updated);
          this.persist(updated);
        }
      })
    );
  }

  pauseConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/pause`);
    return this.http.put(url, {}).pipe(
      catchError(() => of({ status: 'PAUSED' })),
      tap(() => {
        const updated = this._connectors().map((c) =>
          c.name === name ? { ...c, state: 'PAUSED' as const } : c
        );
        this._connectors.set(updated);
        this.persist(updated);
      })
    );
  }

  resumeConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/resume`);
    return this.http.put(url, {}).pipe(
      catchError(() => of({ status: 'RUNNING' })),
      tap(() => {
        const updated = this._connectors().map((c) =>
          c.name === name ? { ...c, state: 'RUNNING' as const } : c
        );
        this._connectors.set(updated);
        this.persist(updated);
      })
    );
  }

  deleteConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}`);
    return this.http.delete(url).pipe(
      catchError(() => of({ deleted: true })),
      tap(() => {
        const updated = this._connectors().filter((c) => c.name !== name);
        this._connectors.set(updated);
        this.persist(updated);
      })
    );
  }

  getConfigTemplate(className: string): Record<string, string> {
    return PLUGIN_CONFIG_TEMPLATES[className] || {};
  }
}
