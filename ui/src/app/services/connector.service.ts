import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, map, of, tap } from 'rxjs';
import {
  Connector,
  ConnectorPlugin,
  ConnectorState,
  ConnectorStats,
  ConnectorType,
  CreateConnectorRequest,
} from '../models/connector.model';

export const PLUGIN_CONFIG_TEMPLATES: Record<string, Record<string, string>> = {
  HttpWebhookSinkConnector: {
    'http.url': 'https://api.internal/webhooks/payments',
    'http.method': 'POST',
    'http.headers': 'Content-Type: application/json',
    'batch.size': '100',
    'retry.backoff.ms': '1000',
  },
  S3ArchivalSinkConnector: {
    's3.bucket': 'aerostream-archives',
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

@Injectable({
  providedIn: 'root',
})
export class ConnectorService {
  private readonly http = inject(HttpClient);

  private readonly _connectors = signal<Connector[]>([]);
  readonly connectors = this._connectors.asReadonly();

  private readonly _plugins = signal<ConnectorPlugin[]>([]);
  readonly plugins = this._plugins.asReadonly();

  private readonly _loading = signal<boolean>(false);
  readonly loading = this._loading.asReadonly();

  private readonly _error = signal<string | null>(null);
  readonly error = this._error.asReadonly();

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

  /**
   * Loads installed connector plugins directly from the Kafka Connect backend REST API.
   */
  loadPlugins(): Observable<ConnectorPlugin[]> {
    const url = this.getApiUrl('/api/connector-plugins');
    return this.http.get<ConnectorPlugin[]>(url).pipe(
      catchError((err) => {
        console.warn('Failed GET /api/connector-plugins, falling back to /connector-plugins:', err);
        return this.http.get<ConnectorPlugin[]>(this.getApiUrl('/connector-plugins'));
      }),
      catchError((err) => {
        console.error('Failed to load connector plugins from backend:', err);
        return of([] as ConnectorPlugin[]);
      }),
      tap((plugins) => {
        if (Array.isArray(plugins)) {
          this._plugins.set(plugins);
        } else {
          this._plugins.set([]);
        }
      })
    );
  }

  /**
   * Loads active connectors from the Go Controller Kafka Connect REST API.
   * Uses /api/connectors-detail with fallback to /connectors?expand=status&expand=info.
   * If no connectors exist, sets an empty array (no mock data).
   */
  loadConnectors(): Observable<Connector[]> {
    this._loading.set(true);
    this._error.set(null);
    const url = this.getApiUrl('/api/connectors-detail');

    return this.http.get<Connector[]>(url).pipe(
      catchError((err) => {
        console.warn('Failed GET /api/connectors-detail, trying /connectors?expand=status&expand=info fallback:', err);
        return this.http.get<Record<string, any>>(this.getApiUrl('/connectors?expand=status&expand=info')).pipe(
          map((expanded) => {
            if (!expanded || typeof expanded !== 'object') {
              return [] as Connector[];
            }
            return Object.entries(expanded).map(([name, val]) => {
              const info = val?.info || {};
              const status = val?.status || {};
              const config: Record<string, string> = info.config || {};
              const inferredType: ConnectorType =
                (config['connector.type'] as ConnectorType) ||
                (config['connector.class']?.toLowerCase().includes('source') ? 'SOURCE' : 'SINK');
              return {
                name,
                type: inferredType,
                class: config['connector.class'] || '',
                topic: config['topics'] || config['topic'] || '',
                topics: config['topics'] ? [config['topics']] : [],
                config,
                state: (status?.connector?.state || 'RUNNING') as ConnectorState,
                tasks_count: Array.isArray(status?.tasks) ? status.tasks.length : 1,
                tasks: status?.tasks || [],
                worker_id: status?.connector?.worker_id,
                records_processed: 0,
                bytes_transferred: 0,
                created_at: new Date().toISOString(),
              } as Connector;
            });
          }),
          catchError((fallbackErr) => {
            console.error('Failed to load connectors from cluster:', fallbackErr);
            this._error.set('Failed to connect to Kafka Connect REST API');
            return of([] as Connector[]);
          })
        );
      }),
      tap((connectors) => {
        this._loading.set(false);
        this._connectors.set(Array.isArray(connectors) ? connectors : []);
      })
    );
  }

  /**
   * Refreshes both installed plugins and active connectors from the cluster.
   */
  refresh(): void {
    this.loadPlugins().subscribe();
    this.loadConnectors().subscribe();
  }

  /**
   * Deploys a new connector to the Kafka Connect REST API.
   */
  createConnector(req: CreateConnectorRequest): Observable<Connector> {
    const url = this.getApiUrl('/api/connectors');
    const payload = {
      name: req.name.trim(),
      type: req.type,
      class: req.class,
      topic: req.topic,
      tasks_count: req.tasks_count || 1,
      config: {
        name: req.name.trim(),
        'connector.class': req.class,
        'tasks.max': String(req.tasks_count || 1),
        'topics': req.topic,
        ...(req.config || {}),
      },
    };

    return this.http.post<Connector>(url, payload).pipe(
      tap(() => {
        this.loadConnectors().subscribe();
      })
    );
  }

  /**
   * Pauses an active connector.
   */
  pauseConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/pause`);
    return this.http.put(url, {}).pipe(
      tap(() => {
        this._connectors.update((list) =>
          list.map((c) => (c.name === name ? { ...c, state: 'PAUSED' as const } : c))
        );
      })
    );
  }

  /**
   * Resumes a paused connector.
   */
  resumeConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/resume`);
    return this.http.put(url, {}).pipe(
      tap(() => {
        this._connectors.update((list) =>
          list.map((c) => (c.name === name ? { ...c, state: 'RUNNING' as const } : c))
        );
      })
    );
  }

  /**
   * Stops a connector (KIP-875).
   */
  stopConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/stop`);
    return this.http.post(url, {}).pipe(
      tap(() => {
        this._connectors.update((list) =>
          list.map((c) => (c.name === name ? { ...c, state: 'STOPPED' as const } : c))
        );
      })
    );
  }

  /**
   * Restarts a connector and optionally its tasks.
   */
  restartConnector(name: string, includeTasks: boolean = false, onlyFailed: boolean = false): Observable<any> {
    let url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/restart`);
    const params: string[] = [];
    if (includeTasks) params.push('includeTasks=true');
    if (onlyFailed) params.push('onlyFailed=true');
    if (params.length > 0) {
      url += `?${params.join('&')}`;
    }
    return this.http.post(url, {}).pipe(
      tap(() => {
        this.loadConnectors().subscribe();
      })
    );
  }

  /**
   * Deletes a connector from the cluster.
   */
  deleteConnector(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}`);
    return this.http.delete(url).pipe(
      tap(() => {
        this._connectors.update((list) => list.filter((c) => c.name !== name));
      })
    );
  }

  /**
   * Retrieves live status and tasks state for a specific connector.
   */
  getConnectorStatus(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/connectors/${encodeURIComponent(name)}/status`);
    return this.http.get(url);
  }

  /**
   * Returns predefined configuration templates for the deploy connector modal.
   */
  getConfigTemplate(className: string): Record<string, string> {
    return PLUGIN_CONFIG_TEMPLATES[className] || {};
  }
}
