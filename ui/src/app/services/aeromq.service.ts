import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, Subscription, catchError, map, of, switchMap, tap, timer } from 'rxjs';
import {
  Broker,
  ClusterInfo,
  ConsumerGroup,
  CreateTopicRequest,
  FetchMessagesResponse,
  PartitionLag,
  ProduceRequest,
  ProduceResponse,
  Topic,
} from '../models/aeromq.models';

const STORAGE_KEY_BASE_URL = 'aeromq_api_base_url';
function getDefaultApiBase(): string {
  if (typeof window !== 'undefined' && window.location && window.location.origin) {
    if (window.location.port !== '4200') {
      return window.location.origin;
    }
  }
  return 'http://localhost:9001';
}

@Injectable({
  providedIn: 'root',
})
export class AeroMQService {
  private readonly http = inject(HttpClient);

  // Configuration Signals
  readonly apiBaseUrl = signal<string>(
    (typeof localStorage !== 'undefined' && localStorage.getItem(STORAGE_KEY_BASE_URL)) || getDefaultApiBase()
  );
  readonly autoRefreshEnabled = signal<boolean>(true);
  readonly refreshIntervalSeconds = signal<number>(3);

  // State Signals
  readonly clusterInfo = signal<ClusterInfo | null>(null);
  readonly brokers = signal<Broker[]>([]);
  readonly isConnected = signal<boolean>(false);
  readonly isLeader = signal<boolean>(false);
  readonly lastUpdated = signal<Date | null>(null);
  readonly isLoading = signal<boolean>(false);
  readonly error = signal<string | null>(null);

  // Computed state
  readonly activeBrokersCount = computed(() => {
    return this.brokers().filter((b) => b.active).length;
  });

  readonly totalBrokersCount = computed(() => {
    return this.brokers().length;
  });

  readonly raftState = computed(() => {
    return this.clusterInfo()?.raft_state ?? 'Unknown';
  });

  readonly raftLeader = computed(() => {
    return this.clusterInfo()?.raft_leader ?? 'N/A';
  });

  private pollingSubscription: Subscription | null = null;

  constructor() {
    this.startPolling();
  }

  // Configuration management
  setApiBaseUrl(url: string): void {
    const cleanUrl = url.trim().replace(/\/+$/, '');
    this.apiBaseUrl.set(cleanUrl);
    localStorage.setItem(STORAGE_KEY_BASE_URL, cleanUrl);
    this.refresh();
  }

  resetApiBaseUrl(): void {
    this.setApiBaseUrl(getDefaultApiBase());
  }

  toggleAutoRefresh(enable?: boolean): void {
    const nextState = enable !== undefined ? enable : !this.autoRefreshEnabled();
    this.autoRefreshEnabled.set(nextState);
    if (nextState) {
      this.startPolling();
    } else {
      this.stopPolling();
    }
  }

  setRefreshInterval(seconds: number): void {
    if (seconds < 1) seconds = 1;
    this.refreshIntervalSeconds.set(seconds);
    if (this.autoRefreshEnabled()) {
      this.startPolling();
    }
  }

  // Polling control
  startPolling(): void {
    this.stopPolling();
    const intervalMs = this.refreshIntervalSeconds() * 1000;
    this.pollingSubscription = timer(0, intervalMs)
      .pipe(
        switchMap(() => {
          if (!this.autoRefreshEnabled()) {
            return of(null);
          }
          return this.fetchClusterAndBrokers();
        })
      )
      .subscribe();
  }

  stopPolling(): void {
    if (this.pollingSubscription) {
      this.pollingSubscription.unsubscribe();
      this.pollingSubscription = null;
    }
  }

  refresh(): Observable<ClusterInfo | null> {
    return this.fetchClusterAndBrokers();
  }

  private fetchClusterAndBrokers(): Observable<ClusterInfo | null> {
    this.isLoading.set(true);
    return this.getClusterInfo().pipe(
      tap((info) => {
        this.clusterInfo.set(info);
        this.brokers.set(info.brokers || []);
        this.isConnected.set(true);
        this.isLeader.set(info.raft_state?.toLowerCase() === 'leader');
        this.lastUpdated.set(new Date());
        this.error.set(null);
        this.isLoading.set(false);
      }),
      catchError((err) => {
        this.isConnected.set(false);
        this.isLoading.set(false);
        const errMsg =
          err?.status === 0
            ? `Connection refused at ${this.apiBaseUrl()}. Is AeroMQ controller running?`
            : err?.message || 'Failed to communicate with AeroMQ cluster';
        this.error.set(errMsg);
        return of(null);
      })
    );
  }

  // Core API Endpoints
  getClusterInfo(): Observable<ClusterInfo> {
    const url = `${this.apiBaseUrl()}/api/cluster`;
    return this.http.get<ClusterInfo>(url);
  }

  getBrokers(): Observable<Broker[]> {
    const url = `${this.apiBaseUrl()}/api/brokers`;
    return this.http.get<Broker[]>(url);
  }

  getTopics(): Observable<Topic[]> {
    const url = `${this.apiBaseUrl()}/api/topics`;
    return this.http.get<Topic[]>(url);
  }

  createTopic(req: CreateTopicRequest): Observable<{ success: boolean; message: string }> {
    const url = `${this.apiBaseUrl()}/api/topics`;
    return this.http.post<{ success: boolean; message: string }>(url, req);
  }

  getConsumerGroups(): Observable<ConsumerGroup[]> {
    const url = `${this.apiBaseUrl()}/api/groups`;
    return this.http.get<ConsumerGroup[]>(url);
  }

  getLag(): Observable<PartitionLag[]> {
    const url = `${this.apiBaseUrl()}/api/lag`;
    return this.http.get<PartitionLag[]>(url);
  }

  produceMessage(
    topic: string,
    partition: number,
    message: string
  ): Observable<ProduceResponse> {
    const url = `${this.apiBaseUrl()}/api/produce`;
    const payload: ProduceRequest = { topic, partition, message };
    return this.http.post<ProduceResponse>(url, payload);
  }

  getMessages(
    topic: string,
    partition: number,
    offset?: number,
    limit?: number
  ): Observable<FetchMessagesResponse> {
    const url = `${this.apiBaseUrl()}/api/messages`;
    let params = new HttpParams()
      .set('topic', topic)
      .set('partition', partition.toString());

    if (offset !== undefined && offset !== null) {
      params = params.set('offset', offset.toString());
    }
    if (limit !== undefined && limit !== null) {
      params = params.set('limit', limit.toString());
    }

    return this.http.get<FetchMessagesResponse>(url, { params });
  }
}
