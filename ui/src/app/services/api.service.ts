import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { TopicInfo, CreateTopicRequest, CreateTopicResponse } from '../models/topic.model';
import { FetchMessagesParams, FetchMessagesResponse } from '../models/message.model';
import {
  StreamTransform,
  RegisterTransformRequest,
  TestTransformRequest,
  TestTransformResponse,
} from '../models/transform.model';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private http = inject(HttpClient);

  /**
   * Resolves the API endpoint URL.
   * If running in development (e.g. localhost:4200), defaults to http://localhost:9001
   * unless overridden by localStorage. In production or behind a reverse proxy,
   * uses relative paths.
   */
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

  /**
   * Fetches all topics with partition details, leader broker ID, replicas, ISR,
   * and high watermark.
   */
  getTopics(): Observable<TopicInfo[]> {
    const url = this.getApiUrl('/api/topics');
    return this.http.get<TopicInfo[]>(url).pipe(
      map(topics => {
        if (!Array.isArray(topics)) {
          return [];
        }
        return topics.map(t => {
          const partitions = t.partitions || [];
          const highWatermarkTotal = partitions.reduce((sum, p) => sum + (p.high_watermark || 0), 0);
          const replicationFactor = partitions.length > 0 && partitions[0].replica_ids
            ? partitions[0].replica_ids.length
            : 1;

          return {
            ...t,
            partitions,
            partitionCount: partitions.length,
            replicationFactor,
            highWatermarkTotal
          };
        });
      })
    );
  }

  /**
   * Creates a new topic on the AeroStream cluster.
   */
  createTopic(request: CreateTopicRequest): Observable<CreateTopicResponse> {
    const url = this.getApiUrl('/api/topics');
    return this.http.post<CreateTopicResponse>(url, request);
  }

  /**
   * Fetches messages from partition leader broker over TCP via controller REST API.
   */
  getMessages(params: FetchMessagesParams): Observable<FetchMessagesResponse> {
    const url = this.getApiUrl('/api/messages');
    let httpParams = new HttpParams()
      .set('topic', params.topic)
      .set('partition', params.partition.toString());

    if (params.offset !== undefined && params.offset !== null) {
      httpParams = httpParams.set('offset', params.offset.toString());
    }
    if (params.limit !== undefined && params.limit !== null) {
      httpParams = httpParams.set('limit', params.limit.toString());
    }

    return this.http.get<FetchMessagesResponse>(url, { params: httpParams });
  }

  /**
   * Fetches all registered stream transforms (WASM, FILTER, MASK_PII, JSON_MAP).
   */
  getTransforms(): Observable<StreamTransform[]> {
    const url = this.getApiUrl('/api/transforms');
    return this.http.get<StreamTransform[]>(url);
  }

  /**
   * Deploys a new stream transform.
   */
  registerTransform(request: RegisterTransformRequest): Observable<StreamTransform> {
    const url = this.getApiUrl('/api/transforms');
    return this.http.post<StreamTransform>(url, request);
  }

  /**
   * Pauses an active stream transform.
   */
  pauseTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}/pause`);
    return this.http.post(url, {});
  }

  /**
   * Resumes a paused stream transform.
   */
  resumeTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}/resume`);
    return this.http.post(url, {});
  }

  /**
   * Deletes a stream transform.
   */
  deleteTransform(name: string): Observable<any> {
    const url = this.getApiUrl(`/api/transforms/${encodeURIComponent(name)}`);
    return this.http.delete(url);
  }

  /**
   * Tests a stream transform with a sample JSON payload.
   */
  testTransform(request: TestTransformRequest): Observable<TestTransformResponse> {
    const url = this.getApiUrl('/api/transforms/test');
    return this.http.post<TestTransformResponse>(url, request);
  }
}

