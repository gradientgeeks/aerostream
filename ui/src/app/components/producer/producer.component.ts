import { Component, OnInit, ViewChild, computed, inject, signal } from '@angular/core';
import { CommonModule, DatePipe, NgClass, NgFor, NgIf } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { MatBadgeModule } from '@angular/material/badge';
import { AeroStreamService } from '../../services/aeromq.service';
import { Topic } from '../../models/aeromq.models';

export interface PublishedRecord {
  id: string;
  topic: string;
  partition: number;
  payload: string;
  offset?: number;
  timestamp: Date;
  latencyMs: number;
  status: 'success' | 'failed';
  error?: string;
}

const STORAGE_KEY_PRODUCER_HISTORY = 'aeromq_producer_history';

@Component({
  selector: 'app-producer',
  standalone: true,
  imports: [
    CommonModule,
    NgIf,
    NgFor,
    NgClass,
    DatePipe,
    FormsModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatProgressBarModule,
    MatButtonToggleModule,
    MatTooltipModule,
    MatSnackBarModule,
    MatTableModule,
    MatPaginatorModule,
    MatBadgeModule,
  ],
  templateUrl: './producer.component.html',
  styleUrl: './producer.component.scss',
})
export class ProducerComponent implements OnInit {
  protected readonly service = inject(AeroStreamService);
  private readonly snackBar = inject(MatSnackBar);

  // Form State Signals
  readonly topicName = signal<string>('');
  readonly partition = signal<number>(0);
  readonly payload = signal<string>('{\n  "event": "user.signup",\n  "userId": "usr_1001",\n  "timestamp": 1727338000\n}');
  readonly produceMode = signal<'single' | 'burst'>('single');
  readonly burstCount = signal<number>(10);

  // Loading & Execution State
  readonly isPublishing = signal<boolean>(false);
  readonly burstProgress = signal<{ current: number; total: number; successCount: number; failureCount: number } | null>(null);
  readonly availableTopics = signal<Topic[]>([]);
  readonly isLoadingTopics = signal<boolean>(false);

  // History State
  readonly history = signal<PublishedRecord[]>([]);
  readonly selectedRecord = signal<PublishedRecord | null>(null);
  readonly historyDataSource = new MatTableDataSource<PublishedRecord>([]);

  @ViewChild(MatPaginator) set matPaginator(p: MatPaginator | undefined) {
    if (p) {
      this.historyDataSource.paginator = p;
    }
  }

  // History Columns
  readonly historyColumns: string[] = [
    'status',
    'topic',
    'partition',
    'offset',
    'latency',
    'timestamp',
    'actions',
  ];

  // Computed available partitions for selected topic
  readonly partitionOptions = computed<number[]>(() => {
    const selected = this.topicName().trim();
    if (!selected) return [0];

    const match = this.availableTopics().find((t) => t.name === selected);
    if (match && match.partitions && match.partitions.length > 0) {
      return match.partitions.map((p) => p.partition_id);
    }
    return [0, 1, 2, 3];
  });

  // Payload statistics
  readonly payloadStats = computed(() => {
    const text = this.payload();
    const bytes = new TextEncoder().encode(text).length;
    const lines = text ? text.split('\n').length : 0;
    let isValidJson = false;
    try {
      if (text.trim()) {
        JSON.parse(text);
        isValidJson = true;
      }
    } catch {
      isValidJson = false;
    }
    return { bytes, lines, isValidJson };
  });

  ngOnInit(): void {
    this.loadHistory();
    this.fetchTopics();
  }

  fetchTopics(): void {
    this.isLoadingTopics.set(true);
    this.service.getTopics().subscribe({
      next: (topics) => {
        this.availableTopics.set(topics);
        this.isLoadingTopics.set(false);
        if (!this.topicName() && topics.length > 0) {
          this.topicName.set(topics[0].name);
        }
      },
      error: () => {
        this.isLoadingTopics.set(false);
      },
    });
  }

  // Quick Sample Presets
  loadSample(type: 'json' | 'ping' | 'order'): void {
    if (type === 'json') {
      const sample = {
        event: 'user_signup',
        userId: `usr_${Math.floor(1000 + Math.random() * 9000)}`,
        email: 'developer@aerostream.io',
        plan: 'enterprise',
        timestamp: Math.floor(Date.now() / 1000),
      };
      this.payload.set(JSON.stringify(sample, null, 2));
    } else if (type === 'ping') {
      const ping = `PING - AeroStream cluster heartbeat probe at ${new Date().toISOString()}`;
      this.payload.set(ping);
    } else if (type === 'order') {
      const order = {
        orderId: `ORD-${Date.now().toString(36).toUpperCase()}`,
        customerId: `cust_${Math.floor(100 + Math.random() * 900)}`,
        currency: 'USD',
        total: 129.99,
        items: [
          { sku: 'AEROSTREAM-LICENSE-PRO', qty: 1, unitPrice: 129.99 },
        ],
        status: 'PENDING',
        created_at: new Date().toISOString(),
      };
      this.payload.set(JSON.stringify(order, null, 2));
    }
  }

  formatJson(): void {
    try {
      const parsed = JSON.parse(this.payload());
      this.payload.set(JSON.stringify(parsed, null, 2));
      this.snackBar.open('JSON formatted cleanly', 'Close', { duration: 2000 });
    } catch (e: any) {
      this.snackBar.open(`Invalid JSON: ${e.message}`, 'Close', { duration: 3000 });
    }
  }

  clearPayload(): void {
    this.payload.set('');
  }

  // Publish Message(s)
  onPublish(): void {
    const topic = this.topicName().trim();
    const partition = this.partition();
    const payload = this.payload();

    if (!topic) {
      this.snackBar.open('Please specify a target topic name', 'Close', { duration: 3000 });
      return;
    }

    if (!payload) {
      this.snackBar.open('Please enter a message payload to send', 'Close', { duration: 3000 });
      return;
    }

    if (this.produceMode() === 'burst') {
      this.executeBurstPublish(topic, partition, payload);
    } else {
      this.executeSinglePublish(topic, partition, payload);
    }
  }

  private executeSinglePublish(topic: string, partition: number, message: string): void {
    this.isPublishing.set(true);
    const startTime = performance.now();

    this.service.produceMessage(topic, partition, message).subscribe({
      next: (resp) => {
        const latencyMs = Math.round(performance.now() - startTime);
        const record: PublishedRecord = {
          id: Math.random().toString(36).substring(2, 9),
          topic,
          partition,
          payload: message,
          offset: resp.offset,
          timestamp: new Date(),
          latencyMs,
          status: 'success',
        };

        this.addHistory(record);
        this.isPublishing.set(false);
        this.snackBar.open(
          `Message published to ${topic}[${partition}] at offset ${resp.offset} (${latencyMs}ms)`,
          'Dismiss',
          { duration: 4000 }
        );
      },
      error: (err) => {
        const latencyMs = Math.round(performance.now() - startTime);
        const errMsg = err?.error || err?.message || 'Failed to produce record to broker';
        const record: PublishedRecord = {
          id: Math.random().toString(36).substring(2, 9),
          topic,
          partition,
          payload: message,
          timestamp: new Date(),
          latencyMs,
          status: 'failed',
          error: errMsg,
        };

        this.addHistory(record);
        this.isPublishing.set(false);
        this.snackBar.open(`Produce failed: ${errMsg}`, 'Dismiss', { duration: 5000 });
      },
    });
  }

  private async executeBurstPublish(topic: string, partition: number, baseMessage: string): Promise<void> {
    const count = this.burstCount();
    this.isPublishing.set(true);
    this.burstProgress.set({ current: 0, total: count, successCount: 0, failureCount: 0 });

    let isJson = false;
    let baseObj: any = null;
    try {
      baseObj = JSON.parse(baseMessage);
      isJson = true;
    } catch {
      isJson = false;
    }

    for (let i = 1; i <= count; i++) {
      let msgToSend = baseMessage;
      if (isJson) {
        msgToSend = JSON.stringify({ ...baseObj, burst_seq: i, burst_batch_id: Date.now() });
      } else {
        msgToSend = `${baseMessage} [seq=${i}/${count}]`;
      }

      const startTime = performance.now();
      try {
        const resp = await this.service.produceMessage(topic, partition, msgToSend).toPromise();
        const latencyMs = Math.round(performance.now() - startTime);
        const record: PublishedRecord = {
          id: Math.random().toString(36).substring(2, 9),
          topic,
          partition,
          payload: msgToSend,
          offset: resp?.offset,
          timestamp: new Date(),
          latencyMs,
          status: 'success',
        };
        this.addHistory(record);

        const currentProg = this.burstProgress()!;
        this.burstProgress.set({
          ...currentProg,
          current: i,
          successCount: currentProg.successCount + 1,
        });
      } catch (err: any) {
        const latencyMs = Math.round(performance.now() - startTime);
        const errMsg = err?.error || err?.message || 'Failed';
        const record: PublishedRecord = {
          id: Math.random().toString(36).substring(2, 9),
          topic,
          partition,
          payload: msgToSend,
          timestamp: new Date(),
          latencyMs,
          status: 'failed',
          error: errMsg,
        };
        this.addHistory(record);

        const currentProg = this.burstProgress()!;
        this.burstProgress.set({
          ...currentProg,
          current: i,
          failureCount: currentProg.failureCount + 1,
        });
      }

      // Small throttle between burst messages
      await new Promise((res) => setTimeout(res, 20));
    }

    const finalProg = this.burstProgress()!;
    this.isPublishing.set(false);
    this.snackBar.open(
      `Burst test finished: ${finalProg.successCount} succeeded, ${finalProg.failureCount} failed`,
      'Close',
      { duration: 5000 }
    );
  }

  // History Management
  private addHistory(record: PublishedRecord): void {
    const updated = [record, ...this.history().slice(0, 199)];
    this.history.set(updated);
    this.historyDataSource.data = updated;
    this.saveHistory(updated);
  }

  private saveHistory(records: PublishedRecord[]): void {
    try {
      localStorage.setItem(STORAGE_KEY_PRODUCER_HISTORY, JSON.stringify(records));
    } catch {
      // LocalStorage error ignore
    }
  }

  private loadHistory(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY_PRODUCER_HISTORY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          const list = parsed.map((item: any) => ({
            ...item,
            timestamp: new Date(item.timestamp),
          }));
          this.history.set(list);
          this.historyDataSource.data = list;
          return;
        }
      }
    } catch {
      // LocalStorage error ignore
    }
    this.history.set([]);
    this.historyDataSource.data = [];
  }

  clearHistory(): void {
    this.history.set([]);
    this.historyDataSource.data = [];
    this.selectedRecord.set(null);
    localStorage.removeItem(STORAGE_KEY_PRODUCER_HISTORY);
    this.snackBar.open('History cleared', 'Close', { duration: 2000 });
  }

  repopulateFromHistory(rec: PublishedRecord): void {
    this.topicName.set(rec.topic);
    this.partition.set(rec.partition);
    this.payload.set(rec.payload);
    this.snackBar.open('Copied record parameters into publisher', 'Close', { duration: 2000 });
  }

  viewRecordDetail(rec: PublishedRecord): void {
    this.selectedRecord.set(this.selectedRecord()?.id === rec.id ? null : rec);
  }
}
