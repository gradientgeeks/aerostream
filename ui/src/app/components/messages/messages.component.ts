import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatCardModule } from '@angular/material/card';

import { ApiService } from '../../services/api.service';
import { TopicInfo, PartitionInfo } from '../../models/topic.model';
import { MessageRecord } from '../../models/message.model';
import { MessageDetailDialogComponent } from './message-detail-dialog.component';

@Component({
  selector: 'app-messages',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatTableModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatProgressSpinnerModule,
    MatDialogModule,
    MatTooltipModule,
    MatCardModule
  ],
  templateUrl: './messages.component.html',
  styleUrl: './messages.component.scss'
})
export class MessagesComponent implements OnInit {
  private apiService = inject(ApiService);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private dialog = inject(MatDialog);

  topics = signal<TopicInfo[]>([]);
  selectedTopic = signal<string>('');
  selectedPartition = signal<number>(0);
  startOffset = signal<number>(0);
  limit = signal<number>(25);

  limitOptions = [10, 25, 50, 100];

  availablePartitions = signal<PartitionInfo[]>([]);
  currentPartitionInfo = signal<PartitionInfo | null>(null);

  messagesDataSource = new MatTableDataSource<MessageRecord>([]);
  displayedColumns: string[] = ['offset', 'key', 'length', 'payload', 'actions'];

  isLoadingTopics = signal(false);
  isFetchingMessages = signal(false);
  hasFetched = signal(false);
  errorMessage = signal<string | null>(null);
  totalMessagesCount = signal<number>(0);

  readonly currentPage = computed(() => {
    const lim = this.limit();
    if (lim <= 0) return 1;
    return Math.floor(this.startOffset() / lim) + 1;
  });

  readonly hasPreviousPage = computed(() => {
    return this.startOffset() > 0;
  });

  readonly hasNextPage = computed(() => {
    const dataLen = this.totalMessagesCount();
    const hw = this.currentPartitionInfo()?.high_watermark || 0;
    return dataLen >= this.limit() && (this.startOffset() + dataLen) < hw;
  });

  ngOnInit(): void {
    this.loadTopicsAndInitParams();
  }

  loadTopicsAndInitParams(): void {
    this.isLoadingTopics.set(true);
    this.errorMessage.set(null);

    this.apiService.getTopics().subscribe({
      next: (topics) => {
        this.topics.set(topics);
        this.isLoadingTopics.set(false);

        // Check query parameters
        this.route.queryParams.subscribe((params) => {
          const qTopic = params['topic'];
          const qPartition = params['partition'];
          const qOffset = params['offset'];

          if (qTopic && topics.some((t) => t.name === qTopic)) {
            this.selectedTopic.set(qTopic);
            this.updatePartitionsForSelectedTopic();

            if (qPartition !== undefined) {
              const pNum = Number(qPartition);
              if (this.availablePartitions().some((p) => p.partition_id === pNum)) {
                this.selectedPartition.set(pNum);
              }
            }

            if (qOffset !== undefined) {
              this.startOffset.set(Math.max(0, Number(qOffset) || 0));
            }

            this.updateCurrentPartitionInfo();
            // Automatically fetch messages if topic was passed in URL
            this.fetchMessages();
          } else if (topics.length > 0) {
            // Default to first topic
            this.selectedTopic.set(topics[0].name);
            this.updatePartitionsForSelectedTopic();
            this.updateCurrentPartitionInfo();
          }
        });
      },
      error: (err) => {
        this.isLoadingTopics.set(false);
        const msg = err.error?.message || err.message || 'Failed to load topics';
        this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
      }
    });
  }

  onTopicChange(topicName: string): void {
    this.selectedTopic.set(topicName);
    this.updatePartitionsForSelectedTopic();
    this.selectedPartition.set(0);
    this.startOffset.set(0);
    this.updateCurrentPartitionInfo();
    this.updateQueryParams();
    this.messagesDataSource.data = [];
    this.hasFetched.set(false);
  }

  onPartitionChange(partitionId: number): void {
    this.selectedPartition.set(partitionId);
    this.startOffset.set(0);
    this.updateCurrentPartitionInfo();
    this.updateQueryParams();
    this.messagesDataSource.data = [];
    this.hasFetched.set(false);
  }

  private updatePartitionsForSelectedTopic(): void {
    const t = this.topics().find((x) => x.name === this.selectedTopic());
    const partitions = t?.partitions || [];
    this.availablePartitions.set(partitions);
    if (!partitions.some((p) => p.partition_id === this.selectedPartition())) {
      this.selectedPartition.set(partitions.length > 0 ? partitions[0].partition_id : 0);
    }
  }

  private updateCurrentPartitionInfo(): void {
    const p = this.availablePartitions().find(
      (part) => part.partition_id === this.selectedPartition()
    );
    this.currentPartitionInfo.set(p || null);
  }

  private updateQueryParams(): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        topic: this.selectedTopic(),
        partition: this.selectedPartition(),
        offset: this.startOffset()
      },
      queryParamsHandling: 'merge'
    });
  }

  jumpToStart(): void {
    this.startOffset.set(0);
    this.updateQueryParams();
    this.fetchMessages();
  }

  jumpToLatest(): void {
    const hw = this.currentPartitionInfo()?.high_watermark || 0;
    const lim = this.limit();
    // Offset that fetches up to latest messages, or 0 if empty
    const targetOffset = Math.max(0, hw - lim);
    this.startOffset.set(targetOffset);
    this.updateQueryParams();
    this.fetchMessages();
  }

  fetchMessages(): void {
    const topic = this.selectedTopic();
    const partition = this.selectedPartition();
    if (!topic) {
      return;
    }

    this.isFetchingMessages.set(true);
    this.errorMessage.set(null);
    this.hasFetched.set(true);

    this.apiService
      .getMessages({
        topic,
        partition,
        offset: this.startOffset(),
        limit: this.limit()
      })
      .subscribe({
        next: (res) => {
          this.messagesDataSource.data = res.messages || [];
          this.totalMessagesCount.set(this.messagesDataSource.data.length);
          this.isFetchingMessages.set(false);
        },
        error: (err) => {
          this.isFetchingMessages.set(false);
          this.messagesDataSource.data = [];
          this.totalMessagesCount.set(0);
          const msg =
            err.error?.message ||
            err.error ||
            err.message ||
            'Failed to fetch messages from leader broker';
          this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
        }
      });
  }

  prevPage(): void {
    if (!this.hasPreviousPage() || this.isFetchingMessages()) return;
    const nextOffset = Math.max(0, this.startOffset() - this.limit());
    this.startOffset.set(nextOffset);
    this.updateQueryParams();
    this.fetchMessages();
  }

  nextPage(): void {
    if (this.isFetchingMessages()) return;
    const data = this.messagesDataSource.data;
    let nextOffset = this.startOffset() + this.limit();
    if (data.length > 0) {
      const maxOffset = Math.max(...data.map(m => m.offset));
      if (maxOffset >= this.startOffset()) {
        nextOffset = maxOffset + 1;
      }
    }
    this.startOffset.set(nextOffset);
    this.updateQueryParams();
    this.fetchMessages();
  }

  onLimitChange(newLimit: number): void {
    this.limit.set(newLimit);
    this.updateQueryParams();
    this.fetchMessages();
  }

  viewFullMessage(message: MessageRecord): void {
    this.dialog.open(MessageDetailDialogComponent, {
      width: '740px',
      maxWidth: '95vw',
      data: {
        topic: this.selectedTopic(),
        partition: this.selectedPartition(),
        offset: message.offset,
        length: message.length,
        payload: message.payload,
        key: message.key,
        timestamp: message.timestamp,
        headers: message.headers
      }
    });
  }

  getPayloadPreview(payload: string): string {
    if (!payload) return '<empty>';
    const hasBinary = /[\x00-\x08\x0E-\x1F\x7F-\xFF]/.test(payload);
    if (!hasBinary) {
      const trimmed = payload.trim();
      if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
        try {
          JSON.parse(trimmed);
          const singleLine = trimmed.replace(/\r?\n|\r/g, ' ');
          return singleLine.length > 80 ? singleLine.substring(0, 80) + '...' : singleLine;
        } catch {
          // Plain text fallback
        }
      }
      const singleLine = payload.replace(/\r?\n|\r/g, ' ');
      return singleLine.length > 80 ? singleLine.substring(0, 80) + '...' : singleLine;
    }

    // Binary payload: show clean hex snippet without corrupted unicode chars
    const byteCount = payload.length;
    const hexSnippet: string[] = [];
    for (let i = 0; i < Math.min(payload.length, 6); i++) {
      hexSnippet.push((payload.charCodeAt(i) & 0xff).toString(16).padStart(2, '0'));
    }
    return `[Binary: ${hexSnippet.join(' ')}${byteCount > 6 ? '...' : ''} (${byteCount} B)]`;
  }
}
