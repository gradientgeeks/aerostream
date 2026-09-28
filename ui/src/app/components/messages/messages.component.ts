import { Component, OnInit, inject, signal } from '@angular/core';
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
          this.isFetchingMessages.set(false);
        },
        error: (err) => {
          this.isFetchingMessages.set(false);
          this.messagesDataSource.data = [];
          const msg =
            err.error?.message ||
            err.error ||
            err.message ||
            'Failed to fetch messages from leader broker';
          this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
        }
      });
  }

  viewFullMessage(message: MessageRecord): void {
    this.dialog.open(MessageDetailDialogComponent, {
      width: '720px',
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
    let cleaned = payload;
    if (/[\x00-\x08\x0E-\x1F]/.test(cleaned)) {
      const jsonMatch = cleaned.match(/(\{[\s\S]*\}|\[[\s\S]*\])/);
      if (jsonMatch) {
        cleaned = jsonMatch[0];
      } else {
        cleaned = cleaned.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '');
      }
    }
    cleaned = cleaned.replace(/\r?\n|\r/g, ' ');
    return cleaned.length > 80 ? cleaned.substring(0, 80) + '...' : cleaned;
  }
}
