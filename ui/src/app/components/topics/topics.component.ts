import { Component, OnInit, ViewChild, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { animate, state, style, transition, trigger } from '@angular/animations';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatChipsModule } from '@angular/material/chips';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatCardModule } from '@angular/material/card';
import { RouterModule } from '@angular/router';

import { ApiService } from '../../services/api.service';
import { SchemaService } from '../../services/schema.service';
import { TopicInfo, PartitionInfo } from '../../models/topic.model';
import { CreateTopicDialogComponent } from './create-topic-dialog.component';

@Component({
  selector: 'app-topics',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    MatTableModule,
    MatSortModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatIconModule,
    MatDialogModule,
    MatSnackBarModule,
    MatChipsModule,
    MatExpansionModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatCardModule
  ],
  templateUrl: './topics.component.html',
  styleUrl: './topics.component.scss',
  animations: [
    trigger('detailExpand', [
      state('collapsed,void', style({ height: '0px', minHeight: '0', opacity: '0' })),
      state('expanded', style({ height: '*', opacity: '1' })),
      transition('expanded <=> collapsed', animate('225ms cubic-bezier(0.4, 0.0, 0.2, 1)')),
      transition('expanded <=> void', animate('225ms cubic-bezier(0.4, 0.0, 0.2, 1)'))
    ])
  ]
})
export class TopicsComponent implements OnInit {
  private apiService = inject(ApiService);
  protected readonly schemaService = inject(SchemaService);
  private dialog = inject(MatDialog);
  private snackBar = inject(MatSnackBar);
  private router = inject(Router);

  displayedColumns: string[] = [
    'name',
    'cleanupPolicy',
    'partitionCount',
    'replicationFactor',
    'highWatermarkTotal',
    'schema',
    'actions'
  ];

  dataSource = new MatTableDataSource<TopicInfo>([]);
  isLoading = signal(true);
  errorMessage = signal<string | null>(null);
  expandedTopic = signal<TopicInfo | null>(null);
  hasFilter = signal(false);

  @ViewChild(MatSort) sort!: MatSort;

  ngOnInit(): void {
    this.loadTopics();
  }

  loadTopics(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.apiService.getTopics().subscribe({
      next: (topics) => {
        this.dataSource.data = topics;
        this.dataSource.sort = this.sort;
        this.dataSource.filterPredicate = (data: TopicInfo, filter: string) => {
          return data.name.toLowerCase().includes(filter.trim().toLowerCase());
        };
        this.isLoading.set(false);
      },
      error: (err) => {
        this.isLoading.set(false);
        const msg = err.error?.message || err.message || 'Failed to connect to AeroStream controller';
        this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
      }
    });
  }

  applyFilter(event: Event): void {
    const filterValue = (event.target as HTMLInputElement).value;
    const trimmed = filterValue.trim().toLowerCase();
    this.dataSource.filter = trimmed;
    this.hasFilter.set(trimmed.length > 0);
  }

  toggleExpand(topic: TopicInfo): void {
    if (this.expandedTopic()?.name === topic.name) {
      this.expandedTopic.set(null);
    } else {
      this.expandedTopic.set(topic);
    }
  }

  openCreateDialog(): void {
    const dialogRef = this.dialog.open(CreateTopicDialogComponent, {
      width: '100%',
      maxWidth: '32rem',
      disableClose: false
    });

    dialogRef.afterClosed().subscribe((result) => {
      if (result?.success) {
        this.snackBar.open(`Topic "${result.name}" created successfully!`, 'Close', {
          duration: 4000,
          horizontalPosition: 'end',
          verticalPosition: 'bottom',
          panelClass: ['snackbar-success']
        });
        this.loadTopics();
      }
    });
  }

  viewMessages(topic: TopicInfo, partitionId?: number): void {
    const queryParams: Record<string, string | number> = { topic: topic.name };
    if (partitionId !== undefined) {
      queryParams['partition'] = partitionId;
    }
    this.router.navigate(['/messages'], { queryParams });
  }

  isIsrHealthy(partition: PartitionInfo): boolean {
    const isrCount = partition.isr ? partition.isr.length : 0;
    const repCount = partition.replica_ids ? partition.replica_ids.length : 0;
    return isrCount >= repCount && repCount > 0;
  }

  getCleanupPolicy(topic: TopicInfo): 'delete' | 'compact' {
    if (topic.cleanup_policy) return topic.cleanup_policy;
    const lower = topic.name.toLowerCase();
    if (lower.includes('compact') || lower.includes('state') || lower.includes('order') || lower.includes('payment')) {
      return 'compact';
    }
    return 'delete';
  }

  hasSchema(topic: TopicInfo): boolean {
    return this.schemaService.getSchemaForTopic(topic.name) !== undefined;
  }

  getSchemaForTopic(topic: TopicInfo) {
    return this.schemaService.getSchemaForTopic(topic.name);
  }

  getSchemaSubject(topic: TopicInfo): string {
    const s = this.schemaService.getSchemaForTopic(topic.name);
    return s ? s.subject : `${topic.name}-value`;
  }

  getSchemaType(topic: TopicInfo): string {
    const s = this.schemaService.getSchemaForTopic(topic.name);
    return s ? s.type : 'AVRO';
  }

  getRetentionPeriod(topic: TopicInfo): string {
    if (!topic.retention_period) return '7 Days';
    const map: Record<string, string> = {
      '1d': '1 Day',
      '3d': '3 Days',
      '7d': '7 Days',
      '14d': '14 Days',
      '30d': '30 Days',
      'infinite': 'Infinite'
    };
    return map[topic.retention_period.toLowerCase()] || topic.retention_period;
  }

  getRetentionQuota(topic: TopicInfo): string {
    if (!topic.retention_size) return '1 GB';
    const map: Record<string, string> = {
      '512mb': '512 MB',
      '1gb': '1 GB',
      '5gb': '5 GB',
      '10gb': '10 GB',
      'unlimited': 'Unlimited'
    };
    return map[topic.retention_size.toLowerCase()] || topic.retention_size;
  }

  getSegmentSize(topic: TopicInfo): string {
    if (!topic.segment_size) return '128 MB';
    const map: Record<string, string> = {
      '64mb': '64 MB',
      '128mb': '128 MB',
      '256mb': '256 MB',
      '512mb': '512 MB'
    };
    return map[topic.segment_size.toLowerCase()] || topic.segment_size;
  }
}
