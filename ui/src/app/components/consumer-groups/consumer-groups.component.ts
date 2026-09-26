import { Component, OnDestroy, OnInit, computed, inject, signal } from '@angular/core';
import { CommonModule, DatePipe, DecimalPipe, NgClass, NgFor, NgIf } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule } from '@angular/material/table';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatBadgeModule } from '@angular/material/badge';
import { MatTabsModule } from '@angular/material/tabs';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { Subject, Subscription, catchError, forkJoin, of, timer } from 'rxjs';
import { AeroMQService } from '../../services/aeromq.service';
import { ConsumerGroup, ConsumerGroupMember, PartitionLag } from '../../models/aeromq.models';

export interface GroupDisplayInfo {
  groupId: string;
  generation: number;
  protocol: string;
  state: string;
  leaderId: string;
  rebalanceCount: number;
  lastRebalanceTime?: string;
  members: ConsumerGroupMember[];
  memberCount: number;
  subscribedTopics: string[];
  totalLag: number;
  maxLag: number;
  partitionCount: number;
  status: 'optimal' | 'warning' | 'critical';
}

export interface MemberAssignmentDisplay {
  topic: string;
  partitions: number[];
}

@Component({
  selector: 'app-consumer-groups',
  standalone: true,
  imports: [
    CommonModule,
    NgIf,
    NgFor,
    NgClass,
    DatePipe,
    DecimalPipe,
    FormsModule,
    RouterModule,
    MatCardModule,
    MatTableModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatTooltipModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatFormFieldModule,
    MatInputModule,
    MatBadgeModule,
    MatTabsModule,
    MatSnackBarModule,
  ],
  templateUrl: './consumer-groups.component.html',
  styleUrl: './consumer-groups.component.scss',
})
export class ConsumerGroupsComponent implements OnInit, OnDestroy {
  protected readonly service = inject(AeroMQService);
  private readonly snackBar = inject(MatSnackBar);
  private readonly destroy$ = new Subject<void>();
  private pollingSub: Subscription | null = null;

  // State Signals
  readonly groups = signal<ConsumerGroup[]>([]);
  readonly allLag = signal<PartitionLag[]>([]);
  readonly isLoading = signal<boolean>(false);
  readonly isRebalancing = signal<boolean>(false);
  readonly errorMessage = signal<string | null>(null);
  readonly lastRefreshed = signal<Date | null>(null);

  // User Selection & Filters
  readonly selectedGroupId = signal<string | null>(null);
  readonly searchQuery = signal<string>('');
  readonly statusFilter = signal<'all' | 'lagging' | 'synced'>('all');

  // Columns for Groups Table
  readonly groupColumns: string[] = [
    'groupId',
    'generation',
    'protocol',
    'state',
    'membersCount',
    'topics',
    'totalLag',
    'status',
    'actions',
  ];

  // Columns for Lag Breakdown Table
  readonly lagColumns: string[] = [
    'topic',
    'partition',
    'committedOffset',
    'highWatermark',
    'lag',
    'status',
  ];

  // Computed: Enriched Groups
  readonly enrichedGroups = computed<GroupDisplayInfo[]>(() => {
    const rawGroups = this.groups() ?? [];
    const lagData = this.allLag() ?? [];

    return rawGroups.map((group) => {
      const groupLags = lagData.filter((l) => l?.group_id === group?.group_id);

      // Collect topics from members and lag data
      const topicSet = new Set<string>();
      group?.members?.forEach((m) => {
        m?.topics?.forEach((t) => topicSet.add(t));
        if (m?.assignments) {
          Object.keys(m.assignments).forEach((t) => topicSet.add(t));
        }
      });
      if (group?.assignments) {
        Object.values(group.assignments).forEach((memberMap: any) => {
          if (Array.isArray(memberMap)) {
            memberMap.forEach((pt: any) => {
              if (pt?.topic) topicSet.add(pt.topic);
            });
          } else if (memberMap && typeof memberMap === 'object') {
            Object.keys(memberMap).forEach((t) => topicSet.add(t));
          }
        });
      }
      groupLags.forEach((l) => {
        if (l?.topic) topicSet.add(l.topic);
      });

      const totalLag = groupLags.reduce((acc, curr) => acc + (curr?.lag || 0), 0);
      const maxLag = groupLags.reduce((max, curr) => Math.max(max, curr?.lag || 0), 0);

      let status: 'optimal' | 'warning' | 'critical' = 'optimal';
      if (maxLag > 50) {
        status = 'critical';
      } else if (maxLag > 0) {
        status = 'warning';
      }

      return {
        groupId: group?.group_id || '',
        generation: group?.generation || 0,
        protocol: group?.protocol || 'COOPERATIVE_STICKY',
        state: group?.state || 'STABLE',
        leaderId: group?.leader_id || (group?.members && group.members.length > 0 ? group.members[0].id : ''),
        rebalanceCount: group?.rebalance_count || 0,
        lastRebalanceTime: group?.last_rebalance_time,
        members: group?.members || [],
        memberCount: group?.members ? group.members.length : 0,
        subscribedTopics: Array.from(topicSet),
        totalLag,
        maxLag,
        partitionCount: groupLags.length,
        status,
      };
    });
  });

  // Filtered Groups based on search and status
  readonly filteredGroups = computed<GroupDisplayInfo[]>(() => {
    const query = this.searchQuery()?.trim().toLowerCase() || '';
    const filter = this.statusFilter();
    let list = this.enrichedGroups() ?? [];

    if (query) {
      list = list.filter(
        (g) =>
          g?.groupId?.toLowerCase().includes(query) ||
          g?.subscribedTopics?.some((t) => t?.toLowerCase().includes(query))
      );
    }

    if (filter === 'lagging') {
      list = list.filter((g) => (g?.totalLag || 0) > 0);
    } else if (filter === 'synced') {
      list = list.filter((g) => (g?.totalLag || 0) === 0);
    }

    return list;
  });

  // Active selected group object
  readonly activeGroup = computed<GroupDisplayInfo | null>(() => {
    const selectedId = this.selectedGroupId();
    const enriched = this.enrichedGroups() ?? [];
    if (!selectedId) {
      return enriched.length > 0 ? enriched[0] : null;
    }
    return enriched.find((g) => g?.groupId === selectedId) || (enriched.length > 0 ? enriched[0] : null);
  });

  // Selected group raw data (for raw member assignments)
  readonly activeRawGroup = computed<ConsumerGroup | null>(() => {
    const active = this.activeGroup();
    if (!active) return null;
    return (this.groups() ?? []).find((g) => g?.group_id === active.groupId) || null;
  });

  // Lag breakdown for the active group
  readonly activeGroupLag = computed<PartitionLag[]>(() => {
    const active = this.activeGroup();
    if (!active) return [];
    return (this.allLag() ?? []).filter((l) => l?.group_id === active.groupId);
  });

  // Summary Metrics
  readonly summaryStats = computed(() => {
    const groups = this.enrichedGroups() ?? [];
    const lagData = this.allLag() ?? [];
    const totalMembers = groups.reduce((acc, g) => acc + (g?.memberCount || 0), 0);
    const totalLag = lagData.reduce((acc, l) => acc + (l?.lag || 0), 0);
    const maxLag = lagData.reduce((max, l) => Math.max(max, l?.lag || 0), 0);
    const laggingPartitionsCount = lagData.filter((l) => (l?.lag || 0) > 0).length;

    return {
      groupCount: groups.length,
      memberCount: totalMembers,
      monitoredPartitionsCount: lagData.length,
      totalLag,
      maxLag,
      laggingPartitionsCount,
    };
  });

  ngOnInit(): void {
    this.fetchData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.stopAutoRefresh();
  }

  private startAutoRefresh(): void {
    this.stopAutoRefresh();
    this.pollingSub = timer(3000, 3000).subscribe(() => {
      if (this.service.autoRefreshEnabled()) {
        this.fetchData(false);
      }
    });
  }

  private stopAutoRefresh(): void {
    if (this.pollingSub) {
      this.pollingSub.unsubscribe();
      this.pollingSub = null;
    }
  }

  fetchData(showLoading = true): void {
    if (showLoading) {
      this.isLoading.set(true);
    }
    this.errorMessage.set(null);

    forkJoin({
      groups: this.service.getConsumerGroups().pipe(
        catchError((err) => {
          console.error('Failed to fetch consumer groups', err);
          return of([] as ConsumerGroup[]);
        })
      ),
      lag: this.service.getLag().pipe(
        catchError((err) => {
          console.error('Failed to fetch partition lag', err);
          return of([] as PartitionLag[]);
        })
      ),
    }).subscribe({
      next: ({ groups, lag }) => {
        const safeGroups = Array.isArray(groups) ? groups : [];
        const safeLag = Array.isArray(lag) ? lag : [];
        this.groups.set(safeGroups);
        this.allLag.set(safeLag);
        this.lastRefreshed.set(new Date());
        this.isLoading.set(false);

        // Keep or select first group
        if (!this.selectedGroupId() && safeGroups.length > 0) {
          this.selectedGroupId.set(safeGroups[0].group_id);
        }
      },
      error: (err) => {
        this.errorMessage.set(err?.message || 'Failed to load consumer group data');
        this.isLoading.set(false);
      },
    });
  }

  selectGroup(groupId: string): void {
    this.selectedGroupId.set(groupId);
  }

  getMemberAssignments(member: ConsumerGroupMember): MemberAssignmentDisplay[] {
    const raw = this.activeRawGroup();
    const result: MemberAssignmentDisplay[] = [];

    // Check member's direct assignments
    const assignments = member.assignments || (raw?.assignments ? raw.assignments[member.id] : undefined);
    if (assignments) {
      if (Array.isArray(assignments)) {
        for (const item of assignments as any[]) {
          if (item?.topic) {
            result.push({ topic: item.topic, partitions: item.partition !== undefined ? [item.partition] : [] });
          }
        }
      } else if (typeof assignments === 'object') {
        for (const [topic, partitions] of Object.entries(assignments)) {
          result.push({ topic, partitions: Array.isArray(partitions) ? (partitions as number[]) : [] });
        }
      }
    }

    // Fallback if no specific partition assignments but topics subscribed
    if (result.length === 0 && member.topics && member.topics.length > 0) {
      member.topics.forEach((topic) => {
        result.push({ topic, partitions: [] });
      });
    }

    return result;
  }

  formatLastSeen(lastSeen: string | number | Date | null | undefined): string {
    if (!lastSeen) return 'Never';
    let date: Date;
    if (typeof lastSeen === 'number') {
      date = new Date(lastSeen < 1e11 ? lastSeen * 1000 : lastSeen);
    } else {
      date = new Date(lastSeen);
    }
    if (isNaN(date.getTime())) return 'Never';

    const diffSeconds = Math.floor((Date.now() - date.getTime()) / 1000);
    if (diffSeconds < 5) return 'Just now';
    if (diffSeconds < 60) return `${diffSeconds}s ago`;
    if (diffSeconds < 3600) return `${Math.floor(diffSeconds / 60)}m ago`;
    return `${Math.floor(diffSeconds / 3600)}h ago`;
  }

  getLagChipClass(lag: number): string {
    if (lag === 0) return 'chip-green';
    if (lag <= 50) return 'chip-orange';
    return 'chip-red';
  }

  getLagChipLabel(lag: number): string {
    if (lag === 0) return '0 (Up to date)';
    if (lag <= 50) return `${lag} (Moderate Lag)`;
    return `${lag} (High Lag)`;
  }

  triggerRebalance(groupId: string): void {
    if (!groupId || this.isRebalancing()) return;
    this.isRebalancing.set(true);
    this.service.triggerRebalance(groupId).subscribe({
      next: (resp) => {
        this.isRebalancing.set(false);
        this.snackBar.open(
          `Cooperative rebalance completed for ${groupId}! Gen #${resp.generation ?? 'updated'} (${resp.state || 'STABLE'})`,
          'Dismiss',
          { duration: 4000, panelClass: ['snackbar-success'] }
        );
        this.fetchData(false);
      },
      error: (err) => {
        this.isRebalancing.set(false);
        this.snackBar.open(
          `Rebalance failed: ${err?.message || 'Server error'}`,
          'Dismiss',
          { duration: 4000, panelClass: ['snackbar-error'] }
        );
      },
    });
  }

  getMemberPartitionCount(member: ConsumerGroupMember): number {
    const assignments = this.getMemberAssignments(member);
    return assignments.reduce((acc, a) => acc + (a.partitions ? a.partitions.length : 0), 0);
  }

  getTotalAssignedPartitions(): number {
    const raw = this.activeRawGroup();
    if (!raw || !raw.members) return 0;
    return raw.members.reduce((acc, m) => acc + this.getMemberPartitionCount(m), 0);
  }

  getMemberLoadPercentage(member: ConsumerGroupMember): number {
    const total = this.getTotalAssignedPartitions();
    if (total === 0) return 0;
    const count = this.getMemberPartitionCount(member);
    return Math.round((count / total) * 100);
  }

  getLoadBarClass(percentage: number): string {
    if (percentage > 60) return 'load-heavy';
    if (percentage > 30) return 'load-balanced';
    return 'load-light';
  }
}
