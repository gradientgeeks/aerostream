import { Component, computed, inject } from '@angular/core';
import { CommonModule, DatePipe, NgClass, NgFor, NgIf } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule } from '@angular/material/table';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { RouterModule } from '@angular/router';
import { AeroMQService } from '../../services/aeromq.service';
import { Broker } from '../../models/aeromq.models';

@Component({
  selector: 'app-cluster-overview',
  standalone: true,
  imports: [
    NgIf,
    NgClass,
    DatePipe,
    RouterModule,
    MatCardModule,
    MatTableModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatTooltipModule,
    MatProgressBarModule,
  ],
  templateUrl: './cluster-overview.component.html',
  styleUrl: './cluster-overview.component.scss',
})
export class ClusterOverviewComponent {
  protected readonly service = inject(AeroMQService);

  readonly displayedColumns: string[] = ['id', 'address', 'status', 'last_seen', 'actions'];
  readonly kafkaSupportedClients = ['kafka-python', 'librdkafka', 'Spring Kafka', 'confluent-kafka'];

  // Cluster Status calculations
  readonly clusterState = computed(() => {
    if (!this.service.isConnected()) {
      return { text: 'Disconnected', color: 'warn', icon: 'cloud_off' };
    }
    const info = this.service.clusterInfo();
    const activeBrokers = this.service.activeBrokersCount();
    const totalBrokers = this.service.totalBrokersCount();

    if (totalBrokers === 0) {
      return { text: 'No Brokers Registered', color: 'accent', icon: 'warning' };
    }
    if (activeBrokers < totalBrokers) {
      return { text: 'Degraded', color: 'accent', icon: 'error_outline' };
    }
    return { text: 'Optimal', color: 'primary', icon: 'check_circle' };
  });

  onRefresh(): void {
    this.service.refresh().subscribe();
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
    return date.toLocaleTimeString();
  }

  trackByBrokerId(_index: number, broker: Broker): number {
    return broker.id;
  }
}
