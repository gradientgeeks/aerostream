import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatTabsModule } from '@angular/material/tabs';
import { MatDividerModule } from '@angular/material/divider';
import { MatMenuModule } from '@angular/material/menu';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

import { Connector, ConnectorPlugin, ConnectorState, ConnectorType } from '../../models/connector.model';
import { ConnectorService } from '../../services/connector.service';
import { DeployConnectorDialogComponent } from './deploy-connector-dialog.component';
import { PaginationComponent } from '../common/pagination.component';

@Component({
  selector: 'app-connectors',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    FormsModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatFormFieldModule,
    MatInputModule,
    MatDialogModule,
    MatSnackBarModule,
    MatTooltipModule,
    MatTabsModule,
    MatDividerModule,
    MatMenuModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    PaginationComponent,
  ],
  templateUrl: './connectors.component.html',
  styleUrl: './connectors.component.scss',
})
export class ConnectorsComponent implements OnInit {
  protected readonly connectorService = inject(ConnectorService);
  private readonly dialog = inject(MatDialog);
  private readonly snackBar = inject(MatSnackBar);

  readonly searchQuery = signal<string>('');
  readonly typeFilter = signal<string>('ALL');
  readonly stateFilter = signal<string>('ALL');
  readonly selectedConnector = signal<Connector | null>(null);
  readonly selectedTab = signal<number>(0);
  readonly pageIndex = signal<number>(0);
  readonly pageSize = signal<number>(10);

  // Filtered connectors
  readonly filteredConnectors = computed(() => {
    const list = this.connectorService.connectors();
    const query = this.searchQuery().trim().toLowerCase();
    const tFilter = this.typeFilter();
    const sFilter = this.stateFilter();

    return list.filter((c) => {
      const matchesQuery =
        !query ||
        c.name.toLowerCase().includes(query) ||
        c.class.toLowerCase().includes(query) ||
        (c.topic && c.topic.toLowerCase().includes(query));

      const matchesType = tFilter === 'ALL' || c.type === tFilter;
      const matchesState = sFilter === 'ALL' || c.state === sFilter;

      return matchesQuery && matchesType && matchesState;
    });
  });

  // Paginated slice
  readonly paginatedConnectors = computed(() => {
    const list = this.filteredConnectors();
    const start = this.pageIndex() * this.pageSize();
    return list.slice(start, start + this.pageSize());
  });

  ngOnInit(): void {
    this.loadConnectors();
    this.loadPlugins();
  }

  loadConnectors(): void {
    this.connectorService.loadConnectors().subscribe({
      error: (err) => {
        this.snackBar.open('Failed to load connectors from cluster.', 'Close', { duration: 3000 });
      },
    });
  }

  loadPlugins(): void {
    this.connectorService.loadPlugins().subscribe({
      error: (err) => {
        this.snackBar.open('Failed to load connector plugins from cluster.', 'Close', { duration: 3000 });
      },
    });
  }

  refresh(): void {
    this.loadConnectors();
    this.loadPlugins();
  }

  openDeployDialog(presetClass?: string, presetTopic?: string): void {
    const dialogRef = this.dialog.open(DeployConnectorDialogComponent, {
      width: '100%',
      maxWidth: '46rem',
      data: {
        presetClass,
        presetTopic,
      },
    });

    dialogRef.afterClosed().subscribe((res) => {
      if (res?.success && res.connector) {
        this.snackBar.open(
          `Connector "${res.connector.name}" deployed successfully!`,
          'Close',
          {
            duration: 4000,
            horizontalPosition: 'end',
            verticalPosition: 'bottom',
          }
        );
      }
    });
  }

  togglePause(c: Connector): void {
    if (c.state === 'RUNNING') {
      this.connectorService.pauseConnector(c.name).subscribe({
        next: () => {
          this.snackBar.open(`Connector "${c.name}" paused.`, 'OK', { duration: 2500 });
        },
        error: () => {
          this.snackBar.open(`Failed to pause connector "${c.name}".`, 'Close', { duration: 3000 });
        },
      });
    } else {
      this.connectorService.resumeConnector(c.name).subscribe({
        next: () => {
          this.snackBar.open(`Connector "${c.name}" resumed.`, 'OK', { duration: 2500 });
        },
        error: () => {
          this.snackBar.open(`Failed to resume connector "${c.name}".`, 'Close', { duration: 3000 });
        },
      });
    }
  }

  stopConnector(c: Connector): void {
    this.connectorService.stopConnector(c.name).subscribe({
      next: () => {
        this.snackBar.open(`Connector "${c.name}" stopped.`, 'OK', { duration: 2500 });
      },
      error: () => {
        this.snackBar.open(`Failed to stop connector "${c.name}".`, 'Close', { duration: 3000 });
      },
    });
  }

  restartConnector(c: Connector): void {
    this.connectorService.restartConnector(c.name, true).subscribe({
      next: () => {
        this.snackBar.open(`Connector "${c.name}" restarted.`, 'OK', { duration: 2500 });
      },
      error: () => {
        this.snackBar.open(`Failed to restart connector "${c.name}".`, 'Close', { duration: 3000 });
      },
    });
  }

  deleteConnector(c: Connector): void {
    if (confirm(`Are you sure you want to delete connector "${c.name}"?`)) {
      this.connectorService.deleteConnector(c.name).subscribe({
        next: () => {
          if (this.selectedConnector()?.name === c.name) {
            this.selectedConnector.set(null);
          }
          this.snackBar.open(`Connector "${c.name}" deleted.`, 'OK', { duration: 2500 });
        },
        error: () => {
          this.snackBar.open(`Failed to delete connector "${c.name}".`, 'Close', { duration: 3000 });
        },
      });
    }
  }

  selectConnector(c: Connector): void {
    if (this.selectedConnector()?.name === c.name) {
      this.selectedConnector.set(null);
    } else {
      this.selectedConnector.set(c);
    }
  }

  getStateClass(state: ConnectorState): string {
    switch (state) {
      case 'RUNNING':
        return 'state-running';
      case 'PAUSED':
        return 'state-paused';
      case 'STOPPED':
        return 'state-stopped';
      case 'FAILED':
        return 'state-failed';
      default:
        return 'state-unassigned';
    }
  }

  getPluginIcon(className: string): string {
    switch (className) {
      case 'S3ArchivalSinkConnector':
        return 'cloud_upload';
      case 'HttpWebhookSinkConnector':
        return 'webhook';
      case 'DatabaseCdcSourceConnector':
        return 'storage';
      case 'ElasticSearchSinkConnector':
        return 'search';
      default:
        return 'cable';
    }
  }

  getTypeBadgeClass(type: ConnectorType): string {
    return type === 'SOURCE' ? 'badge-source' : 'badge-sink';
  }

  formatNumber(num: number | undefined): string {
    if (num === undefined || num === null) return '0';
    return new Intl.NumberFormat().format(num);
  }

  formatBytes(bytes: number | undefined): string {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  configEntries(config: Record<string, string> | undefined): { key: string; value: string }[] {
    if (!config) return [];
    return Object.entries(config).map(([key, value]) => ({ key, value }));
  }
}
