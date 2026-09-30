import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatDividerModule } from '@angular/material/divider';
import { TurbineLogoComponent } from '../logo/turbine-logo.component';
import { AeroMQService } from '../../services/aeromq.service';

@Component({
  selector: 'app-about-dialog',
  standalone: true,
  imports: [
    CommonModule,
    MatDialogModule,
    MatButtonModule,
    MatIconModule,
    MatDividerModule,
    TurbineLogoComponent,
  ],
  template: `
    <div class="about-dialog-container p-6 max-w-lg w-full bg-app-card text-app rounded-xl overflow-hidden box-border">
      <!-- Header -->
      <div class="flex items-start justify-between gap-4 mb-4">
        <div class="flex items-center gap-3.5">
          <div class="logo-box p-2 rounded-xl bg-app-nested border border-app shadow-inner">
            <app-turbine-logo [size]="44" [glow]="true"></app-turbine-logo>
          </div>
          <div>
            <h2 class="text-xl font-bold tracking-tight m-0 text-app flex items-center gap-2">
              Aero<span class="text-cyan-400">Stream</span>
              <span class="text-[11px] font-mono font-medium px-2 py-0.5 rounded-full bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
                v0.1.0
              </span>
            </h2>
            <p class="text-xs text-app-muted m-0 mt-0.5">Distributed Event Streaming Platform</p>
          </div>
        </div>
        <button mat-icon-button (click)="close()" class="text-app-muted hover:text-app" aria-label="Close dialog">
          <mat-icon>close</mat-icon>
        </button>
      </div>

      <mat-divider class="my-4"></mat-divider>

      <!-- Description -->
      <p class="text-xs sm:text-sm text-app-muted leading-relaxed mb-4">
        AeroStream is a next-generation distributed streaming platform architected with a high-throughput dual-engine core.
        Drop-in compatible with Apache Kafka wire protocol clients while delivering lower latency, instant Raft consensus, and zero-dependency deployments.
      </p>

      <!-- Architecture Badges -->
      <div class="grid grid-cols-2 gap-3 mb-5">
        <div class="arch-card p-3 rounded-lg border border-app bg-app-nested">
          <div class="flex items-center gap-2 mb-1">
            <mat-icon class="text-cyan-400 !text-base !w-4 !h-4">memory</mat-icon>
            <span class="text-xs font-semibold text-app">Go Consensus</span>
          </div>
          <p class="text-[11px] text-app-muted m-0">Raft cluster metadata, controller coordination & management APIs</p>
        </div>

        <div class="arch-card p-3 rounded-lg border border-app bg-app-nested">
          <div class="flex items-center gap-2 mb-1">
            <mat-icon class="text-amber-400 !text-base !w-4 !h-4">speed</mat-icon>
            <span class="text-xs font-semibold text-app">Rust Log Engine</span>
          </div>
          <p class="text-[11px] text-app-muted m-0">Lockless I/O ring buffers, zero-copy segment storage & TCP socket server</p>
        </div>
      </div>

      <!-- Links & Repository Info -->
      <div class="repo-info-box p-3.5 rounded-lg border border-app bg-app-nested/60 mb-5 flex flex-col gap-2">
        <div class="flex items-center justify-between text-xs">
          <span class="text-app-muted flex items-center gap-1.5">
            <mat-icon class="!text-sm !w-3.5 !h-3.5 text-app-muted">code</mat-icon>
            Repository:
          </span>
          <a
            href="https://github.com/gradientgeeks/aerostream"
            target="_blank"
            rel="noopener noreferrer"
            class="text-cyan-400 hover:text-cyan-300 font-mono font-medium underline flex items-center gap-1"
          >
            gradientgeeks/aerostream
            <mat-icon class="!text-xs !w-3 !h-3">open_in_new</mat-icon>
          </a>
        </div>

        <div class="flex items-center justify-between text-xs">
          <span class="text-app-muted flex items-center gap-1.5">
            <mat-icon class="!text-sm !w-3.5 !h-3.5 text-app-muted">cable</mat-icon>
            Kafka Wire Port:
          </span>
          <span class="font-mono text-emerald-400 font-medium">9092 (TCP)</span>
        </div>

        <div class="flex items-center justify-between text-xs">
          <span class="text-app-muted flex items-center gap-1.5">
            <mat-icon class="!text-sm !w-3.5 !h-3.5 text-app-muted">http</mat-icon>
            Controller API:
          </span>
          <span class="font-mono text-app font-medium">{{ service.apiBaseUrl() }}</span>
        </div>
      </div>

      <!-- Actions -->
      <div class="flex items-center justify-between gap-3 pt-2">
        <a
          mat-stroked-button
          href="https://github.com/gradientgeeks/aerostream"
          target="_blank"
          rel="noopener noreferrer"
          class="github-btn text-xs flex items-center gap-1.5"
        >
          <mat-icon class="!text-sm !w-4 !h-4">star</mat-icon>
          <span>Star on GitHub</span>
        </a>

        <button mat-flat-button color="primary" (click)="close()">
          Close
        </button>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
    }
  `]
})
export class AboutDialogComponent {
  private dialogRef = inject(MatDialogRef<AboutDialogComponent>);
  protected readonly service = inject(AeroMQService);

  close(): void {
    this.dialogRef.close();
  }
}
