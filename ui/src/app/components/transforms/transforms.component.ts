import { Component, OnInit, inject, signal, computed, effect } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatBadgeModule } from '@angular/material/badge';

import { TransformService } from '../../services/transform.service';
import {
  StreamTransform,
  TransformType,
  TestTransformResponse,
} from '../../models/transform.model';
import { DeployTransformDialogComponent } from './deploy-transform-dialog.component';

export interface SamplePreset {
  name: string;
  suggestedType?: TransformType;
  payload: string;
}

@Component({
  selector: 'app-transforms',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatDialogModule,
    MatSnackBarModule,
    MatTooltipModule,
    MatDividerModule,
    MatProgressSpinnerModule,
    MatBadgeModule,
  ],
  templateUrl: './transforms.component.html',
  styleUrl: './transforms.component.scss',
})
export class TransformsComponent implements OnInit {
  protected readonly transformService = inject(TransformService);
  private readonly dialog = inject(MatDialog);
  private readonly snackBar = inject(MatSnackBar);

  readonly searchQuery = signal<string>('');
  readonly typeFilter = signal<string>('ALL');

  // Playground state
  readonly selectedTestTransform = signal<string>('');
  readonly selectedTemplateName = signal<string>('');

  readonly presets: SamplePreset[] = [
    {
      name: 'Order with Sensitive PII (Credit Card & Passwords)',
      suggestedType: 'MASK_PII',
      payload: JSON.stringify(
        {
          order_id: 'ORD-98421',
          customer: {
            name: 'Sarah Connor',
            email: 'sconnor@cyberdyne.org',
            credit_card: '4532-8819-2041-9923',
            cvv: '812',
            password: 'hk-terminator-pass!',
          },
          amount: 289.5,
          currency: 'USD',
          status: 'PENDING',
        },
        null,
        2
      ),
    },
    {
      name: 'Critical Telemetry Event (Alert Rule)',
      suggestedType: 'FILTER',
      payload: JSON.stringify(
        {
          device_id: 'turbine-blade-09',
          level: 'CRITICAL',
          temperature_c: 124.8,
          rpm: 3820,
          alert_msg: 'Bearing friction threshold exceeded',
        },
        null,
        2
      ),
    },
    {
      name: 'Normal Info Telemetry Event (Filter Drop)',
      suggestedType: 'FILTER',
      payload: JSON.stringify(
        {
          device_id: 'turbine-blade-09',
          level: 'INFO',
          temperature_c: 68.2,
          rpm: 2100,
          status: 'nominal',
        },
        null,
        2
      ),
    },
    {
      name: 'Clickstream Web Payload (WASM / JSON Map)',
      suggestedType: 'WASM',
      payload: JSON.stringify(
        {
          session_id: 'sess-819a-ff2',
          action: 'checkout_click',
          user_agent: 'Mozilla/5.0 Chrome/120',
          ip: '198.51.100.42',
        },
        null,
        2
      ),
    },
    {
      name: 'User Identity & Compliance Record',
      suggestedType: 'MASK_PII',
      payload: JSON.stringify(
        {
          user_id: 'usr-10492',
          email: 'alex.mercer@gentek.com',
          ssn: '984-12-8841',
          role: 'researcher',
          country: 'US',
        },
        null,
        2
      ),
    },
  ];

  readonly testInputJson = signal<string>(this.presets[0].payload);
  readonly isTesting = signal<boolean>(false);
  readonly testResult = signal<TestTransformResponse | null>(null);
  readonly testError = signal<string | null>(null);
  readonly roundTripLatencyMs = signal<number | null>(null);

  readonly filteredTransforms = computed(() => {
    const list = this.transformService.transforms();
    const query = this.searchQuery().trim().toLowerCase();
    const type = this.typeFilter();

    return list.filter((t) => {
      const matchesQuery =
        !query ||
        t.name.toLowerCase().includes(query) ||
        t.source_topic.toLowerCase().includes(query) ||
        t.target_topic.toLowerCase().includes(query) ||
        t.type.toLowerCase().includes(query);

      const matchesType = type === 'ALL' || t.type === type;
      return matchesQuery && matchesType;
    });
  });

  constructor() {
    // Keep selectedTestTransform aligned with available transforms
    effect(() => {
      const list = this.transformService.transforms();
      const current = this.selectedTestTransform();
      if (list.length > 0) {
        if (!current || !list.some((t) => t.name === current)) {
          this.selectedTestTransform.set(list[0].name);
        }
      } else {
        this.selectedTestTransform.set('');
      }
    });
  }

  ngOnInit(): void {
    this.transformService.loadTransforms();
  }

  openDeployDialog(): void {
    const ref = this.dialog.open(DeployTransformDialogComponent, {
      width: '680px',
      panelClass: 'custom-dialog-panel',
    });

    ref.afterClosed().subscribe((result: StreamTransform | undefined) => {
      if (result) {
        this.snackBar.open(`Transform "${result.name}" deployed successfully!`, 'Close', {
          duration: 3500,
          panelClass: 'snackbar-success',
        });
        this.selectedTestTransform.set(result.name);
      }
    });
  }

  togglePauseResume(t: StreamTransform): void {
    if (t.status === 'RUNNING') {
      this.transformService.pauseTransform(t.name).subscribe({
        next: () => {
          this.snackBar.open(`Transform "${t.name}" paused`, 'Close', { duration: 2500 });
        },
        error: (err) => {
          this.snackBar.open(`Failed to pause: ${err.message}`, 'Close', { duration: 3000 });
        },
      });
    } else {
      this.transformService.resumeTransform(t.name).subscribe({
        next: () => {
          this.snackBar.open(`Transform "${t.name}" resumed`, 'Close', { duration: 2500 });
        },
        error: (err) => {
          this.snackBar.open(`Failed to resume: ${err.message}`, 'Close', { duration: 3000 });
        },
      });
    }
  }

  deleteTransform(t: StreamTransform): void {
    if (confirm(`Are you sure you want to delete stream transform "${t.name}"?`)) {
      this.transformService.deleteTransform(t.name).subscribe({
        next: () => {
          this.snackBar.open(`Transform "${t.name}" removed`, 'Close', { duration: 2500 });
        },
        error: (err) => {
          this.snackBar.open(`Failed to delete: ${err.message}`, 'Close', { duration: 3000 });
        },
      });
    }
  }

  selectForTest(t: StreamTransform): void {
    this.selectedTestTransform.set(t.name);
    // Find matching preset by type or keep current
    const preset = this.presets.find((p) => p.suggestedType === t.type);
    if (preset) {
      this.testInputJson.set(preset.payload);
      this.selectedTemplateName.set(preset.name);
    }
    // Scroll to playground
    const element = document.getElementById('transform-playground');
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' });
    }
  }

  applyTemplateByName(name: string): void {
    const preset = this.presets.find((p) => p.name === name);
    if (preset) {
      this.selectedTemplateName.set(preset.name);
      this.testInputJson.set(preset.payload);

      // If user hasn't selected a transform yet, match first transform of same type
      const current = this.selectedTestTransform();
      const list = this.transformService.transforms();
      if ((!current || !list.some((t) => t.name === current)) && preset.suggestedType) {
        const matchingTransform = list.find((t) => t.type === preset.suggestedType);
        if (matchingTransform) {
          this.selectedTestTransform.set(matchingTransform.name);
        }
      }
    }
  }

  runTest(): void {
    const transformName = this.selectedTestTransform();
    if (!transformName) {
      this.testError.set('No stream transform selected. Please deploy or select a transform.');
      return;
    }

    const rawInput = this.testInputJson().trim();
    if (!rawInput) {
      this.testError.set('Payload cannot be empty.');
      return;
    }

    let parsedPayload: any;
    try {
      parsedPayload = JSON.parse(rawInput);
    } catch {
      parsedPayload = rawInput;
    }

    this.isTesting.set(true);
    this.testError.set(null);
    this.testResult.set(null);
    this.roundTripLatencyMs.set(null);

    const startTime = performance.now();

    this.transformService
      .testTransform({
        transform_name: transformName,
        payload: parsedPayload,
      })
      .subscribe({
        next: (res) => {
          const roundTripMs = performance.now() - startTime;
          this.isTesting.set(false);
          this.testResult.set(res);
          this.roundTripLatencyMs.set(roundTripMs);

          // Record real execution latency into running average
          const latency = (res.latency_ms && res.latency_ms > 0) ? res.latency_ms : roundTripMs;
          this.transformService.recordExecutionLatency(latency);
        },
        error: (err) => {
          const roundTripMs = performance.now() - startTime;
          this.isTesting.set(false);
          this.roundTripLatencyMs.set(roundTripMs);
          const msg = err.error?.message || err.error || err.message || 'Execution error';
          this.testError.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
        },
      });
  }

  formatBackendLatency(latencyMs: number): string {
    if (latencyMs === undefined || latencyMs === null) {
      return '0 ms';
    }
    if (latencyMs < 1) {
      const us = latencyMs * 1000;
      return `${us.toFixed(us < 10 ? 1 : 0)} µs (${latencyMs.toFixed(3)} ms)`;
    }
    return `${latencyMs.toFixed(2)} ms`;
  }

  formatJsonInput(): void {
    try {
      const obj = JSON.parse(this.testInputJson());
      this.testInputJson.set(JSON.stringify(obj, null, 2));
    } catch (e) {
      this.snackBar.open('Invalid JSON syntax', 'Close', { duration: 2000 });
    }
  }
}
