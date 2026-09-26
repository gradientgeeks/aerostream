import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';

import {
  StreamJob,
  StreamJobConfig,
  StreamsService,
  WindowResult,
  WindowType,
} from '../../services/streams.service';

@Component({
  selector: 'app-streams',
  standalone: true,
  imports: [CommonModule, FormsModule, MatIconModule, MatSnackBarModule],
  templateUrl: './streams.component.html',
  styleUrl: './streams.component.scss',
})
export class StreamsComponent implements OnInit, OnDestroy {
  readonly svc = inject(StreamsService);
  private snack = inject(MatSnackBar);

  selected = signal<StreamJob | null>(null);
  results = signal<string[]>([]);
  keys = signal<string[]>([]);
  queryKey = '';
  windows = signal<WindowResult[]>([]);
  tableRow = signal<string | null>(null);
  ingestKey = '';
  ingestValue = '{"user":"ann","amt":10}';
  showCreate = false;
  private timer: ReturnType<typeof setInterval> | null = null;

  form: {
    name: string;
    type: 'AGGREGATE' | 'JOIN';
    source_topic: string;
    target_topic: string;
    table_topic: string;
    key_field: string;
    table_key_field: string;
    value_field: string;
    timestamp_field: string;
    agg: string;
    window_type: WindowType;
    size_s: number;
    advance_s: number;
    gap_s: number;
    grace_s: number;
    join_type: string;
  } = this.emptyForm();

  readonly windowTypes: WindowType[] = ['tumbling', 'hopping', 'sliding', 'session'];
  readonly aggs = ['count', 'sum', 'avg', 'min', 'max'];

  private emptyForm() {
    return {
      name: '',
      type: 'AGGREGATE' as 'AGGREGATE' | 'JOIN',
      source_topic: '',
      target_topic: '',
      table_topic: '',
      key_field: '',
      table_key_field: '',
      value_field: '',
      timestamp_field: '',
      agg: 'count',
      window_type: 'tumbling' as WindowType,
      size_s: 60,
      advance_s: 30,
      gap_s: 30,
      grace_s: 0,
      join_type: 'inner',
    };
  }

  ngOnInit(): void {
    this.svc.load();
    this.timer = setInterval(() => {
      this.svc.load();
      this.refreshSelected();
    }, 3000);
  }

  ngOnDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  windowLabel(j: StreamJob): string {
    if (j.type === 'JOIN') return `${j.join_type} join with ${j.table_topic}`;
    const w = j.window;
    if (!w) return '-';
    if (w.type === 'session') return `session gap ${(w.gap_ms ?? 0) / 1000}s`;
    if (w.type === 'hopping') return `hopping ${(w.size_ms ?? 0) / 1000}s / ${(w.advance_ms ?? 0) / 1000}s`;
    return `${w.type} ${(w.size_ms ?? 0) / 1000}s`;
  }

  select(job: StreamJob): void {
    this.selected.set(job);
    this.windows.set([]);
    this.tableRow.set(null);
    this.queryKey = '';
    this.refreshSelected();
  }

  private refreshSelected(): void {
    const s = this.selected();
    if (!s) return;
    this.svc.results(s.name, 30).subscribe({
      next: (r) => this.results.set((r ?? []).map((x) => JSON.stringify(x)).reverse()),
      error: () => this.selected.set(null),
    });
    this.svc.keys(s.name).subscribe({ next: (k) => this.keys.set(k.keys ?? []) });
  }

  runQuery(key = this.queryKey): void {
    const s = this.selected();
    if (!s || !key) return;
    this.queryKey = key;
    this.svc.queryKey(s.name, key).subscribe({
      next: (r) => {
        this.windows.set(r.windows ?? []);
        this.tableRow.set(r.table !== undefined ? JSON.stringify(r.table, null, 2) : null);
      },
      error: () => {
        this.windows.set([]);
        this.tableRow.set(null);
        this.snack.open(`No state for key "${key}"`, 'Dismiss', { duration: 3000 });
      },
    });
  }

  ingest(): void {
    const s = this.selected();
    if (!s) return;
    let value: unknown;
    try {
      value = JSON.parse(this.ingestValue);
    } catch {
      this.snack.open('Event value must be valid JSON', 'Dismiss', { duration: 3000 });
      return;
    }
    this.svc.ingest(s.name, value, undefined, this.ingestKey || undefined).subscribe({
      next: () => this.refreshSelected(),
      error: (e) => this.snack.open(e?.error ?? 'Ingest failed', 'Dismiss', { duration: 4000 }),
    });
  }

  toggle(job: StreamJob): void {
    this.svc.setPaused(job.name, job.status === 'RUNNING').subscribe();
  }

  remove(job: StreamJob): void {
    this.svc.remove(job.name).subscribe(() => {
      if (this.selected()?.name === job.name) this.selected.set(null);
    });
  }

  create(): void {
    const f = this.form;
    const cfg: StreamJobConfig = {
      name: f.name.trim(),
      type: f.type,
      source_topic: f.source_topic.trim(),
      target_topic: f.target_topic.trim() || undefined,
      key_field: f.key_field.trim() || undefined,
      timestamp_field: f.timestamp_field.trim() || undefined,
    };
    if (f.type === 'AGGREGATE') {
      cfg.agg = f.agg;
      cfg.value_field = f.value_field.trim() || undefined;
      cfg.grace_ms = f.grace_s * 1000;
      cfg.window = {
        type: f.window_type,
        size_ms: f.size_s * 1000,
        advance_ms: f.window_type === 'hopping' ? f.advance_s * 1000 : undefined,
        gap_ms: f.window_type === 'session' ? f.gap_s * 1000 : undefined,
      };
      if (f.window_type === 'session') delete cfg.window.size_ms;
    } else {
      cfg.table_topic = f.table_topic.trim();
      cfg.table_key_field = f.table_key_field.trim() || undefined;
      cfg.join_type = f.join_type;
    }
    this.svc.create(cfg).subscribe({
      next: () => {
        this.showCreate = false;
        this.form = this.emptyForm();
        this.snack.open(`Stream job ${cfg.name} started`, 'OK', { duration: 3000 });
      },
      error: (e) => this.snack.open(typeof e?.error === 'string' ? e.error : 'Failed to create job', 'Dismiss', { duration: 5000 }),
    });
  }
}
