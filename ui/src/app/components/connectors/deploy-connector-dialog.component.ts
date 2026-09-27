import { Component, Inject, OnInit, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { ConnectorPlugin } from '../../models/connector.model';
import { ConnectorService } from '../../services/connector.service';

export interface DeployConnectorDialogData {
  presetClass?: string;
  presetTopic?: string;
}

interface ConfigEntry {
  key: string;
  value: string;
}

@Component({
  selector: 'app-deploy-connector-dialog',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatDialogModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatTooltipModule,
    MatDividerModule,
    MatSnackBarModule,
  ],
  templateUrl: './deploy-connector-dialog.component.html',
  styleUrl: './deploy-connector-dialog.component.scss',
})
export class DeployConnectorDialogComponent implements OnInit {
  private readonly fb = inject(FormBuilder);
  readonly connectorService = inject(ConnectorService);
  private readonly dialogRef = inject(MatDialogRef<DeployConnectorDialogComponent>);
  private readonly snackBar = inject(MatSnackBar);

  readonly plugins = this.connectorService.plugins;
  readonly isSubmitting = signal(false);
  readonly configEntries = signal<ConfigEntry[]>([]);

  form!: FormGroup;

  constructor(@Inject(MAT_DIALOG_DATA) public data?: DeployConnectorDialogData) {}

  ngOnInit(): void {
    if (this.plugins().length === 0) {
      this.connectorService.loadPlugins().subscribe((loadedPlugins) => {
        if (loadedPlugins.length > 0 && !this.form?.get('class')?.value) {
          const selected = this.data?.presetClass || loadedPlugins[0].class;
          this.form?.patchValue({ class: selected });
          this.loadTemplateForClass(selected);
        }
      });
    }

    const defaultPlugin = this.plugins().find((p) => p.class === this.data?.presetClass) || this.plugins()[0];
    const initialClass = defaultPlugin?.class || this.data?.presetClass || 'HttpWebhookSinkConnector';

    this.form = this.fb.group({
      name: [
        '',
        [
          Validators.required,
          Validators.minLength(3),
          Validators.pattern(/^[a-zA-Z0-9._-]+$/),
        ],
      ],
      class: [initialClass, [Validators.required]],
      topic: [this.data?.presetTopic || '', [Validators.required, Validators.minLength(1)]],
      tasksCount: [1, [Validators.required, Validators.min(1), Validators.max(32)]],
    });

    this.loadTemplateForClass(initialClass);

    this.form.get('class')?.valueChanges.subscribe((selectedClass: string) => {
      this.loadTemplateForClass(selectedClass);
    });
  }

  loadTemplateForClass(className: string): void {
    const template = this.connectorService.getConfigTemplate(className);
    const entries: ConfigEntry[] = Object.entries(template).map(([key, value]) => ({
      key,
      value,
    }));
    this.configEntries.set(entries);
  }

  addConfigEntry(): void {
    this.configEntries.update((entries) => [...entries, { key: '', value: '' }]);
  }

  removeConfigEntry(index: number): void {
    this.configEntries.update((entries) => entries.filter((_, i) => i !== index));
  }

  updateEntryKey(index: number, newKey: string): void {
    this.configEntries.update((entries) =>
      entries.map((entry, i) => (i === index ? { ...entry, key: newKey } : entry))
    );
  }

  updateEntryValue(index: number, newValue: string): void {
    this.configEntries.update((entries) =>
      entries.map((entry, i) => (i === index ? { ...entry, value: newValue } : entry))
    );
  }

  selectedPlugin(): ConnectorPlugin | undefined {
    const cls = this.form?.get('class')?.value;
    return this.plugins().find((p) => p.class === cls);
  }

  onSubmit(): void {
    if (this.form.invalid || this.isSubmitting()) {
      return;
    }

    this.isSubmitting.set(true);
    const formVal = this.form.value;

    const configMap: Record<string, string> = {};
    for (const entry of this.configEntries()) {
      const k = entry.key.trim();
      if (k) {
        configMap[k] = entry.value.trim();
      }
    }

    const plugin = this.selectedPlugin();

    this.connectorService
      .createConnector({
        name: formVal.name.trim(),
        class: formVal.class,
        type: plugin?.type,
        topic: formVal.topic.trim(),
        tasks_count: formVal.tasksCount,
        config: configMap,
      })
      .subscribe({
        next: (created) => {
          this.isSubmitting.set(false);
          this.dialogRef.close({ success: true, connector: created });
        },
        error: (err) => {
          this.isSubmitting.set(false);
          const errMsg = err?.error?.error || err?.message || 'Failed to deploy connector';
          this.snackBar.open(`Error deploying connector: ${errMsg}`, 'Dismiss', {
            duration: 5000,
          });
          console.error('Failed to create connector:', err);
        },
      });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
