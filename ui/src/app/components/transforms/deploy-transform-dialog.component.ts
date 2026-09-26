import { Component, OnInit, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { TransformService } from '../../services/transform.service';
import { TransformType } from '../../models/transform.model';

@Component({
  selector: 'app-deploy-transform-dialog',
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
    MatChipsModule,
    MatTooltipModule,
    MatProgressSpinnerModule,
  ],
  templateUrl: './deploy-transform-dialog.component.html',
  styleUrl: './deploy-transform-dialog.component.scss',
})
export class DeployTransformDialogComponent implements OnInit {
  private fb = inject(FormBuilder);
  private transformService = inject(TransformService);
  private dialogRef = inject(MatDialogRef<DeployTransformDialogComponent>);

  readonly transformTypes: TransformType[] = ['MASK_PII', 'FILTER', 'WASM', 'JSON_MAP'];

  isSubmitting = signal(false);
  errorMessage = signal<string | null>(null);

  form!: FormGroup;

  ngOnInit(): void {
    const defaultTemplate = this.transformService.getTransformTemplate('MASK_PII');

    this.form = this.fb.group({
      name: [
        '',
        [
          Validators.required,
          Validators.minLength(2),
          Validators.maxLength(100),
          Validators.pattern(/^[a-zA-Z0-9._-]+$/),
        ],
      ],
      source_topic: ['', [Validators.required, Validators.maxLength(100)]],
      target_topic: ['', [Validators.required, Validators.maxLength(100)]],
      type: ['MASK_PII', [Validators.required]],
      config_key1: ['fields_to_mask'],
      config_val1: [defaultTemplate.config['fields_to_mask'] || 'credit_card,cvv,password,email'],
      code: [defaultTemplate.code, [Validators.required]],
    });

    this.form.get('type')?.valueChanges.subscribe((type: TransformType) => {
      this.onTypeChanged(type);
    });
  }

  onTypeChanged(type: TransformType): void {
    const template = this.transformService.getTransformTemplate(type);

    if (type === 'MASK_PII') {
      this.form.patchValue({
        config_key1: 'fields_to_mask',
        config_val1: template.config['fields_to_mask'],
        code: template.code,
      });
    } else if (type === 'FILTER') {
      this.form.patchValue({
        config_key1: 'filter_expression',
        config_val1: template.config['filter_expression'],
        code: template.code,
      });
    } else if (type === 'JSON_MAP') {
      this.form.patchValue({
        config_key1: 'add_fields',
        config_val1: template.config['add_fields'],
        code: template.code,
      });
    } else if (type === 'WASM') {
      this.form.patchValue({
        config_key1: 'runtime',
        config_val1: template.config['runtime'],
        code: template.code,
      });
    }
  }

  onSubmit(): void {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    this.isSubmitting.set(true);
    this.errorMessage.set(null);

    const val = this.form.value;
    const config: Record<string, string> = {};
    if (val.config_key1 && val.config_val1) {
      config[val.config_key1.trim()] = val.config_val1.trim();
    }

    this.transformService
      .registerTransform({
        name: val.name.trim(),
        source_topic: val.source_topic.trim(),
        target_topic: val.target_topic.trim(),
        type: val.type,
        config,
        code: val.code.trim(),
      })
      .subscribe({
        next: (created) => {
          this.isSubmitting.set(false);
          this.dialogRef.close(created);
        },
        error: (err) => {
          this.isSubmitting.set(false);
          const msg = err.error?.message || err.error || err.message || 'Failed to deploy transform';
          this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
        },
      });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
