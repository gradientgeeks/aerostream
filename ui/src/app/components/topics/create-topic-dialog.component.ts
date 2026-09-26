import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSliderModule } from '@angular/material/slider';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-create-topic-dialog',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatDialogModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatSliderModule,
    MatButtonModule,
    MatIconModule,
    MatProgressSpinnerModule
  ],
  templateUrl: './create-topic-dialog.component.html',
  styleUrl: './create-topic-dialog.component.scss'
})
export class CreateTopicDialogComponent {
  private fb = inject(FormBuilder);
  private apiService = inject(ApiService);
  private dialogRef = inject(MatDialogRef<CreateTopicDialogComponent>);

  isSubmitting = signal(false);
  errorMessage = signal<string | null>(null);

  topicForm: FormGroup = this.fb.group({
    name: [
      '',
      [
        Validators.required,
        Validators.minLength(1),
        Validators.maxLength(255),
        Validators.pattern(/^[a-zA-Z0-9._-]+$/)
      ]
    ],
    partitions: [3, [Validators.required, Validators.min(1), Validators.max(100)]],
    replication_factor: [2, [Validators.required, Validators.min(1), Validators.max(10)]],
    cleanup_policy: ['delete', [Validators.required]]
  });

  onSubmit(): void {
    if (this.topicForm.invalid || this.isSubmitting()) {
      return;
    }

    this.isSubmitting.set(true);
    this.errorMessage.set(null);

    const val = this.topicForm.value;
    this.apiService.createTopic({
      name: val.name.trim(),
      partitions: Number(val.partitions),
      replication_factor: Number(val.replication_factor),
      cleanup_policy: val.cleanup_policy
    }).subscribe({
      next: (res) => {
        this.isSubmitting.set(false);
        this.dialogRef.close({ success: true, name: val.name.trim(), message: res.message });
      },
      error: (err) => {
        this.isSubmitting.set(false);
        const msg = err.error?.message || err.error || err.message || 'Failed to create topic';
        this.errorMessage.set(typeof msg === 'string' ? msg : JSON.stringify(msg));
      }
    });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
