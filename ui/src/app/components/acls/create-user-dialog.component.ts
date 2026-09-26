import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { Role } from '../../models/acl.model';
import { AclService } from '../../services/acl.service';

@Component({
  selector: 'app-create-user-dialog',
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
  ],
  template: `
    <div class="dialog-header">
      <div class="header-icon">
        <mat-icon>person_add</mat-icon>
      </div>
      <div class="header-titles">
        <h2 mat-dialog-title>Add Principal / Service Account</h2>
        <p class="subtitle">Register a human user or service account with a defined RBAC role</p>
      </div>
      <button mat-icon-button (click)="onCancel()" class="close-btn" aria-label="Close dialog">
        <mat-icon>close</mat-icon>
      </button>
    </div>

    <mat-dialog-content class="dialog-content">
      @if (errorMessage()) {
        <div class="error-banner">
          <mat-icon>error_outline</mat-icon>
          <span>{{ errorMessage() }}</span>
        </div>
      }

      <form [formGroup]="form" class="user-form">
        <mat-form-field appearance="outline" class="w-full" subscriptSizing="dynamic">
          <mat-label>Username / Account ID</mat-label>
          <input matInput formControlName="username" placeholder="e.g. analytics_worker, dev_john" />
          <mat-icon matPrefix class="field-icon">account_box</mat-icon>
          <mat-hint>Registered identity: <code>User:&lt;username&gt;</code></mat-hint>
          @if (form.get('username')?.hasError('required')) {
            <mat-error>Username is required</mat-error>
          }
        </mat-form-field>

        <mat-form-field appearance="outline" class="w-full" subscriptSizing="dynamic">
          <mat-label>Assigned RBAC Role</mat-label>
          <mat-select formControlName="role">
            @for (role of roles; track role) {
              <mat-option [value]="role">{{ role }}</mat-option>
            }
          </mat-select>
          <mat-icon matPrefix class="field-icon">badge</mat-icon>
          <mat-hint>Determines baseline permissions and cluster privileges</mat-hint>
        </mat-form-field>
      </form>
    </mat-dialog-content>

    <mat-dialog-actions class="dialog-actions">
      <button mat-button type="button" (click)="onCancel()" [disabled]="isSubmitting()">
        Cancel
      </button>
      <button
        mat-flat-button
        color="primary"
        type="button"
        (click)="onSubmit()"
        [disabled]="form.invalid || isSubmitting()"
        class="submit-btn"
      >
        @if (isSubmitting()) {
          <mat-icon class="spinning">sync</mat-icon>
        } @else {
          <mat-icon>how_to_reg</mat-icon>
        }
        <span>{{ isSubmitting() ? 'Registering...' : 'Add Principal' }}</span>
      </button>
    </mat-dialog-actions>
  `,
  styles: [`
    .dialog-header {
      display: flex;
      align-items: center;
      gap: 16px;
      padding: 20px 24px;
      border-bottom: 1px solid var(--app-border, rgba(0, 229, 255, 0.15));

      .header-icon {
        width: 44px;
        height: 44px;
        border-radius: 10px;
        background: rgba(0, 229, 255, 0.1);
        border: 1px solid var(--app-primary-border, rgba(0, 229, 255, 0.3));
        display: flex;
        align-items: center;
        justify-content: center;
        color: var(--app-primary, #00e5ff);

        mat-icon {
          font-size: 24px;
          width: 24px;
          height: 24px;
        }
      }

      .header-titles {
        flex: 1;

        h2 {
          margin: 0;
          font-size: 1.25rem;
          font-weight: 700;
          color: var(--app-text, #ffffff);
          letter-spacing: -0.01em;
        }

        .subtitle {
          margin: 4px 0 0 0;
          font-size: 0.85rem;
          color: var(--app-text-muted, #94a3b8);
        }
      }

      .close-btn {
        color: var(--app-text-muted, #94a3b8);
        &:hover { color: var(--app-text, #ffffff); }
      }
    }

    .dialog-content {
      padding: 24px;
      min-width: 440px;

      @media (max-width: 500px) {
        min-width: unset;
        width: 100%;
        padding: 16px;
      }
    }

    .error-banner {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 12px 16px;
      border-radius: 8px;
      background: rgba(239, 68, 68, 0.12);
      border: 1px solid rgba(239, 68, 68, 0.3);
      color: #ef4444;
      font-size: 0.875rem;
      margin-bottom: 20px;
    }

    .user-form {
      display: flex;
      flex-direction: column;
      gap: 24px;
    }

    .field-icon {
      margin-right: 8px;
      color: var(--app-text-muted, #94a3b8);
      font-size: 20px;
      width: 20px;
      height: 20px;
    }

    .dialog-actions {
      padding: 16px 24px;
      border-top: 1px solid var(--app-border, rgba(0, 229, 255, 0.15));
      display: flex;
      justify-content: flex-end;
      gap: 12px;

      .submit-btn {
        background-color: var(--app-primary, #00e5ff);
        color: #080b11;
        font-weight: 600;
        gap: 8px;
      }
    }

    .spinning {
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  `],
})
export class CreateUserDialogComponent {
  private readonly fb = inject(FormBuilder);
  private readonly aclService = inject(AclService);
  private readonly dialogRef = inject(MatDialogRef<CreateUserDialogComponent>);

  readonly isSubmitting = signal<boolean>(false);
  readonly errorMessage = signal<string | null>(null);

  readonly roles: Role[] = ['SUPER_ADMIN', 'OPERATOR', 'PRODUCER', 'CONSUMER', 'AUDITOR'];

  readonly form: FormGroup = this.fb.group({
    username: ['', [Validators.required, Validators.minLength(2)]],
    role: ['PRODUCER', [Validators.required]],
  });

  onSubmit(): void {
    if (this.form.invalid || this.isSubmitting()) {
      return;
    }

    this.isSubmitting.set(true);
    this.errorMessage.set(null);

    const val = this.form.value;
    this.aclService
      .createUser({
        username: val.username.trim(),
        role: val.role,
      })
      .subscribe({
        next: (created) => {
          this.isSubmitting.set(false);
          this.dialogRef.close(created);
        },
        error: (err) => {
          this.isSubmitting.set(false);
          this.errorMessage.set(err?.error?.message || 'Failed to add user');
        },
      });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
