import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { Operation, Permission, ResourceType } from '../../models/acl.model';
import { AclService } from '../../services/acl.service';

@Component({
  selector: 'app-create-acl-dialog',
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
  templateUrl: './create-acl-dialog.component.html',
  styleUrl: './create-acl-dialog.component.scss',
})
export class CreateAclDialogComponent {
  private readonly fb = inject(FormBuilder);
  protected readonly aclService = inject(AclService);
  private readonly dialogRef = inject(MatDialogRef<CreateAclDialogComponent>);

  readonly isSubmitting = signal<boolean>(false);
  readonly errorMessage = signal<string | null>(null);

  readonly resourceTypes: ResourceType[] = ['TOPIC', 'GROUP', 'CLUSTER'];
  readonly operations: Operation[] = ['READ', 'WRITE', 'DESCRIBE', 'ALTER', 'ALL'];
  readonly permissions: Permission[] = ['ALLOW', 'DENY'];

  readonly form: FormGroup = this.fb.group({
    principal: ['User:', [Validators.required, Validators.minLength(2)]],
    resource_type: ['TOPIC', [Validators.required]],
    resource_name: ['', [Validators.required, Validators.minLength(1)]],
    operation: ['WRITE', [Validators.required]],
    permission: ['ALLOW', [Validators.required]],
  });

  setPrincipal(val: string): void {
    this.form.patchValue({ principal: val });
  }

  setPattern(val: string): void {
    this.form.patchValue({ resource_name: val });
  }

  onSubmit(): void {
    if (this.form.invalid || this.isSubmitting()) {
      return;
    }

    this.isSubmitting.set(true);
    this.errorMessage.set(null);

    const val = this.form.value;
    let principal = val.principal.trim();
    if (!principal.startsWith('User:') && principal !== '*') {
      principal = `User:${principal}`;
    }

    this.aclService
      .createRule({
        principal,
        resource_type: val.resource_type,
        resource_name: val.resource_name.trim(),
        operation: val.operation,
        permission: val.permission,
      })
      .subscribe({
        next: (created) => {
          this.isSubmitting.set(false);
          this.dialogRef.close(created);
        },
        error: (err) => {
          this.isSubmitting.set(false);
          this.errorMessage.set(err?.error?.message || 'Failed to create ACL rule');
        },
      });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
