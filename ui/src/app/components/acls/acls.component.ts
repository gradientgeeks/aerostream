import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
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
import { MatTabsModule } from '@angular/material/tabs';
import { MatDividerModule } from '@angular/material/divider';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

import { AclRule, Operation, Permission, ResourceType, Role, TestAclResponse, User } from '../../models/acl.model';
import { AclService } from '../../services/acl.service';
import { CreateAclDialogComponent } from './create-acl-dialog.component';
import { CreateUserDialogComponent } from './create-user-dialog.component';

interface TestScenario {
  label: string;
  principal: string;
  resource_type: ResourceType;
  resource_name: string;
  operation: Operation;
  expected: 'ALLOW' | 'DENY';
}

@Component({
  selector: 'app-acls',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
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
    MatTabsModule,
    MatDividerModule,
    MatProgressSpinnerModule,
  ],
  templateUrl: './acls.component.html',
  styleUrl: './acls.component.scss',
})
export class AclsComponent implements OnInit {
  protected readonly aclService = inject(AclService);
  private readonly dialog = inject(MatDialog);
  private readonly snackBar = inject(MatSnackBar);
  private readonly fb = inject(FormBuilder);

  // Filters for ACL rules
  readonly ruleSearch = signal<string>('');
  readonly selectedTypeFilter = signal<string>('ALL');
  readonly selectedPermFilter = signal<string>('ALL');

  // Filters for Users
  readonly userSearch = signal<string>('');
  readonly selectedRoleFilter = signal<string>('ALL');

  // Live Policy Evaluator state
  readonly isEvaluating = signal<boolean>(false);
  readonly evaluationResult = signal<TestAclResponse | null>(null);
  readonly lastEvaluatedAt = signal<string | null>(null);

  readonly evaluatorForm: FormGroup = this.fb.group({
    principal: ['User:admin', [Validators.required]],
    resource_type: ['CLUSTER', [Validators.required]],
    resource_name: ['*', [Validators.required]],
    operation: ['ALL', [Validators.required]],
  });

  readonly testScenarios: TestScenario[] = [
    {
      label: 'Admin -> Alter Cluster (SUPER_ADMIN Global)',
      principal: 'User:admin',
      resource_type: 'CLUSTER',
      resource_name: '*',
      operation: 'ALL',
      expected: 'ALLOW',
    },
    {
      label: 'Admin -> Write Topic (SUPER_ADMIN Global)',
      principal: 'User:admin',
      resource_type: 'TOPIC',
      resource_name: 'orders-2026',
      operation: 'WRITE',
      expected: 'ALLOW',
    },
    {
      label: 'Guest / Anonymous -> Write Topic (Zero-Trust Deny)',
      principal: 'User:guest',
      resource_type: 'TOPIC',
      resource_name: 'orders-2026',
      operation: 'WRITE',
      expected: 'DENY',
    },
    {
      label: 'Unauthorized Client -> Alter Cluster (Zero-Trust Deny)',
      principal: 'User:unauthorized_client',
      resource_type: 'CLUSTER',
      resource_name: '*',
      operation: 'ALTER',
      expected: 'DENY',
    },
  ];

  // Filtered Rules
  readonly filteredRules = computed(() => {
    const list = this.aclService.rules();
    const query = this.ruleSearch().trim().toLowerCase();
    const typeFilter = this.selectedTypeFilter();
    const permFilter = this.selectedPermFilter();

    return list.filter((r) => {
      const matchesQuery =
        !query ||
        r.principal.toLowerCase().includes(query) ||
        r.resource_name.toLowerCase().includes(query) ||
        r.operation.toLowerCase().includes(query);

      const matchesType = typeFilter === 'ALL' || r.resource_type === typeFilter;
      const matchesPerm = permFilter === 'ALL' || r.permission === permFilter;

      return matchesQuery && matchesType && matchesPerm;
    });
  });

  // Filtered Users
  readonly filteredUsers = computed(() => {
    const list = this.aclService.users();
    const query = this.userSearch().trim().toLowerCase();
    const roleFilter = this.selectedRoleFilter();

    return list.filter((u) => {
      const matchesQuery = !query || u.username.toLowerCase().includes(query);
      const matchesRole = roleFilter === 'ALL' || u.role === roleFilter;
      return matchesQuery && matchesRole;
    });
  });

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.aclService.refreshAll();
  }

  refreshRules(): void {
    this.aclService.loadRules();
  }

  refreshUsers(): void {
    this.aclService.loadUsers();
  }

  openCreateAclDialog(): void {
    const dialogRef = this.dialog.open(CreateAclDialogComponent, {
      width: '560px',
      panelClass: 'custom-dialog-panel',
    });

    dialogRef.afterClosed().subscribe((result) => {
      if (result) {
        this.snackBar.open(`ACL rule '${result.id}' created successfully`, 'Close', {
          duration: 3500,
          horizontalPosition: 'end',
          verticalPosition: 'bottom',
        });
      }
    });
  }

  openCreateUserDialog(): void {
    const dialogRef = this.dialog.open(CreateUserDialogComponent, {
      width: '480px',
      panelClass: 'custom-dialog-panel',
    });

    dialogRef.afterClosed().subscribe((result) => {
      if (result) {
        this.snackBar.open(`Principal 'User:${result.username}' registered successfully`, 'Close', {
          duration: 3500,
          horizontalPosition: 'end',
          verticalPosition: 'bottom',
        });
      }
    });
  }

  deleteRule(rule: AclRule): void {
    if (confirm(`Are you sure you want to delete ACL rule '${rule.id}' for '${rule.principal}'?`)) {
      this.aclService.deleteRule(rule.id).subscribe({
        next: () => {
          this.snackBar.open(`ACL rule '${rule.id}' removed`, 'Close', {
            duration: 3000,
            horizontalPosition: 'end',
            verticalPosition: 'bottom',
          });
        },
        error: (err) => {
          const msg = err?.error?.message || 'Failed to delete ACL rule';
          this.snackBar.open(msg, 'Close', { duration: 3000 });
        },
      });
    }
  }

  evaluatePolicy(): void {
    if (this.evaluatorForm.invalid || this.isEvaluating()) {
      return;
    }

    this.isEvaluating.set(true);
    const val = this.evaluatorForm.value;

    let principal = val.principal.trim();
    if (!principal.startsWith('User:') && principal !== '*') {
      principal = `User:${principal}`;
    }

    this.aclService
      .testAcl({
        principal,
        resource_type: val.resource_type,
        resource_name: val.resource_name.trim(),
        operation: val.operation,
      })
      .subscribe({
        next: (res) => {
          this.evaluationResult.set(res);
          this.lastEvaluatedAt.set(new Date().toLocaleTimeString());
          this.isEvaluating.set(false);
        },
        error: (err) => {
          this.isEvaluating.set(false);
          const msg = err?.error?.message || 'Failed to evaluate policy on controller';
          this.snackBar.open(msg, 'Close', { duration: 3000 });
        },
      });
  }

  applyScenario(scenario: TestScenario): void {
    this.evaluatorForm.patchValue({
      principal: scenario.principal,
      resource_type: scenario.resource_type,
      resource_name: scenario.resource_name,
      operation: scenario.operation,
    });
    this.evaluatePolicy();
  }

  getRoleBadgeClass(role: Role): string {
    switch (role) {
      case 'SUPER_ADMIN':
        return 'role-super-admin';
      case 'OPERATOR':
        return 'role-operator';
      case 'PRODUCER':
        return 'role-producer';
      case 'CONSUMER':
        return 'role-consumer';
      case 'AUDITOR':
        return 'role-auditor';
      default:
        return '';
    }
  }

  getResourceBadgeClass(type: ResourceType): string {
    switch (type) {
      case 'TOPIC':
        return 'resource-topic';
      case 'GROUP':
        return 'resource-group';
      case 'CLUSTER':
        return 'resource-cluster';
      default:
        return '';
    }
  }
}
