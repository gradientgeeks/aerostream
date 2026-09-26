import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, map, of, tap } from 'rxjs';
import {
  AclRule,
  AclStats,
  CreateAclRequest,
  CreateUserRequest,
  TestAclRequest,
  TestAclResponse,
  User,
} from '../models/acl.model';

const SEED_USERS: User[] = [
  { username: 'admin', role: 'SUPER_ADMIN', created_at: new Date().toISOString() },
  { username: 'order_producer', role: 'PRODUCER', created_at: new Date().toISOString() },
  { username: 'billing_consumer', role: 'CONSUMER', created_at: new Date().toISOString() },
  { username: 'auditor', role: 'AUDITOR', created_at: new Date().toISOString() },
];

const SEED_RULES: AclRule[] = [
  {
    id: 'acl-order-producer-write-orders',
    principal: 'User:order_producer',
    resource_type: 'TOPIC',
    resource_name: 'orders-*',
    operation: 'WRITE',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-billing-consumer-read-orders',
    principal: 'User:billing_consumer',
    resource_type: 'TOPIC',
    resource_name: 'orders-*',
    operation: 'READ',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-billing-consumer-group',
    principal: 'User:billing_consumer',
    resource_type: 'GROUP',
    resource_name: 'billing-group',
    operation: 'READ',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-auditor-describe-topics',
    principal: 'User:auditor',
    resource_type: 'TOPIC',
    resource_name: '*',
    operation: 'DESCRIBE',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-auditor-describe-groups',
    principal: 'User:auditor',
    resource_type: 'GROUP',
    resource_name: '*',
    operation: 'DESCRIBE',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-auditor-describe-cluster',
    principal: 'User:auditor',
    resource_type: 'CLUSTER',
    resource_name: '*',
    operation: 'DESCRIBE',
    permission: 'ALLOW',
    created_at: new Date().toISOString(),
  },
  {
    id: 'acl-deny-billing-consumer-audit',
    principal: 'User:billing_consumer',
    resource_type: 'TOPIC',
    resource_name: 'audit-*',
    operation: 'WRITE',
    permission: 'DENY',
    created_at: new Date().toISOString(),
  },
];

@Injectable({
  providedIn: 'root',
})
export class AclService {
  private readonly http = inject(HttpClient);

  private readonly _rules = signal<AclRule[]>(SEED_RULES);
  readonly rules = this._rules.asReadonly();

  private readonly _users = signal<User[]>(SEED_USERS);
  readonly users = this._users.asReadonly();

  private readonly _loading = signal<boolean>(false);
  readonly loading = this._loading.asReadonly();

  readonly stats = computed<AclStats>(() => {
    const rulesList = this._rules();
    const usersList = this._users();

    const uniquePrincipals = new Set<string>();
    for (const r of rulesList) {
      uniquePrincipals.add(r.principal);
    }
    for (const u of usersList) {
      uniquePrincipals.add(`User:${u.username}`);
    }

    const superAdminCount = usersList.filter((u) => u.role === 'SUPER_ADMIN').length;
    const denyCount = rulesList.filter((r) => r.permission === 'DENY').length;

    return {
      totalRules: rulesList.length,
      activePrincipals: uniquePrincipals.size,
      superAdmins: superAdminCount,
      denyPolicies: denyCount,
    };
  });

  constructor() {
    this.refreshAll();
  }

  private getApiUrl(path: string): string {
    const custom = typeof localStorage !== 'undefined' ? localStorage.getItem('aeromq_api_url') : null;
    if (custom) {
      return `${custom.replace(/\/$/, '')}${path}`;
    }
    if (
      typeof window !== 'undefined' &&
      window.location.hostname === 'localhost' &&
      window.location.port === '4200'
    ) {
      return `http://localhost:9001${path}`;
    }
    return path;
  }

  refreshAll(): void {
    this.loadUsers();
    this.loadRules();
  }

  loadRules(): void {
    this._loading.set(true);
    const url = this.getApiUrl('/api/acls');
    this.http.get<AclRule[]>(url).pipe(
      catchError(() => of(SEED_RULES))
    ).subscribe({
      next: (rules) => {
        if (Array.isArray(rules) && rules.length > 0) {
          this._rules.set(rules);
        }
        this._loading.set(false);
      },
      error: () => this._loading.set(false),
    });
  }

  loadUsers(): void {
    const url = this.getApiUrl('/api/users');
    this.http.get<User[]>(url).pipe(
      catchError(() => of(SEED_USERS))
    ).subscribe({
      next: (users) => {
        if (Array.isArray(users) && users.length > 0) {
          this._users.set(users);
        }
      },
    });
  }

  createRule(req: CreateAclRequest): Observable<AclRule> {
    const url = this.getApiUrl('/api/acls');
    return this.http.post<AclRule>(url, req).pipe(
      catchError(() => {
        // Fallback local creation
        const newRule: AclRule = {
          id: `acl-${Date.now().toString(36)}`,
          principal: req.principal.startsWith('User:') || req.principal === '*' ? req.principal : `User:${req.principal}`,
          resource_type: req.resource_type,
          resource_name: req.resource_name,
          operation: req.operation,
          permission: req.permission,
          created_at: new Date().toISOString(),
        };
        return of(newRule);
      }),
      tap((created) => {
        this._rules.update((prev) => [created, ...prev.filter((r) => r.id !== created.id)]);
      })
    );
  }

  deleteRule(id: string): Observable<boolean> {
    const url = this.getApiUrl(`/api/acls/${encodeURIComponent(id)}`);
    return this.http.delete<{ deleted: boolean; id: string }>(url).pipe(
      map(() => true),
      catchError(() => of(true)),
      tap(() => {
        this._rules.update((prev) => prev.filter((r) => r.id !== id));
      })
    );
  }

  createUser(req: CreateUserRequest): Observable<User> {
    const url = this.getApiUrl('/api/users');
    return this.http.post<User>(url, req).pipe(
      catchError(() => {
        const newUser: User = {
          username: req.username.replace(/^User:/, ''),
          role: req.role,
          created_at: new Date().toISOString(),
        };
        return of(newUser);
      }),
      tap((created) => {
        this._users.update((prev) => [
          ...prev.filter((u) => u.username !== created.username),
          created,
        ]);
      })
    );
  }

  testAuthorization(req: TestAclRequest): Observable<TestAclResponse> {
    const url = this.getApiUrl('/api/acls/test');
    return this.http.post<TestAclResponse>(url, req).pipe(
      catchError(() => {
        // Local zero-trust evaluation fallback
        return of(this.localEvaluate(req));
      })
    );
  }

  private localEvaluate(req: TestAclRequest): TestAclResponse {
    const cleanPrincipal = req.principal.trim();
    const cleanUsername = cleanPrincipal.replace(/^User:/, '');

    const rules = this._rules();
    const users = this._users();

    const matches = rules.filter((r) => {
      // principal
      const rPrincipal = r.principal.replace(/^User:/, '');
      const pMatch = r.principal === '*' || r.principal === 'User:*' || rPrincipal.toLowerCase() === cleanUsername.toLowerCase();
      if (!pMatch) return false;

      // resource type
      if (r.resource_type !== req.resource_type) return false;

      // resource name
      const nameMatch = r.resource_name === '*' ||
        r.resource_name === req.resource_name ||
        (r.resource_name.endsWith('*') && req.resource_name.startsWith(r.resource_name.slice(0, -1)));
      if (!nameMatch) return false;

      // operation
      return r.operation === 'ALL' || r.operation === req.operation;
    });

    // 1. Explicit DENY
    const denyRule = matches.find((r) => r.permission === 'DENY');
    if (denyRule) {
      return {
        allowed: false,
        reason: `Explicit DENY rule matched: ${denyRule.id} on ${denyRule.resource_type} '${denyRule.resource_name}'`,
      };
    }

    // 2. SUPER_ADMIN
    const user = users.find((u) => u.username.toLowerCase() === cleanUsername.toLowerCase());
    if (user && user.role === 'SUPER_ADMIN') {
      return {
        allowed: true,
        reason: `Access granted: principal '${cleanUsername}' holds SUPER_ADMIN role with unrestricted privileges`,
      };
    }

    // 3. Explicit ALLOW
    const allowRule = matches.find((r) => r.permission === 'ALLOW');
    if (allowRule) {
      return {
        allowed: true,
        reason: `Access granted by rule ${allowRule.id} (${allowRule.operation} on ${allowRule.resource_name})`,
      };
    }

    // 4. Default Deny
    return {
      allowed: false,
      reason: `Zero-trust denial: no matching policy found for ${req.operation} on ${req.resource_type} '${req.resource_name}'`,
    };
  }
}
