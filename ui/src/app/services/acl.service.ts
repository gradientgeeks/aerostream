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

@Injectable({
  providedIn: 'root',
})
export class AclService {
  private readonly http = inject(HttpClient);

  private readonly _rules = signal<AclRule[]>([]);
  readonly rules = this._rules.asReadonly();

  private readonly _users = signal<User[]>([]);
  readonly users = this._users.asReadonly();

  private readonly _loading = signal<boolean>(false);
  readonly loading = this._loading.asReadonly();

  readonly stats = computed<AclStats>(() => {
    const rulesList = this._rules();
    const usersList = this._users();

    const uniquePrincipals = new Set<string>();
    for (const r of rulesList) {
      if (r.principal) {
        uniquePrincipals.add(r.principal);
      }
    }
    for (const u of usersList) {
      if (u.username) {
        uniquePrincipals.add(`User:${u.username}`);
      }
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
    const custom = typeof localStorage !== 'undefined'
      ? (localStorage.getItem('aerostream_api_url') || localStorage.getItem('aeromq_api_url') || localStorage.getItem('aerostream_api_base_url') || localStorage.getItem('aeromq_api_base_url'))
      : null;
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
      catchError((err) => {
        console.error('Failed to load ACL rules:', err);
        return of([] as AclRule[]);
      })
    ).subscribe({
      next: (rules) => {
        this._rules.set(Array.isArray(rules) ? rules : []);
        this._loading.set(false);
      },
      error: () => {
        this._rules.set([]);
        this._loading.set(false);
      },
    });
  }

  loadUsers(): void {
    const url = this.getApiUrl('/api/users');
    this.http.get<User[]>(url).pipe(
      catchError((err) => {
        console.error('Failed to load users:', err);
        return of([] as User[]);
      })
    ).subscribe({
      next: (users) => {
        this._users.set(Array.isArray(users) ? users : []);
      },
      error: () => {
        this._users.set([]);
      },
    });
  }

  createRule(req: CreateAclRequest): Observable<AclRule> {
    const url = this.getApiUrl('/api/acls');
    return this.http.post<AclRule>(url, req).pipe(
      tap((created) => {
        if (created && created.id) {
          this._rules.update((prev) => [created, ...prev.filter((r) => r.id !== created.id)]);
        }
      })
    );
  }

  deleteRule(id: string): Observable<boolean> {
    const url = this.getApiUrl(`/api/acls/${encodeURIComponent(id)}`);
    return this.http.delete<{ deleted: boolean; id: string }>(url).pipe(
      map(() => true),
      tap(() => {
        this._rules.update((prev) => prev.filter((r) => r.id !== id));
      })
    );
  }

  createUser(req: CreateUserRequest): Observable<User> {
    const url = this.getApiUrl('/api/users');
    return this.http.post<User>(url, req).pipe(
      tap((created) => {
        if (created && created.username) {
          this._users.update((prev) => [
            ...prev.filter((u) => u.username !== created.username),
            created,
          ]);
        }
      })
    );
  }

  testAcl(req: TestAclRequest): Observable<TestAclResponse> {
    const url = this.getApiUrl('/api/acls/test');
    return this.http.post<TestAclResponse>(url, req);
  }

  testAuthorization(req: TestAclRequest): Observable<TestAclResponse> {
    return this.testAcl(req);
  }
}
