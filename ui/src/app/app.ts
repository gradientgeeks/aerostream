import { Component, inject, signal, computed, OnInit } from '@angular/core';
import { RouterOutlet, RouterLink, RouterLinkActive, Router, NavigationEnd } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { BreakpointObserver } from '@angular/cdk/layout';
import { filter } from 'rxjs/operators';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatListModule } from '@angular/material/list';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatBadgeModule } from '@angular/material/badge';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatChipsModule } from '@angular/material/chips';
import { MatDividerModule } from '@angular/material/divider';
import { AeroMQService } from './services/aeromq.service';
import { ThemeService } from './services/theme.service';
import { TurbineLogoComponent } from './components/logo/turbine-logo.component';

export interface NavItem {
  path: string;
  label: string;
  icon: string;
  badge?: string;
}

export interface NavSection {
  title: string;
  items: NavItem[];
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    RouterOutlet,
    RouterLink,
    RouterLinkActive,
    FormsModule,
    MatToolbarModule,
    MatSidenavModule,
    MatListModule,
    MatIconModule,
    MatButtonModule,
    MatBadgeModule,
    MatTooltipModule,
    MatSlideToggleModule,
    MatChipsModule,
    MatDividerModule,
    TurbineLogoComponent,
  ],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App implements OnInit {
  protected readonly service = inject(AeroMQService);
  readonly themeService = inject(ThemeService);
  private breakpointObserver = inject(BreakpointObserver);
  private router = inject(Router);

  readonly isMobile = signal<boolean>(false);
  readonly isSidebarCollapsed = signal<boolean>(false);
  readonly isMobileDrawerOpen = signal<boolean>(false);
  readonly currentPath = signal<string>('/cluster');

  readonly showApiUrlDialog = signal<boolean>(false);
  readonly editingUrl = signal<string>(this.service.apiBaseUrl());

  readonly navSections: NavSection[] = [
    {
      title: 'Core Streaming',
      items: [
        { path: '/cluster', label: 'Cluster Overview', icon: 'dashboard' },
        { path: '/topics', label: 'Topics & Partitions', icon: 'folder_open' },
        { path: '/messages', label: 'Message Explorer', icon: 'mail' },
        { path: '/producer', label: 'Web Producer', icon: 'send' },
      ]
    },
    {
      title: 'Data & Governance',
      items: [
        { path: '/schemas', label: 'Schema Registry', icon: 'schema' },
        { path: '/transforms', label: 'Stream Transforms', icon: 'transform', badge: 'WASM' },
        { path: '/connectors', label: 'Connectors', icon: 'cable' },
      ]
    },
    {
      title: 'Security & Ops',
      items: [
        { path: '/groups', label: 'Consumer Groups', icon: 'group_work' },
        { path: '/acls', label: 'Security & ACLs', icon: 'admin_panel_settings', badge: 'RBAC' },
      ]
    }
  ];

  readonly currentSectionTitle = computed(() => {
    const path = this.currentPath();
    for (const section of this.navSections) {
      for (const item of section.items) {
        if (path === item.path || path.startsWith(item.path + '/') || path.startsWith(item.path + '?')) {
          return item.label;
        }
      }
    }
    return 'Console';
  });

  ngOnInit(): void {
    // Restore collapsed preference on desktop
    try {
      const saved = localStorage.getItem('aeromq_sidebar_collapsed');
      if (saved !== null) {
        this.isSidebarCollapsed.set(saved === 'true');
      }
    } catch {
      // Ignore storage errors
    }

    // Responsive screen detection using CDK BreakpointObserver
    this.breakpointObserver.observe(['(max-width: 1024px)']).subscribe((result) => {
      this.isMobile.set(result.matches);
      if (result.matches) {
        this.isMobileDrawerOpen.set(false);
      }
    });

    // Track active route for breadcrumbs & auto-close mobile drawer on navigation
    this.currentPath.set(this.router.url);
    this.router.events
      .pipe(filter((event): event is NavigationEnd => event instanceof NavigationEnd))
      .subscribe((event) => {
        this.currentPath.set(event.urlAfterRedirects || event.url);
        if (this.isMobile()) {
          this.isMobileDrawerOpen.set(false);
        }
      });
  }

  toggleSidebar(): void {
    if (this.isMobile()) {
      this.isMobileDrawerOpen.update((open) => !open);
    } else {
      this.isSidebarCollapsed.update((collapsed) => {
        const next = !collapsed;
        try {
          localStorage.setItem('aeromq_sidebar_collapsed', String(next));
        } catch {
          // Ignore storage errors
        }
        return next;
      });
    }
  }

  closeMobileDrawer(): void {
    if (this.isMobile()) {
      this.isMobileDrawerOpen.set(false);
    }
  }

  openUrlDialog(): void {
    this.editingUrl.set(this.service.apiBaseUrl());
    this.showApiUrlDialog.set(true);
  }

  saveUrl(): void {
    this.service.setApiBaseUrl(this.editingUrl());
    this.showApiUrlDialog.set(false);
  }

  closeUrlDialog(): void {
    this.showApiUrlDialog.set(false);
  }

  onToggleAutoRefresh(): void {
    this.service.toggleAutoRefresh();
  }
}
