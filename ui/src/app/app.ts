import { Component, inject, signal, computed, OnInit, ViewChild, ElementRef, HostListener } from '@angular/core';
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

export interface SearchResultItem {
  id: string;
  title: string;
  subtitle: string;
  category: 'PAGE' | 'TOPIC' | 'ACTION' | 'SCHEMA' | 'CONNECTOR';
  icon: string;
  path?: string;
  badge?: string;
  action?: () => void;
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

  @ViewChild('searchInput') searchInputElement?: ElementRef<HTMLInputElement>;
  @ViewChild('searchContainer') searchContainerElement?: ElementRef<HTMLElement>;

  readonly isMobile = signal<boolean>(false);
  readonly isSidebarCollapsed = signal<boolean>(false);
  readonly isMobileDrawerOpen = signal<boolean>(false);
  readonly currentPath = signal<string>('/cluster');

  // Global Search State
  readonly searchQuery = signal<string>('');
  readonly isSearchOpen = signal<boolean>(false);
  readonly selectedResultIndex = signal<number>(0);
  readonly liveTopics = signal<string[]>([
    'orders',
    'telemetry-events',
    'alerts-critical',
    'payment-transactions',
    'orders-sanitized'
  ]);

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
        { path: '/streams', label: 'Stream Processing', icon: 'query_stats', badge: 'STATE' },
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

  // Comprehensive Search Items Catalog
  private readonly staticSearchItems: SearchResultItem[] = [
    // Pages
    { id: 'page-cluster', title: 'Cluster Overview', subtitle: 'View broker status, Raft consensus, metrics & log storage', category: 'PAGE', icon: 'dashboard', path: '/cluster', badge: 'Core' },
    { id: 'page-topics', title: 'Topics & Partitions', subtitle: 'Manage topic metadata, partitions, retention & cleanup policy', category: 'PAGE', icon: 'folder_open', path: '/topics', badge: 'Core' },
    { id: 'page-messages', title: 'Message Explorer', subtitle: 'Search and inspect message records, offsets, headers & JSON payloads', category: 'PAGE', icon: 'mail', path: '/messages', badge: 'Data' },
    { id: 'page-producer', title: 'Web Producer Console', subtitle: 'Publish test records, burst events, and check produce latency', category: 'PAGE', icon: 'send', path: '/producer', badge: 'Tool' },
    { id: 'page-schemas', title: 'Schema Registry', subtitle: 'Register and validate Avro, JSON, and Protobuf schemas with compatibility checks', category: 'PAGE', icon: 'schema', path: '/schemas', badge: 'Governance' },
    { id: 'page-transforms', title: 'Stream Transforms', subtitle: 'In-broker WASM data transforms, PII masking, and JSON filtering', category: 'PAGE', icon: 'transform', path: '/transforms', badge: 'WASM' },
    { id: 'page-streams', title: 'Stream Processing', subtitle: 'Windowed aggregations, stream-table joins and interactive state queries', category: 'PAGE', icon: 'query_stats', path: '/streams', badge: 'STATE' },
    { id: 'page-connectors', title: 'Connectors Ecosystem', subtitle: 'Manage Kafka Connect sources, S3 archival sinks, and CDC plugins', category: 'PAGE', icon: 'cable', path: '/connectors', badge: 'Integration' },
    { id: 'page-groups', title: 'Consumer Groups', subtitle: 'Monitor consumer group rebalancing (KIP-848) and partition lag', category: 'PAGE', icon: 'group_work', path: '/groups', badge: 'Ops' },
    { id: 'page-acls', title: 'Security & ACLs', subtitle: 'Configure role-based access control (RBAC) and granular permission rules', category: 'PAGE', icon: 'admin_panel_settings', path: '/acls', badge: 'RBAC' },

    // Quick Actions
    {
      id: 'action-theme',
      title: 'Toggle Theme Mode',
      subtitle: 'Switch between Dark and Light mode across all components',
      category: 'ACTION',
      icon: 'brightness_6',
      badge: 'Appearance',
      action: () => this.themeService.toggleTheme()
    },
    {
      id: 'action-produce',
      title: 'Publish New Message',
      subtitle: 'Launch Web Producer to send single or burst messages',
      category: 'ACTION',
      icon: 'add_circle',
      path: '/producer',
      badge: 'Action'
    },
    {
      id: 'action-schema',
      title: 'Register Schema Subject',
      subtitle: 'Create new schema version in Schema Registry',
      category: 'ACTION',
      icon: 'post_add',
      path: '/schemas',
      badge: 'Action'
    },
    {
      id: 'action-transform',
      title: 'Deploy Stream Transform',
      subtitle: 'Create a new WASM or PII masking transform pipeline',
      category: 'ACTION',
      icon: 'auto_fix_high',
      path: '/transforms',
      badge: 'Action'
    },
    {
      id: 'action-connector',
      title: 'Deploy Connect Plugin',
      subtitle: 'Configure S3 sink, webhook sink, or database CDC connector',
      category: 'ACTION',
      icon: 'electrical_services',
      path: '/connectors',
      badge: 'Action'
    },
    {
      id: 'action-acl',
      title: 'Create Security ACL Rule',
      subtitle: 'Define Allow/Deny rule for principal and resource pattern',
      category: 'ACTION',
      icon: 'shield',
      path: '/acls',
      badge: 'Action'
    }
  ];

  readonly filteredSearchResults = computed(() => {
    const q = this.searchQuery().trim().toLowerCase();

    // Collect topic search items from live topics
    const topicItems: SearchResultItem[] = this.liveTopics().map(topicName => ({
      id: `topic-${topicName}`,
      title: topicName,
      subtitle: `Topic stream · browse messages & partition status`,
      category: 'TOPIC',
      icon: 'dataset',
      path: `/messages?topic=${encodeURIComponent(topicName)}`,
      badge: 'Topic'
    }));

    const all = [...this.staticSearchItems, ...topicItems];

    if (!q) {
      // Default / Quick access items when search bar is clicked empty
      return all.slice(0, 8);
    }

    return all.filter(item => {
      return (
        item.title.toLowerCase().includes(q) ||
        item.subtitle.toLowerCase().includes(q) ||
        item.category.toLowerCase().includes(q) ||
        (item.badge && item.badge.toLowerCase().includes(q)) ||
        (item.path && item.path.toLowerCase().includes(q))
      );
    }).slice(0, 10);
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
        this.closeSearch();
      });

    // Fetch live topics to populate search
    this.service.getTopics().subscribe({
      next: (topics) => {
        if (topics && topics.length > 0) {
          const names = topics.map(t => t.name).filter(Boolean);
          if (names.length > 0) {
            this.liveTopics.set(Array.from(new Set([...this.liveTopics(), ...names])));
          }
        }
      },
      error: () => {
        // Fallback live topics are preserved
      }
    });
  }

  // Global Keyboard Shortcuts (Press / or Ctrl+K / Cmd+K to search, Esc to close)
  @HostListener('document:keydown', ['$event'])
  handleGlobalShortcuts(event: KeyboardEvent): void {
    const target = event.target as HTMLElement;
    const isInput = target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable);

    // Open search on '/' (only when not typing in another input)
    if (event.key === '/' && !isInput) {
      event.preventDefault();
      this.openSearch();
      return;
    }

    // Open search on Ctrl+K or Cmd+K
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      this.openSearch();
      return;
    }

    // Close on Escape
    if (event.key === 'Escape' && this.isSearchOpen()) {
      this.closeSearch();
    }
  }

  // Click outside to close search results dropdown
  @HostListener('document:click', ['$event'])
  handleDocumentClick(event: MouseEvent): void {
    if (this.isSearchOpen() && this.searchContainerElement) {
      const clickedInside = this.searchContainerElement.nativeElement.contains(event.target as Node);
      if (!clickedInside) {
        this.closeSearch();
      }
    }
  }

  openSearch(): void {
    this.isSearchOpen.set(true);
    setTimeout(() => {
      this.searchInputElement?.nativeElement?.focus();
      this.searchInputElement?.nativeElement?.select();
    }, 50);
  }

  closeSearch(): void {
    this.isSearchOpen.set(false);
    this.selectedResultIndex.set(0);
  }

  clearSearch(event?: Event): void {
    if (event) {
      event.stopPropagation();
    }
    this.searchQuery.set('');
    this.selectedResultIndex.set(0);
    this.searchInputElement?.nativeElement?.focus();
  }

  onSearchInput(event: Event): void {
    const target = event.target as HTMLInputElement;
    this.searchQuery.set(target.value);
    this.selectedResultIndex.set(0);
    this.isSearchOpen.set(true);
  }

  onSearchFocus(): void {
    this.isSearchOpen.set(true);
  }

  onSearchKeydown(event: KeyboardEvent): void {
    const items = this.filteredSearchResults();
    if (!items.length) return;

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      this.selectedResultIndex.update(idx => (idx + 1) % items.length);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      this.selectedResultIndex.update(idx => (idx - 1 + items.length) % items.length);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      const selected = items[this.selectedResultIndex()];
      if (selected) {
        this.selectSearchResult(selected);
      }
    } else if (event.key === 'Escape') {
      event.preventDefault();
      this.closeSearch();
      this.searchInputElement?.nativeElement?.blur();
    }
  }

  selectSearchResult(item: SearchResultItem): void {
    if (item.action) {
      item.action();
    } else if (item.path) {
      this.router.navigateByUrl(item.path);
    }
    this.closeSearch();
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
