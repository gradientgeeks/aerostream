import { Component, inject, signal } from '@angular/core';
import { RouterOutlet, RouterLink, RouterLinkActive } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
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
export class App {
  protected readonly service = inject(AeroMQService);
  readonly themeService = inject(ThemeService);

  readonly isSidenavOpen = signal<boolean>(true);
  readonly showApiUrlDialog = signal<boolean>(false);
  readonly editingUrl = signal<string>(this.service.apiBaseUrl());

  toggleSidenav(): void {
    this.isSidenavOpen.update((open) => !open);
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
