import { Component, inject, signal, ChangeDetectionStrategy, HostListener, DestroyRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet, RouterLink, Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';
import { TurbineLogoComponent } from './components/logo/turbine-logo.component';
import { ThemeService } from './services/theme.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    RouterOutlet,
    RouterLink,
    MatIconModule,
    MatButtonModule,
    MatTooltipModule,
    TurbineLogoComponent,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  readonly themeService = inject(ThemeService);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  readonly isMobileMenuOpen = signal<boolean>(false);

  constructor() {
    this.router.events.pipe(
      filter((e): e is NavigationEnd => e instanceof NavigationEnd),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.closeMobileMenu();
    });
  }

  @HostListener('window:resize')
  onResize(): void {
    if (typeof window !== 'undefined' && window.innerWidth > 768 && this.isMobileMenuOpen()) {
      this.closeMobileMenu();
    }
  }

  toggleMobileMenu(): void {
    this.isMobileMenuOpen.update((v) => !v);
  }

  closeMobileMenu(): void {
    this.isMobileMenuOpen.set(false);
  }

  toggleTheme(): void {
    this.themeService.toggleTheme();
  }

  scrollToTop(event?: Event): void {
    this.closeMobileMenu();
    const currentPath = this.router.url.split('#')[0].split('?')[0];
    const isHome = currentPath === '/' || currentPath === '';

    if (isHome) {
      if (event) {
        event.preventDefault();
      }
      if (typeof window !== 'undefined') {
        window.scrollTo({ top: 0, behavior: 'smooth' });
        history.pushState(null, '', '/');
      }
    }
  }

  navigateToSection(fragment: string, event?: Event): void {
    this.closeMobileMenu();
    const currentPath = this.router.url.split('#')[0].split('?')[0];
    const isHome = currentPath === '/' || currentPath === '';

    if (isHome) {
      if (event) {
        event.preventDefault();
      }
      const el = document.getElementById(fragment);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        history.pushState(null, '', `/#${fragment}`);
      }
    } else {
      if (event) {
        event.preventDefault();
      }
      this.router.navigate(['/'], { fragment }).then(() => {
        setTimeout(() => {
          const el = document.getElementById(fragment);
          if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }
        }, 150);
      });
    }
  }
}

