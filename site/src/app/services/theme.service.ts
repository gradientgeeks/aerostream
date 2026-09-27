import { Injectable, signal, computed } from '@angular/core';

export type ThemeMode = 'dark' | 'light';

@Injectable({
  providedIn: 'root'
})
export class ThemeService {
  private readonly THEME_STORAGE_KEY = 'aerostream-site-theme';

  readonly theme = signal<ThemeMode>(this.getInitialTheme());
  readonly isDark = computed(() => this.theme() === 'dark');

  constructor() {
    this.applyThemeToDOM(this.theme());
  }

  private getInitialTheme(): ThemeMode {
    if (typeof window !== 'undefined' && typeof localStorage !== 'undefined') {
      const savedTheme = localStorage.getItem(this.THEME_STORAGE_KEY) as ThemeMode | null;
      if (savedTheme === 'dark' || savedTheme === 'light') {
        return savedTheme;
      }
      if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
        return 'light';
      }
    }
    return 'dark';
  }

  toggleTheme(): void {
    const nextTheme: ThemeMode = this.theme() === 'dark' ? 'light' : 'dark';
    this.setTheme(nextTheme);
  }

  setTheme(newTheme: ThemeMode): void {
    this.theme.set(newTheme);
    if (typeof localStorage !== 'undefined') {
      try {
        localStorage.setItem(this.THEME_STORAGE_KEY, newTheme);
      } catch (e) {
        console.warn('Could not save theme preference to localStorage', e);
      }
    }
    this.applyThemeToDOM(newTheme);
  }

  private applyThemeToDOM(theme: ThemeMode): void {
    if (typeof document === 'undefined') return;

    const root = document.documentElement;
    const body = document.body;

    if (theme === 'dark') {
      root.classList.remove('light-theme', 'light');
      root.classList.add('dark-theme', 'dark');
      body.classList.remove('light-theme', 'light');
      body.classList.add('dark-theme', 'dark');
    } else {
      root.classList.remove('dark-theme', 'dark');
      root.classList.add('light-theme', 'light');
      body.classList.remove('dark-theme', 'dark');
      body.classList.add('light-theme', 'light');
    }
  }
}
