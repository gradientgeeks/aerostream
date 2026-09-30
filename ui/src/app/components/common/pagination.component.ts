import { Component, input, output, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';

@Component({
  selector: 'app-pagination',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatButtonModule,
    MatIconModule,
    MatTooltipModule,
  ],
  template: `
    <div class="pagination-bar flex flex-wrap items-center justify-between gap-3 px-4 py-3 border-t border-app bg-app-card text-xs text-app-muted min-w-0 w-full select-none">
      <!-- Left: Range Information -->
      <div class="pagination-info flex items-center gap-1.5 whitespace-nowrap min-w-0">
        <span>Showing</span>
        <span class="font-semibold text-app font-mono">{{ startItem() }} - {{ endItem() }}</span>
        <span>of</span>
        <span class="font-semibold text-app font-mono">{{ totalItems() }}</span>
        <span>{{ itemLabel() }}</span>
      </div>

      <!-- Right: Controls (Page size selector + Page navigation) -->
      <div class="pagination-controls flex flex-wrap items-center gap-2 sm:gap-4">
        <!-- Page Size Selector -->
        <div class="page-size-selector flex items-center gap-1.5 whitespace-nowrap" *ngIf="pageSizeOptions().length > 1">
          <label class="text-xs text-app-muted">Per page:</label>
          <select
            [ngModel]="pageSize()"
            (ngModelChange)="onPageSizeChange($event)"
            class="page-size-select bg-app-nested text-app border border-app rounded px-2 py-1 text-xs outline-none cursor-pointer focus:border-cyan-500"
            aria-label="Select items per page"
          >
            <option *ngFor="let opt of pageSizeOptions()" [value]="opt">{{ opt }}</option>
          </select>
        </div>

        <!-- Page Indicator -->
        <div class="page-indicator whitespace-nowrap font-mono text-xs">
          <span>Page <strong class="text-app">{{ totalItems() === 0 ? 0 : pageIndex() + 1 }}</strong> of <strong class="text-app">{{ totalPages() }}</strong></span>
        </div>

        <!-- Navigation Buttons -->
        <div class="pagination-nav-btns flex items-center gap-1">
          <button
            type="button"
            mat-icon-button
            (click)="firstPage()"
            [disabled]="!hasPrevious()"
            matTooltip="First Page"
            class="pag-btn"
            aria-label="Go to first page"
          >
            <mat-icon class="pag-icon">first_page</mat-icon>
          </button>

          <button
            type="button"
            mat-icon-button
            (click)="previousPage()"
            [disabled]="!hasPrevious()"
            matTooltip="Previous Page"
            class="pag-btn"
            aria-label="Go to previous page"
          >
            <mat-icon class="pag-icon">chevron_left</mat-icon>
          </button>

          <button
            type="button"
            mat-icon-button
            (click)="nextPage()"
            [disabled]="!hasNext()"
            matTooltip="Next Page"
            class="pag-btn"
            aria-label="Go to next page"
          >
            <mat-icon class="pag-icon">chevron_right</mat-icon>
          </button>

          <button
            type="button"
            mat-icon-button
            (click)="lastPage()"
            [disabled]="!hasNext()"
            matTooltip="Last Page"
            class="pag-btn"
            aria-label="Go to last page"
          >
            <mat-icon class="pag-icon">last_page</mat-icon>
          </button>
        </div>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      width: 100%;
      max-width: 100%;
    }

    .pagination-bar {
      border-color: var(--app-border, #1E293B);
      background: var(--app-surface-card, #0F172A);
      color: var(--app-text-muted, #94A3B8);
    }

    .page-size-select {
      background-color: var(--app-surface-nested, #1E293B);
      border-color: var(--app-border, #334155);
      color: var(--app-text, #F8FAFC);
    }

    .pag-btn {
      width: 30px !important;
      height: 30px !important;
      line-height: 30px !important;
      padding: 0 !important;
      display: inline-flex !important;
      align-items: center !important;
      justify-content: center !important;
      border-radius: 6px !important;
      color: var(--app-text-muted, #94A3B8) !important;
      transition: background 0.15s ease, color 0.15s ease;

      &:hover:not([disabled]) {
        background-color: rgba(255, 255, 255, 0.08) !important;
        color: var(--app-text, #FFFFFF) !important;
      }

      &[disabled] {
        opacity: 0.35 !important;
        cursor: not-allowed !important;
      }
    }

    .pag-icon {
      font-size: 18px !important;
      width: 18px !important;
      height: 18px !important;
      line-height: 18px !important;
    }
  `]
})
export class PaginationComponent {
  readonly totalItems = input.required<number>();
  readonly pageSize = input<number>(10);
  readonly pageIndex = input<number>(0);
  readonly pageSizeOptions = input<number[]>([10, 25, 50, 100]);
  readonly itemLabel = input<string>('items');

  readonly pageIndexChange = output<number>();
  readonly pageSizeChange = output<number>();

  readonly totalPages = computed(() => {
    const total = this.totalItems();
    const size = this.pageSize();
    if (total <= 0 || size <= 0) return 1;
    return Math.ceil(total / size);
  });

  readonly startItem = computed(() => {
    if (this.totalItems() === 0) return 0;
    return this.pageIndex() * this.pageSize() + 1;
  });

  readonly endItem = computed(() => {
    return Math.min((this.pageIndex() + 1) * this.pageSize(), this.totalItems());
  });

  readonly hasPrevious = computed(() => {
    return this.pageIndex() > 0;
  });

  readonly hasNext = computed(() => {
    return this.pageIndex() < this.totalPages() - 1;
  });

  firstPage(): void {
    if (this.hasPrevious()) {
      this.pageIndexChange.emit(0);
    }
  }

  previousPage(): void {
    if (this.hasPrevious()) {
      this.pageIndexChange.emit(this.pageIndex() - 1);
    }
  }

  nextPage(): void {
    if (this.hasNext()) {
      this.pageIndexChange.emit(this.pageIndex() + 1);
    }
  }

  lastPage(): void {
    if (this.hasNext()) {
      this.pageIndexChange.emit(this.totalPages() - 1);
    }
  }

  onPageSizeChange(newSize: string | number): void {
    const parsed = Number(newSize) || 10;
    this.pageSizeChange.emit(parsed);
    this.pageIndexChange.emit(0);
  }
}
