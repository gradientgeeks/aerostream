import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatTabsModule } from '@angular/material/tabs';
import { MatDividerModule } from '@angular/material/divider';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

import { RegisteredSchema, SchemaType } from '../../models/schema.model';
import { SchemaService } from '../../services/schema.service';
import { RegisterSchemaDialogComponent } from './register-schema-dialog.component';

@Component({
  selector: 'app-schema-registry',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    FormsModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatFormFieldModule,
    MatInputModule,
    MatDialogModule,
    MatSnackBarModule,
    MatTooltipModule,
    MatTabsModule,
    MatDividerModule,
    MatProgressSpinnerModule,
  ],
  templateUrl: './schema-registry.component.html',
  styleUrl: './schema-registry.component.scss',
})
export class SchemaRegistryComponent implements OnInit {
  protected readonly schemaService = inject(SchemaService);
  private readonly dialog = inject(MatDialog);
  private readonly snackBar = inject(MatSnackBar);
  private readonly route = inject(ActivatedRoute);

  readonly searchQuery = signal<string>('');
  readonly selectedSubject = signal<string | null>(null);
  readonly selectedTypeFilter = signal<string>('ALL');

  // Filtered subjects list
  readonly filteredSchemas = computed(() => {
    const list = this.schemaService.schemas();
    const query = this.searchQuery().trim().toLowerCase();
    const typeFilter = this.selectedTypeFilter();

    return list.filter((s) => {
      const matchesQuery =
        !query ||
        s.subject.toLowerCase().includes(query) ||
        s.type.toLowerCase().includes(query) ||
        (s.topic && s.topic.toLowerCase().includes(query));

      const matchesType = typeFilter === 'ALL' || s.type === typeFilter;

      return matchesQuery && matchesType;
    });
  });

  // Current active schema details
  readonly activeSchema = computed<RegisteredSchema | null>(() => {
    const sel = this.selectedSubject();
    if (!sel) {
      const list = this.filteredSchemas();
      return list.length > 0 ? list[0] : null;
    }
    return this.schemaService.getSchemaBySubject(sel) || null;
  });

  ngOnInit(): void {
    this.refresh();

    // Listen for query parameter e.g. /schemas?subject=orders-value
    this.route.queryParams.subscribe((params) => {
      const subject = params['subject'];
      if (subject) {
        if (this.schemaService.hasSubject(subject)) {
          this.selectedSubject.set(subject);
        } else if (this.schemaService.hasSubject(`${subject}-value`)) {
          this.selectedSubject.set(`${subject}-value`);
        }
      }
    });
  }

  refresh(): void {
    this.schemaService.loadSchemas().subscribe((schemas) => {
      const subjectParam = this.route.snapshot.queryParams['subject'];
      if (subjectParam) {
        if (this.schemaService.hasSubject(subjectParam)) {
          this.selectedSubject.set(subjectParam);
          return;
        } else if (this.schemaService.hasSubject(`${subjectParam}-value`)) {
          this.selectedSubject.set(`${subjectParam}-value`);
          return;
        }
      }
      if (schemas.length > 0) {
        if (!this.selectedSubject() || !schemas.some((s) => s.subject === this.selectedSubject())) {
          this.selectedSubject.set(schemas[0].subject);
        }
      } else {
        this.selectedSubject.set(null);
      }
    });
  }

  selectSubject(subject: string): void {
    this.selectedSubject.set(subject);
  }

  setTypeFilter(type: string): void {
    this.selectedTypeFilter.set(type);
  }

  openRegisterDialog(presetSubject?: string, presetType?: SchemaType): void {
    const dialogRef = this.dialog.open(RegisterSchemaDialogComponent, {
      width: '100%',
      maxWidth: '44rem',
      data: {
        subject: presetSubject || '',
        type: presetType || 'AVRO',
      },
    });

    dialogRef.afterClosed().subscribe((res) => {
      if (res?.success && res.schema) {
        this.selectedSubject.set(res.schema.subject);
        this.snackBar.open(
          `Schema for "${res.schema.subject}" registered successfully (v${res.schema.version})!`,
          'Close',
          {
            duration: 4000,
            horizontalPosition: 'end',
            verticalPosition: 'bottom',
            panelClass: ['snackbar-success'],
          }
        );
      }
    });
  }

  deleteSubject(subject: string): void {
    if (
      !confirm(
        `Are you sure you want to delete schema subject "${subject}"? This will remove all versions from the registry.`
      )
    ) {
      return;
    }

    this.schemaService.deleteSubject(subject).subscribe({
      next: () => {
        this.snackBar.open(`Subject "${subject}" successfully deleted`, 'Close', {
          duration: 3500,
          horizontalPosition: 'end',
          verticalPosition: 'bottom',
        });
        const remaining = this.schemaService.schemas();
        if (remaining.length > 0) {
          this.selectedSubject.set(remaining[0].subject);
        } else {
          this.selectedSubject.set(null);
        }
      },
      error: (err) => {
        this.snackBar.open(
          `Failed to delete subject "${subject}": ${err?.error?.message || err?.message || 'Error'}`,
          'Close',
          {
            duration: 5000,
            horizontalPosition: 'end',
            verticalPosition: 'bottom',
          }
        );
      },
    });
  }

  copySchemaToClipboard(schemaText: string): void {
    navigator.clipboard.writeText(schemaText).then(() => {
      this.snackBar.open('Schema contract copied to clipboard!', 'OK', {
        duration: 2500,
        horizontalPosition: 'end',
        verticalPosition: 'bottom',
      });
    });
  }

  downloadSchema(schema: RegisteredSchema): void {
    const ext = schema.type === 'AVRO' ? 'avsc' : schema.type === 'JSON' ? 'json' : 'proto';
    const blob = new Blob([schema.schema], { type: 'text/plain;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${schema.subject}-v${schema.version}.${ext}`;
    link.click();
    window.URL.revokeObjectURL(url);

    this.snackBar.open(`Downloaded ${link.download}`, 'OK', { duration: 2500 });
  }

  getTypeBadgeClass(type: SchemaType): string {
    switch (type) {
      case 'AVRO':
        return 'badge-avro';
      case 'JSON':
        return 'badge-json';
      case 'PROTOBUF':
        return 'badge-protobuf';
      default:
        return '';
    }
  }

  formatLines(text: string): string[] {
    return text ? text.split('\n') : [];
  }
}
