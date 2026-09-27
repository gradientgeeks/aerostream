import { Component, Inject, OnInit, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { CompatibilityMode, SchemaType } from '../../models/schema.model';
import { SchemaService } from '../../services/schema.service';

export interface RegisterSchemaDialogData {
  subject?: string;
  type?: SchemaType;
}

@Component({
  selector: 'app-register-schema-dialog',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatDialogModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatChipsModule,
    MatTooltipModule,
    MatButtonToggleModule,
  ],
  templateUrl: './register-schema-dialog.component.html',
  styleUrl: './register-schema-dialog.component.scss',
})
export class RegisterSchemaDialogComponent implements OnInit {
  private readonly fb = inject(FormBuilder);
  private readonly schemaService = inject(SchemaService);
  private readonly dialogRef = inject(MatDialogRef<RegisterSchemaDialogComponent>);

  readonly schemaTypes: SchemaType[] = ['AVRO', 'JSON', 'PROTOBUF'];
  readonly compatibilityModes: CompatibilityMode[] = ['BACKWARD', 'FORWARD', 'FULL', 'NONE'];

  isSubmitting = signal(false);
  validationResult = signal<{ valid: boolean; error?: string }>({ valid: true });

  form!: FormGroup;

  constructor(@Inject(MAT_DIALOG_DATA) public data?: RegisterSchemaDialogData) {}

  ngOnInit(): void {
    const initialType: SchemaType = this.data?.type || 'AVRO';
    const initialSubject = this.data?.subject || '';
    const initialTemplate = this.schemaService.getTemplate(initialType);

    this.form = this.fb.group({
      subject: [
        initialSubject,
        [
          Validators.required,
          Validators.minLength(2),
          Validators.pattern(/^[a-zA-Z0-9._-]+$/),
        ],
      ],
      type: [initialType, [Validators.required]],
      compatibility: ['BACKWARD', [Validators.required]],
      schema: [initialTemplate, [Validators.required]],
      description: [''],
    });

    this.validateCurrentSchema();

    this.form.get('schema')?.valueChanges.subscribe(() => {
      this.validateCurrentSchema();
    });

    this.form.get('type')?.valueChanges.subscribe((newType: SchemaType) => {
      this.validateCurrentSchema();
    });
  }

  onTypeChange(type: SchemaType): void {
    this.form.patchValue({ type });
    // If the schema matches any existing template or is empty, auto-populate with the new template
    const currentVal = this.form.get('schema')?.value?.trim();
    if (!currentVal || this.isKnownTemplate(currentVal)) {
      this.loadTemplate();
    } else {
      this.validateCurrentSchema();
    }
  }

  loadTemplate(): void {
    const type: SchemaType = this.form.get('type')?.value || 'AVRO';
    const template = this.schemaService.getTemplate(type);
    this.form.patchValue({ schema: template });
    this.validateCurrentSchema();
  }

  private isKnownTemplate(val: string): boolean {
    return Object.values(this.schemaTypes).some(
      (t) => this.schemaService.getTemplate(t).trim() === val
    );
  }

  validateCurrentSchema(): void {
    const type: SchemaType = this.form?.get('type')?.value || 'AVRO';
    const raw: string = this.form?.get('schema')?.value || '';
    const res = this.schemaService.validateSchemaDefinition(type, raw);
    this.validationResult.set(res);
  }

  onSubmit(): void {
    if (this.form.invalid || !this.validationResult().valid || this.isSubmitting()) {
      return;
    }

    this.isSubmitting.set(true);
    const val = this.form.value;

    this.schemaService
      .registerSchema({
        subject: val.subject.trim(),
        type: val.type,
        schema: val.schema,
        compatibility: val.compatibility,
        description: val.description?.trim(),
      })
      .subscribe({
        next: (registered) => {
          this.isSubmitting.set(false);
          this.dialogRef.close({ success: true, schema: registered });
        },
        error: (err) => {
          this.isSubmitting.set(false);
          this.validationResult.set({
            valid: false,
            error: err?.error?.message || err?.message || 'Failed to register schema with registry',
          });
        },
      });
  }

  onCancel(): void {
    this.dialogRef.close();
  }
}
