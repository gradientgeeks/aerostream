import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { ClipboardModule, Clipboard } from '@angular/cdk/clipboard';

export interface MessageDetailData {
  topic: string;
  partition: number;
  offset: number;
  length: number;
  payload: string;
  key?: string;
  timestamp?: number;
  headers?: Record<string, string>;
}

@Component({
  selector: 'app-message-detail-dialog',
  standalone: true,
  imports: [
    CommonModule,
    MatDialogModule,
    MatButtonModule,
    MatIconModule,
    MatSnackBarModule,
    ClipboardModule
  ],
  templateUrl: './message-detail-dialog.component.html',
  styleUrl: './message-detail-dialog.component.scss'
})
export class MessageDetailDialogComponent implements OnInit {
  data: MessageDetailData = inject(MAT_DIALOG_DATA);
  private dialogRef = inject(MatDialogRef<MessageDetailDialogComponent>);
  private snackBar = inject(MatSnackBar);
  private clipboard = inject(Clipboard);

  isJson = false;
  isBinary = false;
  showHexView = false;
  sanitizedKey = '';
  formattedContent = '';
  hexContent = '';
  lineCount = 1;

  ngOnInit(): void {
    // Sanitize Key
    if (this.data.key) {
      this.sanitizedKey = this.data.key.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '').trim();
      if (!this.sanitizedKey && this.data.key.length > 0) {
        this.sanitizedKey = `[Binary Key: ${this.data.key.length}B]`;
      }
    }

    const raw = this.data.payload || '';
    const hasBinaryBytes = /[\x00-\x08\x0E-\x1F]/.test(raw);

    if (hasBinaryBytes) {
      const jsonMatch = raw.match(/(\{[\s\S]*\}|\[[\s\S]*\])/);
      if (jsonMatch) {
        try {
          const parsed = JSON.parse(jsonMatch[0]);
          this.isJson = true;
          this.formattedContent = JSON.stringify(parsed, null, 2);
        } catch {
          this.isBinary = true;
          this.formattedContent = raw.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '.');
        }
      } else {
        this.isBinary = true;
        this.formattedContent = raw.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, '.');
      }
    } else {
      try {
        const parsed = JSON.parse(raw);
        this.isJson = true;
        this.formattedContent = JSON.stringify(parsed, null, 2);
      } catch {
        this.isJson = false;
        this.formattedContent = raw;
      }
    }

    this.hexContent = this.generateHexDump(raw);
    this.lineCount = this.formattedContent.split('\n').length;
  }

  toggleHexView(): void {
    this.showHexView = !this.showHexView;
  }

  generateHexDump(input: string): string {
    if (!input) return '';
    const bytes: number[] = [];
    for (let i = 0; i < input.length; i++) {
      bytes.push(input.charCodeAt(i) & 0xff);
    }
    const lines: string[] = [];
    for (let i = 0; i < bytes.length; i += 16) {
      const chunk = bytes.slice(i, i + 16);
      const offsetHex = i.toString(16).padStart(8, '0');
      const hexPart = chunk.map(b => b.toString(16).padStart(2, '0')).join(' ').padEnd(48, ' ');
      const asciiPart = chunk.map(b => (b >= 32 && b <= 126 ? String.fromCharCode(b) : '.')).join('');
      lines.push(`${offsetHex}  ${hexPart}  |${asciiPart}|`);
    }
    return lines.join('\n');
  }

  hasHeaders(): boolean {
    return !!this.data.headers && Object.keys(this.data.headers).length > 0;
  }

  headerEntries(): [string, string][] {
    return this.data.headers ? Object.entries(this.data.headers) : [];
  }

  copyPayload(): void {
    const success = this.clipboard.copy(this.formattedContent);
    if (success) {
      this.snackBar.open('Message payload copied to clipboard!', 'Dismiss', {
        duration: 3000,
        horizontalPosition: 'end',
        verticalPosition: 'bottom'
      });
    } else {
      navigator.clipboard.writeText(this.formattedContent).then(() => {
        this.snackBar.open('Message payload copied to clipboard!', 'Dismiss', {
          duration: 3000,
          horizontalPosition: 'end',
          verticalPosition: 'bottom'
        });
      });
    }
  }

  onClose(): void {
    this.dialogRef.close();
  }
}
