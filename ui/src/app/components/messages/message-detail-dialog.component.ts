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

export type MessagePayloadViewMode = 'hex' | 'base64' | 'text';

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
  viewMode: MessagePayloadViewMode = 'text';
  sanitizedKey = '';
  formattedContent = '';
  hexContent = '';
  base64Content = '';
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
    const hasBinaryBytes = /[\x00-\x08\x0E-\x1F\x7F-\xFF]/.test(raw);

    if (hasBinaryBytes) {
      // Check if it's actually valid JSON first (without loose substring regex)
      const trimmed = raw.trim();
      let parsedJson: any = null;
      if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        try {
          parsedJson = JSON.parse(trimmed);
        } catch {
          parsedJson = null;
        }
      }

      if (parsedJson) {
        this.isJson = true;
        this.formattedContent = JSON.stringify(parsedJson, null, 2);
        this.viewMode = 'text';
      } else {
        this.isBinary = true;
        this.viewMode = 'hex';
        // Clean text representation with dots for non-printables
        this.formattedContent = raw.replace(/[\x00-\x1F\x7F-\xFF]/g, (ch) => {
          if (ch === '\n' || ch === '\r' || ch === '\t') return ch;
          return '·';
        });
      }
    } else {
      try {
        const parsed = JSON.parse(raw);
        this.isJson = true;
        this.formattedContent = JSON.stringify(parsed, null, 2);
        this.viewMode = 'text';
      } catch {
        this.isJson = false;
        this.formattedContent = raw;
        this.viewMode = 'text';
      }
    }

    this.hexContent = this.generateHexDump(raw);
    this.base64Content = this.generateBase64(raw);
    this.lineCount = (this.viewMode === 'hex' ? this.hexContent : this.formattedContent).split('\n').length;
  }

  setViewMode(mode: MessagePayloadViewMode): void {
    this.viewMode = mode;
  }

  getActiveDisplayContent(): string {
    switch (this.viewMode) {
      case 'hex':
        return this.hexContent;
      case 'base64':
        return this.base64Content;
      case 'text':
      default:
        return this.formattedContent;
    }
  }

  generateBase64(input: string): string {
    if (!input) return '';
    try {
      let binary = '';
      for (let i = 0; i < input.length; i++) {
        binary += String.fromCharCode(input.charCodeAt(i) & 0xff);
      }
      return btoa(binary);
    } catch {
      return '(Base64 encoding unavailable)';
    }
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
      const asciiPart = chunk.map(b => (b >= 32 && b <= 126 ? String.fromCharCode(b) : '·')).join('');
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
    const toCopy = this.getActiveDisplayContent();
    const success = this.clipboard.copy(toCopy);
    const modeLabel = this.viewMode.toUpperCase();
    if (success) {
      this.snackBar.open(`Message payload (${modeLabel}) copied to clipboard!`, 'Dismiss', {
        duration: 3000,
        horizontalPosition: 'end',
        verticalPosition: 'bottom'
      });
    } else {
      navigator.clipboard.writeText(toCopy).then(() => {
        this.snackBar.open(`Message payload (${modeLabel}) copied to clipboard!`, 'Dismiss', {
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
