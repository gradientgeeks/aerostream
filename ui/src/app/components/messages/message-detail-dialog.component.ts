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
  formattedContent = '';
  lineCount = 1;

  ngOnInit(): void {
    const raw = this.data.payload || '';
    try {
      const parsed = JSON.parse(raw);
      this.isJson = true;
      this.formattedContent = JSON.stringify(parsed, null, 2);
    } catch {
      this.isJson = false;
      this.formattedContent = raw;
    }
    this.lineCount = this.formattedContent.split('\n').length;
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
