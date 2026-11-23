import { Component, ErrorHandler, Injectable, signal } from '@angular/core';

@Injectable()
export class GlobalErrorHandler implements ErrorHandler {
  readonly error = signal<string | null>(null);
  handleError(error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    this.error.set(message);
    console.error(error);
  }
}

@Component({
  selector: 'app-error-banner',
  template: `@if (handler.error(); as message) { <div class="error-banner" role="alert"><span>Runtime error</span>{{ message }}</div> }`,
  styles: [`.error-banner{position:fixed;z-index:20;left:24px;bottom:24px;background:#3a1212;border:1px solid #ff5d5d;color:#ffdede;padding:12px 16px;border-radius:10px}.error-banner span{font-weight:800;margin-right:8px}`]
})
export class ErrorBanner {
  constructor(public handler: GlobalErrorHandler) {}
}
