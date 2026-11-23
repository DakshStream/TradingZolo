import { Injectable, NgZone, signal } from '@angular/core';
import { Subject, timer } from 'rxjs';
import { auditTime, filter, map } from 'rxjs/operators';

export interface Ticker {
  symbol: string; price: number; change: number; volume: number;
  high: number; low: number; timestamp: number; movingAverage?: number;
}

@Injectable({ providedIn: 'root' })
export class MarketService {
  readonly status = signal<'connecting' | 'live' | 'reconnecting' | 'offline'>('connecting');
  readonly tickers = signal<Ticker[]>([]);
  readonly gainers = signal<Ticker[]>([]);
  readonly losers = signal<Ticker[]>([]);
  readonly totalVolume = signal(0);
  readonly errors = new Subject<string>();
  readonly streamedTickers = new Subject<Ticker[]>();
  private socket?: WebSocket;
  private worker?: Worker;
  private retry = 0;

  constructor(private zone: NgZone) {
    this.worker = new Worker(new URL('./market.worker.ts', import.meta.url), { type: 'module' });
    this.worker.onmessage = ({ data }) => {
      this.tickers.set(data.tickers);
      this.gainers.set(data.gainers);
      this.losers.set(data.losers);
      this.totalVolume.set(data.totalVolume);
    };
    this.streamedTickers.pipe(
      auditTime(250),
      filter((items) => items.length > 0),
      map((items) => items)
    ).subscribe((items) => this.worker?.postMessage({ tickers: items }));
  }

  connect(): void {
    if (typeof WebSocket === 'undefined') return;
    this.status.set(this.retry ? 'reconnecting' : 'connecting');
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.socket = new WebSocket(`${protocol}//${location.hostname}:8080/stream`);

    this.socket.onopen = () => this.zone.run(() => {
      this.retry = 0;
      this.status.set('live');
    });
    this.socket.onmessage = (event) => this.zone.run(() => {
      const message = JSON.parse(event.data);
      if (message.type === 'ticker') this.streamedTickers.next(message.data);
    });
    this.socket.onerror = () => this.zone.run(() => {
      this.errors.next('Market stream encountered a network error');
    });
    this.socket.onclose = () => this.zone.run(() => {
      this.status.set('reconnecting');
      this.retry += 1;
      const delay = Math.min(30000, 1000 * 2 ** (this.retry - 1));
      timer(delay).subscribe(() => this.connect());
    });
  }

  disconnect(): void {
    this.retry = 0;
    this.socket?.close();
  }

  destroy(): void {
    this.worker?.terminate();
  }
}
