import { Injectable, NgZone, signal } from '@angular/core';
import { timer } from 'rxjs';

export interface IndiaIndex {
  symbol: string; name: string; shortName: string; price: number;
  previousClose: number; change: number; high: number; low: number; volume: number; timestamp: number;
}
export interface ChartPoint { time: number; close: number }

@Injectable({ providedIn: 'root' })
export class IndiaMarketService {
  readonly status = signal<'connecting' | 'live' | 'reconnecting' | 'offline'>('connecting');
  readonly indices = signal<IndiaIndex[]>([]);
  readonly chartPoints = signal<ChartPoint[]>([]);
  readonly selected = signal('^NSEI');
  readonly range = signal<'today' | '30d' | '1y'>('today');
  readonly lastUpdated = signal<number>(0);
  private socket?: WebSocket;
  private retry = 0;

  constructor(private zone: NgZone) {
    this.fetchHistory();
  }

  connect(): void {
    this.status.set(this.retry ? 'reconnecting' : 'connecting');
    this.socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/stream`);
    this.socket.onopen = () => this.zone.run(() => {
      this.retry = 0;
      this.status.set('live');
    });
    this.socket.onmessage = (event) => this.zone.run(() => {
      const message = JSON.parse(event.data);
      if (message.type === 'indices') {
        this.indices.set(message.data);
        this.lastUpdated.set(message.timestamp);
      }
    });
    this.socket.onerror = () => this.zone.run(() => this.status.set('reconnecting'));
    this.socket.onclose = () => this.zone.run(() => {
      this.retry += 1;
      this.status.set('reconnecting');
      timer(Math.min(30000, 1000 * 2 ** (this.retry - 1))).subscribe(() => this.connect());
    });
  }

  async fetchHistory(): Promise<void> {
    const selected = encodeURIComponent(this.selected());
    const range = this.range();
    try {
      const response = await fetch(`/api/history/${selected}?range=${range}`);
      if (!response.ok) throw new Error(`History request failed: ${response.status}`);
      const payload = await response.json();
      this.chartPoints.set((payload.points || []).map((point: ChartPoint) => ({ time: point.time, close: Number(point.close) })));
    } catch (error) {
      console.error(error);
      this.status.set('offline');
    }
  }

  selectIndex(symbol: string): void {
    this.selected.set(symbol);
    void this.fetchHistory();
  }

  selectRange(range: 'today' | '30d' | '1y'): void {
    this.range.set(range);
    void this.fetchHistory();
  }

  disconnect(): void { this.retry = 0; this.socket?.close(); }
}
