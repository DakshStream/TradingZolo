import { ChangeDetectionStrategy, Component, DestroyRef, inject, OnInit, signal } from '@angular/core';
import { IndiaIndex, IndiaMarketService } from './india-market.service';
import { ErrorBanner, GlobalErrorHandler } from './error-boundary';

@Component({
  selector: 'app-root',
  imports: [ErrorBanner],
  providers: [GlobalErrorHandler],
  templateUrl: './dashboard.html',
  styleUrl: './india-dashboard.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class App implements OnInit {
  protected readonly market = inject(IndiaMarketService);
  protected readonly activeTab = signal<'overview' | 'indices' | 'watchlist'>('overview');
  private readonly destroyRef = inject(DestroyRef);

  ngOnInit(): void {
    this.market.connect();
    void this.market.fetchHistory();
    this.destroyRef.onDestroy(() => this.market.disconnect());
  }

  setTab(tab: 'overview' | 'indices' | 'watchlist'): void { this.activeTab.set(tab); }
  formatPrice(value: number): string { return value.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
  formatTime(time: number): string { return time ? new Date(time).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : 'Connecting'; }
  chartPath(): string {
    const points = this.market.chartPoints();
    if (!points.length) return '';
    const width = 800; const height = 300; const pad = 10;
    const values = points.map((point) => point.close);
    const min = Math.min(...values); const max = Math.max(...values); const range = max - min || 1;
    return points.map((point, index) => {
      const x = pad + (index / Math.max(points.length - 1, 1)) * (width - pad * 2);
      const y = height - pad - ((point.close - min) / range) * (height - pad * 2);
      return `${index ? 'L' : 'M'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    }).join(' ');
  }
  chartArea(): string {
    const line = this.chartPath();
    return line ? `${line} L790 290 L10 290 Z` : '';
  }
  trackIndex(_index: number, item: IndiaIndex): string { return item.symbol; }
  selectedIndex(): IndiaIndex | undefined {
    return this.market.indices().find((item) => item.symbol === this.market.selected());
  }
}
