interface WorkerTicker { symbol: string; price: number; change: number; volume: number; high: number; low: number; timestamp: number }

const priceHistory = new Map<string, number[]>();

self.onmessage = ({ data }: MessageEvent<{ tickers: WorkerTicker[] }>) => {
  const normalized = data.tickers.map((ticker) => {
    const points = priceHistory.get(ticker.symbol) ?? [];
    points.push(ticker.price);
    const next = points.length > 60 ? points.slice(-60) : points;
    priceHistory.set(ticker.symbol, next);
    const movingAverage = next.reduce((sum: number, value: number) => sum + value, 0) / next.length;
    return { ...ticker, movingAverage: Number(movingAverage.toFixed(ticker.price < 1 ? 6 : 2)) };
  });
  const gainers = [...normalized].sort((a, b) => b.change - a.change).slice(0, 5);
  const losers = [...normalized].sort((a, b) => a.change - b.change).slice(0, 5);
  const totalVolume = normalized.reduce((sum: number, ticker) => sum + ticker.volume, 0);
  self.postMessage({ tickers: normalized, gainers, losers, totalVolume, processedAt: Date.now() });
};
