'use strict';

const express = require('express');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const https = require('https');
const path = require('path');

const app = express();
app.use(cors());
const server = http.createServer(app);
const clients = new Set();
const wss = new WebSocket.Server({ server, path: '/stream' });

app.get('/health', (_req, res) => {
  res.json({ ok: true, upstream: upstreamReady ? 'connected' : 'connecting', clients: clients.size });
});

let upstreamReady = false;
let upstream;
let reconnectTimer;
let latest = [];

function connectUpstream() {
  upstream = new WebSocket('wss://stream.binance.com:9443/ws/!ticker@arr');
  upstream.on('open', () => {
    upstreamReady = true;
    console.log('Connected to Binance market-data stream');
  });
  upstream.on('message', (payload) => {
    try {
      const raw = JSON.parse(payload.toString());
      const normalized = raw
        .filter((item) => item.s && item.s.endsWith('USDT'))
        .slice(0, 500)
        .map((item) => ({
          symbol: item.s,
          price: Number(item.c),
          change: Number(item.P),
          volume: Number(item.q),
          high: Number(item.h),
          low: Number(item.l),
          timestamp: Date.now()
        }));
      if (!normalized.length) return;
      latest = normalized;
      const message = JSON.stringify({ type: 'ticker', data: normalized, source: 'Binance', timestamp: Date.now() });
      for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) client.send(message);
      }
    } catch (error) {
      console.error('Failed to normalize upstream payload', error.message);
    }
  });
  upstream.on('close', () => {
    upstreamReady = false;
    console.warn('Binance stream disconnected; reconnecting in 3s');
    reconnectTimer = setTimeout(connectUpstream, 3000);
  });
  upstream.on('error', (error) => {
    upstreamReady = false;
    console.error('Binance stream error:', error.message);
  });
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'TradingZolo/1.0' } }, (response) => {
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => {
        if (response.statusCode < 200 || response.statusCode >= 300) {
          reject(new Error(`HTTP ${response.statusCode}`));
          return;
        }
        try { resolve(JSON.parse(body)); } catch (error) { reject(error); }
      });
    }).on('error', reject);
  });
}

function postJson(url, body) {
  const data = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data), 'User-Agent': 'TradingZolo/1.0' }
    }, (response) => {
      let payload = '';
      response.on('data', (chunk) => { payload += chunk; });
      response.on('end', () => {
        if (response.statusCode < 200 || response.statusCode >= 300) return reject(new Error(`HTTP ${response.statusCode}`));
        try { resolve(JSON.parse(payload)); } catch (error) { reject(error); }
      });
    });
    request.on('error', reject);
    request.end(data);
  });
}

let restTimer;

async function fetchRestSnapshot() {
  try {
    const payload = await getJson('https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD,SOLUSD,XRPUSD,ADAUSD,DOTUSD,LINKUSD,AVAXUSD');
    if (payload.error?.length) throw new Error(payload.error.join(', '));
    const normalized = Object.entries(payload.result || {}).map(([pair, item]) => {
      const names = { XXBTZUSD: 'BTCUSDT', XETHZUSD: 'ETHUSDT', SOLUSD: 'SOLUSDT', XXRPZUSD: 'XRPUSDT', XRPUZD: 'XRPUSDT', XADAUSD: 'ADAUSDT', ADAUSD: 'ADAUSDT', DOTUSD: 'DOTUSDT', LINKUSD: 'LINKUSDT', AVAXUSD: 'AVAXUSDT' };
      const symbol = names[pair] || pair.replace(/ZUSD$/i, 'USDT').replace(/USD$/i, 'USDT').replace(/^X/, '');
      const price = Number(item.c?.[0] || 0);
      const openRaw = Array.isArray(item.o) ? item.o[0] : item.o;
      const open = Number(openRaw || price);
      const highRaw = Array.isArray(item.h) ? item.h[item.h.length - 1] : item.h;
      const lowRaw = Array.isArray(item.l) ? item.l[item.l.length - 1] : item.l;
      const high = Number(highRaw || price);
      const low = Number(lowRaw || price);
      const volumeRaw = Array.isArray(item.v) ? item.v[item.v.length - 1] : item.v;
      const change = open ? ((price - open) / open) * 100 : 0;
      return { symbol, price, change, volume: volumeRaw * price, high, low, timestamp: Date.now() };
    }).filter((item) => item.price > 0);
    if (!normalized.length) return;
    latest = normalized;
    upstreamReady = true;
    const message = JSON.stringify({ type: 'ticker', data: normalized, source: 'Kraken REST', timestamp: Date.now() });
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(message);
    }
  } catch (error) {
    upstreamReady = false;
    console.error('Kraken REST snapshot failed:', error.message);
  }
}

async function fetchIndiaIndices() {
  try {
    const scanner = await postJson('https://scanner.tradingview.com/india/scan', {
      symbols: { tickers: ['NSE:NIFTY', 'BSE:SENSEX', 'NSE:BANKNIFTY'], query: { types: [] } },
      columns: ['close', 'change', 'change_abs', 'high', 'low', 'description', 'name', 'update_mode']
    });
    const specs = [
      { symbol: '^NSEI', name: 'NIFTY 50', shortName: 'NIFTY' },
      { symbol: '^BSESN', name: 'SENSEX', shortName: 'SENSEX' },
      { symbol: '^NSEBANK', name: 'NIFTY BANK', shortName: 'BANKNIFTY' }
    ];
    const indices = (scanner.data || []).map((item, index) => {
      const spec = specs[index];
      const price = Number(item.d[0]);
      const change = Number(item.d[1]);
      return { ...spec, price, previousClose: change ? price / (1 + change / 100) : price, change, high: Number(item.d[3]), low: Number(item.d[4]), volume: 0, timestamp: Date.now() };
    }).filter((item) => item.price > 0);
    if (!indices.length) throw new Error('TradingView returned no indices');
    latest = indices;
  } catch (scannerError) {
    try {
      const specs = [
        { symbol: '^NSEI', name: 'NIFTY 50', shortName: 'NIFTY' },
        { symbol: '^BSESN', name: 'SENSEX', shortName: 'SENSEX' },
        { symbol: '^NSEBANK', name: 'NIFTY BANK', shortName: 'BANKNIFTY' }
      ];
      const indices = [];
      for (const spec of specs) {
        const payload = await getYahooJson(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(spec.symbol)}?range=1d&interval=1m`);
        const result = payload.chart?.result?.[0];
        if (!result) continue;
        const quote = result.indicators?.quote?.[0] || {};
        const closes = (quote.close || []).filter(Number.isFinite);
        const meta = result.meta || {};
        const price = Number(meta.regularMarketPrice || closes.at(-1) || 0);
        const previousClose = Number(meta.chartPreviousClose || meta.previousClose || closes[0] || price);
        indices.push({ ...spec, price, previousClose, change: previousClose ? ((price - previousClose) / previousClose) * 100 : 0, high: Number(meta.regularMarketDayHigh || Math.max(...closes)), low: Number(meta.regularMarketDayLow || Math.min(...closes)), volume: 0, timestamp: Date.now() });
      }
      if (!indices.length) throw new Error('Yahoo returned no indices');
      latest = indices;
    } catch (fallbackError) {
      console.error('India index sources failed:', scannerError.message, fallbackError.message);
      if (!latest.length) return;
    }
  }
  if (!latest.length) return;
  upstreamReady = true;
  const message = JSON.stringify({ type: 'indices', data: latest, source: 'Live market data', timestamp: Date.now() });
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(message);
  }
}

app.get('/api/indices', async (_req, res) => {
  await fetchIndiaIndices();
  res.json({ source: 'TradingView / NSE & BSE', timestamp: Date.now(), data: latest });
});

app.get('/api/history/:symbol', async (req, res) => {
  const allowed = ['^NSEI', '^BSESN', '^NSEBANK'];
  const rangeMap = { today: ['1d', '1m'], '30d': ['1mo', '30m'], '1y': ['1y', '1d'] };
  const symbol = req.params.symbol;
  const range = rangeMap[req.query.range] ? req.query.range : 'today';
  if (!allowed.includes(symbol)) return res.status(400).json({ error: 'Unsupported index symbol' });
  const [period, interval] = rangeMap[range];
  try {
    const payload = await getYahooJson(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${period}&interval=${interval}`);
    const result = payload.chart?.result?.[0];
    if (!result) throw new Error('No chart result');
    const quote = result.indicators?.quote?.[0] || {};
    const points = (result.timestamp || []).map((time, index) => ({
      time: time * 1000,
      open: quote.open?.[index] ?? null,
      high: quote.high?.[index] ?? null,
      low: quote.low?.[index] ?? null,
      close: quote.close?.[index] ?? null,
      volume: quote.volume?.[index] ?? null
    })).filter((point) => Number.isFinite(point.close));
    res.json({ symbol, range, source: 'Yahoo Finance', timestamp: Date.now(), points });
  } catch (error) {
    res.status(502).json({ error: 'Unable to retrieve chart history', detail: error.message });
  }
});

wss.on('connection', (socket) => {
  clients.add(socket);
  socket.send(JSON.stringify({ type: 'connected', source: 'TradingView / NSE & BSE', timestamp: Date.now() }));
  if (latest.length) socket.send(JSON.stringify({ type: 'indices', data: latest, source: 'TradingView / NSE & BSE', timestamp: Date.now() }));
  socket.on('close', () => clients.delete(socket));
  socket.on('error', (error) => console.error('Client socket error:', error.message));
});

const port = Number(process.env.PORT || 8080);
server.listen(port, () => {
  console.log(`TradingZolo India market relay listening on http://localhost:${port}`);
  fetchIndiaIndices();
  restTimer = setInterval(fetchIndiaIndices, 15000);
});

const distDir = path.join(__dirname, 'dist', 'trading-zolo', 'browser');
app.use(express.static(distDir));
app.get(/.*/, (req, res, next) => {
  if (req.path.startsWith('/api/') || req.path === '/health' || req.path === '/stream') return next();
  res.sendFile(path.join(distDir, 'index.html'));
});

async function getYahooJson(url) {
  try {
    return await getJson(url);
  } catch (primaryError) {
    if (!url.includes('query1.finance.yahoo.com')) throw primaryError;
    const fallbackUrl = url.replace('query1.finance.yahoo.com', 'query2.finance.yahoo.com');
    try {
      return await getJson(fallbackUrl);
    } catch (fallbackError) {
      throw new Error(`${primaryError.message}; fallback: ${fallbackError.message}`);
    }
  }
}
