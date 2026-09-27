import { readFile, writeFile } from 'node:fs/promises';
import { createTextFetcher } from './fetch-text.ts';
import { tradingDates } from './market-calendar.ts';
import { NASDAQ_FORWARD_WSJ_START, type NasdaqClose } from './nasdaq-forward-policy.ts';

const CACHE = new URL('../../../data/standardized/nasdaq100-forward-closes.json', import.meta.url);
export const FRED_NDX_CLOSE_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQ100';
const request = createTextFetcher({ userAgent: 'Mozilla/5.0', directFallback: true, requestBudgetMs: 15000 });
const valid = (row: NasdaqClose) => /^\d{4}-\d{2}-\d{2}$/.test(row.date) && typeof row.close === 'number' && Number.isFinite(row.close) && row.close > 0 && Boolean(row.source);

export async function loadNasdaqForwardCloses(): Promise<NasdaqClose[]> {
  try {
    const cache = JSON.parse(await readFile(CACHE, 'utf8'));
    if (cache.symbol !== '^NDX' || !Array.isArray(cache.observations) || !cache.observations.every(valid)) throw new Error('Invalid Nasdaq Forward PE close cache');
    return cache.observations;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
}

export function parseNasdaqForwardCloses(raw: string, source: string): NasdaqClose[] {
  if (source === FRED_NDX_CLOSE_URL) {
    const lines = raw.trim().split(/\r?\n/).map(line => line.split(','));
    const column = lines[0].indexOf('NASDAQ100');
    if (column < 1) throw new Error('Missing NASDAQ100 column');
    return lines.slice(1).map(cells => ({ date: cells[0], close: Number(cells[column]), source })).filter(valid);
  }
  const result = JSON.parse(raw)?.chart?.result?.[0];
  if (result?.meta?.symbol !== '^NDX') throw new Error('Expected ^NDX unadjusted index closes');
  return (result.timestamp || []).map((ts: number, i: number) => ({
    date: new Date(ts * 1000).toISOString().slice(0, 10),
    close: result.indicators?.quote?.[0]?.close?.[i], source,
  })).filter(valid);
}

export async function refreshNasdaqForwardCloses(endDate: string): Promise<NasdaqClose[]> {
  const cached = await loadNasdaqForwardCloses();
  const byDate = new Map(cached.map(row => [row.date, row]));
  const required = tradingDates(NASDAQ_FORWARD_WSJ_START, endDate);
  const complete = () => required.every(date => byDate.has(date));
  if (complete()) return cached;
  const start = Date.parse(`${NASDAQ_FORWARD_WSJ_START}T00:00:00Z`) / 1000;
  const end = Date.parse(`${endDate}T00:00:00Z`) / 1000 + 86400;
  const urls = [FRED_NDX_CLOSE_URL, ...['query1', 'query2'].map(host =>
    `https://${host}.finance.yahoo.com/v8/finance/chart/%5ENDX?period1=${start}&period2=${end}&interval=1d`)];
  for (const url of urls) {
    try {
      for (const row of parseNasdaqForwardCloses(await request(url), url)) {
        if (row.date >= NASDAQ_FORWARD_WSJ_START && row.date <= endDate && !byDate.has(row.date)) byDate.set(row.date, row);
      }
      if (complete()) break;
    } catch (error) { console.warn(`[NDX forward closes] ${String(error)}`); }
  }
  const observations = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  if (observations.length > cached.length) await writeFile(CACHE, JSON.stringify({ symbol: '^NDX', observations }, null, 2) + '\n');
  return observations; // The publishing caller checks coverage for its actual rows.
}
