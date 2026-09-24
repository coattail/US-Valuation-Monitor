import { createTextFetcher } from './fetch-text.ts';
import { appendRecentCloses } from './recent-close.ts';

export interface CloseObservation { date: string; close: number; source: string }
export interface GapPoint { date: string; [key: string]: unknown }
export interface GapRepair { date: string; anchorDate: string; anchorClose: CloseObservation; close: CloseObservation; method: string; point: GapPoint }
const fetchText = createTextFetcher({ userAgent: 'Mozilla/5.0', requestBudgetMs: 10000 });
const validClose = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0;

export function parseYahooCloses(raw: string, source: string): CloseObservation[] {
  const result = JSON.parse(raw)?.chart?.result?.[0];
  return (result?.timestamp || []).flatMap((ts: number, i: number) => {
    const close = result?.indicators?.quote?.[0]?.close?.[i];
    return Number.isFinite(ts) && validClose(close) ? [{ date: new Date(ts * 1000).toISOString().slice(0, 10), close, source }] : [];
  });
}
export function parseChartExchangeCloses(html: string, source: string): CloseObservation[] {
  return [...html.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)].flatMap(match => {
    const cells = [...match[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(m => m[1].replace(/<[^>]+>/g, '').trim());
    const date = cells[0];
    const close = Number(cells[4]?.replace(/,/g, ''));
    return /^\d{4}-\d{2}-\d{2}$/.test(date) && validClose(close) ? [{ date, close, source }] : [];
  });
}
export async function fetchGapCloses(symbol: string, assetClass: 'stocks' | 'etf', from: string, to: string, requiredDates: string[], request = (url: string) => fetchText(url, 1, 10000)): Promise<CloseObservation[]> {
  const observations = new Map<string, CloseObservation>();
  const nasdaq = `https://api.nasdaq.com/api/quote/${encodeURIComponent(symbol.replace(/-/g, '.'))}/historical?assetclass=${assetClass}&fromdate=${from}&todate=${to}&limit=5000`;
  const yahoo = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol.replace(/\./g, '-'))}?period1=${Date.parse(from + 'T00:00:00Z') / 1000}&period2=${Date.parse(to + 'T00:00:00Z') / 1000 + 86400}&interval=1d`;
  const chart = `https://chartexchange.com/symbol/otc-${symbol.toLowerCase()}/historical/`;
  const sources = [
    { url: nasdaq, parse: (raw: string) => appendRecentCloses([], raw, to).map(p => ({ date: p.date, close: p.close, source: nasdaq })) },
    { url: yahoo, parse: (raw: string) => parseYahooCloses(raw, yahoo) },
    ...(symbol === 'TCEHY' ? [{ url: chart, parse: (raw: string) => parseChartExchangeCloses(raw, chart) }] : []),
  ];
  for (const { url, parse } of sources) {
    try {
      for (const point of parse(await request(url))) {
        if (point.date >= from && point.date <= to && !observations.has(point.date)) observations.set(point.date, point);
      }
      if (requiredDates.every(date => observations.has(date))) break;
    } catch (error) { console.warn(`[gap] ${symbol}: ${String(error)}`); }
  }
  return [...observations.values()].sort((a, b) => a.date.localeCompare(b.date));
}

// Short-window, price-based estimates, never represented as historical vendor
// observations. Existing values and unavailable metrics remain untouched.
export function repairMissingPoints(points: GapPoint[], expectedDates: string[], closes: CloseObservation[], yields: Map<string, number>, directMetrics: GapPoint[] = []): { points: GapPoint[]; repairs: GapRepair[] } {
  const byDate = new Map(points.map(p => [p.date, p]));
  const closeByDate = new Map(closes.map(p => [p.date, p]));
  const metricsByDate = new Map(directMetrics.map(p => [p.date, p]));
  const repairs: GapRepair[] = [];
  for (const date of expectedDates) {
    if (byDate.has(date) || date < points[0]?.date) continue;
    const anchor = [...points].reverse().find(p => p.date < date && closeByDate.has(p.date));
    const close = closeByDate.get(date), anchorClose = anchor && closeByDate.get(anchor.date);
    if (!anchor || !close || !anchorClose || !validClose(close.close) || !validClose(anchorClose.close)) continue;
    // Long source outages / split-scale mismatches require a fresh fundamental
    // anchor, not an extrapolation of an arbitrarily old valuation.
    if (Date.parse(date) - Date.parse(anchor.date) > 14 * 86400000) continue;
    const ratio = close.close / anchorClose.close;
    if (ratio < 0.5 || ratio > 2) continue;
    const point: GapPoint = { date };
    const direct = metricsByDate.get(date);
    for (const key of ['pe_ttm', 'pe_forward', 'pb', 'peg']) {
      if (!(key in anchor)) continue;
      point[key] = typeof direct?.[key] === 'number' && Number.isFinite(direct[key]) ? direct[key] :
        typeof anchor[key] === 'number' && Number.isFinite(anchor[key]) ? Number((Number(anchor[key]) * ratio).toFixed(6)) : null;
    }
    if ('us10y_yield' in anchor) {
      if (!yields.has(date)) continue;
      point.us10y_yield = yields.get(date);
    }
    const method = 'observed-close-ratio-with-previous-published-fundamentals';
    point.recovery = { method, anchorDate: anchor.date, priceSource: close.source, estimated: true };
    byDate.set(date, point);
    repairs.push({ date, anchorDate: anchor.date, anchorClose, close, method, point });
  }
  const result = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  assertExistingPointsUnchanged(points, result);
  return { points: result, repairs };
}
export function assertExistingPointsUnchanged(before: GapPoint[], after: GapPoint[]): void {
  const afterMap = new Map(after.map(p => [p.date, p]));
  if (afterMap.size !== after.length) throw new Error('Duplicate dates after gap repair');
  for (const point of before) if (JSON.stringify(point) !== JSON.stringify(afterMap.get(point.date))) throw new Error(`Gap repair altered an existing record: ${point.date}`);
}
