import { indexMetricSource, type MetricSourceRow } from './index-source-policy.ts';
import { indexForwardAnchors } from './index-forward-policy.ts';
import { isTradingDate } from './market-calendar.ts';

export const WSJ_URL = 'https://www.wsj.com/market-data/stocks/peyields';
export const WSJ_INDEX_PRICES: Record<string, string> = { sp500: '^GSPC', nasdaq100: '^NDX', dow30: '^DJI', russell2000: '^RUT' };
type Metric = 'pe_ttm' | 'pe_forward';
export interface WsjSnapshot extends MetricSourceRow { date: string; source?: string; pe_ttm?: number | null; pe_forward?: number | null }
export interface IndexClose { date: string; close: number; source: string }
export interface WsjObservation { source: 'wsj-latest'; date: string; value: number; url: string }
const positive = (x: unknown): x is number => typeof x === 'number' && Number.isFinite(x) && x > 0;

export function wsjMetricAnchors(snapshots: WsjSnapshot[], metric: Metric, id: string) {
  const rows = snapshots.filter(row => !(id === 'nasdaq100' && metric === 'pe_ttm' && row.date <= '2026-06-30')).filter(row => indexMetricSource(row, metric) === 'wsj-latest' && positive(row[metric])).map(row => ({...row, source: 'wsj-latest'})).sort((a,b) => a.date.localeCompare(b.date));
  for (const row of rows) if (!isTradingDate(row.date)) throw new Error(`Invalid WSJ observation date: ${id}/${row.date}`);
  // Dow's daily TTM date does not make its repeated weekly forward quote new.
  if (metric === 'pe_forward' && id === 'dow30' && rows.length) {
    const seed = { date: rows[0].date, value: rows[0][metric]!, source: 'wsj-latest' };
    return indexForwardAnchors(rows, { seed, observations: [], closes: [], startDate: seed.date, priceSymbol: '^DJI' }, true);
  }
  return rows.map(row => ({ date: row.date, value: row[metric]!, source: 'wsj-latest' }));
}

// One publication rule for both multiples on every WSJ-covered index. Do not
// substitute ETF prices or fund-provider earnings definitions between quotes.
export function applyWsjIndexPolicy<T extends { date: string; [key: string]: any }>(points: T[], id: string, snapshots: WsjSnapshot[], closes: IndexClose[]): T[] {
  const priceSymbol = WSJ_INDEX_PRICES[id];
  if (!priceSymbol) return points;
  const prices = new Map(closes.filter(row => positive(row.close) && row.source && isTradingDate(row.date)).map(row => [row.date, row]));
  const anchors = { pe_ttm: wsjMetricAnchors(snapshots, 'pe_ttm', id), pe_forward: wsjMetricAnchors(snapshots, 'pe_forward', id) };
  return points.map(point => {
    const next = { ...point };
    for (const metric of ['pe_ttm', 'pe_forward'] as const) {
      const anchor = anchors[metric].findLast(row => row.date <= point.date);
      if (!anchor) continue; // The audited historical series predates WSJ coverage.
      delete next[`${metric}_estimate`];
      delete next[`${metric}_snapshot`];
      delete next[`${metric}_observation`];
      if (anchor.date === point.date) {
        Object.assign(next, { [metric]: anchor.value, [`${metric}_observation`]: { source: 'wsj-latest', date: anchor.date, value: anchor.value, url: WSJ_URL } });
        continue;
      }
      const base = prices.get(anchor.date), current = prices.get(point.date);
      if (!base || !current) throw new Error(`Missing ${priceSymbol} WSJ close: ${anchor.date} -> ${point.date}`);
      Object.assign(next, { [metric]: Number((anchor.value * current.close / base.close).toFixed(4)), [`${metric}_estimate`]: {
        method: 'wsj-index-price-carry', anchorDate: anchor.date, anchorPe: anchor.value,
        anchorClose: base.close, close: current.close, anchorPriceSource: base.source, priceSource: current.source,
        priceSymbol, anchorSource: 'wsj-latest',
      } });
    }
    return next;
  });
}
