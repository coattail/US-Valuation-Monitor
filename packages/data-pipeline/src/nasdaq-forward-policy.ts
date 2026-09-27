import type { ForwardPeEstimate } from '../../core/src/types.ts';

export const NASDAQ_FORWARD_WSJ_START = '2026-04-10';
interface ForwardPoint { date: string; pe_forward?: unknown; pe_forward_estimate?: ForwardPeEstimate }
interface ForwardObservation extends ForwardPoint { source?: string }
export interface NasdaqClose { date: string; close: number; source: string }
const positive = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0;

// Use only the latest quote already dated on/before the session. Between WSJ
// observations, hold its implied forward earnings constant and follow NDX's
// unadjusted close. Never blend another provider or look ahead to a future quote.
export function applyNasdaqForwardPricePolicy<T extends ForwardPoint>(
  points: T[], snapshots: ForwardObservation[], closes: NasdaqClose[] = []
): T[] {
  const observed = new Map<string, ForwardObservation>();
  for (const row of snapshots) {
    if (row.source === 'wsj-latest' && row.date >= NASDAQ_FORWARD_WSJ_START && positive(row.pe_forward)) observed.set(row.date, row);
  }
  const anchors = [...observed.values()].sort((a, b) => a.date.localeCompare(b.date));
  const prices = new Map(closes.filter(row => positive(row.close)).map(row => [row.date, row]));
  return points.map(point => {
    if (point.date < NASDAQ_FORWARD_WSJ_START) return point;
    const { pe_forward_estimate: oldEstimate, ...rest } = point;
    const anchor = anchors.findLast(row => row.date <= point.date);
    if (anchor?.date === point.date) return { ...rest, pe_forward: anchor.pe_forward } as T;
    const base = anchor && prices.get(anchor.date), current = prices.get(point.date);
    if (!anchor || !base || !current) return { ...rest, pe_forward: null } as T;
    const value = Number((Number(anchor.pe_forward) * current.close / base.close).toFixed(4));
    const estimate: ForwardPeEstimate = {
      method: 'wsj-forward-ndx-price-carry', anchorDate: anchor.date,
      anchorPe: Number(anchor.pe_forward), anchorClose: base.close, close: current.close,
      anchorPriceSource: base.source, priceSource: current.source,
    };
    return { ...rest, pe_forward: value, pe_forward_estimate: estimate } as T;
  });
}

export function nasdaqForwardPriceCorrections(snapshots: ForwardObservation[], closes: NasdaqClose[]) {
  const dates = [...new Set([...closes.map(row => row.date), ...snapshots.map(row => row.date)])].sort();
  const points = applyNasdaqForwardPricePolicy(dates.map(date => ({ date, pe_forward: null as number | null })), snapshots, closes);
  return new Map(points.filter(row => positive(row.pe_forward)).map(row => [row.date, { pe_forward: Number(row.pe_forward) }]));
}

export function assertNasdaqForwardCoverage(points: ForwardPoint[]) {
  const missing = points.filter(row => row.date >= NASDAQ_FORWARD_WSJ_START && !positive(row.pe_forward));
  if (missing.length) throw new Error(`Missing NDX close or WSJ anchor for Forward PE: ${missing.map(row => row.date).join(', ')}`);
}
