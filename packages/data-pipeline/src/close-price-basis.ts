import type { CloseObservation } from './gap-repair.ts';

// Explicit corporate actions only: never infer a split from a large market move.
const splits: Record<string, { date: string; ratio: number; source: string }[]> = {
  SOXX: [{ date: '2026-11-05', ratio: 3,
    source: 'https://www.sec.gov/Archives/edgar/data/1100663/000119312526430480/d156277d497.htm' }],
};

export function coherentCloseWindow(rows: CloseObservation[]): boolean {
  const sorted = [...rows].sort((a, b) => a.date.localeCompare(b.date));
  return sorted.every((row, i) => {
    const previous = sorted[i - 1];
    if (!previous || Date.parse(row.date) - Date.parse(previous.date) > 14 * 86400000) return true;
    const ratio = row.close / previous.close;
    return ratio >= 0.5 && ratio <= 2;
  });
}

// A vendor can rebase its entire history after a split while our retained
// cache stays on its original share basis. Query an overlapping session and
// convert only the incoming batch; keep every cached observation unchanged.
export function alignClosePriceBasis(symbol: string, cached: CloseObservation[], incoming: CloseObservation[], target: string): CloseObservation[] {
  const known = new Map(cached.map(row => [row.date, row]));
  const overlaps = incoming.filter(row => known.has(row.date));
  if (!overlaps.length) throw new Error(`Missing ${symbol} overlapping close for price-basis verification`);
  const reference = overlaps.at(-1)!;
  const factor = known.get(reference.date)!.close / reference.close;
  const near = (a: number, b: number) => Math.abs(a / b - 1) < 1e-5;
  if (!overlaps.every(row => near(known.get(row.date)!.close / row.close, factor))) {
    throw new Error(`Inconsistent ${symbol} overlapping price bases`);
  }
  if (near(factor, 1)) return incoming;
  const split = splits[symbol]?.find(action => action.date <= target &&
    (near(factor, action.ratio) || near(factor, 1 / action.ratio)));
  if (!split) throw new Error(`Unverified ${symbol} price-basis change: ${factor}`);
  const multiplier = near(factor, split.ratio) ? split.ratio : 1 / split.ratio;
  return incoming.map(row => ({ ...row, close: row.close * multiplier,
    basisAdjustment: { originalClose: row.close, multiplier, referenceDate: reference.date,
      splitDate: split.date, source: split.source },
  }));
}
