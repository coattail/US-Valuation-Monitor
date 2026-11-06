import type { CloseObservation } from './gap-repair.ts';
import type { SplitEvent } from './split-events.ts';

// Explicit corporate actions only: never infer a split from a large market move.
const splits: Record<string, SplitEvent[]> = {
  SOXX: [{ symbol: 'SOXX', date: '2026-11-05', ratio: 3,
    source: 'https://www.sec.gov/Archives/edgar/data/1100663/000119312526430480/d156277d497.htm' }],
};

export class UnverifiedPriceBasisError extends Error {}

export function splitEventsFor(symbol: string, rows: CloseObservation[], extra: SplitEvent[] = []): SplitEvent[] {
  const events = new Map<string, SplitEvent>();
  for (const event of [...(splits[symbol] || []), ...rows.flatMap(row => [...(row.splitEvents || []), ...(row.basisAdjustment?.splitEvents || [])]), ...extra]) {
    if (event.symbol !== symbol || !/^\d{4}-\d{2}-\d{2}$/.test(event.date) || !Number.isFinite(event.ratio) || event.ratio <= 0 || !event.source) throw new Error(`Invalid ${symbol} split evidence`);
    const previous = events.get(event.date);
    if (previous && previous.ratio !== event.ratio) throw new Error(`Conflicting ${symbol} split events: ${event.date}`);
    if (!previous) events.set(event.date, event);
  }
  return [...events.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function coherentCloseWindow(rows: CloseObservation[], events: SplitEvent[] = []): boolean {
  const sorted = [...rows].sort((a, b) => a.date.localeCompare(b.date));
  return sorted.every((row, i) => {
    const previous = sorted[i - 1];
    if (!previous || Date.parse(row.date) - Date.parse(previous.date) > 14 * 86400000) return true;
    const ratio = row.close / previous.close;
    const splitRatio = events.filter(event => event.date > previous.date && event.date <= row.date)
      .reduce((factor, event) => factor * event.ratio, 1);
    // A confirmed split can hide inside the normal 0.5–2 guard (e.g. 2:1).
    // Reject a window resembling unadjusted prices; never guess new prices.
    const ambiguousSplit = splitRatio !== 1 && Math.abs(Math.log(ratio * splitRatio)) + 1e-5 < Math.abs(Math.log(ratio));
    return ratio >= 0.5 && ratio <= 2 && !ambiguousSplit;
  });
}

// A vendor can rebase its entire history after a split while our retained
// cache stays on its original share basis. Query an overlapping session and
// convert only the incoming batch; keep every cached observation unchanged.
export function alignClosePriceBasis(symbol: string, cached: CloseObservation[], incoming: CloseObservation[], target: string, extraEvents: SplitEvent[] = []): CloseObservation[] {
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
  const events = splitEventsFor(symbol, [...cached, ...incoming], extraEvents).filter(event => event.date <= target);
  let evidence: SplitEvent[] | undefined, multiplier = 1;
  for (let start = 0; start < events.length && !evidence; start++) {
    let product = 1;
    for (let end = start; end < events.length; end++) {
      product *= events[end].ratio;
      if (near(factor, product) || near(factor, 1 / product)) {
        evidence = events.slice(start, end + 1);
        multiplier = near(factor, product) ? product : 1 / product;
        break;
      }
    }
  }
  if (!evidence) throw new UnverifiedPriceBasisError(`Unverified ${symbol} price-basis change: ${factor}`);
  const split = evidence.at(-1)!;
  return incoming.map(row => ({ ...row, close: row.close * multiplier,
    basisAdjustment: { originalClose: row.close, multiplier, referenceDate: reference.date,
      splitDate: split.date, source: split.source, splitEvents: evidence },
  }));
}
