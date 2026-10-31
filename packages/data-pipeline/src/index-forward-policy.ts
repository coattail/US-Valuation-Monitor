import { indexMetricSource, sourcePriority, type MetricSourceRow } from './index-source-policy.ts';
import type { ForwardPeEstimate } from '../../core/src/types.ts';
import type { CloseObservation } from './gap-repair.ts';
import { isTradingDate, shiftDate } from './market-calendar.ts';
import { INDEX_MAP } from '../../core/src/constants.ts';

export interface ForwardAnchor { date: string; value: number; source: string; url?: string; capturedAt?: string }
export interface ForwardInputs { priceSymbol: string; startDate: string; seed: ForwardAnchor; closes: CloseObservation[]; observations: ForwardAnchor[] }
interface Point { date: string; pe_forward?: number | null; pe_forward_estimate?: ForwardPeEstimate; pe_forward_snapshot?: { source: string; dateBasis: 'first-seen' } }
interface Snapshot extends MetricSourceRow { date: string; pe_forward?: number | null; source?: string; capturedAt?: string }
const positive = (x: unknown): x is number => typeof x === 'number' && Number.isFinite(x) && x > 0;
const sources = new Set(['wsj-latest', 'ssga-official-latest', 'ishares-official-latest', 'stockanalysis-latest']);

export function forwardPlan(index: { id: string; symbol: string }): { startDate: string; priceSymbol: string; weekly: boolean } | undefined {
  if(!INDEX_MAP[index.id]) return;
  // Existing SPX/NDX policies are audited independently. DRAM has no observed
  // forward series: its existing explicitly estimated basis stays unchanged.
  if (['sp500', 'nasdaq100', 'dram'].includes(index.id)) return;
  if (index.id === 'dow30') return { startDate: '2026-04-16', priceSymbol: '^DJI', weekly: true };
  if (index.id === 'russell2000') return { startDate: '2026-04-01', priceSymbol: '^RUT', weekly: true };
  return { startDate: ['sp400', 'us_total_market', 'igv', 'soxx', 'smh'].includes(index.id) ? '2026-07-17' : '2026-04-16', priceSymbol: index.symbol, weekly: false };
}

function isWeeklyObservationDate(date: string): boolean {
  // Friday, or the final trading session before a Friday market holiday.
  let next = shiftDate(date, 1);
  while (!isTradingDate(next)) next = shiftDate(next, 1);
  return new Date(date + 'T12:00Z').getUTCDay() === 5 ||
    (new Date(date + 'T12:00Z').getUTCDay() === 4 && next > shiftDate(date, 3));
}

export function indexForwardAnchors(snapshots: Snapshot[], inputs: ForwardInputs, weekly: boolean): ForwardAnchor[] {
  const byDate = new Map<string, ForwardAnchor>([[inputs.seed.date, inputs.seed]]);
  let lastValue: number | undefined;
  for (const row of snapshots.map(row => ({...row, source:indexMetricSource(row,'pe_forward')})).sort((a, b) => a.date.localeCompare(b.date))) {
    if (!positive(row.pe_forward) || !sources.has(row.source || '')) continue;
    if (weekly && row.source !== 'wsj-latest') continue;
    if (row.source === 'stockanalysis-latest' || weekly) {
      const repeated = row.pe_forward === lastValue;
      lastValue = row.pe_forward;
      // WSJ explicitly publishes this FIELD weekly. A fresh date in its daily
      // TTM column cannot refresh the forward denominator. Equal Friday quotes
      // remain real new weekly observations; unexpected changed quotes are kept.
      if (repeated && !(weekly && isWeeklyObservationDate(row.date))) continue;
    }
    byDate.set(row.date, { date: row.date, value: row.pe_forward, source: row.source!, capturedAt:row.capturedAt });
  }
  for (const row of inputs.observations) if (positive(row.value) && !weekly) {
    const existing = byDate.get(row.date);
    if (!existing || sourcePriority(row.source) > sourcePriority(existing.source) ||
      (sourcePriority(row.source) === sourcePriority(existing.source) && (!existing.capturedAt ||
        String(row.capturedAt || '') >= existing.capturedAt))) byDate.set(row.date,row);
  }
  if (byDate.get(inputs.seed.date)?.source === 'previous-published-estimate' &&
    [...byDate.values()].some(row => row.source !== 'previous-published-estimate' && row.date <= inputs.seed.date)) byDate.delete(inputs.seed.date);
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function applyIndexForwardPolicy<T extends Point>(points: T[], snapshots: Snapshot[], inputs: ForwardInputs, weekly = false): T[] {
  const anchors = indexForwardAnchors(snapshots, inputs, weekly);
  const prices = new Map(inputs.closes.map(row => [row.date, row]));
  return points.map(point => {
    if (point.date < inputs.startDate) return point;
    const anchor = anchors.findLast(row => row.date <= point.date);
    if (!anchor) throw new Error(`Missing forward anchor: ${point.date}`);
    const { pe_forward_estimate: oldEstimate, pe_forward_snapshot: oldSnapshot, ...rest } = point;
    if (point.date === anchor.date && anchor.source !== 'previous-published-estimate') {
      return { ...rest, pe_forward: anchor.value,
        ...(anchor.source === 'stockanalysis-latest' ? { pe_forward_snapshot: {source:anchor.source,dateBasis:'first-seen'} } : {}),
      } as T;
    }
    const base = prices.get(anchor.date), current = prices.get(point.date);
    if (!base || !current || !positive(base.close) || !positive(current.close)) throw new Error(`Missing ${inputs.priceSymbol} forward close: ${anchor.date} -> ${point.date}`);
    const estimate: ForwardPeEstimate = {
      method: 'source-forward-price-carry', anchorDate: anchor.date, anchorPe: anchor.value,
      anchorClose: base.close, close: current.close, anchorPriceSource: base.source, priceSource: current.source,
      priceSymbol: inputs.priceSymbol, anchorSource: anchor.source,
    };
    return { ...rest, pe_forward: Number((anchor.value * current.close / base.close).toFixed(4)), pe_forward_estimate: estimate } as T;
  });
}

export function findFlatMetricRuns(points: Array<{date:string;[key:string]:unknown}>, since = '2026-03-27', minPoints = 5) {
  const recent = points.filter(row => row.date >= since);
  const runs: Array<{metric:string;from:string;to:string;count:number;value:number}> = [];
  for (const metric of ['pe_ttm', 'pe_forward', 'pb']) {
    let start = 0;
    for (let i = 1; i <= recent.length; i++) {
      if (i < recent.length && recent[i][metric] === recent[start][metric]) continue;
      if (i - start >= minPoints && positive(recent[start]?.[metric])) runs.push({ metric, from:recent[start].date, to:recent[i-1].date, count:i-start, value:Number(recent[start][metric]) });
      start = i;
    }
  }
  return runs;
}
