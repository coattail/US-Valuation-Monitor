import type { TtmPeEstimate } from '../../core/src/types.ts';
import type { CloseObservation } from './gap-repair.ts';

export const ETF_TTM_START = '2026-07-17';
export const ETF_TTM_SYMBOLS = new Set(['DRAM', 'SMH', 'SOXX', 'IGV', 'IJH', 'VTI']);
const datedSources = new Set(['ishares-official-latest', 'ssga-official-latest', 'wsj-latest']);
const positive = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0;
export interface EtfSnapshot { date: string; pe_ttm?: number | null; source?: string }
interface Point { date: string; pe_ttm?: number | null; pe_ttm_estimate?: TtmPeEstimate; pe_ttm_snapshot?: { source: string; dateBasis: 'first-seen' } }

// Stock Analysis ETF pages do not date the fundamental multiple. A new scrape
// date is not a new valuation observation. Keep the FIRST sighting of each
// distinct value; dated fund-provider observations remain authoritative.
export function etfTtmAnchors(snapshots: EtfSnapshot[]): EtfSnapshot[] {
  let lastUndatedValue: number | undefined;
  return [...snapshots].sort((a, b) => a.date.localeCompare(b.date)).filter(row => {
    if (!positive(row.pe_ttm)) return false;
    if (datedSources.has(row.source || '')) return true;
    if (row.source !== 'stockanalysis-latest') return false;
    if (row.pe_ttm === lastUndatedValue) return false;
    lastUndatedValue = row.pe_ttm;
    return true;
  });
}

export function applyEtfTtmPricePolicy<T extends Point>(points: T[], snapshots: EtfSnapshot[], closes: CloseObservation[]): T[] {
  const anchors = etfTtmAnchors(snapshots);
  const prices = new Map(closes.filter(row => positive(row.close) && row.source).map(row => [row.date, row]));
  return points.map(point => {
    if (point.date < ETF_TTM_START) return point;
    const anchor = anchors.findLast(row => row.date <= point.date);
    if (!anchor) return point;
    const { pe_ttm_estimate: oldEstimate, pe_ttm_snapshot: oldSnapshot, ...rest } = point;
    if (anchor.date === point.date && anchor.source !== 'stockanalysis-latest') return { ...rest, pe_ttm: anchor.pe_ttm } as T;
    if (anchor.date === point.date) return {
      ...rest, pe_ttm: anchor.pe_ttm,
      pe_ttm_snapshot: { source: anchor.source, dateBasis: 'first-seen' },
    } as T;
    const base = prices.get(anchor.date), current = prices.get(point.date);
    // Missing prices must be visible, never silently filled flat.
    if (!base || !current) throw new Error(`Missing ETF TTM close: ${anchor.date} -> ${point.date}`);
    const estimate: TtmPeEstimate = {
      method: anchor.source === 'stockanalysis-latest' ? 'undated-ttm-etf-price-carry' : 'dated-ttm-etf-price-carry',
      anchorDate: anchor.date, anchorPe: Number(anchor.pe_ttm),
      anchorClose: base.close, close: current.close, anchorPriceSource: base.source, priceSource: current.source,
    };
    return { ...rest, pe_ttm: Number((estimate.anchorPe * current.close / base.close).toFixed(4)), pe_ttm_estimate: estimate } as T;
  });
}
