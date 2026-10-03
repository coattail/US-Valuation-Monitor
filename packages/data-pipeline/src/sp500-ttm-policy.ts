import type { TtmPeEstimate } from '../../core/src/types.ts';

export const SP500_TTM_WSJ_START = '2026-05-08';
export const SP500_CLOSE_START = '2026-03-20';
export interface Sp500Close { date: string; close: number; source: string }
interface Point { date: string; pe_ttm?: unknown; pe_ttm_estimate?: TtmPeEstimate }
interface Observation extends Point { source?: string }
const positive = (x: unknown): x is number => typeof x === 'number' && Number.isFinite(x) && x > 0;

// Preserve dated observations, then continue on the same earnings denominator.
// Never reinstate the old provider's estimated multiple the next trading day.
export function applySp500TtmPricePolicy<T extends Point>(points: T[], snapshots: Observation[], closes: Sp500Close[]): T[] {
  const anchors = new Map<string, Observation>();
  for (const row of snapshots) {
    if (row.source === 'wsj-latest' && row.date >= SP500_TTM_WSJ_START && positive(row.pe_ttm)) anchors.set(row.date, row);
  }
  const observed = [...anchors.values()].sort((a,b)=>a.date.localeCompare(b.date));
  const prices = new Map(closes.filter(row=>positive(row.close)).map(row=>[row.date,row]));
  // The locked March 20 quote was followed by three old-basis estimates.
  // Retain that published quote; do not infer any replacement observation.
  const marchAnchor = points.find(row=>row.date === SP500_CLOSE_START && positive(row.pe_ttm));
  return points.map(point=>{
    const legacyRepair = point.date > SP500_CLOSE_START && point.date <= '2026-03-25';
    if (!legacyRepair && point.date < SP500_TTM_WSJ_START) return point;
    const anchor = legacyRepair ? marchAnchor : observed.findLast(row=>row.date<=point.date);
    const {pe_ttm_estimate: oldEstimate,...rest}=point;
    if (anchor?.date === point.date) return {...rest,pe_ttm:anchor.pe_ttm} as T;
    const base=anchor && prices.get(anchor.date), current=prices.get(point.date);
    if (!anchor || !base || !current) return {...rest,pe_ttm:null} as T;
    const estimate: TtmPeEstimate={method:'wsj-ttm-spx-price-carry',anchorDate:anchor.date,anchorPe:Number(anchor.pe_ttm),
      anchorClose:base.close,close:current.close,anchorPriceSource:base.source,priceSource:current.source};
    return {...rest,pe_ttm:Number((estimate.anchorPe*current.close/base.close).toFixed(4)),pe_ttm_estimate:estimate} as T;
  });
}

export function sp500TtmPriceCorrections(points: Point[], snapshots: Observation[], closes: Sp500Close[]) {
  return new Map(applySp500TtmPricePolicy(points,snapshots,closes)
    .filter(row=>row.pe_ttm_estimate || (row.date>=SP500_TTM_WSJ_START && positive(row.pe_ttm)))
    .map(row=>[row.date,{pe_ttm:row.pe_ttm as number}]));
}

export function assertSp500TtmCoverage(points: Point[]) {
  const missing=points.filter(row=>(row.date>=SP500_TTM_WSJ_START || (row.date>SP500_CLOSE_START && row.date<='2026-03-25')) && !positive(row.pe_ttm));
  if(missing.length) throw new Error(`Missing SPX close or dated TTM anchor: ${missing.map(row=>row.date).join(', ')}`);
}
