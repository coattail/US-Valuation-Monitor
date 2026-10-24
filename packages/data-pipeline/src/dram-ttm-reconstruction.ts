import type { CloseObservation } from './gap-repair.ts';
import type { RawValuationPoint } from '../../core/src/types.ts';

// Fixed retrospective reconstruction for the documented October 14 break.
// These are captured provider snapshots, not issuer-certified fund earnings.
// The original observation log is retained unchanged.
export const DRAM_TTM_RECONSTRUCTION = {
  left: {date:'2026-07-16',pe:32.38},
  right: {date:'2026-10-14',pe:23.95},
  check: {date:'2026-10-23',pe:22.48},
  source:'https://stockanalysis.com/etf/dram/',
} as const;

export function reconstructDramTtm<T extends RawValuationPoint>(points: T[], snapshots: Array<{date:string;pe_ttm?:number|null;source?:string}>, closes: CloseObservation[]): T[] {
  const config = DRAM_TTM_RECONSTRUCTION;
  if(!points.some(row=>row.date > config.left.date && row.date < config.right.date)) return points;
  const prices = new Map(closes.map(row=>[row.date,row]));
  const raw = new Map(snapshots.filter(row=>row.source === 'stockanalysis-latest').map(row=>[row.date,row]));
  for(const anchor of [config.left,config.right,config.check]) {
    if(raw.get(anchor.date)?.pe_ttm !== anchor.pe || !prices.get(anchor.date)) throw new Error(`Unverified DRAM reconstruction anchor: ${anchor.date}`);
  }
  const left = prices.get(config.left.date)!, right = prices.get(config.right.date)!, check = prices.get(config.check.date)!;
  const leftEarnings = left.close/config.left.pe, rightEarnings = right.close/config.right.pe;
  if(Math.abs((check.close/config.check.pe)/rightEarnings - 1) > .001) throw new Error('DRAM post-break TTM denominator is not corroborated');
  const span = Date.parse(config.right.date)-Date.parse(config.left.date);
  return points.map(point=>{
    if(point.date <= config.left.date || point.date >= config.right.date) return point;
    const current = prices.get(point.date);
    if(!current) throw new Error(`Missing DRAM reconstruction close: ${point.date}`);
    const elapsed = (Date.parse(point.date)-Date.parse(config.left.date))/span;
    const earnings = leftEarnings+(rightEarnings-leftEarnings)*elapsed;
    const {pe_ttm_estimate:oldEstimate,pe_ttm_snapshot:oldSnapshot,...rest} = point;
    return {...rest,pe_ttm:Number((current.close/earnings).toFixed(4)),pe_ttm_estimate:{
      method:'snapshot-implied-earnings-interpolation',anchorDate:config.left.date,anchorPe:config.left.pe,
      anchorClose:left.close,close:current.close,anchorPriceSource:left.source,priceSource:current.source,
      endAnchorDate:config.right.date,endAnchorPe:config.right.pe,endAnchorClose:right.close,
      earnings,usesFutureAnchor:true,valuationSource:config.source,
    }} as T;
  });
}
