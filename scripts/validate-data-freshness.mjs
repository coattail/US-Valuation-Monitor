import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { lastCompletedSession, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { applyEtfTtmPricePolicy, ETF_TTM_SYMBOLS } from '../packages/data-pipeline/src/etf-ttm-policy.ts';
import { reconstructDramTtm } from '../packages/data-pipeline/src/dram-ttm-reconstruction.ts';
import { forwardPlan, applyIndexForwardPolicy } from '../packages/data-pipeline/src/index-forward-policy.ts';
import {applySp500PbPolicy} from '../packages/data-pipeline/src/sp500-pb-policy.ts';

export function findMissingSessions(series, target, since = '2026-09-18') {
  const present = new Set(series.points.map(p => p.date));
  const first = series.points[0]?.date || since;
  return tradingDates(first > since ? first : since, target).filter(date => !present.has(date));
}
export async function validateFreshness(kind, target = lastCompletedSession(), {
  dataDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../data/standardized'),
} = {}) {
  if (!['index', 'company'].includes(kind)) throw new Error('Expected index or company');
  const dir = path.join(dataDir, `${kind}-series`);
  const files = (await readdir(dir)).filter(f => f.endsWith('.json'));
  const expectedCount = kind === 'index' ? 21 : 100;
  if (files.length !== expectedCount) throw new Error(`${kind}: expected ${expectedCount} series, found ${files.length}`);
  const problems = [];
  let etfCloses, metrics, forwardInputs, pbInputs;
  if (kind === 'index') {
    etfCloses = JSON.parse(await readFile(path.join(dir, '../etf-ttm-closes.json'), 'utf8'));
    metrics = JSON.parse(await readFile(path.join(dir, '../index-yahoo-daily-metrics.json'), 'utf8'));
    try { forwardInputs = JSON.parse(await readFile(path.join(dir,'../index-forward-inputs.json'),'utf8')); }
    catch(error) { if(error.code !== 'ENOENT') throw error; }
  }
  for (const file of files) {
    const series = JSON.parse(await readFile(path.join(dir, file), 'utf8'));
    const missing = findMissingSessions(series, target);
    if (missing.length) problems.push(`${series.symbol}: ${missing.join(', ')}`);
    if(kind === 'index' && series.id === 'sp500') {
      try {
        pbInputs ??= JSON.parse(await readFile(path.join(dataDir,'sp500-pb-inputs.json'),'utf8'));
        const expected = applySp500PbPolicy(series.points,pbInputs);
        const stale = series.points.filter((point,i) => point.pb !== expected[i].pb || JSON.stringify(point.pb_basis) !== JSON.stringify(expected[i].pb_basis));
        if(stale.length) problems.push(`SPY: inconsistent PB on ${stale.map(row => row.date).join(', ')}`);
      } catch(error) {problems.push(`SPY PB: ${error.message}`);}
    }
    if (kind === 'index' && ETF_TTM_SYMBOLS.has(series.symbol)) {
      try {
        let expected = applyEtfTtmPricePolicy(series.points, metrics.symbols[series.symbol] || [], etfCloses.symbols[series.symbol] || []);
        if(series.id === 'dram' && series.points.at(-1)?.date >= '2026-10-23') expected = reconstructDramTtm(expected,metrics.symbols.DRAM || [],etfCloses.symbols.DRAM || []);
        const stale = series.points.filter((point, i) => point.pe_ttm !== expected[i].pe_ttm ||
          JSON.stringify(point.pe_ttm_estimate) !== JSON.stringify(expected[i].pe_ttm_estimate) ||
          JSON.stringify(point.pe_ttm_snapshot) !== JSON.stringify(expected[i].pe_ttm_snapshot));
        if (stale.length) problems.push(`${series.symbol}: stale TTM values/provenance on ${stale.map(row => row.date).join(', ')}`);
      } catch (error) { problems.push(`${series.symbol}: ${error.message}`); }
    }
    if(kind === 'index' && forwardPlan(series)) {
      try {
        const inputs = forwardInputs?.indices?.[series.id];
        if(!inputs) throw new Error('missing forward reconciliation inputs');
        const expected = applyIndexForwardPolicy(series.points,metrics.symbols[series.symbol] || [],inputs,forwardPlan(series).weekly);
        const stale = series.points.filter((point,i)=>point.pe_forward !== expected[i].pe_forward ||
          JSON.stringify(point.pe_forward_estimate) !== JSON.stringify(expected[i].pe_forward_estimate) ||
          JSON.stringify(point.pe_forward_snapshot) !== JSON.stringify(expected[i].pe_forward_snapshot));
        if(stale.length) problems.push(`${series.symbol}: stale Forward PE on ${stale.map(row=>row.date).join(', ')}`);
      } catch(error) { problems.push(`${series.symbol}: ${error.message}`); }
    }
  }
  if (problems.length) throw new Error(`Incomplete ${kind} data through ${target}:\n${problems.join('\n')}`);
  console.log(`[freshness] ${kind}: all ${files.length} series complete through ${target}`);
}
if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  validateFreshness(process.argv[2]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
