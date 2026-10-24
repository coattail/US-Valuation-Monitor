import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { lastCompletedSession, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { applyEtfTtmPricePolicy, ETF_TTM_SYMBOLS } from '../packages/data-pipeline/src/etf-ttm-policy.ts';

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
  let etfCloses, metrics;
  if (kind === 'index') {
    etfCloses = JSON.parse(await readFile(path.join(dir, '../etf-ttm-closes.json'), 'utf8'));
    metrics = JSON.parse(await readFile(path.join(dir, '../index-yahoo-daily-metrics.json'), 'utf8'));
  }
  for (const file of files) {
    const series = JSON.parse(await readFile(path.join(dir, file), 'utf8'));
    const missing = findMissingSessions(series, target);
    if (missing.length) problems.push(`${series.symbol}: ${missing.join(', ')}`);
    if (kind === 'index' && ETF_TTM_SYMBOLS.has(series.symbol)) {
      try {
        const expected = applyEtfTtmPricePolicy(series.points, metrics.symbols[series.symbol] || [], etfCloses.symbols[series.symbol] || []);
        const stale = series.points.filter((point, i) => point.pe_ttm !== expected[i].pe_ttm ||
          JSON.stringify(point.pe_ttm_estimate) !== JSON.stringify(expected[i].pe_ttm_estimate) ||
          JSON.stringify(point.pe_ttm_snapshot) !== JSON.stringify(expected[i].pe_ttm_snapshot));
        if (stale.length) problems.push(`${series.symbol}: stale TTM values/provenance on ${stale.map(row => row.date).join(', ')}`);
      } catch (error) { problems.push(`${series.symbol}: ${error.message}`); }
    }
  }
  if (problems.length) throw new Error(`Incomplete ${kind} data through ${target}:\n${problems.join('\n')}`);
  console.log(`[freshness] ${kind}: all ${files.length} series complete through ${target}`);
}
if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  validateFreshness(process.argv[2]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
