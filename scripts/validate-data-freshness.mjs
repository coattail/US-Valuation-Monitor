import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { lastCompletedSession, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';

export function findMissingSessions(series, target, since = '2026-09-18') {
  const present = new Set(series.points.map(p => p.date));
  const first = series.points[0]?.date || since;
  return tradingDates(first > since ? first : since, target).filter(date => !present.has(date));
}
export async function validateFreshness(kind, target = lastCompletedSession()) {
  if (!['index', 'company'].includes(kind)) throw new Error('Expected index or company');
  const dir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../data/standardized', `${kind}-series`);
  const files = (await readdir(dir)).filter(f => f.endsWith('.json'));
  const expectedCount = kind === 'index' ? 21 : 100;
  if (files.length !== expectedCount) throw new Error(`${kind}: expected ${expectedCount} series, found ${files.length}`);
  const problems = [];
  for (const file of files) {
    const series = JSON.parse(await readFile(path.join(dir, file), 'utf8'));
    const missing = findMissingSessions(series, target);
    if (missing.length) problems.push(`${series.symbol}: ${missing.join(', ')}`);
  }
  if (problems.length) throw new Error(`Incomplete ${kind} data through ${target}:\n${problems.join('\n')}`);
  console.log(`[freshness] ${kind}: all ${files.length} series complete through ${target}`);
}
if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  validateFreshness(process.argv[2]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
