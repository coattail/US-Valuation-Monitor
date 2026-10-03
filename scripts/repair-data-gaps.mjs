import { readFile, writeFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';
import { assertExistingPointsUnchanged, fetchGapCloses, repairMissingPoints, recoverWithBudget } from '../packages/data-pipeline/src/gap-repair.ts';
import { lastCompletedSession, shiftDate, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { refreshNasdaqForwardCloses } from '../packages/data-pipeline/src/nasdaq-forward-closes.ts';
import { applyNasdaqForwardPricePolicy, assertNasdaqForwardCoverage } from '../packages/data-pipeline/src/nasdaq-forward-policy.ts';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dir = path.join(root, 'data/standardized');
const kind = process.argv[2];
if (!['index', 'company'].includes(kind)) throw new Error('Usage: node scripts/repair-data-gaps.mjs index|company');
const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
const write = async (name, value) => writeFile(path.join(dir, name), JSON.stringify(value, null, 2) + '\n');
const historyFile = kind === 'index' ? 'valuation-history.json' : 'company-valuation-history.json';
let dataset;
try { dataset = await read(historyFile); } catch (error) {
  if (kind !== 'company' || error.code !== 'ENOENT') throw error;
  const snapshot = await read('company-valuation-snapshot.json');
  dataset = { ...snapshot, indices: await Promise.all(snapshot.indices.map(async row => { const series = await read('company-series/' + row.id + '.json'); return { ...series, id: series.id || series.indexId }; })) };
}
if (kind === 'index') assertDatasetMatchesIndexHistoryLock(dataset, await read('index-history-lock.json'));
let ledger;
try { ledger = await read(`${kind}-gap-repairs.json`); } catch (error) { if (error.code !== 'ENOENT') throw error; ledger = { version: 1, repairs: {} }; }
const metrics = await read(`${kind}-yahoo-daily-metrics.json`);
const target = lastCompletedSession();
const ndxCloses = kind === 'index' ? await refreshNasdaqForwardCloses(target) : [];
// The outage boundary is permanent, so missed dates never age out of validation.
const expected = tradingDates('2026-09-18', target);
const yields = new Map();
if (kind === 'index') {
  for (const item of dataset.indices) for (const point of item.points) if (Number.isFinite(point.us10y_yield)) yields.set(point.date, point.us10y_yield);
} else {
  for (const date of expected) yields.set(date, 0);
} // Existing company schema convention.
let added = 0;
const originals = new Map(dataset.indices.map(item => [item, structuredClone(item.points)]));
// Replay cached recoveries for every ticker before starting network work.
for (const item of dataset.indices) {
  const known = new Map(item.points.map(p => [p.date, p]));
  for (const repair of ledger.repairs[item.symbol] || []) if (!known.has(repair.date) && repair.date <= target) known.set(repair.date, repair.point);
  item.points = [...known.values()].sort((a, b) => a.date.localeCompare(b.date));
}
const pending = dataset.indices.filter(item => {
  const known = new Set(item.points.map(p => p.date));
  return expected.some(date => date >= item.points[0]?.date && !known.has(date));
});
const skipped = await recoverWithBudget(pending, async item => {
  const known = new Set(item.points.map(p => p.date));
  const missing = expected.filter(date => date >= item.points[0]?.date && !known.has(date));
  const from = shiftDate(missing[0], -14);
  const anchorDates = missing.map(date => [...item.points].reverse().find(p => p.date < date)?.date).filter(Boolean);
  const closes = await fetchGapCloses(item.symbol, kind === 'index' ? 'etf' : 'stocks', from, target, [...missing, ...anchorDates]);
  const result = repairMissingPoints(item.points, missing, closes, yields, metrics.symbols[item.symbol] || []);
  item.points = result.points;
  if (result.repairs.length) {
    ledger.repairs[item.symbol] = [...(ledger.repairs[item.symbol] || []), ...result.repairs];
    console.log(`[gap] ${item.symbol}: recovered ${result.repairs.map(p => p.date).join(', ')}`);
  }
});
if (skipped.length) console.warn(`[gap] recovery budget exhausted; not attempted: ${skipped.map(item => item.symbol).join(', ')}`);
for (const item of dataset.indices) {
  const original = originals.get(item);
  if (kind === 'index' && item.id === 'nasdaq100') {
    item.points = applyNasdaqForwardPricePolicy(item.points, metrics.symbols.QQQ || [], ndxCloses);
    assertNasdaqForwardCoverage(item.points);
    for (const repair of ledger.repairs.QQQ || []) {
      [repair.point] = applyNasdaqForwardPricePolicy([repair.point], metrics.symbols.QQQ || [], ndxCloses);
    }
  }
  assertExistingPointsUnchanged(original, item.points);
  added += item.points.length - original.length;
}
if (added) {
  await write(historyFile, dataset);
  // Only after the old lock and every previously published row are verified.
  if (kind === 'index') await write('index-history-lock.json', buildIndexHistoryLock(dataset));
  await write(`${kind}-gap-repairs.json`, ledger);
}
console.log(`[gap] ${kind}: inserted/replayed ${added} records; existing records preserved`);
