import { readFile, writeFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';
import { assertExistingPointsUnchanged, fetchGapCloses, repairMissingPoints } from '../packages/data-pipeline/src/gap-repair.ts';
import { lastCompletedSession, shiftDate, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { applyNasdaqForwardObservationPolicy } from '../packages/data-pipeline/src/nasdaq-forward-policy.ts';

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
// The outage boundary is permanent, so missed dates never age out of validation.
const expected = tradingDates('2026-09-18', target);
const yields = new Map();
if (kind === 'index') {
  for (const item of dataset.indices) for (const point of item.points) if (Number.isFinite(point.us10y_yield)) yields.set(point.date, point.us10y_yield);
} else {
  for (const date of expected) yields.set(date, 0);
} // Existing company schema convention.
let added = 0;
const deadline = Date.now() + 120000; // Leave time to publish progress even in a widespread outage.
for (const item of dataset.indices) {
  const original = structuredClone(item.points);
  const known = new Map(item.points.map(p => [p.date, p]));
  // Replay proven recoveries if a primary source omits the same date again.
  for (const repair of ledger.repairs[item.symbol] || []) if (!known.has(repair.date) && repair.date <= target) known.set(repair.date, repair.point);
  item.points = [...known.values()].sort((a, b) => a.date.localeCompare(b.date));
  const missing = expected.filter(date => date >= item.points[0]?.date && !known.has(date));
  if (missing.length && Date.now() < deadline) {
    const from = shiftDate(missing[0], -14);
    const anchorDates = missing.map(date => [...item.points].reverse().find(p => p.date < date)?.date).filter(Boolean);
    const closes = await fetchGapCloses(item.symbol, kind === 'index' ? 'etf' : 'stocks', from, target, [...missing, ...anchorDates]);
    const result = repairMissingPoints(item.points, missing, closes, yields, metrics.symbols[item.symbol] || []);
    item.points = result.points;
    if (result.repairs.length) {
      ledger.repairs[item.symbol] = [...(ledger.repairs[item.symbol] || []), ...result.repairs];
      console.log(`[gap] ${item.symbol}: recovered ${result.repairs.map(p => p.date).join(', ')}`);
    }
  }
  if (kind === 'index' && item.id === 'nasdaq100') {
    item.points = applyNasdaqForwardObservationPolicy(item.points, metrics.symbols.QQQ || []);
    for (const repair of ledger.repairs.QQQ || []) {
      [repair.point] = applyNasdaqForwardObservationPolicy([repair.point], metrics.symbols.QQQ || []);
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
