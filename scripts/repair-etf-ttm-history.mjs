import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { applyEtfTtmPricePolicy, etfTtmAnchors, ETF_TTM_START, ETF_TTM_SYMBOLS } from '../packages/data-pipeline/src/etf-ttm-policy.ts';
import { reconstructDramTtm } from '../packages/data-pipeline/src/dram-ttm-reconstruction.ts';
import { fetchGapCloses } from '../packages/data-pipeline/src/gap-repair.ts';
import { tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

const defaultDir = fileURLToPath(new URL('../data/standardized/', import.meta.url));
export async function repairEtfTtmHistory({ dir = defaultDir, write = false,
  fetchCloses = (...args) => fetchGapCloses(...args, undefined, { preferYahoo: true }),
} = {}) {
  const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
  const save = async (name, value) => writeFile(path.join(dir, name), JSON.stringify(value, null, 2) + '\n');
  const dataset = await read('valuation-history.json');
  const lock = await read('index-history-lock.json');
  assertDatasetMatchesIndexHistoryLock(dataset, lock);
  const metrics = await read('index-yahoo-daily-metrics.json');
  let cache, ledger;
  try { cache = await read('etf-ttm-closes.json'); } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    cache = { version: 1, symbols: {} };
  }
  if (cache.version !== 1 || !cache.symbols) throw new Error('Invalid ETF TTM close cache');
  try { ledger = await read('index-gap-repairs.json'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const changes = [];
  await Promise.all(dataset.indices.filter(item => ETF_TTM_SYMBOLS.has(item.symbol)).map(async item => {
    const snapshots = metrics.symbols[item.symbol] || [];
    const anchors = etfTtmAnchors(snapshots);
    if (!anchors.some(row => row.source === 'stockanalysis-latest')) return;
    const first = (anchors.findLast(row => row.date <= ETF_TTM_START) || anchors[0]).date;
    const target = item.points.at(-1).date;
    const prices = new Map();
    for (const row of cache.symbols[item.symbol] || []) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(row.date) || !Number.isFinite(row.close) || row.close <= 0 || !row.source) throw new Error(`Invalid ETF close: ${item.symbol}`);
      prices.set(row.date, row);
    }
    const required = tradingDates(first, target);
    const missing = required.filter(date => !prices.has(date));
    if (missing.length) {
      for (const row of await fetchCloses(item.symbol, 'etf', missing[0], target, missing)) {
        if (row.date >= first && row.date <= target && !prices.has(row.date)) prices.set(row.date, row);
      }
    }
    const remaining = required.filter(date => !prices.has(date));
    if (remaining.length) throw new Error(`Incomplete ETF closes for ${item.symbol}: ${remaining.join(', ')}`);
    cache.symbols[item.symbol] = [...prices.values()].sort((a, b) => a.date.localeCompare(b.date));
    const before = item.points;
    item.points = applyEtfTtmPricePolicy(before, snapshots, cache.symbols[item.symbol]);
    if(item.id === 'dram' && item.points.at(-1).date >= '2026-10-23') item.points = reconstructDramTtm(item.points,snapshots,cache.symbols[item.symbol]);
    // This publication repair may only change TTM and its provenance.
    for (let i = 0; i < before.length; i++) {
      const { pe_ttm: oldPe, pe_ttm_estimate: oldEstimate, pe_ttm_snapshot: oldSnapshot, ...oldOther } = before[i];
      const { pe_ttm, pe_ttm_estimate, pe_ttm_snapshot, ...other } = item.points[i];
      assert.deepEqual(other, oldOther);
      if (before[i].date < ETF_TTM_START) assert.deepEqual(item.points[i], before[i]);
      if (!isDeepStrictEqual(before[i],item.points[i])) changes.push({ symbol: item.symbol, date: before[i].date, before: oldPe, after: pe_ttm });
    }
    if (ledger) for (const repair of ledger.repairs[item.symbol] || []) {
      [repair.point] = applyEtfTtmPricePolicy([repair.point], snapshots, cache.symbols[item.symbol]);
      if(item.id === 'dram' && item.points.at(-1).date >= '2026-10-23') [repair.point] = reconstructDramTtm([repair.point],snapshots,cache.symbols[item.symbol]);
    }
  }));
  if (write) {
    await save('etf-ttm-closes.json', cache);
    if (changes.length) {
      dataset.generatedAt = new Date().toISOString();
      await save('valuation-history.json', dataset);
      await save('index-history-lock.json', buildIndexHistoryLock(dataset));
      if (ledger) await save('index-gap-repairs.json', ledger);
    }
  }
  return changes.sort((a, b) => a.symbol.localeCompare(b.symbol) || a.date.localeCompare(b.date));
}
if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  const changes = await repairEtfTtmHistory({ write: process.argv.includes('--write') });
  console.log('[ETF TTM]', JSON.stringify({ changed: changes.length, symbols: [...new Set(changes.map(row => row.symbol))], examples: changes.slice(0, 5) }));
}
