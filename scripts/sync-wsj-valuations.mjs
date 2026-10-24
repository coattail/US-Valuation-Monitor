import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { fetchWsjPeSnapshot, upsertIndexYahooDailyMetricSnapshot as upsert } from '../packages/data-pipeline/src/generate.ts';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';
import { applySp500TtmPricePolicy } from '../packages/data-pipeline/src/sp500-ttm-policy.ts';
import { applyNasdaqForwardPricePolicy } from '../packages/data-pipeline/src/nasdaq-forward-policy.ts';
import { lastCompletedSession, isTradingDate, shiftDate } from '../packages/data-pipeline/src/market-calendar.ts';
import { forwardPlan, applyIndexForwardPolicy } from '../packages/data-pipeline/src/index-forward-policy.ts';

const ids = ['sp500', 'nasdaq100', 'dow30', 'russell2000'];
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// Correct only the dated source observation and its SPX/NDX estimate tail.
// A source refresh is still necessary when every trading date already exists.
export function reconcileWsj(dataset, metrics, observations, { now = new Date(), spx = [], ndx = [] } = {}) {
  const next = structuredClone(dataset), cache = structuredClone(metrics);
  const bySymbol = new Map(Object.entries(cache.symbols));
  const changes = [];
  for (const id of ids) {
    const quote = observations.get(id), index = next.indices.find(row => row.id === id);
    if (!quote || !index) continue;
    const date = quote.asOfDate;
    if (!date || !isTradingDate(date) || date > lastCompletedSession(now)) throw new Error(`Invalid WSJ date: ${id}/${date}`);
    if (![quote.trailing, quote.forward].every(value => Number.isFinite(value) && value > 0)) throw new Error(`Incomplete WSJ row: ${id}`);
    const existing = bySymbol.get(index.symbol) || [];
    if (existing.some(row => row.source === 'wsj-latest' && row.date > date)) continue;
    const point = index.points.find(row => row.date === date);
    if (!point) throw new Error(`WSJ ${id}/${date}: missing trading row; repair dates before source synchronization`);
    const same = existing.find(row => row.date === date);
    const unchanged = same?.source === 'wsj-latest' && same.pe_ttm === quote.trailing && same.pe_forward === quote.forward;
    upsert(bySymbol, index.symbol, { date, pe_ttm: quote.trailing, pe_forward: quote.forward, pb: null,
      source: 'wsj-latest', capturedAt: unchanged ? same.capturedAt : now.toISOString() });
    const original = structuredClone(index.points);
    Object.assign(point, { pe_ttm: quote.trailing, pe_forward: quote.forward });
    delete point.pe_ttm_estimate;
    delete point.pe_forward_estimate;
    const snapshots = bySymbol.get(index.symbol);
    if (id === 'sp500') {
      const tail = applySp500TtmPricePolicy(index.points.filter(row => row.date >= date), snapshots, spx);
      if (tail.some(row => row.pe_ttm === null)) throw new Error('Missing SPX closes for WSJ estimate tail');
      index.points = index.points.map(row => tail.find(p => p.date === row.date) || row);
    }
    if (id === 'nasdaq100') {
      const tail = applyNasdaqForwardPricePolicy(index.points.filter(row => row.date >= date), snapshots, ndx);
      if (tail.some(row => row.pe_forward === null)) throw new Error('Missing NDX closes for WSJ estimate tail');
      index.points = index.points.map(row => tail.find(p => p.date === row.date) || row);
    }
    for (let i = 0; i < original.length; i++) {
      const before = original[i], after = index.points[i];
      if (before.date < date) assert.deepEqual(after, before);
      const { pe_ttm, pe_forward, pe_ttm_estimate, pe_forward_estimate, ...other } = after;
      const { pe_ttm: a, pe_forward: b, pe_ttm_estimate: c, pe_forward_estimate: d, ...oldOther } = before;
      assert.deepEqual(other, oldOther);
      if (JSON.stringify(before) !== JSON.stringify(after)) changes.push({ id, date: before.date, before: [before.pe_ttm, before.pe_forward], after: [after.pe_ttm, after.pe_forward] });
    }
    const published = index.points.find(row => row.date === date);
    assert.equal(published.pe_ttm, quote.trailing);
    assert.equal(published.pe_forward, quote.forward);
  }
  cache.symbols = Object.fromEntries(bySymbol);
  const changed = changes.length > 0 || JSON.stringify(cache.symbols) !== JSON.stringify(metrics.symbols);
  if (changed) {
    cache.generatedAt = next.generatedAt = now.toISOString();
    if (!next.source.includes('+wsj-reconciled')) next.source += '+wsj-reconciled';
  }
  return { dataset: next, metrics: cache, changes, changed };
}

export function validateWsjFreshness(dataset, metrics, now = new Date(), { forwardInputs } = {}) {
  // Allow the publisher an overnight update window; then verify source dates,
  // not just the presence of a trading-day row.
  const target = lastCompletedSession(new Date(now.getTime() - 12 * 3600000));
  let friday = target;
  while (new Date(friday + 'T12:00Z').getUTCDay() !== 5) friday = shiftDate(friday, -1);
  while (!isTradingDate(friday)) friday = shiftDate(friday, -1);
  const problems = [];
  for (const id of ids) {
    const index = dataset.indices.find(row => row.id === id);
    const row = (metrics.symbols[index?.symbol] || []).filter(row => row.source === 'wsj-latest').at(-1);
    const expected = id === 'dow30' ? target : friday;
    if (!row || row.date < expected) problems.push(`${id}: WSJ ${row?.date || 'missing'}, expected >= ${expected}`);
    if (row) {
      const point = index.points.find(p => p.date === row.date);
      const plan = index && forwardPlan(index), inputs = forwardInputs?.indices?.[id];
      const expectedForward = point && plan?.weekly && inputs
        ? applyIndexForwardPolicy([point],metrics.symbols[index.symbol] || [],inputs,true)[0].pe_forward : row.pe_forward;
      if (point?.pe_ttm !== row.pe_ttm || point?.pe_forward !== expectedForward) problems.push(`${id}: published values disagree with WSJ ${row.date}`);
    }
  }
  if (problems.length) throw new Error(`WSJ source validation failed:\n${problems.join('\n')}`);
}

export async function syncWsj({ dir = path.join(root, 'data/standardized'), now = new Date(), fetchSnapshots = fetchWsjPeSnapshot, check = false } = {}) {
  const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
  const write = async (name, value) => writeFile(path.join(dir, name), JSON.stringify(value, null, 2) + '\n');
  const dataset = await read('valuation-history.json'), metrics = await read('index-yahoo-daily-metrics.json');
  assertDatasetMatchesIndexHistoryLock(dataset, await read('index-history-lock.json'));
  if (check) {
    let forwardInputs;
    try { forwardInputs = await read('index-forward-inputs.json'); } catch(error) { if(error.code !== 'ENOENT') throw error; }
    return validateWsjFreshness(dataset, metrics, now, {forwardInputs});
  }
  const observations = await fetchSnapshots(undefined, lastCompletedSession(now));
  if (!observations.size) throw new Error('No dated WSJ observations available; retain existing published data');
  const result = reconcileWsj(dataset, metrics, observations, { now,
    spx: (await read('sp500-ttm-closes.json')).observations,
    ndx: (await read('nasdaq100-forward-closes.json')).observations,
  });
  if (result.changed) {
    // Keep gap provenance in agreement with the corrected published point.
    let ledger;
    try { ledger = await read('index-gap-repairs.json'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (ledger) for (const index of result.dataset.indices) {
      const changedDates = new Set(result.changes.filter(row => row.id === index.id).map(row => row.date));
      const points = new Map(index.points.map(row => [row.date, row]));
      for (const repair of ledger.repairs[index.symbol] || []) if (changedDates.has(repair.date)) repair.point = points.get(repair.date);
    }
    await write('index-yahoo-daily-metrics.json', result.metrics);
    await write('valuation-history.json', result.dataset);
    await write('index-history-lock.json', buildIndexHistoryLock(result.dataset));
    if (ledger) await write('index-gap-repairs.json', ledger);
  }
  console.log('[wsj]', JSON.stringify({ changed: result.changed, corrections: result.changes }));
  return result;
}

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  syncWsj({ check: process.argv.includes('--check') }).catch(error => { console.error(error); process.exitCode = 1; });
}
