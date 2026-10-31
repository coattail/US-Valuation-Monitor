import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fetchWsjPeSnapshot, parseWsjRowPeValues, parseWsjPeSnapshotFromTextForTest } from '../packages/data-pipeline/src/generate.ts';
import { reconcileWsj, syncWsj, validateWsjFreshness } from './sync-wsj-valuations.mjs';
import { buildIndexHistoryLock, assertDatasetMatchesIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

const html = await readFile(new URL('../packages/data-pipeline/tests/fixtures/wsj-pe-yields-2026-10-16.html', import.meta.url), 'utf8');
const now = new Date('2026-10-17T10:00Z');
const observations = await fetchWsjPeSnapshot(async () => html, '2026-10-16');
const pairs = { sp500: ['SPY', 23.51, 19.25], nasdaq100: ['QQQ', 30.27, 21.45], dow30: ['DIA', 20.87, 19.78], russell2000: ['IWM', 34.62, 26.32] };
function fixture() {
  const indices = Object.entries(pairs).map(([id, [symbol]]) => ({ id, symbol, points: [
    { date: '2026-10-15', pe_ttm: 24, pe_forward: 20, pb: 3, us10y_yield: .04 },
    { date: '2026-10-16', pe_ttm: 24.1, pe_forward: 20.1, pb: 3.1, us10y_yield: .041,
      pe_ttm_estimate: { method: 'old' }, pe_forward_estimate: { method: 'old' }, recovery: { estimated: true } },
  ] }));
  return { dataset: { source: 'test', generatedAt: 'old', indices }, metrics: { symbols: {} } };
}

test('actual WSJ layout maps all four dated TTM/forward pairs', () => {
  for (const [id, [, trailing, forward]] of Object.entries(pairs)) assert.deepEqual(observations.get(id), { trailing, forward, asOfDate: '2026-10-16' });
});

test('stale mirrors cannot overwrite newer sections; equal dates retain canonical priority', async () => {
  const mixed = html.replaceAll('10/16/26', '10/09/26').replaceAll('October 16', 'October 9');
  const partial = html.replaceAll('10/16/26', '10/15/26').replaceAll('October 16', 'October 15');
  const calls = [];
  const result = await fetchWsjPeSnapshot(async url => { calls.push(url); return calls.length === 1 ? partial : mixed; }, '2026-10-16');
  for (const row of result.values()) assert.equal(row.asOfDate, '2026-10-15');
  const sections = html.replace('10/16/26†', '10/15/26†');
  const result2 = await fetchWsjPeSnapshot(async () => sections, '2026-10-16');
  assert.equal(result2.get('dow30').asOfDate, '2026-10-15');
  assert.equal(result2.get('sp500').asOfDate, '2026-10-16');
  const result3 = await fetchWsjPeSnapshot(async url => url.includes('market-data') ? partial : mixed, '2026-10-14');
  assert.equal(result3.size, 4);
  for (const row of result3.values()) assert.equal(row.asOfDate, '2026-10-09');
});

test('missing or extreme year-ago columns never shift forward values or dividend yields', () => {
  for (const yearAgo of ['N/A', '', '9999']) assert.equal(parseWsjRowPeValues(['NASDAQ 100 Index', '30.27', yearAgo, '21.45', '0.57'], 'nasdaq100').forward, 21.45);
  assert.equal(parseWsjRowPeValues(['NASDAQ 100 Index', '30.27', '33.15', '--', '5.57'], 'nasdaq100'), undefined);
  const parsed = parseWsjPeSnapshotFromTextForTest('Friday, October 16, 2026\n| NASDAQ 100 Index | 30.27 | | 21.45 | 0.57 |');
  assert.equal(parsed.get('nasdaq100').forward, 21.45);
});

test('already complete sessions receive delayed WSJ values, preserve other fields, and retry without changes', () => {
  const { dataset, metrics } = fixture();
  const result = reconcileWsj(dataset, metrics, observations, { now });
  assert.equal(result.changes.length, 4);
  for (const [i, row] of result.dataset.indices.entries()) {
    assert.deepEqual(row.points[0], dataset.indices[i].points[0]);
    assert.equal(row.points[1].pb, 3.1);
    assert.equal(row.points[1].us10y_yield, .041);
    assert.equal(row.points[1].pe_ttm_estimate, undefined);
    assert.equal(row.points[1].pe_forward_estimate, undefined);
  }
  validateWsjFreshness(result.dataset, result.metrics, now);
  const retry = reconcileWsj(result.dataset, result.metrics, observations, { now: new Date('2026-10-17T11:00Z') });
  assert.equal(retry.changed, false);
  assert.deepEqual(retry.dataset, result.dataset);
  assert.deepEqual(retry.metrics, result.metrics);
  assert.throws(() => validateWsjFreshness(dataset, metrics, now), /source validation failed/);
  result.dataset.indices[0].points.at(-1).pe_ttm = 1;
  assert.throws(() => validateWsjFreshness(result.dataset, result.metrics, now), /disagree/);
});

test('new WSJ anchors correct the following SPX/NDX estimates without rewriting earlier history', () => {
  const { dataset, metrics } = fixture();
  for (const row of dataset.indices.slice(0, 2)) row.points.push({ date: '2026-10-19', pe_ttm: 24, pe_forward: 20, pb: 3, us10y_yield: .04 });
  const closes = [{ date: '2026-10-16', close: 100, source: 'index' }, { date: '2026-10-19', close: 110, source: 'index' }];
  const result = reconcileWsj(dataset, metrics, observations, { now: new Date('2026-10-20T10:00Z'), spx: closes, ndx: closes });
  assert.equal(result.dataset.indices[0].points.at(-1).pe_ttm, 25.861);
  assert.equal(result.dataset.indices[1].points.at(-1).pe_forward, 23.595);
  assert.throws(() => reconcileWsj(dataset, metrics, observations, { now }), /Missing SPX/);
});

test('persisted synchronization updates the lock and ledger; failed fetch leaves all files intact', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'usvm-wsj-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const { dataset, metrics } = fixture();
  const files = { 'valuation-history.json': dataset, 'index-yahoo-daily-metrics.json': metrics,
    'index-history-lock.json': buildIndexHistoryLock(dataset), 'sp500-ttm-closes.json': { observations: [] }, 'nasdaq100-forward-closes.json': { observations: [] },
    'index-gap-repairs.json': { repairs: { SPY: [{ date: '2026-10-16', point: dataset.indices[0].points[1] }] } } };
  for (const [name, value] of Object.entries(files)) await writeFile(path.join(dir, name), JSON.stringify(value));
  await assert.rejects(syncWsj({ dir, now, fetchSnapshots: async () => new Map() }), /No dated WSJ/);
  assert.deepEqual(JSON.parse(await readFile(path.join(dir, 'valuation-history.json'))), dataset);
  const fallback = await syncWsj({ dir, now, fetchSnapshots: async () => new Map(), allowUnavailable: true });
  assert.equal(fallback.changed, false);
  assert.equal(fallback.sourceUnavailable, true);
  const unavailable = await syncWsj({ dir, now, fetchSnapshots: async () => { throw new Error('network unavailable'); }, allowUnavailable: true });
  assert.equal(unavailable.sourceUnavailable, true);
  assert.deepEqual(JSON.parse(await readFile(path.join(dir, 'valuation-history.json'))), dataset);
  const result = await syncWsj({ dir, now, fetchSnapshots: async () => observations });
  assertDatasetMatchesIndexHistoryLock(result.dataset, JSON.parse(await readFile(path.join(dir, 'index-history-lock.json'))));
  const ledger = JSON.parse(await readFile(path.join(dir, 'index-gap-repairs.json')));
  assert.equal(ledger.repairs.SPY[0].point.pe_ttm, 23.51);
});


test('every daily refresh fetches WSJ before applying cached-anchor estimates', async () => {
  const { scripts } = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
  assert.match(scripts['refresh:daily:index'], /^node scripts\/sync-wsj-valuations\.mjs --allow-unavailable &&/);
  for (const name of ['refresh:wsj:index', 'refresh:close:index', 'build:data:index']) assert.match(scripts[name], /refresh:daily:index/);
});
