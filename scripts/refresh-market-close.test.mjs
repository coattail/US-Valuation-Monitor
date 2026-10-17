import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { refreshMarketClose, parseTreasuryYields } from './refresh-market-close.mjs';
import { marketClosePlan } from './market-close-plan.mjs';
import { buildIndexHistoryLock, assertDatasetMatchesIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';
import { fetchGapCloses } from '../packages/data-pipeline/src/gap-repair.ts';

test('close schedule starts after 16:00 New York in both DST offsets and skips holidays', () => {
  const plan = time => marketClosePlan({ schedule: '7,22,47 20,21 * * 1-5', now: new Date(time) });
  assert.deepEqual(plan('2026-10-14T20:07:00Z'), { mode: 'close', target: '2026-10-14', run: 'true', datasets: '["index"]' });
  assert.equal(plan('2026-12-14T20:07:00Z').run, 'false');
  assert.deepEqual(plan('2026-12-14T21:07:00Z'), { mode: 'close', target: '2026-12-14', run: 'true', datasets: '["index"]' });
  assert.equal(plan('2026-12-25T21:07:00Z').run, 'false');
  assert.equal(plan('2026-10-17T20:07:00Z').run, 'false');
  assert.equal(marketClosePlan({ schedule: '17 5 * * 2-6', now: new Date('2026-10-15T05:17:00Z') }).mode, 'full');
  assert.equal(marketClosePlan({ schedule: '17 9 * * 2-6', now: new Date('2026-10-15T09:17:00Z') }).run, 'true');
  assert.equal(marketClosePlan({ event: 'workflow_dispatch', mode: 'close' }).run, 'true');
});

test('overnight company runs also synchronize WSJ without rebuilding index history', () => {
  for (const schedule of ['17 22 * * 1-5', '17 1 * * 2-6']) {
    const plan = marketClosePlan({ schedule, now: new Date('2026-10-15T01:17:00Z') });
    assert.equal(plan.mode, 'company');
    assert.equal(plan.datasets, '["index","company"]');
    assert.equal(plan.run, 'true');
  }
  assert.equal(marketClosePlan({ event: 'workflow_dispatch', mode: 'company' }).datasets, '["index","company"]');
  assert.equal(marketClosePlan({ event: 'workflow_dispatch', mode: 'full' }).datasets, '["index","company"]');
  assert.equal(marketClosePlan({ event: 'workflow_dispatch', mode: 'close' }).datasets, '["index"]');
});

test('fast price fetch uses verified Yahoo symbol and unadjusted close before lagging Nasdaq history', async () => {
  const calls = [];
  const prices = await fetchGapCloses('BRK.B', 'stocks', '2026-10-13', '2026-10-14', ['2026-10-13', '2026-10-14'], async url => {
    calls.push(url);
    return JSON.stringify({ chart: { result: [{ meta: { symbol: 'BRK-B' },
      timestamp: [Date.parse('2026-10-13T13:30Z') / 1000, Date.parse('2026-10-14T13:30Z') / 1000],
      indicators: { quote: [{ close: [100, 105] }], adjclose: [{ adjclose: [10, 11] }] } }] } });
  }, { preferYahoo: true });
  assert.equal(calls.length, 1);
  assert.match(calls[0], /query1.finance.yahoo.com/);
  assert.deepEqual(prices.map(row => row.close), [100, 105]);
});

async function fixture(t, kind, indices, symbols = {}) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'usvm-close-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const write = async (name, data) => writeFile(path.join(dir, name), JSON.stringify(data));
  const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
  const dataset = { generatedAt: '2026-10-14T09:00:00Z', source: 'published', indices };
  await write(`${kind}-yahoo-daily-metrics.json`, { symbols });
  if (kind === 'index') {
    await write('valuation-history.json', dataset);
    await write('index-history-lock.json', buildIndexHistoryLock(dataset));
  } else {
    await mkdir(path.join(dir, 'company-series'));
    await write('company-valuation-snapshot.json', { ...dataset, indices: indices.map(({ points, ...row }) => row) });
    for (const row of indices) await write(`company-series/${row.id}.json`, row);
    await write('company-valuation-history.json', { indices: [] }); // Must not be used.
  }
  return { dir, read, dataset };
}

const now = new Date('2026-10-14T20:07:00Z');
const anchor = { date: '2026-10-13', pe_ttm: 20, pe_forward: 18, pb: 3, us10y_yield: .04 };
const fetchCloses = async () => [{ date: '2026-10-13', close: 100, source: 'dated-price' }, { date: '2026-10-14', close: 105, source: 'dated-price' }];

test('company close estimation is rejected before touching files or sources', async () => {
  await assert.rejects(refreshMarketClose('company', { dir: '/does-not-exist', now,
    fetchCloses: () => { throw new Error('Must not fetch company prices'); },
  }), /index-only/);
});

test('index appends with dated Treasury carry when FRED has not finalized and preserves history lock', async t => {
  const { dir, read, dataset } = await fixture(t, 'index', [{ id: 'dow30', symbol: 'DIA', points: [anchor] }]);
  const result = await refreshMarketClose('index', { dir, now, fetchCloses,
    ndxCloses: async () => [], spxCloses: async () => [],
    fetchYield: async () => 'observation_date,DGS10\n2026-10-13,4.1\n2026-10-14,.\n2026-10-15,9.9',
  });
  assert.equal(result.added, 1);
  const next = await read('valuation-history.json');
  assertDatasetMatchesIndexHistoryLock(next, buildIndexHistoryLock(dataset), { allowAppendedPoints: true });
  assertDatasetMatchesIndexHistoryLock(next, await read('index-history-lock.json'));
  const point = next.indices[0].points[1];
  assert.equal(point.us10y_yield, .041);
  assert.equal(point.recovery.us10yYieldDate, '2026-10-13');
  assert.match(point.recovery.us10yYieldSource, /DGS10/);
  const retry = await refreshMarketClose('index', { dir, now, fetchCloses: () => { throw new Error('No network on a completed retry'); } });
  assert.equal(retry.added, 0);
  assert.deepEqual(await read('valuation-history.json'), next);
});

test('special index policies use actual SPX/NDX closes and preserve all published rows', async t => {
  const snapshot = { ...anchor, source: 'wsj-latest' };
  const indices = [{ id: 'sp500', symbol: 'SPY', points: [anchor] }, { id: 'nasdaq100', symbol: 'QQQ', points: [anchor] }];
  const { dir, read } = await fixture(t, 'index', indices, { SPY: [snapshot], QQQ: [snapshot] });
  const indexCloses = async () => [{ date: anchor.date, close: 1000, source: 'actual-index' }, { date: '2026-10-14', close: 1020, source: 'actual-index' }];
  await refreshMarketClose('index', { dir, now, fetchCloses, ndxCloses: indexCloses, spxCloses: indexCloses,
    fetchYield: async () => { throw new Error('FRED temporarily unavailable'); } });
  const next = await read('valuation-history.json');
  assert.equal(next.indices[0].points[1].pe_ttm, 20.4);
  assert.equal(next.indices[1].points[1].pe_forward, 18.36);
  for (const row of next.indices) assert.deepEqual(row.points[0], anchor);
});

test('missing actual index close does not prevent another symbol publishing valid progress', async t => {
  const { dir, read } = await fixture(t, 'index', [
    { id: 'nasdaq100', symbol: 'QQQ', points: [anchor] }, { id: 'dow30', symbol: 'DIA', points: [anchor] },
  ], { QQQ: [{ ...anchor, source: 'wsj-latest' }] });
  const result = await refreshMarketClose('index', { dir, now, fetchCloses,
    ndxCloses: async () => [], spxCloses: async () => [], fetchYield: async () => 'observation_date,DGS10\n2026-10-14,4.1',
  });
  assert.equal(result.added, 1);
  assert.deepEqual(result.pending, ['QQQ']);
  assert.equal((await read('valuation-history.json')).indices[0].points.length, 1);
});

test('missing price never advances the date and an open session is rejected', async t => {
  const { dir, read } = await fixture(t, 'index', [{ id: 'dow30', symbol: 'DIA', points: [anchor] }]);
  assert.equal((await refreshMarketClose('index', { dir, now, fetchCloses: async () => [],
    ndxCloses: async () => [], spxCloses: async () => [], fetchYield: async () => 'date,DGS10',
  })).added, 0);
  assert.equal((await read('valuation-history.json')).indices[0].points.at(-1).date, anchor.date);
  await assert.rejects(refreshMarketClose('index', { dir, now: new Date('2026-10-14T19:59:00Z'), target: '2026-10-14' }), /uncompleted/);
  assert.deepEqual(parseTreasuryYields('observation_date,DGS10\n2026-10-14,.', 'fred'), []);
});
