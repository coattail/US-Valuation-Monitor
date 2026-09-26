import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { applyNasdaqForwardObservationPolicy } from '../src/nasdaq-forward-policy.ts';
import { applyAuthoritativePublishedMetricCorrectionsForTest, assertPublishedIndexHistoryAppendOnly } from '../src/generate.ts';
import { repairMissingPoints } from '../src/gap-repair.ts';

const point = (date, pe_forward) => ({ date, pe_forward, pe_ttm: 36, pb: 8, us10y_yield: 0.04 });
const quotes = [
  { date: '2026-04-17', pe_forward: 24.62, source: 'wsj-latest' },
  { date: '2026-04-24', pe_forward: 25.15, source: 'wsj-latest' },
];

test('removes the mixed daily estimates without changing exact WSJ quotes or other metrics', () => {
  const input = [point('2026-04-09', 21.8677), point('2026-04-17', 24.62), point('2026-04-20', 22.756), point('2026-04-24', 25.15)];
  const output = applyAuthoritativePublishedMetricCorrectionsForTest(input, 'nasdaq100', quotes);
  assert.deepEqual(output.map(p => p.pe_forward), [21.8677, 24.62, null, 25.15]);
  assert.deepEqual(output.map((p, i) => ({ ...p, pe_forward: input[i].pe_forward })), input);
  assert.deepEqual(applyAuthoritativePublishedMetricCorrectionsForTest(output, 'nasdaq100', quotes), output);
});

test('a different provider and an unavailable quote cannot refill unobserved days', () => {
  const input = [point('2026-04-20', 22.756), point('2026-04-21', 22.6)];
  const snapshots = [
    { date: '2026-04-20', pe_forward: 30, source: 'ssga-official-latest' },
    { date: '2026-04-21', pe_forward: null, source: 'wsj-latest' },
  ];
  assert.deepEqual(applyAuthoritativePublishedMetricCorrectionsForTest(input, 'nasdaq100', snapshots).map(p => p.pe_forward), [null, null]);
  assert.deepEqual(applyNasdaqForwardObservationPolicy(input, []).map(p => p.pe_forward), [null, null]);
});

test('a delayed observation can fill a null without weakening the history lock', () => {
  const before = { indices: [{ id: 'nasdaq100', points: [point('2026-04-17', null), point('2026-04-20', null)] }] };
  const after = structuredClone(before);
  after.indices[0].points = applyAuthoritativePublishedMetricCorrectionsForTest(before.indices[0].points, 'nasdaq100', quotes);
  const allowed = new Map([['nasdaq100', new Map([['2026-04-17', { pe_forward: 24.62 }]])]]);
  assert.doesNotThrow(() => assertPublishedIndexHistoryAppendOnly(before, after, allowed));
  after.indices[0].points[1].pe_forward = 25;
  assert.throws(() => assertPublishedIndexHistoryAppendOnly(before, after, allowed), /published index history changed/);
});

test('price gap recovery cannot manufacture Nasdaq forward observations', () => {
  const recovered = repairMissingPoints([point('2026-04-17', 24.62)], ['2026-04-20'], [
    { date: '2026-04-17', close: 100, source: 'test' },
    { date: '2026-04-20', close: 110, source: 'test' },
  ], new Map([['2026-04-20', 0.04]]), quotes);
  const output = applyNasdaqForwardObservationPolicy(recovered.points, quotes);
  assert.equal(output[0].pe_forward, 24.62);
  assert.equal(output[1].pe_forward, null);
  assert.equal(output[1].pe_ttm, 39.6);
});

test('committed Nasdaq history exactly matches the persisted WSJ observations', async () => {
  const read = async name => JSON.parse(await readFile(new URL(`../../../data/standardized/${name}`, import.meta.url), 'utf8'));
  const [history, split, metrics] = await Promise.all([read('valuation-history.json'), read('index-series/nasdaq100.json'), read('index-yahoo-daily-metrics.json')]);
  const points = history.indices.find(row => row.id === 'nasdaq100').points;
  assert.deepEqual(points, applyNasdaqForwardObservationPolicy(points, metrics.symbols.QQQ));
  assert.deepEqual(split.points, points);
  assert.equal(points.find(p => p.date === '2026-04-17').pe_forward, 24.62);
});
