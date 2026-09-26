import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { applyNasdaqForwardPricePolicy, nasdaqForwardPriceCorrections, assertNasdaqForwardCoverage } from '../src/nasdaq-forward-policy.ts';
import { parseNasdaqForwardCloses, FRED_NDX_CLOSE_URL } from '../src/nasdaq-forward-closes.ts';
import { applyAuthoritativePublishedMetricCorrectionsForTest, assertPublishedIndexHistoryAppendOnly, loadAuthoritativePublishedMetricCorrections } from '../src/generate.ts';
import { repairMissingPoints } from '../src/gap-repair.ts';
const point = (date, pe_forward) => ({ date, pe_forward, pe_ttm: 36, pb: 8, us10y_yield: 0.04 });
const quotes = [
  { date: '2026-04-17', pe_forward: 24.62, source: 'wsj-latest' },
  { date: '2026-04-24', pe_forward: 25.15, source: 'wsj-latest' },
];
const closes = [['2026-04-17', 100], ['2026-04-20', 110], ['2026-04-21', 90], ['2026-04-24', 120], ['2026-04-27', 126]]
  .map(([date, close]) => ({ date, close, source: 'NDX-test' }));
const read = async name => JSON.parse(await readFile(new URL(`../../../data/standardized/${name}`, import.meta.url), 'utf8'));

test('daily PE follows actual NDX returns and resets at each untouched WSJ quote', () => {
  const input = [point('2026-04-09', 21.8677), ...closes.map(p => point(p.date, null))];
  const output = applyAuthoritativePublishedMetricCorrectionsForTest(input, 'nasdaq100', quotes, closes);
  assert.deepEqual(output.map(p => p.pe_forward), [21.8677, 24.62, 27.082, 22.158, 25.15, 26.4075]);
  assert.equal(output[2].pe_forward_estimate.anchorDate, '2026-04-17');
  assert.equal(output.at(-1).pe_forward_estimate.anchorDate, '2026-04-24');
  assert.equal(output[1].pe_forward_estimate, undefined);
  assert.equal(output[4].pe_forward_estimate, undefined);
  for (const row of output) assert.deepEqual([row.pe_ttm, row.pb, row.us10y_yield], [36, 8, 0.04]);
  assert.deepEqual(applyAuthoritativePublishedMetricCorrectionsForTest(output, 'nasdaq100', quotes, closes), output);
  // A future quote does not interpolate or change the days before it.
  assert.equal(applyNasdaqForwardPricePolicy(input, quotes.slice(0, 1), closes)[2].pe_forward, output[2].pe_forward);
});

test('another provider cannot replace WSJ or serve as an earnings anchor', () => {
  const snapshots = [...quotes, { date: '2026-04-20', pe_forward: 99, source: 'ssga-official-latest' }];
  assert.equal(applyAuthoritativePublishedMetricCorrectionsForTest([point('2026-04-20', null)], 'nasdaq100', snapshots, closes)[0].pe_forward, 27.082);
});

test('a delayed WSJ quote updates its following daily estimates and the lock permits only reproducible corrections', () => {
  const input = [point('2026-04-17', null), point('2026-04-20', null)];
  const previous = applyNasdaqForwardPricePolicy(input, [{ date: '2026-04-10', pe_forward: 20, source: 'wsj-latest' }], [...closes, { date: '2026-04-10', close: 100, source: 'NDX-test' }]);
  const corrected = applyNasdaqForwardPricePolicy(input, quotes, closes);
  const before = { indices: [{ id: 'nasdaq100', points: previous }] };
  const after = { indices: [{ id: 'nasdaq100', points: corrected }] };
  const allowed = new Map([['nasdaq100', nasdaqForwardPriceCorrections(quotes, closes)]]);
  assert.doesNotThrow(() => assertPublishedIndexHistoryAppendOnly(before, after, allowed));
  after.indices[0].points[1].pe_forward += 0.1;
  assert.throws(() => assertPublishedIndexHistoryAppendOnly(before, after, allowed), /published index history changed/);
});

test('an estimated correction cannot replace a previously published original quote', () => {
  const before = { indices: [{ id: 'nasdaq100', points: [point('2026-04-20', 30)] }] };
  const after = { indices: [{ id: 'nasdaq100', points: applyNasdaqForwardPricePolicy(before.indices[0].points, quotes, closes) }] };
  assert.throws(() => assertPublishedIndexHistoryAppendOnly(before, after, new Map([['nasdaq100', nasdaqForwardPriceCorrections(quotes, closes)]])), /published index history changed/);
});

test('unavailable or nonpositive closes cannot be invented from PE or QQQ', () => {
  const input = [point('2026-04-17', null), point('2026-04-20', null)];
  const result = applyNasdaqForwardPricePolicy(input, quotes, [{ date: '2026-04-17', close: 0, source: 'bad' }]);
  assert.equal(result[0].pe_forward, 24.62);
  assert.equal(result[1].pe_forward, null);
  assert.throws(() => assertNasdaqForwardCoverage(result), /2026-04-20/);
  assert.throws(() => parseNasdaqForwardCloses(JSON.stringify({ chart: { result: [{ meta: { symbol: 'QQQ' } }] } }), 'yahoo'), /Expected \^NDX/);
});

test('NDX parser uses unadjusted closing prices and skips missing FRED sessions', () => {
  const raw = { chart: { result: [{ meta: { symbol: '^NDX' }, timestamp: [Date.parse('2026-04-17') / 1000], indicators: { quote: [{ close: [100] }], adjclose: [{ adjclose: [90] }] } }] } };
  assert.equal(parseNasdaqForwardCloses(JSON.stringify(raw), 'yahoo')[0].close, 100);
  assert.deepEqual(parseNasdaqForwardCloses('observation_date,NASDAQ100\n2026-04-17,100\n2026-04-18,.', FRED_NDX_CLOSE_URL).map(p => p.close), [100]);
});

test('gap recovery uses NDX prices for Forward PE while keeping other recovered metrics', () => {
  const recovered = repairMissingPoints([point('2026-04-17', 24.62)], ['2026-04-20'], [
    { date: '2026-04-17', close: 100, source: 'QQQ-test' },
    { date: '2026-04-20', close: 120, source: 'QQQ-test' },
  ], new Map([['2026-04-20', 0.04]]), quotes);
  const output = applyNasdaqForwardPricePolicy(recovered.points, quotes, closes);
  assert.equal(output[1].pe_forward, 27.082);
  assert.equal(output[1].pe_forward_estimate.priceSource, 'NDX-test');
  assert.equal(output[1].pe_ttm, 43.2);
});

test('committed daily history is complete, reproducible, and permitted by persisted price corrections', async () => {
  const [history, split, metrics, prices] = await Promise.all([read('valuation-history.json'), read('index-series/nasdaq100.json'), read('index-yahoo-daily-metrics.json'), read('nasdaq100-forward-closes.json')]);
  const points = history.indices.find(row => row.id === 'nasdaq100').points;
  assertNasdaqForwardCoverage(points);
  assert.deepEqual(points, applyNasdaqForwardPricePolicy(points, metrics.symbols.QQQ, prices.observations));
  assert.deepEqual(split.points, points);
  assert.equal(points.find(p => p.date === '2026-04-17').pe_forward, 24.62);
  assert.equal(points.find(p => p.date === '2026-04-20').pe_forward, 24.5442);
  const corrections = (await loadAuthoritativePublishedMetricCorrections()).get('nasdaq100');
  for (const row of points.filter(p => p.date >= '2026-04-10')) assert.equal(corrections.get(row.date).pe_forward, row.pe_forward);
});
