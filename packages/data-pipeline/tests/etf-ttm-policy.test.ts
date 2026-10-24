import test from 'node:test';
import assert from 'node:assert/strict';
import { applyEtfTtmPricePolicy, etfTtmAnchors } from '../src/etf-ttm-policy.ts';

const dates = ['2026-07-16', '2026-07-17', '2026-07-20', '2026-07-21'];
const points = dates.map(date => ({ date, pe_ttm: 30, pe_forward: 20, pb: 4, us10y_yield: .04 }));
const closes = dates.map((date, i) => ({ date, close: 100 + 10 * i, source: 'actual-unadjusted-close' }));
const snapshot = (date: string, pe_ttm = 30, source = 'stockanalysis-latest') => ({ date, pe_ttm, source });

test('repeated undated values stay anchored to their first sighting instead of the latest scrape', () => {
  const raw = dates.map(date => snapshot(date));
  const original = structuredClone(raw);
  const result = applyEtfTtmPricePolicy(points, raw, closes);
  assert.deepEqual(etfTtmAnchors(raw).map(row => row.date), [dates[0]]);
  assert.deepEqual(result.map(row => row.pe_ttm), [30, 33, 36, 39]);
  assert.deepEqual(raw, original); // Raw observation ledger is not rewritten.
  assert.deepEqual(result[0], points[0]); // Validated prefix is untouched.
  assert.equal(result[3].pe_ttm_estimate?.anchorDate, dates[0]);
  assert.equal(result[3].pe_ttm_estimate?.anchorPriceSource, closes[0].source);
  assert.deepEqual(applyEtfTtmPricePolicy(result, raw, closes), result);
});

test('a changed snapshot replaces a frozen estimate even when its trading date already exists', () => {
  const raw = [snapshot(dates[0]), snapshot(dates[2], 24), snapshot(dates[3], 24)];
  const result = applyEtfTtmPricePolicy(points, raw, closes);
  assert.deepEqual(result.map(row => row.pe_ttm), [30, 33, 24, 26]);
  assert.deepEqual(result[2].pe_ttm_snapshot, {source:'stockanalysis-latest',dateBasis:'first-seen'});
  assert.equal(result[2].pe_ttm_estimate, undefined);
  for (let i = 0; i < result.length; i++) {
    assert.equal(result[i].pe_forward, points[i].pe_forward);
    assert.equal(result[i].pb, points[i].pb);
    assert.equal(result[i].us10y_yield, points[i].us10y_yield);
  }
});

test('equal dated official observations are preserved and supersede an undated snapshot', () => {
  const raw = [snapshot(dates[0]), snapshot(dates[1], 28, 'ishares-official-latest'), snapshot(dates[2], 28, 'ishares-official-latest')];
  const result = applyEtfTtmPricePolicy(points, raw, closes);
  assert.equal(etfTtmAnchors(raw).length, 3);
  assert.equal(result[1].pe_ttm, 28);
  assert.equal(result[2].pe_ttm, 28);
  assert.equal(result[2].pe_ttm_estimate, undefined);
  assert.equal(result[3].pe_ttm, Number((28 * 130 / 120).toFixed(4)));
  assert.equal(result[3].pe_ttm_estimate?.method, 'dated-ttm-etf-price-carry');
});

test('missing actual prices fail instead of certifying a flat daily series', () => {
  assert.throws(() => applyEtfTtmPricePolicy(points, [snapshot(dates[0])], closes.slice(0, 2)), /Missing ETF TTM close/);
});

test('no anchor is borrowed from the future or from another source', () => {
  assert.deepEqual(applyEtfTtmPricePolicy(points, [snapshot('2026-07-22')], closes), points);
  assert.deepEqual(applyEtfTtmPricePolicy(points, [snapshot(dates[0], 80, 'unknown-source')], closes), points);
});
