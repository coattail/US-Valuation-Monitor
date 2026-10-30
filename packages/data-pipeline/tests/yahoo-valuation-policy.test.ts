import test from 'node:test';
import assert from 'node:assert/strict';
import { parseYahooCurrentValuationTable, publishYahooValuationObservations } from '../src/yahoo-valuation-policy.mjs';
import { parseYahooValuationMeasuresFromHtml, mergeYahooLatestQuotePayloadsForTest as merge,
  createYahooDailyMetricSnapshots, mergeCurrentYahooSnapshotsIntoHistoryForTest as store } from '../src/build-company-snapshot.ts';

const table = (ttm = '16.97') => `<table><thead><tr><th></th><th>Current</th><th>6/30/2026</th></tr></thead>
  <tbody><tr><td><span>Trailing P/E</span><sup>3</sup></td><td><span>${ttm}</span></td><td>27.26</td></tr>
  <tr><td>Forward P/E</td><td>20.76</td><td>25.33</td></tr><tr><td>Price/Book</td><td>6.02</td><td>9.04</td></tr>
  <tr><td>PEG Ratio (current)</td><td>0.57</td><td>0.96</td></tr></tbody></table>`;

test('explicit Current cells survive nested markup and footnotes; missing Current never borrows history', () => {
  assert.deepEqual(parseYahooCurrentValuationTable(table()), { pe_ttm: 16.97, pe_forward: 20.76, pb: 6.02, peg: 0.57 });
  for (const missing of ['--', 'N/A', '<span class="loading"></span>']) {
    assert.equal(parseYahooCurrentValuationTable(table(missing)).pe_ttm, null);
    assert.equal(parseYahooValuationMeasuresFromHtml(table(missing))?.latest.pe_ttm, null);
  }
  assert.equal(parseYahooCurrentValuationTable(`<script>${table()}</script>`), null);
});

test('Current table wins over a conflicting quote and stale timeseries through storage and final publication', () => {
  const canonical = parseYahooValuationMeasuresFromHtml(table());
  const quote = { anchors: [], latest: { pe_ttm: 14.74, pe_forward: null, pb: null, peg: null }, source: 'yahoo-quote-page-latest:finance.yahoo.com' };
  const selected = merge([quote], canonical);
  assert.equal(selected.latest.pe_ttm, 16.97);
  assert.equal(selected.comparisons.pe_ttm.fallback, 14.74);
  const observations = createYahooDailyMetricSnapshots(selected, '2026-10-29', { pe_ttm: '2026-10-27', pe_forward: '2026-10-27' }, { pe_ttm: 99, pe_forward: 88 });
  const saved = store([], observations, { pe_ttm: '2026-10-27', pe_forward: '2026-10-27' }, { pe_ttm: ['2026-10-27'], pe_forward: ['2026-10-27'] }, '2026-10-29');
  assert.equal(saved.at(-1).date, '2026-10-29');
  assert.equal(saved.at(-1).pe_ttm, 16.97);
  assert.equal(saved.at(-1).pe_forward, 20.76);
  const repaired = publishYahooValuationObservations([{ date: '2026-10-28', pe_ttm: 14.87 }, { date: '2026-10-29', pe_ttm: 14.74, pb: 5 }], saved, 'GOOGL');
  assert.equal(repaired[0].pe_ttm, 14.87);
  assert.equal(repaired[1].pb, 6.02);
  assert.equal(repaired[1].valuationSources.pe_ttm.symbol, 'GOOGL');
  assert.equal(repaired[1].valuationSources.pe_ttm.basis, 'current-table');
});

test('a dated old Current observation never replaces the latest quote', () => {
  const canonical = parseYahooValuationMeasuresFromHtml(`<section>As of 4/23/2026${table()}</section>`);
  const quote = { anchors: [], latest: { pe_ttm: 14.74, pe_forward: null, pb: null, peg: null }, source: 'yahoo-quote-page-latest' };
  const snapshots = createYahooDailyMetricSnapshots(merge([quote], canonical), '2026-10-29', {}, {});
  assert.equal(snapshots.find(row => row.date === '2026-04-23').pe_ttm, 16.97);
  assert.equal(snapshots.find(row => row.date === '2026-10-29').pe_ttm, 14.74);
});

test('a later quote-only refresh cannot overwrite a verified Current observation on the same date', () => {
  const canonical = parseYahooValuationMeasuresFromHtml(table());
  const observations = createYahooDailyMetricSnapshots(merge([], canonical), '2026-10-29', {}, {});
  const quote = { date: '2026-10-29', pe_ttm: 14.74, pe_forward: null, pb: null, peg: null, source: 'yahoo-quote-page-latest' };
  const saved = store(observations, [quote], {}, {}, '2026-10-29');
  assert.equal(saved[0].pe_ttm, 16.97);
  assert.match(saved[0].metricSources.pe_ttm, /current-table/);
});

test('an explicit unavailable Current value clears fallback PE and retains its capture evidence', () => {
  const canonical = parseYahooValuationMeasuresFromHtml(table('--'));
  const quote = { anchors: [], latest: { pe_ttm: 14.74, pe_forward: null, pb: null, peg: null }, source: 'yahoo-quote-page-latest' };
  const rows = createYahooDailyMetricSnapshots(merge([quote], canonical), '2026-10-29', {}, {});
  const saved = store([{ date: '2026-10-29', pe_ttm: 14.74, pe_forward: 15, pb: 5, peg: 1, source: quote.source }], rows, {}, {}, '2026-10-29');
  assert.equal(saved[0].pe_ttm, null);
  const published = publishYahooValuationObservations([{ date: '2026-10-29', pe_ttm: 14.74 }], saved, 'CRWD');
  assert.equal(published[0].pe_ttm, null);
  const later = store(saved, [{ date: '2026-10-29', pe_ttm: 14.74, pe_forward: null, pb: null, peg: null, source: quote.source, capturedAt: '2099-01-01T00:00:00Z' }], {}, {}, '2026-10-29');
  assert.equal(later[0].pe_ttm, null);
  assert.notEqual(later[0].metricCapturedAt.pe_ttm, '2099-01-01T00:00:00Z');
});

test('an entirely unavailable Current table does not silently revive quote values', () => {
  const html = table('--').replace('20.76', '--').replace('6.02', '--').replace('0.57', '--');
  const canonical = parseYahooValuationMeasuresFromHtml(html);
  const selected = merge([], canonical);
  assert.ok(selected);
  const rows = createYahooDailyMetricSnapshots(selected, '2026-10-29', {}, {});
  const saved = store([], rows, {}, {}, '2026-10-29');
  assert.equal(saved.length, 1);
  assert.equal(saved[0].pe_ttm, null);
});

test('a new Current observation is not assigned to a stale company price tail', () => {
  const canonical = parseYahooValuationMeasuresFromHtml(table());
  const selected = merge([], canonical);
  assert.deepEqual(createYahooDailyMetricSnapshots(selected, '2026-10-28', {}, {}, '2026-10-29'), []);
});
