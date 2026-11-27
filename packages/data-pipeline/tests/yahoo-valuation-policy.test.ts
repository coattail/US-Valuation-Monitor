import test from 'node:test';
import assert from 'node:assert/strict';
import { parseYahooForwardQuote } from '../src/yahoo-forward-quote.mjs';
import { parseYahooCurrentValuationTable, publishYahooValuationObservations, readYahooClosingQuote } from '../src/yahoo-valuation-policy.mjs';
import { createYahooDailyMetricSnapshots, parseYahooQuotePageRatioPayload, parseYahooValuationMeasuresFromHtml,
  mergeCurrentYahooSnapshotsIntoHistoryForTest as store } from '../src/build-company-snapshot.ts';

const table = (ttm = '17.73', pb = '14.95') => `<table><thead><tr><th></th><th>Current</th><th>6/30/2026</th></tr></thead>
  <tbody><tr><td><span>Trailing P/E</span><sup>3</sup></td><td><span>${ttm}</span></td><td>27.26</td></tr>
  <tr><td>Forward P/E</td><td>9.88</td><td>25.33</td></tr><tr><td>Price/Book</td><td>${pb}</td><td>9.04</td></tr></tbody></table>`;
const quoteHtml = (label, price, ttm) => `<section data-testid="quote-hdr">
    <fin-streamer data-field="regularMarketPrice" data-value="${price}">${price}</fin-streamer>
    <span>${label}</span><fin-streamer data-field="postMarketPrice" data-value="199.00">199</fin-streamer>
  </section><span title="PE Ratio (TTM)">PE Ratio (TTM)</span><fin-streamer data-value="${ttm}" data-field="trailingPE">${ttm}</fin-streamer>
  <span>EPS (TTM)</span><fin-streamer data-field="epsTrailingTwelveMonths" data-value="9.31">9.31</fin-streamer>`;
const prices = [
  { date: '2026-11-20', close: 125.53, ts: Date.parse('2026-11-20') },
  { date: '2026-11-23', close: 165.05, ts: Date.parse('2026-11-23') },
];
const target = '2026-11-23';
const fiscalData = { pe_ttm: null, pe_forward: 9.88, pb: null, peg: null };

test('explicit Current cells survive nested markup and footnotes; missing Current never borrows history', () => {
  assert.deepEqual(parseYahooCurrentValuationTable(table()), { ...fiscalData, pe_ttm: 17.73, pb: 14.95 });
  for (const missing of ['--', 'N/A', '<span class="loading"></span>']) {
    assert.equal(parseYahooCurrentValuationTable(table(missing)).pe_ttm, null);
    assert.equal(parseYahooValuationMeasuresFromHtml(table(missing))?.latest.pe_ttm, null);
  }
  assert.equal(parseYahooCurrentValuationTable(`<script>${table()}</script>`), null);
});

test('a verified NVDA close keeps its 17.73 TTM through a later intraday refresh and a stale multiple', () => {
  const afterClose = (pe = 17.73) => parseYahooQuotePageRatioPayload(quoteHtml('At close: November 23 at 4:00:01 PM EST', 165.05, pe));
  const intraday = parseYahooQuotePageRatioPayload(quoteHtml('As of 3:09:51 PM EST. Market Open.', 152.12, 16.34));
  const stale = afterClose(13.48);
  const undatedTable = parseYahooValuationMeasuresFromHtml(`<aside>As of 11/23/2026</aside>${table()}`);
  const original = createYahooDailyMetricSnapshots(afterClose(), null, null, prices, target);
  assert.equal(original[0].pe_ttm, 17.73);
  assert.equal(original[0].metricSessionDates?.pe_ttm, target);
  for (const quote of [intraday, stale]) {
    const incoming = createYahooDailyMetricSnapshots(quote, undatedTable, null, prices, target);
    assert.deepEqual(incoming, []);
    assert.equal(store(original, incoming, {}, {}, target)[0].pe_ttm, 17.73);
  }
  assert.deepEqual(createYahooDailyMetricSnapshots(afterClose(), null, null, [prices[0]], target), []);
  const syntheticClose = { ...prices[1], estimated: true as const };
  assert.deepEqual(createYahooDailyMetricSnapshots(afterClose(), null, null, [syntheticClose], target), []);
});

test('statistics dates and forward feed dates stay on their original close sessions', () => {
  const dated = parseYahooValuationMeasuresFromHtml(`<section data-testid="valuation-measures"><span>As of 11/20/2026</span>${table('17.73', '--')}</section>`);
  const forward = { anchors: [{ date: target, ...fiscalData }], latest: fiscalData, source: 'yahoo-forward-pe-timeseries' };
  const rows = createYahooDailyMetricSnapshots(null, dated, forward, prices, target);
  assert.deepEqual(rows.map(row => [row.date, row.pe_ttm, row.pe_forward, row.pb, row.metricSessionDates]), [
    ['2026-11-20', null, 9.88, null, { pe_forward: '2026-11-20', pb: '2026-11-20', peg: '2026-11-20' }],
    ['2026-11-23', null, 9.88, null, { pe_forward: '2026-11-23' }],
  ]);
});

test('mixed-source rows keep each verified metric when the daily store is reloaded', () => {
  const close = prices.at(-1)!;
  const stats = parseYahooValuationMeasuresFromHtml(`<section data-testid="valuation-measures">As of 11/23/2026${table().replace('</tbody>', '<tr><td>PEG Ratio (5yr expected)</td><td>1.45</td><td>1.60</td></tr></tbody>')}</section>`);
  const quote = parseYahooQuotePageRatioPayload(quoteHtml('At close: November 23 at 4:00:01 PM EST', close.close, 17.73));
  const forward = { anchors: [{ date: target, ...fiscalData }], latest: fiscalData, source: 'yahoo-forward-pe-timeseries' };
  const current = createYahooDailyMetricSnapshots(quote, stats, forward, prices, target);
  const saved = store([], current, {}, {}, target);
  assert.equal(saved[0].source, forward.source);
  const [reloaded] = store(saved, [], {}, {}, target);
  assert.deepEqual(reloaded, saved[0]);
  assert.deepEqual([reloaded.pe_ttm, reloaded.pe_forward, reloaded.pb, reloaded.peg], [17.73, 9.88, 14.95, 1.45]);
});

test('only fields with their own verified date can publish on a new session', () => {
  const date = '2026-11-27';
  const original = [{ date, pe_ttm: 12.4, pb: 11.37, pe_forward: 7, peg: 2 }];
  const observation = { date, pe_ttm: 17.73, pb: 11.37, source: 'yahoo-quote-page-latest', metricSessionDates: { pe_ttm: date } };
  const [point] = publishYahooValuationObservations(original, [observation], 'NVDA');
  assert.equal(point.pe_ttm, 17.73);
  assert.equal(point.pb, null);
  assert.equal(point.pe_forward, null);
  assert.equal(point.peg, null);
  assert.equal(point.valuationSources.pe_ttm.verifiedSession, date);
  assert.deepEqual(publishYahooValuationObservations(original, [{ ...observation, metricSessionDates: {} }], 'NVDA'), []);
});

test('Yahoo closing quote identifies US and UK close labels across New Year', () => {
  const now = new Date('2026-01-01T15:00:00Z');
  for (const label of ['At close: December 31 at 4:00:00 PM EST', 'At close: 31 December at 16:00:00 GMT-5']) {
    assert.deepEqual(readYahooClosingQuote(quoteHtml(label, 100, 10), now), { date: '2025-12-31', close: 100 });
  }
  assert.equal(readYahooClosingQuote(quoteHtml('At close: November 31 at 4:00:00 PM EST', 100, 10), new Date('2026-12-01T15:00Z')), null);
});

test('dated PB with a stale numerator is rejected while independently dated Forward PE is kept', () => {
  const financials = '<table><tr><td>Book Value Per Share (mrq)</td><td>11.06</td></tr></table>';
  const observed = pb => parseYahooValuationMeasuresFromHtml(`<section data-testid="valuation-measures"><span>As of 11/23/2026</span>${table('17.73', pb)}</section>${financials}`);
  const [stale] = createYahooDailyMetricSnapshots(null, observed('11.37'), null, prices, target);
  assert.equal(stale.pe_forward, 9.88);
  assert.equal(stale.pb, null);
  assert.equal(stale.metricSessionDates?.pb, undefined);
  const [verified] = createYahooDailyMetricSnapshots(null, observed('14.92'), null, prices, target);
  assert.equal(verified.pb, 14.92);
  assert.equal(verified.metricSessionDates?.pb, target);
});

test('direct Yahoo forward quotes retain the original multiple and reject intraday or stale values', () => {
  const raw = { symbol: 'NVDA', marketState: 'CLOSED', regularMarketTime: 1795640401,
    regularMarketPrice: 149.3, forwardPE: 8.991117, epsForward: 16.60528 };
  const date = '2026-11-25';
  const quote = parseYahooForwardQuote(raw);
  assert.deepEqual(quote, { symbol: 'NVDA', date, close: 149.3, value: 8.991117 });
  assert.equal(parseYahooForwardQuote({ ...raw, marketState: 'REGULAR' }), null);
  assert.equal(parseYahooForwardQuote({ ...raw, forwardPE: 9.30 }), null);
  const closes = [{ date, ts: Date.parse(date), close: 149.3 }];
  const direct = createYahooDailyMetricSnapshots(null, null, null, closes, date, quote!);
  assert.equal(direct[0].pe_forward, 8.991117);
  assert.equal(direct[0].metricSessionDates?.pe_forward, date);
  assert.deepEqual(createYahooDailyMetricSnapshots(null, null, null, [{ ...closes[0], close: 155.38 }], date, quote!), []);
});

test('the localized valuation date belongs to the panel rather than the quote header', () => {
  const compact = (date: string) => `<section data-testid="valuation-measures">As of ${date}${table()}</section>`;
  const actual = parseYahooValuationMeasuresFromHtml(compact('23/11/2026'), 'sg.finance.yahoo.com');
  assert.equal(actual?.anchors[0].date, '2026-11-23');
  const canadian = parseYahooValuationMeasuresFromHtml(compact('2026-11-20'), 'ca.finance.yahoo.com');
  assert.equal(canadian?.anchors[0].date, '2026-11-20');
  const ambiguous = parseYahooValuationMeasuresFromHtml(compact('10/11/2026'), 'uk.finance.yahoo.com');
  assert.equal(ambiguous?.anchors[0].date, '2026-11-10');
});

test('a dated panel cannot overwrite a date-matched direct Forward PE', () => {
  const date = '2026-11-25';
  const close = { date, close: 149.3, ts: Date.parse(date) };
  const quote = { date, close: 149.3, value: 8.991117 };
  const old = createYahooDailyMetricSnapshots(null, null, null, [close], date, quote);
  const panel = parseYahooValuationMeasuresFromHtml(`<section data-testid="valuation-measures">As of 11/25/2026${table()}</section>`);
  const newer = createYahooDailyMetricSnapshots(null, panel, null, [close], date);
  const [kept] = store(old, newer, {}, {}, date);
  assert.equal(kept.pe_forward, 8.991117);
  assert.equal(kept.metricSources?.pe_forward, 'yahoo-quote-api-forward');
});
