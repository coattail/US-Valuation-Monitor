import test from 'node:test';
import assert from 'node:assert/strict';
import { alignClosePriceBasis } from '../src/close-price-basis.ts';
import { fetchGapCloses, repairMissingPoints } from '../src/gap-repair.ts';

test('a small Nasdaq primary quote and Yahoo overlap difference does not rebase DIA', () => {
  const nasdaq = 'https://api.nasdaq.com/api/quote/DIA/info?assetclass=etf';
  const yahoo = 'https://query1.finance.yahoo.com/v8/finance/chart/DIA?interval=1d';
  const cached = [{ date: '2026-11-11', close: 524.39, source: nasdaq }];
  const incoming = [
    { date: '2026-11-11', close: 524.4400024414062, source: yahoo },
    { date: '2026-11-12', close: 523.20, source: yahoo },
  ];
  assert.deepEqual(alignClosePriceBasis('DIA', cached, incoming, '2026-11-12'), incoming);
  assert.equal(cached[0].close, 524.39);
  assert.throws(() => alignClosePriceBasis('DIA', [{ ...cached[0], source: yahoo }], incoming, '2026-11-12'), /Unverified/);
  assert.throws(() => alignClosePriceBasis('DIA', [{ ...cached[0], close: 524.17 }], incoming, '2026-11-12'), /Unverified/);
  const iwmY = [{ ...incoming[0], close: 283.09, source: yahoo.replace('/DIA?', '/IWM?') }];
  const iwmN = [{ ...cached[0], close: 283.06, source: nasdaq.replace('/DIA/', '/IWM/') }];
  assert.deepEqual(alignClosePriceBasis('IWM', iwmN, iwmY, '2026-11-12'), iwmY);
});

const prices = [
  { date: '2026-11-04', close: 150.1367, source: 'nasdaq' },
  { date: '2026-11-05', close: 131.9, source: 'nasdaq' },
];

test('SOXX split-day discontinuity retries a coherent fallback and recovers the missing session', async () => {
  const calls: string[] = [];
  const closes = await fetchGapCloses('SOXX', 'etf', '2026-11-04', '2026-11-05', prices.map(row => row.date), async url => {
    calls.push(url);
    if (url.includes('yahoo')) return JSON.stringify({ chart: { result: [{ meta: { symbol: 'SOXX' },
      timestamp: prices.map(row => Date.parse(row.date + 'T14:30Z') / 1000),
      indicators: { quote: [{ close: [450.41, 131.9] }] },
    }] } });
    return JSON.stringify({ status: { rCode: 200 }, data: { tradesTable: { rows: [
      { date: '11/04/2026', close: '150.1367' }, { date: '11/05/2026', close: '131.90' },
    ] } } });
  }, { preferYahoo: true });
  assert.equal(calls.length, 2);
  assert.match(calls[1], /api.nasdaq.com/);
  assert.deepEqual(closes.map(row => row.close), prices.map(row => row.close));
  const result = repairMissingPoints([{ date: '2026-11-04', pe_ttm: 42.73 }], ['2026-11-05'], closes, new Map());
  assert.equal(result.repairs.length, 1);
  assert.ok(Number(result.repairs[0].point.pe_ttm) > 37);
});

test('incoming split-adjusted closes retain the cached share basis with auditable conversion', () => {
  const cached = [{ ...prices[0], close: 450.4100036621094 }];
  const before = structuredClone(cached);
  const aligned = alignClosePriceBasis('SOXX', cached, prices, '2026-11-05');
  assert.equal(aligned[1].close, 395.70000000000005);
  assert.equal(aligned[1].basisAdjustment?.multiplier, 3);
  assert.equal(aligned[1].basisAdjustment?.originalClose, 131.9);
  assert.match(aligned[1].basisAdjustment!.source, /sec.gov/);
  assert.deepEqual(cached, before);
  assert.deepEqual(alignClosePriceBasis('SOXX', prices, prices, '2026-11-05'), prices);
  assert.throws(() => alignClosePriceBasis('SMH', cached, prices, '2026-11-05'), /Unverified/);
  assert.throws(() => alignClosePriceBasis('SOXX', cached, prices, '2026-11-04'), /Unverified/);
  assert.throws(() => alignClosePriceBasis('SOXX', cached, prices.slice(1), '2026-11-05'), /overlapping/);
  assert.throws(() => alignClosePriceBasis('SOXX', [cached[0], { ...prices[1], close: 131.9 }], prices, '2026-11-05'), /Inconsistent/);
});
