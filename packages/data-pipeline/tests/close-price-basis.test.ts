import test from 'node:test';
import assert from 'node:assert/strict';
import { alignClosePriceBasis } from '../src/close-price-basis.ts';
import { fetchGapCloses, repairMissingPoints } from '../src/gap-repair.ts';

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
