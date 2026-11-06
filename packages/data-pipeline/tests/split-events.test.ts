import test from 'node:test';
import assert from 'node:assert/strict';
import { parseYahooSplitEvents, fetchYahooSplitEvents, type SplitEvent } from '../src/split-events.ts';
import { alignClosePriceBasis, coherentCloseWindow } from '../src/close-price-basis.ts';
import { fetchGapCloses } from '../src/gap-repair.ts';

const date = '2026-11-05', source = 'https://query1.finance.yahoo.com/v8/finance/chart/TEST?events=splits';
const event = (ratio: number, day = date): SplitEvent => ({ symbol: 'TEST', date: day, ratio, source });
const payload = (numerator = 2, denominator = 1) => ({ chart: { result: [{ meta: { symbol: 'TEST' }, events: { splits: {
  split: { date: Date.parse(date + 'T14:30Z') / 1000, numerator, denominator, splitRatio: `${numerator}:${denominator}` },
} } }] } });

test('split metadata validates symbol, ratio and date and filters future events', async () => {
  assert.deepEqual(parseYahooSplitEvents(JSON.stringify(payload()), source, 'TEST'), [event(2)]);
  assert.equal(parseYahooSplitEvents(JSON.stringify(payload(1,10)), source, 'TEST')[0].ratio, .1);
  assert.throws(() => parseYahooSplitEvents(JSON.stringify(payload()), source, 'OTHER'), /symbol/);
  assert.throws(() => parseYahooSplitEvents(JSON.stringify(payload(2,0)), source, 'TEST'), /Invalid/);
  const invalid = payload(); invalid.chart.result[0].events.splits.split.splitRatio = '3:1';
  assert.throws(() => parseYahooSplitEvents(JSON.stringify(invalid), source, 'TEST'), /Inconsistent/);
  const events = await fetchYahooSplitEvents('TEST','2026-11-03','2026-11-04',async url => {
    assert.match(url,/events=splits/); return JSON.stringify(payload());
  });
  assert.deepEqual(events,[]);
});

test('any symbol supports forward, reverse and compounded splits; evidence persists for later retries', () => {
  for (const ratio of [2, 3, 1.5, .1]) {
    const cached = [{date:'2026-11-04',close:120,source:'cache'}];
    const incoming = [{date:'2026-11-04',close:120/ratio,source}, {date,close:126/ratio,source}];
    const aligned = alignClosePriceBasis('TEST',cached,incoming,date,[event(ratio)]);
    assert.ok(Math.abs(aligned[1].close-126)<1e-9);
    assert.deepEqual(aligned[1].basisAdjustment?.splitEvents,[event(ratio)]);
    const next = alignClosePriceBasis('TEST',[aligned[1]],[incoming[1],{date:'2026-11-06',close:132/ratio,source}],'2026-11-06');
    assert.ok(Math.abs(next[1].close-132)<1e-9);
    assert.equal(cached[0].close,120);
  }
  const actions = [event(2,'2026-10-14'),event(3)];
  const result = alignClosePriceBasis('TEST',[{date:'2026-10-13',close:120,source:'cache'}],
    [{date:'2026-10-13',close:20,source},{date,close:21,source}],date,actions);
  assert.equal(result[1].close,126);
  assert.deepEqual(result[1].basisAdjustment?.splitEvents,actions);
  assert.throws(() => alignClosePriceBasis('TEST',[{date:'2026-11-04',close:120,source:'cache'}],
    [{date:'2026-11-04',close:60,source}],date,[]), /Unverified/);
});

test('a 2:1 split cannot silently pass the generic 0.5–2 price guard', async () => {
  const raw = payload() as any;
  Object.assign(raw.chart.result[0],{timestamp:[Date.parse('2026-11-04T14:30Z')/1000,Date.parse(date+'T14:30Z')/1000],
    indicators:{quote:[{close:[200,100]}]}});
  const closes = await fetchGapCloses('TEST','etf','2026-11-04',date,['2026-11-04',date],async url => {
    if (url.includes('yahoo')) return JSON.stringify(raw);
    return JSON.stringify({data:{tradesTable:{rows:[{date:'11/04/2026',close:'100'},{date:'11/05/2026',close:'100'}]}}});
  },{preferYahoo:true});
  assert.deepEqual(closes.map(row=>row.close),[100,100]);
  assert.equal(closes.at(-1)?.splitEvents?.[0].ratio,2);
  assert.equal(coherentCloseWindow([{date:'2026-11-04',close:100,source},{date,close:1000,source}],[event(.1)]),false);
});
