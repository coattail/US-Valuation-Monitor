import test from 'node:test';
import assert from 'node:assert/strict';
import { parseYahooCloses, parseChartExchangeCloses, repairMissingPoints, assertExistingPointsUnchanged, fetchGapCloses } from '../src/gap-repair.ts';
import { lastCompletedSession, isTradingDate, tradingDates } from '../src/market-calendar.ts';
import { findMissingSessions } from '../../../scripts/validate-data-freshness.mjs';

test('calendar excludes observed holidays and respects New York close and DST', () => {
  for (const date of ['2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25','2025-01-09']) assert.equal(isTradingDate(date), false, date);
  for (const date of ['2026-11-27','2026-10-12','2027-12-31','2026-09-22']) assert.equal(isTradingDate(date), true, date);
  assert.equal(lastCompletedSession(new Date('2026-09-24T03:00:00Z')), '2026-09-23');
  assert.equal(lastCompletedSession(new Date('2026-09-22T19:59:00Z')), '2026-09-21');
  assert.equal(lastCompletedSession(new Date('2026-09-22T20:01:00Z')), '2026-09-22');
  assert.equal(lastCompletedSession(new Date('2026-12-28T20:59:00Z')), '2026-12-24');
  assert.deepEqual(tradingDates('2026-09-04','2026-09-08'), ['2026-09-04','2026-09-08']);
});
test('detects interior gaps even when latest date is current', () => {
  assert.deepEqual(findMissingSessions({points:[{date:'2026-09-18'},{date:'2026-09-21'},{date:'2026-09-23'}]}, '2026-09-23'), ['2026-09-22']);
});
test('Yahoo null is never a zero close; OTC close column is selected', () => {
  const raw = JSON.stringify({chart:{result:[{timestamp:[1790078400,1790164800],indicators:{quote:[{close:[55.85,null]}]}}]}});
  assert.equal(parseYahooCloses(raw,'yahoo').length, 1);
  assert.deepEqual(parseChartExchangeCloses('<tr><td><a>2026-09-22</a></td><td>57.78</td><td>58.19</td><td>57.41</td><td>57.91</td></tr>','otc'), [{date:'2026-09-22',close:57.91,source:'otc'}]);
});
test('repairs only missing rows from observed prices and records estimation', () => {
  const before = [{date:'2026-09-21',pe_ttm:20,pe_forward:null,pb:4,us10y_yield:.04},{date:'2026-09-23',pe_ttm:21,pe_forward:null,pb:4.2,us10y_yield:.041}];
  const prices = [{date:'2026-09-21',close:100,source:'nasdaq'},{date:'2026-09-22',close:105,source:'nasdaq'}];
  const result = repairMissingPoints(before,['2026-09-21','2026-09-22','2026-09-23'],prices,new Map([['2026-09-22',.041]]));
  assert.equal(result.repairs.length,1);
  assert.equal(result.points[1].pe_ttm,21);
  assert.equal(result.points[1].pe_forward,null);
  assert.equal((result.points[1].recovery as any).estimated,true);
  assert.deepEqual(result.points[0],before[0]); assert.deepEqual(result.points[2],before[1]);
  assert.equal(repairMissingPoints(result.points,['2026-09-22'],prices,new Map()).repairs.length,0);
  assert.throws(()=>assertExistingPointsUnchanged(before,[{...before[0],pe_ttm:99},before[1]]));
});
test('no price, yield, valid scale, or recent anchor means no fabricated row', () => {
  const before = [{date:'2026-09-01',pe_ttm:20,us10y_yield:.04}];
  const prices = [{date:'2026-09-01',close:100,source:'a'},{date:'2026-09-22',close:105,source:'a'}];
  assert.equal(repairMissingPoints(before,['2026-09-22'],prices,new Map([['2026-09-22',.04]])).repairs.length,0);
  assert.equal(repairMissingPoints([{...before[0],date:'2026-09-21'}],['2026-09-22'],prices,new Map()).repairs.length,0);
});
test('fallback fills an interior null despite a newer primary observation', async () => {
  const calls: string[] = [];
  const result = await fetchGapCloses('TCEHY','stocks','2026-09-18','2026-09-23',['2026-09-22'],async url => {
    calls.push(url);
    if (url.includes('nasdaq')) return JSON.stringify({status:{rCode:400}});
    if (url.includes('yahoo')) return JSON.stringify({chart:{result:[{timestamp:[1790164800],indicators:{quote:[{close:[null]}]}}]}});
    return '<tr><td>2026-09-22</td><td>57.78</td><td>58.19</td><td>57.41</td><td>57.91</td></tr>';
  });
  assert.equal(calls.length,3); assert.equal(result[0].close,57.91);
});
