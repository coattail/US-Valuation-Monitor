import test from 'node:test';
import assert from 'node:assert/strict';
import { repairCompanyMetricHistory, preserveSpcxUnavailableMetrics } from '../packages/data-pipeline/src/company-data-corrections.mjs';
test('SpaceX repair removes borrowed PE and PEG but keeps legitimate forward PE and PB',()=>{
 const rows=[{date:'2026-09-08',pe_ttm:200,pe_forward:200,pb:15.33,peg:68.3}];
 assert.deepEqual(repairCompanyMetricHistory('SPCX',rows),[{...rows[0],pe_ttm:null,peg:null}]);
 assert.equal(repairCompanyMetricHistory('NVDA',rows),rows);
});
test('SpaceX future explicit unavailable metrics cannot be backfilled; later valid earnings are allowed',()=>{
 const item={symbol:'SPCX',points:[{date:'2026-09-10',pe_ttm:200,peg:68},{date:'2027-02-01',pe_ttm:200,peg:68}]};
 const result=preserveSpcxUnavailableMetrics(item,[{date:'2026-09-10',pe_ttm:null,peg:null},{date:'2027-02-01',pe_ttm:150,peg:2}]);
 assert.equal(result.points[0].pe_ttm,null);
 assert.equal(result.points[1].pe_ttm,150);
 assert.equal(result.peg,2);
});

test('NVDA keeps direct Forward PE and leaves an unsupported date empty', () => {
  const [friday, monday, tuesday, wednesday] = repairCompanyMetricHistory('NVDA', [
    { date: '2026-11-20', pe_ttm: 13.48, pb: 13.67, pe_forward: 9.24 },
    { date: '2026-11-23', pe_ttm: 16.34, pb: 11.37, pe_forward: 9.88 },
    { date: '2026-11-24', pe_forward: 9.35 },
    { date: '2026-11-25', pe_forward: 8.99 },
  ]);
  assert.deepEqual([friday.pe_ttm, friday.pb, friday.pe_forward], [13.48, 11.35, 7.9302]);
  assert.deepEqual([monday.pe_ttm, monday.pb, monday.pe_forward], [17.73, 14.92, 9.30]);
  assert.equal(monday.valuationSources.pb.close, 165.05);
  assert.equal(monday.valuationSources.pb.denominator, 11.06);
  assert.equal(tuesday.pe_forward, null);
  assert.equal(wednesday.pe_forward, 8.991117);
});
