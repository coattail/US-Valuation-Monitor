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
  assert.deepEqual([friday.peg, monday.peg, tuesday.peg, wednesday.peg], [0.1519, 0.19, null, 0.19]);
});


test('Micron PE uses the available trailing earnings on the five affected sessions', () => {
  const rows = [
    { date: '2023-02-27', pe_ttm: 10.4981 },
    { date: '2023-02-28', pe_ttm: 41.4926 },
    { date: '2023-03-01', pe_ttm: 41.1482, pe_forward: 206.3284, pb: 1.3281 },
    { date: '2023-03-02', pe_ttm: 40.4952 },
    { date: '2023-03-03', pe_ttm: 40.7463 },
    { date: '2023-03-06', pe_ttm: 40.7822 },
    { date: '2023-03-07', pe_ttm: 10.047016 },
  ];
  const repaired = repairCompanyMetricHistory('MU', rows);
  assert.deepEqual(repaired.map(row => Number(row.pe_ttm.toFixed(2))), [10.5, 10.49, 10.41, 10.24, 10.3, 10.31, 10.05]);
  assert.equal(repaired[2].valuationSources.pe_ttm.close, 57.34);
  assert.equal(repaired[2].valuationSources.pe_ttm.denominator, 5.51);
  assert.equal(repaired[2].pe_forward, rows[2].pe_forward);
  assert.equal(repaired[2].pb, rows[2].pb);
  assert.equal(repaired[0], rows[0]);
  assert.equal(repaired.at(-1), rows.at(-1));
  assert.deepEqual(repairCompanyMetricHistory('MU', repaired), repaired);
  const yahooRow = { ...rows[2], capturedAt: '2023-03-02T01:00:00Z' };
  assert.equal(repairCompanyMetricHistory('MU', [yahooRow])[0], yahooRow);
});
