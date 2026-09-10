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
