import test from 'node:test';
import assert from 'node:assert/strict';
import { applyWsjIndexPolicy, WSJ_INDEX_PRICES } from '../src/wsj-index-policy.ts';
import { indexMetricSource, mergeIndexSourceSnapshot } from '../src/index-source-policy.ts';

const dates = ['2026-10-15', '2026-10-16', '2026-10-19', '2026-10-20'];
const points = dates.map(date => ({date, pe_ttm:99, pe_forward:99, pb:4, us10y_yield:.04}));
const prices = dates.map((date,i) => ({date, close:100 + i * 10, source:'actual-index'}));
const quotes = [{date:dates[1], pe_ttm:30, pe_forward:20, source:'wsj-latest'}];

test('both multiples of all four indices reset to a delayed source quote and follow actual index prices thereafter', () => {
  for (const id of Object.keys(WSJ_INDEX_PRICES)) {
    const output = applyWsjIndexPolicy(points,id,quotes,prices);
    assert.deepEqual(output[0],points[0]);
    assert.equal(output[1].pe_ttm,30);
    assert.equal(output[1].pe_forward,20);
    assert.equal(output[1].pe_ttm_estimate,undefined);
    assert.equal(output[2].pe_ttm,32.7273);
    assert.equal(output[2].pe_forward,21.8182);
    for (const row of output.slice(2)) assert.equal(row.pe_ttm_estimate.priceSymbol,WSJ_INDEX_PRICES[id]);
    for (const row of output) assert.deepEqual([row.pb,row.us10y_yield],[4,.04]);
    assert.deepEqual(applyWsjIndexPolicy(output,id,quotes,prices),output);
    const revised = applyWsjIndexPolicy(output,id,[...quotes,{date:dates[3],pe_ttm:25,pe_forward:18,source:'wsj-latest'}],prices);
    assert.deepEqual(revised.slice(0,3),output.slice(0,3));
    assert.equal(revised[3].pe_ttm,25);
    assert.equal(revised[3].pe_forward,18);
    assert.equal(revised[3].pe_forward_estimate,undefined);
    assert.throws(() => applyWsjIndexPolicy(points,id,quotes,[]), /Missing .* WSJ close/);
  }
});

test('a PB-only update cannot change the WSJ PE sources or permit a lower-priority PE to replace them', () => {
  const source = {...quotes[0],pb:null};
  const mixed = mergeIndexSourceSnapshot(source,{date:dates[1],source:'ishares-official-latest',pe_ttm:40,pe_forward:35,pb:5});
  assert.deepEqual([mixed.pe_ttm,mixed.pe_forward,mixed.pb],[30,20,5]);
  assert.equal(indexMetricSource(mixed,'pe_ttm'),'wsj-latest');
  assert.equal(indexMetricSource(mixed,'pb'),'ishares-official-latest');
  assert.deepEqual(applyWsjIndexPolicy(points,'nasdaq100',[mixed],prices).map(row => row.pe_ttm), [99,30,32.7273,35.4545]);
  const corrected = mergeIndexSourceSnapshot(mixed,{date:dates[1],source:'wsj-latest',pe_ttm:29,pe_forward:19,pb:null});
  assert.equal(corrected.pb,5);
  assert.equal(indexMetricSource(corrected,'pb'),'ishares-official-latest');
  assert.equal(corrected.pe_ttm,29);
});

test('Nasdaq TTM retains its audited Nasdaq/Bloomberg history before the WSJ source cutover', () => {
  const historical = {date:'2026-05-08',pe_ttm:40,pe_forward:24.52};
  const result = applyWsjIndexPolicy([historical],'nasdaq100',[{date:historical.date,pe_ttm:35.65,pe_forward:24.52,source:'wsj-latest'}],[]);
  assert.equal(result[0].pe_ttm,40);
  assert.equal(result[0].pe_ttm_observation,undefined);
});
