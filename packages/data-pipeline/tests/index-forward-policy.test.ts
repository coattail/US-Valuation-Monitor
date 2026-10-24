import test from 'node:test';
import assert from 'node:assert/strict';
import { applyIndexForwardPolicy, indexForwardAnchors, forwardPlan, findFlatMetricRuns } from '../src/index-forward-policy.ts';

const dates = ['2026-10-16','2026-10-19','2026-10-20','2026-10-21','2026-10-22','2026-10-23'];
const points = dates.map(date=>({date,pe_forward:19.78,pe_ttm:21,pb:4,us10y_yield:.04}));
const closes = dates.map((date,i)=>({date,close:50000+i*100,source:'verified-DJI-close'}));
const inputs = {priceSymbol:'^DJI',startDate:dates[0],seed:{date:dates[0],value:19.78,source:'previous-published-estimate'},closes,observations:[]};
const snapshots = dates.map(date=>({date,pe_forward:19.78,source:'wsj-latest'}));

test('weekly WSJ forward field uses actual DJIA closes between quotes despite daily TTM table dates',()=>{
  const result = applyIndexForwardPolicy(points,snapshots,inputs,true);
  assert.deepEqual(indexForwardAnchors(snapshots,inputs,true).map(row=>row.date),[dates[0],dates[5]]);
  assert.equal(result[1].pe_forward,Number((19.78*50100/50000).toFixed(4)));
  assert.equal(result[4].pe_forward_estimate?.priceSymbol,'^DJI');
  assert.equal(result[5].pe_forward,19.78); // Equal next-Friday quote is still a new observation.
  assert.equal(result[5].pe_forward_estimate,undefined);
  assert.deepEqual(findFlatMetricRuns(result).filter(row=>row.metric==='pe_forward'),[]);
  for(let i=0;i<points.length;i++) assert.equal(result[i].pe_ttm,points[i].pe_ttm);
});

test('new and delayed quotes replace existing estimates without borrowing a future quote',()=>{
  const raw = [...snapshots,{date:dates[3],pe_forward:18,source:'wsj-latest'}];
  // Supply one observation per date, as the persisted source log does.
  const unique = [...new Map(raw.map(row=>[row.date,row])).values()];
  const result = applyIndexForwardPolicy(points,unique,inputs,true);
  assert.equal(result[2].pe_forward,Number((19.78*50200/50000).toFixed(4)));
  assert.equal(result[3].pe_forward,18);
  assert.equal(result[3].pe_forward_estimate,undefined);
});

test('official forward PE may exceed TTM, while duplicate undated snapshots are not new anchors',()=>{
  const raw = [
    {date:dates[0],pe_forward:30,source:'stockanalysis-latest'},
    {date:dates[1],pe_forward:30,source:'stockanalysis-latest'},
    {date:dates[2],pe_forward:33.9,source:'ssga-official-latest'},
    {date:dates[3],pe_forward:33.9,source:'ssga-official-latest'},
  ];
  const result = applyIndexForwardPolicy(points,raw,inputs);
  assert.equal(result[1].pe_forward,30.06);
  assert.equal(result[2].pe_forward,33.9);
  assert.equal(result[3].pe_forward,33.9); // Two dated daily quotes remain intact.
  assert.equal(result[2].pe_ttm,21);
  assert.throws(()=>applyIndexForwardPolicy(points,raw,{...inputs,closes:[]}),/Missing.*forward close/);
});

test('legacy seed is explicitly estimated; source cache and outputs are idempotent',()=>{
  const result = applyIndexForwardPolicy(points,[],inputs);
  assert.equal(result[0].pe_forward_estimate?.anchorSource,'previous-published-estimate');
  assert.deepEqual(applyIndexForwardPolicy(result,[],inputs),result);
  assert.equal(forwardPlan({id:'dow30',symbol:'DIA'})?.priceSymbol,'^DJI');
  assert.equal(forwardPlan({id:'nasdaq100',symbol:'QQQ'}),undefined);
});

test('a later official correction for an already published date supersedes the older cached observation',()=>{
  const cached = {...inputs,observations:[{date:dates[0],value:33.9,source:'ssga-official-latest',capturedAt:'2026-10-17T00:00Z'}]};
  const raw = [{date:dates[0],pe_forward:34,source:'ssga-official-latest',capturedAt:'2026-10-18T00:00Z'}];
  assert.equal(applyIndexForwardPolicy(points,raw,cached)[0].pe_forward,34);
});
