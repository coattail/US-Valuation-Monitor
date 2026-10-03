import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {applySp500TtmPricePolicy,sp500TtmPriceCorrections,assertSp500TtmCoverage} from '../src/sp500-ttm-policy.ts';
import {parseSp500TtmCloses} from '../src/sp500-ttm-closes.ts';
import {assertPublishedIndexHistoryAppendOnly,loadAuthoritativePublishedMetricCorrections} from '../src/generate.ts';
const point=(date,pe_ttm)=>({date,pe_ttm,pe_forward:20,pb:4,us10y_yield:.04});
const quotes=[{date:'2026-05-08',pe_ttm:26.4,source:'wsj-latest'},{date:'2026-05-15',pe_ttm:25.7,source:'wsj-latest'}];
const closes=[['2026-05-08',100],['2026-05-11',101],['2026-05-15',102],['2026-05-18',103]]
  .map(([date,close])=>({date,close,source:'SPX-test'}));
const read=async name=>JSON.parse(await readFile(new URL(`../../../data/standardized/${name}`,import.meta.url),'utf8'));

test('TTM estimates stay on the latest dated earnings anchor instead of snapping to an old curve',()=>{
  const input=closes.map(row=>point(row.date,20));
  const output=applySp500TtmPricePolicy(input,quotes,closes);
  assert.deepEqual(output.map(row=>row.pe_ttm),[26.4,26.664,25.7,25.952]);
  assert.equal(output[1].pe_ttm_estimate.anchorDate,'2026-05-08');
  assert.equal(output[2].pe_ttm_estimate,undefined);
  for(const row of output) assert.deepEqual([row.pe_forward,row.pb,row.us10y_yield],[20,4,.04]);
  assert.deepEqual(applySp500TtmPricePolicy(output,quotes,closes),output);
  // A future observation does not interpolate earnings backward.
  assert.equal(applySp500TtmPricePolicy(input,quotes.slice(0,1),closes)[1].pe_ttm,output[1].pe_ttm);
  const unrelated={date:'2026-05-11',pe_ttm:99,source:'ssga-official-latest'};
  assert.deepEqual(applySp500TtmPricePolicy(input,[...quotes,unrelated],closes),output);
});

test('retained March quote repairs only the three mixed-source estimates',()=>{
  const input=[point('2026-03-19',25),point('2026-03-20',23.75),point('2026-03-23',26.2251),point('2026-03-26',24.6943)];
  const output=applySp500TtmPricePolicy(input,[],[{date:'2026-03-20',close:100,source:'SPX'}, {date:'2026-03-23',close:101,source:'SPX'}]);
  assert.equal(output[2].pe_ttm,23.9875);
  assert.deepEqual(output[0],input[0]);assert.deepEqual(output[1],input[1]);assert.deepEqual(output[3],input[3]);
});

test('missing closes cannot fabricate TTM estimates; observed quotes remain intact',()=>{
  const output=applySp500TtmPricePolicy(closes.map(row=>point(row.date,20)),quotes,[]);
  assert.equal(output[0].pe_ttm,26.4);assert.equal(output[1].pe_ttm,null);
  assert.throws(()=>assertSp500TtmCoverage(output),/2026-05-11/);
  assert.throws(()=>parseSp500TtmCloses(JSON.stringify({chart:{result:[{meta:{symbol:'SPY'}}]}}),'test'),/\^GSPC/);
  const raw={chart:{result:[{meta:{symbol:'^GSPC'},timestamp:[Date.parse('2026-05-08')/1000,Date.parse('2026-05-11')/1000],indicators:{quote:[{close:[100,null]}],adjclose:[{adjclose:[90,91]}]}}]}};
  assert.deepEqual(parseSp500TtmCloses(JSON.stringify(raw),'test').map(row=>row.close),[100]);
});

test('late observations update estimates with reproducible corrections while original quotes stay protected',()=>{
  const previous=applySp500TtmPricePolicy(closes.map(row=>point(row.date,20)),[quotes[0]],closes);
  const next=applySp500TtmPricePolicy(previous,quotes,closes);
  const allowed=new Map([['sp500',sp500TtmPriceCorrections(previous,quotes,closes)]]);
  assert.doesNotThrow(()=>assertPublishedIndexHistoryAppendOnly({indices:[{id:'sp500',points:previous}]},{indices:[{id:'sp500',points:next}]},allowed));
  const tampered=structuredClone(next);tampered[1].pe_ttm+=.1;
  assert.throws(()=>assertPublishedIndexHistoryAppendOnly({indices:[{id:'sp500',points:previous}]},{indices:[{id:'sp500',points:tampered}]},allowed),/published index history changed/);
});

test('committed TTM history and price cache reproduce every repair and retain every WSJ quote',async()=>{
  const [history,split,metrics,prices]=await Promise.all([read('valuation-history.json'),read('index-series/sp500.json'),read('index-yahoo-daily-metrics.json'),read('sp500-ttm-closes.json')]);
  const points=history.indices.find(row=>row.id==='sp500').points;
  assertSp500TtmCoverage(points);
  assert.deepEqual(points,applySp500TtmPricePolicy(points,metrics.symbols.SPY,prices.observations));
  assert.deepEqual(split.points,points);
  for(const row of metrics.symbols.SPY.filter(row=>row.source==='wsj-latest' && row.pe_ttm>0)) {
    assert.equal(points.find(p=>p.date===row.date).pe_ttm,row.pe_ttm);
  }
  assert.equal(points.find(row=>row.date==='2026-03-20').pe_ttm,23.75);
  assert.equal(points.find(row=>row.date==='2026-03-23').pe_ttm,24.022);
  const corrections=(await loadAuthoritativePublishedMetricCorrections()).get('sp500');
  for(const row of points.filter(row=>row.pe_ttm_estimate)) assert.equal(corrections.get(row.date).pe_ttm,row.pe_ttm);
});
