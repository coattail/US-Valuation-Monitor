import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {applySp500PbPolicy,parseSp500BookValues,validateSp500PbInputs,SP500_BOOK_VALUE_URL,type Sp500PbInputs} from '../src/sp500-pb-policy.ts';
const fixture = (): Sp500PbInputs => ({version:1,symbol:'^GSPC',bookValueSource:SP500_BOOK_VALUE_URL,
  bookValues:[{date:'2025-12-31',value:100},{date:'2026-03-31',value:110}],
  priceSources:['https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1d'],
  closes:[{date:'2025-12-31',close:500},{date:'2026-03-30',close:600},{date:'2026-03-31',close:605},{date:'2026-04-01',close:610}]});
const point = (date:string) => ({date,pe_ttm:20,pe_forward:18,pb:999,us10y_yield:.04});
const read = async(name:string) => JSON.parse(await readFile(new URL(`../../../data/standardized/${name}`,import.meta.url),'utf8'));

test('PB uses actual index prices and changes its book denominator each quarter without future interpolation',()=>{
  const inputs=fixture(), before=inputs.closes.map(row=>point(row.date));
  const result=applySp500PbPolicy(before,inputs);
  assert.deepEqual(result.map(row=>row.pb),[5,6,5.5,5.545455]);
  assert.equal(result[1].pb_basis.bookValueDate,'2025-12-31');
  assert.equal(result[2].pb_basis.bookValueDate,'2026-03-31');
  for(let i=0;i<before.length;i++) {
    const {pb,pb_basis,...other}=result[i];
    const {pb:oldPb,...oldOther}=before[i];
    assert.deepEqual(other,oldOther);
  }
  assert.deepEqual(applySp500PbPolicy(result,inputs),result);
});

test('missing or wrong price inputs fail instead of reusing ETF carries or another provider PB',()=>{
  const inputs=fixture();
  assert.throws(()=>applySp500PbPolicy([point('2026-04-02')],inputs),/Missing.*2026-04-02/);
  inputs.symbol='SPY' as '^GSPC';
  assert.throws(()=>validateSp500PbInputs(inputs),/Invalid/);
  inputs.symbol='^GSPC';inputs.priceSources=['https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1d'];
  assert.throws(()=>validateSp500PbInputs(inputs),/index prices/);
});

test('book-value parser preserves thousands and ignores estimates and unrelated numeric rows',()=>{
  const html='<h1>S&amp;P 500 Book Value Per Share by Quarter</h1><table>'+[
    '<tr><td>Sep 30, 2025</td><td>&#x2002;1,270.32</td></tr>',
    '<tr><td>Dec 31, 2025</td><td>† (Estimate) 1,300.00</td></tr>',
    '<tr><td>Jun 30, 2025</td><td>1,238.93</td></tr>',
  ].join('')+'</table>';
  assert.deepEqual(parseSp500BookValues(html),[{date:'2025-06-30',value:1238.93},{date:'2025-09-30',value:1270.32}]);
  assert.throws(()=>parseSp500BookValues(html.replace('Book Value','Price to Book')),/Expected/);
});

test('weekend quarter end refers to that quarters final market close',()=>{
  const inputs=fixture();inputs.bookValues=[{date:'2025-12-31',value:100},{date:'2026-02-28',value:120}];
  inputs.closes=[{date:'2026-02-26',close:600},{date:'2026-02-27',close:600},{date:'2026-03-02',close:600}];
  const result=applySp500PbPolicy(inputs.closes.map(row=>point(row.date)),inputs);
  assert.deepEqual(result.map(row=>row.pb),[6,5,5]);
});

test('published PB is fully reproducible and the September one-day source switch is removed',async()=>{
  const [history,split,inputs,ledger]=await Promise.all(['valuation-history.json','index-series/sp500.json','sp500-pb-inputs.json','index-gap-repairs.json'].map(read));
  const points=history.indices.find(row=>row.id==='sp500').points;
  assert.deepEqual(points,applySp500PbPolicy(points,inputs));
  assert.deepEqual(split.points,points);
  assert.equal(points.at(-1).pb,Number((inputs.closes.at(-1).close/inputs.bookValues.at(-1).value).toFixed(6)));
  assert.equal(points.at(-1).pb_basis.bookValueDate,inputs.bookValues.at(-1).date);
  const before=points.find(row=>row.date==='2026-09-03'),after=points.find(row=>row.date==='2026-09-04');
  const prices=new Map(inputs.closes.map(row=>[row.date,row.close]));
  assert.ok(Math.abs(after.pb/before.pb-prices.get(after.date)/prices.get(before.date))<1e-6);
  for(const row of ledger.repairs.SPY || []) assert.deepEqual(row.point,points.find(point=>point.date===row.date));
});
