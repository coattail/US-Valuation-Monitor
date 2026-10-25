import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {reconcileSp500Pb} from './reconcile-sp500-pb.mjs';
import {buildIndexHistoryLock,assertDatasetMatchesIndexHistoryLock} from '../packages/data-pipeline/src/index-history-lock.ts';
import {SP500_BOOK_VALUE_URL} from '../packages/data-pipeline/src/sp500-pb-policy.ts';
async function setup() {
  const dir=await mkdtemp(path.join(os.tmpdir(),'sp500-pb-'));
  const points=[{date:'2026-10-22',pe_ttm:23,pe_forward:19,pb:6.08,us10y_yield:.04}];
  const dataset={generatedAt:'2026-10-24',source:'test',indices:[{id:'sp500',symbol:'SPY',points},{id:'dow30',symbol:'DIA',points:structuredClone(points)}]};
  const inputs={version:1,symbol:'^GSPC',bookValueSource:SP500_BOOK_VALUE_URL,bookValues:[{date:'2025-09-30',value:1000}],
    priceSources:['https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1d'],closes:[{date:'2026-10-22',close:6000}]};
  const save=async(name,data)=>writeFile(path.join(dir,name),JSON.stringify(data));
  const read=async(name)=>JSON.parse(await readFile(path.join(dir,name),'utf8'));
  await save('valuation-history.json',dataset);await save('index-history-lock.json',buildIndexHistoryLock(dataset));await save('sp500-pb-inputs.json',inputs);
  return {dir,dataset,inputs,save,read};
}
test('PB reconciliation changes only SP500 PB and metadata, protects lock and replays idempotently',async()=>{
  const f=await setup();
  try {
    assert.equal((await reconcileSp500Pb({dir:f.dir,write:true,refreshSources:false})).changed,1);
    const result=await f.read('valuation-history.json');
    assert.deepEqual(result.indices[1],f.dataset.indices[1]);
    assert.equal(result.indices[0].points[0].pb,6);
    assertDatasetMatchesIndexHistoryLock(result,await f.read('index-history-lock.json'));
    assert.equal((await reconcileSp500Pb({dir:f.dir,write:true,refreshSources:false})).changed,0);
    assert.deepEqual(await f.read('valuation-history.json'),result);
  } finally {await rm(f.dir,{recursive:true,force:true});}
});
test('an unavailable required close cannot partially publish a cache, history or lock',async()=>{
  const f=await setup();
  try {
    const dataset=structuredClone(f.dataset);dataset.indices[0].points.push({...dataset.indices[0].points[0],date:'2026-10-23'});
    await f.save('valuation-history.json',dataset);await f.save('index-history-lock.json',buildIndexHistoryLock(dataset));
    const lock=await f.read('index-history-lock.json');
    await assert.rejects(reconcileSp500Pb({dir:f.dir,write:true,refreshSources:false,fetchText:async()=>{throw Error('unavailable');}}),/Missing/);
    assert.deepEqual(await f.read('valuation-history.json'),dataset);
    assert.deepEqual(await f.read('index-history-lock.json'),lock);
    assert.deepEqual(await f.read('sp500-pb-inputs.json'),f.inputs);
  } finally {await rm(f.dir,{recursive:true,force:true});}
});

test('a newly reported book value updates the denominator without importing another providers PB',async()=>{
  const f=await setup();
  try {
    const html='<h1>S&amp;P 500 Book Value Per Share</h1><table><tr><td>Jun 30, 2026</td><td>1,200.00</td></tr></table>';
    await reconcileSp500Pb({dir:f.dir,write:true,fetchText:async url=>{assert.equal(url,SP500_BOOK_VALUE_URL);return html;}});
    const row=(await f.read('valuation-history.json')).indices[0].points[0];
    assert.equal(row.pb,5);
    assert.equal(row.pb_basis.bookValueDate,'2026-06-30');
    assert.equal((await f.read('sp500-pb-inputs.json')).bookValues.length,2);
  } finally {await rm(f.dir,{recursive:true,force:true});}
});
