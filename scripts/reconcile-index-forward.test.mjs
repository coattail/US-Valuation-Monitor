import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { reconcileIndexForward } from './reconcile-index-forward.mjs';
import { buildIndexHistoryLock,assertDatasetMatchesIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

async function fixture(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(),'index-forward-'));
  t.after(()=>rm(dir,{recursive:true,force:true}));
  const dates = ['2026-04-16','2026-04-17','2026-04-20','2026-04-21','2026-04-22'];
  const points = dates.map(date=>({date,pe_ttm:23,pe_forward:20,pb:4,us10y_yield:.04}));
  const dataset = {generatedAt:'old',source:'test',indices:[{id:'dow30',symbol:'DIA',points}]};
  const read = async name=>JSON.parse(await readFile(path.join(dir,name),'utf8'));
  const save = async (name,value)=>writeFile(path.join(dir,name),JSON.stringify(value));
  await save('valuation-history.json',dataset);
  await save('index-history-lock.json',buildIndexHistoryLock(dataset));
  await save('index-yahoo-daily-metrics.json',{symbols:{DIA:dates.map(date=>({date,pe_ttm:23,pe_forward:20,source:'wsj-latest'}))}});
  await save('index-gap-repairs.json',{repairs:{DIA:[{date:dates[2],point:points[2]}]}});
  const fetchCloses = async symbol=>{ assert.equal(symbol,'^DJI');return dates.map((date,i)=>({date,close:100+i,source:'actual-DJI'})); };
  return {dir,dates,read,save,dataset,fetchCloses};
}

test('complete dates are reconciled using actual index closes, with atomic failure and idempotent cached retries',async t=>{
  const f = await fixture(t);
  await assert.rejects(reconcileIndexForward({...f,write:true,fetchCloses:async()=>[]}),/Incomplete.*DJI/);
  assert.deepEqual(await f.read('valuation-history.json'),f.dataset);
  const result = await reconcileIndexForward({...f,write:true});
  assert.equal(result.changes.length,3);
  const after = await f.read('valuation-history.json');
  assertDatasetMatchesIndexHistoryLock(after,await f.read('index-history-lock.json'));
  assert.equal(after.indices[0].points[2].pe_forward,Number((20*102/101).toFixed(4)));
  assert.equal((await f.read('index-gap-repairs.json')).repairs.DIA[0].point.pe_forward,after.indices[0].points[2].pe_forward);
  const noFetch = ()=>{throw new Error('Cache complete; no fetch expected');};
  assert.equal((await reconcileIndexForward({dir:f.dir,write:true,fetchCloses:noFetch})).changes.length,0);
  assert.deepEqual(await f.read('valuation-history.json'),after);
  const cache = await f.read('index-yahoo-daily-metrics.json');
  cache.symbols.DIA.at(-1).pe_forward = 19;
  await f.save('index-yahoo-daily-metrics.json',cache);
  await reconcileIndexForward({dir:f.dir,write:true,fetchCloses:noFetch});
  assert.equal((await f.read('valuation-history.json')).indices[0].points.at(-1).pe_forward,19);
});

test('official FY1 above trailing PE is adopted on its actual date and does not rebase prior estimates',async t=>{
  const f = await fixture(t);
  const dataset = f.dataset;
  dataset.indices[0] = {...dataset.indices[0],id:'sector_real_estate',symbol:'XLRE'};
  await f.save('valuation-history.json',dataset);
  await f.save('index-history-lock.json',buildIndexHistoryLock(dataset));
  await f.save('index-yahoo-daily-metrics.json',{symbols:{XLRE:[]}});
  const fetchCloses = async symbol=>{assert.equal(symbol,'XLRE');return f.dates.map((date,i)=>({date,close:100+i,source:'actual-ETF'}));};
  await reconcileIndexForward({dir:f.dir,write:true,refreshSources:true,fetchCloses,
    fetchSnapshot:async()=>({date:f.dates.at(-1),pe_forward:33.9,pe_ttm:31.63,source:'ssga-official-latest'})});
  const after = (await f.read('valuation-history.json')).indices[0].points;
  assert.equal(after.at(-1).pe_forward,33.9);
  assert.equal(after.at(-2).pe_forward,20.6);
  assert.equal(after.at(-1).pe_ttm,23);
});
