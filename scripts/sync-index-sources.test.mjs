import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { syncIndexSources } from './sync-index-sources.mjs';
import { repairEtfTtmHistory } from './repair-etf-ttm-history.mjs';
import { reconcileIndexForward } from './reconcile-index-forward.mjs';
import { buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

async function fixture(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(),'index-source-'));
  t.after(() => rm(dir,{recursive:true,force:true}));
  const dates = ['2026-07-16','2026-07-17','2026-07-20','2026-07-21'];
  const dataset = {generatedAt:'old',source:'test',indices:[{id:'soxx',symbol:'SOXX',points:dates.map(date => ({date,pe_ttm:30,pe_forward:20,pb:4,us10y_yield:.04}))}]};
  const metrics = {symbols:{SOXX:[{date:dates[0],pe_ttm:30,pe_forward:20,pb:4,source:'ishares-official-latest'}]}};
  const save = (name,value) => writeFile(path.join(dir,name),JSON.stringify(value));
  const read = async name => JSON.parse(await readFile(path.join(dir,name),'utf8'));
  await save('valuation-history.json',dataset);
  await save('index-history-lock.json',buildIndexHistoryLock(dataset));
  await save('index-yahoo-daily-metrics.json',metrics);
  const fetchCloses = async symbol => {assert.equal(symbol,'SOXX');return dates.map((date,i) => ({date,close:100 + 10*i,source:'SOXX actual close'}));};
  return {dir,dates,dataset,metrics,read,save,fetchCloses,now:new Date('2026-07-22T10:00Z')};
}

test('complete ETF dates still receive delayed source updates; source anchors replace estimates for both metrics',async t => {
  const f = await fixture(t);
  const quote = {date:f.dates[2],source:'ishares-official-latest',pe_ttm:24,pe_forward:15,pb:3};
  const result = await syncIndexSources({...f,fetchOfficial:async()=>quote});
  assert.equal(result.changed,true);
  await repairEtfTtmHistory({...f,write:true});
  await reconcileIndexForward({...f,write:true});
  const points = (await f.read('valuation-history.json')).indices[0].points;
  assert.deepEqual(points.map(row=>row.pe_ttm),[30,33,24,26]);
  assert.deepEqual(points.map(row=>row.pe_forward),[20,22,15,16.25]);
  assert.deepEqual(points.map(row=>row.pb),[4,4.4,3,3.25]);
  assert.equal((await syncIndexSources({...f,fetchOfficial:async()=>quote})).changed,false);
  assert.equal((await repairEtfTtmHistory({...f,write:true})).length,0);
  assert.equal((await reconcileIndexForward({...f,write:true})).changes.length,0);
  // Even an equal multiple on a NEW source date is a real source update.
  await syncIndexSources({...f,fetchOfficial:async()=>({...quote,date:f.dates[3]})});
  await repairEtfTtmHistory({...f,write:true});
  assert.equal((await f.read('valuation-history.json')).indices[0].points.at(-1).pe_ttm,24);
});

test('unavailable or future-dated providers cannot manufacture a new current-day anchor',async t => {
  const f = await fixture(t);
  const before = await readFile(path.join(f.dir,'index-yahoo-daily-metrics.json'),'utf8');
  await syncIndexSources({...f,fetchOfficial:async()=>{throw new Error('source unavailable');}});
  assert.equal(await readFile(path.join(f.dir,'index-yahoo-daily-metrics.json'),'utf8'),before);
  await syncIndexSources({...f,fetchOfficial:async()=>({date:'2026-07-22',pe_ttm:99,source:'ishares-official-latest'}),fetchUndated:async()=>({})});
  assert.equal(await readFile(path.join(f.dir,'index-yahoo-daily-metrics.json'),'utf8'),before);
  await repairEtfTtmHistory({...f,write:true});
  assert.equal((await f.read('valuation-history.json')).indices[0].points.at(-1).pe_ttm,39);
});
