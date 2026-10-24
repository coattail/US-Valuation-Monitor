import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { repairEtfTtmHistory } from './repair-etf-ttm-history.mjs';
import { validateFreshness } from './validate-data-freshness.mjs';
import { buildIndexHistoryLock, assertDatasetMatchesIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

async function fixture(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'etf-ttm-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const save = async (name, value) => writeFile(path.join(dir, name), JSON.stringify(value));
  const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
  const dates = ['2026-07-16', '2026-07-17', '2026-07-20', '2026-07-21'];
  const points = dates.map(date => ({date,pe_ttm:30,pe_forward:20,pb:4,us10y_yield:.04}));
  const dataset = {generatedAt:'unchanged',source:'test',indices:[
    {id:'dram',symbol:'DRAM',points}, {id:'sp500',symbol:'SPY',points:structuredClone(points)},
  ]};
  await save('valuation-history.json', dataset);
  await save('index-history-lock.json', buildIndexHistoryLock(dataset));
  await save('index-yahoo-daily-metrics.json', {symbols:{DRAM:dates.map(date=>({date,pe_ttm:30,source:'stockanalysis-latest'}))}});
  await save('index-gap-repairs.json', {version:1,repairs:{DRAM:[{date:dates[2],point:points[2]}]}});
  const fetchCloses = async () => dates.map((date,i)=>({date,close:100+i*10,source:'actual-price'}));
  return {dir,read,save,dataset,fetchCloses};
}

test('publication repairs existing dates, persists reproducible closes/lock and recovery rows, and is idempotent', async t => {
  const f = await fixture(t);
  const raw = await f.read('index-yahoo-daily-metrics.json');
  const changes = await repairEtfTtmHistory({...f,write:true});
  assert.equal(changes.length, 3);
  const result = await f.read('valuation-history.json');
  assert.deepEqual(result.indices[0].points.map(row=>row.pe_ttm), [30,33,36,39]);
  assert.deepEqual(result.indices[1], f.dataset.indices[1]);
  assert.deepEqual(await f.read('index-yahoo-daily-metrics.json'), raw);
  assertDatasetMatchesIndexHistoryLock(result, await f.read('index-history-lock.json'));
  assert.equal((await f.read('index-gap-repairs.json')).repairs.DRAM[0].point.pe_ttm,36);
  assert.equal((await f.read('etf-ttm-closes.json')).symbols.DRAM.length,4);
  const failFetch = () => { throw new Error('Complete price cache must not refetch'); };
  assert.deepEqual(await repairEtfTtmHistory({dir:f.dir,write:true,fetchCloses:failFetch}),[]);
  assert.deepEqual(await f.read('valuation-history.json'),result);
  // A delayed, changed source snapshot must replace the earlier frozen estimate.
  raw.symbols.DRAM[3].pe_ttm = 24;
  await f.save('index-yahoo-daily-metrics.json',raw);
  await repairEtfTtmHistory({dir:f.dir,write:true,fetchCloses:failFetch});
  assert.equal((await f.read('valuation-history.json')).indices[0].points.at(-1).pe_ttm,24);
});

test('missing prices and a broken history lock block publication without rewriting the dataset', async t => {
  const f = await fixture(t);
  await assert.rejects(repairEtfTtmHistory({...f,write:true,fetchCloses:async()=>[]}),/Incomplete ETF closes/);
  assert.deepEqual(await f.read('valuation-history.json'),f.dataset);
  const broken = structuredClone(f.dataset);
  broken.indices[0].points[1].pe_ttm = 999;
  await f.save('valuation-history.json',broken);
  await assert.rejects(repairEtfTtmHistory({...f,write:true}),/checksum mismatch/);
  assert.deepEqual(await f.read('valuation-history.json'),broken);
});

test('the declared daily refresh commands all run ETF TTM reconciliation before splitting publication data', async () => {
  const {scripts} = JSON.parse(await readFile(new URL('../package.json',import.meta.url),'utf8'));
  for (const name of ['build:data:index','refresh:close:index','refresh:wsj:index']) {
    assert.match(scripts[name],/refresh:etf-ttm:index.*split-index-dataset/);
  }
});

test('freshness rejects complete daily dates with stale TTM values and accepts the reconciled publication', async t => {
  const f = await fixture(t);
  const seriesDir = path.join(f.dir,'index-series');
  await mkdir(seriesDir);
  await f.save('etf-ttm-closes.json',{version:1,symbols:{DRAM:await f.fetchCloses()}});
  await f.save('index-series/dram.json',f.dataset.indices[0]);
  for (let i=0;i<20;i++) await f.save(`index-series/other-${i}.json`,{symbol:`OTHER-${i}`,points:f.dataset.indices[1].points});
  await assert.rejects(validateFreshness('index','2026-07-21',{dataDir:f.dir}),/stale TTM/);
  await repairEtfTtmHistory({...f,write:true});
  await f.save('index-series/dram.json',(await f.read('valuation-history.json')).indices[0]);
  await validateFreshness('index','2026-07-21',{dataDir:f.dir});
});
