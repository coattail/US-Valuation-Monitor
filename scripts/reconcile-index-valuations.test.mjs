import test from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { reconcileIndexValuations, validateIndexPublication, loadIndexPublicationInputs } from './reconcile-index-valuations.mjs';

test('split-day price extension queries overlap and preserves cached SOXX prices', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(),'index-split-'));
  t.after(() => rm(dir,{recursive:true,force:true}));
  const source = new URL('../data/standardized/',import.meta.url);
  await Promise.all(['sp500-ttm-closes.json','nasdaq100-forward-closes.json','index-forward-inputs.json','etf-ttm-closes.json','sp500-pb-inputs.json']
    .map(name => cp(new URL(name,source),path.join(dir,name))));
  const read = async name => JSON.parse(await readFile(path.join(dir,name),'utf8'));
  const etf = await read('etf-ttm-closes.json'), forward = await read('index-forward-inputs.json');
  // Reproduce the retained pre-split cache independently of future data refreshes.
  const cached = [{ date:'2026-11-04',close:450.4100036621094,source:'cached-pre-split' }];
  etf.symbols.SOXX = cached;
  forward.indices.soxx = { priceSymbol:'SOXX', startDate:'2026-11-04',
    seed:{date:'2026-11-04',value:13.47,source:'previous-published-estimate'}, observations:[], closes:cached };
  await writeFile(path.join(dir,'etf-ttm-closes.json'),JSON.stringify(etf));
  await writeFile(path.join(dir,'index-forward-inputs.json'),JSON.stringify(forward));
  const result = await loadIndexPublicationInputs(dir, {
    refresh:true, dataset:{indices:[{id:'soxx',symbol:'SOXX',points:[{date:'2026-11-04'},{date:'2026-11-05'}]}]},
    metrics:{symbols:{SOXX:[{date:'2026-11-04',pe_ttm:42.73,source:'ishares-official-latest'}]}},
    fetchCloses:async (symbol,asset,from,to,required) => {
      assert.equal(symbol,'SOXX'); assert.equal(from,'2026-11-04');
      assert.deepEqual(required,['2026-11-04','2026-11-05']);
      return [{date:'2026-11-04',close:150.1367,source:'nasdaq'}, {date:'2026-11-05',close:131.9,source:'nasdaq'}];
    },
  });
  const closes = result.files['etf-ttm-closes.json'].symbols.SOXX;
  assert.deepEqual(closes[0],cached[0]);
  assert.ok(Math.abs(closes[1].close - 395.7) < 1e-9);
  assert.equal(closes[1].basisAdjustment.multiplier,3);
  assert.deepEqual(result.files['index-forward-inputs.json'].indices.soxx.closes,closes);
});

test('all 21 published series are reproducible; stale snapshots, series and incomplete prices block publication', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(),'index-publication-'));
  t.after(() => rm(dir,{recursive:true,force:true}));
  const source = new URL('../data/standardized/',import.meta.url);
  await Promise.all(['valuation-history.json','valuation-snapshot.json','index-series','index-history-lock.json','index-yahoo-daily-metrics.json',
    'sp500-ttm-closes.json','nasdaq100-forward-closes.json','index-forward-inputs.json','etf-ttm-closes.json','sp500-pb-inputs.json'].map(name => cp(new URL(name,source),path.join(dir,name),{recursive:true})));
  const read = async name => JSON.parse(await readFile(path.join(dir,name),'utf8'));
  const save = (name,value) => writeFile(path.join(dir,name),JSON.stringify(value));
  await validateIndexPublication(dir);
  const original = await readFile(path.join(dir,'valuation-history.json'),'utf8');
  const retry = await reconcileIndexValuations({dir,write:true,fetchCloses:async()=>{throw new Error('Complete cache must not refetch');}});
  assert.equal(retry.changes.length,0);
  assert.equal(await readFile(path.join(dir,'valuation-history.json'),'utf8'),original);
  const snapshot = await read('valuation-snapshot.json'), changed = structuredClone(snapshot);
  changed.indices.find(row=>row.id==='nasdaq100').pe_ttm += 1;
  await save('valuation-snapshot.json',changed);
  await assert.rejects(validateIndexPublication(dir),/Stale snapshot nasdaq100/);
  await save('valuation-snapshot.json',snapshot);
  const series = await read('index-series/sector_technology.json');
  series.points.at(-1).pb += 1;
  await save('index-series/sector_technology.json',series);
  await assert.rejects(validateIndexPublication(dir),/Stale series: sector_technology/);
  const prices = await read('nasdaq100-forward-closes.json');
  prices.observations.pop();
  await save('nasdaq100-forward-closes.json',prices);
  await assert.rejects(reconcileIndexValuations({dir,write:true,fetchCloses:async()=>[]}),/Incomplete \^NDX WSJ prices/);
  assert.equal(await readFile(path.join(dir,'valuation-history.json'),'utf8'),original);
});
