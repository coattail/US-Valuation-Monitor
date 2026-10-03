import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {refreshSp500TtmCloses} from '../packages/data-pipeline/src/sp500-ttm-closes.ts';
import {applySp500TtmPricePolicy,assertSp500TtmCoverage} from '../packages/data-pipeline/src/sp500-ttm-policy.ts';
import {assertDatasetMatchesIndexHistoryLock,buildIndexHistoryLock} from '../packages/data-pipeline/src/index-history-lock.ts';
const dir=new URL('../data/standardized/',import.meta.url);
const read=async name=>JSON.parse(await readFile(new URL(name,dir),'utf8'));
const write=async(name,value)=>writeFile(new URL(name,dir),JSON.stringify(value,null,2)+'\n');
const dataset=await read('valuation-history.json');
assertDatasetMatchesIndexHistoryLock(dataset,await read('index-history-lock.json'));
const before=structuredClone(dataset);
const sp500=dataset.indices.find(row=>row.id==='sp500');
const snapshots=(await read('index-yahoo-daily-metrics.json')).symbols.SPY || [];
const closes=await refreshSp500TtmCloses(sp500.points.at(-1).date);
sp500.points=applySp500TtmPricePolicy(sp500.points,snapshots,closes);
assertSp500TtmCoverage(sp500.points);
for(let n=0;n<dataset.indices.length;n++) {
  const next=dataset.indices[n],old=before.indices[n];
  if(next.id!=='sp500') {assert.deepEqual(next,old);continue;}
  for(let i=0;i<next.points.length;i++) {
    const {pe_ttm,pe_ttm_estimate,...rest}=next.points[i];
    const {pe_ttm:oldPe,pe_ttm_estimate:oldEstimate,...oldRest}=old.points[i];
    assert.deepEqual(rest,oldRest);
  }
}
const changes=sp500.points.flatMap((row,i)=>row.pe_ttm!==before.indices.find(row=>row.id==='sp500').points[i].pe_ttm
  ? [{date:row.date,before:before.indices.find(row=>row.id==='sp500').points[i].pe_ttm,after:row.pe_ttm}] : []);
console.log(JSON.stringify({changed:changes.length,examples:changes.slice(0,12)},null,2));
if(process.argv.includes('--write')) {
  dataset.generatedAt=new Date().toISOString();
  await write('valuation-history.json',dataset);
  await write('index-history-lock.json',buildIndexHistoryLock(dataset));
  const ledger=await read('index-gap-repairs.json');
  for(const repair of ledger.repairs.SPY || []) {
    repair.point=applySp500TtmPricePolicy([sp500.points.find(row=>row.date==='2026-03-20'),repair.point].filter(Boolean),snapshots,closes).at(-1);
  }
  await write('index-gap-repairs.json',ledger);
  console.log('Repaired TTM only; split-index-dataset.ts regenerates the derived files.');
} else console.log('Dry run; pass --write for the audited correction.');
