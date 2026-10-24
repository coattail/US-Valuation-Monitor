import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { reconstructDramTtm } from '../src/dram-ttm-reconstruction.ts';

const read = async (name:string)=>JSON.parse(await readFile(new URL('../../../data/standardized/'+name,import.meta.url),'utf8'));
test('DRAM uses recorded PE anchors and actual prices, retains endpoints and eliminates the October basis jump',async()=>{
  const snapshots = (await read('index-yahoo-daily-metrics.json')).symbols.DRAM;
  const closes = (await read('etf-ttm-closes.json')).symbols.DRAM;
  const series = await read('index-series/dram.json');
  const before = structuredClone(series.points);
  const result = reconstructDramTtm(before,snapshots,closes);
  const p13 = result.find((p:any)=>p.date==='2026-10-13'), p14 = result.find((p:any)=>p.date==='2026-10-14');
  assert.equal(p13.pe_ttm,25.3038);
  assert.equal(p14.pe_ttm,23.95);
  assert.ok(Math.abs(p14.pe_ttm/p13.pe_ttm-1)<.06);
  assert.equal(p13.pe_ttm_estimate.usesFutureAnchor,true);
  assert.equal(p13.pe_ttm_estimate.endAnchorDate,'2026-10-14');
  assert.deepEqual(reconstructDramTtm(result,snapshots,closes),result);
  for(let i=0;i<before.length;i++) {
    assert.equal(result[i].pe_forward,before[i].pe_forward);
    assert.equal(result[i].pb,before[i].pb);
    if(before[i].date <= '2026-07-16' || before[i].date >= '2026-10-14') assert.deepEqual(result[i],before[i]);
  }
  assert.throws(()=>reconstructDramTtm(before,snapshots.filter((r:any)=>r.date!=='2026-10-14'),closes),/Unverified/);
  assert.throws(()=>reconstructDramTtm(before,snapshots,closes.filter((r:any)=>r.date!=='2026-10-13')),/Missing DRAM/);
});
