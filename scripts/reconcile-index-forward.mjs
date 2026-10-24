import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { forwardPlan, applyIndexForwardPolicy, findFlatMetricRuns } from '../packages/data-pipeline/src/index-forward-policy.ts';
import { fetchOfficialIndexLatestSnapshot } from '../packages/data-pipeline/src/generate.ts';
import { fetchGapCloses, recoverWithBudget } from '../packages/data-pipeline/src/gap-repair.ts';
import { tradingDates, isTradingDate } from '../packages/data-pipeline/src/market-calendar.ts';
import { buildIndexHistoryLock, assertDatasetMatchesIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

const defaultDir = fileURLToPath(new URL('../data/standardized/', import.meta.url));
export async function reconcileIndexForward({ dir = defaultDir, write = false, refreshSources = false,
  fetchSnapshot = fetchOfficialIndexLatestSnapshot,
  fetchCloses = (...args) => fetchGapCloses(...args, undefined, {preferYahoo:true}),
} = {}) {
  const read = async name => JSON.parse(await readFile(path.join(dir, name),'utf8'));
  const save = async (name, data) => writeFile(path.join(dir,name), JSON.stringify(data,null,2)+'\n');
  const dataset = await read('valuation-history.json');
  assertDatasetMatchesIndexHistoryLock(dataset, await read('index-history-lock.json'));
  const metrics = await read('index-yahoo-daily-metrics.json');
  let cache, ledger;
  try { cache = await read('index-forward-inputs.json'); } catch(error) {
    if(error.code !== 'ENOENT') throw error;
    cache = { version:1, indices:{} };
  }
  if(cache.version !== 1 || !cache.indices) throw new Error('Invalid forward input cache');
  try { ledger = await read('index-gap-repairs.json'); } catch(error) { if(error.code !== 'ENOENT') throw error; }
  const changes = [], errors = [];
  const targets = dataset.indices.filter(row => forwardPlan(row));
  const skipped = await recoverWithBudget(targets, async item => {
    try {
      const plan = forwardPlan(item), snapshots = metrics.symbols[item.symbol] || [];
      const seedPoint = item.points.findLast(row => row.date <= plan.startDate && Number.isFinite(row.pe_forward) && row.pe_forward > 0);
      if(!seedPoint) throw new Error(`Missing published forward seed: ${item.id}`);
      const inputs = cache.indices[item.id] || { startDate:plan.startDate, priceSymbol:plan.priceSymbol,
        seed:{date:seedPoint.date,value:seedPoint.pe_forward,source:'previous-published-estimate'}, closes:[], observations:[] };
      assert.equal(inputs.priceSymbol,plan.priceSymbol);
      assert.equal(inputs.startDate,plan.startDate);
      const target = item.points.at(-1).date;
      if(refreshSources && !plan.weekly) {
        const observation = await fetchSnapshot(item.id);
        if(observation?.pe_forward > 0) {
          if(!isTradingDate(observation.date) || observation.date > target) throw new Error(`Invalid dated forward observation: ${item.id}/${observation.date}`);
          const byDate = new Map(inputs.observations.map(row=>[row.date,row]));
          const existing = byDate.get(observation.date);
          byDate.set(observation.date,{date:observation.date,value:observation.pe_forward,source:observation.source,
            ...(observation.sourceUrl ? {url:observation.sourceUrl} : {}),
            capturedAt:existing?.value === observation.pe_forward ? existing.capturedAt : new Date().toISOString()});
          inputs.observations = [...byDate.values()].sort((a,b)=>a.date.localeCompare(b.date));
        }
      }
      const prices = new Map();
      for(const row of inputs.closes) {
        if(!isTradingDate(row.date) || !Number.isFinite(row.close) || row.close <= 0 || !row.source) throw new Error(`Invalid cached forward close: ${item.id}/${row.date}`);
        prices.set(row.date,row);
      }
      const required = tradingDates(inputs.seed.date,target), missing = required.filter(date=>!prices.has(date));
      if(missing.length) for(const row of await fetchCloses(inputs.priceSymbol,'etf',missing[0],target,missing)) {
        if(required.includes(row.date) && !prices.has(row.date)) prices.set(row.date,row);
      }
      const remaining = required.filter(date=>!prices.has(date));
      if(remaining.length) throw new Error(`Incomplete ${inputs.priceSymbol} closes: ${remaining.join(', ')}`);
      inputs.closes = [...prices.values()].sort((a,b)=>a.date.localeCompare(b.date));
      const before = item.points;
      item.points = applyIndexForwardPolicy(before,snapshots,inputs,plan.weekly);
      for(let i=0;i<before.length;i++) {
        const {pe_forward:a,pe_forward_estimate:b,pe_forward_snapshot:c,...oldOther} = before[i];
        const {pe_forward,pe_forward_estimate,pe_forward_snapshot,...other} = item.points[i];
        assert.deepEqual(other,oldOther);
        if(before[i].date < plan.startDate) assert.deepEqual(item.points[i],before[i]);
        if(!isDeepStrictEqual(before[i],item.points[i])) changes.push({id:item.id,date:before[i].date,before:a,after:pe_forward});
      }
      if(ledger) for(const repair of ledger.repairs[item.symbol] || []) [repair.point] = applyIndexForwardPolicy([repair.point],snapshots,inputs,plan.weekly);
      cache.indices[item.id] = inputs;
    } catch(error) { errors.push(`${item.id}: ${error.message}`); }
  },{concurrency:6,budgetMs:120000});
  errors.push(...skipped.map(item=>`${item.id}: recovery budget exhausted`));
  // No partial cache/history/lock publication when an input is unavailable.
  if(errors.length) throw new Error(errors.join('\n'));
  if(write) {
    await save('index-forward-inputs.json',cache);
    if(changes.length) {
      dataset.generatedAt = new Date().toISOString();
      await save('valuation-history.json',dataset);
      await save('index-history-lock.json',buildIndexHistoryLock(dataset));
      if(ledger) await save('index-gap-repairs.json',ledger);
    }
  }
  return { changes:changes.sort((a,b)=>a.id.localeCompare(b.id)||a.date.localeCompare(b.date)),
    flatRuns:dataset.indices.flatMap(item=>findFlatMetricRuns(item.points).map(row=>({id:item.id,...row}))) };
}

if(path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  const result = await reconcileIndexForward({write:process.argv.includes('--write'),refreshSources:process.argv.includes('--sources')});
  console.log('[index forward]',JSON.stringify({changed:result.changes.length,indices:[...new Set(result.changes.map(row=>row.id))],flatRuns:result.flatRuns}));
}
