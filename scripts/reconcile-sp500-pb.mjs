import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import {readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createTextFetcher} from '../packages/data-pipeline/src/fetch-text.ts';
import {applySp500PbPolicy, parseSp500BookValues, SP500_BOOK_VALUE_URL} from '../packages/data-pipeline/src/sp500-pb-policy.ts';
import {parseSp500TtmCloses} from '../packages/data-pipeline/src/sp500-ttm-closes.ts';
import {assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock} from '../packages/data-pipeline/src/index-history-lock.ts';
const request = createTextFetcher({userAgent:'Mozilla/5.0',requestBudgetMs:25000});
const defaultDir = fileURLToPath(new URL('../data/standardized/',import.meta.url));

export async function reconcileSp500Pb({dir=defaultDir, write=false, refreshSources=true, fetchText=request}={}) {
  const read = async name => JSON.parse(await readFile(path.join(dir,name),'utf8'));
  const save = async(name,data) => writeFile(path.join(dir,name),JSON.stringify(data,null,2)+'\n');
  const dataset = await read('valuation-history.json');
  assertDatasetMatchesIndexHistoryLock(dataset,await read('index-history-lock.json'));
  const series = dataset.indices.find(row => row.id === 'sp500');
  if (!series?.points?.length) throw new Error('Missing S&P 500 history');
  let inputs;
  try { inputs = await read('sp500-pb-inputs.json'); } catch(error) {
    if (error.code !== 'ENOENT') throw error;
    inputs = {version:1,symbol:'^GSPC',bookValueSource:SP500_BOOK_VALUE_URL,bookValues:[],priceSources:[],closes:[]};
  }
  if (refreshSources) {
    try {
      const books = parseSp500BookValues(await fetchText(SP500_BOOK_VALUE_URL));
      const byDate = new Map(inputs.bookValues.map(row => [row.date,row]));
      for (const row of books) byDate.set(row.date,row);
      inputs.bookValues = [...byDate.values()].sort((a,b) => a.date.localeCompare(b.date));
    } catch(error) {
      if (!inputs.bookValues.length) throw error;
      console.warn(`[SPX PB] Book-value source unavailable; retaining dated observations: ${error.message}`);
    }
  }
  const prices = new Map(inputs.closes.map(row => [row.date,row]));
  const missing = series.points.filter(row => !prices.has(row.date));
  if (missing.length) {
    const from = Date.parse(missing[0].date)/1000, to = Date.parse(missing.at(-1).date)/1000+86400;
    for (const host of ['query1','query2']) {
      const url = `https://${host}.finance.yahoo.com/v8/finance/chart/%5EGSPC?period1=${from}&period2=${to}&interval=1d`;
      try {
        const rows = parseSp500TtmCloses(await fetchText(url),url);
        for (const {date,close} of rows) if (!prices.has(date)) prices.set(date,{date,close});
        if (!inputs.priceSources.includes(url)) inputs.priceSources.push(url);
        if (missing.every(row => prices.has(row.date))) break;
      } catch(error) { console.warn(`[SPX PB] ${error.message}`); }
    }
  }
  inputs.closes = [...prices.values()].sort((a,b) => a.date.localeCompare(b.date));
  const before = series.points;
  series.points = applySp500PbPolicy(before,inputs);
  const changes = [];
  for (let i=0;i<before.length;i++) {
    const {pb:oldPb,pb_basis:oldBasis,...oldOther} = before[i];
    const {pb,pb_basis,...other} = series.points[i];
    assert.deepEqual(other,oldOther);
    if (!isDeepStrictEqual(before[i],series.points[i])) changes.push({date:before[i].date,before:oldPb,after:pb});
  }
  let ledger;
  try { ledger=await read('index-gap-repairs.json'); } catch(error) { if(error.code!=='ENOENT') throw error; }
  if (ledger) {
    const points = new Map(series.points.map(row => [row.date,row]));
    for (const repair of ledger.repairs.SPY || []) if(points.has(repair.date)) repair.point = points.get(repair.date);
  }
  if (write) {
    await save('sp500-pb-inputs.json',inputs);
    if (changes.length) {
      await save('valuation-history.json',dataset);
      await save('index-history-lock.json',buildIndexHistoryLock(dataset));
      if(ledger) await save('index-gap-repairs.json',ledger);
    }
  }
  return {changed:changes.length,latest:series.points.at(-1),changes};
}
if(path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  const {changes,...result} = await reconcileSp500Pb({write:process.argv.includes('--write'),refreshSources:!process.argv.includes('--cached')});
  console.log('[SPX PB]',JSON.stringify(result));
}
