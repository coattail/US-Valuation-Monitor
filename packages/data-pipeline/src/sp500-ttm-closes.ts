import {readFile,writeFile} from 'node:fs/promises';
import {createTextFetcher} from './fetch-text.ts';
import {tradingDates} from './market-calendar.ts';
import {SP500_CLOSE_START,type Sp500Close} from './sp500-ttm-policy.ts';
const CACHE=new URL('../../../data/standardized/sp500-ttm-closes.json',import.meta.url);
const request=createTextFetcher({userAgent:'Mozilla/5.0',requestBudgetMs:10000});
const valid=(row:Sp500Close)=>/^\d{4}-\d{2}-\d{2}$/.test(row.date) && Number.isFinite(row.close) && row.close>0 && Boolean(row.source);
export async function loadSp500TtmCloses():Promise<Sp500Close[]> {
  try {
    const data=JSON.parse(await readFile(CACHE,'utf8'));
    if(data.symbol!=='^GSPC' || !Array.isArray(data.observations) || !data.observations.every(valid)) throw new Error('Invalid S&P 500 close cache');
    return data.observations;
  } catch(error) {if((error as NodeJS.ErrnoException).code==='ENOENT') return [];throw error;}
}
export function parseSp500TtmCloses(raw:string,source:string):Sp500Close[] {
  const result=JSON.parse(raw)?.chart?.result?.[0];
  if(result?.meta?.symbol!=='^GSPC') throw new Error('Expected unadjusted ^GSPC index closes');
  return (result.timestamp || []).map((ts:number,i:number)=>({date:new Date(ts*1000).toISOString().slice(0,10),
    close:result.indicators?.quote?.[0]?.close?.[i],source})).filter(valid);
}
export async function refreshSp500TtmCloses(endDate:string):Promise<Sp500Close[]> {
  const cached=await loadSp500TtmCloses();
  const byDate=new Map(cached.map(row=>[row.date,row]));
  const required=tradingDates(SP500_CLOSE_START,endDate);
  const complete=()=>required.every(date=>byDate.has(date));
  if(complete()) return cached;
  const from=Date.parse(SP500_CLOSE_START+'T00:00:00Z')/1000,to=Date.parse(endDate+'T00:00:00Z')/1000+86400;
  for(const host of ['query1','query2']) {
    const url=`https://${host}.finance.yahoo.com/v8/finance/chart/%5EGSPC?period1=${from}&period2=${to}&interval=1d`;
    try {
      for(const row of parseSp500TtmCloses(await request(url),url)) {
        if(row.date>=SP500_CLOSE_START && row.date<=endDate && !byDate.has(row.date)) byDate.set(row.date,row);
      }
      if(complete()) break;
    } catch(error) {console.warn(`[SPX TTM closes] ${String(error)}`);}
  }
  const observations=[...byDate.values()].sort((a,b)=>a.date.localeCompare(b.date));
  if(observations.length>cached.length) await writeFile(CACHE,JSON.stringify({symbol:'^GSPC',observations},null,2)+'\n');
  return observations;
}
