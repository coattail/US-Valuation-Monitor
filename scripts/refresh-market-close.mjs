import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createTextFetcher } from '../packages/data-pipeline/src/fetch-text.ts';
import { assertExistingPointsUnchanged, fetchGapCloses, repairMissingPoints, recoverWithBudget } from '../packages/data-pipeline/src/gap-repair.ts';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';
import { lastCompletedSession, shiftDate, tradingDates } from '../packages/data-pipeline/src/market-calendar.ts';
import { refreshNasdaqForwardCloses } from '../packages/data-pipeline/src/nasdaq-forward-closes.ts';
import { applyNasdaqForwardPricePolicy, assertNasdaqForwardCoverage } from '../packages/data-pipeline/src/nasdaq-forward-policy.ts';
import { refreshSp500TtmCloses } from '../packages/data-pipeline/src/sp500-ttm-closes.ts';
import { applySp500TtmPricePolicy, assertSp500TtmCoverage } from '../packages/data-pipeline/src/sp500-ttm-policy.ts';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const request = createTextFetcher({ userAgent: 'Mozilla/5.0', requestBudgetMs: 10000 });

export function parseTreasuryYields(raw, source) {
  const rows = raw.trim().split(/\r?\n/).map(row => row.split(','));
  const column = rows[0].indexOf('DGS10');
  if (column < 1) throw new Error('Missing DGS10 column');
  return rows.slice(1).flatMap(row => /^\d{4}-\d{2}-\d{2}$/.test(row[0]) && Number.isFinite(Number(row[column])) && Number(row[column]) > 0
    ? [{ date: row[0], value: Number((Number(row[column]) / 100).toFixed(5)), source }] : []);
}

export async function refreshMarketClose(kind, {
  dir = path.join(root, 'data/standardized'), now = new Date(), target = lastCompletedSession(now),
  fetchCloses = (...args) => fetchGapCloses(...args, undefined, { preferYahoo: true }),
  ndxCloses = refreshNasdaqForwardCloses, spxCloses = refreshSp500TtmCloses,
  fetchYield = url => request(url, 0, 10000),
} = {}) {
  if (kind !== 'index') throw new Error('Close estimates are index-only; company valuations must synchronize Yahoo sources');
  if (target > lastCompletedSession(now)) throw new Error('Cannot refresh an uncompleted session');
  const read = async name => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
  const write = async (name, value) => writeFile(path.join(dir, name), JSON.stringify(value, null, 2) + '\n');
  const historyFile = 'valuation-history.json';
  const dataset = await read(historyFile);
  assertDatasetMatchesIndexHistoryLock(dataset, await read('index-history-lock.json'));
  const published = structuredClone(dataset);
  const missingSessions = item => {
    const present = new Set(item.points.map(point => point.date));
    const first = item.points[0]?.date;
    return first ? tradingDates(first > '2026-09-18' ? first : '2026-09-18',target).filter(date => !present.has(date)) : [];
  };
  const pending = dataset.indices.filter(item => missingSessions(item).length);
  if (!pending.length) {
    console.log(`[close] ${kind}: already through ${target}`);
    return { added: 0, pending: [] };
  }
  const metrics = await read(`${kind}-yahoo-daily-metrics.json`);
  let ledger;
  try { ledger = await read(`${kind}-gap-repairs.json`); } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    ledger = { version: 1, repairs: {} };
  }
  const [ndx, spx] = (await Promise.allSettled([ndxCloses(target), spxCloses(target)])).map((result, i) => {
    if (result.status === 'fulfilled') return result.value;
    console.warn(`[close] ${i === 0 ? 'NDX' : 'SPX'} closes unavailable; retaining affected index for retry: ${String(result.reason)}`);
    return [];
  });
  const yieldRows = [];
  {
    for (const item of dataset.indices) for (const point of item.points) {
      if (Number.isFinite(point.us10y_yield) && point.us10y_yield > 0) yieldRows.push({
        date: point.date, value: point.us10y_yield, source: 'previous-published-us10y-yield',
      });
    }
    const from = shiftDate(pending.map(item => missingSessions(item)[0]).sort()[0], -7);
    const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10&cosd=${from}&coed=${target}`;
    try { yieldRows.push(...parseTreasuryYields(await fetchYield(url), url)); }
    catch (error) { console.warn(`[close] Treasury update unavailable; carrying the last dated yield: ${String(error)}`); }
    yieldRows.sort((a, b) => a.date.localeCompare(b.date));
  }
  let added = 0;
  const skipped = await recoverWithBudget(pending, async item => {
    const original = structuredClone(item.points);
    const missing = missingSessions(item);
    const anchorDates = [...new Set(missing.map(date => original.findLast(point => point.date < date)?.date).filter(Boolean))].sort();
    const closes = await fetchCloses(item.symbol, 'etf', anchorDates[0] || missing[0], target, [...anchorDates, ...missing]);
    const yieldByDate = new Map(missing.map(date => [date, yieldRows.findLast(row => row.date <= date)]));
    const yields = new Map(missing.flatMap(date => yieldByDate.get(date) ? [[date, yieldByDate.get(date).value]] : []));
    const result = repairMissingPoints(original, missing, closes, yields, metrics.symbols[item.symbol] || []);
    let points = result.repairs.map(repair => repair.point);
    // Keep the audited SPX/NDX actual-index-price rules. Only new rows enter
    // these policies; every previously published row stays byte-for-byte fixed.
    try {
      if (item.id === 'nasdaq100') {
        points = applyNasdaqForwardPricePolicy(points, metrics.symbols.QQQ || [], ndx);
        assertNasdaqForwardCoverage(points);
      }
      if (item.id === 'sp500') {
        points = applySp500TtmPricePolicy(points, metrics.symbols.SPY || [], spx);
        assertSp500TtmCoverage(points);
      }
    } catch (error) {
      console.warn(`[close] ${item.symbol}: retain published data until actual index closes are available: ${String(error)}`);
      return;
    }
    for (const [index, point] of points.entries()) {
      const repair = result.repairs[index];
      if (yieldByDate.get(point.date)) {
        const observation = yieldByDate.get(point.date);
        point.recovery = { ...point.recovery, us10yYieldDate: observation.date, us10yYieldSource: observation.source };
      }
      repair.point = point;
    }
    item.points = [...original, ...points].sort((a, b) => a.date.localeCompare(b.date));
    assertExistingPointsUnchanged(original, item.points);
    if (points.length) {
      ledger.repairs[item.symbol] = [...(ledger.repairs[item.symbol] || []), ...result.repairs];
      added += points.length;
      console.log(`[close] ${item.symbol}: appended ${points.map(point => point.date).join(', ')}`);
    }
  }, { concurrency: 8, budgetMs: 300000,
    onError: (item, error) => console.warn(`[close] ${item.symbol}: recovery failed; retaining published data for retry: ${String(error)}`),
  });
  if (skipped.length) console.warn(`[close] budget exhausted: ${skipped.map(item => item.symbol).join(', ')}`);
  if (added) {
    dataset.generatedAt = now.toISOString();
    if (!dataset.source?.includes('+market-close-estimate')) dataset.source = `${dataset.source || kind}+market-close-estimate`;
    // The input lock was checked before recovery. Interior insertions are
    // permitted only after every existing record is verified unchanged.
    for (const before of published.indices) assertExistingPointsUnchanged(before.points,dataset.indices.find(item => item.id === before.id).points);
    await write('index-history-lock.json', buildIndexHistoryLock(dataset));
    await write(`${kind}-gap-repairs.json`, ledger);
  }
  if (added) await write(historyFile, dataset);
  return { added, pending: dataset.indices.filter(item => missingSessions(item).length).map(item => item.symbol) };
}

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  refreshMarketClose(process.argv[2]).then(result => console.log('[close]', result)).catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
}
