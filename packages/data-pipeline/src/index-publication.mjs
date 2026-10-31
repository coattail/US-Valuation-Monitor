import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { applyWsjIndexPolicy, wsjMetricAnchors, WSJ_INDEX_PRICES } from './wsj-index-policy.ts';
import { applyEtfTtmPricePolicy, applyEtfPbPricePolicy, ETF_TTM_SYMBOLS, ETF_PB_SYMBOLS, ETF_TTM_START, etfTtmAnchors, etfPbAnchors } from './etf-ttm-policy.ts';
import { applySp500PbPolicy } from './sp500-pb-policy.ts';
import { reconstructDramTtm } from './dram-ttm-reconstruction.ts';
import { applyIndexForwardPolicy, indexForwardAnchors, forwardPlan } from './index-forward-policy.ts';
import { fetchGapCloses } from './gap-repair.ts';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from './index-history-lock.ts';

const defaultDir = fileURLToPath(new URL('../../../data/standardized/', import.meta.url));
const read = async (dir, name) => JSON.parse(await readFile(path.join(dir, name), 'utf8'));
const save = async (dir, name, data) => writeFile(path.join(dir, name), JSON.stringify(data, null, 2) + '\n');

export async function loadIndexPublicationInputs(dir = defaultDir, { dataset, metrics, refresh = false,
  fetchCloses = (...args) => fetchGapCloses(...args, undefined, { preferYahoo: true }),
} = {}) {
  const files = Object.fromEntries(await Promise.all(['sp500-ttm-closes.json', 'nasdaq100-forward-closes.json', 'index-forward-inputs.json', 'etf-ttm-closes.json', 'sp500-pb-inputs.json'].map(async name => [name, await read(dir, name)])));
  const prices = {};
  for (const [id, symbol] of Object.entries(WSJ_INDEX_PRICES)) {
    const file = id === 'sp500' ? files['sp500-ttm-closes.json'] : id === 'nasdaq100' ? files['nasdaq100-forward-closes.json'] : files['index-forward-inputs.json'].indices[id];
    assert.equal(file.symbol || file.priceSymbol, symbol, `Wrong WSJ price universe: ${id}`);
    const rows = file.observations && file.symbol ? file.observations : file.closes;
    for (const row of rows) if (!Number.isFinite(row.close) || row.close <= 0 || !row.source) throw new Error(`Invalid ${symbol} close: ${row.date}`);
    const byDate = new Map(rows.map(row => [row.date, row]));
    const index = dataset?.indices.find(row => row.id === id);
    if (refresh && index) {
      const snapshots = metrics.symbols[index.symbol] || [];
      const anchors = ['pe_ttm', 'pe_forward'].flatMap(metric => wsjMetricAnchors(snapshots, metric, id));
      const first = anchors.map(row => row.date).sort()[0], target = index.points.at(-1).date;
      const required = [...new Set([...anchors.map(row => row.date), ...index.points.filter(row => row.date >= first).map(row => row.date)])].filter(date => date <= target).sort();
      const missing = required.filter(date => !byDate.has(date));
      if (missing.length) for (const row of await fetchCloses(symbol, 'etf', missing[0], target, missing)) {
        if (missing.includes(row.date) && Number.isFinite(row.close) && row.close > 0 && row.source) byDate.set(row.date, row);
      }
      if (required.some(date => !byDate.has(date))) throw new Error(`Incomplete ${symbol} WSJ prices`);
    }
    prices[id] = [...byDate.values()].sort((a,b) => a.date.localeCompare(b.date));
    if (file.symbol) file.observations = prices[id]; else file.closes = prices[id];
  }
  if (refresh && dataset) {
    const pb = files['sp500-pb-inputs.json'], pbPrices = new Map(pb.closes.map(row => [row.date,row]));
    for (const row of prices.sp500) if (!pbPrices.has(row.date) && /^https:\/\/query[12]\.finance\.yahoo\.com\/v8\/finance\/chart\/%5EGSPC\?/i.test(row.source)) {
      pbPrices.set(row.date,{date:row.date,close:row.close});
      if (!pb.priceSources.includes(row.source)) pb.priceSources.push(row.source);
    }
    pb.closes = [...pbPrices.values()].sort((a,b)=>a.date.localeCompare(b.date));
    for (const index of dataset.indices) {
      const snapshots = metrics.symbols[index.symbol] || [], target = index.points.at(-1)?.date;
      if (!target) continue;
      const plan = forwardPlan(index), forward = files['index-forward-inputs.json'].indices[index.id];
      const isFundForward = plan && !WSJ_INDEX_PRICES[index.id];
      if (isFundForward && !forward) throw new Error(`Missing forward inputs: ${index.id}`);
      if (isFundForward) assert.equal(forward.priceSymbol,index.symbol,`Wrong forward price universe: ${index.id}`);
      const groups = [];
      if (ETF_TTM_SYMBOLS.has(index.symbol)) groups.push({ start:ETF_TTM_START, anchors:etfTtmAnchors(snapshots) });
      if (ETF_PB_SYMBOLS.has(index.symbol)) groups.push({ start:ETF_TTM_START, anchors:etfPbAnchors(snapshots) });
      if (isFundForward) groups.push({ start:forward.startDate, anchors:indexForwardAnchors(snapshots,forward,plan.weekly) });
      if (!groups.length) continue;
      const required = new Set();
      for (const { start, anchors } of groups) {
        const before = anchors.findLast(row => row.date <= start);
        const applicable = [...(before ? [before] : []), ...anchors.filter(row => row.date > start && row.date <= target)];
        if (!applicable.length) continue;
        for (const row of applicable) required.add(row.date);
        const from = start > applicable[0].date ? start : applicable[0].date;
        for (const row of index.points) if (row.date >= from) required.add(row.date);
      }
      const byDate = new Map([...(files['etf-ttm-closes.json'].symbols[index.symbol] || []), ...(isFundForward ? forward.closes : [])].map(row => [row.date,row]));
      for (const row of byDate.values()) if (!Number.isFinite(row.close) || row.close <= 0 || !row.source) throw new Error(`Invalid ${index.symbol} cached close: ${row.date}`);
      const missing = [...required].filter(date => date <= target && !byDate.has(date)).sort();
      if (missing.length) for (const row of await fetchCloses(index.symbol,'etf',missing[0],target,missing)) {
        if (missing.includes(row.date) && Number.isFinite(row.close) && row.close > 0 && row.source) byDate.set(row.date,row);
      }
      if ([...required].some(date => date <= target && !byDate.has(date))) throw new Error(`Incomplete ${index.symbol} valuation prices`);
      const closes = [...byDate.values()].sort((a,b)=>a.date.localeCompare(b.date));
      files['etf-ttm-closes.json'].symbols[index.symbol] = closes;
      if (isFundForward) forward.closes = closes;
    }
  }
  return { prices, files };
}

export function reconcileIndexDataset(dataset, metrics, prices, files) {
  const next = structuredClone(dataset);
  for (const index of next.indices) {
    const snapshots = metrics.symbols[index.symbol] || [];
    if (WSJ_INDEX_PRICES[index.id]) index.points = applyWsjIndexPolicy(index.points, index.id, snapshots, prices[index.id] || []);
    if (files) {
      if (index.id === 'sp500') index.points = applySp500PbPolicy(index.points,files['sp500-pb-inputs.json'],false);
      const closes = files['etf-ttm-closes.json'].symbols[index.symbol] || [];
      if (ETF_TTM_SYMBOLS.has(index.symbol)) {
        index.points = applyEtfTtmPricePolicy(index.points,snapshots,closes);
        if (index.id === 'dram' && index.points.at(-1).date >= '2026-10-23') index.points = reconstructDramTtm(index.points,snapshots,closes);
      }
      if (ETF_PB_SYMBOLS.has(index.symbol)) index.points = applyEtfPbPricePolicy(index.points,snapshots,closes);
      const plan = forwardPlan(index);
      if (plan && !WSJ_INDEX_PRICES[index.id]) index.points = applyIndexForwardPolicy(index.points,snapshots,files['index-forward-inputs.json'].indices[index.id],plan.weekly);
    }
  }
  return next;
}

export async function validateIndexPublication(dir = defaultDir) {
  const dataset = await read(dir, 'valuation-history.json'), metrics = await read(dir, 'index-yahoo-daily-metrics.json');
  assertDatasetMatchesIndexHistoryLock(dataset, await read(dir, 'index-history-lock.json'));
  const { prices, files } = await loadIndexPublicationInputs(dir);
  assert.deepEqual(dataset, reconcileIndexDataset(dataset, metrics, prices, files), 'Published valuations disagree with source-anchor policy');
  const snapshot = await read(dir, 'valuation-snapshot.json');
  for (const index of dataset.indices) {
    const series = await read(dir, `index-series/${index.id}.json`), row = snapshot.indices.find(row => row.id === index.id);
    assert.deepEqual(series.points, index.points, `Stale series: ${index.id}`);
    assert.ok(row, `Missing snapshot: ${index.id}`);
    for (const metric of ['pe_ttm', 'pe_forward', 'pb']) {
      const latest = index.points.findLast(p => typeof p[metric] === 'number' && Number.isFinite(p[metric]));
      assert.equal(row[metric], latest?.[metric] ?? null, `Stale snapshot ${index.id}/${metric}`);
    }
  }
}

export async function reconcileIndexValuations({ dir = defaultDir, write = false, ...options } = {}) {
  const dataset = await read(dir, 'valuation-history.json'), metrics = await read(dir, 'index-yahoo-daily-metrics.json');
  assertDatasetMatchesIndexHistoryLock(dataset, await read(dir, 'index-history-lock.json'));
  const { prices, files } = await loadIndexPublicationInputs(dir, { dataset, metrics, refresh: true, ...options });
  const next = reconcileIndexDataset(dataset, metrics, prices, files);
  const changes = next.indices.flatMap((index, i) => index.points.flatMap((point, j) => {
    const old = dataset.indices[i].points[j];
    const { pe_ttm:a, pe_forward:b, pe_ttm_estimate:c, pe_forward_estimate:d, pe_ttm_snapshot:e, pe_forward_snapshot:f, pe_ttm_observation:g, pe_forward_observation:h, pb:oldPb, pb_basis:oldPbBasis, ...other } = old;
    const { pe_ttm, pe_forward, pe_ttm_estimate, pe_forward_estimate, pe_ttm_snapshot, pe_forward_snapshot, pe_ttm_observation, pe_forward_observation, pb, pb_basis, ...rest } = point;
    assert.deepEqual(rest, other);
    return isDeepStrictEqual(point, old) ? [] : [{ id:index.id, date:point.date, before:[a,b,oldPb], after:[pe_ttm,pe_forward,pb] }];
  }));
  if (write) {
    for (const [name, file] of Object.entries(files)) await save(dir, name, file);
    if (!isDeepStrictEqual(next, dataset)) {
      next.generatedAt = new Date().toISOString();
      let ledger;
      try { ledger = await read(dir, 'index-gap-repairs.json'); } catch(error) { if(error.code !== 'ENOENT') throw error; }
      if (ledger) for (const index of next.indices) {
        const points = new Map(index.points.map(point => [point.date, point]));
        for (const repair of ledger.repairs[index.symbol] || []) if (points.has(repair.date)) repair.point = points.get(repair.date);
      }
      await save(dir, 'valuation-history.json', next);
      await save(dir, 'index-history-lock.json', buildIndexHistoryLock(next));
      if (ledger) await save(dir, 'index-gap-repairs.json', ledger);
    }
  }
  return { dataset: next, changes };
}

