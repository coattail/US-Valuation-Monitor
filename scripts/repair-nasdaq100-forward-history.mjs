import assert from 'node:assert/strict';
import { refreshNasdaqForwardCloses } from '../packages/data-pipeline/src/nasdaq-forward-closes.ts';
import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { applyNasdaqForwardPricePolicy, assertNasdaqForwardCoverage, NASDAQ_FORWARD_WSJ_START } from '../packages/data-pipeline/src/nasdaq-forward-policy.ts';
import { assertDatasetMatchesIndexHistoryLock, buildIndexHistoryLock } from '../packages/data-pipeline/src/index-history-lock.ts';

const dir = new URL('../data/standardized/', import.meta.url);
const read = async name => JSON.parse(await readFile(new URL(name, dir), 'utf8'));
const write = async (name, value) => writeFile(new URL(name, dir), JSON.stringify(value, null, 2) + '\n');
const dataset = await read('valuation-history.json');
assertDatasetMatchesIndexHistoryLock(dataset, await read('index-history-lock.json'));
const snapshots = (await read('index-yahoo-daily-metrics.json')).symbols.QQQ || [];
assert(snapshots.some(row => row.date === NASDAQ_FORWARD_WSJ_START && row.source === 'wsj-latest'));
const closes = await refreshNasdaqForwardCloses(dataset.indices.find(row => row.id === 'nasdaq100').points.at(-1).date);
const ndx = dataset.indices.find(row => row.id === 'nasdaq100');
const before = structuredClone(ndx.points);
ndx.points = applyNasdaqForwardPricePolicy(ndx.points, snapshots, closes);
assertNasdaqForwardCoverage(ndx.points);
const changes = ndx.points.filter((row, i) => row.pe_forward !== before[i].pe_forward || JSON.stringify(row.pe_forward_estimate) !== JSON.stringify(before[i].pe_forward_estimate));
for (let i = 0; i < before.length; i++) {
  const { pe_forward, pe_forward_estimate, ...afterOther } = ndx.points[i];
  const { pe_forward: oldValue, pe_forward_estimate: oldEstimate, ...beforeOther } = before[i];
  assert.deepEqual(afterOther, beforeOther);
}
const observations = ndx.points.filter(row => row.date >= NASDAQ_FORWARD_WSJ_START && row.pe_forward !== null && !row.pe_forward_estimate);
console.log(JSON.stringify({ changed: changes.length, observed: observations.length, estimated: ndx.points.filter(row => row.pe_forward_estimate).length,
  first: observations[0], latest: observations.at(-1), examples: changes.slice(0, 5) }, null, 2));
if (process.argv.includes('--write') && changes.length) {
  dataset.generatedAt = new Date().toISOString();
  await write('valuation-history.json', dataset);
  await write('index-history-lock.json', buildIndexHistoryLock(dataset));
  const ledger = await read('index-gap-repairs.json');
  for (const repair of ledger.repairs.QQQ || []) {
    [repair.point] = applyNasdaqForwardPricePolicy([repair.point], snapshots, closes);
  }
  await write('index-gap-repairs.json', ledger);
  console.log(`Repaired ${fileURLToPath(dir)}; run split-index-dataset.ts and build:site next.`);
} else {
  console.log(changes.length ? 'Dry run; pass --write for the audited rewrite.' : 'Already repaired.');
}
