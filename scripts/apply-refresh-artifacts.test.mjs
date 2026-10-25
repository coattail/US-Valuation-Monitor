import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { applyArtifacts } from './apply-refresh-artifacts.mjs';
test('publishes a successful group when the other fails and removes departed symbols', async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(),'refresh-artifacts-'));
  try {
    const root = path.join(dir,'repo'), artifact = path.join(dir,'artifact');
    const dest = path.join(root,'data/standardized'), source = path.join(artifact,'data/standardized');
    await mkdir(path.join(dest,'company-series'),{recursive:true});
    await mkdir(path.join(dest,'index-series'),{recursive:true});
    await mkdir(path.join(source,'company-series'),{recursive:true});
    await writeFile(path.join(dest,'company-series/old.json'),'{}');
    await writeFile(path.join(dest,'index-series/existing.json'),'preserved');
    await writeFile(path.join(source,'company-series/new.json'),'{}');
    await writeFile(path.join(source,'company-valuation-snapshot.json'),JSON.stringify({indices:[{id:'new'}]}));
    await writeFile(path.join(source,'company-yahoo-daily-metrics.json'),'{}');
    assert.deepEqual(await applyArtifacts(root,artifact),['company']);
    assert.deepEqual(await readdir(path.join(dest,'company-series')),['new.json']);
    assert.equal(await readFile(path.join(dest,'index-series/existing.json'),'utf8'),'preserved');
    await rm(path.join(source,'company-series/new.json'));
    await assert.rejects(applyArtifacts(root,artifact),/Incomplete/);
    assert.deepEqual(await readdir(path.join(dest,'company-series')),['new.json']);
  } finally { await rm(dir,{recursive:true,force:true}); }
});

test('index artifacts must include and publish the reproducible NDX price cache', async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'ndx-artifact-'));
  try {
    const root = path.join(dir, 'repo'), artifact = path.join(dir, 'artifact');
    const source = path.join(artifact, 'data/standardized');
    await mkdir(path.join(source, 'index-series'), { recursive: true });
    await writeFile(path.join(source, 'index-series/nasdaq100.json'), '{}');
    await writeFile(path.join(source, 'valuation-snapshot.json'), JSON.stringify({ indices: [{ id: 'nasdaq100' }] }));
    for (const name of ['index-yahoo-daily-metrics.json', 'valuation-history.json', 'index-history-lock.json']) await writeFile(path.join(source, name), '{}');
    await assert.rejects(applyArtifacts(root, artifact), /nasdaq100-forward-closes/);
    const prices = JSON.stringify({ symbol: '^NDX', observations: [{ date: '2026-04-17', close: 100, source: 'test' }] });
    await writeFile(path.join(source, 'nasdaq100-forward-closes.json'), prices);
    await assert.rejects(applyArtifacts(root, artifact), /sp500-ttm-closes/);
    await writeFile(path.join(source, 'sp500-ttm-closes.json'), JSON.stringify({symbol:'^GSPC',observations:[]}));
    await assert.rejects(applyArtifacts(root, artifact), /etf-ttm-closes/);
    await writeFile(path.join(source, 'etf-ttm-closes.json'), JSON.stringify({version:1,symbols:{}}));
    await assert.rejects(applyArtifacts(root, artifact), /index-forward-inputs/);
    await writeFile(path.join(source,'index-forward-inputs.json'),JSON.stringify({version:1,indices:{}}));
    await assert.rejects(applyArtifacts(root, artifact), /sp500-pb-inputs/);
    await writeFile(path.join(source,'sp500-pb-inputs.json'),JSON.stringify({version:1,symbol:'^GSPC'}));
    assert.deepEqual(await applyArtifacts(root, artifact), ['index']);
    assert.equal(await readFile(path.join(root, 'data/standardized/nasdaq100-forward-closes.json'), 'utf8'), prices);
    assert.deepEqual(JSON.parse(await readFile(path.join(root, 'data/standardized/etf-ttm-closes.json'), 'utf8')), {version:1,symbols:{}});
  } finally { await rm(dir, { recursive: true, force: true }); }
});
