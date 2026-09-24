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
