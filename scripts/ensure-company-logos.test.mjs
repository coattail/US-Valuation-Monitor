import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {ensureCompanyLogos} from './ensure-company-logos.mjs';
test('new constituents receive a local fallback while existing brand assets are preserved', async()=>{
 const root=await mkdtemp(path.join(os.tmpdir(),'logos-'));
 try {
  const dir=path.join(root,'apps/web/assets/company-logos/64');await mkdir(dir,{recursive:true});
  await mkdir(path.join(root,'data/standardized'),{recursive:true});
  await writeFile(path.join(root,'data/standardized/company-valuation-snapshot.json'),JSON.stringify({indices:[{symbol:'NEW'},{symbol:'BX'}]}));
  await writeFile(path.join(dir,'_default.png'),'fallback');await writeFile(path.join(dir,'BX.png'),'brand');
  assert.deepEqual(await ensureCompanyLogos(root),['NEW']);
  assert.equal(await readFile(path.join(dir,'NEW.png'),'utf8'),'fallback');
  assert.equal(await readFile(path.join(dir,'BX.png'),'utf8'),'brand');
  assert.deepEqual(await ensureCompanyLogos(root),[]);
 }finally{await rm(root,{recursive:true,force:true});}
});
