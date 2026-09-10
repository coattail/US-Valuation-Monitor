import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

test('every company in the published universe has a local logo asset', async () => {
 const snapshot = JSON.parse(await readFile(new URL('../../data/standardized/company-valuation-snapshot.json', import.meta.url), 'utf8'));
 for (const company of snapshot.indices) {
  const ext = company.symbol === 'SPCX' ? 'ico' : 'png';
  const data = await readFile(new URL(`./assets/company-logos/64/${company.symbol}.${ext}`,import.meta.url));
  assert.ok(data.length > 100, `${company.symbol}: empty asset`);
  assert.equal(ext==='ico' ? data.readUInt32LE(0) : data.readUInt32BE(0), ext==='ico' ? 65536 : 0x89504e47, `${company.symbol}: invalid image`);
 }
});
