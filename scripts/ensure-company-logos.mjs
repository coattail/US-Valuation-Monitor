import { readFile, copyFile, access } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
export async function ensureCompanyLogos(root = process.cwd()) {
 const snapshot = JSON.parse(await readFile(path.join(root, 'data/standardized/company-valuation-snapshot.json'), 'utf8'));
 const dir = path.join(root, 'apps/web/assets/company-logos/64');
 const added=[];
 for (const {symbol} of snapshot.indices) {
  if (!/^[A-Z0-9.-]+$/.test(symbol)) throw Error(`Invalid logo symbol: ${symbol}`);
  const ext=symbol==='SPCX'?'ico':symbol==='SKHY'?'svg':'png';
  const target=path.join(dir,`${symbol}.${ext}`);
  try { await access(target); } catch(error) {
   if(error.code!=='ENOENT')throw error;
   if(ext!=='png')throw Error(`Missing special logo: ${symbol}`);
   await copyFile(path.join(dir,'_default.png'),target);
   console.warn(`::notice::${symbol}: using local placeholder logo until a brand asset is supplied`);
   added.push(symbol);
  }
 }
 return added;
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href)await ensureCompanyLogos();
