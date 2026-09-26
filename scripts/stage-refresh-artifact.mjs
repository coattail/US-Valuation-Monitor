import { cp, mkdir, access } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const kind = process.argv[2];
if (!['index', 'company'].includes(kind)) throw new Error('Expected index or company');
const names = kind === 'index'
  ? ['valuation-history.json', 'valuation-snapshot.json', 'index-series', 'index-history-lock.json', 'index-yahoo-daily-metrics.json', 'nasdaq100-forward-closes.json']
  : ['company-valuation-snapshot.json', 'company-series', 'company-yahoo-daily-metrics.json'];
const ledger = `${kind}-gap-repairs.json`;
try { await access(path.join(root, 'data/standardized', ledger)); names.push(ledger); } catch (error) { if (error.code !== 'ENOENT') throw error; }
const out = path.join(root, 'refresh-artifact/data/standardized');
await mkdir(out, { recursive: true });
for (const name of names) await cp(path.join(root, 'data/standardized', name), path.join(out, name), { recursive: true });
