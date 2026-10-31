import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {reconcileIndexValuations,validateIndexPublication} from '../packages/data-pipeline/src/index-publication.mjs';
export * from '../packages/data-pipeline/src/index-publication.mjs';

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  if (process.argv.includes('--check')) await validateIndexPublication();
  else {
    const result = await reconcileIndexValuations({ write: process.argv.includes('--write') });
    console.log('[index valuation policy]', JSON.stringify({ changed: result.changes.length, indices: [...new Set(result.changes.map(row => row.id))] }));
  }
}
