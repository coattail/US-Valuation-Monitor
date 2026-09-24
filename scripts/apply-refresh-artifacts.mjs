import { access, cp, mkdir, readdir, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
export async function applyArtifacts(root, artifacts) {
  const source = path.join(artifacts, 'data/standardized');
  const destination = path.join(root, 'data/standardized');
  const groups = [];
  for (const kind of ['index', 'company']) {
    const snapshot = kind === 'index' ? 'valuation-snapshot.json' : 'company-valuation-snapshot.json';
    try { await access(path.join(source, snapshot)); } catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    const rows = JSON.parse(await readFile(path.join(source, snapshot), 'utf8')).indices;
    const expected = rows.map(row => `${row.id}.json`).sort();
    const actual = (await readdir(path.join(source, `${kind}-series`))).sort();
    if (!expected.length || JSON.stringify(expected) !== JSON.stringify(actual)) throw new Error(`Incomplete ${kind} artifact`);
    const files = [snapshot, `${kind}-yahoo-daily-metrics.json`, ...(kind === 'index' ? ['valuation-history.json', 'index-history-lock.json'] : [])];
    for (const file of files) await access(path.join(source, file));
    try { await access(path.join(source, `${kind}-gap-repairs.json`)); files.push(`${kind}-gap-repairs.json`); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    groups.push({ kind, files });
  }
  if (!groups.length) throw new Error('No validated dataset artifacts to publish');
  await mkdir(destination, { recursive: true });
  for (const { kind, files } of groups) {
    // Replace only the successful group's universe, including removed tickers.
    await rm(path.join(destination, `${kind}-series`), { force: true, recursive: true });
    for (const name of [`${kind}-series`, ...files]) await cp(path.join(source, name), path.join(destination, name), { recursive: true });
  }
  return groups.map(g => g.kind);
}
if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  console.log('Applied datasets:', await applyArtifacts(root, path.join(root, 'refresh-artifact')));
}
