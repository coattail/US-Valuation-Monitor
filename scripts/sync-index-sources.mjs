import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { fetchOfficialIndexLatestSnapshot, fetchStockAnalysisPe, fetchStockAnalysisForwardPe, upsertIndexYahooDailyMetricSnapshot } from '../packages/data-pipeline/src/generate.ts';
import { indexMetricSource } from '../packages/data-pipeline/src/index-source-policy.ts';
import { WSJ_INDEX_PRICES } from '../packages/data-pipeline/src/wsj-index-policy.ts';
import { isTradingDate, lastCompletedSession } from '../packages/data-pipeline/src/market-calendar.ts';
import { recoverWithBudget } from '../packages/data-pipeline/src/gap-repair.ts';

const defaultDir = fileURLToPath(new URL('../data/standardized/', import.meta.url));
const fallbackIds = new Set(['sp400', 'us_total_market', 'igv', 'soxx', 'smh', 'dram']);
const positive = x => typeof x === 'number' && Number.isFinite(x) && x > 0;

// Run independently of missing-day detection: a late source revision must
// update an already complete series. Fetch failures retain the last anchor.
export async function syncIndexSources({ dir = defaultDir, now = new Date(),
  fetchOfficial = fetchOfficialIndexLatestSnapshot,
  fetchUndated = async symbol => {
    const [ttm, forward] = await Promise.allSettled([fetchStockAnalysisPe(symbol), fetchStockAnalysisForwardPe(symbol)]);
    return { pe_ttm: ttm.status === 'fulfilled' ? ttm.value : null, pe_forward: forward.status === 'fulfilled' ? forward.value : null };
  },
} = {}) {
  const dataset = JSON.parse(await readFile(path.join(dir, 'valuation-history.json'), 'utf8'));
  const file = path.join(dir, 'index-yahoo-daily-metrics.json');
  const metrics = JSON.parse(await readFile(file, 'utf8'));
  const bySymbol = new Map(Object.entries(metrics.symbols));
  const original = JSON.stringify(metrics.symbols), target = lastCompletedSession(now);
  const unavailable = [];
  const skipped = await recoverWithBudget(dataset.indices.filter(row => !['sp500','nasdaq100'].includes(row.id)), async index => {
    try {
      const through = index.points.at(-1)?.date;
      if (!through) return;
      let quote = await fetchOfficial(index.id);
      if (quote && (!quote.date || !isTradingDate(quote.date) || quote.date > target || quote.date > through)) quote = null;
      const quotes = quote ? [quote] : [];
      if (fallbackIds.has(index.id) && (!quote || !positive(quote.pe_ttm) || !positive(quote.pe_forward))) {
        const fallback = await fetchUndated(index.symbol);
        quotes.push({ date:through, source:'stockanalysis-latest',
          pe_ttm: !positive(quote?.pe_ttm) ? fallback.pe_ttm : null,
          pe_forward: !positive(quote?.pe_forward) ? fallback.pe_forward : null,
        });
      }
      if (!quotes.length) { unavailable.push(index.id); return; }
      for (const quote of quotes) {
        const row = { date: quote.date, source: quote.source, capturedAt: now.toISOString(),
          pe_ttm: WSJ_INDEX_PRICES[index.id] ? null : positive(quote.pe_ttm) ? quote.pe_ttm : null,
          pe_forward: WSJ_INDEX_PRICES[index.id] ? null : positive(quote.pe_forward) ? quote.pe_forward : null,
          pb: positive(quote.pb) ? quote.pb : null,
        };
        if (![row.pe_ttm, row.pe_forward, row.pb].some(positive)) continue;
        const existing = (bySymbol.get(index.symbol) || []).find(row => row.date === quote.date);
        const unchanged = existing && ['pe_ttm', 'pe_forward', 'pb'].every(metric => !positive(row[metric]) ||
          (existing[metric] === row[metric] && indexMetricSource(existing, metric) === row.source));
        if (!unchanged) upsertIndexYahooDailyMetricSnapshot(bySymbol, index.symbol, row);
      }
    } catch(error) { unavailable.push(index.id); console.warn(`[index source] ${index.id}: ${error.message}`); }
  }, { concurrency: 6, budgetMs: 120000 });
  metrics.symbols = Object.fromEntries(bySymbol);
  const changed = original !== JSON.stringify(metrics.symbols);
  if (changed) {
    metrics.generatedAt = now.toISOString();
    await writeFile(file, JSON.stringify(metrics, null, 2) + '\n');
  }
  return { changed, unavailable: [...unavailable, ...skipped.map(row => row.id)] };
}

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) console.log('[index sources]', await syncIndexSources());
