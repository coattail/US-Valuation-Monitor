import { readFile, writeFile, readdir } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createTextFetcher } from '../packages/data-pipeline/src/fetch-text.ts';
import { parseYahooValuationMeasuresFromHtml, parseYahooQuotePageRatioPayload } from '../packages/data-pipeline/src/build-company-snapshot.ts';
import { isCurrentTable, publishYahooValuationObservations, valuationMetrics, matchesYahooPageSymbol } from '../packages/data-pipeline/src/yahoo-valuation-policy.mjs';
import { lastCompletedSession } from '../packages/data-pipeline/src/market-calendar.ts';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const data = path.join(root, 'data/standardized');
const hosts = ['finance.yahoo.com', 'uk.finance.yahoo.com', 'fr.finance.yahoo.com'];
const fetchHtml = createTextFetcher({ userAgent: 'Mozilla/5.0', directFallback: true, requestBudgetMs: 12000 });

// A second source read audits what users compare against, independently of the
// stored daily cache. No GOOGL observation is ever written into a GOOG series.
export async function readCurrentValuation(symbol, fetcher = fetchHtml) {
  for (const host of hosts) {
    const url = `https://${host}/quote/${encodeURIComponent(symbol.replaceAll('.', '-'))}/key-statistics/`;
    try {
      const html = await fetcher(url, 0, 12000);
      if (!matchesYahooPageSymbol(html, symbol)) throw new Error("Yahoo returned another symbol");
      const payload = parseYahooValuationMeasuresFromHtml(html);
      if (!payload || !isCurrentTable(payload.source)) continue;
      return { ...payload, source: `${payload.source}:${host}`, url, capturedAt: new Date().toISOString() };
    } catch { /* Try the next Yahoo region; never infer values from blocked HTML. */ }
  }
  return null;
}

export async function readQuoteTtm(symbol, fetcher = fetchHtml) {
  for (const host of hosts) {
    const url = `https://${host}/quote/${encodeURIComponent(symbol.replaceAll('.', '-'))}/`;
    try {
      const html = await fetcher(url, 0, 12000);
      if (!matchesYahooPageSymbol(html, symbol)) throw new Error('Yahoo returned another symbol');
      const payload = parseYahooQuotePageRatioPayload(html);
      if (payload?.latest.pe_ttm == null) continue;
      return { ...payload, source: `${payload.source}:${host}`, url, capturedAt: new Date().toISOString() };
    } catch { /* Preserve the last observation when the quote field is unavailable. */ }
  }
  return null;
}

async function main() {
  const write = process.argv.includes('--write');
  const quoteTtm = process.argv.includes('--quote-ttm');
  const selectedMetrics = quoteTtm ? ['pe_ttm'] : valuationMetrics.filter(metric => metric !== 'pe_ttm');
  const target = lastCompletedSession();
  const names = (await readdir(path.join(data, 'company-series'))).filter(name => /^company_.*\.json$/.test(name)).sort();
  const series = await Promise.all(names.map(async name => JSON.parse(await readFile(path.join(data, 'company-series', name), 'utf8'))));
  const daily = JSON.parse(await readFile(path.join(data, 'company-yahoo-daily-metrics.json'), 'utf8'));
  const audit = { generatedAt: new Date().toISOString(), target, policy: quoteTtm ? 'same-symbol-quote-ttm' : 'same-symbol-current-excluding-quote-ttm', companies: [] };
  let cursor = 0;
  await Promise.all(Array.from({ length: 4 }, async () => {
    for (;;) {
      const item = series[cursor++];
      if (!item) return;
      const latest = item.points.at(-1);
      const provider = quoteTtm ? await readQuoteTtm(item.symbol) : await readCurrentValuation(item.symbol);
      const sourceDate = provider?.anchors.at(-1)?.date || target;
      const values = provider?.anchors.at(-1) || provider?.latest;
      const record = { symbol: item.symbol, publishedDate: latest?.date || '', status: 'source-unavailable', sourceUrl: provider?.url || `https://finance.yahoo.com/quote/${item.symbol}/key-statistics/`, sourceDate, capturedAt: provider?.capturedAt || '', metrics: {} };
      if (provider) {
        record.status = latest?.date === target && sourceDate === target ? 'verified' : 'date-mismatch';
        for (const metric of selectedMetrics) {
          const published = latest?.[metric] ?? null;
          record.metrics[metric] = { published, current: values[metric], relativeDifference: published && values[metric] != null ? Math.abs(values[metric] / published - 1) : null };
        }
        if (record.status === 'verified' && write) {
          const prior = (daily.symbols[item.symbol] || []).find(row => row.date === target);
          const observation = { ...prior, date: target, source: provider.source, capturedAt: provider.capturedAt, metricSources: Object.fromEntries(valuationMetrics.map(metric => [metric, prior?.metricSources?.[metric] || prior?.source || ""])), metricCapturedAt: Object.fromEntries(valuationMetrics.map(metric => [metric, prior?.metricCapturedAt?.[metric] || prior?.capturedAt || ""])), comparisons: {} };
          for (const metric of selectedMetrics) {
            observation[metric] = values[metric];
            observation.metricSources[metric] = provider.source;
            observation.metricCapturedAt[metric] = provider.capturedAt;
            const published = latest[metric];
            if (published && values[metric] != null && Math.abs(values[metric] / published - 1) > 0.02) observation.comparisons[metric] = { current: values[metric], fallback: published, fallbackSource: prior?.metricSources?.[metric] || prior?.source || 'previous-publication' };
          }
          daily.symbols[item.symbol] = [...(daily.symbols[item.symbol] || []).filter(row => row.date !== target), observation].sort((a, b) => a.date.localeCompare(b.date));
          const selectedObservation = { date: target, source: provider.source, capturedAt: provider.capturedAt,
            metricSources: Object.fromEntries(selectedMetrics.map(metric => [metric, observation.metricSources[metric]])),
            metricCapturedAt: Object.fromEntries(selectedMetrics.map(metric => [metric, observation.metricCapturedAt[metric]])),
            comparisons: Object.fromEntries(selectedMetrics.filter(metric => observation.comparisons[metric]).map(metric => [metric, observation.comparisons[metric]])),
            ...Object.fromEntries(selectedMetrics.map(metric => [metric, observation[metric]])) };
          item.points = publishYahooValuationObservations(item.points, [selectedObservation], item.symbol);
        }
      }
      audit.companies.push(record);
      console.log(`[current] ${item.symbol} ${record.status} TTM=${values?.pe_ttm ?? 'unavailable'} published=${latest?.pe_ttm ?? 'unavailable'}`);
    }
  }));
  audit.companies.sort((a, b) => a.symbol.localeCompare(b.symbol));
  if (!quoteTtm && series.some(item => item.symbol === 'GOOG')) {
    const related = await readCurrentValuation('GOOGL');
    audit.alphabetShareClassComparison = related ? { symbol: 'GOOGL', url: related.url, capturedAt: related.capturedAt, values: related.anchors.at(-1) || related.latest, note: 'Comparison only. The platform tracks GOOG, a different share class.' } : { symbol: 'GOOGL', status: 'source-unavailable' };
  }
  audit.verifiedCount = audit.companies.filter(item => item.status === 'verified').length;
  audit.status = audit.verifiedCount === series.length ? 'complete' : 'partial';
  await writeFile(path.join(data, 'company-valuation-audit.json'), `${JSON.stringify(audit, null, 2)}\n`);
  if (write && audit.verifiedCount) {
    const generatedAt = new Date().toISOString();
    const snapshot = JSON.parse(await readFile(path.join(data, 'company-valuation-snapshot.json'), 'utf8'));
    await writeFile(path.join(data, 'company-valuation-history.json'), `${JSON.stringify({ generatedAt, source: `${snapshot.source}+${quoteTtm ? "yahoo-quote-ttm-reconciliation" : "yahoo-key-statistics-current-reconciliation"}`, indices: series.map(item => ({ ...item, id: item.indexId })) })}\n`);
    await writeFile(path.join(data, 'company-yahoo-daily-metrics.json'), `${JSON.stringify({ ...daily, generatedAt })}\n`);
    execFileSync(process.execPath, ['packages/data-pipeline/src/split-company-dataset.ts'], { cwd: root, stdio: 'inherit' });
    execFileSync(process.execPath, ['scripts/validate-company-series.mjs'], { cwd: root, stdio: 'inherit' });
  }
  console.log(`[current] independently verified ${audit.verifiedCount}/${series.length}; status=${audit.status}`);
  if (!audit.verifiedCount || (!write && (audit.status !== 'complete' || audit.companies.some(item => Object.values(item.metrics).some(metric => metric.relativeDifference > 0.02))))) process.exitCode = 1;
}

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) main().catch(error => { console.error(error); process.exitCode = 1; });
