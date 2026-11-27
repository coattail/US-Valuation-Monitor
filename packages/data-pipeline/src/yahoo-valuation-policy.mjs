import { isTradingDate, VALUATION_SESSION_EVIDENCE_START } from './market-calendar.ts';

export const valuationMetrics = ['pe_ttm', 'pe_forward', 'pb', 'peg'];
const text = html => String(html).replace(/<sup\b[^>]*>[\s\S]*?<\/sup>/gi, '')
  .replace(/<[^>]*>/g, ' ').replace(/&(?:nbsp|#160);/gi, ' ').replace(/&amp;/gi, '&')
  .replace(/\s+/g, ' ').trim();
const number = value => typeof value === 'number' && Number.isFinite(value) && value !== 0 ? value : null;
const labels = { 'trailing p/e': 'pe_ttm', 'forward p/e': 'pe_forward', 'price/book': 'pb',
  'price/book (mrq)': 'pb', 'peg ratio (current)': 'peg', 'peg ratio (5yr expected)': 'peg',
  'peg ratio (5 yr expected)': 'peg' };

const marketDate = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
});
const months = 'January February March April May June July August September October November December'.split(' ');
const attr = (tag, name) => tag.match(new RegExp(`\\b${name}=["']([^"']*)["']`, 'i'))?.[1];
const noScripts = html => String(html).replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '');

// The quote heading identifies the regular session. Intraday, pre-market and
// after-hours prices are never used to date a trailing multiple.
export function readYahooClosingQuote(html, asOf = new Date()) {
  const body = noScripts(html);
  const quote = body.match(/\bdata-testid=["']quote-hdr["'][\s\S]*?(?=<\/section>|<\/header>|<h[12]\b[^>]*>\s*Summary|$)/i)?.[0];
  if (!quote) return null;
  const closeLabel = text(quote).match(/\bAt close:\s*(?:([A-Za-z]+)\s+(\d{1,2})|(\d{1,2})\s+([A-Za-z]+))\s+at\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:[AP]\.?M\.?\s+)?(?:EST|EDT|GMT-4|GMT-5)\b/i);
  if (!closeLabel) return null;
  const month = months.findIndex(name => name.toLowerCase() === (closeLabel[1] || closeLabel[4]).toLowerCase()) + 1;
  if (!month) return null;
  const today = marketDate.format(asOf);
  const suffix = `-${String(month).padStart(2, '0')}-${(closeLabel[2] || closeLabel[3]).padStart(2, '0')}`;
  let year = Number(today.slice(0, 4));
  if (`${year}${suffix}` > today && today.slice(5, 7) === '01' && month === 12) year -= 1;
  const date = `${year}${suffix}`;
  if (date > today || !isTradingDate(date)) return null;
  const priceTag = [...quote.matchAll(/<fin-streamer\b[^>]*>/gi)].map(match => match[0])
    .find(tag => attr(tag, 'data-field') === 'regularMarketPrice');
  const close = Number(attr(priceTag || '', 'data-value'));
  return Number.isFinite(close) && close > 0 ? { date, close } : null;
}

export function readYahooTrailingEps(html) {
  const body = noScripts(html);
  const field = [...body.matchAll(/<fin-streamer\b[^>]*>/gi)].map(match => match[0])
    .find(tag => attr(tag, 'data-field') === 'epsTrailingTwelveMonths');
  const value = attr(field || '', 'data-value');
  const visible = text(body).match(/\bEPS\s*\(TTM\)\s+(-?\d+(?:\.\d+)?)(?=\s|$)/i)?.[1];
  const eps = Number(value ?? visible);
  return Number.isFinite(eps) && eps !== 0 ? eps : null;
}

export function readYahooBookValuePerShare(html) {
  const value = text(noScripts(html)).match(/\bBook Value Per Share\s*\(mrq\)\s+(-?\d+(?:,\d{3})*(?:\.\d+)?)(?=\s|$)/i)?.[1];
  return value ? number(Number(value.replaceAll(',', ''))) : null;
}

// The source displays multiples and per-share fundamentals to two decimals.
export function valuationMatchesClose(ratio, close, denominator) {
  if (![ratio, close, denominator].every(Number.isFinite) || close <= 0 || denominator === 0) return false;
  const expected = close / denominator;
  return Math.sign(expected) === Math.sign(ratio) &&
    Math.abs(ratio - expected) <= 0.006 + Math.abs(expected * 0.005 / Math.max(Math.abs(denominator) - 0.005, 0.005));
}

// Only a date attached to the valuation table or its own Valuation Measures
// heading can date the Current cells. Quote headers and other widgets update
// independently, and their dates are not evidence for these cells.
export function readYahooValuationMeasuresRegion(html) {
  return noScripts(html).match(/<section\b[^>]*\bdata-testid=["']valuation-measures["'][^>]*>[\s\S]*?<\/section>/i)?.[0] || '';
}

export function readYahooValuationTableDate(html) {
  const body = noScripts(html);
  const dated = /\bAs of\s+(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4})\b/i;
  const sectionDate = text(readYahooValuationMeasuresRegion(body)).match(dated)?.[1];
  if (sectionDate) return sectionDate;
  for (const match of body.matchAll(/<table\b[^>]*>[\s\S]*?<\/table>/gi)) {
    if (!parseYahooCurrentValuationTable(match[0])) continue;
    const inside = text(match[0]).match(dated)?.[1];
    const preceding = body.slice(Math.max(0, match.index - 4000), match.index)
      .match(/<h[1-6]\b[^>]*>\s*Valuation Measures\s*<\/h[1-6]>((?:(?!<\/?(?:h[1-6]|section|table)\b)[\s\S])*)$/i)?.[1];
    return inside || text(preceding || '').match(dated)?.[1] || '';
  }
  return '';
}

export function verifyYahooQuoteTtm(payload, target, close) {
  const quote = payload?.closingQuote;
  const ratio = payload?.latest?.pe_ttm;
  const eps = payload?.trailingEps;
  if (quote?.date !== target || !Number.isFinite(ratio) || !Number.isFinite(eps) || eps === 0) return false;
  if (close !== undefined && (!Number.isFinite(close) || Math.abs(quote.close - close) > 0.011)) return false;
  return valuationMatchesClose(ratio, quote.close, eps);
}

// Read the explicit Current cell, including a missing Current cell. Never scan
// into a historical column, another row, embedded JSON or a loading skeleton.
export function parseYahooCurrentValuationTable(html) {
  for (const table of String(html).replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '').matchAll(/<table\b[^>]*>[\s\S]*?<\/table>/gi)) {
    const rows = [...table[0].matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)]
      .map(row => [...row[1].matchAll(/<t[hd]\b[^>]*>([\s\S]*?)<\/t[hd]>/gi)].map(cell => text(cell[1])));
    const header = rows.find(row => row.some(cell => /^current$/i.test(cell)));
    if (!header) continue;
    const column = header.findIndex(cell => /^current$/i.test(cell));
    const latest = Object.fromEntries(valuationMetrics.map(key => [key, null]));
    let found = false;
    for (const row of rows) {
      const key = labels[row[0]?.toLowerCase()];
      if (!key) continue;
      found = true;
      const cell = row[column] || '';
      latest[key] = /^-?\d+(?:,\d{3})*(?:\.\d+)?$/.test(cell) ? number(Number(cell.replaceAll(',', ''))) : null;
    }
    if (found) return latest;
  }
  return null;
}

export function isCurrentTable(source = '') { return source.includes(':current-table'); }

export function yahooForwardSourcePriority(source = '') {
  if (source === 'yahoo-forward-pe-timeseries') return 3;
  if (source === 'yahoo-quote-api-forward') return 2;
  return source.startsWith('yahoo-key-statistics-valuation-measures') ? 1 : 0;
}

export function yahooPegSourcePriority(source = '') {
  if (source === 'yahoo-trailing-peg-timeseries') return 3;
  if (source === 'yahoo-quote-summary-peg') return 2;
  return source.startsWith('yahoo-key-statistics-valuation-measures') ? 1 : 0;
}

// This is the last publication step, after every repair and fallback. Apply
// observations only to their own symbol/date and retain metric-level evidence.
export function publishYahooValuationObservations(points, observations, symbol) {
  const byDate = new Map(observations.map(row => [row.date, row]));
  return points.flatMap(point => {
    const row = byDate.get(point.date);
    const requireSession = point.date >= VALUATION_SESSION_EVIDENCE_START;
    if (!row && !requireSession) return [point];
    const next = { ...point, valuationSources: { ...point.valuationSources } };
    for (const metric of valuationMetrics) {
      const source = row?.metricSources?.[metric] || row?.source || '';
      const hasVerifiedSession = row?.metricSessionDates?.[metric] === point.date &&
        (metric !== 'pe_forward' || yahooForwardSourcePriority(source) > 0);
      if (requireSession && !hasVerifiedSession) {
        next[metric] = null;
        delete next.valuationSources[metric];
        continue;
      }
      const value = number(row[metric]);
      const explicitMissing = Object.hasOwn(row, metric) && row[metric] === null && isCurrentTable(source) && !source.includes('+');
      if ((!explicitMissing && value === null) || !source.includes('yahoo')) continue;
      next[metric] = value;
      next.valuationSources[metric] = {
        symbol, source, date: row.date, capturedAt: row.metricCapturedAt?.[metric] || row.capturedAt || '',
        ...(hasVerifiedSession ? { verifiedSession: row.date } : {}),
        basis: isCurrentTable(source) && !source.includes('+') ? 'current-table' : source.includes('quote-') ? 'quote' : 'provider-observation',
        url: source === 'yahoo-quote-api-forward'
          ? `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbol.replaceAll('.', '-'))}`
          : source === 'yahoo-quote-summary-peg'
            ? `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(symbol.replaceAll('.', '-'))}?modules=price,defaultKeyStatistics`
            : `https://finance.yahoo.com/quote/${encodeURIComponent(symbol.replaceAll('.', '-'))}/${isCurrentTable(source) ? 'key-statistics/' : ''}`,
        ...(row.comparisons?.[metric] ? { comparison: row.comparisons[metric] } : {}),
      };
    }
    if (!Object.keys(next.valuationSources).length) delete next.valuationSources;
    // A session with no verified valuation does not advance publication.
    return requireSession && !valuationMetrics.some(metric => number(next[metric]) !== null) ? [] : [next];
  });
}

export function matchesYahooPageSymbol(html, symbol) {
  for (const match of String(html).matchAll(/<link\b[^>]*>/gi)) {
    if (!/\brel=["']canonical["']/i.test(match[0])) continue;
    const identity = match[0].match(/\bhref=["'][^"']*\/quote\/([^/"'?]+)/i)?.[1];
    if (identity && decodeURIComponent(identity).toUpperCase() !== symbol.replaceAll('.', '-').toUpperCase()) return false;
  }
  return true;
}
