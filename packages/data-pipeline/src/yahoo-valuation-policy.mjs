export const valuationMetrics = ['pe_ttm', 'pe_forward', 'pb', 'peg'];
const text = html => String(html).replace(/<sup\b[^>]*>[\s\S]*?<\/sup>/gi, '')
  .replace(/<[^>]*>/g, ' ').replace(/&(?:nbsp|#160);/gi, ' ').replace(/&amp;/gi, '&')
  .replace(/\s+/g, ' ').trim();
const number = value => typeof value === 'number' && Number.isFinite(value) && value !== 0 ? value : null;
const labels = { 'trailing p/e': 'pe_ttm', 'forward p/e': 'pe_forward', 'price/book': 'pb',
  'price/book (mrq)': 'pb', 'peg ratio (current)': 'peg', 'peg ratio (5yr expected)': 'peg',
  'peg ratio (5 yr expected)': 'peg' };

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

// This is the last publication step, after every repair and fallback. Apply
// observations only to their own symbol/date and retain metric-level evidence.
export function publishYahooValuationObservations(points, observations, symbol) {
  const byDate = new Map(observations.map(row => [row.date, row]));
  return points.map(point => {
    const row = byDate.get(point.date);
    if (!row) return point;
    const next = { ...point, valuationSources: { ...point.valuationSources } };
    for (const metric of valuationMetrics) {
      const value = number(row[metric]);
      const source = row.metricSources?.[metric] || row.source || '';
      if (value === null || !source.includes('yahoo')) continue;
      next[metric] = value;
      next.valuationSources[metric] = {
        symbol, source, date: row.date, capturedAt: row.capturedAt || '',
        basis: isCurrentTable(source) && !source.includes('+') ? 'current-table' : source.includes('quote-') ? 'quote' : 'provider-observation',
        url: `https://finance.yahoo.com/quote/${encodeURIComponent(symbol.replaceAll('.', '-'))}/${isCurrentTable(source) ? 'key-statistics/' : ''}`,
        ...(row.comparisons?.[metric] ? { comparison: row.comparisons[metric] } : {}),
      };
    }
    if (!Object.keys(next.valuationSources).length) delete next.valuationSources;
    return next;
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
