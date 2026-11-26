import { yahooForwardEstimateValue, YAHOO_FORWARD_ESTIMATE_BASIS } from './yahoo-valuation-policy.mjs';

// Yahoo's old table parser skipped unavailable cells and borrowed values from
// the next metric (SPCX: Forward PE -> trailing PE, Price/Sales -> PEG).
// SEC loss statement: https://www.sec.gov/Archives/edgar/data/1181412/000162828026052535/spcx-20260630.htm
export const SPCX_REPAIR_THROUGH = '2026-09-09';
export function repairCompanyMetricHistory(symbol, rows) {
  if (symbol === 'NVDA') return repairNvdaValuations(rows);
  if (symbol !== 'SPCX') return rows;
  return rows.map(row => row.date <= SPCX_REPAIR_THROUGH
    ? { ...row, pe_ttm: null, peg: null }
    : row);
}
export function preserveSpcxUnavailableMetrics(company, snapshots = []) {
  if (company?.symbol !== 'SPCX') return company;
  const anchors = repairCompanyMetricHistory('SPCX', snapshots).slice().sort((a,b)=>a.date.localeCompare(b.date));
  const points = repairCompanyMetricHistory('SPCX', company.points).map(point => {
    if (point.date <= SPCX_REPAIR_THROUGH) return point;
    // Preserve explicit provider unavailability instead of backfilling a ratio
    // from another field or a default. Never carry a positive stale PE forward.
    const anchor = anchors.findLast(row => row.date === point.date);
    return { ...point, pe_ttm: anchor?.pe_ttm ?? null, peg: anchor?.peg ?? null };
  });
  return { ...company, points, peg: points.at(-1)?.peg ?? null };
}

// Yahoo's published multiples mixed intraday and lagging snapshots for these NVDA
// sessions. Reprice using verified closes and the fundamentals already released
// on November 17. The dated Yahoo quote and Next Year EPS support Forward PE.
const nvdaCloses = {
  '2026-11-20': 125.53,
  '2026-11-23': 165.05,
  '2026-11-24': 155.38,
  '2026-11-25': 149.30,
};
const nvdaCloseUrl = 'https://finance.yahoo.com/quote/NVDA/history/';
const nvdaFundamentals = {
  pe_ttm: { denominator: 9.31, url: 'https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-third-quarter-fiscal-2027/' },
  pb: { denominator: 11.06, url: 'https://finance.yahoo.com/quote/NVDA/key-statistics/' },
};
const nvdaDatedForward = { value: 7.93, url: 'https://finance.yahoo.com/quote/NVDA/?err=1' };
const nvdaForwardConsensus = {
  asOf: '2026-11-23', eps: 16.61, fiscalYear: 2028,
  url: 'https://uk.finance.yahoo.com/quote/NVDA/analysis/?err_load=1',
};
function repairNvdaValuations(rows) {
  if (!rows.some(row => Object.hasOwn(nvdaCloses, row.date))) return rows;
  return rows.map(row => {
    const close = nvdaCloses[row.date];
    if (!close) return row;
    const ledger = Object.hasOwn(row, 'source');
    const next = { ...row };
    const dates = { ...row.metricSessionDates };
    const evidence = { ...row.valuationSources };
    const sources = { ...row.metricSources };
    for (const [metric, { denominator, url }] of Object.entries(nvdaFundamentals)) {
      next[metric] = Number((close / denominator).toFixed(2));
      sources[metric] = 'reconstructed-close-fundamentals';
      evidence[metric] = { symbol: 'NVDA', date: row.date, basis: 'close-fundamentals', source: sources[metric], url, priceUrl: nvdaCloseUrl, close, denominator };
      delete dates[metric];
    }
    const source = sources.pe_forward || evidence.pe_forward?.source || '';
    const isDatedFeed = source.startsWith('yahoo-forward-pe-timeseries') &&
      (dates.pe_forward === row.date || evidence.pe_forward?.verifiedSession === row.date) &&
      Number.isFinite(next.pe_forward);
    if (row.date === '2026-11-20') {
      next.pe_forward = nvdaDatedForward.value;
      sources.pe_forward = 'yahoo-key-statistics-valuation-measures:finance.yahoo.com';
      dates.pe_forward = row.date;
      evidence.pe_forward = { symbol: 'NVDA', date: row.date, basis: 'provider-observation',
        verifiedSession: row.date, source: sources.pe_forward, url: nvdaDatedForward.url };
      delete next.forwardEstimate;
    } else if (!isDatedFeed) {
      const estimate = { ...nvdaForwardConsensus, close };
      next.pe_forward = yahooForwardEstimateValue(row.date, estimate, 'NVDA');
      sources.pe_forward = YAHOO_FORWARD_ESTIMATE_BASIS;
      delete dates.pe_forward;
      if (ledger) next.forwardEstimate = estimate;
      evidence.pe_forward = { ...estimate, symbol: 'NVDA', date: row.date,
        basis: YAHOO_FORWARD_ESTIMATE_BASIS, source: YAHOO_FORWARD_ESTIMATE_BASIS };
    }
    if (dates.peg !== row.date && evidence.peg?.verifiedSession !== row.date) {
      next.peg = null;
      delete evidence.peg;
      delete sources.peg;
    }
    if (ledger) return { ...next, metricSources: sources, metricSessionDates: dates };
    return { ...next, valuationSources: evidence };
  });
}
