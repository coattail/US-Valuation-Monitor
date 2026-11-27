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
// on November 17. Forward P/E comes from independently dated Yahoo observations.
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
const nvdaDirectForward = {
  '2026-11-20': { value: 7.9302, source: 'yahoo-forward-pe-timeseries',
    url: 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/NVDA?symbol=NVDA&type=trailingForwardPeRatio&period1=1794528000&period2=1795824000' },
  '2026-11-23': { value: 9.30, source: 'yahoo-key-statistics-valuation-measures:finance.yahoo.com',
    url: 'https://finance.yahoo.com/quote/NVDA/?p=NVDA' },
  '2026-11-25': { value: 8.991117, source: 'yahoo-quote-api-forward',
    url: 'https://query1.finance.yahoo.com/v7/finance/quote?symbols=NVDA' },
};
const nvdaDirectPeg = {
  '2026-11-20': { value: 0.1519, source: 'yahoo-trailing-peg-timeseries',
    url: 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/NVDA?symbol=NVDA&type=trailingPegRatio&period1=1794528000&period2=1795824000' },
  '2026-11-23': { value: 0.19, source: 'yahoo-key-statistics-valuation-measures:finance.yahoo.com',
    url: 'https://finance.yahoo.com/quote/NVDA/?p=NVDA' },
  '2026-11-25': { value: 0.19, source: 'yahoo-quote-summary-peg',
    url: 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/NVDA?modules=price,defaultKeyStatistics' },
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
    const existingForwardSource = sources.pe_forward || evidence.pe_forward?.source;
    const verifiedForward = dates.pe_forward === row.date || evidence.pe_forward?.verifiedSession === row.date;
    if (!verifiedForward || !['yahoo-forward-pe-timeseries', 'yahoo-quote-api-forward'].includes(existingForwardSource)) {
      const forward = nvdaDirectForward[row.date];
      next.pe_forward = forward?.value ?? null;
      if (forward) {
        dates.pe_forward = row.date;
        sources.pe_forward = forward.source;
        evidence.pe_forward = { symbol: 'NVDA', date: row.date, verifiedSession: row.date,
          basis: 'provider-observation', source: forward.source, url: forward.url };
      } else {
        delete dates.pe_forward;
        delete sources.pe_forward;
        delete evidence.pe_forward;
      }
    }
    delete next.forwardEstimate;
    if (dates.peg !== row.date && evidence.peg?.verifiedSession !== row.date) {
      const peg = nvdaDirectPeg[row.date];
      next.peg = peg?.value ?? null;
      if (peg) {
        dates.peg = row.date;
        sources.peg = peg.source;
        evidence.peg = { symbol: 'NVDA', date: row.date, verifiedSession: row.date,
          basis: 'provider-observation', source: peg.source, url: peg.url };
      } else {
        delete evidence.peg;
        delete sources.peg;
      }
    }
    if (ledger) return { ...next, metricSources: sources, ...(Object.keys(dates).length || row.metricSessionDates ? { metricSessionDates: dates } : {}) };
    return { ...next, valuationSources: evidence };
  });
}
