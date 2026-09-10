// Yahoo's old table parser skipped unavailable cells and borrowed values from
// the next metric (SPCX: Forward PE -> trailing PE, Price/Sales -> PEG).
// SEC loss statement: https://www.sec.gov/Archives/edgar/data/1181412/000162828026052535/spcx-20260630.htm
export const SPCX_REPAIR_THROUGH = '2026-09-09';
export function repairCompanyMetricHistory(symbol, rows) {
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
