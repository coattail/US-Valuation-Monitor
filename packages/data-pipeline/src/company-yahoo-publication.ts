interface Point { date: string; recovery?: { estimated?: boolean } }
interface YahooObservation { date: string; source?: string; metricSources?: Record<string, string>; pe_ttm?: unknown; pe_forward?: unknown; pb?: unknown }

export function restoreCompanyPublishedArchive<T extends Point>(points: T[], archive: { date: string; point: T }[]): T[] {
  const through = points.reduce((date, point) => point.date > date ? point.date : date, '');
  const restored = new Map(points.map(point => [point.date, point]));
  for (const record of archive) {
    // The former gap ledger is now a read-only archive. It may restore older
    // dates, but never extend the current tail or replace a source observation.
    if (record.point?.date === record.date && record.date < through && !restored.has(record.date)) restored.set(record.date, record.point);
  }
  return [...restored.values()].sort((a, b) => a.date.localeCompare(b.date));
}

// A newer close alone must not advance a company's published valuation date.
// Keep settled history, but wait for a dated Yahoo valuation observation before
// appending a session. Earlier provisional close estimates are not settled data.
export function capCompanyPublicationToYahoo<T extends Point>(
  points: T[], previousPoints: T[], observations: YahooObservation[], completedDate: string, requireCurrentTable = false
): T[] {
  let through = '';
  for (const point of previousPoints) {
    if (!point.recovery?.estimated && point.date <= completedDate && point.date > through) through = point.date;
  }
  for (const row of observations) {
    const hasMetric = [row.pe_ttm, row.pe_forward, row.pb].some(value => typeof value === 'number' && Number.isFinite(value) && value !== 0);
    const verifiedCurrent = Object.entries(row.metricSources || {}).some(([metric, source]) => source.includes(':current-table') && typeof row[metric] === 'number' && Number.isFinite(row[metric]));
    if ((!requireCurrentTable || verifiedCurrent) && row.source?.includes('yahoo') && hasMetric && row.date <= completedDate && row.date > through) through = row.date;
  }
  const published = new Map(points.filter(point => point.date <= through).map(point => [point.date, point]));
  // Sparse responses must not erase already-published historical dates. Reuse
  // those exact rows; this is preservation, not a newly estimated price path.
  for (const point of previousPoints) {
    if (point.date <= through && !published.has(point.date)) published.set(point.date, point);
  }
  return [...published.values()].sort((a, b) => a.date.localeCompare(b.date));
}
