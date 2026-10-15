interface Point { date: string; recovery?: { estimated?: boolean } }
interface YahooObservation { date: string; source?: string; pe_ttm?: unknown; pe_forward?: unknown; pb?: unknown }

// A newer close alone must not advance a company's published valuation date.
// Keep settled history, but wait for a dated Yahoo valuation observation before
// appending a session. Earlier provisional close estimates are not settled data.
export function capCompanyPublicationToYahoo<T extends Point>(
  points: T[], previousPoints: Point[], observations: YahooObservation[], completedDate: string
): T[] {
  let through = '';
  for (const point of previousPoints) {
    if (!point.recovery?.estimated && point.date <= completedDate && point.date > through) through = point.date;
  }
  for (const row of observations) {
    const hasMetric = [row.pe_ttm, row.pe_forward, row.pb].some(value => typeof value === 'number' && Number.isFinite(value) && value !== 0);
    if (row.source?.includes('yahoo') && hasMetric && row.date <= completedDate && row.date > through) through = row.date;
  }
  return points.filter(point => point.date <= through);
}
