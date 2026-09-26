// WSJ's estimated P/E and the older blended-forward history are different
// series. Since this cutover, publish dated WSJ observations only; a price
// ratio cannot recover the missing consensus earnings estimate for a day.
export const NASDAQ_FORWARD_WSJ_START = "2026-04-10";

interface ForwardPoint { date: string; pe_forward?: unknown }
interface ForwardObservation extends ForwardPoint { source?: string }

export function applyNasdaqForwardObservationPolicy<T extends ForwardPoint>(
  points: T[], snapshots: ForwardObservation[]
): T[] {
  const observed = new Map<string, number>();
  for (const row of snapshots) {
    if (row.source === "wsj-latest" && row.date >= NASDAQ_FORWARD_WSJ_START &&
        typeof row.pe_forward === "number" && Number.isFinite(row.pe_forward) && row.pe_forward > 0) {
      observed.set(row.date, row.pe_forward);
    }
  }
  return points.map(point => point.date < NASDAQ_FORWARD_WSJ_START ? point : {
    ...point, pe_forward: observed.get(point.date) ?? null,
  });
}
