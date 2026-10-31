export type IndexMetric = 'pe_ttm' | 'pe_forward' | 'pb';
export interface MetricSourceRow { source?: string; metricSources?: Partial<Record<IndexMetric, string>> }
export const indexMetricSource = (row: MetricSourceRow, metric: IndexMetric) => {
  const source = row.metricSources?.[metric] || row.source || '';
  return metric === 'pb' && source === 'wsj-latest' ? '' : source; // WSJ's P/E table has no P/B field.
};
export const datedIndexSource = (source: string) => ['wsj-latest', 'ssga-official-latest', 'ishares-official-latest'].includes(source);
export const sourcePriority = (source: string) => source === 'wsj-latest' ? 100 : datedIndexSource(source) ? 80 : source === 'stockanalysis-latest' ? 50 : 0;

// A row may combine WSJ P/E with a fund provider's P/B. Keep provenance per
// field so an unrelated partial update cannot relabel or replace its anchors.
export function mergeIndexSourceSnapshot<T extends MetricSourceRow & { date: string; [key: string]: any }>(existing: T | undefined, incoming: T): T {
  const next = { ...incoming };
  const sources: Partial<Record<IndexMetric, string>> = {};
  for (const metric of ['pe_ttm', 'pe_forward', 'pb'] as const) {
    const candidate = incoming[metric], previous = existing?.[metric];
    const hasCandidate = typeof candidate === 'number' && Number.isFinite(candidate) && candidate > 0;
    const hasPrevious = typeof previous === 'number' && Number.isFinite(previous) && previous > 0;
    const incomingSource = indexMetricSource(incoming, metric), oldSource = existing ? indexMetricSource(existing, metric) : '';
    const useIncoming = hasCandidate && (!hasPrevious || sourcePriority(incomingSource) >= sourcePriority(oldSource));
    Object.assign(next, { [metric]: useIncoming ? candidate : hasPrevious ? previous : null });
    if (useIncoming || hasPrevious) sources[metric] = useIncoming ? incomingSource : oldSource;
  }
  if (Object.values(sources).some(source => source !== next.source)) next.metricSources = sources;
  else delete next.metricSources;
  return next;
}
