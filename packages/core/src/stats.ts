export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function percentileRank(values: number[], current: number): number {
  if (!values.length) return 0.5;
  let belowOrEqual = 0;
  for (const value of values) {
    if (value <= current) belowOrEqual += 1;
  }
  return clamp(belowOrEqual / values.length, 0, 1);
}

// Midrank empirical percentile. Giving tied observations half weight avoids
// reporting a constant series as 100% and keeps an in-sample minimum/maximum
// away from misleading absolute 0%/100% boundaries.
export function midrankPercentileRank(values: number[], current: number): number {
  if (!values.length) return 0.5;
  let below = 0;
  let equal = 0;
  for (const value of values) {
    if (value < current) below += 1;
    else if (value === current) equal += 1;
  }
  return clamp((below + equal / 2) / values.length, 0, 1);
}

export function mean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

export function stdDev(values: number[]): number {
  if (values.length <= 1) return 0;
  const avg = mean(values);
  const variance =
    values.reduce((acc, value) => acc + (value - avg) * (value - avg), 0) /
    (values.length - 1);
  return Math.sqrt(variance);
}

export function zScore(values: number[], current: number): number {
  const sigma = stdDev(values);
  if (sigma === 0) return 0;
  return (current - mean(values)) / sigma;
}

export function percentileRankWindow(
  values: number[],
  startIndex: number,
  endIndex: number,
  current: number
): number {
  const start = Math.max(0, startIndex);
  const end = Math.min(values.length - 1, endIndex);
  if (end < start) return 0.5;
  let count = 0;
  const length = end - start + 1;
  for (let i = start; i <= end; i += 1) {
    if (values[i] <= current) count += 1;
  }
  return clamp(count / length, 0, 1);
}

export function zScoreWindow(
  values: number[],
  startIndex: number,
  endIndex: number,
  current: number
): number {
  const start = Math.max(0, startIndex);
  const end = Math.min(values.length - 1, endIndex);
  if (end < start) return 0;

  const length = end - start + 1;
  if (length <= 1) return 0;

  let sum = 0;
  for (let i = start; i <= end; i += 1) {
    sum += values[i];
  }
  const avg = sum / length;

  let varianceSum = 0;
  for (let i = start; i <= end; i += 1) {
    const delta = values[i] - avg;
    varianceSum += delta * delta;
  }
  const sigma = Math.sqrt(varianceSum / (length - 1));
  if (sigma === 0) return 0;
  return (current - avg) / sigma;
}

export function rollingSlice<T>(items: T[], index: number, windowSize: number): T[] {
  const start = Math.max(0, index - windowSize + 1);
  return items.slice(start, index + 1);
}

export function businessDaysBetween(orderedDates: string[], fromDate: string, toDate: string): number {
  const startIndex = orderedDates.indexOf(fromDate);
  const endIndex = orderedDates.indexOf(toDate);
  if (startIndex < 0 || endIndex < 0 || endIndex < startIndex) return Number.POSITIVE_INFINITY;
  return endIndex - startIndex;
}
