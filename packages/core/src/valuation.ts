import { clamp, percentileRankWindow, zScoreWindow } from "./stats.ts";
import type { MetricId, RawValuationPoint, Regime, SeriesMetricPoint, SnapshotRow, ValuationPoint } from "./types.ts";
import { INDEX_MAP } from "./constants.ts";

const TRADING_DAYS_PER_YEAR = 252;
const NEGATIVE_VALUATION_BASE = 1_000_000;
const NEGATIVE_VALUATION_EPSILON = 1e-6;

function resolveRegime(percentileFull: number): Regime {
  if (percentileFull <= 0.15) return "low";
  if (percentileFull >= 0.85) return "high";
  return "neutral";
}

function metricUsesNegativeAwareRanking(metric: MetricId): boolean {
  return metric === "pe_ttm" || metric === "pe_forward" || metric === "pb";
}

function valuationRankValue(value: number): number {
  if (value >= 0) return value;
  const abs = Math.max(Math.abs(value), NEGATIVE_VALUATION_EPSILON);
  return NEGATIVE_VALUATION_BASE + 1 / abs;
}

function rankMetricValue(metric: MetricId, value: number): number {
  return metricUsesNegativeAwareRanking(metric) ? valuationRankValue(value) : value;
}

export function enrichSeries(points: RawValuationPoint[]): ValuationPoint[] {
  const result: ValuationPoint[] = [];
  const peSeries: number[] = [];
  const peRankSeries: number[] = [];
  const window5 = TRADING_DAYS_PER_YEAR * 5;
  const window10 = TRADING_DAYS_PER_YEAR * 10;
  const window3 = TRADING_DAYS_PER_YEAR * 3;

  for (let i = 0; i < points.length; i += 1) {
    const point = points[i];
    peSeries.push(point.pe_ttm);
    const peRankValue = valuationRankValue(point.pe_ttm);
    peRankSeries.push(peRankValue);

    const percentile5y = percentileRankWindow(peRankSeries, i - window5 + 1, i, peRankValue);
    const percentile10y = percentileRankWindow(peRankSeries, i - window10 + 1, i, peRankValue);
    const percentileFull = percentileRankWindow(peRankSeries, 0, i, peRankValue);

    const earningsYield = point.pe_ttm > 0 ? 1 / point.pe_ttm : 0;
    const erpProxy = earningsYield - point.us10y_yield;

    result.push({
      ...point,
      earnings_yield: clamp(earningsYield, -1, 1),
      erp_proxy: clamp(erpProxy, -1, 1),
      percentile_5y: percentile5y,
      percentile_10y: percentile10y,
      percentile_full: percentileFull,
      z_score_3y: zScoreWindow(peSeries, i - window3 + 1, i, point.pe_ttm),
      regime: resolveRegime(percentileFull),
    });
  }

  return result;
}

function pickMetricValue(point: ValuationPoint, metric: MetricId): number {
  return point[metric];
}

export function buildMetricSeries(
  points: RawValuationPoint[],
  metric: MetricId,
  fromDate?: string,
  toDate?: string
): SeriesMetricPoint[] {
  const enriched = enrichSeries(points);
  const values: number[] = [];
  const rankValues: number[] = [];
  const rows: SeriesMetricPoint[] = [];
  const window5 = TRADING_DAYS_PER_YEAR * 5;
  const window10 = TRADING_DAYS_PER_YEAR * 10;

  for (let i = 0; i < enriched.length; i += 1) {
    const point = enriched[i];
    if ((fromDate && point.date < fromDate) || (toDate && point.date > toDate)) {
      continue;
    }

    const metricValue = pickMetricValue(point, metric);
    const rankValue = rankMetricValue(metric, metricValue);
    values.push(metricValue);
    rankValues.push(rankValue);
    const valueIndex = values.length - 1;

    const percentile5 = percentileRankWindow(rankValues, valueIndex - window5 + 1, valueIndex, rankValue);
    const percentile10 = percentileRankWindow(rankValues, valueIndex - window10 + 1, valueIndex, rankValue);
    const percentileFull = percentileRankWindow(rankValues, 0, valueIndex, rankValue);

    rows.push({
      date: point.date,
      value: metricValue,
      percentile_5y: percentile5,
      percentile_10y: percentile10,
      percentile_full: percentileFull,
      regime: resolveRegime(percentileFull),
    });
  }

  return rows;
}

export function buildSnapshot(indexId: string, points: RawValuationPoint[]): SnapshotRow {
  const meta = INDEX_MAP[indexId];
  if (!meta) {
    throw new Error(`Unknown index id: ${indexId}`);
  }
  const enriched = enrichSeries(points);
  const latest = enriched.at(-1);
  if (!latest) {
    throw new Error(`No data for index: ${indexId}`);
  }

  return {
    indexId,
    displayName: meta.displayName,
    symbol: meta.symbol,
    group: meta.group,
    date: latest.date,
    pe_ttm: latest.pe_ttm,
    pe_forward: latest.pe_forward,
    pb: latest.pb,
    earnings_yield: latest.earnings_yield,
    erp_proxy: latest.erp_proxy,
    percentile_5y: latest.percentile_5y,
    percentile_10y: latest.percentile_10y,
    percentile_full: latest.percentile_full,
    z_score_3y: latest.z_score_3y,
    regime: latest.regime,
  };
}

export function resolveDateRange(points: RawValuationPoint[]): { startDate: string; endDate: string; pointCount: number } {
  if (!points.length) {
    return { startDate: "", endDate: "", pointCount: 0 };
  }
  return {
    startDate: points[0].date,
    endDate: points[points.length - 1].date,
    pointCount: points.length,
  };
}
