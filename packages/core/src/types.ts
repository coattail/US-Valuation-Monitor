export type IndexGroup = "core" | "sector";

export type MetricId = "pe_ttm" | "pe_forward" | "pb" | "earnings_yield" | "erp_proxy";

export type Regime = "low" | "neutral" | "high";

export interface IndexMeta {
  id: string;
  symbol: string;
  group: IndexGroup;
  displayName: string;
  description: string;
  order: number;
}

export interface RawValuationPoint {
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  us10y_yield: number;
}

export interface ValuationPoint extends RawValuationPoint {
  earnings_yield: number;
  erp_proxy: number;
  percentile_5y: number;
  percentile_10y: number;
  percentile_full: number;
  z_score_3y: number;
  regime: Regime;
}

export interface IndexSeries {
  indexId: string;
  points: RawValuationPoint[];
}

export interface ValuationDataset {
  generatedAt: string;
  source: string;
  indices: Array<{
    id: string;
    symbol: string;
    group: IndexGroup;
    displayName: string;
    description: string;
    forwardStartDate?: string;
    points: RawValuationPoint[];
  }>;
}

export interface SnapshotRow {
  indexId: string;
  displayName: string;
  symbol: string;
  group: IndexGroup;
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  earnings_yield: number;
  erp_proxy: number;
  percentile_5y: number;
  percentile_10y: number;
  percentile_full: number;
  z_score_3y: number;
  regime: Regime;
}

export interface PercentileBand {
  lower: number;
  upper: number;
}

export interface AlertRule {
  metric: "percentile_full";
  upper: number;
  lower: number;
  cooldownTradingDays: number;
}

export interface UserWatchlist {
  userId: string;
  watchIndexIds: string[];
  alertRule: AlertRule;
  themePreference: ThemePreference;
}

export type ThemePreference = "fresh" | "terminal";

export interface AlertEvent {
  id: string;
  userId: string;
  indexId: string;
  indexName: string;
  metric: "percentile_full";
  direction: "high" | "low";
  severity: "P1" | "P2";
  triggerDate: string;
  percentile: number;
  value: number;
  read: boolean;
  createdAt: string;
}

export interface AlertState {
  key: string;
  lastDirection: "high" | "low" | "neutral";
  lastTriggeredDate?: string;
}

export interface SeriesMetricPoint {
  date: string;
  value: number;
  percentile_5y: number;
  percentile_10y: number;
  percentile_full: number;
  regime: Regime;
}
