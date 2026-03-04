import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, readFile, writeFile } from "node:fs/promises";

import {
  ALL_INDICES,
  DEFAULT_ALERT_RULE,
  DEFAULT_THEME,
  INDEX_MAP,
  allowByCooldown,
  buildMetricSeries,
  buildSnapshot,
  detectCrossing,
  resolveDateRange,
  resolveDirection,
  severityFromPercentile,
} from "../../packages/core/src/index.ts";
import type {
  AlertEvent,
  AlertRule,
  AlertState,
  MetricId,
  RawValuationPoint,
  UserWatchlist,
  ValuationDataset,
} from "../../packages/core/src/index.ts";
import { generateDataset, validateDataset } from "../../packages/data-pipeline/src/generate.ts";

const CURRENT_FILE = fileURLToPath(import.meta.url);
const DEFAULT_ROOT = path.resolve(path.dirname(CURRENT_FILE), "../..");
const DATA_FILE = ["data", "standardized", "valuation-history.json"];
const COMPANY_DATA_FILE = ["data", "standardized", "company-valuation-history.json"];
const WATCHLIST_FILE = ["data", "runtime", "watchlists.json"];
const ALERTS_FILE = ["data", "runtime", "alerts.json"];
const ALERT_STATE_FILE = ["data", "runtime", "alert-state.json"];
const execFileAsync = promisify(execFile);
type CompanyMetricId = MetricId;
const METRIC_SET = new Set<MetricId>([
  "pe_ttm",
  "pe_forward",
  "pb",
  "earnings_yield",
]);
const COMPANY_METRIC_SET = new Set<CompanyMetricId>([
  "pe_ttm",
  "pe_forward",
  "pb",
  "earnings_yield",
]);

interface CompanyValuationPoint extends RawValuationPoint {
  close?: number;
}

interface CompanyDatasetIndex {
  id: string;
  symbol: string;
  displayName: string;
  description: string;
  forwardStartDate?: string;
  rank?: number;
  marketCap?: number;
  peg?: number | null;
  points: CompanyValuationPoint[];
  quarterlyNetIncome?: Array<{ date: string; netIncome: number; source?: string }>;
  quarterlyEps?: Array<{ date: string; eps: number; source?: string }>;
}

interface CompanyValuationDataset {
  generatedAt: string;
  source?: string;
  indices: CompanyDatasetIndex[];
}

type UserMap<T> = Record<string, T>;

export interface WatchlistStore {
  users: UserMap<UserWatchlist>;
}

export interface AlertStore {
  users: UserMap<AlertEvent[]>;
}

export interface AlertStateStore {
  users: UserMap<Record<string, AlertState>>;
}

export interface RuntimeStores {
  watchlists: WatchlistStore;
  alerts: AlertStore;
  alertState: AlertStateStore;
}

export interface ApiServerOptions {
  rootDir?: string;
}

const DATASET_CACHE_TTL_MS = 30_000;

interface DatasetCacheEntry<T> {
  rootDir: string;
  expiresAt: number;
  payload: T;
}

let indexDatasetCache: DatasetCacheEntry<ValuationDataset> | null = null;
let companyDatasetCache: DatasetCacheEntry<CompanyValuationDataset> | null = null;
let companySnapshotPayloadCache: { cacheKey: string; payload: ReturnType<typeof buildCompanySnapshotPayload> } | null =
  null;

function resolvePath(rootDir: string, segments: string[]): string {
  return path.join(rootDir, ...segments);
}

function parseJsonSafely<T>(text: string, fallback: T): T {
  try {
    return JSON.parse(text) as T;
  } catch {
    return fallback;
  }
}

async function ensureDirFor(filePath: string): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
}

async function readJsonFile<T>(filePath: string, fallback: T): Promise<T> {
  try {
    const content = await readFile(filePath, "utf8");
    return parseJsonSafely(content, fallback);
  } catch {
    return fallback;
  }
}

async function writeJsonFile(filePath: string, payload: unknown): Promise<void> {
  await ensureDirFor(filePath);
  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function todayIsoDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function json(res: http.ServerResponse, statusCode: number, payload: unknown): void {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,X-Dev-Token",
  });
  res.end(JSON.stringify(payload));
}

async function readBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return {};
  }
}

function getUserId(req: http.IncomingMessage): string {
  const token = String(req.headers["x-dev-token"] || "").trim();
  if (!token) return "demo-user";
  if (token.startsWith("dev-token:")) {
    return token.split(":").slice(1).join(":") || "demo-user";
  }
  return "demo-user";
}

export function defaultWatchlist(userId: string): UserWatchlist {
  return {
    userId,
    watchIndexIds: ALL_INDICES.slice(0, 6).map((item) => item.id),
    alertRule: { ...DEFAULT_ALERT_RULE },
    themePreference: DEFAULT_THEME,
  };
}

export function normalizeWatchlist(input: Partial<UserWatchlist> | null | undefined, userId: string): UserWatchlist {
  const ruleSource = input?.alertRule;
  const alertRule: AlertRule = {
    metric: "percentile_full",
    upper: Number(ruleSource?.upper ?? DEFAULT_ALERT_RULE.upper),
    lower: Number(ruleSource?.lower ?? DEFAULT_ALERT_RULE.lower),
    cooldownTradingDays: Number(ruleSource?.cooldownTradingDays ?? DEFAULT_ALERT_RULE.cooldownTradingDays),
  };

  const watchSet = new Set<string>();
  for (const id of input?.watchIndexIds || []) {
    if (INDEX_MAP[id]) watchSet.add(id);
  }
  if (!watchSet.size) {
    for (const id of defaultWatchlist(userId).watchIndexIds) {
      watchSet.add(id);
    }
  }

  return {
    userId,
    watchIndexIds: [...watchSet],
    alertRule,
    themePreference: input?.themePreference === "terminal" ? "terminal" : "fresh",
  };
}

function findIndexSeries(dataset: ValuationDataset, indexId: string): RawValuationPoint[] {
  const index = dataset.indices.find((item) => item.id === indexId);
  if (!index) {
    throw new Error(`Unknown indexId: ${indexId}`);
  }
  return index.points;
}

function stripCompanyPointGrowthFields(point: CompanyValuationPoint): CompanyValuationPoint {
  const nextPoint = { ...(point as Record<string, unknown>) };
  delete nextPoint.close;
  return nextPoint as CompanyValuationPoint;
}

export async function loadDataset(rootDir: string): Promise<ValuationDataset> {
  const dataPath = resolvePath(rootDir, DATA_FILE);
  const payload = await readJsonFile<ValuationDataset | null>(dataPath, null);

  if (payload) {
    try {
      validateDataset(payload);
      return payload;
    } catch {
      // invalid existing payload; rebuild below
    }
  }

  const fallback = await generateDataset(todayIsoDate());
  validateDataset(fallback);
  await writeJsonFile(dataPath, fallback);
  return fallback;
}

export async function loadCompanyDataset(rootDir: string): Promise<CompanyValuationDataset> {
  const dataPath = resolvePath(rootDir, COMPANY_DATA_FILE);
  const payload = await readJsonFile<CompanyValuationDataset | null>(dataPath, null);
  if (!payload || !Array.isArray(payload.indices)) {
    return {
      generatedAt: "",
      source: "company-snapshot",
      indices: [],
    };
  }

  const normalizedIndices = payload.indices
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      ...item,
      points: Array.isArray(item.points) ? item.points.map((point) => stripCompanyPointGrowthFields(point)) : [],
    }))
    .filter((item) => item.id && item.symbol && item.displayName);

  return {
    generatedAt: String(payload.generatedAt || ""),
    source: payload.source ? String(payload.source) : "company-snapshot",
    indices: normalizedIndices,
  };
}

function cacheStillValid<T>(cache: DatasetCacheEntry<T> | null, rootDir: string): cache is DatasetCacheEntry<T> {
  return Boolean(cache && cache.rootDir === rootDir && cache.expiresAt > Date.now());
}

function invalidateDatasetCaches(): void {
  indexDatasetCache = null;
  companyDatasetCache = null;
  companySnapshotPayloadCache = null;
}

async function loadDatasetCached(rootDir: string): Promise<ValuationDataset> {
  if (cacheStillValid(indexDatasetCache, rootDir)) {
    return indexDatasetCache.payload;
  }
  const payload = await loadDataset(rootDir);
  indexDatasetCache = {
    rootDir,
    expiresAt: Date.now() + DATASET_CACHE_TTL_MS,
    payload,
  };
  return payload;
}

async function loadCompanyDatasetCached(rootDir: string): Promise<CompanyValuationDataset> {
  if (cacheStillValid(companyDatasetCache, rootDir)) {
    return companyDatasetCache.payload;
  }
  const payload = await loadCompanyDataset(rootDir);
  companyDatasetCache = {
    rootDir,
    expiresAt: Date.now() + DATASET_CACHE_TTL_MS,
    payload,
  };
  return payload;
}

export async function loadStores(rootDir: string): Promise<RuntimeStores> {
  const watchlistPath = resolvePath(rootDir, WATCHLIST_FILE);
  const alertsPath = resolvePath(rootDir, ALERTS_FILE);
  const statePath = resolvePath(rootDir, ALERT_STATE_FILE);

  const watchlists = await readJsonFile<WatchlistStore>(watchlistPath, { users: {} });
  const alerts = await readJsonFile<AlertStore>(alertsPath, { users: {} });
  const alertState = await readJsonFile<AlertStateStore>(statePath, { users: {} });

  return { watchlists, alerts, alertState };
}

export async function saveStores(rootDir: string, stores: RuntimeStores): Promise<void> {
  await writeJsonFile(resolvePath(rootDir, WATCHLIST_FILE), stores.watchlists);
  await writeJsonFile(resolvePath(rootDir, ALERTS_FILE), stores.alerts);
  await writeJsonFile(resolvePath(rootDir, ALERT_STATE_FILE), stores.alertState);
}

export function buildMeta(dataset: ValuationDataset) {
  return {
    generatedAt: dataset.generatedAt,
    source: dataset.source,
    indices: dataset.indices.map((item) => ({
      id: item.id,
      symbol: item.symbol,
      group: item.group,
      displayName: item.displayName,
      description: item.description,
      forwardStartDate: item.forwardStartDate,
      ...resolveDateRange(item.points),
    })),
  };
}

export function buildSnapshotPayload(dataset: ValuationDataset, group?: string) {
  const rows = dataset.indices
    .filter((item) => (group === "core" || group === "sector" ? item.group === group : true))
    .map((item) => buildSnapshot(item.id, item.points))
    .sort((a, b) => b.percentile_full - a.percentile_full);

  return {
    generatedAt: dataset.generatedAt,
    rows,
  };
}

export function buildHeatmapPayload(dataset: ValuationDataset, group?: string) {
  const rows = dataset.indices
    .filter((item) => (group === "core" || group === "sector" ? item.group === group : true))
    .map((item) => {
      const snap = buildSnapshot(item.id, item.points);
      return {
        indexId: snap.indexId,
        displayName: snap.displayName,
        group: snap.group,
        percentile_full: snap.percentile_full,
        percentile_10y: snap.percentile_10y,
        percentile_5y: snap.percentile_5y,
        regime: snap.regime,
      };
    })
    .sort((a, b) => (INDEX_MAP[a.indexId].order || 0) - (INDEX_MAP[b.indexId].order || 0));

  return {
    generatedAt: dataset.generatedAt,
    rows,
  };
}

export function buildSeriesPayload(
  dataset: ValuationDataset,
  indexId: string,
  metric: MetricId,
  fromDate?: string,
  toDate?: string
) {
  if (!METRIC_SET.has(metric)) {
    throw new Error(`Invalid metric: ${metric}`);
  }
  const points = findIndexSeries(dataset, indexId);
  const meta = dataset.indices.find((item) => item.id === indexId);
  const forwardStartDate = metric === "pe_forward" ? meta?.forwardStartDate : undefined;
  const effectiveFrom = metric === "pe_forward" ? (fromDate && forwardStartDate ? (fromDate > forwardStartDate ? fromDate : forwardStartDate) : fromDate || forwardStartDate) : fromDate;

  const rows = buildMetricSeries(points, metric, effectiveFrom, toDate);
  const range = resolveDateRange(points);
  return {
    generatedAt: dataset.generatedAt,
    indexId,
    metric,
    forwardStartDate,
    availableRange: range,
    rows,
  };
}

function subtractYears(dateText: string, years: number): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) return dateText;
  const date = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return dateText;
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return date.toISOString().slice(0, 10);
}

function toFiniteNumber(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

const NEGATIVE_VALUATION_BASE = 1_000_000;
const NEGATIVE_VALUATION_EPSILON = 1e-6;

function valuationRankValue(value: number): number {
  if (value >= 0) return value;
  const abs = Math.max(Math.abs(value), NEGATIVE_VALUATION_EPSILON);
  return NEGATIVE_VALUATION_BASE + 1 / abs;
}

function findCompanyIndex(dataset: CompanyValuationDataset, indexId: string): CompanyDatasetIndex | null {
  return dataset.indices.find((item) => item.id === indexId) || null;
}

function findPeReferenceRow(rows: Array<{ date: string; value: number }>, targetDate: string) {
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    if (rows[i].date <= targetDate) return rows[i];
  }
  for (let i = 0; i < rows.length; i += 1) {
    if (rows[i].date >= targetDate) return rows[i];
  }
  return rows[0] || null;
}

function regimeFromPercentile(percentile: number): "high" | "low" | "neutral" {
  if (percentile >= 0.85) return "high";
  if (percentile <= 0.15) return "low";
  return "neutral";
}

function computeLatestCompanyPeStats(points: CompanyValuationPoint[]) {
  const validRows = (Array.isArray(points) ? points : [])
    .map((point) => ({
      date: String(point?.date || ""),
      value: Number(point?.pe_ttm),
    }))
    .filter((row) => /^\d{4}-\d{2}-\d{2}$/.test(row.date) && Number.isFinite(row.value));

  if (!validRows.length) {
    return {
      latestDate: "",
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      regime: "neutral" as const,
      pe_ttm_change_1y: 0,
    };
  }

  const latest = validRows[validRows.length - 1];
  const latestDate = latest.date;
  const latestValue = latest.value;
  const latestRankValue = valuationRankValue(latestValue);
  const cutoff5 = subtractYears(latestDate, 5);
  const cutoff10 = subtractYears(latestDate, 10);
  const cutoff3 = subtractYears(latestDate, 3);
  const lookbackDate = subtractYears(latestDate, 1);

  const rowsFromCutoff = (cutoffDate: string) =>
    validRows.filter((row) => row.date >= cutoffDate && row.date <= latestDate);
  const rows5 = rowsFromCutoff(cutoff5);
  const rows10 = rowsFromCutoff(cutoff10);
  const rows3 = rowsFromCutoff(cutoff3);

  const percentileFromRows = (rows: Array<{ date: string; value: number }>) => {
    if (!rows.length) return 0.5;
    const count = rows.filter((row) => valuationRankValue(row.value) <= latestRankValue).length;
    return Math.max(0, Math.min(1, count / rows.length));
  };

  const percentileFull = percentileFromRows(validRows);
  const percentile5 = percentileFromRows(rows5);
  const percentile10 = percentileFromRows(rows10);

  let zScore3y = 0;
  if (rows3.length > 1) {
    const values = rows3.map((row) => row.value);
    const sum = values.reduce((acc, value) => acc + value, 0);
    const avg = sum / values.length;
    const variance =
      values.reduce((acc, value) => acc + (value - avg) * (value - avg), 0) / (values.length - 1);
    const sigma = Math.sqrt(variance);
    if (sigma > 1e-12) {
      zScore3y = (latestValue - avg) / sigma;
    }
  }

  const peReference = findPeReferenceRow(validRows, lookbackDate);
  const referenceValue = Number(peReference?.value);
  const peChange1y =
    Number.isFinite(referenceValue) && Math.abs(referenceValue) > 1e-12
      ? (latestValue - referenceValue) / Math.abs(referenceValue)
      : 0;

  return {
    latestDate,
    percentile_5y: percentile5,
    percentile_10y: percentile10,
    percentile_full: percentileFull,
    z_score_3y: zScore3y,
    regime: regimeFromPercentile(percentile10),
    pe_ttm_change_1y: peChange1y,
  };
}

export function buildCompanyMeta(dataset: CompanyValuationDataset) {
  return {
    generatedAt: dataset.generatedAt,
    source: dataset.source || "company-snapshot",
    indices: dataset.indices.map((item) => ({
      id: item.id,
      symbol: item.symbol,
      group: "company",
      displayName: item.displayName,
      description: item.description,
      rank: toFiniteNumber(item.rank, 9999),
      marketCap: toFiniteNumber(item.marketCap, 0),
      forwardStartDate: item.forwardStartDate,
      ...resolveDateRange(item.points),
    })),
  };
}

function buildCompanySnapshotRow(item: CompanyDatasetIndex) {
  const points = Array.isArray(item.points) ? item.points : [];
  const latestRaw = points[points.length - 1] || null;
  const latestPe = toFiniteNumber(latestRaw?.pe_ttm, NaN);
  const peStats = computeLatestCompanyPeStats(points);

  return {
    indexId: item.id,
    symbol: item.symbol,
    displayName: item.displayName,
    group: "company",
    rank: toFiniteNumber(item.rank, 9999),
    marketCap: toFiniteNumber(item.marketCap, 0),
    date: peStats.latestDate || String(latestRaw?.date || ""),
    pe_ttm: toFiniteNumber(latestRaw?.pe_ttm, 0),
    pe_forward: toFiniteNumber(latestRaw?.pe_forward, 0),
    pb: toFiniteNumber(latestRaw?.pb, 0),
    peg: Number.isFinite(Number(item.peg)) ? Number(item.peg) : null,
    earnings_yield:
      Number.isFinite(latestPe) && Math.abs(latestPe) > 1e-12 ? Number((1 / latestPe).toFixed(8)) : 0,
    percentile_5y: peStats.percentile_5y,
    percentile_10y: peStats.percentile_10y,
    percentile_full: peStats.percentile_full,
    z_score_3y: peStats.z_score_3y,
    regime: peStats.regime,
    pe_ttm_change_1y: peStats.pe_ttm_change_1y,
    startDate: points[0]?.date || "",
    endDate: points[points.length - 1]?.date || "",
    pointCount: points.length,
  };
}

export function buildCompanySnapshotPayload(dataset: CompanyValuationDataset) {
  const rows = dataset.indices
    .map((item) => buildCompanySnapshotRow(item))
    .sort((a, b) => {
      const capDiff = Number(b.marketCap || 0) - Number(a.marketCap || 0);
      if (capDiff !== 0) return capDiff;
      const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
      if (rankDiff !== 0) return rankDiff;
      return String(a.displayName || "").localeCompare(String(b.displayName || ""));
    });

  return {
    generatedAt: dataset.generatedAt,
    rows,
  };
}

function companySnapshotCacheKey(dataset: CompanyValuationDataset): string {
  const count = Array.isArray(dataset.indices) ? dataset.indices.length : 0;
  const first = count ? dataset.indices[0].id : "";
  const last = count ? dataset.indices[count - 1].id : "";
  return `${dataset.generatedAt || "na"}|${count}|${first}|${last}`;
}

function buildCompanySnapshotPayloadCached(dataset: CompanyValuationDataset) {
  const cacheKey = companySnapshotCacheKey(dataset);
  if (companySnapshotPayloadCache && companySnapshotPayloadCache.cacheKey === cacheKey) {
    return companySnapshotPayloadCache.payload;
  }
  const payload = buildCompanySnapshotPayload(dataset);
  companySnapshotPayloadCache = { cacheKey, payload };
  return payload;
}

export function buildCompanySeriesPayload(
  dataset: CompanyValuationDataset,
  indexId: string,
  metric: CompanyMetricId,
  fromDate?: string,
  toDate?: string
) {
  if (!COMPANY_METRIC_SET.has(metric)) {
    throw new Error(`Invalid metric: ${metric}`);
  }

  const index = findCompanyIndex(dataset, indexId);
  if (!index) {
    throw new Error(`Invalid indexId`);
  }

  const forwardStartDate = metric === "pe_forward" ? index.forwardStartDate : undefined;
  const effectiveFrom =
    metric === "pe_forward"
      ? fromDate && forwardStartDate
        ? fromDate > forwardStartDate
          ? fromDate
          : forwardStartDate
        : fromDate || forwardStartDate
      : fromDate;

  const rows = buildMetricSeries(index.points, metric, effectiveFrom, toDate);
  const range = resolveDateRange(index.points);
  return {
    generatedAt: dataset.generatedAt,
    indexId,
    metric,
    forwardStartDate,
    availableRange: range,
    rows,
  };
}

function nextAlertId(userId: string, indexId: string, date: string): string {
  return `${userId}-${indexId}-${date}-${Math.random().toString(36).slice(2, 8)}`;
}

export async function applyDailyUpdate(
  dataset: ValuationDataset,
  stores: RuntimeStores,
  options: {
    endDate?: string;
    generateDatasetFn?: (endDate?: string) => Promise<ValuationDataset> | ValuationDataset;
  } = {}
): Promise<{ createdAlerts: number; dataset: ValuationDataset }> {
  const generator = options.generateDatasetFn || generateDataset;
  const updatedDataset = await generator(options.endDate || todayIsoDate());
  validateDataset(updatedDataset);

  let createdAlerts = 0;

  for (const [userId, watchlist] of Object.entries(stores.watchlists.users)) {
    const normalized = normalizeWatchlist(watchlist, userId);
    stores.watchlists.users[userId] = normalized;

    const userAlerts = stores.alerts.users[userId] || [];
    const userState = stores.alertState.users[userId] || {};

    for (const indexId of normalized.watchIndexIds) {
      const points = findIndexSeries(updatedDataset, indexId);
      const metricRows = buildMetricSeries(points, "pe_ttm");
      if (metricRows.length < 2) continue;

      const previous = metricRows[metricRows.length - 2];
      const current = metricRows[metricRows.length - 1];
      const alertDirection = detectCrossing(previous.percentile_full, current.percentile_full, normalized.alertRule);
      const stateKey = `${indexId}:${normalized.alertRule.metric}`;
      const currentState =
        userState[stateKey] ||
        ({ key: stateKey, lastDirection: "neutral" } satisfies AlertState);

      const dates = points.map((item) => item.date);
      const cooldownReady = allowByCooldown(
        dates,
        currentState.lastTriggeredDate,
        current.date,
        normalized.alertRule.cooldownTradingDays
      );

      if (alertDirection && cooldownReady) {
        const snapshot = buildSnapshot(indexId, points);
        const alert: AlertEvent = {
          id: nextAlertId(userId, indexId, current.date),
          userId,
          indexId,
          indexName: snapshot.displayName,
          metric: "percentile_full",
          direction: alertDirection,
          severity: severityFromPercentile(current.percentile_full),
          triggerDate: current.date,
          percentile: Number(current.percentile_full.toFixed(4)),
          value: Number(current.value.toFixed(4)),
          read: false,
          createdAt: new Date().toISOString(),
        };

        userAlerts.unshift(alert);
        currentState.lastTriggeredDate = current.date;
        currentState.lastDirection = alertDirection;
        createdAlerts += 1;
      } else {
        currentState.lastDirection = resolveDirection(current.percentile_full, normalized.alertRule);
      }

      userState[stateKey] = currentState;
    }

    stores.alerts.users[userId] = userAlerts.slice(0, 500);
    stores.alertState.users[userId] = userState;
  }

  return { createdAlerts, dataset: updatedDataset };
}

function normalizeCompanySymbols(rawSymbols: unknown): string[] {
  if (!Array.isArray(rawSymbols)) return [];
  return [
    ...new Set(
      rawSymbols
        .map((item) => String(item || "").trim().toUpperCase())
        .filter((item) => /^[A-Z0-9.\-]{1,16}$/.test(item))
    ),
  ];
}

async function runCompanyDataRefresh(
  rootDir: string,
  symbols: string[]
): Promise<{
  generatedAt: string;
  seriesCount: number;
  mode: "full" | "filtered";
  symbols: string[];
}> {
  const scriptPath = path.join(rootDir, "packages", "data-pipeline", "src", "build-company-snapshot.ts");
  const normalizedSymbols = normalizeCompanySymbols(symbols);
  const env = { ...process.env };
  if (normalizedSymbols.length) {
    env.COMPANY_SYMBOLS = normalizedSymbols.join(",");
  }

  await execFileAsync("node", [scriptPath], {
    cwd: rootDir,
    env,
    maxBuffer: 20 * 1024 * 1024,
  });

  const companyDataset = await readJsonFile<{
    generatedAt?: string;
    indices?: unknown[];
  }>(resolvePath(rootDir, COMPANY_DATA_FILE), {
    generatedAt: "",
    indices: [],
  });

  return {
    generatedAt: String(companyDataset.generatedAt || ""),
    seriesCount: Array.isArray(companyDataset.indices) ? companyDataset.indices.length : 0,
    mode: normalizedSymbols.length ? "filtered" : "full",
    symbols: normalizedSymbols,
  };
}

export function createApiServer(options: ApiServerOptions = {}): http.Server {
  const rootDir = options.rootDir || DEFAULT_ROOT;

  return http.createServer(async (req, res) => {
    if (!req.url) {
      json(res, 404, { error: "Not found" });
      return;
    }

    if (req.method === "OPTIONS") {
      json(res, 200, { ok: true });
      return;
    }

    const url = new URL(req.url, "http://127.0.0.1");

    try {
      const userId = getUserId(req);
      let datasetPromise: Promise<ValuationDataset> | null = null;
      let companyDatasetPromise: Promise<CompanyValuationDataset> | null = null;
      let storesPromise: Promise<RuntimeStores> | null = null;

      const getDataset = async () => {
        if (!datasetPromise) {
          datasetPromise = loadDatasetCached(rootDir);
        }
        return datasetPromise;
      };

      const getCompanyDataset = async () => {
        if (!companyDatasetPromise) {
          companyDatasetPromise = loadCompanyDatasetCached(rootDir);
        }
        return companyDatasetPromise;
      };

      const getStores = async () => {
        if (!storesPromise) {
          storesPromise = loadStores(rootDir);
        }
        return storesPromise;
      };

      if (url.pathname === "/healthz" && req.method === "GET") {
        json(res, 200, { ok: true, now: new Date().toISOString() });
        return;
      }

      if (url.pathname === "/api/meta" && req.method === "GET") {
        const dataset = await getDataset();
        json(res, 200, buildMeta(dataset));
        return;
      }

      if (url.pathname === "/api/snapshot" && req.method === "GET") {
        const group = url.searchParams.get("group") || undefined;
        const dataset = await getDataset();
        json(res, 200, buildSnapshotPayload(dataset, group || undefined));
        return;
      }

      if (url.pathname === "/api/series" && req.method === "GET") {
        const indexId = String(url.searchParams.get("indexId") || "");
        const metric = String(url.searchParams.get("metric") || "pe_ttm");
        const fromDate = url.searchParams.get("from") || undefined;
        const toDate = url.searchParams.get("to") || undefined;
        const dataset = await getDataset();

        if (!INDEX_MAP[indexId]) {
          json(res, 400, { error: "Invalid indexId" });
          return;
        }
        if (!METRIC_SET.has(metric as MetricId)) {
          json(res, 400, { error: "Invalid metric" });
          return;
        }

        json(res, 200, buildSeriesPayload(dataset, indexId, metric as MetricId, fromDate, toDate));
        return;
      }

      if (url.pathname === "/api/heatmap" && req.method === "GET") {
        const group = url.searchParams.get("group") || undefined;
        const dataset = await getDataset();
        json(res, 200, buildHeatmapPayload(dataset, group || undefined));
        return;
      }

      if (url.pathname === "/api/company/meta" && req.method === "GET") {
        const companyDataset = await getCompanyDataset();
        json(res, 200, buildCompanyMeta(companyDataset));
        return;
      }

      if (url.pathname === "/api/company/snapshot" && req.method === "GET") {
        const companyDataset = await getCompanyDataset();
        json(res, 200, buildCompanySnapshotPayloadCached(companyDataset));
        return;
      }

      if (url.pathname === "/api/company/series" && req.method === "GET") {
        const indexId = String(url.searchParams.get("indexId") || "");
        const metric = String(url.searchParams.get("metric") || "pe_ttm");
        const fromDate = url.searchParams.get("from") || undefined;
        const toDate = url.searchParams.get("to") || undefined;
        const companyDataset = await getCompanyDataset();

        const companyExists = companyDataset.indices.some((item) => item.id === indexId);
        if (!companyExists) {
          json(res, 400, { error: "Invalid indexId" });
          return;
        }
        if (!COMPANY_METRIC_SET.has(metric as CompanyMetricId)) {
          json(res, 400, { error: "Invalid metric" });
          return;
        }

        json(
          res,
          200,
          buildCompanySeriesPayload(companyDataset, indexId, metric as CompanyMetricId, fromDate, toDate)
        );
        return;
      }

      if (url.pathname === "/api/watchlist" && req.method === "GET") {
        const stores = await getStores();
        const config = normalizeWatchlist(stores.watchlists.users[userId], userId);
        stores.watchlists.users[userId] = config;
        await saveStores(rootDir, stores);
        json(res, 200, config);
        return;
      }

      if (url.pathname === "/api/watchlist" && req.method === "POST") {
        const stores = await getStores();
        const body = (await readBody(req)) as Partial<UserWatchlist>;
        const merged = normalizeWatchlist(
          {
            ...stores.watchlists.users[userId],
            ...body,
            alertRule: {
              ...stores.watchlists.users[userId]?.alertRule,
              ...body.alertRule,
            },
          },
          userId
        );
        stores.watchlists.users[userId] = merged;
        await saveStores(rootDir, stores);
        json(res, 200, merged);
        return;
      }

      if (url.pathname === "/api/alerts" && req.method === "GET") {
        const stores = await getStores();
        const alerts = stores.alerts.users[userId] || [];
        json(res, 200, {
          userId,
          count: alerts.length,
          unreadCount: alerts.filter((item) => !item.read).length,
          rows: alerts,
        });
        return;
      }

      if (url.pathname === "/api/alerts/ack" && req.method === "POST") {
        const stores = await getStores();
        const body = (await readBody(req)) as { ids?: string[] };
        const rows = stores.alerts.users[userId] || [];
        const ids = new Set(body.ids || []);
        if (!ids.size) {
          for (const row of rows) {
            row.read = true;
          }
        } else {
          for (const row of rows) {
            if (ids.has(row.id)) {
              row.read = true;
            }
          }
        }
        stores.alerts.users[userId] = rows;
        await saveStores(rootDir, stores);
        json(res, 200, { ok: true, affected: ids.size || rows.length });
        return;
      }

      if (url.pathname === "/api/jobs/daily-update" && req.method === "POST") {
        const dataset = await getDataset();
        const stores = await getStores();
        const result = await applyDailyUpdate(dataset, stores);
        await writeJsonFile(resolvePath(rootDir, DATA_FILE), result.dataset);
        await saveStores(rootDir, stores);
        invalidateDatasetCaches();
        json(res, 200, {
          ok: true,
          generatedAt: result.dataset.generatedAt,
          createdAlerts: result.createdAlerts,
        });
        return;
      }

      if (url.pathname === "/api/jobs/company-refresh" && req.method === "POST") {
        const body = (await readBody(req)) as { symbols?: unknown[] };
        const symbols = normalizeCompanySymbols(body?.symbols);
        const result = await runCompanyDataRefresh(rootDir, symbols);
        invalidateDatasetCaches();
        json(res, 200, {
          ok: true,
          generatedAt: result.generatedAt,
          seriesCount: result.seriesCount,
          mode: result.mode,
          symbols: result.symbols,
        });
        return;
      }

      if (url.pathname === "/api/auth/dev-login" && req.method === "POST") {
        const stores = await getStores();
        const body = (await readBody(req)) as { userId?: string; userName?: string };
        const pickedUserId =
          body.userId?.trim() ||
          body.userName?.trim().toLowerCase().replace(/[^a-z0-9_-]/g, "-") ||
          "demo-user";

        const config = normalizeWatchlist(stores.watchlists.users[pickedUserId], pickedUserId);
        stores.watchlists.users[pickedUserId] = config;
        await saveStores(rootDir, stores);

        json(res, 200, {
          ok: true,
          userId: pickedUserId,
          token: `dev-token:${pickedUserId}`,
          expiresInDays: 3650,
        });
        return;
      }

      if (url.pathname === "/api/auth/wechat-login" && req.method === "POST") {
        json(res, 501, {
          ok: false,
          error: "wechat-login not available until mini program AppID is configured",
        });
        return;
      }

      json(res, 404, { error: "Not found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      json(res, 500, { error: message });
    }
  });
}
