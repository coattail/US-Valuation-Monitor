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
const METRIC_SET = new Set<MetricId>([
  "pe_ttm",
  "pe_forward",
  "pb",
  "earnings_yield",
]);

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
      const dataset = await loadDataset(rootDir);
      const stores = await loadStores(rootDir);
      const userId = getUserId(req);

      if (url.pathname === "/healthz" && req.method === "GET") {
        json(res, 200, { ok: true, now: new Date().toISOString() });
        return;
      }

      if (url.pathname === "/api/meta" && req.method === "GET") {
        json(res, 200, buildMeta(dataset));
        return;
      }

      if (url.pathname === "/api/snapshot" && req.method === "GET") {
        const group = url.searchParams.get("group") || undefined;
        json(res, 200, buildSnapshotPayload(dataset, group || undefined));
        return;
      }

      if (url.pathname === "/api/series" && req.method === "GET") {
        const indexId = String(url.searchParams.get("indexId") || "");
        const metric = String(url.searchParams.get("metric") || "pe_ttm");
        const fromDate = url.searchParams.get("from") || undefined;
        const toDate = url.searchParams.get("to") || undefined;

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
        json(res, 200, buildHeatmapPayload(dataset, group || undefined));
        return;
      }

      if (url.pathname === "/api/watchlist" && req.method === "GET") {
        const config = normalizeWatchlist(stores.watchlists.users[userId], userId);
        stores.watchlists.users[userId] = config;
        await saveStores(rootDir, stores);
        json(res, 200, config);
        return;
      }

      if (url.pathname === "/api/watchlist" && req.method === "POST") {
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
        const result = await applyDailyUpdate(dataset, stores);
        await writeJsonFile(resolvePath(rootDir, DATA_FILE), result.dataset);
        await saveStores(rootDir, stores);
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
