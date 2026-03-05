import path from "node:path";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

interface CompanySeed {
  rank: number;
  symbol: string;
  slug: string;
  displayName: string;
  marketCap: number;
}

interface ClosePoint {
  date: string;
  close: number;
  ts: number;
}

interface RatioAnchor {
  date: string;
  pe_ttm: number | null;
  pe_forward: number | null;
  pb: number | null;
  peg: number | null;
}

interface RatioPayload {
  anchors: RatioAnchor[];
  latest: {
    pe_ttm: number | null;
    pe_forward: number | null;
    pb: number | null;
    peg: number | null;
  };
  source: string;
}
type RatioMetricKey = keyof RatioPayload["latest"];

interface MetricPoint {
  date: string;
  ts: number;
  value: number;
}

interface MetricAnchorDenominator {
  date: string;
  ts: number;
  ratio: number;
  denominator: number;
}

interface SnapshotPoint {
  date: string;
  close: number | null;
  pe_ttm: number;
  pe_forward: number | null;
  pb: number;
  peg: number | null;
  us10y_yield: number;
}

interface QuarterlyEpsPoint {
  date: string;
  eps: number;
  source: "actual" | "expected";
  availableDate?: string;
}

interface QuarterlyNetIncomePoint {
  date: string;
  netIncome: number;
  source: "actual" | "expected";
}

interface QuarterlyShareCountPoint {
  date: string;
  shares: number;
}

interface StockAnalysisIncomeStatementQuarterlyPayload {
  epsRows: QuarterlyEpsPoint[];
  netIncomeRows: QuarterlyNetIncomePoint[];
  shareRows: QuarterlyShareCountPoint[];
}

interface StockAnalysisForecastQuarterlyPayload {
  epsRows: QuarterlyEpsPoint[];
  netIncomeRows: QuarterlyNetIncomePoint[];
}

interface QuarterlyFinancialSeriesResult {
  quarterlyEps: QuarterlyEpsPoint[];
  quarterlyNetIncome: QuarterlyNetIncomePoint[];
}

interface SecQuarterlyEpsSeriesResult {
  rows: QuarterlyEpsPoint[];
  availabilityByQuarter: Map<string, string>;
}

interface SplitEvent {
  date: string;
  ratio: number;
}

interface PreviousSeries {
  forwardStartDate: string;
  points: SnapshotPoint[];
  peg?: number | null;
  quarterlyEps?: QuarterlyEpsPoint[];
  quarterlyNetIncome?: QuarterlyNetIncomePoint[];
}

interface FetchTextOptions {
  headers?: string[];
}

interface StockAnalysisDataRatioPayloadResult {
  payload: RatioPayload | null;
  selectedSource: string;
  availableSources: string[];
}

interface StockAnalysisStatisticsRatioPayloadResult {
  payload: RatioPayload | null;
  redirectPath: string;
}

interface YchartsSeriesResult {
  securityId: string;
  price: MetricPoint[];
  pe_ttm: MetricPoint[];
  pe_forward: MetricPoint[];
  pb: MetricPoint[];
}

interface YahooDailyMetricSnapshot {
  date: string;
  pe_ttm: number | null;
  pe_forward: number | null;
  pb: number | null;
  peg: number | null;
  source?: string;
  capturedAt?: string;
}

interface YahooRatioPayloadResult {
  payload: RatioPayload | null;
  quoteLatestPayload: RatioPayload | null;
}

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const OUTPUT_DIR = path.join(ROOT_DIR, "data", "standardized");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "company-valuation-history.json");
const YAHOO_DAILY_METRICS_FILE = path.join(OUTPUT_DIR, "company-yahoo-daily-metrics.json");
const execFileAsync = promisify(execFile);
let companiesMarketCapFetchChain: Promise<unknown> = Promise.resolve();
let yahooSplitFetchChain: Promise<unknown> = Promise.resolve();
let ychartsFetchChain: Promise<unknown> = Promise.resolve();
let secTickerToCikMapPromise: Promise<Map<string, string>> | null = null;

const TOP_COMPANY_URLS = [
  "https://companiesmarketcap.com/usd/",
  "https://companiesmarketcap.com/usd/page/2/",
  "https://companiesmarketcap.com/usd/page/3/",
  "https://companiesmarketcap.com/",
  "https://companiesmarketcap.com/page/2/",
  "https://companiesmarketcap.com/page/3/",
];

const HISTORY_START_DATE = "2000-01-01";
const CONCURRENCY = 2;
const REQUEST_TIMEOUT_MS = 12000;

const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const DEFAULT_METRICS = {
  pe_ttm: 20,
  pe_forward: 18,
  pb: 3,
  peg: 1.5,
};

const METRIC_MAX = {
  pe_ttm: 1_000_000,
  pe_forward: 1_000_000,
  pb: 1_000_000,
  peg: 1_000_000,
};

const LONG_SERIES_FRESH_DAYS = 120;
const MAX_REASONABLE_DATA_DATE_OFFSET_DAYS = 14;
const STOCK_LATEST_OUTLIER_FACTOR = 3.2;
const STOCK_TTM_BASIS_MISMATCH_FACTOR = 3.5;
const MIN_FORWARD_ANCHORS_FOR_HISTORY = 4;
const YAHOO_FORWARD_OVERRIDE_MAX_JUMP_FACTOR = 1.18;
const YAHOO_FORWARD_STABLE_NEIGHBOR_FACTOR = 1.12;
const YAHOO_FORWARD_GUARD_MIN_VALUE = 4;
const ENABLE_DIRECT_FETCH_FALLBACK = process.env.ENABLE_DIRECT_FETCH_FALLBACK === "1";
const STOCK_ANALYSIS_SOURCE_PRIORITY = ["fai", "nasdaq", "fmp", "spg"];
const SEC_USER_AGENT = process.env.SEC_USER_AGENT || "us-valuation-monitor/1.0 contact@example.com";
const ADR_LATEST_PE_DIVISOR_BY_SYMBOL: Record<
  string,
  Partial<Record<"pe_ttm" | "pe_forward", number>>
> = {
  // TSM ADR is commonly quoted on Yahoo/WSJ with a different per-share basis in latest TTM fields.
  // We only normalize TTM here; forward PE may come from another source basis and should not reuse this factor.
  TSM: { pe_ttm: 5 },
};
const YAHOO_KEY_STATISTICS_HOSTS = [
  "https://uk.finance.yahoo.com",
  "https://finance.yahoo.com",
  "https://fr.finance.yahoo.com",
];
const YCHARTS_CALC_PRICE = "price";
const YCHARTS_CALC_PE = "pe_ratio";
const YCHARTS_CALC_PB = "price_to_book_value";
const YCHARTS_CALC_FORWARD_PE = "forward_pe_ratio";
const YCHARTS_CALC_FORWARD_PE_1Y = "forward_pe_ratio_1y";
const YAHOO_LATEST_OVERRIDE_SYMBOLS = new Set(
  String(process.env.YAHOO_LATEST_OVERRIDE_SYMBOLS || "*")
    .split(/[,\s]+/)
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean)
);
const YAHOO_LATEST_OVERRIDE_EXCLUDE_SYMBOLS = new Set(
  String(process.env.YAHOO_LATEST_OVERRIDE_EXCLUDE_SYMBOLS || "")
    .split(/[,\s]+/)
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean)
);
const FORWARD_PE_PROXY_FROM_TTM_SYMBOLS = new Set(
  String(process.env.FORWARD_PE_PROXY_FROM_TTM_SYMBOLS || "TM")
    .split(/[,\s]+/)
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean)
);
const YAHOO_TRUSTED_SOURCE_TAGS_BY_METRIC: Record<RatioMetricKey, string[]> = {
  pe_ttm: [
    "yahoo-trailing-pe-timeseries",
    "yahoo-key-statistics-valuation-measures",
    "yahoo-quote-summary-latest",
    "yahoo-quote-api-latest",
  ],
  pe_forward: [
    "yahoo-forward-pe-timeseries",
    "yahoo-key-statistics-valuation-measures",
    "yahoo-quote-summary-latest",
    "yahoo-quote-api-latest",
  ],
  pb: [
    "yahoo-key-statistics-valuation-measures",
    "yahoo-quote-summary-latest",
    "yahoo-quote-api-latest",
  ],
  peg: [
    "yahoo-trailing-peg-timeseries",
    "yahoo-key-statistics-valuation-measures",
    "yahoo-quote-summary-latest",
    "yahoo-quote-api-latest",
  ],
};

function parseSymbolFilterFromEnv(): string[] {
  const raw = String(process.env.COMPANY_SYMBOLS || process.env.COMPANY_SYMBOL || "").trim();
  if (!raw) return [];

  return [...new Set(raw.split(/[,\s]+/).map((item) => item.trim().toUpperCase()).filter(Boolean))];
}

function shouldUseYahooLatestOverrideForSymbol(symbol: string): boolean {
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();
  if (!normalizedSymbol) return false;
  if (YAHOO_LATEST_OVERRIDE_EXCLUDE_SYMBOLS.has(normalizedSymbol)) return false;
  return (
    YAHOO_LATEST_OVERRIDE_SYMBOLS.has("*") ||
    YAHOO_LATEST_OVERRIDE_SYMBOLS.has(normalizedSymbol)
  );
}

function hasAnyYahooSourceTag(source: string, tags: string[]): boolean {
  const normalizedSource = String(source || "").toLowerCase();
  if (!normalizedSource) return false;
  return tags.some((tag) => normalizedSource.includes(tag.toLowerCase()));
}

function isTrustedYahooMetricSource(metric: RatioMetricKey, source: string): boolean {
  const tags = YAHOO_TRUSTED_SOURCE_TAGS_BY_METRIC[metric] || [];
  return hasAnyYahooSourceTag(source, tags);
}

function sanitizeYahooMetricValue(metric: RatioMetricKey, value: unknown, source: string): number | null {
  const normalizedValue = sanitizeSignedRatio(value);
  if (normalizedValue === null) return null;
  if (!isTrustedYahooMetricSource(metric, source)) return null;
  return normalizedValue;
}

function sanitizeYahooRatioPayloadMetrics(payload: RatioPayload | null): RatioPayload | null {
  if (!payload) return null;
  const source = String(payload.source || "").trim();
  if (!source) return null;

  const latest: RatioPayload["latest"] = {
    pe_ttm: sanitizeYahooMetricValue("pe_ttm", payload.latest.pe_ttm, source),
    pe_forward: sanitizeYahooMetricValue("pe_forward", payload.latest.pe_forward, source),
    pb: sanitizeYahooMetricValue("pb", payload.latest.pb, source),
    peg: sanitizeYahooMetricValue("peg", payload.latest.peg, source),
  };

  const anchors = payload.anchors
    .map((item) => ({
      date: item.date,
      pe_ttm: sanitizeYahooMetricValue("pe_ttm", item.pe_ttm, source),
      pe_forward: sanitizeYahooMetricValue("pe_forward", item.pe_forward, source),
      pb: sanitizeYahooMetricValue("pb", item.pb, source),
      peg: sanitizeYahooMetricValue("peg", item.peg, source),
    }))
    .filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg);

  if (!anchors.length && !latest.pe_ttm && !latest.pe_forward && !latest.pb && !latest.peg) {
    return null;
  }

  return {
    anchors,
    latest,
    source,
  };
}

function normalizeTickerSymbol(symbol: string): string {
  return String(symbol || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function roundTo(value: number, digits = 3): number {
  const ratio = 10 ** digits;
  return Math.round(value * ratio) / ratio;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function ratioDistance(leftRaw: unknown, rightRaw: unknown): number | null {
  const left = sanitizeSignedRatio(leftRaw);
  const right = sanitizeSignedRatio(rightRaw);
  if (!left || !right) return null;
  if (left <= 0 || right <= 0) return null;
  return Math.max(left / right, right / left);
}

function isYahooForwardOverrideLikelyOutlier(
  points: SnapshotPoint[],
  index: number,
  candidateRaw: unknown
): boolean {
  const candidate = sanitizeSignedRatio(candidateRaw);
  if (!candidate || candidate < YAHOO_FORWARD_GUARD_MIN_VALUE) return false;
  if (!Array.isArray(points) || index < 0 || index > points.length) return false;

  let prev1: number | null = null;
  let prev2: number | null = null;
  for (let i = index - 1; i >= 0; i -= 1) {
    const value = sanitizeSignedRatio(points[i]?.pe_forward);
    if (!value) continue;
    if (prev1 === null) {
      prev1 = value;
      continue;
    }
    prev2 = value;
    break;
  }

  if (prev1 === null || prev1 < YAHOO_FORWARD_GUARD_MIN_VALUE) return false;

  const prevStabilityFactor = ratioDistance(prev1, prev2);
  const isPrevStable = prev2 === null || (prevStabilityFactor !== null && prevStabilityFactor <= YAHOO_FORWARD_STABLE_NEIGHBOR_FACTOR);
  if (!isPrevStable) return false;

  const jumpFactor = ratioDistance(candidate, prev1);
  if (jumpFactor === null) return false;
  return jumpFactor >= YAHOO_FORWARD_OVERRIDE_MAX_JUMP_FACTOR;
}

function toTs(dateText: string): number {
  return Date.parse(`${dateText}T00:00:00Z`) || 0;
}

function subtractYears(dateText: string, years: number): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateText || ""))) return String(dateText || "");
  const date = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return String(dateText || "");
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return date.toISOString().slice(0, 10);
}

function stripTags(raw: string): string {
  return raw.replace(/<[^>]+>/g, " ");
}

function decodeHtml(raw: string): string {
  return raw
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;/g, "'")
    .replace(/&#x2F;/g, "/")
    .replace(/&nbsp;/g, " ");
}

function stripScriptsAndStyles(raw: string): string {
  return String(raw || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ");
}

function escapeForRegex(raw: string): string {
  return String(raw || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sanitizeSignedRatio(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (Math.abs(n) < 1e-8) return null;
  return n;
}

function sanitizeEps(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return roundTo(n, 6);
}

function sanitizeNetIncome(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return roundTo(n, 3);
}

function sanitizeShareCount(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return roundTo(n, 3);
}

function toIsoDateFromEpoch(epochLike: unknown): string | null {
  const value = Number(epochLike);
  if (!Number.isFinite(value) || value <= 0) return null;

  const ms = value > 1e12 ? value : value * 1000;
  const date = new Date(ms);
  if (!Number.isFinite(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function toIsoDateFromText(rawDate: unknown): string | null {
  const value = String(rawDate || "").trim();
  if (!value) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;

  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString().slice(0, 10);
}

function quarterKeyFromDate(dateText: string): string {
  const date = toIsoDateFromText(dateText);
  if (!date) return "";

  const year = Number(date.slice(0, 4));
  const month = Number(date.slice(5, 7));
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return "";

  const quarter = Math.floor((month - 1) / 3) + 1;
  return `${year}-Q${quarter}`;
}

function addDays(dateText: string, days: number): string {
  const ts = toTs(dateText);
  if (!ts) return dateText;
  const shifted = new Date(ts + days * 86_400_000);
  return shifted.toISOString().slice(0, 10);
}

function median(values: readonly number[]): number | null {
  const arr = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  if (arr.length % 2) return arr[mid];
  return (arr[mid - 1] + arr[mid]) / 2;
}

function isLikelyCoarseSeries(points: MetricPoint[]): boolean {
  if (points.length < 3) return true;

  const ordered = [...points].sort((a, b) => a.ts - b.ts);
  const intervals: number[] = [];

  for (let i = 1; i < ordered.length; i += 1) {
    const gapDays = (ordered[i].ts - ordered[i - 1].ts) / 86_400_000;
    if (Number.isFinite(gapDays) && gapDays > 0) {
      intervals.push(gapDays);
    }
  }

  const med = median(intervals);
  if (!med) return true;
  return med >= 120;
}

function isRejectedPayload(text: string): boolean {
  const raw = String(text || "").trim().toLowerCase();
  if (!raw) return true;
  if (raw.includes("exceeded the daily hits limit")) return true;
  if (raw.includes("too many requests")) return true;
  if (raw.includes("sad-panda-201402200631.png")) return true;
  if (raw.includes("no longer be accessible from mainland china")) return true;
  if (raw.includes("enable javascript and cookies to continue")) return true;
  if (raw.includes("<title>just a moment")) return true;
  if (raw.includes("just a moment") && raw.includes("cf-chl")) return true;
  return false;
}

async function fetchText(
  url: string,
  retries = 1,
  timeoutMs = REQUEST_TIMEOUT_MS,
  directFirst = false,
  options: FetchTextOptions = {}
): Promise<string> {
  let lastError: unknown;
  const extraHeaders = (options.headers || []).flatMap((header) => ["-H", header]);

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const maxTimeSec = String(Math.max(6, Math.ceil(timeoutMs / 1000)));
      const baseArgs = [
        "-4",
        "-sSL",
        "--max-time",
        maxTimeSec,
        "-A",
        USER_AGENT,
        "-H",
        "accept-language: en-US,en;q=0.9",
        ...extraHeaders,
        url,
      ];

      const proxyCmd = baseArgs;
      const directCmd = ["--noproxy", "*", ...baseArgs];
      const commandCandidates: string[][] = ENABLE_DIRECT_FETCH_FALLBACK
        ? directFirst
          ? [directCmd, proxyCmd]
          : [proxyCmd, directCmd]
        : [proxyCmd];

      for (const args of commandCandidates) {
        try {
          const { stdout } = await execFileAsync("curl", args, { maxBuffer: 24 * 1024 * 1024 });
          const text = String(stdout || "").trim();
          if (!isRejectedPayload(text)) return text;
        } catch (error) {
          const partial = String((error as { stdout?: string })?.stdout || "").trim();
          if (!isRejectedPayload(partial)) return partial;
        }
      }

      throw new Error(`Empty response for ${url}`);
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        await sleep(280 * (attempt + 1));
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`Failed to fetch: ${url}`);
}

async function mapLimit<T, R>(
  items: readonly T[],
  limit: number,
  mapper: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let pointer = 0;

  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (pointer < items.length) {
      const current = pointer;
      pointer += 1;
      results[current] = await mapper(items[current], current);
    }
  });

  await Promise.all(workers);
  return results;
}

function enqueueCompaniesMarketCapFetch<T>(task: () => Promise<T>): Promise<T> {
  const run = companiesMarketCapFetchChain.then(task, task);
  companiesMarketCapFetchChain = run.then(
    () => undefined,
    () => undefined
  );
  return run;
}

function enqueueYahooSplitFetch<T>(task: () => Promise<T>): Promise<T> {
  const run = yahooSplitFetchChain.then(task, task);
  yahooSplitFetchChain = run.then(
    () => undefined,
    () => undefined
  );
  return run;
}

function enqueueYchartsFetch<T>(task: () => Promise<T>): Promise<T> {
  const run = ychartsFetchChain.then(task, task);
  ychartsFetchChain = run.then(
    () => undefined,
    () => undefined
  );
  return run;
}

async function fetchCompaniesMarketCapText(url: string, retries: number, timeoutMs: number): Promise<string> {
  return enqueueCompaniesMarketCapFetch(async () => {
    await sleep(140);
    return fetchText(url, retries, timeoutMs);
  });
}

function parseTopCompanies(html: string): CompanySeed[] {
  const rowRegex =
    /<tr><td class="fav">[\s\S]*?<td class="rank-td td-right"[^>]*data-sort="(\d+)"[^>]*>[\s\S]*?<\/td><td class="name-td">[\s\S]*?<a href="\/([^"/]+)\/marketcap\/"[\s\S]*?<div class="company-name">([\s\S]*?)<\/div><div class="company-code">[\s\S]*?<\/span>\s*([^<\s]+)\s*<\/div>[\s\S]*?<\/td><td class="td-right" data-sort="(\d+)"/g;

  const companies: CompanySeed[] = [];
  const seen = new Set<string>();

  for (const match of html.matchAll(rowRegex)) {
    const rank = Number(match[1]);
    const slug = decodeHtml(match[2]).replace(/\s+/g, "").trim().toLowerCase();
    const displayName = decodeHtml(stripTags(match[3])).replace(/\s+/g, " ").trim();
    const symbol = decodeHtml(match[4]).replace(/\s+/g, "").trim().toUpperCase();
    const marketCap = Number(match[5]);

    if (!Number.isFinite(rank) || rank <= 0) continue;
    if (!symbol || seen.has(symbol)) continue;
    if (!slug) continue;
    if (!Number.isFinite(marketCap) || marketCap <= 0) continue;

    seen.add(symbol);
    companies.push({ rank, symbol, slug, displayName, marketCap });
  }

  return companies.sort((a, b) => a.rank - b.rank);
}

function isUsListedCandidateSymbol(symbol: string): boolean {
  const s = String(symbol || "").trim().toUpperCase();
  if (!s) return false;
  if (s.includes(".")) return false;
  if (!/^[A-Z][A-Z0-9-]{0,9}$/.test(s)) return false;
  return true;
}

async function fetchTopCompanies(): Promise<CompanySeed[]> {
  const merged = new Map<string, CompanySeed>();

  for (const url of TOP_COMPANY_URLS) {
    try {
      const html = await fetchText(url, 1, 14000);
      const companies = parseTopCompanies(html);
      for (const company of companies) {
        if (!isUsListedCandidateSymbol(company.symbol)) continue;
        const current = merged.get(company.symbol);
        if (!current || company.rank < current.rank) {
          merged.set(company.symbol, company);
        }
      }

      if (merged.size >= 120) break;
    } catch {
      // try next source
    }
  }

  const selected = [...merged.values()]
    .sort((a, b) => a.rank - b.rank)
    .slice(0, 100);

  if (selected.length >= 90) {
    return selected;
  }

  throw new Error(`Unable to parse enough US-listed companies from companiesmarketcap: ${selected.length}`);
}

function parseStooqCsv(csvText: string): ClosePoint[] {
  const text = String(csvText || "").trim();
  if (!text || /^No data/i.test(text)) return [];

  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length <= 1) return [];

  const out: ClosePoint[] = [];

  for (let i = 1; i < lines.length; i += 1) {
    const [date, , , , closeRaw] = lines[i].split(",");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || "")) continue;
    if (date < HISTORY_START_DATE) continue;

    const close = Number(closeRaw);
    if (!Number.isFinite(close) || close <= 0) continue;

    out.push({ date, close, ts: toTs(date) });
  }

  return out.sort((a, b) => a.date.localeCompare(b.date));
}

function getStooqSymbolCandidates(symbol: string): string[] {
  const seed = symbol.toLowerCase();
  return [...new Set([seed, seed.replace(/\./g, "-"), seed.replace(/-/g, ""), seed.replace(/-/g, ".")])].filter(Boolean);
}

async function fetchCloseHistoryFromCompaniesMarketCap(slug: string): Promise<ClosePoint[]> {
  const normalized = String(slug || "").trim().toLowerCase();
  if (!normalized) return [];

  const urls = [
    `https://companiesmarketcap.com/${normalized}/stock-price-history/`,
    `https://companiesmarketcap.com/usd/${normalized}/stock-price-history/`,
  ];

  for (const url of urls) {
    try {
      const html = await fetchCompaniesMarketCapText(url, 3, 20000);
      const series = parseCompaniesMarketCapRatioSeries(html);
      const points = series
        .map((item) => ({ date: item.date, ts: item.ts, close: item.value }))
        .filter((item) => item.date >= HISTORY_START_DATE && item.close > 0)
        .sort((a, b) => a.ts - b.ts);

      if (points.length >= 24) {
        return points;
      }
    } catch {
      // try next url
    }
  }

  return [];
}

function parseYahooChartClose(jsonText: string): ClosePoint[] {
  try {
    const payload = JSON.parse(jsonText) as {
      chart?: {
        result?: Array<{
          timestamp?: Array<number | null>;
          indicators?: {
            quote?: Array<{
              close?: Array<number | null>;
            }>;
          };
        }>;
      };
    };

    const result = payload?.chart?.result?.[0];
    const timestamps = result?.timestamp || [];
    const closes = result?.indicators?.quote?.[0]?.close || [];
    if (!timestamps.length || !closes.length) return [];

    const byDate = new Map<string, ClosePoint>();
    const size = Math.min(timestamps.length, closes.length);

    for (let i = 0; i < size; i += 1) {
      const ts = Number(timestamps[i]);
      const close = Number(closes[i]);
      if (!Number.isFinite(ts) || ts <= 0) continue;
      if (!Number.isFinite(close) || close <= 0) continue;

      const date = new Date(ts * 1000).toISOString().slice(0, 10);
      if (date < HISTORY_START_DATE) continue;

      byDate.set(date, {
        date,
        ts: toTs(date),
        close,
      });
    }

    return [...byDate.values()].sort((a, b) => a.ts - b.ts);
  } catch {
    return [];
  }
}

function parseYahooChartSplits(jsonText: string): SplitEvent[] {
  try {
    const payload = JSON.parse(jsonText) as {
      chart?: {
        result?: Array<{
          events?: {
            splits?: Record<
              string,
              {
                date?: number;
                numerator?: number;
                denominator?: number;
                splitRatio?: string;
              }
            >;
          };
        }>;
      };
    };

    const splitRoot = payload?.chart?.result?.[0]?.events?.splits || {};
    const rows = Object.values(splitRoot);
    if (!rows.length) return [];

    const byDate = new Map<string, SplitEvent>();

    for (const row of rows) {
      const date = toIsoDateFromEpoch(row?.date);
      const numerator = Number(row?.numerator);
      const denominator = Number(row?.denominator);
      const ratio = Number.isFinite(numerator) && Number.isFinite(denominator) && denominator !== 0
        ? numerator / denominator
        : NaN;
      if (!date || !Number.isFinite(ratio) || ratio <= 0) continue;
      byDate.set(date, { date, ratio });
    }

    return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  } catch {
    return [];
  }
}

function toIsoFromSlashDate(raw: string): string | null {
  const text = String(raw || "").trim();
  const match = text.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  const date = `${match[3]}-${match[1]}-${match[2]}`;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  return date;
}

function parseNasdaqHistoricalClose(jsonText: string): ClosePoint[] {
  try {
    const payload = JSON.parse(jsonText) as {
      data?: {
        tradesTable?: {
          rows?: Array<{
            date?: string;
            close?: string;
          }>;
        };
      };
    };

    const rows = payload?.data?.tradesTable?.rows || [];
    if (!rows.length) return [];

    const byDate = new Map<string, ClosePoint>();

    for (const row of rows) {
      const date = toIsoFromSlashDate(String(row?.date || ""));
      if (!date || date < HISTORY_START_DATE) continue;

      const closeText = String(row?.close || "").replace(/[^\d.\-]/g, "");
      const close = Number(closeText);
      if (!Number.isFinite(close) || close <= 0) continue;

      byDate.set(date, {
        date,
        ts: toTs(date),
        close,
      });
    }

    return [...byDate.values()].sort((a, b) => a.ts - b.ts);
  } catch {
    return [];
  }
}

function mergeCloseSeries(base: ClosePoint[], overlay: ClosePoint[]): ClosePoint[] {
  const byDate = new Map<string, ClosePoint>();
  for (const item of base) {
    byDate.set(item.date, item);
  }
  for (const item of overlay) {
    byDate.set(item.date, item);
  }
  return [...byDate.values()].sort((a, b) => a.ts - b.ts);
}

function capCloseSeriesByDate(points: ClosePoint[], maxDate = ""): ClosePoint[] {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(maxDate || ""))) return points;
  return (points || []).filter((point) => String(point?.date || "") <= maxDate);
}

function capSnapshotSeriesByDate(points: SnapshotPoint[], maxDate = ""): SnapshotPoint[] {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(maxDate || ""))) return points;
  return (points || []).filter((point) => String(point?.date || "") <= maxDate);
}

function businessDaysBetween(startDate: string, endDate: string): string[] {
  const out: string[] = [];
  const endTs = toTs(endDate);
  const cursor = new Date(`${startDate}T00:00:00Z`);
  cursor.setUTCDate(cursor.getUTCDate() + 1);

  while (cursor.getTime() < endTs) {
    const weekday = cursor.getUTCDay();
    if (weekday !== 0 && weekday !== 6) {
      out.push(cursor.toISOString().slice(0, 10));
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return out;
}

function densifyCloseSeriesWithRecentDailyVol(closePoints: ClosePoint[]): ClosePoint[] {
  if (closePoints.length < 2) return closePoints;

  const points = [...closePoints].sort((a, b) => a.ts - b.ts);
  const dailyReturns: number[] = [];

  for (let i = 1; i < points.length; i += 1) {
    const gap = Math.round((points[i].ts - points[i - 1].ts) / 86400000);
    if (gap <= 4 && points[i - 1].close > 0 && points[i].close > 0) {
      const daily = points[i].close / points[i - 1].close - 1;
      if (Number.isFinite(daily) && daily > -0.95 && daily < 2.5) {
        dailyReturns.push(daily);
      }
    }
  }

  if (!dailyReturns.length) return points;

  const template = dailyReturns.slice(-Math.min(252, dailyReturns.length));
  const filled: ClosePoint[] = [points[0]];

  let syntheticSegment = 0;

  for (let i = 1; i < points.length; i += 1) {
    const left = points[i - 1];
    const right = points[i];
    const gap = Math.round((right.ts - left.ts) / 86400000);

    if (gap <= 4) {
      filled.push(right);
      continue;
    }

    const dates = businessDaysBetween(left.date, right.date);
    if (!dates.length) {
      filled.push(right);
      continue;
    }

    const steps = dates.length + 1;
    const targetRatio = right.close / left.close;

    const seed = (syntheticSegment * 17) % template.length;
    syntheticSegment += 1;

    const baseLogs = Array.from({ length: steps }, (_, index) => {
      const daily = clamp(template[(seed + index) % template.length], -0.35, 0.35);
      return Math.log1p(daily);
    });

    const logBase = baseLogs.reduce((sum, item) => sum + item, 0);
    const logTarget = Math.log(Math.max(targetRatio, 1e-9));

    let runningClose = left.close;

    if (Number.isFinite(logBase) && Number.isFinite(logTarget)) {
      const drift = (logTarget - logBase) / steps;
      if (Number.isFinite(drift) && Math.abs(drift) < 1.6) {
        for (let j = 0; j < dates.length; j += 1) {
          const stepLog = clamp(baseLogs[j] + drift, -1.5, 1.5);
          runningClose *= Math.exp(stepLog);
          filled.push({
            date: dates[j],
            ts: toTs(dates[j]),
            close: Math.max(runningClose, 1e-8),
          });
        }
        filled.push(right);
        continue;
      }
    }

    const geometricFactor = Math.exp(logTarget / steps);
    for (let j = 0; j < dates.length; j += 1) {
      runningClose *= geometricFactor;
      filled.push({
        date: dates[j],
        ts: toTs(dates[j]),
        close: Math.max(runningClose, 1e-8),
      });
    }

    filled.push(right);
  }

  return filled;
}

function getYchartsSecurityIdCandidates(symbol: string): string[] {
  const seed = String(symbol || "").trim().toUpperCase();
  if (!seed) return [];

  return [
    ...new Set([
      seed,
      seed.replace(/-/g, "."),
      seed.replace(/\./g, "-"),
      seed.replace(/[^A-Z0-9]/g, ""),
    ]),
  ].filter(Boolean);
}

function buildYchartsFundDataUrl(securityId: string, calcIds: string[]): string {
  const params = new URLSearchParams();
  params.set("securities", `include:true,id:${securityId},,`);
  params.set(
    "calcs",
    calcIds
      .map((calcId) => `include:true,id:${calcId},,`)
      .join("")
  );
  params.set("format", "real");
  params.set("zoom", "max");
  params.set("dateSelection", "range");
  params.set("legendOnChart", "true");
  params.set("chartType", "interactive");
  params.set("nameInLegend", "name_and_ticker");
  params.set("dataInLegend", "value");
  params.set("quoteLegend", "false");
  params.set("recessions", "false");
  params.set("displayDateRange", "false");
  params.set("source", "false");
  params.set("units", "false");
  params.set("useCustomColors", "false");
  params.set("useEstimates", "false");
  params.set("hideValueFlags", "false");
  params.set("performanceDisclosure", "false");
  params.set("splitType", "single");
  params.set("chartCreator", "true");

  return `https://ycharts.com/charts/fund_data.json?${params.toString()}`;
}

function parseYchartsFundDataSeries(jsonText: string): Map<string, MetricPoint[]> {
  const byCalc = new Map<string, Map<string, MetricPoint>>();
  if (!jsonText || !jsonText.trim().startsWith("{")) return new Map();
  const maxAcceptedDate = addDays(new Date().toISOString().slice(0, 10), MAX_REASONABLE_DATA_DATE_OFFSET_DAYS);

  try {
    const payload = JSON.parse(jsonText) as {
      chart_data?: Array<
        Array<{
          object_calc?: string;
          raw_data?: Array<[number | null, number | null]>;
        }>
      >;
    };

    const panels = Array.isArray(payload?.chart_data) ? payload.chart_data : [];
    for (const panel of panels) {
      if (!Array.isArray(panel)) continue;

      for (const rawSeries of panel) {
        const calcId = String(rawSeries?.object_calc || "").trim();
        const rawData = Array.isArray(rawSeries?.raw_data) ? rawSeries.raw_data : [];
        if (!calcId || !rawData.length) continue;

        const current = byCalc.get(calcId) || new Map<string, MetricPoint>();
        for (const [epochLike, valueLike] of rawData) {
          const date = toIsoDateFromEpoch(epochLike);
          const value = sanitizeSignedRatio(valueLike);
          if (!date || value === null || date < HISTORY_START_DATE || date > maxAcceptedDate) continue;

          current.set(date, {
            date,
            ts: toTs(date),
            value,
          });
        }

        if (current.size) {
          byCalc.set(calcId, current);
        }
      }
    }
  } catch {
    return new Map();
  }

  const out = new Map<string, MetricPoint[]>();
  for (const [calcId, pointsByDate] of byCalc.entries()) {
    const points = [...pointsByDate.values()].sort((a, b) => a.ts - b.ts);
    if (points.length) {
      out.set(calcId, points);
    }
  }

  return out;
}

function mergeMetricSeriesWithPreference(primary: MetricPoint[], secondary: MetricPoint[]): MetricPoint[] {
  const byDate = new Map<string, MetricPoint>();

  for (const point of secondary || []) {
    if (!point?.date || !Number.isFinite(point.ts) || !Number.isFinite(point.value)) continue;
    byDate.set(point.date, point);
  }

  for (const point of primary || []) {
    if (!point?.date || !Number.isFinite(point.ts) || !Number.isFinite(point.value)) continue;
    byDate.set(point.date, point);
  }

  return [...byDate.values()].sort((a, b) => a.ts - b.ts);
}

function metricPointsToCloseSeries(points: MetricPoint[]): ClosePoint[] {
  return (points || [])
    .filter((point) => point?.date && Number.isFinite(point.ts) && Number.isFinite(point.value) && point.value > 0)
    .map((point) => ({
      date: point.date,
      ts: point.ts,
      close: point.value,
    }))
    .sort((a, b) => a.ts - b.ts);
}

function deriveForwardPeProxySeriesFromTtmPe(
  peSeries: MetricPoint[],
  latestPeTtmRaw: unknown,
  latestForwardPeRaw: unknown,
  lastCloseDate: string,
  lookbackYears = 8
): MetricPoint[] {
  const latestPeTtm = sanitizeSignedRatio(latestPeTtmRaw);
  const latestForwardPe = sanitizeSignedRatio(latestForwardPeRaw);
  if (!latestPeTtm || !latestForwardPe) return [];

  const ratio = latestForwardPe / latestPeTtm;
  if (!Number.isFinite(ratio) || Math.abs(ratio) < 0.35 || Math.abs(ratio) > 3.5) {
    return [];
  }

  const cutoffDate = lastCloseDate ? subtractYears(lastCloseDate, lookbackYears) : "";

  return (peSeries || [])
    .filter((point) => {
      if (!point?.date || !Number.isFinite(point.ts) || !Number.isFinite(point.value)) return false;
      if (cutoffDate && point.date < cutoffDate) return false;
      return true;
    })
    .map((point) => ({
      date: point.date,
      ts: point.ts,
      value: roundTo(clamp(point.value * ratio, -METRIC_MAX.pe_forward, METRIC_MAX.pe_forward), 4),
    }));
}

async function fetchYchartsSeriesBundle(symbol: string): Promise<YchartsSeriesResult | null> {
  const calcIds = [
    YCHARTS_CALC_PRICE,
    YCHARTS_CALC_PE,
    YCHARTS_CALC_PB,
    YCHARTS_CALC_FORWARD_PE_1Y,
    YCHARTS_CALC_FORWARD_PE,
  ];

  const candidates = getYchartsSecurityIdCandidates(symbol);
  for (const candidate of candidates) {
    const url = buildYchartsFundDataUrl(candidate, calcIds);

    try {
      const raw = await enqueueYchartsFetch(async () => {
        await sleep(110);
        return fetchText(url, 1, 20000);
      });

      const parsed = parseYchartsFundDataSeries(raw);
      if (!parsed.size) continue;

      const price = parsed.get(YCHARTS_CALC_PRICE) || [];
      const peTtm = parsed.get(YCHARTS_CALC_PE) || [];
      const pb = parsed.get(YCHARTS_CALC_PB) || [];
      const forwardPrimary = parsed.get(YCHARTS_CALC_FORWARD_PE_1Y) || [];
      const forwardFallback = parsed.get(YCHARTS_CALC_FORWARD_PE) || [];
      const peForward = mergeMetricSeriesWithPreference(forwardPrimary, forwardFallback);

      if (price.length < 24 && peTtm.length < 24 && pb.length < 24 && peForward.length < 4) {
        continue;
      }

      return {
        securityId: candidate,
        price,
        pe_ttm: peTtm,
        pe_forward: peForward,
        pb,
      };
    } catch {
      // try next candidate
    }
  }

  return null;
}

async function fetchCloseHistoryFromYahoo(symbol: string): Promise<ClosePoint[]> {
  const candidates = [...new Set([symbol, symbol.replace(/\./g, "-"), symbol.replace(/-/g, ".")])]
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);

  for (const candidate of candidates) {
    const url =
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(candidate)}` +
      "?range=max&interval=1d&events=history&includeAdjustedClose=true";
    try {
      const json = await fetchText(url, 1, 18000);
      const points = parseYahooChartClose(json);
      if (points.length >= 200) {
        return points;
      }
    } catch {
      // try next candidate
    }
  }

  return [];
}

async function fetchSplitEventsFromYahoo(symbol: string): Promise<SplitEvent[]> {
  const candidates = [...new Set([symbol, symbol.replace(/\./g, "-"), symbol.replace(/-/g, ".")])]
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);

  for (const candidate of candidates) {
    const urls = [
      `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(candidate)}?range=max&interval=1d&events=split`,
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(candidate)}?range=max&interval=1d&events=split`,
    ];

    for (const url of urls) {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          const json = await enqueueYahooSplitFetch(async () => {
            await sleep(140 + attempt * 120);
            const { stdout } = await execFileAsync(
              "curl",
              [
                "-4",
                "-sSL",
                "--compressed",
                "--max-time",
                "25",
                "-A",
                "Mozilla/5.0",
                url,
              ],
              { maxBuffer: 24 * 1024 * 1024 }
            );
            return String(stdout || "").trim();
          });

          if (!json || isRejectedPayload(json)) {
            continue;
          }
          const events = parseYahooChartSplits(json);
          if (events.length) return events;
        } catch {
          // retry
        }
      }
    }
  }

  return [];
}

async function fetchCloseHistoryFromNasdaq(symbol: string): Promise<ClosePoint[]> {
  const candidates = [...new Set([symbol, symbol.replace(/-/g, "."), symbol.replace(/\./g, "-")])]
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);

  for (const candidate of candidates) {
    const url =
      `https://api.nasdaq.com/api/quote/${encodeURIComponent(candidate)}/historical` +
      `?assetclass=stocks&fromdate=${HISTORY_START_DATE}&limit=5000`;
    try {
      const json = await fetchText(url, 1, 18000);
      const points = parseNasdaqHistoricalClose(json);
      if (points.length >= 200) {
        return points;
      }
    } catch {
      // try next candidate
    }
  }

  return [];
}

async function fetchCloseHistory(symbol: string, slug: string): Promise<ClosePoint[]> {
  const candidates = getStooqSymbolCandidates(symbol);
  let fromStooq: ClosePoint[] = [];

  for (const candidate of candidates) {
    const url = `https://stooq.com/q/d/l/?s=${encodeURIComponent(candidate)}.us&i=d`;
    try {
      await sleep(120);
      const csv = await fetchText(url, 2, 32000, true);
      const points = parseStooqCsv(csv);
      if (points.length >= 200) {
        fromStooq = points;
        break;
      }
    } catch {
      // try next candidate
    }
  }

  const fromYahoo = await fetchCloseHistoryFromYahoo(symbol);
  const fromNasdaq = await fetchCloseHistoryFromNasdaq(symbol);
  const fromCompaniesMarketCap = await fetchCloseHistoryFromCompaniesMarketCap(slug);

  let selected: ClosePoint[] = [];

  if (fromStooq.length >= 200 && fromCompaniesMarketCap.length >= 24) {
    selected = mergeCloseSeries(fromCompaniesMarketCap, fromStooq);
  } else if (fromStooq.length >= 200) {
    selected = fromStooq;
  } else if (fromYahoo.length >= 200 && fromCompaniesMarketCap.length >= 24) {
    selected = mergeCloseSeries(fromCompaniesMarketCap, fromYahoo);
  } else if (fromNasdaq.length >= 200 && fromCompaniesMarketCap.length >= 24) {
    selected = mergeCloseSeries(fromCompaniesMarketCap, fromNasdaq);
  } else if (fromYahoo.length >= 200) {
    selected = fromYahoo;
  } else if (fromNasdaq.length >= 200) {
    selected = fromNasdaq;
  } else if (fromCompaniesMarketCap.length >= 24) {
    selected = fromCompaniesMarketCap;
  }

  if (!selected.length) {
    return [];
  }

  return densifyCloseSeriesWithRecentDailyVol(selected);
}

async function fetchYahooMarketLatestDate(): Promise<string | null> {
  const configuredSymbol = String(process.env.YAHOO_REFERENCE_SYMBOL || "").trim().toUpperCase();
  const symbolCandidates = [
    configuredSymbol,
    "SPY",
    "^GSPC",
    "QQQ",
    "^IXIC",
  ].filter(Boolean);

  let bestDate = "";
  for (const symbol of symbolCandidates) {
    const encoded = encodeURIComponent(symbol);
    const urls = [
      `https://query2.finance.yahoo.com/v8/finance/chart/${encoded}?range=15d&interval=1d&events=history&includeAdjustedClose=true`,
      `https://query1.finance.yahoo.com/v8/finance/chart/${encoded}?range=15d&interval=1d&events=history&includeAdjustedClose=true`,
    ];

    for (const url of urls) {
      try {
        const json = await fetchText(url, 1, 18000);
        const points = parseYahooChartClose(json);
        const latestDate = points[points.length - 1]?.date || "";
        if (latestDate && latestDate > bestDate) {
          bestDate = latestDate;
        }
      } catch {
        // continue trying next endpoint/symbol
      }
    }
  }

  if (!bestDate) return null;
  const maxAcceptableDate = addDays(new Date().toISOString().slice(0, 10), 1);
  if (bestDate > maxAcceptableDate) return null;
  return bestDate;
}

function parseCompaniesMarketCapRatioSeries(html: string): MetricPoint[] {
  const text = String(html || "");
  const arrayMatches = [...text.matchAll(/data\s*=\s*(\[\{[\s\S]*?\}\]);/g)];
  const yearlyMatches = [...text.matchAll(/data\s*=\s*(\{[\s\S]*?\});/g)];

  let best: Array<{ d?: unknown; v?: unknown }> = [];

  for (const match of arrayMatches) {
    try {
      const parsed = JSON.parse(match[1]) as Array<{ d?: unknown; v?: unknown }>;
      if (Array.isArray(parsed) && parsed.length > best.length) {
        best = parsed;
      }
    } catch {
      // keep trying
    }
  }

  const byDate = new Map<string, MetricPoint>();
  if (best.length) {
    for (const item of best) {
      const date = toIsoDateFromEpoch(item?.d);
      const value = sanitizeSignedRatio(item?.v);
      if (!date || !value) continue;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;
      if (date < HISTORY_START_DATE) continue;
      byDate.set(date, {
        date,
        ts: toTs(date),
        value,
      });
    }
  }

  if (byDate.size) {
    return [...byDate.values()].sort((a, b) => a.ts - b.ts);
  }

  let bestYearly: Record<string, unknown> | null = null;
  let bestYearlyCount = 0;

  for (const match of yearlyMatches) {
    try {
      const parsed = JSON.parse(match[1]) as Record<string, unknown>;
      const validCount = Object.entries(parsed).filter(([key, value]) => {
        if (!/^\d{4}$/.test(key)) return false;
        const year = Number(key);
        if (!Number.isFinite(year) || year < 1900 || year > 2100) return false;
        return sanitizeSignedRatio(value) !== null;
      }).length;

      if (validCount > bestYearlyCount) {
        bestYearly = parsed;
        bestYearlyCount = validCount;
      }
    } catch {
      // keep trying
    }
  }

  if (!bestYearly || !bestYearlyCount) return [];

  for (const [key, rawValue] of Object.entries(bestYearly)) {
    if (!/^\d{4}$/.test(key)) continue;
    const year = Number(key);
    if (!Number.isFinite(year) || year < 1900 || year > 2100) continue;

    const value = sanitizeSignedRatio(rawValue);
    if (!value) continue;

    const date = `${key}-12-31`;
    if (date < HISTORY_START_DATE) continue;
    byDate.set(date, {
      date,
      ts: toTs(date),
      value,
    });
  }

  return [...byDate.values()].sort((a, b) => a.ts - b.ts);
}

async function fetchCompaniesMarketCapMetricSeries(slug: string, metricPath: "pe-ratio" | "pb-ratio"): Promise<MetricPoint[]> {
  const normalized = String(slug || "").trim().toLowerCase();
  if (!normalized) return [];

  const urls = [
    `https://companiesmarketcap.com/${normalized}/${metricPath}/`,
    `https://companiesmarketcap.com/usd/${normalized}/${metricPath}/`,
  ];

  for (const url of urls) {
    try {
      const html = await fetchCompaniesMarketCapText(url, 3, 20000);
      const series = parseCompaniesMarketCapRatioSeries(html);
      if (series.length >= 8) return series;
    } catch {
      // try next url
    }
  }

  return [];
}

function extractArrayLiteral(text: string, key: string, nextKey: string): unknown[] {
  const pattern = new RegExp(`${key}:\\[(.*?)\\],${nextKey}:`, "s");
  const match = text.match(pattern);
  if (!match) return [];

  try {
    return JSON.parse(`[${match[1]}]`) as unknown[];
  } catch {
    return [];
  }
}

function extractArrayLiteralByCandidates(
  text: string,
  candidates: Array<{ key: string; nextKey: string }>
): unknown[] {
  for (const candidate of candidates) {
    const values = extractArrayLiteral(text, candidate.key, candidate.nextKey);
    if (values.length) return values;
  }
  return [];
}

function normalizeRatioDateKey(value: unknown): string {
  const text = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}$/.test(text)) return `${text}-12-31`;
  return "";
}

function parseRatioPayloadFromScript(rawText: string): RatioPayload | null {
  const text = String(rawText || "").replace(/\n/g, " ");
  if (!text.includes("financialData:{")) return null;

  const dateKeysRaw = extractArrayLiteral(text, "datekey", "fiscalYear");
  const peRaw = extractArrayLiteralByCandidates(text, [
    { key: "pe", nextKey: "peForward" },
    { key: "pe", nextKey: "ps" },
  ]);
  const fwdRaw = extractArrayLiteralByCandidates(text, [
    { key: "peForward", nextKey: "ps" },
    { key: "forwardPE", nextKey: "ps" },
    { key: "forwardPe", nextKey: "ps" },
  ]);
  const pbRaw = extractArrayLiteral(text, "pb", "ptbvRatio");
  const pegRaw = extractArrayLiteralByCandidates(text, [
    { key: "pegRatio", nextKey: "fiscalYear" },
    { key: "pegRatio", nextKey: "ttmRevenueGrowth" },
    { key: "peg", nextKey: "fiscalYear" },
    { key: "peg", nextKey: "ttmRevenueGrowth" },
  ]);

  const dateKeys = dateKeysRaw.map((item) => String(item || ""));
  if (!dateKeys.length) return null;

  const anchors: RatioAnchor[] = [];
  let latest: RatioPayload["latest"] = {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
    peg: null,
  };

  for (let i = 0; i < dateKeys.length; i += 1) {
    const key = dateKeys[i];
    const pe = sanitizeSignedRatio(peRaw[i]);
    const peForward = sanitizeSignedRatio(fwdRaw[i]);
    const pb = sanitizeSignedRatio(pbRaw[i]);
    const peg = sanitizeSignedRatio(pegRaw[i]);

    if (i === 0 || key === "TTM") {
      latest = {
        pe_ttm: pe,
        pe_forward: peForward,
        pb,
        peg,
      };
      continue;
    }

    const normalizedDate = normalizeRatioDateKey(key);
    if (!normalizedDate) continue;
    if (!pe && !peForward && !pb && !peg) continue;

    anchors.push({
      date: normalizedDate,
      pe_ttm: pe,
      pe_forward: peForward,
      pb,
      peg,
    });
  }

  return {
    anchors: anchors.sort((a, b) => a.date.localeCompare(b.date)),
    latest,
    source: "stockanalysis-financial-ratios",
  };
}

function countForwardAnchors(payload: RatioPayload | null): number {
  if (!payload) return 0;
  return payload.anchors.filter((item) => sanitizeSignedRatio(item.pe_forward)).length;
}

function countPegAnchors(payload: RatioPayload | null): number {
  if (!payload) return 0;
  return payload.anchors.filter((item) => sanitizeSignedRatio(item.peg)).length;
}

function parseYahooValuationMeasuresFromHtml(rawText: string): RatioPayload | null {
  const raw = String(rawText || "");
  if (!raw.trim()) return null;

  const normalizedPlain = decodeHtml(stripTags(stripScriptsAndStyles(raw)))
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const candidates = [
    raw,
    raw.replace(/\\"/g, '"'),
    raw.replace(/\\\//g, "/"),
    raw.replace(/\\"/g, '"').replace(/\\\//g, "/"),
    normalizedPlain,
  ].filter((item) => String(item || "").trim());

  const parseLabelMetric = (
    labels: string[],
    minValue: number,
    maxValue: number
  ): number | null => {
    let resolved: number | null = null;

    for (const candidate of candidates) {
      for (const label of labels) {
        const escapedLabel = escapeForRegex(label);
        const patterns = [
          new RegExp(`${escapedLabel}[^0-9\\-]{0,120}(-?\\d+(?:\\.\\d+)?)`, "gi"),
          new RegExp(`${escapedLabel}[\\s\\S]{0,220}?<td[^>]*>\\s*(-?\\d+(?:\\.\\d+)?)\\s*<`, "gi"),
          new RegExp(`"label"\\s*:\\s*"${escapedLabel}"[\\s\\S]{0,220}?"raw"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)`, "gi"),
        ];

        for (const pattern of patterns) {
          for (const match of candidate.matchAll(pattern)) {
            const value = sanitizeSignedRatio(match[1]);
            if (value === null) continue;
            if (value < minValue || value > maxValue) continue;
            resolved = value;
          }
        }
      }
    }

    return resolved;
  };

  const peTtm = parseLabelMetric(["Trailing P/E"], 2, 400);
  const peForward = parseLabelMetric(["Forward P/E"], 2, 400);
  const pb = parseLabelMetric(["Price/Book"], 0.01, 400);
  const peg = parseLabelMetric(
    [
      "PEG Ratio (5 yr expected)",
      "PEG Ratio (5yr expected)",
    ],
    -100,
    100
  );

  if (!peTtm && !peForward && !pb && !peg) return null;

  return {
    anchors: [],
    latest: {
      pe_ttm: peTtm,
      pe_forward: peForward,
      pb,
      peg,
    },
    source: "yahoo-key-statistics-valuation-measures",
  };
}

function parseYahooKeyStatisticsRatioPayload(rawText: string): RatioPayload | null {
  const text = String(rawText || "");
  if (!text) return null;
  const valuationMeasuresPayload = parseYahooValuationMeasuresFromHtml(text);
  const normalizedText = text.includes('\\"') ? text.replace(/\\"/g, '"') : text;
  const haystacks = [text, normalizedText];

  const pickRawNumberByKeys = (keys: string[]): number | null => {
    for (const key of keys) {
      const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const regex = new RegExp(`"${escaped}"\\s*:\\s*\\{[^{}]{0,320}?"raw"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)`, "i");

      for (const haystack of haystacks) {
        const match = haystack.match(regex);
        if (!match) continue;

        const value = sanitizeSignedRatio(Number(match[1]));
        if (value !== null) return value;
      }
    }
    return null;
  };

  const peTtm = pickRawNumberByKeys(["trailingPE", "trailingPe"]);

  const rawPayload =
    peTtm
      ? {
          anchors: [],
          latest: {
            pe_ttm: peTtm,
            pe_forward: null,
            pb: null,
            peg: null,
          },
          source: "yahoo-key-statistics-latest-raw",
        }
      : null;

  return mergeRatioPayloadList(
    [valuationMeasuresPayload, rawPayload].filter(Boolean) as RatioPayload[]
  );
}

function parseYahooQuoteSummaryRatioPayload(rawText: string): RatioPayload | null {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const result = (
    ((parsed.quoteSummary as Record<string, unknown> | undefined)?.result as unknown[]) || []
  ).find((item) => item && typeof item === "object") as Record<string, unknown> | undefined;
  if (!result) return null;

  const defaultKeyStatistics = (result.defaultKeyStatistics as Record<string, unknown> | undefined) || {};
  const financialData = (result.financialData as Record<string, unknown> | undefined) || {};
  const summaryDetail = (result.summaryDetail as Record<string, unknown> | undefined) || {};

  const pickRaw = (...sources: unknown[]): number | null => {
    for (const source of sources) {
      if (!source || typeof source !== "object") continue;
      const value = sanitizeSignedRatio((source as { raw?: unknown }).raw);
      if (value !== null) return value;
    }
    return null;
  };

  const peTtm = pickRaw(
    summaryDetail.trailingPE,
    defaultKeyStatistics.trailingPE,
    defaultKeyStatistics.trailingPe
  );
  const peForward = pickRaw(
    summaryDetail.forwardPE,
    defaultKeyStatistics.forwardPE,
    defaultKeyStatistics.forwardPe,
    financialData.forwardPE,
    financialData.forwardPe
  );
  const pb = pickRaw(summaryDetail.priceToBook, defaultKeyStatistics.priceToBook, financialData.priceToBook);
  const peg = pickRaw(defaultKeyStatistics.pegRatio, defaultKeyStatistics.peg);

  if (!peTtm && !peForward && !pb && !peg) return null;

  return {
    anchors: [],
    latest: {
      pe_ttm: peTtm,
      pe_forward: peForward,
      pb,
      peg,
    },
    source: "yahoo-quote-summary-latest",
  };
}

function parseYahooQuoteApiRatioPayload(rawText: string): RatioPayload | null {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const result = (((parsed.quoteResponse as Record<string, unknown> | undefined)?.result as unknown[]) || []).find(
    (item) => item && typeof item === "object"
  ) as Record<string, unknown> | undefined;
  if (!result) return null;

  const peTtm = sanitizeSignedRatio(result.trailingPE);
  const peForward = sanitizeSignedRatio(result.forwardPE);
  const pb = sanitizeSignedRatio(result.priceToBook);
  const peg = sanitizeSignedRatio(result.pegRatio);
  if (!peTtm && !peForward && !pb && !peg) return null;

  return {
    anchors: [],
    latest: {
      pe_ttm: peTtm,
      pe_forward: peForward,
      pb,
      peg,
    },
    source: "yahoo-quote-api-latest",
  };
}

function parseYahooTrailingPegTimeseriesPayload(rawText: string): RatioPayload | null {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const resultList = (((parsed.timeseries as Record<string, unknown> | undefined)?.result as unknown[]) || []).filter(
    (item) => item && typeof item === "object"
  ) as Array<Record<string, unknown>>;
  if (!resultList.length) return null;

  const sourceRows = resultList
    .map((item) => (Array.isArray(item.trailingPegRatio) ? item.trailingPegRatio : []))
    .find((rows) => rows.length) as Array<Record<string, unknown>> | undefined;
  if (!sourceRows || !sourceRows.length) return null;

  let latestPeg: number | null = null;
  let latestDate = "";
  for (const row of sourceRows) {
    if (!row || typeof row !== "object") continue;
    const asOfDate = String(row.asOfDate || "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) continue;

    const rawValue = (row.reportedValue as Record<string, unknown> | undefined)?.raw;
    const peg = sanitizeSignedRatio(rawValue);
    if (peg === null) continue;

    if (!latestDate || asOfDate >= latestDate) {
      latestDate = asOfDate;
      latestPeg = peg;
    }
  }

  if (latestPeg === null) return null;

  return {
    anchors: [],
    latest: {
      pe_ttm: null,
      pe_forward: null,
      pb: null,
      peg: latestPeg,
    },
    source: "yahoo-trailing-peg-timeseries",
  };
}

function parseYahooTrailingPeTimeseriesPayload(rawText: string): RatioPayload | null {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const resultList = (((parsed.timeseries as Record<string, unknown> | undefined)?.result as unknown[]) || []).filter(
    (item) => item && typeof item === "object"
  ) as Array<Record<string, unknown>>;
  if (!resultList.length) return null;

  const sourceRows = resultList
    .map((item) => (Array.isArray(item.trailingPeRatio) ? item.trailingPeRatio : []))
    .find((rows) => rows.length) as Array<Record<string, unknown>> | undefined;
  if (!sourceRows || !sourceRows.length) return null;

  const byDate = new Map<string, RatioAnchor>();
  let latestPe: number | null = null;
  let latestDate = "";
  for (const row of sourceRows) {
    if (!row || typeof row !== "object") continue;
    const asOfDate = String(row.asOfDate || "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) continue;

    const rawValue = (row.reportedValue as Record<string, unknown> | undefined)?.raw;
    const pe = sanitizeSignedRatio(rawValue);
    if (pe === null) continue;

    byDate.set(asOfDate, {
      date: asOfDate,
      pe_ttm: pe,
      pe_forward: null,
      pb: null,
      peg: null,
    });

    if (!latestDate || asOfDate >= latestDate) {
      latestDate = asOfDate;
      latestPe = pe;
    }
  }

  if (!byDate.size && latestPe === null) return null;

  return {
    anchors: [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
    latest: {
      pe_ttm: latestPe,
      pe_forward: null,
      pb: null,
      peg: null,
    },
    source: "yahoo-trailing-pe-timeseries",
  };
}

function parseYahooForwardPeTimeseriesPayload(rawText: string): RatioPayload | null {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const resultList = (((parsed.timeseries as Record<string, unknown> | undefined)?.result as unknown[]) || []).filter(
    (item) => item && typeof item === "object"
  ) as Array<Record<string, unknown>>;
  if (!resultList.length) return null;

  const sourceRows = resultList
    .map((item) => (Array.isArray(item.forwardPeRatio) ? item.forwardPeRatio : []))
    .find((rows) => rows.length) as Array<Record<string, unknown>> | undefined;
  if (!sourceRows || !sourceRows.length) return null;

  const byDate = new Map<string, RatioAnchor>();
  let latestForwardPe: number | null = null;
  let latestDate = "";
  for (const row of sourceRows) {
    if (!row || typeof row !== "object") continue;
    const asOfDate = String(row.asOfDate || "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) continue;

    const rawValue = (row.reportedValue as Record<string, unknown> | undefined)?.raw;
    const peForward = sanitizeSignedRatio(rawValue);
    if (peForward === null) continue;

    byDate.set(asOfDate, {
      date: asOfDate,
      pe_ttm: null,
      pe_forward: peForward,
      pb: null,
      peg: null,
    });

    if (!latestDate || asOfDate >= latestDate) {
      latestDate = asOfDate;
      latestForwardPe = peForward;
    }
  }

  if (!byDate.size && latestForwardPe === null) return null;

  return {
    anchors: [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
    latest: {
      pe_ttm: null,
      pe_forward: latestForwardPe,
      pb: null,
      peg: null,
    },
    source: "yahoo-forward-pe-timeseries",
  };
}

async function fetchYahooKeyStatisticsRatioPayload(symbol: string): Promise<YahooRatioPayloadResult> {
  const candidates = [...new Set([symbol, symbol.replace(/\./g, "-"), symbol.replace(/-/g, ".")])]
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean);
  const period1 = Math.floor(toTs(HISTORY_START_DATE) / 1000);
  const period2 = Math.floor(Date.now() / 1000) + 86400 * 30;
  let quoteSummaryPayload: RatioPayload | null = null;
  let quoteApiPayload: RatioPayload | null = null;
  let trailingPePayload: RatioPayload | null = null;
  let forwardPePayload: RatioPayload | null = null;
  let trailingPegPayload: RatioPayload | null = null;

  const fetchYahooPayloadText = async (url: string): Promise<string | null> => {
    try {
      const { stdout } = await execFileAsync(
        "curl",
        [
          "-4",
          "-sSL",
          "--compressed",
          "--max-time",
          "25",
          "-A",
          "Mozilla/5.0",
          "-H",
          "accept-language: en-US,en;q=0.9",
          url,
        ],
        { maxBuffer: 24 * 1024 * 1024 }
      );
      const text = String(stdout || "").trim();
      if (!text || isRejectedPayload(text)) return null;
      return text;
    } catch {
      return null;
    }
  };

  for (const candidate of candidates) {
    if (!quoteSummaryPayload) {
      const quoteSummaryUrls = [
        `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(candidate)}?modules=defaultKeyStatistics,financialData,summaryDetail`,
        `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(candidate)}?modules=defaultKeyStatistics,financialData,summaryDetail`,
      ];

      for (const url of quoteSummaryUrls) {
        const text = await fetchYahooPayloadText(url);
        if (!text) continue;
        const payload = parseYahooQuoteSummaryRatioPayload(text);
        if (payload) {
          quoteSummaryPayload = payload;
          break;
        }
      }
    }

    if (!quoteApiPayload) {
      const quoteApiUrls = [
        `https://query2.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(candidate)}`,
        `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(candidate)}`,
      ];
      for (const url of quoteApiUrls) {
        const text = await fetchYahooPayloadText(url);
        if (!text) continue;
        const payload = parseYahooQuoteApiRatioPayload(text);
        if (payload) {
          quoteApiPayload = payload;
          break;
        }
      }
    }

    const fetchTimeseriesPayload = async (
      type: "trailingPeRatio" | "forwardPeRatio" | "trailingPegRatio"
    ): Promise<RatioPayload | null> => {
      const timeseriesUrl =
        `https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/` +
        `${encodeURIComponent(candidate)}?type=${type}&period1=${period1}&period2=${period2}`;

      const text = await fetchYahooPayloadText(timeseriesUrl);
      if (!text) return null;
      if (type === "trailingPeRatio") return parseYahooTrailingPeTimeseriesPayload(text);
      if (type === "forwardPeRatio") return parseYahooForwardPeTimeseriesPayload(text);
      return parseYahooTrailingPegTimeseriesPayload(text);
    };

    try {
      trailingPePayload = trailingPePayload || (await fetchTimeseriesPayload("trailingPeRatio"));
    } catch {
      // continue
    }

    try {
      forwardPePayload = forwardPePayload || (await fetchTimeseriesPayload("forwardPeRatio"));
    } catch {
      // continue
    }

    try {
      trailingPegPayload = trailingPegPayload || (await fetchTimeseriesPayload("trailingPegRatio"));
    } catch {
      // continue
    }
  }

  let keyStatisticsPayload: RatioPayload | null = null;

  parseKeyStatisticsHtml:
  for (const candidate of candidates) {
    for (const host of YAHOO_KEY_STATISTICS_HOSTS) {
      const urls = [
        `${host}/quote/${encodeURIComponent(candidate)}/`,
        `${host}/quote/${encodeURIComponent(candidate)}/key-statistics/`,
      ];

      for (const url of urls) {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          try {
            const { stdout } = await execFileAsync(
              "curl",
              [
                "-4",
                "-sSL",
                "--compressed",
                "--max-time",
                "25",
                "-A",
                "Mozilla/5.0",
                "-H",
                "accept-language: en-US,en;q=0.9",
                url,
              ],
              { maxBuffer: 24 * 1024 * 1024 }
            );
            const html = String(stdout || "").trim();
            if (!html || isRejectedPayload(html)) {
              continue;
            }

            const payload = parseYahooKeyStatisticsRatioPayload(html);
            if (payload) {
              keyStatisticsPayload = {
                ...payload,
                source: `${payload.source}:${host.replace(/^https?:\/\//i, "")}`,
              };
              break parseKeyStatisticsHtml;
            }
          } catch {
            // retry once, then fallback to next url/host
          }
        }
      }
    }
  }

  quoteSummaryPayload = sanitizeYahooRatioPayloadMetrics(quoteSummaryPayload);
  quoteApiPayload = sanitizeYahooRatioPayloadMetrics(quoteApiPayload);
  trailingPePayload = sanitizeYahooRatioPayloadMetrics(trailingPePayload);
  forwardPePayload = sanitizeYahooRatioPayloadMetrics(forwardPePayload);
  trailingPegPayload = sanitizeYahooRatioPayloadMetrics(trailingPegPayload);
  keyStatisticsPayload = sanitizeYahooRatioPayloadMetrics(keyStatisticsPayload);

  const quoteLatestPayload = mergeRatioPayloadList(
    [keyStatisticsPayload, trailingPePayload, forwardPePayload, quoteSummaryPayload, quoteApiPayload].filter(
      Boolean
    ) as RatioPayload[]
  );
  const payload = mergeRatioPayloadList(
    [
      quoteSummaryPayload,
      quoteApiPayload,
      keyStatisticsPayload,
      trailingPePayload,
      forwardPePayload,
      trailingPegPayload,
    ].filter(Boolean) as RatioPayload[]
  );
  return {
    payload,
    quoteLatestPayload,
  };
}

function applyLatestRatioOverrideAtLastDate(
  payload: RatioPayload | null,
  latestOverride: RatioPayload | null,
  lastDate: string
): RatioPayload | null {
  if (!payload && !latestOverride) return null;
  if (!lastDate || !latestOverride) return payload;

  const overrideSource = String(latestOverride.source || "").trim();
  const overrideLatest = {
    pe_ttm: sanitizeYahooMetricValue("pe_ttm", latestOverride.latest.pe_ttm, overrideSource),
    pe_forward: sanitizeYahooMetricValue("pe_forward", latestOverride.latest.pe_forward, overrideSource),
    // Yahoo PB may have mixed share-class/ADR basis on certain tickers; keep existing PB source for stability.
    pb: null,
    peg: sanitizeYahooMetricValue("peg", latestOverride.latest.peg, overrideSource),
  };

  if (!overrideLatest.pe_ttm && !overrideLatest.pe_forward && !overrideLatest.pb && !overrideLatest.peg) {
    return payload;
  }

  const base: RatioPayload = payload
    ? {
        anchors: [...(payload.anchors || [])],
        latest: {
          pe_ttm: sanitizeSignedRatio(payload.latest.pe_ttm),
          pe_forward: sanitizeSignedRatio(payload.latest.pe_forward),
          pb: sanitizeSignedRatio(payload.latest.pb),
          peg: sanitizeSignedRatio(payload.latest.peg),
        },
        source: payload.source,
      }
    : {
        anchors: [],
        latest: {
          pe_ttm: null,
          pe_forward: null,
          pb: null,
          peg: null,
        },
        source: "",
      };

  const byDate = new Map<string, RatioAnchor>();
  for (const item of base.anchors) {
    byDate.set(item.date, {
      date: item.date,
      pe_ttm: sanitizeSignedRatio(item.pe_ttm),
      pe_forward: sanitizeSignedRatio(item.pe_forward),
      pb: sanitizeSignedRatio(item.pb),
      peg: sanitizeSignedRatio(item.peg),
    });
  }

  const current = byDate.get(lastDate) || {
    date: lastDate,
    pe_ttm: null,
    pe_forward: null,
    pb: null,
    peg: null,
  };

  if (overrideLatest.pe_ttm) {
    current.pe_ttm = overrideLatest.pe_ttm;
    base.latest.pe_ttm = overrideLatest.pe_ttm;
  }
  const orderedByDate = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  const currentIndexInSeries = orderedByDate.findIndex((item) => item.date === lastDate);
  const forwardOverrideOutlier =
    overrideLatest.pe_forward &&
    isYahooForwardOverrideLikelyOutlier(
      orderedByDate.map((item) => ({
        date: item.date,
        close: null,
        pe_ttm: item.pe_ttm || 0,
        pe_forward: item.pe_forward,
        pb: item.pb || 0,
        peg: item.peg,
        us10y_yield: 0,
      })),
      currentIndexInSeries >= 0 ? currentIndexInSeries : orderedByDate.length,
      overrideLatest.pe_forward
    );

  if (overrideLatest.pe_forward && !forwardOverrideOutlier) {
    current.pe_forward = overrideLatest.pe_forward;
    base.latest.pe_forward = overrideLatest.pe_forward;
  }
  if (overrideLatest.pb) {
    current.pb = overrideLatest.pb;
    base.latest.pb = overrideLatest.pb;
  }
  if (overrideLatest.peg) {
    current.peg = overrideLatest.peg;
    base.latest.peg = overrideLatest.peg;
  }

  byDate.set(lastDate, current);
  base.anchors = [...byDate.values()]
    .filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg)
    .sort((a, b) => a.date.localeCompare(b.date));
  base.source = [base.source, latestOverride.source].filter(Boolean).join("+");
  return base;
}

function applyLatestRatioOverrideToLastPoint(
  valuationPoints: SnapshotPoint[],
  latestOverride: RatioPayload | null
): SnapshotPoint[] {
  if (!Array.isArray(valuationPoints) || !valuationPoints.length || !latestOverride) {
    return valuationPoints;
  }

  const overrideSource = String(latestOverride.source || "").trim();
  const overridePeTtm = sanitizeYahooMetricValue("pe_ttm", latestOverride.latest.pe_ttm, overrideSource);
  const overridePeForward = sanitizeYahooMetricValue("pe_forward", latestOverride.latest.pe_forward, overrideSource);
  const lastIndex = valuationPoints.length - 1;
  const isForwardOutlier =
    overridePeForward !== null &&
    isYahooForwardOverrideLikelyOutlier(valuationPoints, lastIndex, overridePeForward);
  const safeOverridePeForward = isForwardOutlier ? null : overridePeForward;
  if (!overridePeTtm && !safeOverridePeForward) {
    return valuationPoints;
  }

  const currentLastPoint = valuationPoints[lastIndex];
  let nextLastPoint: SnapshotPoint | null = null;

  if (overridePeTtm && overridePeTtm !== sanitizeSignedRatio(currentLastPoint.pe_ttm)) {
    nextLastPoint = {
      ...(nextLastPoint || currentLastPoint),
      pe_ttm: roundTo(overridePeTtm, 6),
    };
  }

  if (safeOverridePeForward && safeOverridePeForward !== sanitizeSignedRatio(currentLastPoint.pe_forward)) {
    nextLastPoint = {
      ...(nextLastPoint || currentLastPoint),
      pe_forward: roundTo(safeOverridePeForward, 4),
    };
  }

  if (!nextLastPoint) {
    return valuationPoints;
  }

  const nextPoints = [...valuationPoints];
  nextPoints[lastIndex] = nextLastPoint;
  return nextPoints;
}

function normalizeYahooDailyMetricSnapshot(raw: unknown): YahooDailyMetricSnapshot | null {
  if (!raw || typeof raw !== "object") return null;
  const item = raw as Record<string, unknown>;
  const date = String(item.date || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;

  const source = String(item.source || "").trim();
  const peTtm = sanitizeYahooMetricValue("pe_ttm", item.pe_ttm, source);
  const peForward = sanitizeYahooMetricValue("pe_forward", item.pe_forward, source);
  const pb = sanitizeYahooMetricValue("pb", item.pb, source);
  const peg = sanitizeYahooMetricValue("peg", item.peg, source);
  if (!peTtm && !peForward && !pb && !peg) return null;

  return {
    date,
    pe_ttm: peTtm,
    pe_forward: peForward,
    pb,
    peg,
    source,
    capturedAt: String(item.capturedAt || "").trim(),
  };
}

function createYahooDailyMetricSnapshot(date: string, payload: RatioPayload | null): YahooDailyMetricSnapshot | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !payload) return null;

  const source = String(payload.source || "").trim();
  const peTtm = sanitizeYahooMetricValue("pe_ttm", payload.latest.pe_ttm, source);
  const peForward = sanitizeYahooMetricValue("pe_forward", payload.latest.pe_forward, source);
  const pb = sanitizeYahooMetricValue("pb", payload.latest.pb, source);
  const peg = sanitizeYahooMetricValue("peg", payload.latest.peg, source);
  if (!peTtm && !peForward && !pb && !peg) return null;

  return {
    date,
    pe_ttm: peTtm,
    pe_forward: peForward,
    pb,
    peg,
    source,
    capturedAt: new Date().toISOString(),
  };
}

function upsertYahooDailyMetricSnapshot(
  bySymbol: Map<string, YahooDailyMetricSnapshot[]>,
  symbol: string,
  snapshot: YahooDailyMetricSnapshot | null
): void {
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();
  if (!normalizedSymbol || !snapshot) return;

  const nextByDate = new Map<string, YahooDailyMetricSnapshot>();
  for (const item of bySymbol.get(normalizedSymbol) || []) {
    const normalized = normalizeYahooDailyMetricSnapshot(item);
    if (!normalized) continue;
    nextByDate.set(normalized.date, normalized);
  }

  nextByDate.set(snapshot.date, snapshot);
  bySymbol.set(
    normalizedSymbol,
    [...nextByDate.values()].sort((a, b) => a.date.localeCompare(b.date))
  );
}

function applyYahooDailyMetricSnapshotsToPoints(
  valuationPoints: SnapshotPoint[],
  snapshots: YahooDailyMetricSnapshot[]
): SnapshotPoint[] {
  if (!Array.isArray(valuationPoints) || !valuationPoints.length || !Array.isArray(snapshots) || !snapshots.length) {
    return valuationPoints;
  }

  const byDate = new Map<string, YahooDailyMetricSnapshot>();
  for (const item of snapshots) {
    const normalized = normalizeYahooDailyMetricSnapshot(item);
    if (!normalized) continue;
    byDate.set(normalized.date, normalized);
  }
  if (!byDate.size) return valuationPoints;

  let changed = false;
  const nextPoints = valuationPoints.map((point, index) => {
    const snapshot = byDate.get(point.date);
    if (!snapshot) return point;

    let nextPoint: SnapshotPoint | null = null;

    if (snapshot.pe_ttm && snapshot.pe_ttm !== sanitizeSignedRatio(point.pe_ttm)) {
      nextPoint = {
        ...(nextPoint || point),
        pe_ttm: roundTo(snapshot.pe_ttm, 6),
      };
    }

    const shouldSkipForwardSnapshot =
      snapshot.pe_forward !== null &&
      isYahooForwardOverrideLikelyOutlier(valuationPoints, index, snapshot.pe_forward);
    if (!shouldSkipForwardSnapshot && snapshot.pe_forward && snapshot.pe_forward !== sanitizeSignedRatio(point.pe_forward)) {
      nextPoint = {
        ...(nextPoint || point),
        pe_forward: roundTo(snapshot.pe_forward, 4),
      };
    }

    if (snapshot.pb && snapshot.pb !== sanitizeSignedRatio(point.pb)) {
      nextPoint = {
        ...(nextPoint || point),
        pb: roundTo(snapshot.pb, 4),
      };
    }

    if (snapshot.peg && snapshot.peg !== sanitizeSignedRatio(point.peg)) {
      nextPoint = {
        ...(nextPoint || point),
        peg: roundTo(snapshot.peg, 4),
      };
    }

    if (!nextPoint) return point;
    changed = true;
    return nextPoint;
  });

  return changed ? nextPoints : valuationPoints;
}

function preserveRecordedYahooDailyPoints(
  generatedPoints: SnapshotPoint[],
  previousPoints: SnapshotPoint[],
  snapshots: YahooDailyMetricSnapshot[],
  maxDate = ""
): SnapshotPoint[] {
  if (!Array.isArray(generatedPoints) || !generatedPoints.length) {
    return generatedPoints;
  }
  if (!Array.isArray(previousPoints) || !previousPoints.length || !Array.isArray(snapshots) || !snapshots.length) {
    return generatedPoints;
  }

  const recordedDates = new Set(
    snapshots
      .map((item) => normalizeYahooDailyMetricSnapshot(item)?.date || "")
      .filter(
        (date) =>
          /^\d{4}-\d{2}-\d{2}$/.test(date) &&
          (!maxDate || date <= maxDate)
      )
  );
  if (!recordedDates.size) return generatedPoints;

  const byDate = new Map<string, SnapshotPoint>();
  for (const point of generatedPoints) {
    const date = String(point?.date || "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;
    byDate.set(date, point);
  }

  let changed = false;
  for (const point of previousPoints) {
    const date = String(point?.date || "");
    if ((!maxDate || date <= maxDate) && recordedDates.has(date) && !byDate.has(date)) {
      byDate.set(date, point);
      changed = true;
    }
  }

  if (!changed) return generatedPoints;
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function mergeRatioPayloadList(payloads: RatioPayload[]): RatioPayload | null {
  const normalized = payloads.filter(
    (item) =>
      item &&
      (item.anchors.length ||
        item.latest.pe_ttm ||
        item.latest.pe_forward ||
        item.latest.pb ||
        item.latest.peg)
  );
  if (!normalized.length) return null;

  const byDate = new Map<string, RatioAnchor>();
  const latest: RatioPayload["latest"] = {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
    peg: null,
  };
  const sourceTags: string[] = [];

  for (const payload of normalized) {
    sourceTags.push(payload.source);

    for (const item of payload.anchors) {
      const current = byDate.get(item.date) || {
        date: item.date,
        pe_ttm: null,
        pe_forward: null,
        pb: null,
        peg: null,
      };
      byDate.set(item.date, {
        date: item.date,
        pe_ttm: current.pe_ttm ?? sanitizeSignedRatio(item.pe_ttm),
        pe_forward: current.pe_forward ?? sanitizeSignedRatio(item.pe_forward),
        pb: current.pb ?? sanitizeSignedRatio(item.pb),
        peg: current.peg ?? sanitizeSignedRatio(item.peg),
      });
    }

    latest.pe_ttm = latest.pe_ttm ?? sanitizeSignedRatio(payload.latest.pe_ttm);
    latest.pe_forward = latest.pe_forward ?? sanitizeSignedRatio(payload.latest.pe_forward);
    latest.pb = latest.pb ?? sanitizeSignedRatio(payload.latest.pb);
    latest.peg = latest.peg ?? sanitizeSignedRatio(payload.latest.peg);
  }

  const anchors = [...byDate.values()]
    .filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg)
    .sort((a, b) => a.date.localeCompare(b.date));

  if (!anchors.length && !latest.pe_ttm && !latest.pe_forward && !latest.pb && !latest.peg) {
    return null;
  }

  return {
    anchors,
    latest,
    source: [...new Set(sourceTags.filter(Boolean))].join("+"),
  };
}

function resolveStockAnalysisTableRef(
  value: unknown,
  table: unknown[],
  memo: Map<number, unknown>
): unknown {
  if (value === -1) return undefined;

  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value < table.length) {
    if (memo.has(value)) return memo.get(value);
    memo.set(value, null);
    const resolved = resolveStockAnalysisTableRef(table[value], table, memo);
    memo.set(value, resolved);
    return resolved;
  }

  if (Array.isArray(value)) {
    return value.map((item) => resolveStockAnalysisTableRef(item, table, memo));
  }

  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      out[key] = resolveStockAnalysisTableRef(entry, table, memo);
    }
    return out;
  }

  return value;
}

function parseStockAnalysisDataJsonRoot(rawText: string): Record<string, unknown> | null {
  let parsed: {
    nodes?: Array<{ data?: unknown }>;
  };
  try {
    parsed = JSON.parse(rawText) as {
      nodes?: Array<{ data?: unknown }>;
    };
  } catch {
    return null;
  }

  const table = parsed.nodes?.[2]?.data;
  if (!Array.isArray(table) || !table.length) return null;

  const root = resolveStockAnalysisTableRef(0, table, new Map()) as Record<string, unknown>;
  if (!root || typeof root !== "object") return null;
  return root;
}

function parseRatioPayloadFromDataJson(rawText: string): StockAnalysisDataRatioPayloadResult | null {
  const root = parseStockAnalysisDataJsonRoot(rawText);
  if (!root) return null;

  const rawAvailableSources = Array.isArray(root.availableSources) ? root.availableSources : [];
  const availableSources = rawAvailableSources
    .map((item) => String(item || "").trim().toLowerCase())
    .filter(Boolean);
  const selectedSource = String(root.source || root.selectedSource || root.defaultSource || "")
    .trim()
    .toLowerCase();

  const financialData = root.financialData as Record<string, unknown> | undefined;
  if (!financialData || typeof financialData !== "object") {
    return {
      payload: null,
      selectedSource,
      availableSources,
    };
  }

  const pickArray = (...keys: string[]): unknown[] => {
    for (const key of keys) {
      const value = financialData[key];
      if (Array.isArray(value)) return value;
    }
    return [];
  };

  const dateKeys = pickArray("datekey").map((item) => String(item || ""));
  if (!dateKeys.length) {
    return {
      payload: null,
      selectedSource,
      availableSources,
    };
  }

  const peRaw = pickArray("pe");
  const fwdRaw = pickArray("peForward", "forwardPE", "forwardPe");
  const pbRaw = pickArray("pb", "ptbvRatio");
  const pegRaw = pickArray("pegRatio", "peg");

  const anchors: RatioAnchor[] = [];
  const ttmIndex = Math.max(
    0,
    dateKeys.findIndex((item) => item.toUpperCase() === "TTM")
  );
  const latest: RatioPayload["latest"] = {
    pe_ttm: sanitizeSignedRatio(peRaw[ttmIndex]),
    pe_forward: sanitizeSignedRatio(fwdRaw[ttmIndex]),
    pb: sanitizeSignedRatio(pbRaw[ttmIndex]),
    peg: sanitizeSignedRatio(pegRaw[ttmIndex]),
  };
  const trailingDate = toIsoDateFromText(
    (root.details as Record<string, unknown> | undefined)?.lastTrailingDate
  );

  if (trailingDate && (latest.pe_ttm || latest.pe_forward || latest.pb || latest.peg)) {
    anchors.push({
      date: trailingDate,
      // Keep trailing-date anchor focused on forward/PB to avoid injecting
      // potentially mismatched TTM bases (common on some ADR pages).
      pe_ttm: null,
      pe_forward: latest.pe_forward,
      pb: latest.pb,
      peg: latest.peg,
    });
  }

  for (let i = 0; i < dateKeys.length; i += 1) {
    if (i === ttmIndex) continue;

    const key = dateKeys[i];
    const normalizedDate = normalizeRatioDateKey(key);
    if (!normalizedDate) continue;

    const pe = sanitizeSignedRatio(peRaw[i]);
    const peForward = sanitizeSignedRatio(fwdRaw[i]);
    const pb = sanitizeSignedRatio(pbRaw[i]);
    const peg = sanitizeSignedRatio(pegRaw[i]);
    if (!pe && !peForward && !pb && !peg) continue;

    anchors.push({
      date: normalizedDate,
      pe_ttm: pe,
      pe_forward: peForward,
      pb,
      peg,
    });
  }

  const payload: RatioPayload | null =
    anchors.length || latest.pe_ttm || latest.pe_forward || latest.pb || latest.peg
      ? {
          anchors: anchors.sort((a, b) => a.date.localeCompare(b.date)),
          latest,
          source: `stockanalysis-data-json:${selectedSource || "unknown"}`,
        }
      : null;

  return {
    payload,
    selectedSource,
    availableSources,
  };
}

function parseStockAnalysisIncomeStatementQuarterlyPayload(
  rawText: string
): StockAnalysisIncomeStatementQuarterlyPayload {
  const root = parseStockAnalysisDataJsonRoot(rawText);
  if (!root) {
    return {
      epsRows: [],
      netIncomeRows: [],
      shareRows: [],
    };
  }

  const financialData = root.financialData as Record<string, unknown> | undefined;
  if (!financialData || typeof financialData !== "object") {
    return {
      epsRows: [],
      netIncomeRows: [],
      shareRows: [],
    };
  }

  const pickArray = (...keys: string[]): unknown[] => {
    for (const key of keys) {
      const value = financialData[key];
      if (Array.isArray(value)) return value;
    }
    return [];
  };

  const dateKeys = pickArray("datekey");
  const epsRaw = pickArray("epsdil", "epsDiluted", "epsBasic", "eps");
  const netIncomeRaw = pickArray("netinc", "netIncome", "netIncomeLoss", "profitloss");
  const shareRaw = pickArray("shareswadil", "sharesDiluted", "shareswa", "shares");

  const epsByDate = new Map<string, QuarterlyEpsPoint>();
  const netIncomeByDate = new Map<string, QuarterlyNetIncomePoint>();
  const shareByDate = new Map<string, QuarterlyShareCountPoint>();

  for (let i = 0; i < dateKeys.length; i += 1) {
    const date = toIsoDateFromText(dateKeys[i]);
    if (!date) continue;

    const eps = sanitizeEps(epsRaw[i]);
    if (eps !== null) {
      epsByDate.set(date, {
        date,
        eps,
        source: "actual",
        availableDate: date,
      });
    }

    const netIncome = sanitizeNetIncome(netIncomeRaw[i]);
    if (netIncome !== null) {
      netIncomeByDate.set(date, {
        date,
        netIncome,
        source: "actual",
      });
    }

    const shares = sanitizeShareCount(shareRaw[i]);
    if (shares !== null) {
      shareByDate.set(date, {
        date,
        shares,
      });
    }
  }

  return {
    epsRows: [...epsByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
    netIncomeRows: [...netIncomeByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
    shareRows: [...shareByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
  };
}

function parseQuarterlyEpsFromIncomeStatementDataJson(rawText: string): QuarterlyEpsPoint[] {
  return parseStockAnalysisIncomeStatementQuarterlyPayload(rawText).epsRows;
}

function parseQuarterlyNetIncomeFromIncomeStatementDataJson(rawText: string): QuarterlyNetIncomePoint[] {
  return parseStockAnalysisIncomeStatementQuarterlyPayload(rawText).netIncomeRows;
}

function parseQuarterlyShareCountFromIncomeStatementDataJson(rawText: string): QuarterlyShareCountPoint[] {
  return parseStockAnalysisIncomeStatementQuarterlyPayload(rawText).shareRows;
}

function parseStockAnalysisForecastQuarterlyPayload(rawText: string): StockAnalysisForecastQuarterlyPayload {
  const root = parseStockAnalysisDataJsonRoot(rawText);
  if (!root) {
    return {
      epsRows: [],
      netIncomeRows: [],
    };
  }

  const quarterlyTable = (
    (root.estimates as Record<string, unknown> | undefined)?.table as Record<string, unknown> | undefined
  )?.quarterly as Record<string, unknown> | undefined;
  if (!quarterlyTable || typeof quarterlyTable !== "object") {
    return {
      epsRows: [],
      netIncomeRows: [],
    };
  }

  const dateKeys = Array.isArray(quarterlyTable.dates) ? quarterlyTable.dates : [];
  const epsRaw = Array.isArray(quarterlyTable.eps) ? quarterlyTable.eps : [];
  const netIncomeRaw = Array.isArray(quarterlyTable.netIncome)
    ? quarterlyTable.netIncome
    : Array.isArray(quarterlyTable.netincome)
      ? quarterlyTable.netincome
      : Array.isArray(quarterlyTable.netinc)
        ? quarterlyTable.netinc
        : [];
  if (!dateKeys.length) {
    return {
      epsRows: [],
      netIncomeRows: [],
    };
  }

  const epsByDate = new Map<string, QuarterlyEpsPoint>();
  const netIncomeByDate = new Map<string, QuarterlyNetIncomePoint>();

  for (let i = 0; i < dateKeys.length; i += 1) {
    const date = toIsoDateFromText(dateKeys[i]);
    if (!date) continue;

    const eps = sanitizeEps(epsRaw[i]);
    if (eps !== null) {
      epsByDate.set(date, {
        date,
        eps,
        source: "expected",
        availableDate: date,
      });
    }

    const netIncome = sanitizeNetIncome(netIncomeRaw[i]);
    if (netIncome !== null) {
      netIncomeByDate.set(date, {
        date,
        netIncome,
        source: "expected",
      });
    }
  }

  return {
    epsRows: [...epsByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
    netIncomeRows: [...netIncomeByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
  };
}

function parseQuarterlyEpsFromForecastDataJson(rawText: string): QuarterlyEpsPoint[] {
  return parseStockAnalysisForecastQuarterlyPayload(rawText).epsRows;
}

function parseQuarterlyNetIncomeFromForecastDataJson(rawText: string): QuarterlyNetIncomePoint[] {
  return parseStockAnalysisForecastQuarterlyPayload(rawText).netIncomeRows;
}

function mergeQuarterlyEpsSeries(
  actualRows: QuarterlyEpsPoint[],
  expectedRows: QuarterlyEpsPoint[],
  lastCloseDate: string
): QuarterlyEpsPoint[] {
  const actualSorted = [...actualRows].sort((a, b) => a.date.localeCompare(b.date));
  const latestActualDate = actualSorted[actualSorted.length - 1]?.date || "";
  const latestActualTs = toTs(latestActualDate);
  const expectedSorted = [...expectedRows].sort((a, b) => a.date.localeCompare(b.date));

  const expectedHorizonDate = lastCloseDate ? addDays(lastCloseDate, 125) : "";
  const expectedCurrentQuarter = expectedSorted.find((row) => {
    if (!row?.date) return false;
    if (latestActualDate && row.date <= latestActualDate) return false;
    if (latestActualTs) {
      const dayGap = (toTs(row.date) - latestActualTs) / 86_400_000;
      // Some forecast tables keep a stale estimate for the quarter that just reported.
      // Skip near-duplicate quarter rows and only keep the next real quarter estimate.
      if (Number.isFinite(dayGap) && dayGap > 0 && dayGap < 45) return false;
    }
    if (expectedHorizonDate && row.date > expectedHorizonDate) return false;
    return true;
  });

  const byDate = new Map<string, QuarterlyEpsPoint>();

  if (expectedCurrentQuarter) {
    byDate.set(expectedCurrentQuarter.date, expectedCurrentQuarter);
  }

  for (const row of actualSorted) {
    byDate.set(row.date, row);
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function mergeQuarterlyNetIncomeSeries(
  actualRows: QuarterlyNetIncomePoint[],
  expectedRows: QuarterlyNetIncomePoint[],
  lastCloseDate: string
): QuarterlyNetIncomePoint[] {
  const actualSorted = [...actualRows].sort((a, b) => a.date.localeCompare(b.date));
  const latestActualDate = actualSorted[actualSorted.length - 1]?.date || "";
  const latestActualTs = toTs(latestActualDate);
  const expectedSorted = [...expectedRows].sort((a, b) => a.date.localeCompare(b.date));

  const expectedHorizonDate = lastCloseDate ? addDays(lastCloseDate, 125) : "";
  const expectedCurrentQuarter = expectedSorted.find((row) => {
    if (!row?.date) return false;
    if (latestActualDate && row.date <= latestActualDate) return false;
    if (latestActualTs) {
      const dayGap = (toTs(row.date) - latestActualTs) / 86_400_000;
      if (Number.isFinite(dayGap) && dayGap > 0 && dayGap < 45) return false;
    }
    if (expectedHorizonDate && row.date > expectedHorizonDate) return false;
    return true;
  });

  const byDate = new Map<string, QuarterlyNetIncomePoint>();

  if (expectedCurrentQuarter) {
    byDate.set(expectedCurrentQuarter.date, expectedCurrentQuarter);
  }

  for (const row of actualSorted) {
    byDate.set(row.date, row);
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function mergeExpectedQuarterlyNetIncomeCandidates(
  forecastRows: QuarterlyNetIncomePoint[],
  estimatedRows: QuarterlyNetIncomePoint[]
): QuarterlyNetIncomePoint[] {
  const byQuarter = new Map<
    string,
    QuarterlyNetIncomePoint & {
      quarterKey: string;
      rank: number;
    }
  >();

  const upsert = (rows: QuarterlyNetIncomePoint[], rank: number) => {
    for (const rawRow of rows || []) {
      const date = toIsoDateFromText(rawRow?.date);
      const netIncome = sanitizeNetIncome(rawRow?.netIncome);
      const quarterKey = quarterKeyFromDate(date || "");
      if (!date || netIncome === null || !quarterKey) continue;

      const candidate = {
        date,
        netIncome,
        source: "expected" as const,
        quarterKey,
        rank,
      };
      const current = byQuarter.get(quarterKey);
      if (!current) {
        byQuarter.set(quarterKey, candidate);
        continue;
      }

      if (candidate.rank < current.rank || (candidate.rank === current.rank && candidate.date < current.date)) {
        byQuarter.set(quarterKey, candidate);
      }
    }
  };

  // Prefer direct market consensus net income estimates; fall back to EPS*shares.
  upsert(forecastRows, 0);
  upsert(estimatedRows, 1);

  return [...byQuarter.values()]
    .sort((a, b) => a.date.localeCompare(b.date))
    .map(({ date, netIncome, source }) => ({ date, netIncome, source }));
}

function estimateExpectedQuarterlyNetIncomeFromEps(
  expectedEpsRows: QuarterlyEpsPoint[],
  shareRows: QuarterlyShareCountPoint[]
): QuarterlyNetIncomePoint[] {
  if (!Array.isArray(expectedEpsRows) || !expectedEpsRows.length) return [];
  if (!Array.isArray(shareRows) || !shareRows.length) return [];

  const orderedShares = [...shareRows]
    .map((row) => ({
      date: toIsoDateFromText(row?.date),
      shares: sanitizeShareCount(row?.shares),
      quarterKey: quarterKeyFromDate(String(row?.date || "")),
    }))
    .filter((row): row is { date: string; shares: number; quarterKey: string } => Boolean(row.date && row.shares && row.quarterKey))
    .sort((a, b) => a.date.localeCompare(b.date));
  if (!orderedShares.length) return [];

  const sharesByQuarter = new Map<string, { date: string; shares: number }>();
  for (const row of orderedShares) {
    const current = sharesByQuarter.get(row.quarterKey);
    if (!current || row.date < current.date) {
      sharesByQuarter.set(row.quarterKey, { date: row.date, shares: row.shares });
    }
  }

  const pickShareCount = (date: string, quarterKey: string): number | null => {
    const exact = sharesByQuarter.get(quarterKey);
    if (exact && Number.isFinite(exact.shares) && exact.shares > 0) {
      return exact.shares;
    }

    let fallback: { date: string; shares: number } | null = null;
    for (const row of orderedShares) {
      if (row.date <= date) {
        fallback = row;
      } else {
        break;
      }
    }
    if (fallback && Number.isFinite(fallback.shares) && fallback.shares > 0) {
      return fallback.shares;
    }

    const latest = orderedShares[orderedShares.length - 1];
    return latest && Number.isFinite(latest.shares) && latest.shares > 0 ? latest.shares : null;
  };

  const byQuarter = new Map<string, QuarterlyNetIncomePoint & { quarterKey: string }>();
  for (const expectedRow of expectedEpsRows) {
    if (!expectedRow || expectedRow.source !== "expected") continue;
    const date = toIsoDateFromText(expectedRow.date);
    const eps = sanitizeEps(expectedRow.eps);
    const quarterKey = quarterKeyFromDate(date || "");
    if (!date || eps === null || !quarterKey) continue;

    const shares = pickShareCount(date, quarterKey);
    if (!shares || !Number.isFinite(shares) || shares <= 0) continue;

    const netIncome = sanitizeNetIncome(eps * shares);
    if (netIncome === null) continue;

    const current = byQuarter.get(quarterKey);
    const candidate = {
      date,
      netIncome,
      source: "expected" as const,
      quarterKey,
    };
    if (!current || date < current.date) {
      byQuarter.set(quarterKey, candidate);
    }
  }

  return [...byQuarter.values()]
    .sort((a, b) => a.date.localeCompare(b.date))
    .map(({ date, netIncome, source }) => ({ date, netIncome, source }));
}

function parseRatioNumber(rawValue: unknown): number | null {
  const text = String(rawValue ?? "")
    .replace(/[,%xX]/g, "")
    .trim();
  if (!text) return null;
  if (/^(n\/a|na|--|-|none)$/i.test(text)) return null;
  return sanitizeSignedRatio(text);
}

function parseRatioPayloadFromStatisticsDataJson(rawText: string): StockAnalysisStatisticsRatioPayloadResult | null {
  let parsed: {
    type?: string;
    location?: string;
    nodes?: Array<{ data?: unknown }>;
  };
  try {
    parsed = JSON.parse(rawText) as {
      type?: string;
      location?: string;
      nodes?: Array<{ data?: unknown }>;
    };
  } catch {
    return null;
  }

  if (String(parsed.type || "").toLowerCase() === "redirect") {
    return {
      payload: null,
      redirectPath: String(parsed.location || "").trim(),
    };
  }

  const table = parsed.nodes?.[2]?.data;
  if (!Array.isArray(table) || !table.length) {
    return {
      payload: null,
      redirectPath: "",
    };
  }

  const root = resolveStockAnalysisTableRef(0, table, new Map()) as Record<string, unknown>;
  if (!root || typeof root !== "object") {
    return {
      payload: null,
      redirectPath: "",
    };
  }

  const ratioEntries = (root.ratios as Record<string, unknown> | undefined)?.data;
  if (!Array.isArray(ratioEntries)) {
    return {
      payload: null,
      redirectPath: "",
    };
  }

  let peTtm: number | null = null;
  let peForward: number | null = null;
  let pb: number | null = null;
  let peg: number | null = null;

  for (const entry of ratioEntries) {
    if (!entry || typeof entry !== "object") continue;
    const ratioObj = entry as Record<string, unknown>;
    const id = String(ratioObj.id || "").trim().toLowerCase();
    if (!id) continue;

    const value = parseRatioNumber(ratioObj.hover ?? ratioObj.value ?? null);
    if (!value) continue;

    if ((id === "pe" || id === "peratio") && !peTtm) {
      peTtm = value;
      continue;
    }
    if ((id === "peforward" || id === "forwardpe" || id === "forwardperatio") && !peForward) {
      peForward = value;
      continue;
    }
    if ((id === "pb" || id === "pbratio") && !pb) {
      pb = value;
      continue;
    }
    if ((id === "peg" || id === "pegratio") && !peg) {
      peg = value;
    }
  }

  if (!peTtm && !peForward && !pb && !peg) {
    return {
      payload: null,
      redirectPath: "",
    };
  }

  return {
    payload: {
      anchors: [],
      latest: {
        pe_ttm: peTtm,
        pe_forward: peForward,
        pb,
        peg,
      },
      source: "stockanalysis-statistics-data-json",
    },
    redirectPath: "",
  };
}

function toStockAnalysisDataJsonUrl(pathOrUrl: string): string {
  let pathText = String(pathOrUrl || "").trim();
  if (!pathText) return "";

  if (/^https?:\/\//i.test(pathText)) {
    try {
      const parsed = new URL(pathText);
      pathText = parsed.pathname;
    } catch {
      return "";
    }
  }

  if (!pathText.startsWith("/")) {
    pathText = `/${pathText}`;
  }

  if (!pathText.includes("__data.json")) {
    if (!pathText.endsWith("/")) {
      pathText += "/";
    }
    pathText += "__data.json";
  }

  return `https://stockanalysis.com${pathText}`;
}

async function fetchStockAnalysisStatisticsRatioPayload(symbol: string): Promise<RatioPayload | null> {
  const queue = getStockAnalysisSlugCandidates(symbol).map((slug) => `/stocks/${slug}/statistics/`);
  const seenUrls = new Set<string>();

  while (queue.length) {
    const candidate = queue.shift();
    if (!candidate) continue;

    const url = toStockAnalysisDataJsonUrl(candidate);
    if (!url || seenUrls.has(url)) continue;
    seenUrls.add(url);

    try {
      const raw = await fetchText(url, 1, 12000);
      const parsed = parseRatioPayloadFromStatisticsDataJson(raw);
      if (!parsed) continue;

      if (parsed.redirectPath) {
        queue.push(parsed.redirectPath);
      }

      if (parsed.payload) {
        const routeTag = (() => {
          try {
            const pathname = new URL(url).pathname;
            return pathname.replace(/\/__data\.json$/i, "");
          } catch {
            return "/statistics";
          }
        })();

        return {
          ...parsed.payload,
          source: `${parsed.payload.source}:${routeTag}`,
        };
      }
    } catch {
      // try next candidate
    }
  }

  return null;
}

function getStockAnalysisDataJsonUrl(slug: string, period: "annual" | "quarterly"): string {
  const suffix = period === "quarterly" ? "?p=quarterly" : "";
  return `https://stockanalysis.com/stocks/${slug}/financials/ratios/__data.json${suffix}`;
}

function getStockAnalysisIncomeStatementDataJsonUrl(slug: string): string {
  return `https://stockanalysis.com/stocks/${slug}/financials/income-statement/__data.json?p=quarterly`;
}

function getStockAnalysisForecastDataJsonUrl(slug: string): string {
  return `https://stockanalysis.com/stocks/${slug}/forecast/__data.json`;
}

async function fetchStockAnalysisDataRatioPayload(
  slug: string,
  period: "annual" | "quarterly",
  source = ""
): Promise<StockAnalysisDataRatioPayloadResult | null> {
  const url = getStockAnalysisDataJsonUrl(slug, period);
  const headers = source ? [`cookie: finsrc=${source}`] : [];
  const raw = await fetchText(url, 1, period === "quarterly" ? 16000 : 13000, false, { headers });
  return parseRatioPayloadFromDataJson(raw);
}

async function fetchStockAnalysisIncomeStatementQuarterlyPayload(
  slug: string
): Promise<StockAnalysisIncomeStatementQuarterlyPayload> {
  const url = getStockAnalysisIncomeStatementDataJsonUrl(slug);
  const raw = await fetchText(url, 1, 16000);
  return parseStockAnalysisIncomeStatementQuarterlyPayload(raw);
}

async function fetchStockAnalysisForecastQuarterlyPayload(
  slug: string
): Promise<StockAnalysisForecastQuarterlyPayload> {
  const url = getStockAnalysisForecastDataJsonUrl(slug);
  const raw = await fetchText(url, 1, 12000);
  return parseStockAnalysisForecastQuarterlyPayload(raw);
}

function parseSecQuarterlyEpsFromCompanyFacts(rawText: string): QuarterlyEpsPoint[] {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return [];
  }

  const taxonomyCandidates: Array<{ taxonomy: string; metrics: string[] }> = [
    {
      taxonomy: "us-gaap",
      metrics: [
        "EarningsPerShareDiluted",
        "IncomeLossFromContinuingOperationsPerDilutedShare",
        "EarningsPerShareBasicAndDiluted",
        "EarningsPerShareBasic",
      ],
    },
    {
      taxonomy: "ifrs-full",
      metrics: [
        "DilutedEarningsLossPerShare",
        "BasicEarningsLossPerShare",
      ],
    },
  ];

  const pickUnitRows = (factsRoot: Record<string, unknown>, metricKey: string): unknown[] => {
    const metric = factsRoot[metricKey] as Record<string, unknown> | undefined;
    const units = metric?.units as Record<string, unknown> | undefined;
    if (!units || typeof units !== "object") return [];

    const unitPriority = ["USD/shares", "USD / shares", "USD/share", "USD / share", "pure"];
    for (const unitName of unitPriority) {
      const candidate = units[unitName];
      if (Array.isArray(candidate)) return candidate;
    }

    for (const candidate of Object.values(units)) {
      if (Array.isArray(candidate)) return candidate;
    }

    return [];
  };

  const extractQuarterRows = (rawRows: unknown[]): QuarterlyEpsPoint[] => {
    type ScoredRow = {
      date: string;
      eps: number;
      filed: string;
      availableDate: string;
      hasQuarterDuration: boolean;
    };

    const byDate = new Map<string, ScoredRow>();
    const quarterFrameRegex = /Q[1-4]$/i;

    for (const rawEntry of rawRows) {
      if (!rawEntry || typeof rawEntry !== "object") continue;
      const entry = rawEntry as Record<string, unknown>;
      const date = toIsoDateFromText(entry.end);
      const eps = sanitizeEps(entry.val);
      const frame = String(entry.frame || "").trim().toUpperCase();
      const filed = toIsoDateFromText(entry.filed) || "0000-00-00";
      if (!date || eps === null || date < HISTORY_START_DATE) continue;
      const availableDate = filed >= date ? filed : date;

      const startDate = toIsoDateFromText(entry.start);
      const durationDays =
        startDate && toTs(date) > toTs(startDate) ? (toTs(date) - toTs(startDate)) / 86_400_000 : NaN;
      const hasQuarterDuration =
        Number.isFinite(durationDays) && durationDays >= 40 && durationDays <= 140;

      const fp = String(entry.fp || "").trim().toUpperCase();
      const isQuarterFp = fp === "Q1" || fp === "Q2" || fp === "Q3" || fp === "Q4";
      const isQuarterFrame = quarterFrameRegex.test(frame);

      // Prefer explicit quarter frames. If missing, fallback to quarter fp rows.
      if (!isQuarterFrame && !isQuarterFp) continue;

      const current = byDate.get(date);
      const candidate: ScoredRow = {
        date,
        eps,
        filed,
        availableDate,
        hasQuarterDuration,
      };

      if (!current) {
        byDate.set(date, candidate);
        continue;
      }

      if (candidate.hasQuarterDuration !== current.hasQuarterDuration) {
        if (candidate.hasQuarterDuration) {
          byDate.set(date, candidate);
        }
        continue;
      }

      if (candidate.filed > current.filed) {
        byDate.set(date, candidate);
        continue;
      }

      if (candidate.filed === current.filed && Math.abs(candidate.eps) < Math.abs(current.eps)) {
        // On duplicated quarter disclosures, the smaller magnitude is typically the single-quarter value.
        byDate.set(date, candidate);
      }
    }

    return [...byDate.values()]
      .sort((a, b) => a.date.localeCompare(b.date))
      .map((row) => ({
        date: row.date,
        eps: row.eps,
        source: "actual",
        availableDate: row.availableDate,
      }));
  };

  let bestRows: QuarterlyEpsPoint[] = [];
  for (const candidate of taxonomyCandidates) {
    const factsRoot = (parsed.facts as Record<string, unknown> | undefined)?.[candidate.taxonomy] as
      | Record<string, unknown>
      | undefined;
    if (!factsRoot || typeof factsRoot !== "object") continue;

    for (const metricKey of candidate.metrics) {
      const unitRows = pickUnitRows(factsRoot, metricKey);
      if (!unitRows.length) continue;
      const parsedRows = extractQuarterRows(unitRows);
      if (parsedRows.length > bestRows.length) {
        bestRows = parsedRows;
      }
    }
  }

  return bestRows;
}

function parseSecQuarterlyEpsAvailabilityByQuarterFromCompanyFacts(rawText: string): Map<string, string> {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return new Map();
  }

  const taxonomyCandidates: Array<{ taxonomy: string; metrics: string[] }> = [
    {
      taxonomy: "us-gaap",
      metrics: [
        "EarningsPerShareDiluted",
        "IncomeLossFromContinuingOperationsPerDilutedShare",
        "EarningsPerShareBasicAndDiluted",
        "EarningsPerShareBasic",
      ],
    },
    {
      taxonomy: "ifrs-full",
      metrics: [
        "DilutedEarningsLossPerShare",
        "BasicEarningsLossPerShare",
      ],
    },
  ];

  const pickUnitRows = (factsRoot: Record<string, unknown>, metricKey: string): unknown[] => {
    const metric = factsRoot[metricKey] as Record<string, unknown> | undefined;
    const units = metric?.units as Record<string, unknown> | undefined;
    if (!units || typeof units !== "object") return [];

    const unitPriority = ["USD/shares", "USD / shares", "USD/share", "USD / share", "pure"];
    for (const unitName of unitPriority) {
      const candidate = units[unitName];
      if (Array.isArray(candidate)) return candidate;
    }

    for (const candidate of Object.values(units)) {
      if (Array.isArray(candidate)) return candidate;
    }

    return [];
  };

  const availabilityByQuarter = new Map<string, string>();
  const quarterFrameRegex = /Q[1-4]$/i;
  const annualFrameRegex = /^CY\d{4}$/i;

  for (const candidate of taxonomyCandidates) {
    const factsRoot = (parsed.facts as Record<string, unknown> | undefined)?.[candidate.taxonomy] as
      | Record<string, unknown>
      | undefined;
    if (!factsRoot || typeof factsRoot !== "object") continue;

    for (const metricKey of candidate.metrics) {
      const unitRows = pickUnitRows(factsRoot, metricKey);
      if (!unitRows.length) continue;

      for (const rawEntry of unitRows) {
        if (!rawEntry || typeof rawEntry !== "object") continue;
        const entry = rawEntry as Record<string, unknown>;
        const date = toIsoDateFromText(entry.end);
        const quarterKey = quarterKeyFromDate(date || "");
        if (!date || !quarterKey || date < HISTORY_START_DATE) continue;

        const frame = String(entry.frame || "").trim().toUpperCase();
        const fp = String(entry.fp || "").trim().toUpperCase();
        const isQuarterFp = fp === "Q1" || fp === "Q2" || fp === "Q3" || fp === "Q4";
        const isQuarterFrame = quarterFrameRegex.test(frame);
        const isAnnualFp = fp === "FY";
        const isAnnualFrame = annualFrameRegex.test(frame);
        if (!isQuarterFp && !isQuarterFrame && !isAnnualFp && !isAnnualFrame) continue;

        const filed = toIsoDateFromText(entry.filed) || date;
        const availableDate = filed >= date ? filed : date;

        const current = availabilityByQuarter.get(quarterKey);
        if (!current || availableDate < current) {
          availabilityByQuarter.set(quarterKey, availableDate);
        }
      }
    }
  }

  return availabilityByQuarter;
}

function parseSecQuarterlyNetIncomeFromCompanyFacts(rawText: string): QuarterlyNetIncomePoint[] {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(rawText) as Record<string, unknown>;
  } catch {
    return [];
  }

  const taxonomyCandidates: Array<{ taxonomy: string; metrics: string[] }> = [
    {
      taxonomy: "us-gaap",
      metrics: [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
      ],
    },
    {
      taxonomy: "ifrs-full",
      metrics: [
        "ProfitLoss",
        "ProfitLossAttributableToOwnersOfParent",
      ],
    },
  ];

  const pickUnitRows = (factsRoot: Record<string, unknown>, metricKey: string): unknown[] => {
    const metric = factsRoot[metricKey] as Record<string, unknown> | undefined;
    const units = metric?.units as Record<string, unknown> | undefined;
    if (!units || typeof units !== "object") return [];

    const unitPriority = [
      "USD",
      "TWD",
      "JPY",
      "EUR",
      "CNY",
      "HKD",
      "CAD",
      "GBP",
      "KRW",
      "INR",
      "pure",
    ];
    for (const unitName of unitPriority) {
      const candidate = units[unitName];
      if (Array.isArray(candidate)) return candidate;
    }

    for (const [unitName, candidate] of Object.entries(units)) {
      if (!Array.isArray(candidate)) continue;
      if (String(unitName || "").toLowerCase().includes("/share")) continue;
      return candidate;
    }

    for (const candidate of Object.values(units)) {
      if (Array.isArray(candidate)) return candidate;
    }

    return [];
  };

  const extractQuarterRows = (rawRows: unknown[]): QuarterlyNetIncomePoint[] => {
    type ScoredRow = {
      date: string;
      netIncome: number;
      filed: string;
      hasQuarterDuration: boolean;
    };

    const byDate = new Map<string, ScoredRow>();
    const quarterFrameRegex = /Q[1-4]$/i;

    for (const rawEntry of rawRows) {
      if (!rawEntry || typeof rawEntry !== "object") continue;
      const entry = rawEntry as Record<string, unknown>;
      const date = toIsoDateFromText(entry.end);
      const netIncome = sanitizeNetIncome(entry.val);
      const frame = String(entry.frame || "").trim().toUpperCase();
      const filed = toIsoDateFromText(entry.filed) || "0000-00-00";
      if (!date || netIncome === null || date < HISTORY_START_DATE) continue;

      const startDate = toIsoDateFromText(entry.start);
      const durationDays =
        startDate && toTs(date) > toTs(startDate) ? (toTs(date) - toTs(startDate)) / 86_400_000 : NaN;
      const hasQuarterDuration =
        Number.isFinite(durationDays) && durationDays >= 40 && durationDays <= 140;

      const fp = String(entry.fp || "").trim().toUpperCase();
      const isQuarterFp = fp === "Q1" || fp === "Q2" || fp === "Q3" || fp === "Q4";
      const isQuarterFrame = quarterFrameRegex.test(frame);
      if (!isQuarterFrame && !isQuarterFp) continue;

      const current = byDate.get(date);
      const candidate: ScoredRow = {
        date,
        netIncome,
        filed,
        hasQuarterDuration,
      };

      if (!current) {
        byDate.set(date, candidate);
        continue;
      }

      if (candidate.hasQuarterDuration !== current.hasQuarterDuration) {
        if (candidate.hasQuarterDuration) {
          byDate.set(date, candidate);
        }
        continue;
      }

      if (candidate.filed > current.filed) {
        byDate.set(date, candidate);
        continue;
      }

      if (candidate.filed === current.filed && Math.abs(candidate.netIncome) < Math.abs(current.netIncome)) {
        byDate.set(date, candidate);
      }
    }

    return [...byDate.values()]
      .sort((a, b) => a.date.localeCompare(b.date))
      .map((row) => ({
        date: row.date,
        netIncome: row.netIncome,
        source: "actual",
      }));
  };

  let bestRows: QuarterlyNetIncomePoint[] = [];
  for (const candidate of taxonomyCandidates) {
    const factsRoot = (parsed.facts as Record<string, unknown> | undefined)?.[candidate.taxonomy] as
      | Record<string, unknown>
      | undefined;
    if (!factsRoot || typeof factsRoot !== "object") continue;

    for (const metricKey of candidate.metrics) {
      const unitRows = pickUnitRows(factsRoot, metricKey);
      if (!unitRows.length) continue;
      const parsedRows = extractQuarterRows(unitRows);
      if (parsedRows.length > bestRows.length) {
        bestRows = parsedRows;
      }
    }
  }

  return bestRows;
}

async function fetchSecTickerToCikMap(): Promise<Map<string, string>> {
  if (secTickerToCikMapPromise) return secTickerToCikMapPromise;

  secTickerToCikMapPromise = (async () => {
    const raw = await fetchText("https://www.sec.gov/files/company_tickers.json", 1, 18000, false, {
      headers: [`User-Agent: ${SEC_USER_AGENT}`],
    });
    const parsed = JSON.parse(raw) as Record<string, { cik_str?: number; ticker?: string }>;

    const byTicker = new Map<string, string>();
    for (const item of Object.values(parsed || {})) {
      const ticker = String(item?.ticker || "").trim().toUpperCase();
      const cikRaw = Number(item?.cik_str);
      if (!ticker || !Number.isFinite(cikRaw) || cikRaw <= 0) continue;
      const cik = String(Math.trunc(cikRaw)).padStart(10, "0");
      byTicker.set(ticker, cik);
      byTicker.set(normalizeTickerSymbol(ticker), cik);
    }
    return byTicker;
  })().catch((error) => {
    secTickerToCikMapPromise = null;
    throw error;
  });

  return secTickerToCikMapPromise;
}

async function fetchSecQuarterlyEpsSeries(symbol: string): Promise<SecQuarterlyEpsSeriesResult> {
  try {
    const tickerMap = await fetchSecTickerToCikMap();
    const normalizedSymbol = String(symbol || "").trim().toUpperCase();
    const cik =
      tickerMap.get(normalizedSymbol) || tickerMap.get(normalizeTickerSymbol(normalizedSymbol)) || "";
    if (!cik) {
      return {
        rows: [],
        availabilityByQuarter: new Map(),
      };
    }

    const url = `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json`;
    const raw = await fetchText(url, 1, 18000, false, {
      headers: [`User-Agent: ${SEC_USER_AGENT}`],
    });
    return {
      rows: parseSecQuarterlyEpsFromCompanyFacts(raw),
      availabilityByQuarter: parseSecQuarterlyEpsAvailabilityByQuarterFromCompanyFacts(raw),
    };
  } catch {
    return {
      rows: [],
      availabilityByQuarter: new Map(),
    };
  }
}

async function fetchSecQuarterlyNetIncomeSeries(symbol: string): Promise<QuarterlyNetIncomePoint[]> {
  try {
    const tickerMap = await fetchSecTickerToCikMap();
    const normalizedSymbol = String(symbol || "").trim().toUpperCase();
    const cik =
      tickerMap.get(normalizedSymbol) || tickerMap.get(normalizeTickerSymbol(normalizedSymbol)) || "";
    if (!cik) return [];

    const url = `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json`;
    const raw = await fetchText(url, 1, 18000, false, {
      headers: [`User-Agent: ${SEC_USER_AGENT}`],
    });
    return parseSecQuarterlyNetIncomeFromCompanyFacts(raw);
  } catch {
    return [];
  }
}

function mergeActualQuarterlyEpsSources(
  secRows: QuarterlyEpsPoint[],
  stockAnalysisRows: QuarterlyEpsPoint[],
  secAvailabilityByQuarter: Map<string, string> = new Map()
): QuarterlyEpsPoint[] {
  type QuarterRow = {
    quarterKey: string;
    date: string;
    eps: number;
    availableDate: string;
  };

  const secByQuarter = new Map<string, QuarterRow>();
  const stockByQuarter = new Map<string, QuarterRow>();
  const maxIsoDate = (...candidates: string[]): string =>
    candidates.filter(Boolean).sort((a, b) => a.localeCompare(b)).pop() || "";

  const upsertByQuarter = (target: Map<string, QuarterRow>, row: QuarterRow) => {
    const current = target.get(row.quarterKey);
    if (!current) {
      target.set(row.quarterKey, row);
      return;
    }

    // Prefer earlier quarter-end timestamps to keep cadence stable.
    if (row.date < current.date) {
      target.set(row.quarterKey, row);
    }
  };

  for (const row of secRows || []) {
    if (!row || row.source !== "actual") continue;
    const date = toIsoDateFromText(row.date);
    const availableDate = toIsoDateFromText(row.availableDate) || date;
    const eps = sanitizeEps(row.eps);
    const quarterKey = quarterKeyFromDate(date || "");
    if (!date || !availableDate || eps === null || !quarterKey) continue;
    upsertByQuarter(secByQuarter, { quarterKey, date, eps, availableDate });
  }

  for (const row of stockAnalysisRows || []) {
    if (!row || row.source !== "actual") continue;
    const date = toIsoDateFromText(row.date);
    const availableDate = toIsoDateFromText(row.availableDate) || date;
    const eps = sanitizeEps(row.eps);
    const quarterKey = quarterKeyFromDate(date || "");
    if (!date || !availableDate || eps === null || !quarterKey) continue;
    upsertByQuarter(stockByQuarter, { quarterKey, date, eps, availableDate });
  }

  const overlapRatios: number[] = [];
  const overlapRatioRows: Array<{ date: string; ratio: number }> = [];
  for (const [quarterKey, secRow] of secByQuarter.entries()) {
    const stockRow = stockByQuarter.get(quarterKey);
    if (!stockRow) continue;
    const secEps = secRow.eps;
    const stockEps = stockRow.eps;
    const absSec = Math.abs(secEps);
    const absStock = Math.abs(stockEps);
    if (absSec <= 1e-8 || absStock <= 1e-8) continue;
    const ratio = absSec / absStock;
    overlapRatios.push(ratio);
    overlapRatioRows.push({
      date: stockRow.date > secRow.date ? stockRow.date : secRow.date,
      ratio,
    });
  }

  const normalizeDivisor = (rawValue: number | null): number => {
    if (!Number.isFinite(rawValue as number) || !rawValue || rawValue <= 0) return 1;
    const anchors = [1, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20];
    let best = rawValue;
    let bestErr = Number.POSITIVE_INFINITY;
    for (const candidate of anchors) {
      const err = Math.abs(rawValue - candidate) / candidate;
      if (err < bestErr) {
        bestErr = err;
        best = candidate;
      }
    }
    return bestErr <= 0.22 ? best : rawValue;
  };

  const overlapMedian = median(overlapRatios);
  const secScaleDivisor =
    overlapMedian && overlapMedian > 0 && (overlapMedian >= 1.8 || overlapMedian <= 0.55)
      ? normalizeDivisor(overlapMedian)
      : 1;

  const earliestStockDate = [...stockByQuarter.values()]
    .map((row) => row.date)
    .sort((a, b) => a.localeCompare(b))[0] || "";

  const transitionRatios = [...overlapRatioRows]
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(0, 8)
    .map((row) => row.ratio);
  const transitionMedian = median(transitionRatios);
  const secLegacyScaleDivisor =
    transitionMedian && transitionMedian > 0 && (transitionMedian >= 1.8 || transitionMedian <= 0.55)
      ? normalizeDivisor(transitionMedian)
      : 1;

  const quarterKeys = [...new Set([...secByQuarter.keys(), ...stockByQuarter.keys()])].sort((a, b) =>
    a.localeCompare(b)
  );
  const merged: QuarterlyEpsPoint[] = [];

  for (const quarterKey of quarterKeys) {
    const stock = stockByQuarter.get(quarterKey);
    if (stock) {
      const eps = sanitizeEps(stock.eps);
      if (eps !== null) {
        const sec = secByQuarter.get(quarterKey);
        const secAvailableDate = toIsoDateFromText(sec?.availableDate) || "";
        const secAvailabilityDate = toIsoDateFromText(secAvailabilityByQuarter.get(quarterKey)) || "";
        const availableDate = maxIsoDate(stock.availableDate, secAvailableDate, secAvailabilityDate, stock.date);
        merged.push({
          date: stock.date,
          eps,
          source: "actual",
          availableDate,
        });
      }
      continue;
    }

    const sec = secByQuarter.get(quarterKey);
    if (!sec) continue;
    let divisor = secScaleDivisor > 0 ? secScaleDivisor : 1;
    if (earliestStockDate && sec.date < earliestStockDate && secLegacyScaleDivisor > 0) {
      divisor = secLegacyScaleDivisor;
    }
    const adjustedSec = divisor > 0 ? sec.eps / divisor : sec.eps;
    const eps = sanitizeEps(adjustedSec);
    if (eps === null) continue;
    const secAvailabilityDate = toIsoDateFromText(secAvailabilityByQuarter.get(quarterKey)) || "";
    const availableDate = maxIsoDate(sec.availableDate, secAvailabilityDate, sec.date);
    merged.push({
      date: sec.date,
      eps,
      source: "actual",
      availableDate,
    });
  }

  return merged;
}

function mergeActualQuarterlyNetIncomeSources(
  secRows: QuarterlyNetIncomePoint[],
  stockAnalysisRows: QuarterlyNetIncomePoint[]
): QuarterlyNetIncomePoint[] {
  type QuarterRow = {
    quarterKey: string;
    date: string;
    netIncome: number;
  };

  const secByQuarter = new Map<string, QuarterRow>();
  const stockByQuarter = new Map<string, QuarterRow>();

  const upsertByQuarter = (target: Map<string, QuarterRow>, row: QuarterRow) => {
    const current = target.get(row.quarterKey);
    if (!current) {
      target.set(row.quarterKey, row);
      return;
    }
    if (row.date < current.date) {
      target.set(row.quarterKey, row);
    }
  };

  for (const row of secRows || []) {
    if (!row || row.source !== "actual") continue;
    const date = toIsoDateFromText(row.date);
    const netIncome = sanitizeNetIncome(row.netIncome);
    const quarterKey = quarterKeyFromDate(date || "");
    if (!date || netIncome === null || !quarterKey) continue;
    upsertByQuarter(secByQuarter, { quarterKey, date, netIncome });
  }

  for (const row of stockAnalysisRows || []) {
    if (!row || row.source !== "actual") continue;
    const date = toIsoDateFromText(row.date);
    const netIncome = sanitizeNetIncome(row.netIncome);
    const quarterKey = quarterKeyFromDate(date || "");
    if (!date || netIncome === null || !quarterKey) continue;
    upsertByQuarter(stockByQuarter, { quarterKey, date, netIncome });
  }

  const overlapRatios: number[] = [];
  for (const [quarterKey, secRow] of secByQuarter.entries()) {
    const stockRow = stockByQuarter.get(quarterKey);
    if (!stockRow) continue;
    const absSec = Math.abs(secRow.netIncome);
    const absStock = Math.abs(stockRow.netIncome);
    if (absSec <= 1e-8 || absStock <= 1e-8) continue;
    overlapRatios.push(absSec / absStock);
  }

  const normalizeDivisor = (rawValue: number | null): number => {
    if (!Number.isFinite(rawValue as number) || !rawValue || rawValue <= 0) return 1;
    const anchors = [
      0.001,
      0.01,
      0.1,
      0.25,
      0.5,
      1,
      2,
      4,
      10,
      100,
      1000,
      10000,
      1000000,
    ];
    let best = rawValue;
    let bestErr = Number.POSITIVE_INFINITY;
    for (const candidate of anchors) {
      const err = Math.abs(rawValue - candidate) / candidate;
      if (err < bestErr) {
        bestErr = err;
        best = candidate;
      }
    }
    return bestErr <= 0.22 ? best : rawValue;
  };

  const overlapMedian = median(overlapRatios);
  const secScaleDivisor =
    overlapMedian && overlapMedian > 0 && (overlapMedian >= 1.8 || overlapMedian <= 0.55)
      ? normalizeDivisor(overlapMedian)
      : 1;

  const quarterKeys = [...new Set([...secByQuarter.keys(), ...stockByQuarter.keys()])].sort((a, b) =>
    a.localeCompare(b)
  );
  const merged: QuarterlyNetIncomePoint[] = [];

  for (const quarterKey of quarterKeys) {
    const stock = stockByQuarter.get(quarterKey);
    if (stock) {
      const netIncome = sanitizeNetIncome(stock.netIncome);
      if (netIncome !== null) {
        merged.push({
          date: stock.date,
          netIncome,
          source: "actual",
        });
      }
      continue;
    }

    const sec = secByQuarter.get(quarterKey);
    if (!sec) continue;
    const adjustedSec = secScaleDivisor > 0 ? sec.netIncome / secScaleDivisor : sec.netIncome;
    const netIncome = sanitizeNetIncome(adjustedSec);
    if (netIncome === null) continue;
    merged.push({
      date: sec.date,
      netIncome,
      source: "actual",
    });
  }

  return merged;
}

function getStockAnalysisSlugCandidates(symbol: string): string[] {
  const seed = symbol.toLowerCase();
  return [...new Set([seed, seed.replace(/-/g, "."), seed.replace(/\./g, "-"), seed.replace(/[.\-]/g, "")])].filter(Boolean);
}

async function fetchQuarterlyFinancialSeries(
  symbol: string,
  lastCloseDate: string
): Promise<QuarterlyFinancialSeriesResult> {
  const [secEpsPayload, secActualNetIncomeRows] = await Promise.all([
    fetchSecQuarterlyEpsSeries(symbol),
    fetchSecQuarterlyNetIncomeSeries(symbol),
  ]);
  const candidates = getStockAnalysisSlugCandidates(symbol);
  let bestResult: QuarterlyFinancialSeriesResult = {
    quarterlyEps: [],
    quarterlyNetIncome: [],
  };
  let bestScore = 0;

  for (const slug of candidates) {
    try {
      const [incomePayload, forecastPayload] = await Promise.all([
        fetchStockAnalysisIncomeStatementQuarterlyPayload(slug),
        fetchStockAnalysisForecastQuarterlyPayload(slug),
      ]);

      const mergedActualEpsRows = mergeActualQuarterlyEpsSources(
        secEpsPayload.rows,
        incomePayload.epsRows,
        secEpsPayload.availabilityByQuarter
      );
      const mergedEpsRows = mergeQuarterlyEpsSeries(
        mergedActualEpsRows,
        forecastPayload.epsRows,
        lastCloseDate
      );

      const estimatedExpectedNetIncomeRows = estimateExpectedQuarterlyNetIncomeFromEps(
        forecastPayload.epsRows,
        incomePayload.shareRows
      );
      const mergedExpectedNetIncomeRows = mergeExpectedQuarterlyNetIncomeCandidates(
        forecastPayload.netIncomeRows,
        estimatedExpectedNetIncomeRows
      );
      const mergedActualNetIncomeRows = mergeActualQuarterlyNetIncomeSources(
        secActualNetIncomeRows,
        incomePayload.netIncomeRows
      );
      const mergedNetIncomeRows = mergeQuarterlyNetIncomeSeries(
        mergedActualNetIncomeRows,
        mergedExpectedNetIncomeRows,
        lastCloseDate
      );

      if (mergedEpsRows.length >= 4 && mergedNetIncomeRows.length >= 4) {
        return {
          quarterlyEps: mergedEpsRows,
          quarterlyNetIncome: mergedNetIncomeRows,
        };
      }

      const score = mergedEpsRows.length + mergedNetIncomeRows.length;
      if (score > bestScore) {
        bestScore = score;
        bestResult = {
          quarterlyEps: mergedEpsRows,
          quarterlyNetIncome: mergedNetIncomeRows,
        };
      }
    } catch {
      // try next candidate
    }
  }

  const secOnlyEps = mergeQuarterlyEpsSeries(secEpsPayload.rows, [], lastCloseDate);
  const secOnlyNetIncome = mergeQuarterlyNetIncomeSeries(secActualNetIncomeRows, [], lastCloseDate);
  const secScore = secOnlyEps.length + secOnlyNetIncome.length;
  if (secScore > bestScore) {
    bestResult = {
      quarterlyEps: secOnlyEps,
      quarterlyNetIncome: secOnlyNetIncome,
    };
  }

  return bestResult;
}

async function fetchQuarterlyRatioPayloadFromLegacyHtml(slug: string): Promise<RatioPayload | null> {
  try {
    const quarterlyUrl = `https://stockanalysis.com/stocks/${slug}/financials/ratios/?p=quarterly`;
    const quarterlyHtml = await fetchText(quarterlyUrl, 1, 14000);
    const quarterly = parseRatioPayloadFromScript(quarterlyHtml);

    const annualUrl = `https://stockanalysis.com/stocks/${slug}/financials/ratios/`;
    const annualHtml = await fetchText(annualUrl, 1, 12000);
    const annual = parseRatioPayloadFromScript(annualHtml);

    const merged = mergeRatioPayloadList([quarterly, annual].filter(Boolean) as RatioPayload[]);
    if (!merged) return null;

    return {
      ...merged,
      source: `${merged.source}:${slug}:legacy-html`,
    };
  } catch {
    return null;
  }
}

async function fetchQuarterlyRatioPayload(symbol: string): Promise<RatioPayload | null> {
  const candidates = getStockAnalysisSlugCandidates(symbol);

  for (const slug of candidates) {
    try {
      const payloads: RatioPayload[] = [];
      const seenSourceTags = new Set<string>();
      const discoveredSources = new Set<string>();
      let primarySource = "";

      const collect = async (period: "annual" | "quarterly", source = ""): Promise<void> => {
        const parsed = await fetchStockAnalysisDataRatioPayload(slug, period, source);
        if (!parsed) return;

        if (parsed.selectedSource) {
          discoveredSources.add(parsed.selectedSource);
          primarySource = primarySource || parsed.selectedSource;
        }
        for (const sourceName of parsed.availableSources) {
          discoveredSources.add(sourceName);
        }

        if (!parsed.payload) return;

        const sourceTag = `${parsed.payload.source}:${slug}:${period}`;
        if (seenSourceTags.has(sourceTag)) return;
        seenSourceTags.add(sourceTag);
        payloads.push({
          ...parsed.payload,
          source: sourceTag,
        });
      };

      await collect("quarterly");
      await collect("annual");

      const mergedPrimary = mergeRatioPayloadList(payloads);
      const shouldBackfillForwardOrPeg =
        !sanitizeSignedRatio(mergedPrimary?.latest.pe_forward) ||
        countForwardAnchors(mergedPrimary) < MIN_FORWARD_ANCHORS_FOR_HISTORY ||
        !sanitizeSignedRatio(mergedPrimary?.latest.peg) ||
        countPegAnchors(mergedPrimary) < MIN_FORWARD_ANCHORS_FOR_HISTORY;

      if (shouldBackfillForwardOrPeg) {
        const alternatives = [...discoveredSources]
          .map((item) => item.trim().toLowerCase())
          .filter(Boolean)
          .filter((item) => item !== primarySource)
          .sort((left, right) => {
            const leftPriority = STOCK_ANALYSIS_SOURCE_PRIORITY.indexOf(left);
            const rightPriority = STOCK_ANALYSIS_SOURCE_PRIORITY.indexOf(right);
            const leftRank = leftPriority >= 0 ? leftPriority : 99;
            const rightRank = rightPriority >= 0 ? rightPriority : 99;
            if (leftRank !== rightRank) return leftRank - rightRank;
            return left.localeCompare(right);
          });

        for (const sourceName of alternatives) {
          await collect("quarterly", sourceName);
        }
      }

      const merged = mergeRatioPayloadList(payloads);
      if (merged) {
        return merged;
      }
    } catch {
      // fallback to legacy parser below
    }

    const legacy = await fetchQuarterlyRatioPayloadFromLegacyHtml(slug);
    if (legacy) {
      return legacy;
    }
  }

  return null;
}

function mergeRatioPayloads(
  stockPayload: RatioPayload | null,
  longPeSeries: MetricPoint[],
  longPbSeries: MetricPoint[],
  longForwardSeries: MetricPoint[],
  sourceHints: { pe?: string; pb?: string; forward?: string } = {},
  symbol = ""
): RatioPayload | null {
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();

  const normalizeByAdrRatio = (metricKey: "pe_ttm" | "pe_forward" | "pb", rawValue: unknown): number | null => {
    const base = sanitizeSignedRatio(rawValue);
    if (!base) return null;

    if (metricKey !== "pe_ttm") return base;

    const divisor = ADR_LATEST_PE_DIVISOR_BY_SYMBOL[normalizedSymbol]?.pe_ttm;
    if (!Number.isFinite(divisor) || divisor <= 1) return base;

    const adjusted = sanitizeSignedRatio(base / divisor);
    return adjusted ?? base;
  };

  const alignedLongForwardSeries = longForwardSeries;

  const resolveLatestMetricValue = (
    metricKey: "pe_ttm" | "pe_forward" | "pb",
    longSeries: MetricPoint[],
    stockLatestRaw: unknown
  ): number | null => {
    const stockLatest = normalizeByAdrRatio(metricKey, stockLatestRaw);
    const longLatestPoint = longSeries[longSeries.length - 1];
    const longLatest = sanitizeSignedRatio(longLatestPoint?.value ?? null);
    const latestDiffRatio =
      stockLatest && longLatest
        ? Math.max(stockLatest / longLatest, longLatest / stockLatest)
        : 1;

    if (longLatest && longLatestPoint?.date) {
      const ageDays = (toTs(new Date().toISOString().slice(0, 10)) - toTs(longLatestPoint.date)) / 86_400_000;
      if (Number.isFinite(ageDays) && ageDays >= 0 && ageDays <= LONG_SERIES_FRESH_DAYS) {
        if (stockLatest && Number.isFinite(latestDiffRatio) && latestDiffRatio >= STOCK_LATEST_OUTLIER_FACTOR) {
          return stockLatest;
        }
        return longLatest;
      }
    }

    if (stockLatest && longLatest) {
      const mismatch =
        !Number.isFinite(latestDiffRatio) ||
        latestDiffRatio >= STOCK_LATEST_OUTLIER_FACTOR;

      if (mismatch) {
        return stockLatest;
      }
    }

    return stockLatest ?? longLatest;
  };

  const mergedByDate = new Map<string, RatioAnchor>();
  const coarsePeSeries = isLikelyCoarseSeries(longPeSeries);
  const coarsePbSeries = isLikelyCoarseSeries(longPbSeries);
  const coarseForwardSeries = isLikelyCoarseSeries(alignedLongForwardSeries);
  const recentStockAnchorPeValues = (stockPayload?.anchors || [])
    .map((item) => sanitizeSignedRatio(item.pe_ttm))
    .filter((value): value is number => !!value)
    .slice(-8);
  const recentStockAnchorPeMedian = median(recentStockAnchorPeValues);
  const stockLatestPeRaw = sanitizeSignedRatio(stockPayload?.latest.pe_ttm ?? null);
  const stockRawMismatchFactor =
    stockLatestPeRaw && recentStockAnchorPeMedian
      ? Math.max(stockLatestPeRaw / recentStockAnchorPeMedian, recentStockAnchorPeMedian / stockLatestPeRaw)
      : 1;
  const dropStockPeAnchorsDueBasisMismatch =
    longPeSeries.length >= 16 && stockRawMismatchFactor >= STOCK_TTM_BASIS_MISMATCH_FACTOR;

  const upsert = (date: string, patch: Partial<RatioAnchor>, preferPatch = true): void => {
    const current = mergedByDate.get(date) || {
      date,
      pe_ttm: null,
      pe_forward: null,
      pb: null,
      peg: null,
    };
    mergedByDate.set(date, {
      date,
      pe_ttm: preferPatch ? patch.pe_ttm ?? current.pe_ttm : current.pe_ttm ?? patch.pe_ttm,
      pe_forward: preferPatch ? patch.pe_forward ?? current.pe_forward : current.pe_forward ?? patch.pe_forward,
      pb: preferPatch ? patch.pb ?? current.pb : current.pb ?? patch.pb,
      peg: preferPatch ? patch.peg ?? current.peg : current.peg ?? patch.peg,
    });
  };

  for (const item of stockPayload?.anchors || []) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(item.date)) continue;
    upsert(item.date, {
      pe_ttm: dropStockPeAnchorsDueBasisMismatch ? null : sanitizeSignedRatio(item.pe_ttm),
      pe_forward: sanitizeSignedRatio(item.pe_forward),
      pb: sanitizeSignedRatio(item.pb),
      peg: sanitizeSignedRatio(item.peg),
    });
  }

  for (const point of longPeSeries) {
    upsert(point.date, { pe_ttm: point.value }, !coarsePeSeries);
  }

  for (const point of longPbSeries) {
    upsert(point.date, { pb: point.value }, !coarsePbSeries);
  }

  for (const point of alignedLongForwardSeries) {
    upsert(point.date, { pe_forward: point.value }, !coarseForwardSeries);
  }

  const latest = {
    pe_ttm: resolveLatestMetricValue("pe_ttm", longPeSeries, stockPayload?.latest.pe_ttm ?? null),
    pe_forward: resolveLatestMetricValue(
      "pe_forward",
      alignedLongForwardSeries,
      stockPayload?.latest.pe_forward ?? null
    ),
    pb: resolveLatestMetricValue("pb", longPbSeries, stockPayload?.latest.pb ?? null),
    peg: sanitizeSignedRatio(stockPayload?.latest.peg ?? null),
  };

  let anchors = [...mergedByDate.values()]
    .filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg)
    .sort((a, b) => a.date.localeCompare(b.date));

  anchors = dropIsolatedMetricSpikeAnchors(anchors, "pe_ttm", {
    spikeFactor: 1.9,
    shortPulseSpikeFactor: 1.3,
    neighborSimilarityFactor: 1.28,
    maxNeighborGapDays: 16,
    shortPulseMaxGapDays: 8,
    minAbsValue: 4,
  }).filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg);

  anchors = dropIsolatedMetricSpikeAnchors(anchors, "pe_forward", {
    spikeFactor: 1.8,
    neighborSimilarityFactor: 1.32,
    maxNeighborGapDays: 32,
    minAbsValue: 4,
  }).filter((item) => item.pe_ttm || item.pe_forward || item.pb || item.peg);

  if (!anchors.length && !latest.pe_ttm && !latest.pe_forward && !latest.pb && !latest.peg) {
    return null;
  }

  const sourceTags = [
    stockPayload?.source || "",
    longPeSeries.length ? sourceHints.pe || "companiesmarketcap-pe-ratio" : "",
    longPbSeries.length ? sourceHints.pb || "companiesmarketcap-pb-ratio" : "",
    alignedLongForwardSeries.length ? sourceHints.forward || "forward-series" : "",
  ].filter(Boolean);

  return {
    anchors,
    latest,
    source: sourceTags.join("+"),
  };
}

function dropIsolatedMetricSpikeAnchors(
  anchors: RatioAnchor[],
  key: "pe_ttm" | "pe_forward" | "pb",
  options: {
    spikeFactor?: number;
    shortPulseSpikeFactor?: number;
    neighborSimilarityFactor?: number;
    maxNeighborGapDays?: number;
    shortPulseMaxGapDays?: number;
    minAbsValue?: number;
  } = {}
): RatioAnchor[] {
  if (!Array.isArray(anchors) || anchors.length < 3) return anchors;

  const spikeFactor = Number.isFinite(options.spikeFactor as number) ? (options.spikeFactor as number) : 1.9;
  const shortPulseSpikeFactor = Number.isFinite(options.shortPulseSpikeFactor as number)
    ? (options.shortPulseSpikeFactor as number)
    : 1.34;
  const neighborSimilarityFactor = Number.isFinite(options.neighborSimilarityFactor as number)
    ? (options.neighborSimilarityFactor as number)
    : 1.35;
  const maxNeighborGapDays = Number.isFinite(options.maxNeighborGapDays as number)
    ? (options.maxNeighborGapDays as number)
    : 40;
  const shortPulseMaxGapDays = Number.isFinite(options.shortPulseMaxGapDays as number)
    ? (options.shortPulseMaxGapDays as number)
    : 5;
  const minAbsValue = Number.isFinite(options.minAbsValue as number) ? (options.minAbsValue as number) : 4;

  const ratioDistance = (a: number, b: number): number => {
    const absA = Math.abs(a);
    const absB = Math.abs(b);
    if (absA < 1e-8 || absB < 1e-8) return Number.POSITIVE_INFINITY;
    return Math.max(absA / absB, absB / absA);
  };

  const cleaned = anchors.map((item) => ({ ...item }));
  let changed = true;

  // Iterate a few passes so adjacent pulse artifacts can also be removed.
  for (let pass = 0; pass < 4 && changed; pass += 1) {
    changed = false;

    for (let i = 0; i < cleaned.length; i += 1) {
      const currValue = sanitizeSignedRatio(cleaned[i][key]);
      if (currValue === null) continue;

      let prevIndex = i - 1;
      while (prevIndex >= 0 && sanitizeSignedRatio(cleaned[prevIndex][key]) === null) {
        prevIndex -= 1;
      }

      let nextIndex = i + 1;
      while (nextIndex < cleaned.length && sanitizeSignedRatio(cleaned[nextIndex][key]) === null) {
        nextIndex += 1;
      }

      if (prevIndex < 0 || nextIndex >= cleaned.length) continue;

      const prevValue = sanitizeSignedRatio(cleaned[prevIndex][key]);
      const nextValue = sanitizeSignedRatio(cleaned[nextIndex][key]);
      if (prevValue === null || currValue === null || nextValue === null) continue;

      const prevTs = toTs(cleaned[prevIndex].date);
      const currTs = toTs(cleaned[i].date);
      const nextTs = toTs(cleaned[nextIndex].date);
      if (!prevTs || !currTs || !nextTs) continue;

      const prevGapDays = (currTs - prevTs) / 86_400_000;
      const nextGapDays = (nextTs - currTs) / 86_400_000;
      if (
        !Number.isFinite(prevGapDays) ||
        !Number.isFinite(nextGapDays) ||
        prevGapDays <= 0 ||
        nextGapDays <= 0 ||
        prevGapDays > maxNeighborGapDays ||
        nextGapDays > maxNeighborGapDays
      ) {
        continue;
      }

      if (
        Math.abs(prevValue) < minAbsValue ||
        Math.abs(currValue) < minAbsValue ||
        Math.abs(nextValue) < minAbsValue
      ) {
        continue;
      }

      if (prevValue * nextValue <= 0) continue;

      const neighborsSimilarity = ratioDistance(prevValue, nextValue);
      if (neighborsSimilarity > neighborSimilarityFactor) continue;

      const currVsPrev = ratioDistance(currValue, prevValue);
      const currVsNext = ratioDistance(currValue, nextValue);
      const isHardSpike = currVsPrev >= spikeFactor && currVsNext >= spikeFactor;
      const isLocalExtreme =
        (currValue > prevValue && currValue > nextValue) ||
        (currValue < prevValue && currValue < nextValue);
      const isShortPulseSpike =
        isLocalExtreme &&
        currVsPrev >= shortPulseSpikeFactor &&
        currVsNext >= shortPulseSpikeFactor &&
        prevGapDays <= shortPulseMaxGapDays &&
        nextGapDays <= shortPulseMaxGapDays;

      if (isHardSpike || isShortPulseSpike) {
        cleaned[i][key] = null;
        changed = true;
      }
    }
  }

  return cleaned;
}

function dropForwardPeShortPulseFromPoints(points: SnapshotPoint[]): SnapshotPoint[] {
  if (!Array.isArray(points) || points.length < 3) return points;

  const forwardAnchors: RatioAnchor[] = points.map((point) => ({
    date: point.date,
    pe_ttm: null,
    pe_forward: Number.isFinite(point.pe_forward as number) ? (point.pe_forward as number) : null,
    pb: null,
    peg: null,
  }));

  const cleanedAnchors = dropIsolatedMetricSpikeAnchors(forwardAnchors, "pe_forward", {
    spikeFactor: 1.8,
    shortPulseSpikeFactor: 1.34,
    neighborSimilarityFactor: 1.32,
    maxNeighborGapDays: 7,
    shortPulseMaxGapDays: 5,
    minAbsValue: 4,
  });

  const cleanedByDate = new Map<string, number | null>();
  for (const item of cleanedAnchors) {
    cleanedByDate.set(item.date, sanitizeSignedRatio(item.pe_forward));
  }

  let changed = false;
  const nextPoints = points.map((point) => {
    const nextForward = cleanedByDate.get(point.date) ?? null;
    if (nextForward === point.pe_forward) {
      return point;
    }

    changed = true;
    return {
      ...point,
      pe_forward: nextForward,
    };
  });

  return changed ? nextPoints : points;
}

function getCloseAtOrBefore(closePoints: ClosePoint[], targetDate: string): ClosePoint {
  if (!closePoints.length) {
    return { date: targetDate, close: 1, ts: toTs(targetDate) };
  }

  let left = 0;
  let right = closePoints.length - 1;
  let answer = -1;

  while (left <= right) {
    const mid = (left + right) >> 1;
    if (closePoints[mid].date <= targetDate) {
      answer = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  if (answer >= 0) return closePoints[answer];
  return closePoints[0];
}

function getCloseIndexAtOrBeforeTs(closePoints: ClosePoint[], targetTs: number): number {
  if (!closePoints.length) return -1;

  let left = 0;
  let right = closePoints.length - 1;
  let answer = -1;

  while (left <= right) {
    const mid = (left + right) >> 1;
    if (closePoints[mid].ts <= targetTs) {
      answer = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return answer;
}

function buildUnifiedAnchors(closePoints: ClosePoint[], ratioPayload: RatioPayload | null): RatioAnchor[] {
  const firstDate = closePoints[0]?.date || HISTORY_START_DATE;
  const lastDate = closePoints[closePoints.length - 1]?.date || HISTORY_START_DATE;

  const byDate = new Map<string, RatioAnchor>();

  for (const item of ratioPayload?.anchors || []) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(item.date)) continue;
    if (item.date < firstDate || item.date > lastDate) continue;
    byDate.set(item.date, {
      date: item.date,
      pe_ttm: sanitizeSignedRatio(item.pe_ttm),
      pe_forward: sanitizeSignedRatio(item.pe_forward),
      pb: sanitizeSignedRatio(item.pb),
      peg: sanitizeSignedRatio(item.peg),
    });
  }

  const latest = ratioPayload?.latest || {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
    peg: null,
  };

  if (latest.pe_ttm || latest.pe_forward || latest.pb || latest.peg) {
    const current = byDate.get(lastDate) || {
      date: lastDate,
      pe_ttm: null,
      pe_forward: null,
      pb: null,
      peg: null,
    };

    byDate.set(lastDate, {
      date: lastDate,
      pe_ttm: current.pe_ttm ?? sanitizeSignedRatio(latest.pe_ttm),
      pe_forward: current.pe_forward ?? sanitizeSignedRatio(latest.pe_forward),
      pb: current.pb ?? sanitizeSignedRatio(latest.pb),
      peg: current.peg ?? sanitizeSignedRatio(latest.peg),
    });
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function buildMetricAnchorDenominators(
  closePoints: ClosePoint[],
  anchors: RatioAnchor[],
  key: "pe_ttm" | "pe_forward" | "pb",
  fallbackMetric: number,
  options: { allowFallback?: boolean } = {}
): { anchors: MetricAnchorDenominator[]; usedFallback: boolean } {
  const firstPoint = closePoints[0] || {
    date: HISTORY_START_DATE,
    close: 1,
    ts: toTs(HISTORY_START_DATE),
  };

  const byDate = new Map<string, MetricAnchorDenominator>();

  for (const item of anchors) {
    const ratio = sanitizeSignedRatio(item[key]);
    if (!ratio) continue;

    const closeAtAnchor = getCloseAtOrBefore(closePoints, item.date).close;
    const denominator = closeAtAnchor / ratio;
    if (!Number.isFinite(denominator) || Math.abs(denominator) < 1e-8) continue;

    byDate.set(item.date, {
      date: item.date,
      ts: toTs(item.date),
      ratio,
      denominator,
    });
  }

  const ordered = [...byDate.values()].sort((a, b) => a.ts - b.ts);
  if (ordered.length) {
    return { anchors: ordered, usedFallback: false };
  }

  if (options.allowFallback === false) {
    return { anchors: [], usedFallback: false };
  }

  return {
    anchors: [
      {
        date: firstPoint.date,
        ts: firstPoint.ts,
        ratio: fallbackMetric,
        denominator: firstPoint.close / fallbackMetric,
      },
    ],
    usedFallback: true,
  };
}

function projectMetricByAnchorWindow(
  closePoints: ClosePoint[],
  anchors: MetricAnchorDenominator[],
  min: number,
  max: number
): Array<number | null> {
  if (!closePoints.length || !anchors.length) {
    return [];
  }

  const ordered = [...anchors].sort((a, b) => a.ts - b.ts);
  const out: Array<number | null> = Array.from({ length: closePoints.length }, () => null);

  const clampRatio = (value: number): number => roundTo(clamp(value, min, max), 4);

  const projectInterval = (leftAnchor: MetricAnchorDenominator, rightAnchor: MetricAnchorDenominator): void => {
    const leftIndex = getCloseIndexAtOrBeforeTs(closePoints, leftAnchor.ts);
    const rightIndex = getCloseIndexAtOrBeforeTs(closePoints, rightAnchor.ts);
    if (leftIndex < 0 || rightIndex < 0) return;

    const leftRatio = Number(leftAnchor.ratio);
    const rightRatio = Number(rightAnchor.ratio);
    if (!Number.isFinite(leftRatio) || !Number.isFinite(rightRatio)) return;

    if (rightIndex <= leftIndex) {
      out[rightIndex] = clampRatio(rightRatio);
      return;
    }

    const steps = rightIndex - leftIndex;
    const sameSign =
      leftRatio * rightRatio > 0 &&
      Math.abs(leftRatio) > 1e-9 &&
      Math.abs(rightRatio) > 1e-9;

    if (sameSign) {
      const startAbs = Math.abs(leftRatio);
      const endAbs = Math.abs(rightRatio);
      const baseLog = Math.log(startAbs);
      const targetLog = Math.log(endAbs / startAbs);

      let closeLogSum = 0;
      for (let i = leftIndex + 1; i <= rightIndex; i += 1) {
        const closeRatio = closePoints[i].close / closePoints[i - 1].close;
        const stepLog = Number.isFinite(closeRatio) && closeRatio > 0 ? Math.log(closeRatio) : 0;
        closeLogSum += stepLog;
      }

      const drift = (targetLog - closeLogSum) / steps;
      let cumulativeCloseLog = 0;
      const ratioSign = leftRatio > 0 ? 1 : -1;
      out[leftIndex] = clampRatio(leftRatio);

      for (let i = leftIndex + 1; i <= rightIndex; i += 1) {
        const closeRatio = closePoints[i].close / closePoints[i - 1].close;
        const stepLog = Number.isFinite(closeRatio) && closeRatio > 0 ? Math.log(closeRatio) : 0;
        cumulativeCloseLog += stepLog;

        const value = ratioSign * Math.exp(baseLog + cumulativeCloseLog + drift * (i - leftIndex));
        out[i] = clampRatio(value);
      }
      return;
    }

    const rawLeftDenominator = Number(leftAnchor.denominator);
    const leftDenominator =
      Math.abs(rawLeftDenominator) < 1e-6
        ? rawLeftDenominator < 0
          ? -1e-6
          : 1e-6
        : rawLeftDenominator;

    for (let i = leftIndex; i < rightIndex; i += 1) {
      out[i] = clampRatio(closePoints[i].close / leftDenominator);
    }
    out[rightIndex] = clampRatio(rightRatio);
  };

  for (let i = 0; i < ordered.length - 1; i += 1) {
    projectInterval(ordered[i], ordered[i + 1]);
  }

  const tailAnchor = ordered[ordered.length - 1];
  const tailIndex = getCloseIndexAtOrBeforeTs(closePoints, tailAnchor.ts);
  if (tailIndex >= 0) {
    const rawDenominator = tailAnchor.denominator;
    const denominator =
      Math.abs(rawDenominator) < 1e-6 ? (rawDenominator < 0 ? -1e-6 : 1e-6) : rawDenominator;

    for (let i = tailIndex; i < closePoints.length; i += 1) {
      out[i] = clampRatio(closePoints[i].close / denominator);
    }
  }

  return out;
}

function projectDirectMetricByAnchorCarry(
  closePoints: ClosePoint[],
  anchors: RatioAnchor[],
  key: "peg",
  min: number,
  max: number
): Array<number | null> {
  if (!closePoints.length || !anchors.length) {
    return [];
  }

  const ordered = anchors
    .map((item) => ({
      ts: toTs(item.date),
      value: sanitizeSignedRatio(item[key]),
    }))
    .filter((item) => Number.isFinite(item.ts) && Number.isFinite(item.value as number))
    .sort((a, b) => a.ts - b.ts);

  if (!ordered.length) return [];

  const out: Array<number | null> = Array.from({ length: closePoints.length }, () => null);
  let anchorIndex = 0;
  let latestValue: number | null = null;

  for (let i = 0; i < closePoints.length; i += 1) {
    const pointTs = closePoints[i].ts;
    while (anchorIndex < ordered.length && ordered[anchorIndex].ts <= pointTs) {
      latestValue = Number(ordered[anchorIndex].value);
      anchorIndex += 1;
    }

    if (!Number.isFinite(latestValue as number)) continue;
    out[i] = roundTo(clamp(Number(latestValue), min, max), 4);
  }

  return out;
}

function buildValuationSeries(
  closePoints: ClosePoint[],
  ratioPayload: RatioPayload | null
): { points: SnapshotPoint[]; usedFallback: boolean; forwardStartDate: string } {
  if (!closePoints.length) {
    return { points: [], usedFallback: true, forwardStartDate: "" };
  }

  const unifiedAnchors = buildUnifiedAnchors(closePoints, ratioPayload);

  const lastDate = closePoints[closePoints.length - 1]?.date || HISTORY_START_DATE;
  const forwardAnchorDates = unifiedAnchors
    .filter((item) => sanitizeSignedRatio(item.pe_forward))
    .map((item) => item.date)
    .sort((a, b) => a.localeCompare(b));

  let forwardStartDate = "9999-12-31";
  if (forwardAnchorDates.length) {
    // If we found at least one real forward anchor, expose forward series from the earliest anchor date.
    forwardStartDate = forwardAnchorDates[0] || lastDate;
  }

  const peAnchors = buildMetricAnchorDenominators(
    closePoints,
    unifiedAnchors,
    "pe_ttm",
    DEFAULT_METRICS.pe_ttm
  );
  const forwardAnchors = buildMetricAnchorDenominators(
    closePoints,
    unifiedAnchors,
    "pe_forward",
    DEFAULT_METRICS.pe_forward,
    { allowFallback: false }
  );
  const pbAnchors = buildMetricAnchorDenominators(closePoints, unifiedAnchors, "pb", DEFAULT_METRICS.pb);

  const peSeries = projectMetricByAnchorWindow(
    closePoints,
    peAnchors.anchors,
    -METRIC_MAX.pe_ttm,
    METRIC_MAX.pe_ttm
  );
  const forwardSeries = projectMetricByAnchorWindow(
    closePoints,
    forwardAnchors.anchors,
    -METRIC_MAX.pe_forward,
    METRIC_MAX.pe_forward
  );
  const pbSeries = projectMetricByAnchorWindow(
    closePoints,
    pbAnchors.anchors,
    -METRIC_MAX.pb,
    METRIC_MAX.pb
  );
  const pegSeries = projectDirectMetricByAnchorCarry(
    closePoints,
    unifiedAnchors,
    "peg",
    -METRIC_MAX.peg,
    METRIC_MAX.peg
  );
  const impliedTtmEpsSeries: Array<number | null> = closePoints.map((point, index) => {
    const close = Number(point.close);
    const peTtm = Number(peSeries[index]);
    if (!Number.isFinite(close) || close <= 0) return null;
    if (!Number.isFinite(peTtm) || Math.abs(peTtm) <= 1e-8) return null;
    return close / peTtm;
  });

  const points: SnapshotPoint[] = [];

  for (let i = 0; i < closePoints.length; i += 1) {
    const peTtm = peSeries[i];
    const pb = pbSeries[i];
    const peForward = Number.isFinite(forwardSeries[i] as number) ? (forwardSeries[i] as number) : null;
    const pegFromAnchors = Number.isFinite(pegSeries[i] as number) ? (pegSeries[i] as number) : null;

    if (!Number.isFinite(peTtm) || !Number.isFinite(pb)) {
      continue;
    }

    let pegDerived: number | null = null;
    if (Number.isFinite(peTtm as number) && Number.isFinite(peForward as number) && Math.abs(Number(peForward)) > 1e-8) {
      const growthRate = Number(peTtm) / Number(peForward) - 1;
      if (Number.isFinite(growthRate) && Math.abs(growthRate) > 1e-8) {
        const growthPercent = growthRate * 100;
        const pegValue = Number(peTtm) / growthPercent;
        if (Number.isFinite(pegValue)) {
          pegDerived = roundTo(clamp(pegValue, -METRIC_MAX.peg, METRIC_MAX.peg), 4);
        }
      }
    }

    let pegImpliedGrowth: number | null = null;
    const impliedCurrentEps = impliedTtmEpsSeries[i];
    if (Number.isFinite(impliedCurrentEps as number)) {
      const referenceDate = subtractYears(closePoints[i].date, 1);
      const referenceTs = toTs(referenceDate);
      const referenceIndex = referenceTs ? getCloseIndexAtOrBeforeTs(closePoints, referenceTs) : -1;
      if (referenceIndex >= 0 && referenceIndex < i) {
        const impliedPrevEps = impliedTtmEpsSeries[referenceIndex];
        if (Number.isFinite(impliedPrevEps as number) && Math.abs(Number(impliedPrevEps)) > 1e-8) {
          const growthRate = Number(impliedCurrentEps) / Number(impliedPrevEps) - 1;
          if (Number.isFinite(growthRate) && Math.abs(growthRate) > 1e-8) {
            const pegValue = Number(peTtm) / (growthRate * 100);
            if (Number.isFinite(pegValue)) {
              pegImpliedGrowth = roundTo(clamp(pegValue, -METRIC_MAX.peg, METRIC_MAX.peg), 4);
            }
          }
        }
      }
    }

    const peg = Number.isFinite(pegFromAnchors as number)
      ? pegFromAnchors
      : Number.isFinite(pegDerived as number)
        ? pegDerived
        : pegImpliedGrowth;

    points.push({
      date: closePoints[i].date,
      close: closePoints[i].close,
      pe_ttm: peTtm as number,
      pe_forward: peForward,
      pb: pb as number,
      peg,
      us10y_yield: 0,
    });
  }

  return {
    points: dropForwardPeShortPulseFromPoints(points),
    usedFallback: peAnchors.usedFallback || pbAnchors.usedFallback,
    forwardStartDate,
  };
}

function normalizeQuarterlyEpsRows(rawRows: unknown): QuarterlyEpsPoint[] {
  if (!Array.isArray(rawRows)) return [];

  const byQuarter = new Map<string, QuarterlyEpsPoint & { quarterKey: string }>();

  for (const rawItem of rawRows) {
    if (!rawItem || typeof rawItem !== "object") continue;
    const item = rawItem as Record<string, unknown>;
    const date = toIsoDateFromText(item.date);
    const availableDate = toIsoDateFromText(item.availableDate) || date;
    const eps = sanitizeEps(item.eps);
    if (!date || !availableDate || eps === null) continue;

    const quarterKey = quarterKeyFromDate(date);
    if (!quarterKey) continue;

    const source = item.source === "expected" ? "expected" : "actual";
    const current = byQuarter.get(quarterKey);
    if (!current || (current.source === "expected" && source === "actual")) {
      byQuarter.set(quarterKey, { quarterKey, date, eps, source, availableDate });
      continue;
    }

    if (current.source === source && date < current.date) {
      byQuarter.set(quarterKey, { quarterKey, date, eps, source, availableDate });
    }
  }

  return [...byQuarter.values()]
    .sort((a, b) => a.date.localeCompare(b.date))
    .map(({ date, eps, source, availableDate }) => ({ date, eps, source, availableDate }));
}

function normalizeQuarterlyNetIncomeRows(rawRows: unknown): QuarterlyNetIncomePoint[] {
  if (!Array.isArray(rawRows)) return [];

  const byQuarter = new Map<string, QuarterlyNetIncomePoint & { quarterKey: string }>();

  for (const rawItem of rawRows) {
    if (!rawItem || typeof rawItem !== "object") continue;
    const item = rawItem as Record<string, unknown>;
    const date = toIsoDateFromText(item.date);
    const netIncome = sanitizeNetIncome(item.netIncome);
    if (!date || netIncome === null) continue;

    const quarterKey = quarterKeyFromDate(date);
    if (!quarterKey) continue;

    const source = item.source === "expected" ? "expected" : "actual";
    const current = byQuarter.get(quarterKey);
    if (!current || (current.source === "expected" && source === "actual")) {
      byQuarter.set(quarterKey, { quarterKey, date, netIncome, source });
      continue;
    }

    if (current.source === source && date < current.date) {
      byQuarter.set(quarterKey, { quarterKey, date, netIncome, source });
    }
  }

  return [...byQuarter.values()]
    .sort((a, b) => a.date.localeCompare(b.date))
    .map(({ date, netIncome, source }) => ({ date, netIncome, source }));
}

function buildTtmAnchorsFromQuarterlyEps(rows: QuarterlyEpsPoint[]): QuarterlyEpsPoint[] {
  if (!Array.isArray(rows) || rows.length < 4) return [];

  const ordered = normalizeQuarterlyEpsRows(rows);
  if (ordered.length < 4) return [];

  const anchors: QuarterlyEpsPoint[] = [];

  for (let i = 3; i < ordered.length; i += 1) {
    const window = ordered.slice(i - 3, i + 1);
    if (window.some((row) => !Number.isFinite(row.eps))) continue;

    let isContiguous = true;
    for (let j = 1; j < window.length; j += 1) {
      const prevTs = toTs(window[j - 1].date);
      const currTs = toTs(window[j].date);
      if (!prevTs || !currTs) {
        isContiguous = false;
        break;
      }
      const gapDays = (currTs - prevTs) / 86_400_000;
      if (!Number.isFinite(gapDays) || gapDays < 40 || gapDays > 140) {
        isContiguous = false;
        break;
      }
    }

    if (!isContiguous) continue;

    const anchorAvailableDate = toIsoDateFromText(window[3].availableDate) || window[3].date;
    const anchorDate = anchorAvailableDate < window[3].date ? window[3].date : anchorAvailableDate;
    anchors.push({
      date: anchorDate,
      eps: roundTo(window.reduce((sum, item) => sum + item.eps, 0), 6),
      source: window[3].source,
      availableDate: anchorDate,
    });
  }

  return anchors;
}

function findNearestPointByDate(points: SnapshotPoint[], targetDate: string, maxGapDays = 7): SnapshotPoint | null {
  if (!Array.isArray(points) || !points.length) return null;

  const targetTs = toTs(targetDate);
  if (!targetTs) return null;

  let best: SnapshotPoint | null = null;
  let bestGap = Number.POSITIVE_INFINITY;

  for (const point of points) {
    const ts = toTs(point.date);
    const close = Number(point.close);
    const pe = Number(point.pe_ttm);
    if (!ts) continue;
    if (!Number.isFinite(close) || close <= 0) continue;
    if (!Number.isFinite(pe) || Math.abs(pe) <= 1e-8) continue;

    const gapDays = Math.abs(ts - targetTs) / 86_400_000;
    if (gapDays > maxGapDays) continue;

    if (gapDays < bestGap) {
      best = point;
      bestGap = gapDays;
    }
  }

  return best;
}

function snapQuarterlyEpsScaleFactor(rawFactor: number): number | null {
  if (!Number.isFinite(rawFactor) || rawFactor <= 0) return null;

  const hints = [
    0.1,
    0.125,
    1 / 6,
    0.2,
    0.25,
    1 / 3,
    0.4,
    0.5,
    2 / 3,
    0.8,
    1,
    1.25,
    1.5,
    2,
    2.5,
    3,
    4,
    5,
    6,
    8,
    10,
  ];

  let best: number | null = null;
  let bestErr = Number.POSITIVE_INFINITY;

  for (const candidate of hints) {
    const err = Math.abs(rawFactor - candidate) / candidate;
    if (err < bestErr) {
      bestErr = err;
      best = candidate;
    }
  }

  return best !== null && bestErr <= 0.12 ? best : null;
}

function alignQuarterlyEpsToValuationBasis(
  quarterlyRows: QuarterlyEpsPoint[],
  valuationPoints: SnapshotPoint[]
): QuarterlyEpsPoint[] {
  if (!Array.isArray(quarterlyRows) || quarterlyRows.length < 4) return quarterlyRows;
  if (!Array.isArray(valuationPoints) || valuationPoints.length < 24) return quarterlyRows;

  const anchors = buildTtmAnchorsFromQuarterlyEps(quarterlyRows).filter((row) => row.source === "actual");
  if (anchors.length < 8) return quarterlyRows;

  const ratioSamples: Array<{ date: string; ratio: number }> = [];

  for (const anchor of anchors) {
    if (!Number.isFinite(anchor.eps) || Math.abs(anchor.eps) <= 1e-8) continue;

    const point = findNearestPointByDate(valuationPoints, anchor.date, 7);
    if (!point) continue;

    const close = Number(point.close);
    const pe = Number(point.pe_ttm);
    if (!Number.isFinite(close) || close <= 0) continue;
    if (!Number.isFinite(pe) || Math.abs(pe) <= 1e-8) continue;

    const impliedTtm = close / pe;
    const ratio = impliedTtm / anchor.eps;
    if (!Number.isFinite(ratio) || ratio <= 0) continue;
    ratioSamples.push({
      date: anchor.date,
      ratio,
    });
  }

  if (ratioSamples.length < 8) return quarterlyRows;

  const ordered = [...ratioSamples].sort((a, b) => a.date.localeCompare(b.date));
  const recent = ordered.slice(-16);
  const selected = recent.length >= 8 ? recent : ordered;
  const selectedRatios = selected.map((item) => item.ratio);

  const ratioMedian = median(selectedRatios);
  if (!ratioMedian || ratioMedian <= 0) return quarterlyRows;

  const logMedian = Math.log(ratioMedian);
  const madLog = median(selectedRatios.map((item) => Math.abs(Math.log(item) - logMedian)));
  if (!Number.isFinite(madLog as number) || (madLog as number) > 0.45) {
    return quarterlyRows;
  }

  const isMeaningfulDeviation = ratioMedian < 0.7 || ratioMedian > 1.43;
  if (!isMeaningfulDeviation) return quarterlyRows;

  const snapped = snapQuarterlyEpsScaleFactor(ratioMedian);
  const scaleFactor = snapped ?? ratioMedian;
  if (!Number.isFinite(scaleFactor) || scaleFactor < 0.1 || scaleFactor > 10) {
    return quarterlyRows;
  }

  let cutoffDate = "";
  const older = ordered.slice(0, Math.max(0, ordered.length - selected.length));
  if (older.length >= 8) {
    const olderMedian = median(older.map((item) => item.ratio));
    if (olderMedian && olderMedian > 0) {
      const olderIsNearOne = olderMedian >= 0.8 && olderMedian <= 1.25;
      const regimeDelta = ratioMedian / olderMedian;
      if (olderIsNearOne && (regimeDelta >= 1.8 || regimeDelta <= 0.55)) {
        cutoffDate = selected[0]?.date || "";
      }
    }
  }

  return quarterlyRows.map((row) => ({
    ...row,
    eps:
      cutoffDate && row.date < cutoffDate
        ? row.eps
        : roundTo(row.eps * scaleFactor, 6),
  }));
}

function overrideRecentPeTtmWithLatestActualTtmEps(
  valuationPoints: SnapshotPoint[],
  quarterlyRows: QuarterlyEpsPoint[]
): SnapshotPoint[] {
  if (!Array.isArray(valuationPoints) || !valuationPoints.length) return valuationPoints;
  if (!Array.isArray(quarterlyRows) || quarterlyRows.length < 4) return valuationPoints;

  const latestValuationDate = valuationPoints[valuationPoints.length - 1]?.date || "";
  const latestValuationTs = toTs(latestValuationDate);
  if (!latestValuationTs) return valuationPoints;

  const actualRows = normalizeQuarterlyEpsRows(quarterlyRows).filter((row) => row.source === "actual");
  if (actualRows.length < 4) return valuationPoints;

  const actualAnchors = buildTtmAnchorsFromQuarterlyEps(actualRows).filter((row) => row.source === "actual");
  const latestAnchor = actualAnchors[actualAnchors.length - 1];
  const latestActualQuarter = actualRows[actualRows.length - 1];
  if (!latestAnchor || !latestActualQuarter) return valuationPoints;

  const effectiveStartDate = (() => {
    const availableDate = toIsoDateFromText(latestActualQuarter.availableDate) || latestActualQuarter.date;
    return availableDate > latestActualQuarter.date ? availableDate : latestActualQuarter.date;
  })();
  const latestAnchorTs = toTs(effectiveStartDate);
  const latestTtmEps = sanitizeEps(latestAnchor.eps);
  if (!latestAnchorTs || latestTtmEps === null || Math.abs(latestTtmEps) <= 1e-8) {
    return valuationPoints;
  }

  const stalenessDays = (latestValuationTs - latestAnchorTs) / 86_400_000;
  if (!Number.isFinite(stalenessDays) || stalenessDays < 0 || stalenessDays > 190) {
    // Avoid forcing stale-denominator PE when latest actual quarter is too old.
    return valuationPoints;
  }

  const latestValuationPoint = valuationPoints[valuationPoints.length - 1];
  const latestClose = Number(latestValuationPoint?.close);
  const latestSeriesPe = sanitizeSignedRatio(latestValuationPoint?.pe_ttm ?? null);
  const latestActualBasedPe =
    Number.isFinite(latestClose) && latestClose > 0 ? sanitizeSignedRatio(latestClose / latestTtmEps) : null;
  if (latestSeriesPe && latestActualBasedPe) {
    const basisMismatchFactor = Math.max(latestSeriesPe / latestActualBasedPe, latestActualBasedPe / latestSeriesPe);
    if (!Number.isFinite(basisMismatchFactor) || basisMismatchFactor >= STOCK_TTM_BASIS_MISMATCH_FACTOR) {
      // Typical for ADR/foreign listings where latest actual EPS basis may not match quote currency/share class.
      return valuationPoints;
    }
  }

  return valuationPoints.map((point) => {
    if (point.date < effectiveStartDate) return point;
    const close = Number(point.close);
    if (!Number.isFinite(close) || close <= 0) return point;

    const peTtm = close / latestTtmEps;
    if (!Number.isFinite(peTtm) || Math.abs(peTtm) > METRIC_MAX.pe_ttm) {
      return point;
    }

    return {
      ...point,
      pe_ttm: roundTo(peTtm, 6),
    };
  });
}

function buildTtmGrowthAnchorsFromQuarterlyEps(
  quarterlyRows: QuarterlyEpsPoint[]
): Array<{ date: string; growthRate: number }> {
  if (!Array.isArray(quarterlyRows) || quarterlyRows.length < 8) return [];

  const actualRows = normalizeQuarterlyEpsRows(quarterlyRows).filter((row) => row.source === "actual");
  if (actualRows.length < 8) return [];

  const ttmAnchors = buildTtmAnchorsFromQuarterlyEps(actualRows).filter((row) => row.source === "actual");
  if (ttmAnchors.length < 8) return [];

  const out: Array<{ date: string; growthRate: number }> = [];
  for (let i = 4; i < ttmAnchors.length; i += 1) {
    const current = ttmAnchors[i];
    const prevYear = ttmAnchors[i - 4];
    const currEps = Number(current.eps);
    const prevEps = Number(prevYear.eps);
    if (!Number.isFinite(currEps) || !Number.isFinite(prevEps) || Math.abs(prevEps) <= 1e-8) continue;

    const growthRate = currEps / prevEps - 1;
    if (!Number.isFinite(growthRate) || Math.abs(growthRate) <= 1e-8) continue;
    if (Math.abs(growthRate) > 20) continue;

    out.push({
      date: current.date,
      growthRate,
    });
  }

  const byDate = new Map<string, number>();
  for (const item of out) {
    byDate.set(item.date, item.growthRate);
  }

  return [...byDate.entries()]
    .map(([date, growthRate]) => ({ date, growthRate }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function enrichPegFromQuarterlyEpsGrowth(
  valuationPoints: SnapshotPoint[],
  quarterlyRows: QuarterlyEpsPoint[]
): SnapshotPoint[] {
  if (!Array.isArray(valuationPoints) || !valuationPoints.length) return valuationPoints;

  const growthAnchors = buildTtmGrowthAnchorsFromQuarterlyEps(quarterlyRows);
  if (!growthAnchors.length) return valuationPoints;

  let anchorIndex = 0;
  let latestGrowthRate: number | null = null;
  let changed = false;

  const nextPoints = valuationPoints.map((point) => {
    while (anchorIndex < growthAnchors.length && growthAnchors[anchorIndex].date <= point.date) {
      latestGrowthRate = growthAnchors[anchorIndex].growthRate;
      anchorIndex += 1;
    }

    const currentPeg = Number(point.peg);
    if (Number.isFinite(currentPeg)) return point;
    if (!Number.isFinite(latestGrowthRate as number) || Math.abs(Number(latestGrowthRate)) <= 1e-8) return point;

    const peTtm = Number(point.pe_ttm);
    if (!Number.isFinite(peTtm)) return point;

    const pegValue = peTtm / (Number(latestGrowthRate) * 100);
    if (!Number.isFinite(pegValue)) return point;

    changed = true;
    return {
      ...point,
      peg: roundTo(clamp(pegValue, -METRIC_MAX.peg, METRIC_MAX.peg), 4),
    };
  });

  return changed ? nextPoints : valuationPoints;
}

async function loadPreviousSeriesBySymbol(): Promise<Map<string, PreviousSeries>> {
  const bySymbol = new Map<string, PreviousSeries>();

  try {
    const raw = await readFile(OUTPUT_FILE, "utf8");
    const parsed = JSON.parse(raw) as {
      indices?: Array<{
        symbol?: string;
        forwardStartDate?: string;
        peg?: number | null;
        points?: SnapshotPoint[];
        quarterlyEps?: QuarterlyEpsPoint[];
        quarterlyNetIncome?: QuarterlyNetIncomePoint[];
      }>;
    };

    for (const item of parsed.indices || []) {
      const symbol = String(item?.symbol || "").trim().toUpperCase();
      const points = Array.isArray(item?.points) ? item.points : [];
      if (!symbol || points.length < 24) continue;

      bySymbol.set(symbol, {
        forwardStartDate: String(item?.forwardStartDate || points[0]?.date || ""),
        peg: sanitizeSignedRatio(item?.peg),
        points,
        quarterlyEps: normalizeQuarterlyEpsRows(item?.quarterlyEps),
        quarterlyNetIncome: normalizeQuarterlyNetIncomeRows(item?.quarterlyNetIncome),
      });
    }
  } catch {
    // no previous dataset yet
  }

  return bySymbol;
}

async function loadYahooDailyMetricSnapshotsBySymbol(): Promise<Map<string, YahooDailyMetricSnapshot[]>> {
  const bySymbol = new Map<string, YahooDailyMetricSnapshot[]>();

  try {
    const raw = await readFile(YAHOO_DAILY_METRICS_FILE, "utf8");
    const parsed = JSON.parse(raw) as {
      symbols?: Record<string, unknown>;
    };
    const source = parsed?.symbols && typeof parsed.symbols === "object" ? parsed.symbols : {};

    for (const [rawSymbol, rawRows] of Object.entries(source)) {
      const symbol = String(rawSymbol || "").trim().toUpperCase();
      if (!symbol || !Array.isArray(rawRows)) continue;

      const byDate = new Map<string, YahooDailyMetricSnapshot>();
      for (const row of rawRows) {
        const normalized = normalizeYahooDailyMetricSnapshot(row);
        if (!normalized) continue;
        byDate.set(normalized.date, normalized);
      }

      if (!byDate.size) continue;
      bySymbol.set(
        symbol,
        [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date))
      );
    }
  } catch {
    // no previous yahoo daily metric store yet
  }

  return bySymbol;
}

function serializeYahooDailyMetricSnapshots(
  bySymbol: Map<string, YahooDailyMetricSnapshot[]>
): { generatedAt: string; symbols: Record<string, YahooDailyMetricSnapshot[]> } {
  const symbols = [...bySymbol.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .reduce<Record<string, YahooDailyMetricSnapshot[]>>((acc, [symbol, rows]) => {
      const normalizedRows = rows
        .map((item) => normalizeYahooDailyMetricSnapshot(item))
        .filter((item): item is YahooDailyMetricSnapshot => !!item)
        .sort((a, b) => a.date.localeCompare(b.date));
      if (!normalizedRows.length) return acc;
      acc[symbol] = normalizedRows;
      return acc;
    }, {});

  return {
    generatedAt: new Date().toISOString(),
    symbols,
  };
}

function toCompanyId(symbol: string): string {
  return `company_${symbol.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "")}`;
}

function stripGrowthOnlyFieldsFromSnapshotPoints(points: SnapshotPoint[]): Array<Record<string, unknown>> {
  return points
    .filter((point) => point && typeof point === "object")
    .map((point) => {
      const nextPoint = { ...(point as Record<string, unknown>) };
      delete nextPoint.close;
      return nextPoint;
    });
}

async function main(): Promise<void> {
  const previousSeriesBySymbol = await loadPreviousSeriesBySymbol();
  const yahooDailyMetricsBySymbol = await loadYahooDailyMetricSnapshotsBySymbol();
  const symbolFilter = parseSymbolFilterFromEnv();
  const symbolFilterSet = symbolFilter.length ? new Set(symbolFilter) : null;

  console.log("[company] loading top 100 companies...");
  const companies = await fetchTopCompanies();
  if (companies.length < 90) {
    throw new Error(`Top company list is too short: ${companies.length}`);
  }

  if (symbolFilterSet) {
    const availableSymbols = new Set(companies.map((item) => item.symbol));
    const missing = symbolFilter.filter((symbol) => !availableSymbols.has(symbol));
    if (missing.length) {
      console.warn(`[company] symbol filter missing from top list: ${missing.join(", ")}`);
    }
    console.log(
      `[company] symbol filter active: ${symbolFilter.join(", ")} (refresh selected symbols, reuse previous for others)`
    );
  }

  let yahooMarketLatestDate = "";
  try {
    const resolved = await fetchYahooMarketLatestDate();
    yahooMarketLatestDate = resolved || "";
  } catch {
    yahooMarketLatestDate = "";
  }
  console.log(`[company] yahoo market latest date: ${yahooMarketLatestDate || "unavailable"}`);

  console.log(`[company] parsed ${companies.length} companies, building series...`);

  let fallbackAnchorCount = 0;
  let skippedCount = 0;
  let reusedPreviousCount = 0;
  let yahooLatestOverrideTargetCount = 0;
  let yahooLatestOverrideAppliedCount = 0;
  let yahooLatestOverrideMissingCount = 0;

  const built = await mapLimit(companies, CONCURRENCY, async (company, index) => {
    console.log(`[company] ${String(index + 1).padStart(3, "0")}/${companies.length} ${company.symbol}`);
    const previousSeries = previousSeriesBySymbol.get(company.symbol);

    if (symbolFilterSet && !symbolFilterSet.has(company.symbol)) {
      if (previousSeries) {
        reusedPreviousCount += 1;
        const reusedPointsRaw = applyYahooDailyMetricSnapshotsToPoints(
          previousSeries.points,
          yahooDailyMetricsBySymbol.get(company.symbol) || []
        );
        const reusedPoints = capSnapshotSeriesByDate(reusedPointsRaw, yahooMarketLatestDate);
        const reusedLatestPoint = reusedPoints[reusedPoints.length - 1] || null;
        return {
          id: toCompanyId(company.symbol),
          symbol: company.symbol,
          displayName: company.displayName,
          description: `${company.displayName} (${company.symbol})`,
          rank: company.rank,
          marketCap: company.marketCap,
          peg:
            sanitizeSignedRatio(reusedLatestPoint?.peg) ??
            previousSeries.peg ??
            null,
          forwardStartDate: previousSeries.forwardStartDate || previousSeries.points[0]?.date || "",
          points: reusedPoints,
          quarterlyEps: previousSeries.quarterlyEps || [],
          quarterlyNetIncome: previousSeries.quarterlyNetIncome || [],
        };
      }

      skippedCount += 1;
      console.warn(`[company] skip ${company.symbol}: filtered out and no previous series`);
      return null;
    }

    const shouldUseYahooLatestOverride = shouldUseYahooLatestOverrideForSymbol(company.symbol);
    if (shouldUseYahooLatestOverride) {
      yahooLatestOverrideTargetCount += 1;
    }
    const [
      closePointsRaw,
      stockQuarterlyPayload,
      stockStatisticsPayload,
      yahooFetchResult,
      longPeSeries,
      longPbSeries,
      ychartsSeries,
    ] = await Promise.all([
      fetchCloseHistory(company.symbol, company.slug),
      fetchQuarterlyRatioPayload(company.symbol),
      fetchStockAnalysisStatisticsRatioPayload(company.symbol),
      fetchYahooKeyStatisticsRatioPayload(company.symbol),
      fetchCompaniesMarketCapMetricSeries(company.slug, "pe-ratio"),
      fetchCompaniesMarketCapMetricSeries(company.slug, "pb-ratio"),
      fetchYchartsSeriesBundle(company.symbol),
    ]);
    const yahooLatestPayload = yahooFetchResult?.payload || null;
    const yahooQuoteLatestPayload = yahooFetchResult?.quoteLatestPayload || null;
    const yahooLatestPeg = sanitizeSignedRatio(
      yahooQuoteLatestPayload?.latest.peg ?? yahooLatestPayload?.latest.peg ?? null
    );
    const yahooLatestPeTtm = sanitizeSignedRatio(yahooQuoteLatestPayload?.latest.pe_ttm ?? null);
    const yahooLatestPeForward = sanitizeSignedRatio(yahooQuoteLatestPayload?.latest.pe_forward ?? null);
    if (shouldUseYahooLatestOverride) {
      if (yahooLatestPeTtm || yahooLatestPeForward) {
        yahooLatestOverrideAppliedCount += 1;
      } else {
        yahooLatestOverrideMissingCount += 1;
      }
    }
    const preferredLatestRatioOverride = shouldUseYahooLatestOverride
      ? yahooQuoteLatestPayload
      : null;

    const stockPayload = mergeRatioPayloadList(
      [stockQuarterlyPayload, stockStatisticsPayload].filter(Boolean) as RatioPayload[]
    );

    const ychartsClosePoints = metricPointsToCloseSeries(ychartsSeries?.price || []);
    let closePoints = closePointsRaw;
    if (ychartsClosePoints.length >= 200) {
      closePoints =
        closePointsRaw.length >= 200
          ? mergeCloseSeries(closePointsRaw, ychartsClosePoints)
          : ychartsClosePoints;
      closePoints = densifyCloseSeriesWithRecentDailyVol(closePoints);
    }
    closePoints = capCloseSeriesByDate(closePoints, yahooMarketLatestDate);

    const mergedLongPeSeries = mergeMetricSeriesWithPreference(ychartsSeries?.pe_ttm || [], longPeSeries);
    const mergedLongPbSeries = mergeMetricSeriesWithPreference(ychartsSeries?.pb || [], longPbSeries);
    // Forward PE source policy:
    // 1) Prefer stock-level ratios from StockAnalysis (quarterly anchors + latest),
    // 2) Use YCharts forward series only as sparse-history fallback.
    const stockForwardAnchorCount = countForwardAnchors(stockPayload);
    const ychartsForwardSeries = ychartsSeries?.pe_forward || [];
    const useYchartsForwardFallback =
      stockForwardAnchorCount < MIN_FORWARD_ANCHORS_FOR_HISTORY &&
      ychartsForwardSeries.length >= MIN_FORWARD_ANCHORS_FOR_HISTORY;
    let mergedLongForwardSeries = useYchartsForwardFallback ? ychartsForwardSeries : [];

    const sourceHints = {
      pe: (ychartsSeries?.pe_ttm || []).length ? "ycharts-pe-ratio" : "companiesmarketcap-pe-ratio",
      pb: (ychartsSeries?.pb || []).length ? "ycharts-price-to-book-value" : "companiesmarketcap-pb-ratio",
      forward: useYchartsForwardFallback ? "ycharts-forward-pe-ratio-fallback" : "stockanalysis-forward-ratios-primary",
    };

    if (!closePoints.length) {
      if (previousSeries) {
        reusedPreviousCount += 1;
        console.warn(`[company] reuse ${company.symbol}: close history unavailable`);
        const reusedPointsRaw = applyYahooDailyMetricSnapshotsToPoints(
          previousSeries.points,
          yahooDailyMetricsBySymbol.get(company.symbol) || []
        );
        const reusedPoints = capSnapshotSeriesByDate(reusedPointsRaw, yahooMarketLatestDate);
        const reusedLatestPoint = reusedPoints[reusedPoints.length - 1] || null;
        return {
          id: toCompanyId(company.symbol),
          symbol: company.symbol,
          displayName: company.displayName,
          description: `${company.displayName} (${company.symbol})`,
          rank: company.rank,
          marketCap: company.marketCap,
          peg:
            yahooLatestPeg ??
            sanitizeSignedRatio(reusedLatestPoint?.peg) ??
            previousSeries.peg ??
            null,
          forwardStartDate: previousSeries.forwardStartDate || previousSeries.points[0]?.date || "",
          points: reusedPoints,
          quarterlyEps: previousSeries.quarterlyEps || [],
          quarterlyNetIncome: previousSeries.quarterlyNetIncome || [],
        };
      }

      skippedCount += 1;
      console.warn(`[company] skip ${company.symbol}: close history unavailable`);
      return null;
    }

    const lastCloseDate = closePoints[closePoints.length - 1]?.date || "";
    if (lastCloseDate) {
      upsertYahooDailyMetricSnapshot(
        yahooDailyMetricsBySymbol,
        company.symbol,
        createYahooDailyMetricSnapshot(lastCloseDate, yahooQuoteLatestPayload)
      );
    }
    const yahooDailySnapshots = yahooDailyMetricsBySymbol.get(company.symbol) || [];

    if (
      FORWARD_PE_PROXY_FROM_TTM_SYMBOLS.has(company.symbol) &&
      mergedLongForwardSeries.length < MIN_FORWARD_ANCHORS_FOR_HISTORY
    ) {
      const proxySeries = deriveForwardPeProxySeriesFromTtmPe(
        mergedLongPeSeries,
        yahooQuoteLatestPayload?.latest.pe_ttm ?? yahooLatestPayload?.latest.pe_ttm ?? stockPayload?.latest.pe_ttm,
        yahooQuoteLatestPayload?.latest.pe_forward ??
          yahooLatestPayload?.latest.pe_forward ??
          stockPayload?.latest.pe_forward,
        lastCloseDate,
        8
      );

      if (proxySeries.length >= MIN_FORWARD_ANCHORS_FOR_HISTORY) {
        mergedLongForwardSeries = mergeMetricSeriesWithPreference(mergedLongForwardSeries, proxySeries);
        sourceHints.forward = [sourceHints.forward, "forward-pe-proxy-from-ttm-pe"].filter(Boolean).join("+");
      }
    }

    const quarterlyFinancialSeries = await fetchQuarterlyFinancialSeries(company.symbol, lastCloseDate);
    const quarterlyEpsRaw = normalizeQuarterlyEpsRows(quarterlyFinancialSeries.quarterlyEps);
    const quarterlyNetIncome = normalizeQuarterlyNetIncomeRows(
      quarterlyFinancialSeries.quarterlyNetIncome
    );

    let ratioPayload = mergeRatioPayloads(
      stockPayload,
      mergedLongPeSeries,
      mergedLongPbSeries,
      mergedLongForwardSeries,
      sourceHints,
      company.symbol
    );
    if (shouldUseYahooLatestOverride) {
      ratioPayload = applyLatestRatioOverrideAtLastDate(ratioPayload, yahooQuoteLatestPayload, lastCloseDate);
    }
    const { points, usedFallback, forwardStartDate } = buildValuationSeries(closePoints, ratioPayload);
    const quarterlyEps = alignQuarterlyEpsToValuationBasis(quarterlyEpsRaw, points);
    const pointsWithLatestActualTtm = overrideRecentPeTtmWithLatestActualTtmEps(points, quarterlyEps);
    const finalPoints = applyLatestRatioOverrideToLastPoint(pointsWithLatestActualTtm, preferredLatestRatioOverride);
    const pointsWithPreservedYahooDates = preserveRecordedYahooDailyPoints(
      finalPoints,
      previousSeries?.points || [],
      yahooDailySnapshots,
      yahooMarketLatestDate
    );
    const pointsWithYahooDailyMetricsRaw = applyYahooDailyMetricSnapshotsToPoints(
      pointsWithPreservedYahooDates,
      yahooDailySnapshots
    );
    const pointsWithYahooDailyMetrics = capSnapshotSeriesByDate(
      pointsWithYahooDailyMetricsRaw,
      yahooMarketLatestDate
    );
    if (usedFallback) {
      fallbackAnchorCount += 1;
    }

    if (pointsWithYahooDailyMetrics.length < 24) {
      if (previousSeries) {
        reusedPreviousCount += 1;
        console.warn(
          `[company] reuse ${company.symbol}: insufficient points (${pointsWithYahooDailyMetrics.length})`
        );
        const reusedPointsRaw = applyYahooDailyMetricSnapshotsToPoints(
          previousSeries.points,
          yahooDailyMetricsBySymbol.get(company.symbol) || []
        );
        const reusedPoints = capSnapshotSeriesByDate(reusedPointsRaw, yahooMarketLatestDate);
        const reusedLatestPoint = reusedPoints[reusedPoints.length - 1] || null;
        return {
          id: toCompanyId(company.symbol),
          symbol: company.symbol,
          displayName: company.displayName,
          description: `${company.displayName} (${company.symbol})`,
          rank: company.rank,
          marketCap: company.marketCap,
          peg:
            yahooLatestPeg ??
            sanitizeSignedRatio(reusedLatestPoint?.peg) ??
            previousSeries.peg ??
            null,
          forwardStartDate: previousSeries.forwardStartDate || previousSeries.points[0]?.date || "",
          points: reusedPoints,
          quarterlyEps: previousSeries.quarterlyEps || [],
          quarterlyNetIncome: previousSeries.quarterlyNetIncome || [],
        };
      }

      skippedCount += 1;
      console.warn(
        `[company] skip ${company.symbol}: insufficient points (${pointsWithYahooDailyMetrics.length})`
      );
      return null;
    }

    const latestPointWithYahooDailyMetrics = pointsWithYahooDailyMetrics[pointsWithYahooDailyMetrics.length - 1] || null;

    return {
      id: toCompanyId(company.symbol),
      symbol: company.symbol,
      displayName: company.displayName,
      description: `${company.displayName} (${company.symbol})`,
      rank: company.rank,
      marketCap: company.marketCap,
      peg:
        sanitizeSignedRatio(latestPointWithYahooDailyMetrics?.peg) ??
        yahooLatestPeg,
      forwardStartDate:
        forwardStartDate || pointsWithYahooDailyMetrics[pointsWithYahooDailyMetrics.length - 1]?.date || "",
      points: pointsWithYahooDailyMetrics,
      quarterlyEps,
      quarterlyNetIncome,
    };
  });

  const indices = built.filter(Boolean).sort((a, b) => a.rank - b.rank);

  const minRequiredSeries = symbolFilterSet ? Math.max(1, symbolFilterSet.size) : 80;
  if (indices.length < minRequiredSeries) {
    throw new Error(`Too few company series generated: ${indices.length} (required: ${minRequiredSeries})`);
  }

  const symbolFilterSourceTag = symbolFilter.length
    ? `symbol-filter-target-${symbolFilter.join("_").toLowerCase()}`
    : "symbol-filter-target-all";

  const serializedIndices = indices.map((item) => ({
    id: item.id,
    symbol: item.symbol,
    displayName: item.displayName,
    description: item.description,
    rank: item.rank,
    marketCap: item.marketCap,
    peg: sanitizeSignedRatio(item.peg ?? null),
    forwardStartDate: item.forwardStartDate,
    points: stripGrowthOnlyFieldsFromSnapshotPoints(item.points),
  }));

  const dataset = {
    generatedAt: new Date().toISOString(),
    source: [
      "companiesmarketcap-global-toplist",
      "us-listed-symbol-filter",
      symbolFilterSourceTag,
      "stooq-daily-close",
      "yahoo-chart-close-fallback",
      "nasdaq-historical-close-fallback",
      "companiesmarketcap-close-fallback",
      "ycharts-fund-data-price",
      "ycharts-fund-data-pe-ratio",
      "ycharts-fund-data-forward-pe-ratio",
      "ycharts-fund-data-price-to-book-value",
      "companiesmarketcap-pe-ratio",
      "companiesmarketcap-pb-ratio",
      "stockanalysis-quarterly-ratios",
      "stockanalysis-statistics-ratios-latest-fallback",
      "yahoo-key-statistics-latest-metrics",
      "yahoo-daily-metric-history-store",
      `yahoo-market-latest-date-${yahooMarketLatestDate || "unavailable"}`,
      `yahoo-latest-override-target-${yahooLatestOverrideTargetCount}`,
      `yahoo-latest-override-applied-${yahooLatestOverrideAppliedCount}`,
      `yahoo-latest-override-missing-${yahooLatestOverrideMissingCount}`,
      "stockanalysis-quarterly-income-eps",
      "stockanalysis-quarterly-income-net-income",
      "stockanalysis-forecast-quarterly-eps",
      "stockanalysis-forecast-quarterly-net-income",
      "expected-net-income-from-forecast-eps-x-diluted-shares",
      "sec-companyfacts-quarterly-eps",
      "sec-companyfacts-quarterly-net-income",
      "anchor-interval-daily-return-projection",
      `fallback-anchor-${fallbackAnchorCount}`,
      `reused-previous-series-${reusedPreviousCount}`,
      `skipped-${skippedCount}`,
      `history-start-${HISTORY_START_DATE}`,
    ].join("+"),
    indices: serializedIndices,
  };

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(OUTPUT_FILE, `${JSON.stringify(dataset)}\n`, "utf8");
  await writeFile(
    YAHOO_DAILY_METRICS_FILE,
    `${JSON.stringify(serializeYahooDailyMetricSnapshots(yahooDailyMetricsBySymbol))}\n`,
    "utf8"
  );

  console.log(`[company] snapshot written: ${OUTPUT_FILE}`);
  console.log(`[company] yahoo daily metrics written: ${YAHOO_DAILY_METRICS_FILE}`);
  console.log(`[company] generatedAt: ${dataset.generatedAt}`);
  console.log(`[company] series count: ${indices.length}`);
  console.log(`[company] fallback anchors: ${fallbackAnchorCount}`);
  console.log(`[company] reused previous: ${reusedPreviousCount}`);
  console.log(`[company] skipped: ${skippedCount}`);
  console.log(
    `[company] yahoo latest override: target=${yahooLatestOverrideTargetCount} ` +
      `applied=${yahooLatestOverrideAppliedCount} missing=${yahooLatestOverrideMissingCount}`
  );
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
