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
}

interface RatioPayload {
  anchors: RatioAnchor[];
  latest: {
    pe_ttm: number | null;
    pe_forward: number | null;
    pb: number | null;
  };
  source: string;
}

interface MetricPoint {
  date: string;
  ts: number;
  value: number;
}

interface MetricAnchorDenominator {
  date: string;
  ts: number;
  denominator: number;
}

interface SnapshotPoint {
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  us10y_yield: number;
}

interface PreviousSeries {
  forwardStartDate: string;
  points: SnapshotPoint[];
}

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const OUTPUT_DIR = path.join(ROOT_DIR, "data", "standardized");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "company-valuation-history.json");
const execFileAsync = promisify(execFile);
let companiesMarketCapFetchChain: Promise<unknown> = Promise.resolve();

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
};

const METRIC_MAX = {
  pe_ttm: 1_000_000,
  pe_forward: 1_000_000,
  pb: 1_000_000,
};

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

function toTs(dateText: string): number {
  return Date.parse(`${dateText}T00:00:00Z`) || 0;
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

function sanitizeSignedRatio(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (Math.abs(n) < 1e-8) return null;
  return n;
}

function toIsoDateFromEpoch(epochLike: unknown): string | null {
  const value = Number(epochLike);
  if (!Number.isFinite(value) || value <= 0) return null;

  const ms = value > 1e12 ? value : value * 1000;
  const date = new Date(ms);
  if (!Number.isFinite(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function median(values: readonly number[]): number | null {
  const arr = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  if (arr.length % 2) return arr[mid];
  return (arr[mid - 1] + arr[mid]) / 2;
}

function isRejectedPayload(text: string): boolean {
  const raw = String(text || "").trim().toLowerCase();
  if (!raw) return true;
  if (raw.includes("exceeded the daily hits limit")) return true;
  if (raw.includes("too many requests")) return true;
  if (raw.includes("enable javascript and cookies to continue")) return true;
  if (raw.includes("<title>just a moment")) return true;
  if (raw.includes("just a moment") && raw.includes("cf-chl")) return true;
  return false;
}

async function fetchText(
  url: string,
  retries = 1,
  timeoutMs = REQUEST_TIMEOUT_MS,
  directFirst = false
): Promise<string> {
  let lastError: unknown;

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
        url,
      ];

      const proxyCmd = baseArgs;
      const directCmd = ["--noproxy", "*", ...baseArgs];
      const commandCandidates: string[][] = directFirst ? [directCmd, proxyCmd] : [proxyCmd, directCmd];

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

function parseCompaniesMarketCapRatioSeries(html: string): MetricPoint[] {
  const text = String(html || "");
  const matches = [...text.matchAll(/data\s*=\s*(\[\{[\s\S]*?\}\]);/g)];
  if (!matches.length) return [];

  let best: Array<{ d?: unknown; v?: unknown }> = [];

  for (const match of matches) {
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

function parseRatioPayloadFromScript(rawText: string): RatioPayload | null {
  const text = String(rawText || "").replace(/\n/g, " ");
  if (!text.includes("financialData:{")) return null;

  const dateKeysRaw = extractArrayLiteral(text, "datekey", "fiscalYear");
  const peRaw = extractArrayLiteral(text, "pe", "peForward");
  const fwdRaw = extractArrayLiteral(text, "peForward", "ps");
  const pbRaw = extractArrayLiteral(text, "pb", "ptbvRatio");

  const dateKeys = dateKeysRaw.map((item) => String(item || ""));
  if (!dateKeys.length) return null;

  const anchors: RatioAnchor[] = [];
  let latest: RatioPayload["latest"] = {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
  };

  for (let i = 0; i < dateKeys.length; i += 1) {
    const key = dateKeys[i];
    const pe = sanitizeSignedRatio(peRaw[i]);
    const peForward = sanitizeSignedRatio(fwdRaw[i]);
    const pb = sanitizeSignedRatio(pbRaw[i]);

    if (i === 0 || key === "TTM") {
      latest = {
        pe_ttm: pe,
        pe_forward: peForward,
        pb,
      };
      continue;
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(key)) continue;
    if (!pe && !peForward && !pb) continue;

    anchors.push({
      date: key,
      pe_ttm: pe,
      pe_forward: peForward,
      pb,
    });
  }

  return {
    anchors: anchors.sort((a, b) => a.date.localeCompare(b.date)),
    latest,
    source: "stockanalysis-financial-ratios",
  };
}

function getStockAnalysisSlugCandidates(symbol: string): string[] {
  const seed = symbol.toLowerCase();
  return [...new Set([seed, seed.replace(/-/g, "."), seed.replace(/\./g, "-"), seed.replace(/[.\-]/g, "")])].filter(Boolean);
}

async function fetchQuarterlyRatioPayload(symbol: string): Promise<RatioPayload | null> {
  const candidates = getStockAnalysisSlugCandidates(symbol);

  for (const slug of candidates) {
    try {
      const quarterlyUrl = `https://stockanalysis.com/stocks/${slug}/financials/ratios/?p=quarterly`;
      const quarterlyHtml = await fetchText(quarterlyUrl, 1, 14000);
      const quarterly = parseRatioPayloadFromScript(quarterlyHtml);

      const annualUrl = `https://stockanalysis.com/stocks/${slug}/financials/ratios/`;
      const annualHtml = await fetchText(annualUrl, 1, 12000);
      const annual = parseRatioPayloadFromScript(annualHtml);

      const mergedByDate = new Map<string, RatioAnchor>();
      for (const item of [...(quarterly?.anchors || []), ...(annual?.anchors || [])]) {
        const current = mergedByDate.get(item.date);
        if (!current) {
          mergedByDate.set(item.date, { ...item });
          continue;
        }
        mergedByDate.set(item.date, {
          date: item.date,
          pe_ttm: current.pe_ttm ?? item.pe_ttm,
          pe_forward: current.pe_forward ?? item.pe_forward,
          pb: current.pb ?? item.pb,
        });
      }

      const merged: RatioPayload = {
        anchors: [...mergedByDate.values()].sort((a, b) => a.date.localeCompare(b.date)),
        latest: {
          pe_ttm: quarterly?.latest.pe_ttm ?? annual?.latest.pe_ttm ?? null,
          pe_forward: quarterly?.latest.pe_forward ?? annual?.latest.pe_forward ?? null,
          pb: quarterly?.latest.pb ?? annual?.latest.pb ?? null,
        },
        source: `stockanalysis-financial-ratios:${slug}`,
      };

      if (merged.anchors.length || merged.latest.pe_ttm || merged.latest.pe_forward || merged.latest.pb) {
        return merged;
      }
    } catch {
      // try next slug
    }
  }

  return null;
}

function mergeRatioPayloads(
  stockPayload: RatioPayload | null,
  longPeSeries: MetricPoint[],
  longPbSeries: MetricPoint[]
): RatioPayload | null {
  const mergedByDate = new Map<string, RatioAnchor>();

  const upsert = (date: string, patch: Partial<RatioAnchor>): void => {
    const current = mergedByDate.get(date) || {
      date,
      pe_ttm: null,
      pe_forward: null,
      pb: null,
    };
    mergedByDate.set(date, {
      date,
      pe_ttm: patch.pe_ttm ?? current.pe_ttm,
      pe_forward: patch.pe_forward ?? current.pe_forward,
      pb: patch.pb ?? current.pb,
    });
  };

  for (const item of stockPayload?.anchors || []) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(item.date)) continue;
    upsert(item.date, {
      pe_ttm: sanitizeSignedRatio(item.pe_ttm),
      pe_forward: sanitizeSignedRatio(item.pe_forward),
      pb: sanitizeSignedRatio(item.pb),
    });
  }

  for (const point of longPeSeries) {
    upsert(point.date, { pe_ttm: point.value });
  }

  for (const point of longPbSeries) {
    upsert(point.date, { pb: point.value });
  }

  const latest = {
    pe_ttm:
      longPeSeries[longPeSeries.length - 1]?.value ??
      sanitizeSignedRatio(stockPayload?.latest.pe_ttm ?? null),
    pe_forward: sanitizeSignedRatio(stockPayload?.latest.pe_forward ?? null),
    pb:
      longPbSeries[longPbSeries.length - 1]?.value ??
      sanitizeSignedRatio(stockPayload?.latest.pb ?? null),
  };

  const anchors = [...mergedByDate.values()]
    .filter((item) => item.pe_ttm || item.pe_forward || item.pb)
    .sort((a, b) => a.date.localeCompare(b.date));

  if (!anchors.length && !latest.pe_ttm && !latest.pe_forward && !latest.pb) {
    return null;
  }

  const sourceTags = [
    stockPayload?.source || "",
    longPeSeries.length ? "companiesmarketcap-pe-ratio" : "",
    longPbSeries.length ? "companiesmarketcap-pb-ratio" : "",
  ].filter(Boolean);

  return {
    anchors,
    latest,
    source: sourceTags.join("+"),
  };
}

function deriveForwardScale(anchors: RatioAnchor[], latest: RatioPayload["latest"] | null): number {
  const overlapRatios = anchors
    .map((item) => {
      const pe = sanitizeSignedRatio(item.pe_ttm);
      const fwd = sanitizeSignedRatio(item.pe_forward);
      if (!pe || !fwd) return null;
      const ratio = fwd / pe;
      if (!Number.isFinite(ratio) || ratio <= 0.2 || ratio >= 2.8) return null;
      return ratio;
    })
    .filter((value): value is number => Number.isFinite(value));

  const fromAnchor = median(overlapRatios);
  if (fromAnchor) return fromAnchor;

  const latestPe = sanitizeSignedRatio(latest?.pe_ttm ?? null);
  const latestFwd = sanitizeSignedRatio(latest?.pe_forward ?? null);
  if (latestPe && latestFwd) {
    const ratio = latestFwd / latestPe;
    if (Number.isFinite(ratio) && ratio > 0.2 && ratio < 2.8) {
      return ratio;
    }
  }

  return 0.88;
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
    });
  }

  const latest = ratioPayload?.latest || {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
  };

  if (latest.pe_ttm || latest.pe_forward || latest.pb) {
    const current = byDate.get(lastDate) || {
      date: lastDate,
      pe_ttm: null,
      pe_forward: null,
      pb: null,
    };

    byDate.set(lastDate, {
      date: lastDate,
      pe_ttm: current.pe_ttm ?? sanitizeSignedRatio(latest.pe_ttm),
      pe_forward: current.pe_forward ?? sanitizeSignedRatio(latest.pe_forward),
      pb: current.pb ?? sanitizeSignedRatio(latest.pb),
    });
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function buildMetricAnchorDenominators(
  closePoints: ClosePoint[],
  anchors: RatioAnchor[],
  key: "pe_ttm" | "pe_forward" | "pb",
  fallbackMetric: number
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
      denominator,
    });
  }

  const ordered = [...byDate.values()].sort((a, b) => a.ts - b.ts);
  if (ordered.length) {
    return { anchors: ordered, usedFallback: false };
  }

  return {
    anchors: [
      {
        date: firstPoint.date,
        ts: firstPoint.ts,
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
  let anchorIndex = 0;
  const out: Array<number | null> = [];

  for (const point of closePoints) {
    if (point.ts < ordered[0].ts) {
      out.push(null);
      continue;
    }

    while (anchorIndex < ordered.length - 1 && point.ts >= ordered[anchorIndex + 1].ts) {
      anchorIndex += 1;
    }

    const rawDenominator = ordered[anchorIndex].denominator;
    const denominator =
      Math.abs(rawDenominator) < 1e-6 ? (rawDenominator < 0 ? -1e-6 : 1e-6) : rawDenominator;
    out.push(roundTo(clamp(point.close / denominator, min, max), 4));
  }

  return out;
}

function buildValuationSeries(closePoints: ClosePoint[], ratioPayload: RatioPayload | null): { points: SnapshotPoint[]; usedFallback: boolean } {
  if (!closePoints.length) {
    return { points: [], usedFallback: true };
  }

  const unifiedAnchors = buildUnifiedAnchors(closePoints, ratioPayload);
  const forwardScale = deriveForwardScale(unifiedAnchors, ratioPayload?.latest || null);
  const firstForwardTs = Math.min(
    ...unifiedAnchors
      .map((item) => (sanitizeSignedRatio(item.pe_forward) ? toTs(item.date) : Number.POSITIVE_INFINITY))
      .filter((ts) => Number.isFinite(ts))
  );

  const enrichedAnchors = unifiedAnchors.map((item) => {
    if (sanitizeSignedRatio(item.pe_forward)) return item;
    const peTtm = sanitizeSignedRatio(item.pe_ttm);
    if (!peTtm) return item;

    if (!Number.isFinite(firstForwardTs) || toTs(item.date) < firstForwardTs) {
      return {
        ...item,
        pe_forward: sanitizeSignedRatio(peTtm * forwardScale),
      };
    }

    return item;
  });

  const peAnchors = buildMetricAnchorDenominators(
    closePoints,
    enrichedAnchors,
    "pe_ttm",
    DEFAULT_METRICS.pe_ttm
  );
  const forwardAnchors = buildMetricAnchorDenominators(
    closePoints,
    enrichedAnchors,
    "pe_forward",
    DEFAULT_METRICS.pe_forward
  );
  const pbAnchors = buildMetricAnchorDenominators(closePoints, enrichedAnchors, "pb", DEFAULT_METRICS.pb);

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

  const points: SnapshotPoint[] = [];

  for (let i = 0; i < closePoints.length; i += 1) {
    const peTtm = peSeries[i];
    const pb = pbSeries[i];
    let peForward = forwardSeries[i];

    if (!Number.isFinite(peForward) && Number.isFinite(peTtm)) {
      peForward = roundTo(
        clamp((peTtm as number) * forwardScale, -METRIC_MAX.pe_forward, METRIC_MAX.pe_forward),
        4
      );
    }

    if (!Number.isFinite(peTtm) || !Number.isFinite(pb) || !Number.isFinite(peForward)) {
      continue;
    }

    points.push({
      date: closePoints[i].date,
      pe_ttm: peTtm as number,
      pe_forward: peForward as number,
      pb: pb as number,
      us10y_yield: 0,
    });
  }

  return {
    points,
    usedFallback: peAnchors.usedFallback || forwardAnchors.usedFallback || pbAnchors.usedFallback,
  };
}

async function loadPreviousSeriesBySymbol(): Promise<Map<string, PreviousSeries>> {
  const bySymbol = new Map<string, PreviousSeries>();

  try {
    const raw = await readFile(OUTPUT_FILE, "utf8");
    const parsed = JSON.parse(raw) as {
      indices?: Array<{
        symbol?: string;
        forwardStartDate?: string;
        points?: SnapshotPoint[];
      }>;
    };

    for (const item of parsed.indices || []) {
      const symbol = String(item?.symbol || "").trim().toUpperCase();
      const points = Array.isArray(item?.points) ? item.points : [];
      if (!symbol || points.length < 24) continue;

      bySymbol.set(symbol, {
        forwardStartDate: String(item?.forwardStartDate || points[0]?.date || ""),
        points,
      });
    }
  } catch {
    // no previous dataset yet
  }

  return bySymbol;
}

function toCompanyId(symbol: string): string {
  return `company_${symbol.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "")}`;
}

async function main(): Promise<void> {
  const previousSeriesBySymbol = await loadPreviousSeriesBySymbol();

  console.log("[company] loading top 100 companies...");
  const companies = await fetchTopCompanies();
  if (companies.length < 90) {
    throw new Error(`Top company list is too short: ${companies.length}`);
  }

  console.log(`[company] parsed ${companies.length} companies, building series...`);

  let fallbackAnchorCount = 0;
  let skippedCount = 0;
  let reusedPreviousCount = 0;

  const built = await mapLimit(companies, CONCURRENCY, async (company, index) => {
    console.log(`[company] ${String(index + 1).padStart(3, "0")}/${companies.length} ${company.symbol}`);
    const previousSeries = previousSeriesBySymbol.get(company.symbol);

    const [closePoints, stockPayload, longPeSeries, longPbSeries] = await Promise.all([
      fetchCloseHistory(company.symbol, company.slug),
      fetchQuarterlyRatioPayload(company.symbol),
      fetchCompaniesMarketCapMetricSeries(company.slug, "pe-ratio"),
      fetchCompaniesMarketCapMetricSeries(company.slug, "pb-ratio"),
    ]);

    if (!closePoints.length) {
      if (previousSeries) {
        reusedPreviousCount += 1;
        console.warn(`[company] reuse ${company.symbol}: close history unavailable`);
        return {
          id: toCompanyId(company.symbol),
          symbol: company.symbol,
          displayName: company.displayName,
          description: `${company.displayName} (${company.symbol})`,
          rank: company.rank,
          marketCap: company.marketCap,
          forwardStartDate: previousSeries.forwardStartDate || previousSeries.points[0]?.date || "",
          points: previousSeries.points,
        };
      }

      skippedCount += 1;
      console.warn(`[company] skip ${company.symbol}: close history unavailable`);
      return null;
    }

    const ratioPayload = mergeRatioPayloads(stockPayload, longPeSeries, longPbSeries);
    const { points, usedFallback } = buildValuationSeries(closePoints, ratioPayload);
    if (usedFallback) {
      fallbackAnchorCount += 1;
    }

    if (points.length < 24) {
      if (previousSeries) {
        reusedPreviousCount += 1;
        console.warn(`[company] reuse ${company.symbol}: insufficient points (${points.length})`);
        return {
          id: toCompanyId(company.symbol),
          symbol: company.symbol,
          displayName: company.displayName,
          description: `${company.displayName} (${company.symbol})`,
          rank: company.rank,
          marketCap: company.marketCap,
          forwardStartDate: previousSeries.forwardStartDate || previousSeries.points[0]?.date || "",
          points: previousSeries.points,
        };
      }

      skippedCount += 1;
      console.warn(`[company] skip ${company.symbol}: insufficient points (${points.length})`);
      return null;
    }

    return {
      id: toCompanyId(company.symbol),
      symbol: company.symbol,
      displayName: company.displayName,
      description: `${company.displayName} (${company.symbol})`,
      rank: company.rank,
      marketCap: company.marketCap,
      forwardStartDate: points[0]?.date || "",
      points,
    };
  });

  const indices = built.filter(Boolean).sort((a, b) => a.rank - b.rank);

  if (indices.length < 80) {
    throw new Error(`Too few company series generated: ${indices.length}`);
  }

  const dataset = {
    generatedAt: new Date().toISOString(),
    source: [
      "companiesmarketcap-global-toplist",
      "us-listed-symbol-filter",
      "stooq-daily-close",
      "yahoo-chart-close-fallback",
      "nasdaq-historical-close-fallback",
      "companiesmarketcap-close-fallback",
      "companiesmarketcap-pe-ratio",
      "companiesmarketcap-pb-ratio",
      "stockanalysis-quarterly-ratios",
      "step-hold-daily-return-in-anchor-window",
      `fallback-anchor-${fallbackAnchorCount}`,
      `reused-previous-series-${reusedPreviousCount}`,
      `skipped-${skippedCount}`,
      `history-start-${HISTORY_START_DATE}`,
    ].join("+"),
    indices,
  };

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(OUTPUT_FILE, `${JSON.stringify(dataset)}\n`, "utf8");

  console.log(`[company] snapshot written: ${OUTPUT_FILE}`);
  console.log(`[company] generatedAt: ${dataset.generatedAt}`);
  console.log(`[company] series count: ${indices.length}`);
  console.log(`[company] fallback anchors: ${fallbackAnchorCount}`);
  console.log(`[company] reused previous: ${reusedPreviousCount}`);
  console.log(`[company] skipped: ${skippedCount}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
