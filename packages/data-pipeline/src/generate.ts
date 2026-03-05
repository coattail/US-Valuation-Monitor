import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { ALL_INDICES, INDEX_MAP } from "../../core/src/constants.ts";
import type { RawValuationPoint, ValuationDataset } from "../../core/src/types.ts";

const execFileAsync = promisify(execFile);
const CURRENT_FILE = fileURLToPath(import.meta.url);
const DATA_PIPELINE_ROOT = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const SP500_FORWARD_PE_MM_CSV = path.join(
  DATA_PIPELINE_ROOT,
  "data",
  "bootstrap",
  "sp500-forward-pe-macromicro.csv"
);

const INDEX_START_DATE: Record<string, string> = {
  sp500: "1995-01-03",
  nasdaq100: "1999-03-10",
  dow30: "1998-01-02",
  russell2000: "2001-01-03",
  sp400: "2000-01-03",
  us_total_market: "2001-06-15",
  sector_communication: "2018-06-18",
  sector_consumer_discretionary: "1999-01-04",
  sector_consumer_staples: "1999-01-04",
  sector_energy: "1999-01-04",
  sector_financials: "1999-01-04",
  sector_healthcare: "1999-01-04",
  sector_industrials: "1999-01-04",
  sector_materials: "1999-01-04",
  sector_real_estate: "2015-10-08",
  sector_technology: "1999-01-04",
  sector_utilities: "1999-01-04",
};

const BASELINE_BY_INDEX: Record<string, { pe: number; pb: number }> = {
  sp500: { pe: 20.5, pb: 3.9 },
  nasdaq100: { pe: 27.8, pb: 6.8 },
  dow30: { pe: 19.1, pb: 4.2 },
  russell2000: { pe: 22.6, pb: 2.4 },
  sp400: { pe: 19.8, pb: 2.5 },
  us_total_market: { pe: 21.3, pb: 3.7 },
  sector_communication: { pe: 21.7, pb: 4.1 },
  sector_consumer_discretionary: { pe: 23.3, pb: 6.1 },
  sector_consumer_staples: { pe: 20.3, pb: 4.9 },
  sector_energy: { pe: 14.6, pb: 2.0 },
  sector_financials: { pe: 15.1, pb: 1.8 },
  sector_healthcare: { pe: 22.0, pb: 4.5 },
  sector_industrials: { pe: 19.4, pb: 3.6 },
  sector_materials: { pe: 17.1, pb: 2.8 },
  sector_real_estate: { pe: 24.2, pb: 2.3 },
  sector_technology: { pe: 26.5, pb: 7.4 },
  sector_utilities: { pe: 18.2, pb: 2.2 },
};

const MONTH_TO_INDEX: Record<string, number> = {
  jan: 0,
  feb: 1,
  mar: 2,
  apr: 3,
  may: 4,
  jun: 5,
  jul: 6,
  aug: 7,
  sep: 8,
  oct: 9,
  nov: 10,
  dec: 11,
};

const TRENDONIFY_ROUTES: Record<
  string,
  {
    trailing: string[];
    forward: string[];
  }
> = {
  sp500: {
    trailing: [
      "https://trendonify.com/united-states/stock-market/pe-ratio",
      "https://trendonify.com/united-states/stock-market/s-and-p-500/pe-ratio",
    ],
    forward: [
      "https://trendonify.com/united-states/stock-market/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/s-and-p-500/forward-pe-ratio",
    ],
  },
  nasdaq100: {
    trailing: ["https://trendonify.com/united-states/stock-market/nasdaq-100/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/nasdaq-100/forward-pe-ratio"],
  },
  dow30: {
    trailing: ["https://trendonify.com/united-states/stock-market/dow-jones/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/dow-jones/forward-pe-ratio"],
  },
  russell2000: {
    trailing: ["https://trendonify.com/united-states/stock-market/russell-2000/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/russell-2000/forward-pe-ratio"],
  },
  sp400: {
    trailing: [
      "https://trendonify.com/united-states/stock-market/s-and-p-midcap-400/pe-ratio",
      "https://trendonify.com/united-states/stock-market/sp-midcap-400/pe-ratio",
      "https://trendonify.com/united-states/stock-market/midcap-400/pe-ratio",
    ],
    forward: [
      "https://trendonify.com/united-states/stock-market/s-and-p-midcap-400/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/sp-midcap-400/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/midcap-400/forward-pe-ratio",
    ],
  },
  us_total_market: {
    trailing: [
      "https://trendonify.com/united-states/stock-market/wilshire-5000/pe-ratio",
      "https://trendonify.com/united-states/stock-market/us-total-stock-market/pe-ratio",
      "https://trendonify.com/united-states/stock-market/total-stock-market/pe-ratio",
    ],
    forward: [
      "https://trendonify.com/united-states/stock-market/wilshire-5000/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/us-total-stock-market/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/total-stock-market/forward-pe-ratio",
    ],
  },
  sector_communication: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-communication-services/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-communication-services/forward-pe-ratio"],
  },
  sector_consumer_discretionary: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-consumer-discretionary/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-consumer-discretionary/forward-pe-ratio"],
  },
  sector_consumer_staples: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-consumer-staples/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-consumer-staples/forward-pe-ratio"],
  },
  sector_energy: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-energy/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-energy/forward-pe-ratio"],
  },
  sector_financials: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-financials/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-financials/forward-pe-ratio"],
  },
  sector_healthcare: {
    trailing: [
      "https://trendonify.com/united-states/stock-market/sp-500-health-care/pe-ratio",
      "https://trendonify.com/united-states/stock-market/sp-500-healthcare/pe-ratio",
    ],
    forward: [
      "https://trendonify.com/united-states/stock-market/sp-500-health-care/forward-pe-ratio",
      "https://trendonify.com/united-states/stock-market/sp-500-healthcare/forward-pe-ratio",
    ],
  },
  sector_industrials: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-industrials/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-industrials/forward-pe-ratio"],
  },
  sector_materials: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-materials/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-materials/forward-pe-ratio"],
  },
  sector_real_estate: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-real-estate/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-real-estate/forward-pe-ratio"],
  },
  sector_technology: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-information-technology/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-information-technology/forward-pe-ratio"],
  },
  sector_utilities: {
    trailing: ["https://trendonify.com/united-states/stock-market/sp-500-utilities/pe-ratio"],
    forward: ["https://trendonify.com/united-states/stock-market/sp-500-utilities/forward-pe-ratio"],
  },
};

const SIBLIS_ROUTES: Partial<
  Record<
    string,
    {
      url: string;
    }
  >
> = {
  nasdaq100: {
    url: "https://siblisresearch.com/data/nasdaq-100-pe-ratio/",
  },
  russell2000: {
    url: "https://siblisresearch.com/data/russell-2000-pe-yield/",
  },
};

const MACROMICRO_CHART_IDS: Partial<
  Record<
    string,
    {
      trailing?: number[];
      forward?: number[];
    }
  >
> = {
  sp500: { trailing: [1633], forward: [20052] },
  nasdaq100: { trailing: [1637], forward: [23955, 15115] },
};

const MACROMICRO_SERIES_ROUTES: Partial<
  Record<
    string,
    {
      trailing?: string[];
      forward?: string[];
    }
  >
> = {
  sp500: {
    forward: [
      "https://en.macromicro.me/series/20052/sp500-forward-pe-ratio",
      "https://en.macromicro.me/series/20052/us-sp500-forward-pe-ratio",
    ],
  },
};

const RECENT_OVERRIDE_INDEX_IDS = new Set(["russell2000"]);
const FORWARD_LOCKED_INDEX_IDS = new Set(["nasdaq100"]);
const SIBLIS_FULL_HISTORY_INDEX_IDS = new Set(["nasdaq100"]);
const TRENDONIFY_TRAILING_PRIMARY_INDEX_IDS = new Set(["nasdaq100"]);

const CURATED_WSJ_TTM_REFERENCES: Partial<Record<string, Array<{ date: string; value: number }>>> = {
  nasdaq100: [{ date: "2026-02-20", value: 31.62 }],
};

const WSJ_PEYIELD_URLS = [
  "https://online.wsj.com/mdc/public/page/2_3021-peyield.html",
  "https://www.wsj.com/mdc/public/page/2_3021-peyield.html",
  "https://www.wsj.com/market-data/stocks/peyields",
  "https://r.jina.ai/http://online.wsj.com/mdc/public/page/2_3021-peyield.html",
  "https://r.jina.ai/http://www.wsj.com/market-data/stocks/peyields",
];

const WSJ_ROW_KEYWORDS: Record<string, string[]> = {
  sp500: ["s&p500", "sp500"],
  nasdaq100: ["nasdaq100", "nasdaq-100"],
  russell2000: ["russell2000"],
};

const NASDAQ100_FORWARD_MM_BOOTSTRAP: Array<{ date: string; value: number }> = [
  { date: "2000-01-31", value: 95.92 },
  { date: "2000-02-29", value: 98.98 },
  { date: "2000-03-31", value: 98.47 },
  { date: "2000-04-30", value: 93.35 },
  { date: "2000-05-31", value: 81.95 },
  { date: "2000-06-30", value: 71.31 },
  { date: "2000-07-31", value: 67.78 },
  { date: "2000-08-31", value: 72.42 },
  { date: "2000-09-30", value: 58.67 },
  { date: "2000-10-31", value: 52.44 },
  { date: "2000-11-30", value: 57.78 },
  { date: "2000-12-31", value: 65.72 },
  { date: "2001-01-31", value: 67.95 },
  { date: "2001-02-28", value: 56.53 },
  { date: "2001-03-31", value: 42.89 },
  { date: "2001-04-30", value: 32.5 },
  { date: "2001-05-31", value: 34.05 },
  { date: "2001-06-30", value: 37.95 },
  { date: "2001-07-31", value: 36.95 },
  { date: "2001-08-31", value: 38.64 },
  { date: "2001-09-30", value: 29.61 },
  { date: "2001-10-31", value: 29.66 },
  { date: "2001-11-30", value: 33.88 },
  { date: "2001-12-31", value: 37.58 },
  { date: "2002-01-31", value: 35.87 },
  { date: "2002-02-28", value: 34.3 },
  { date: "2002-03-31", value: 29.53 },
  { date: "2002-04-30", value: 31.54 },
  { date: "2002-05-31", value: 29.16 },
  { date: "2002-06-30", value: 32.44 },
  { date: "2002-07-31", value: 27.66 },
  { date: "2002-08-31", value: 24.8 },
  { date: "2002-09-30", value: 17.97 },
  { date: "2002-10-31", value: 18.95 },
  { date: "2002-11-30", value: 26.28 },
  { date: "2002-12-31", value: 34.12 },
];

// Nasdaq article cites FactSet index-level points: 1999-12-31 ~104x, 2000-12-29 ~113x,
// and indicates 1Q00 peak likely in 150-200x zone; we pin the lower bound for stability.
const NASDAQ100_TTM_BUBBLE_FACTSET_BOOTSTRAP: Array<{ date: string; value: number }> = [
  { date: "1999-12-31", value: 104.0 },
  { date: "2000-03-31", value: 150.0 },
  { date: "2000-12-31", value: 113.0 },
];

interface ClosePoint {
  date: string;
  close: number;
}

interface YieldPoint {
  date: string;
  value: number;
}

interface MonthlyMetricPoint {
  date: string;
  value: number;
  ts: number;
}

interface BuildSeriesOptions {
  anchorPe: number;
  anchorForwardPe: number;
  anchorPb: number;
  yields: YieldPoint[];
  peSmoothingAlpha?: number;
  forwardSmoothingAlpha?: number;
  pbSmoothingAlpha?: number;
}

interface GenerateDatasetOptions {
  previousDataset?: ValuationDataset | null;
}

interface LatestPeSnapshot {
  trailing?: number;
  forward?: number;
}

type PeMetricKey = "pe_ttm" | "pe_forward";

class ReliableSourceError extends Error {}

function parseDate(dateText: string): Date {
  return new Date(`${dateText}T00:00:00Z`);
}

function formatDate(input: Date): string {
  return input.toISOString().slice(0, 10);
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function roundTo(value: number, digits = 4): number {
  const ratio = 10 ** digits;
  return Math.round(value * ratio) / ratio;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

function parseCsv(csvText: string): string[][] {
  return csvText
    .replace(/\r/g, "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split(","));
}

async function loadLocalMetricSeriesFromCsv(
  filePath: string,
  minValue: number,
  maxValue: number
): Promise<MonthlyMetricPoint[] | undefined> {
  try {
    const text = await readFile(filePath, "utf8");
    const rows = parseCsv(text);
    const byDate = new Map<string, number>();

    for (const row of rows) {
      const date = String(row[0] || "").trim();
      if (date.toLowerCase() === "date") continue;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;

      const value = parseNumericText(String(row[1] || ""));
      if (!Number.isFinite(value) || value <= minValue || value >= maxValue) continue;
      byDate.set(date, Number(value));
    }

    if (!byDate.size) return undefined;

    return [...byDate.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
  } catch {
    return undefined;
  }
}

async function curlGet(url: string, timeoutMs = 25000, extraHeaders: Record<string, string> = {}): Promise<string> {
  const timeoutSec = Math.max(8, Math.ceil(timeoutMs / 1000));
  const args = ["-sS", "-L", "--max-time", String(timeoutSec), "--connect-timeout", "8"];

  for (const [key, value] of Object.entries(extraHeaders)) {
    args.push("-H", `${key}: ${value}`);
  }

  args.push(url);

  const clearProxyEnv: NodeJS.ProcessEnv = { ...process.env };
  delete clearProxyEnv.HTTP_PROXY;
  delete clearProxyEnv.HTTPS_PROXY;
  delete clearProxyEnv.ALL_PROXY;
  delete clearProxyEnv.http_proxy;
  delete clearProxyEnv.https_proxy;
  delete clearProxyEnv.all_proxy;
  clearProxyEnv.NO_PROXY = "*";
  clearProxyEnv.no_proxy = "*";

  try {
    const { stdout } = await execFileAsync("curl", args, {
      maxBuffer: 32 * 1024 * 1024,
    });
    return stdout;
  } catch (error) {
    try {
      const { stdout } = await execFileAsync("curl", args, {
        maxBuffer: 32 * 1024 * 1024,
        env: clearProxyEnv,
      });
      return stdout;
    } catch (retryError) {
      const fallbackMessage = retryError instanceof Error ? retryError.message : "curl failed";
      const stderr =
        typeof retryError === "object" && retryError && "stderr" in retryError
          ? String((retryError as { stderr?: string }).stderr || "")
          : "";
      throw new Error(`${fallbackMessage}${stderr ? ` | ${stderr.trim()}` : ""}`);
    }
  }
}

function monthYearToIsoDate(monthRaw: string, yearRaw: string): string | undefined {
  const year = Number(yearRaw);
  if (!Number.isFinite(year) || year < 1800 || year > 2200) return undefined;

  const monthKey = String(monthRaw || "").slice(0, 3).toLowerCase();
  const monthIndex = MONTH_TO_INDEX[monthKey];
  if (monthIndex === undefined) return undefined;

  const monthEnd = new Date(Date.UTC(year, monthIndex + 1, 0));
  return monthEnd.toISOString().slice(0, 10);
}

function monthDayYearToIsoDate(monthRaw: string, dayRaw: string, yearRaw: string): string | undefined {
  const year = Number(yearRaw);
  const day = Number(dayRaw);
  if (!Number.isFinite(year) || year < 1800 || year > 2200) return undefined;
  if (!Number.isFinite(day) || day < 1 || day > 31) return undefined;

  const monthKey = String(monthRaw || "").slice(0, 3).toLowerCase();
  const monthIndex = MONTH_TO_INDEX[monthKey];
  if (monthIndex === undefined) return undefined;

  const date = new Date(Date.UTC(year, monthIndex, day));
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toISOString().slice(0, 10);
}

function mmddyyyyToIsoDate(dateText: string): string | undefined {
  const match = String(dateText || "").trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) return undefined;
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  if (!Number.isFinite(month) || month < 1 || month > 12) return undefined;
  if (!Number.isFinite(day) || day < 1 || day > 31) return undefined;
  if (!Number.isFinite(year) || year < 1800 || year > 2200) return undefined;

  const date = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toISOString().slice(0, 10);
}

function stripHtmlText(raw: string): string {
  return String(raw || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function parseNumericText(raw: string): number | undefined {
  const normalized = String(raw || "")
    .replace(/,/g, "")
    .replace(/[^0-9.+-]/g, "");
  if (!normalized) return undefined;
  const value = Number(normalized);
  if (!Number.isFinite(value)) return undefined;
  return value;
}

function normalizeLookupText(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/&amp;/g, "&")
    .replace(/[^a-z0-9&]+/g, "");
}

function parseWsjRowPeValues(cells: string[]): LatestPeSnapshot | undefined {
  const values: number[] = [];
  for (const cell of cells) {
    const matches = String(cell || "").match(/[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?/g) || [];
    for (const raw of matches) {
      const value = Number(raw.replace(/,/g, ""));
      if (!Number.isFinite(value)) continue;
      if (value < 4 || value > 120) continue;
      values.push(value);
    }
  }

  if (values.length < 2) return undefined;
  const trailing = values[0];
  const forwardCandidates = values.slice(1).filter((value) => value < trailing);
  const forward = forwardCandidates.length ? Math.min(...forwardCandidates) : Math.min(...values.slice(1));
  if (!Number.isFinite(trailing) || !Number.isFinite(forward)) return undefined;
  return {
    trailing,
    forward,
  };
}

function parseWsjPeSnapshotFromHtml(html: string): Map<string, LatestPeSnapshot> {
  const result = new Map<string, LatestPeSnapshot>();
  const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch: RegExpExecArray | null;

  while ((rowMatch = rowRegex.exec(html)) !== null) {
    const rowHtml = rowMatch[1];
    const cells = [...rowHtml.matchAll(/<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/gi)].map((item) =>
      stripHtmlText(item[1])
    );
    if (!cells.length) continue;
    const rowKey = normalizeLookupText(cells.join(" "));
    if (!rowKey) continue;

    for (const [indexId, keywords] of Object.entries(WSJ_ROW_KEYWORDS)) {
      if (result.has(indexId)) continue;
      if (!keywords.some((keyword) => rowKey.includes(normalizeLookupText(keyword)))) continue;
      const values = parseWsjRowPeValues(cells);
      if (values) result.set(indexId, values);
    }
  }

  return result;
}

function parseWsjPeSnapshotFromText(text: string): Map<string, LatestPeSnapshot> {
  const result = new Map<string, LatestPeSnapshot>();
  const lines = String(text || "")
    .replace(/\r/g, "\n")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const lineKey = normalizeLookupText(line);
    if (!lineKey) continue;

    for (const [indexId, keywords] of Object.entries(WSJ_ROW_KEYWORDS)) {
      if (result.has(indexId)) continue;
      if (!keywords.some((keyword) => lineKey.includes(normalizeLookupText(keyword)))) continue;
      const values = parseWsjRowPeValues([line]);
      if (values) result.set(indexId, values);
    }
  }

  return result;
}

function mergeLatestPeSnapshotMaps(primary: Map<string, LatestPeSnapshot>, secondary: Map<string, LatestPeSnapshot>): Map<string, LatestPeSnapshot> {
  const merged = new Map<string, LatestPeSnapshot>(secondary);
  for (const [key, value] of primary.entries()) {
    merged.set(key, value);
  }
  return merged;
}

async function fetchWsjPeSnapshot(): Promise<Map<string, LatestPeSnapshot>> {
  let combined = new Map<string, LatestPeSnapshot>();

  for (const url of WSJ_PEYIELD_URLS) {
    try {
      const body = await curlGet(url, 28000, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        Accept: "text/html,application/xhtml+xml,text/plain",
      });
      const htmlParsed = parseWsjPeSnapshotFromHtml(body);
      const textParsed = parseWsjPeSnapshotFromText(stripHtmlText(body));
      const merged = mergeLatestPeSnapshotMaps(htmlParsed, textParsed);
      if (merged.size > 0) {
        combined = mergeLatestPeSnapshotMaps(merged, combined);
      }
      if (combined.size >= 3) break;
    } catch {
      // try next source
    }
  }

  return combined;
}

function parseSiblisSeries(
  html: string
): {
  trailing: MonthlyMetricPoint[];
  forward: MonthlyMetricPoint[];
} {
  const trailingByDate = new Map<string, number>();
  const forwardByDate = new Map<string, number>();

  const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch: RegExpExecArray | null;
  while ((rowMatch = rowRegex.exec(html)) !== null) {
    const rowHtml = rowMatch[1];
    const rawCells = [...rowHtml.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripHtmlText(cell[1]));
    if (rawCells.length < 5) continue;

    const date = mmddyyyyToIsoDate(rawCells[0]);
    if (!date) continue;

    const trailing = parseNumericText(rawCells[2]);
    const forward = parseNumericText(rawCells[4]);

    if (Number.isFinite(trailing) && Number(trailing) > 0 && Number(trailing) <= 250) {
      trailingByDate.set(date, Number(trailing));
    }
    if (Number.isFinite(forward) && Number(forward) > 0 && Number(forward) <= 250) {
      forwardByDate.set(date, Number(forward));
    }
  }

  const toSeries = (map: Map<string, number>): MonthlyMetricPoint[] =>
    [...map.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));

  return {
    trailing: toSeries(trailingByDate),
    forward: toSeries(forwardByDate),
  };
}

async function fetchSiblisSeries(
  url: string
): Promise<
  | {
      trailing?: MonthlyMetricPoint[];
      forward?: MonthlyMetricPoint[];
    }
  | undefined
> {
  const html = await curlGet(url, 28000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const parsed = parseSiblisSeries(html);
  const result: { trailing?: MonthlyMetricPoint[]; forward?: MonthlyMetricPoint[] } = {};
  if (parsed.trailing.length >= 4) {
    result.trailing = parsed.trailing;
  }
  if (parsed.forward.length >= 4) {
    result.forward = parsed.forward;
  }
  return Object.keys(result).length ? result : undefined;
}

function parseTrendonifyMonthlySeries(html: string): MonthlyMetricPoint[] {
  const byDate = new Map<string, number>();
  const tableRegex =
    /<td[^>]*>\s*([A-Za-z]{3,9})\s+([12][0-9]{3})\s*<\/td>\s*<td[^>]*>\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*<\/td>/gi;

  let tableMatch: RegExpExecArray | null;
  while ((tableMatch = tableRegex.exec(html)) !== null) {
    const isoDate = monthYearToIsoDate(tableMatch[1], tableMatch[2]);
    if (!isoDate) continue;
    const value = Number(tableMatch[3].replace(/,/g, ""));
    if (!Number.isFinite(value) || value <= 0 || value > 250) continue;
    if (!byDate.has(isoDate)) byDate.set(isoDate, value);
  }

  if (!byDate.size) {
    const plain = html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<\/(p|div|li|tr|h[1-6]|table|section)>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/\r/g, " ")
      .replace(/[ \t]+/g, " ")
      .replace(/\n+/g, "\n");

    const lineRegex =
      /\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+([12][0-9]{3})\s*\|?\s*([0-9]+(?:\.[0-9]+)?)/gi;

    let lineMatch: RegExpExecArray | null;
    while ((lineMatch = lineRegex.exec(plain)) !== null) {
      const isoDate = monthYearToIsoDate(lineMatch[1], lineMatch[2]);
      if (!isoDate) continue;
      const value = Number(lineMatch[3]);
      if (!Number.isFinite(value) || value <= 0 || value > 250) continue;
      if (!byDate.has(isoDate)) byDate.set(isoDate, value);
    }
  }

  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
}

async function fetchTrendonifySeries(urls: string[]): Promise<MonthlyMetricPoint[] | undefined> {
  const expandedUrls: string[] = [];
  for (const rawUrl of urls) {
    const url = rawUrl.trim();
    if (!url) continue;
    expandedUrls.push(url);
    if (/^https?:\/\//i.test(url)) {
      expandedUrls.push(`https://r.jina.ai/http://${url.replace(/^https?:\/\//i, "")}`);
    }
  }

  const visited = new Set<string>();
  for (const rawUrl of expandedUrls) {
    const url = rawUrl.trim();
    if (!url || visited.has(url)) continue;
    visited.add(url);
    try {
      const html = await curlGet(url, 28000, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        Accept: "text/html,application/xhtml+xml",
      });
      const series = parseTrendonifyMonthlySeries(html);
      if (series.length >= 12) {
        return series;
      }
    } catch {
      // try next url
    }
  }
  return undefined;
}

async function fetchMultplSp500PeSeries(): Promise<MonthlyMetricPoint[] | undefined> {
  const html = await curlGet("https://www.multpl.com/s-p-500-pe-ratio/table/by-month", 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const byDate = new Map<string, number>();
  const rowRegex =
    /<tr[^>]*>\s*<td>\s*([A-Za-z]{3,9})\s+([0-9]{1,2}),\s*([12][0-9]{3})\s*<\/td>\s*<td>[\s\S]*?([0-9]+(?:\.[0-9]+)?)\s*<\/td>\s*<\/tr>/gi;
  let match: RegExpExecArray | null;
  while ((match = rowRegex.exec(html)) !== null) {
    const isoDate = monthDayYearToIsoDate(match[1], match[2], match[3]);
    if (!isoDate) continue;
    const value = Number(match[4]);
    if (!Number.isFinite(value) || value <= 0 || value > 250) continue;
    if (!byDate.has(isoDate)) byDate.set(isoDate, value);
  }

  if (!byDate.size) return undefined;
  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
}

function normalizeMacroMicroMonthlyDate(dateText: string): string | undefined {
  const raw = String(dateText || "").trim();
  if (!raw) return undefined;

  let normalized = "";
  if (/^\d{13}$/.test(raw)) {
    const ts = Number(raw);
    if (!Number.isFinite(ts)) return undefined;
    const date = new Date(ts);
    if (Number.isNaN(date.getTime())) return undefined;
    normalized = formatDate(date);
  } else if (/^\d{10}$/.test(raw)) {
    const ts = Number(raw) * 1000;
    if (!Number.isFinite(ts)) return undefined;
    const date = new Date(ts);
    if (Number.isNaN(date.getTime())) return undefined;
    normalized = formatDate(date);
  } else if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    normalized = raw;
  } else if (/^\d{4}-\d{2}$/.test(raw)) {
    normalized = `${raw}-01`;
  } else if (/^\d{4}\/\d{2}\/\d{2}$/.test(raw)) {
    normalized = raw.replace(/\//g, "-");
  } else if (/^\d{4}\/\d{2}$/.test(raw)) {
    normalized = `${raw.replace(/\//g, "-")}-01`;
  } else if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(raw)) {
    const asIso = mmddyyyyToIsoDate(raw);
    if (!asIso) return undefined;
    normalized = asIso;
  } else {
    const leadingIso = raw.slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(leadingIso)) {
      normalized = leadingIso;
    } else {
      return undefined;
    }
  }

  const parsed = parseDate(normalized);
  if (Number.isNaN(parsed.getTime())) return undefined;
  const monthEnd = new Date(Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth() + 1, 0));
  return formatDate(monthEnd);
}

function parseMacroMicroChartSeries(body: string): MonthlyMetricPoint[] {
  const text = String(body || "").trim();
  if (!text) return [];

  const parsePayload = (raw: string): MonthlyMetricPoint[] => {
    const payload = JSON.parse(raw) as
      | Array<{ Date?: string; Value?: string | number }>
      | {
          data?: Array<{ Date?: string; Value?: string | number }>;
          returndata?: {
            data?: Array<{ Date?: string; Value?: string | number }>;
            rows?: Array<{ Date?: string; Value?: string | number }>;
          };
        };
    const rows = Array.isArray(payload)
      ? payload
      : Array.isArray(payload?.data)
        ? payload.data
        : Array.isArray(payload?.returndata?.data)
          ? payload.returndata.data
          : Array.isArray(payload?.returndata?.rows)
            ? payload.returndata.rows
            : [];
    const byDate = new Map<string, number>();

    for (const row of rows || []) {
      const date = normalizeMacroMicroMonthlyDate(String(row?.Date || ""));
      const value = parseNumericText(String(row?.Value ?? ""));
      if (!date) continue;
      if (!Number.isFinite(value) || value <= 0 || value > 250) continue;
      byDate.set(date, Number(value));
    }

    return [...byDate.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
  };

  try {
    return parsePayload(text);
  } catch {
    const jsonMatch = text.match(/\[\s*\{[\s\S]*\}\s*\]/);
    if (!jsonMatch) return [];
    try {
      return parsePayload(jsonMatch[0]);
    } catch {
      return [];
    }
  }
}

function parseMacroMicroSeriesPage(body: string): MonthlyMetricPoint[] {
  const text = String(body || "");
  if (!text) return [];
  const byDate = new Map<string, number>();

  const upsert = (rawDate: string, rawValue: string | number): void => {
    const date = normalizeMacroMicroMonthlyDate(String(rawDate || ""));
    const value = parseNumericText(String(rawValue ?? ""));
    if (!date) return;
    if (!Number.isFinite(value) || value <= 0 || value > 250) return;
    byDate.set(date, Number(value));
  };

  const kvRegex =
    /"(?:Date|date)"\s*:\s*"([^"]+)"[\s\S]{0,180}?"(?:Value|value)"\s*:\s*("?[-+]?[0-9]+(?:\.[0-9]+)?"?)/g;
  let kvMatch: RegExpExecArray | null;
  while ((kvMatch = kvRegex.exec(text)) !== null) {
    upsert(kvMatch[1], kvMatch[2]);
  }

  const tsPairRegex = /\[(\d{10,13})\s*,\s*([-+]?[0-9]+(?:\.[0-9]+)?)\]/g;
  let tsPairMatch: RegExpExecArray | null;
  while ((tsPairMatch = tsPairRegex.exec(text)) !== null) {
    upsert(tsPairMatch[1], tsPairMatch[2]);
  }

  const isoPairRegex = /\["?([12]\d{3}-\d{2}(?:-\d{2})?)"?\s*,\s*([-+]?[0-9]+(?:\.[0-9]+)?)\]/g;
  let isoPairMatch: RegExpExecArray | null;
  while ((isoPairMatch = isoPairRegex.exec(text)) !== null) {
    upsert(isoPairMatch[1], isoPairMatch[2]);
  }

  if (byDate.size >= 12) {
    return [...byDate.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
  }

  const tableCellRegex =
    /<td[^>]*>\s*([A-Za-z]{3,9}\s+[12]\d{3}|\d{4}-\d{2}(?:-\d{2})?)\s*<\/td>\s*<td[^>]*>\s*([-+]?[0-9]+(?:\.[0-9]+)?)\s*<\/td>/gi;
  let cellMatch: RegExpExecArray | null;
  while ((cellMatch = tableCellRegex.exec(text)) !== null) {
    const rawDate = cellMatch[1];
    const parsedMonthYear = monthYearToIsoDate(rawDate.split(/\s+/)[0], rawDate.split(/\s+/)[1]);
    upsert(parsedMonthYear || rawDate, cellMatch[2]);
  }

  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
}

async function fetchMacroMicroChartSeries(chartId: number): Promise<MonthlyMetricPoint[] | undefined> {
  const sources = [
    `https://en.macromicro.me/api/v1/chart-data?id=${chartId}`,
    `https://r.jina.ai/http://en.macromicro.me/api/v1/chart-data?id=${chartId}`,
  ];

  for (const url of sources) {
    try {
      const body = await curlGet(url, 25000, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        Accept: "application/json,text/plain,*/*",
      });
      const series = parseMacroMicroChartSeries(body);
      if (series.length >= 12) return series;
    } catch {
      // try next source
    }
  }

  return undefined;
}

async function fetchMacroMicroSeriesByPageRoutes(urls: string[]): Promise<MonthlyMetricPoint[] | undefined> {
  const expandedUrls: string[] = [];
  for (const rawUrl of urls) {
    const url = rawUrl.trim();
    if (!url) continue;
    expandedUrls.push(url);
    if (/^https?:\/\//i.test(url)) {
      expandedUrls.push(`https://r.jina.ai/http://${url.replace(/^https?:\/\//i, "")}`);
    }
  }

  const visited = new Set<string>();
  for (const rawUrl of expandedUrls) {
    const url = rawUrl.trim();
    if (!url || visited.has(url)) continue;
    visited.add(url);
    try {
      const body = await curlGet(url, 28000, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        Accept: "text/html,application/xhtml+xml,application/json,text/plain,*/*",
      });

      const apiLikeSeries = parseMacroMicroChartSeries(body);
      const pageSeries = parseMacroMicroSeriesPage(body);
      const merged =
        apiLikeSeries.length && pageSeries.length
          ? mergeMonthlySeries(apiLikeSeries, pageSeries)
          : apiLikeSeries.length
            ? apiLikeSeries
            : pageSeries;
      if (merged.length >= 12) return merged;
    } catch {
      // try next source
    }
  }

  return undefined;
}

function buildBootstrapSeries(points: Array<{ date: string; value: number }>): MonthlyMetricPoint[] {
  return points
    .filter(
      (point) =>
        /^\d{4}-\d{2}-\d{2}$/.test(point.date || "") &&
        Number.isFinite(point.value) &&
        point.value > 0 &&
        point.value < 250
    )
    .map((point) => ({ date: point.date, value: point.value, ts: parseDate(point.date).getTime() }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function mergeMonthlySeries(primary: MonthlyMetricPoint[], secondary: MonthlyMetricPoint[]): MonthlyMetricPoint[] {
  const byDate = new Map<string, number>();
  for (const point of secondary) {
    byDate.set(point.date, point.value);
  }
  for (const point of primary) {
    byDate.set(point.date, point.value);
  }
  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
}

function mergeMonthlySeriesRecentOverride(
  baseSeries: MonthlyMetricPoint[] | undefined,
  overlaySeries: MonthlyMetricPoint[] | undefined,
  recentDays = 720
): MonthlyMetricPoint[] | undefined {
  if (!overlaySeries?.length) return baseSeries;
  if (!baseSeries?.length) return overlaySeries;

  const latestBaseTs = baseSeries[baseSeries.length - 1]?.ts;
  if (!Number.isFinite(latestBaseTs)) return baseSeries;
  const cutoffTs = Number(latestBaseTs) - recentDays * 24 * 60 * 60 * 1000;
  const recentOverlay = overlaySeries.filter((point) => point.ts >= cutoffTs);
  if (!recentOverlay.length) return baseSeries;

  return mergeMonthlySeries(recentOverlay, baseSeries);
}

function isSeriesStale(
  series: MonthlyMetricPoint[] | undefined,
  effectiveEndDate: string,
  maxLagDays = 50
): boolean {
  if (!series?.length) return true;
  const latest = series[series.length - 1];
  if (!latest?.date) return true;
  const latestTs = latest.ts;
  const targetTs = parseDate(effectiveEndDate).getTime();
  if (!Number.isFinite(latestTs) || !Number.isFinite(targetTs)) return true;
  return targetTs - latestTs > maxLagDays * 24 * 60 * 60 * 1000;
}

async function fetchMacroMicroSeriesByCandidates(chartIds: number[] | undefined): Promise<MonthlyMetricPoint[] | undefined> {
  if (!chartIds?.length) return undefined;

  for (const chartId of chartIds) {
    try {
      const series = await fetchMacroMicroChartSeries(chartId);
      if (series?.length) return series;
    } catch {
      // try next chart id
    }
  }
  return undefined;
}

function upsertSeriesValueAtDate(
  series: MonthlyMetricPoint[] | undefined,
  date: string,
  value: number
): MonthlyMetricPoint[] {
  const byDate = new Map<string, number>();
  for (const point of series || []) {
    byDate.set(point.date, point.value);
  }
  byDate.set(date, value);

  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([seriesDate, seriesValue]) => ({ date: seriesDate, value: seriesValue, ts: parseDate(seriesDate).getTime() }));
}

function pickCuratedWsjTtmReference(
  indexId: string,
  effectiveEndDate: string,
  maxAgeDays = 45
): { date: string; value: number } | undefined {
  const list = CURATED_WSJ_TTM_REFERENCES[indexId];
  if (!list?.length) return undefined;

  const targetTs = parseDate(effectiveEndDate).getTime();
  if (!Number.isFinite(targetTs)) return undefined;

  const candidates = list
    .filter((item) => /^\d{4}-\d{2}-\d{2}$/.test(item.date || "") && Number.isFinite(item.value) && item.value > 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  if (!candidates.length) return undefined;

  const chosen = [...candidates].reverse().find((item) => item.date <= effectiveEndDate);
  if (!chosen) return undefined;
  const chosenTs = parseDate(chosen.date).getTime();
  if (!Number.isFinite(chosenTs)) return undefined;

  const ageDays = (targetTs - chosenTs) / (24 * 60 * 60 * 1000);
  if (ageDays > maxAgeDays) return undefined;
  return chosen;
}

function scaleSeriesByFactor(
  series: MonthlyMetricPoint[] | undefined,
  factor: number,
  minValue = 2.4,
  maxValue = 140
): MonthlyMetricPoint[] | undefined {
  if (!series?.length) return series;
  if (!Number.isFinite(factor) || factor <= 0) return series;

  return series.map((point) => ({
    ...point,
    value: clamp(point.value * factor, minValue, maxValue),
  }));
}

function interpolateMonthlyMetric(
  date: string,
  series: MonthlyMetricPoint[],
  maxInterpolationSpanDays = Number.POSITIVE_INFINITY,
  maxForwardFillDays = Number.POSITIVE_INFINITY
): number | undefined {
  if (!series.length) return undefined;
  const targetTs = parseDate(date).getTime();

  if (targetTs < series[0].ts) return undefined;
  if (targetTs >= series[series.length - 1].ts) {
    const lagDays = (targetTs - series[series.length - 1].ts) / (24 * 60 * 60 * 1000);
    if (lagDays > maxForwardFillDays) return undefined;
    return series[series.length - 1].value;
  }

  let lo = 0;
  let hi = series.length - 1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const ts = series[mid].ts;
    if (ts === targetTs) return series[mid].value;
    if (ts < targetTs) lo = mid + 1;
    else hi = mid - 1;
  }

  const left = series[Math.max(0, hi)];
  const right = series[Math.min(series.length - 1, lo)];
  if (!left || !right) return undefined;
  const span = right.ts - left.ts;
  if (span <= 0) return left.value;
  const spanDays = span / (24 * 60 * 60 * 1000);
  if (spanDays > maxInterpolationSpanDays) return undefined;

  const ratio = (targetTs - left.ts) / span;
  return left.value + (right.value - left.value) * ratio;
}

function interpolateSeriesValueAtTs(series: MonthlyMetricPoint[], targetTs: number): number | undefined {
  if (!series.length || !Number.isFinite(targetTs)) return undefined;
  if (targetTs <= series[0].ts) return series[0].value;
  if (targetTs >= series[series.length - 1].ts) return series[series.length - 1].value;

  let lo = 0;
  let hi = series.length - 1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const ts = series[mid].ts;
    if (ts === targetTs) return series[mid].value;
    if (ts < targetTs) lo = mid + 1;
    else hi = mid - 1;
  }

  const left = series[Math.max(0, hi)];
  const right = series[Math.min(series.length - 1, lo)];
  if (!left || !right) return undefined;
  const span = right.ts - left.ts;
  if (span <= 0) return left.value;
  const ratio = (targetTs - left.ts) / span;
  return left.value + (right.value - left.value) * ratio;
}

function sanitizeMonthlySeries(
  series: MonthlyMetricPoint[] | undefined,
  minValue: number,
  maxValue: number
): MonthlyMetricPoint[] {
  if (!series?.length) return [];
  const byDate = new Map<string, number>();
  for (const point of series) {
    if (!point?.date || !/^\d{4}-\d{2}-\d{2}$/.test(point.date)) continue;
    if (!Number.isFinite(point.value)) continue;
    const value = Number(point.value);
    if (value < minValue || value > maxValue) continue;
    byDate.set(point.date, value);
  }

  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, value]) => ({ date, value, ts: parseDate(date).getTime() }));
}

function buildAnchoredOverride(
  points: RawValuationPoint[],
  metric: PeMetricKey,
  rawAnchorSeries: MonthlyMetricPoint[] | undefined,
  options: {
    minValue: number;
    maxValue: number;
    maxInterpolationSpanDays: number;
    maxForwardFillDays: number;
    maxBackFillDays: number;
    minScale: number;
    maxScale: number;
  }
): number[] | undefined {
  const anchors = sanitizeMonthlySeries(rawAnchorSeries, options.minValue, options.maxValue);
  if (!anchors.length || !points.length) return undefined;

  const dayMs = 24 * 60 * 60 * 1000;
  const proxySeries: MonthlyMetricPoint[] = points
    .map((point) => ({
      date: point.date,
      value: Number(point[metric]),
      ts: parseDate(point.date).getTime(),
    }))
    .filter((point) => Number.isFinite(point.value) && point.value > 0);

  if (!proxySeries.length) return undefined;

  const scales = anchors.map((anchor) => {
    const proxyAtAnchor = interpolateSeriesValueAtTs(proxySeries, anchor.ts);
    if (!Number.isFinite(proxyAtAnchor) || Number(proxyAtAnchor) <= 0) return undefined;
    return clamp(anchor.value / Number(proxyAtAnchor), options.minScale, options.maxScale);
  });

  const lastAnchorIndex = anchors.length - 1;
  const result = new Array<number>(points.length);
  let anchorCursor = 0;

  for (let index = 0; index < points.length; index += 1) {
    const point = points[index];
    const base = Number(point[metric]);
    const ts = parseDate(point.date).getTime();

    if (!Number.isFinite(base) || base <= 0 || !Number.isFinite(ts)) {
      result[index] = base;
      continue;
    }

    while (anchorCursor + 1 < anchors.length && ts > anchors[anchorCursor + 1].ts) {
      anchorCursor += 1;
    }

    let override: number | undefined;
    if (ts < anchors[0].ts) {
      const gapDays = (anchors[0].ts - ts) / dayMs;
      const scale = scales[0];
      if (gapDays <= options.maxBackFillDays && Number.isFinite(scale)) {
        override = base * Number(scale);
      }
    } else if (ts > anchors[lastAnchorIndex].ts) {
      const gapDays = (ts - anchors[lastAnchorIndex].ts) / dayMs;
      const scale = scales[lastAnchorIndex];
      if (gapDays <= options.maxForwardFillDays && Number.isFinite(scale)) {
        override = base * Number(scale);
      }
    } else {
      const leftIndex = anchorCursor;
      const rightIndex = Math.min(lastAnchorIndex, leftIndex + 1);
      const leftAnchor = anchors[leftIndex];
      const rightAnchor = anchors[rightIndex];
      const leftScale = scales[leftIndex];
      const rightScale = scales[rightIndex];

      if (Number.isFinite(leftScale) && Number.isFinite(rightScale)) {
        if (rightAnchor.ts === leftAnchor.ts) {
          override = base * Number(leftScale);
        } else {
          const spanDays = (rightAnchor.ts - leftAnchor.ts) / dayMs;
          if (spanDays <= options.maxInterpolationSpanDays) {
            const ratio = clamp((ts - leftAnchor.ts) / (rightAnchor.ts - leftAnchor.ts), 0, 1);
            const scale = Number(leftScale) + (Number(rightScale) - Number(leftScale)) * ratio;
            override = base * scale;
          }
        }
      }
    }

    const value = Number.isFinite(override) ? Number(override) : base;
    result[index] = clamp(value, options.minValue, options.maxValue);
  }

  return result;
}

async function fetchStooqCloseSeries(symbol: string, startDate: string, endDate: string): Promise<ClosePoint[]> {
  const url = `https://stooq.com/q/d/l/?s=${symbol.toLowerCase()}.us&i=d`;
  const csvText = await curlGet(url, 35000);
  const rows = parseCsv(csvText);

  if (!rows.length || rows[0][0]?.toLowerCase() !== "date") {
    throw new Error(`Unexpected Stooq CSV format for ${symbol}`);
  }

  const result: ClosePoint[] = [];
  for (const row of rows.slice(1)) {
    const date = row[0];
    const close = Number(row[4]);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || "")) continue;
    if (date < startDate || date > endDate) continue;
    if (!Number.isFinite(close) || close <= 0) continue;
    result.push({ date, close });
  }

  if (!result.length) {
    throw new Error(`No Stooq data for ${symbol}`);
  }

  return result;
}

async function fetchUs10ySeries(endDate: string): Promise<YieldPoint[]> {
  const csvText = await curlGet("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10", 25000);
  const rows = parseCsv(csvText);

  if (!rows.length || !rows[0][0]?.toLowerCase().includes("observation")) {
    throw new Error("Unexpected FRED CSV format for DGS10");
  }

  const result: YieldPoint[] = [];
  for (const row of rows.slice(1)) {
    const date = row[0];
    const rawValue = row[1];

    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || "")) continue;
    if (date > endDate) continue;
    if (!rawValue || rawValue === ".") continue;

    const value = Number(rawValue) / 100;
    if (!Number.isFinite(value) || value <= 0) continue;

    result.push({ date, value });
  }

  if (!result.length) {
    throw new Error("No usable DGS10 values from FRED");
  }

  return result;
}

async function fetchStockAnalysisPe(symbol: string): Promise<number | undefined> {
  const url = `https://stockanalysis.com/etf/${symbol.toLowerCase()}/`;
  const html = await curlGet(url, 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const plain = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const match =
    html.match(/"peRatio":"([0-9.]+)"/i) ||
    html.match(/"peRatio":([0-9.]+)/i) ||
    plain.match(/PE Ratio\s*([0-9]+(?:\.[0-9]+)?)/i);
  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  return value;
}

async function fetchStockAnalysisForwardPe(symbol: string): Promise<number | undefined> {
  const url = `https://stockanalysis.com/etf/${symbol.toLowerCase()}/`;
  const html = await curlGet(url, 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const plain = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const match =
    html.match(/"forwardPE":"([0-9.]+)"/i) ||
    html.match(/"forwardPE":([0-9.]+)/i) ||
    html.match(/"forwardPERatio":"?([0-9.]+)"?/i) ||
    html.match(/"forwardPeRatio":"?([0-9.]+)"?/i) ||
    html.match(/"forward[_-]?pe(?:ratio)?":"?([0-9.]+)"?/i) ||
    html.match(/"forward[_-]?pe(?:ratio)?":([0-9.]+)/i) ||
    html.match(/"forward_price_to_earnings":"?([0-9.]+)"?/i) ||
    html.match(/"forwardPriceToEarnings":"?([0-9.]+)"?/i) ||
    plain.match(/Forward\s*(?:P\/E|PE)\s*([0-9]+(?:\.[0-9]+)?)/i) ||
    plain.match(/P\/E\s*\(Forward\)\s*([0-9]+(?:\.[0-9]+)?)/i) ||
    plain.match(/Forward\s+Price\s+to\s+Earnings\s*([0-9]+(?:\.[0-9]+)?)/i);

  if (!match) return undefined;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  return value;
}

async function fetchFinvizForwardPe(symbol: string): Promise<number | undefined> {
  const url = `https://finviz.com/quote.ashx?t=${symbol.toUpperCase()}&p=d`;
  const html = await curlGet(url, 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const plain = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const match =
    html.match(/Forward P\/E<\/td>\s*<td[^>]*>\s*([0-9]+(?:\.[0-9]+)?)/i) ||
    plain.match(/Forward P\/E\s*([0-9]+(?:\.[0-9]+)?)/i);
  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  return value;
}

async function fetchGuruFocusEtfPe(symbol: string): Promise<number | undefined> {
  const url = `https://www.gurufocus.com/etf/${symbol.toUpperCase()}/summary`;
  const html = await curlGet(url, 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const plain = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const match = plain.match(/PE Ratio\s*:?\s*([0-9]+(?:\.[0-9]+)?)/i);
  if (!match) return undefined;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  return value;
}

async function fetchMacroMicroSp500Pe(): Promise<number | undefined> {
  const url = "https://en.macromicro.me/series/1633/us-sp500-pe-ratio";
  const html = await curlGet(url, 25000, {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    Accept: "text/html,application/xhtml+xml",
  });

  const plain = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const match = plain.match(/US\s*-\s*S&P\s*500\s*PE\s*Ratio\s*(20\d{2}-\d{2}(?:-\d{2})?)\s*([0-9]+(?:\.[0-9]+)?)/i);
  if (!match) return undefined;
  const value = Number(match[2]);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  return value;
}

async function fetchMacroMicroNasdaq100ForwardPe(): Promise<number | undefined> {
  const urls = [
    "https://en.macromicro.me/series/23955/nasdaq-100-pe",
    "https://en.macromicro.me/series/23955/us-nasdaq-100-forward-pe",
    "https://r.jina.ai/http://en.macromicro.me/series/23955/nasdaq-100-pe",
    "https://r.jina.ai/http://en.macromicro.me/series/23955/us-nasdaq-100-forward-pe",
  ];

  for (const url of urls) {
    try {
      const html = await curlGet(url, 25000, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        Accept: "text/html,application/xhtml+xml,text/plain",
      });

      const plain = html
        .replace(/<script[\s\S]*?<\/script>/gi, " ")
        .replace(/<style[\s\S]*?<\/style>/gi, " ")
        .replace(/<[^>]+>/g, " ")
        .replace(/&nbsp;/gi, " ")
        .replace(/\s+/g, " ")
        .trim();

      const match =
        plain.match(
          /(?:US\s*-\s*)?NASDAQ\s*100\s*PE[^0-9]{0,180}(20\d{2}-\d{2}(?:-\d{2})?)\s*([0-9]+(?:\.[0-9]+)?)/i
        ) ||
        plain.match(/NASDAQ\s*100[^0-9]{0,120}Forward[^0-9]{0,80}PE[^0-9]{0,80}([0-9]+(?:\.[0-9]+)?)/i);

      if (!match) continue;

      const value = Number(match[2] || match[1]);
      if (!Number.isFinite(value) || value <= 0 || value > 120) continue;
      return value;
    } catch {
      // try next source
    }
  }

  return undefined;
}

function buildProxySeriesFromClose(closes: ClosePoint[], options: BuildSeriesOptions): RawValuationPoint[] {
  const yields = [...options.yields].sort((a, b) => a.date.localeCompare(b.date));
  let yieldIndex = 0;
  let currentYield = yields[0]?.value ?? 0.035;

  const alpha63 = 2 / (63 + 1);
  const alpha252 = 2 / (252 + 1);
  const alpha756 = 2 / (756 + 1);
  const alphaPe = clamp(options.peSmoothingAlpha ?? 0.11, 0.05, 0.32);
  const alphaForwardPe = clamp(options.forwardSmoothingAlpha ?? 0.13, 0.06, 0.34);
  const alphaPb = clamp(options.pbSmoothingAlpha ?? 0.08, 0.04, 0.24);

  let ema63 = closes[0]?.close || 1;
  let ema252 = ema63;
  let ema756 = ema63;

  let rollingYieldFast = currentYield;
  let rollingYieldSlow = currentYield;
  let rollingAbsReturnFast = 0;
  let rollingAbsReturnSlow = 0;

  let peTtmSmooth = options.anchorPe;
  let peForwardSmooth = options.anchorForwardPe;
  let pbSmooth = options.anchorPb;

  const peRaw: number[] = [];
  const peForwardRaw: number[] = [];
  const pbRaw: number[] = [];
  const yieldSeries: number[] = [];

  for (let i = 0; i < closes.length; i += 1) {
    const closePoint = closes[i];

    while (yieldIndex < yields.length && yields[yieldIndex].date <= closePoint.date) {
      currentYield = yields[yieldIndex].value;
      yieldIndex += 1;
    }

    const close = Math.max(closePoint.close, 0.01);
    const prevClose = Math.max(closes[Math.max(i - 1, 0)].close, 0.01);
    const dailyReturn = close / prevClose - 1;

    ema63 += alpha63 * (close - ema63);
    ema252 += alpha252 * (close - ema252);
    ema756 += alpha756 * (close - ema756);

    rollingYieldFast += alpha63 * (currentYield - rollingYieldFast);
    rollingYieldSlow += alpha756 * (currentYield - rollingYieldSlow);

    const absRet = Math.abs(dailyReturn);
    rollingAbsReturnFast += alpha63 * (absRet - rollingAbsReturnFast);
    rollingAbsReturnSlow += alpha252 * (absRet - rollingAbsReturnSlow);

    const premiumShort = Math.log(close / Math.max(ema252, 0.01));
    const premiumLong = Math.log(Math.max(ema252, 0.01) / Math.max(ema756, 0.01));
    const premiumUltraShort = Math.log(close / Math.max(ema63, 0.01));
    const yieldGap = currentYield - rollingYieldSlow;
    const yieldMomentum = currentYield - rollingYieldFast;
    const volGap = rollingAbsReturnFast - rollingAbsReturnSlow;

    const ttmSignal = clamp(
      premiumUltraShort * 1.25 +
        premiumShort * 0.72 +
        premiumLong * 0.46 -
        yieldGap * 4.8 -
        yieldMomentum * 1.4 -
        volGap * 6.2,
      -0.92,
      0.92
    );
    const pbSignal = clamp(ttmSignal * 0.62 + premiumLong * 0.28 - yieldGap * 1.8 - volGap * 2.2, -0.9, 0.9);
    const forwardSpread = clamp(
      0.085 + premiumLong * 0.22 + premiumShort * 0.12 - yieldGap * 0.42 - yieldMomentum * 0.2,
      -0.08,
      0.24
    );

    const targetPeTtm = options.anchorPe * Math.exp(ttmSignal);
    const targetPeForward = targetPeTtm * (1 - forwardSpread);
    const targetPb = options.anchorPb * Math.exp(pbSignal);

    const peShock = clamp(dailyReturn * 1.2, -0.1, 0.1);
    const forwardShock = clamp(dailyReturn * 1.05, -0.09, 0.09);
    const pbShock = clamp(dailyReturn * 0.9, -0.08, 0.08);

    peTtmSmooth = clamp(peTtmSmooth * (1 + peShock), 0.1, 500);
    peForwardSmooth = clamp(peForwardSmooth * (1 + forwardShock), 0.1, 500);
    pbSmooth = clamp(pbSmooth * (1 + pbShock), 0.05, 120);

    peTtmSmooth += alphaPe * (targetPeTtm - peTtmSmooth);
    peForwardSmooth += alphaForwardPe * (targetPeForward - peForwardSmooth);
    pbSmooth += alphaPb * (targetPb - pbSmooth);

    peRaw.push(peTtmSmooth);
    peForwardRaw.push(peForwardSmooth);
    pbRaw.push(pbSmooth);
    yieldSeries.push(clamp(currentYield, 0.001, 0.12));
  }

  const scalePe = options.anchorPe / Math.max(peRaw[peRaw.length - 1] || options.anchorPe, 0.0001);
  const scaleForward =
    options.anchorForwardPe / Math.max(peForwardRaw[peForwardRaw.length - 1] || options.anchorForwardPe, 0.0001);
  const scalePb = options.anchorPb / Math.max(pbRaw[pbRaw.length - 1] || options.anchorPb, 0.0001);

  return closes.map((closePoint, index) => {
    const peTtm = roundTo(clamp(peRaw[index] * scalePe, 2.4, 240), 4);
    const peForward = roundTo(clamp(peForwardRaw[index] * scaleForward, 2, 120), 4);
    return {
      date: closePoint.date,
      pe_ttm: peTtm,
      pe_forward: peForward,
      pb: roundTo(clamp(pbRaw[index] * scalePb, 0.2, 28), 4),
      us10y_yield: roundTo(yieldSeries[index], 5),
    };
  });
}

function applyMonthlyPeOverrides(
  points: RawValuationPoint[],
  trailingSeries?: MonthlyMetricPoint[],
  forwardSeries?: MonthlyMetricPoint[],
  options: {
    trailingMaxInterpolationSpanDays?: number;
    trailingMaxForwardFillDays?: number;
    trailingMaxBackFillDays?: number;
    trailingMinScale?: number;
    trailingMaxScale?: number;
    forwardMaxInterpolationSpanDays?: number;
    forwardMaxForwardFillDays?: number;
    forwardMaxBackFillDays?: number;
    forwardMinScale?: number;
    forwardMaxScale?: number;
  } = {}
): RawValuationPoint[] {
  if ((!trailingSeries || !trailingSeries.length) && (!forwardSeries || !forwardSeries.length)) {
    return points;
  }

  const trailingMaxInterpolationSpanDays = options.trailingMaxInterpolationSpanDays ?? 180;
  const trailingMaxForwardFillDays = options.trailingMaxForwardFillDays ?? 35;
  const trailingMaxBackFillDays = options.trailingMaxBackFillDays ?? 35;
  const trailingMinScale = options.trailingMinScale ?? 0.7;
  const trailingMaxScale = options.trailingMaxScale ?? 1.4;

  const forwardMaxInterpolationSpanDays = options.forwardMaxInterpolationSpanDays ?? 120;
  const forwardMaxForwardFillDays = options.forwardMaxForwardFillDays ?? 20;
  const forwardMaxBackFillDays = options.forwardMaxBackFillDays ?? 20;
  const forwardMinScale = options.forwardMinScale ?? 0.65;
  const forwardMaxScale = options.forwardMaxScale ?? 1.45;

  const trailingOverride = buildAnchoredOverride(points, "pe_ttm", trailingSeries, {
    minValue: 2.4,
    maxValue: 240,
    maxInterpolationSpanDays: trailingMaxInterpolationSpanDays,
    maxForwardFillDays: trailingMaxForwardFillDays,
    maxBackFillDays: trailingMaxBackFillDays,
    minScale: trailingMinScale,
    maxScale: trailingMaxScale,
  });

  const forwardOverride = buildAnchoredOverride(points, "pe_forward", forwardSeries, {
    minValue: 2,
    maxValue: 120,
    maxInterpolationSpanDays: forwardMaxInterpolationSpanDays,
    maxForwardFillDays: forwardMaxForwardFillDays,
    maxBackFillDays: forwardMaxBackFillDays,
    minScale: forwardMinScale,
    maxScale: forwardMaxScale,
  });

  return points.map((point, index) => {
    const trailing = trailingOverride?.[index];
    const forward = forwardOverride?.[index];

    const peTtm = Number.isFinite(trailing) ? clamp(Number(trailing), 2.4, 240) : point.pe_ttm;
    const peForward = Number.isFinite(forward) ? clamp(Number(forward), 2, 120) : point.pe_forward;

    return {
      ...point,
      pe_ttm: roundTo(peTtm, 4),
      pe_forward: roundTo(peForward, 4),
    };
  });
}

function findCloseAtOrBeforeDate(closes: ClosePoint[], targetDate: string): ClosePoint | undefined {
  if (!closes.length) return undefined;
  if (targetDate < closes[0].date) return undefined;

  let lo = 0;
  let hi = closes.length - 1;
  let ans = -1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const date = closes[mid].date;
    if (date <= targetDate) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans >= 0 ? closes[ans] : undefined;
}

function buildCloseAnchoredOverride(
  points: RawValuationPoint[],
  closes: ClosePoint[],
  rawAnchorSeries: MonthlyMetricPoint[] | undefined,
  options: {
    minValue: number;
    maxValue: number;
    maxAnchorLagDays: number;
    segmentMode?: "denom_progress" | "daily_return_path";
  }
): number[] | undefined {
  const anchors = sanitizeMonthlySeries(rawAnchorSeries, options.minValue, options.maxValue);
  if (anchors.length < 2 || !points.length || !closes.length) return undefined;

  const dayMs = 24 * 60 * 60 * 1000;
  const closeByDate = new Map<string, number>();
  for (const close of closes) {
    if (Number.isFinite(close.close) && close.close > 0) {
      closeByDate.set(close.date, close.close);
    }
  }

  const enriched: Array<{ date: string; ts: number; value: number; close: number; denom: number }> = [];
  for (const anchor of anchors) {
    const closePoint = findCloseAtOrBeforeDate(closes, anchor.date);
    if (!closePoint) continue;
    const closeTs = parseDate(closePoint.date).getTime();
    const lagDays = Math.max(0, (anchor.ts - closeTs) / dayMs);
    if (lagDays > options.maxAnchorLagDays) continue;
    if (!Number.isFinite(anchor.value) || anchor.value <= 0) continue;
    const denom = closePoint.close / anchor.value;
    if (!Number.isFinite(denom) || denom <= 0) continue;
    enriched.push({
      date: closePoint.date,
      ts: closeTs,
      value: anchor.value,
      close: closePoint.close,
      denom,
    });
  }

  if (enriched.length < 2) return undefined;

  const byDate = new Map<string, { date: string; ts: number; value: number; close: number; denom: number }>();
  for (const item of enriched) {
    byDate.set(item.date, item);
  }
  const tradeAnchors = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  if (tradeAnchors.length < 2) return undefined;

  const indexByDate = new Map<string, number>();
  for (let i = 0; i < points.length; i += 1) {
    indexByDate.set(points[i].date, i);
  }

  const result = new Array<number>(points.length);
  const segmentMode = options.segmentMode ?? "denom_progress";

  for (let segment = 0; segment + 1 < tradeAnchors.length; segment += 1) {
    const left = tradeAnchors[segment];
    const right = tradeAnchors[segment + 1];

    const leftIndex = indexByDate.get(left.date);
    const rightIndex = indexByDate.get(right.date);
    if (leftIndex === undefined || rightIndex === undefined) continue;
    if (rightIndex <= leftIndex) continue;

    const leftDenom = Number(left.denom);
    const rightDenom = Number(right.denom);
    if (!Number.isFinite(leftDenom) || !Number.isFinite(rightDenom) || leftDenom <= 0 || rightDenom <= 0) continue;

    const span = rightIndex - leftIndex;
    const stepLogReturns = new Array<number>(span + 1).fill(0);
    const weights = new Array<number>(span + 1).fill(0);
    let totalWeight = 0;
    let sumLogReturns = 0;

    for (let i = leftIndex + 1; i <= rightIndex; i += 1) {
      const prevClose = closeByDate.get(points[i - 1].date);
      const currentClose = closeByDate.get(points[i].date);
      if (
        !Number.isFinite(prevClose) ||
        !Number.isFinite(currentClose) ||
        Number(prevClose) <= 0 ||
        Number(currentClose) <= 0
      ) {
        continue;
      }
      const stepLogRet = Math.log(Number(currentClose) / Number(prevClose));
      stepLogReturns[i - leftIndex] = stepLogRet;
      sumLogReturns += stepLogRet;
      const weight = Math.max(Math.abs(stepLogRet), 1e-6);
      weights[i - leftIndex] = weight;
      totalWeight += weight;
    }

    if (segmentMode === "daily_return_path") {
      const targetLogChange = Math.log(right.value / left.value);
      const residual = targetLogChange - sumLogReturns;
      const adjustments = new Array<number>(span + 1).fill(0);
      const candidateOffsets: number[] = [];
      let candidateWeight = 0;

      for (let offset = 1; offset <= span; offset += 1) {
        const step = stepLogReturns[offset];
        if (
          (residual >= 0 && step >= 0) ||
          (residual < 0 && step <= 0)
        ) {
          candidateOffsets.push(offset);
          candidateWeight += Math.max(Math.abs(step), 1e-6);
        }
      }

      if (candidateOffsets.length && candidateWeight > 0) {
        for (const offset of candidateOffsets) {
          const weight = Math.max(Math.abs(stepLogReturns[offset]), 1e-6);
          adjustments[offset] = (residual * weight) / candidateWeight;
        }
      } else {
        const drift = residual / Math.max(1, span);
        for (let offset = 1; offset <= span; offset += 1) {
          adjustments[offset] = drift;
        }
      }

      let value = Number(left.value);
      if (Number.isFinite(value) && value > 0) {
        result[leftIndex] = clamp(value, options.minValue, options.maxValue);
      }

      for (let offset = 1; offset <= span; offset += 1) {
        const step = stepLogReturns[offset] + adjustments[offset];
        value *= Math.exp(step);
        if (Number.isFinite(value) && value > 0) {
          result[leftIndex + offset] = clamp(value, options.minValue, options.maxValue);
        }
      }
      continue;
    }

    const logLeftDenom = Math.log(leftDenom);
    const logRightDenom = Math.log(rightDenom);
    let cumulativeWeight = 0;

    for (let i = leftIndex; i <= rightIndex; i += 1) {
      const close = closeByDate.get(points[i].date);
      if (!Number.isFinite(close) || Number(close) <= 0) continue;

      if (i > leftIndex) cumulativeWeight += weights[i - leftIndex];
      const progress =
        totalWeight > 1e-9
          ? clamp(cumulativeWeight / totalWeight, 0, 1)
          : clamp((i - leftIndex) / Math.max(1, span), 0, 1);
      const logDenom = logLeftDenom + (logRightDenom - logLeftDenom) * progress;
      const denom = Math.exp(logDenom);
      if (!Number.isFinite(denom) || denom <= 0) continue;

      const value = Number(close) / denom;
      if (Number.isFinite(value) && value > 0) {
        result[i] = clamp(value, options.minValue, options.maxValue);
      }
    }
  }

  for (const anchor of tradeAnchors) {
    const index = indexByDate.get(anchor.date);
    if (index === undefined) continue;
    result[index] = clamp(anchor.value, options.minValue, options.maxValue);
  }

  return result;
}

function applyCloseAnchoredOverrides(
  points: RawValuationPoint[],
  closes: ClosePoint[],
  trailingSeries?: MonthlyMetricPoint[],
  forwardSeries?: MonthlyMetricPoint[],
  options: {
    minTtm?: number;
    maxTtm?: number;
    minForward?: number;
    maxForward?: number;
    maxAnchorLagDays?: number;
  } = {}
): RawValuationPoint[] {
  if (!points.length || !closes.length) return points;
  const minTtm = options.minTtm ?? 2.4;
  const maxTtm = options.maxTtm ?? 240;
  const minForward = options.minForward ?? 2;
  const maxForward = options.maxForward ?? 120;
  const maxAnchorLagDays = options.maxAnchorLagDays ?? 5;

  const trailingOverride = buildCloseAnchoredOverride(points, closes, trailingSeries, {
    minValue: minTtm,
    maxValue: maxTtm,
    maxAnchorLagDays,
    segmentMode: "daily_return_path",
  });

  const forwardOverride = buildCloseAnchoredOverride(points, closes, forwardSeries, {
    minValue: minForward,
    maxValue: maxForward,
    maxAnchorLagDays,
    segmentMode: "denom_progress",
  });

  if (!trailingOverride?.length && !forwardOverride?.length) return points;

  return points.map((point, index) => {
    const trailing = trailingOverride?.[index];
    const forward = forwardOverride?.[index];
    return {
      ...point,
      pe_ttm: roundTo(
        Number.isFinite(trailing) ? clamp(Number(trailing), minTtm, maxTtm) : Number(point.pe_ttm),
        4
      ),
      pe_forward: roundTo(
        Number.isFinite(forward) ? clamp(Number(forward), minForward, maxForward) : Number(point.pe_forward),
        4
      ),
    };
  });
}

function isReasonablePe(value: number | undefined): value is number {
  return Number.isFinite(value) && value > 4 && value < 80;
}

function isReasonableForwardPe(value: number | undefined): value is number {
  return Number.isFinite(value) && value > 2 && value < 120;
}

function isPlausibleForwardPair(trailing: number | undefined, forward: number | undefined): boolean {
  if (!isReasonablePe(trailing) || !isReasonableForwardPe(forward)) return false;
  if (forward >= trailing) return false;
  const ratio = forward / trailing;
  return ratio >= 0.45 && ratio <= 0.95 && trailing - forward >= 0.8;
}

function pickForwardStartDate(
  closes: ClosePoint[],
  forwardSeries: MonthlyMetricPoint[] | undefined,
  historyForwardStartDate: string | undefined
): string {
  const earliestCloseDate = closes[0]?.date;
  if (!earliestCloseDate) return historyForwardStartDate || "";

  if (forwardSeries?.length && forwardSeries.length >= 4) {
    const candidate = forwardSeries[0].date;
    if (!candidate || !/^\d{4}-\d{2}-\d{2}$/.test(candidate)) return earliestCloseDate;
    return candidate < earliestCloseDate ? earliestCloseDate : candidate;
  }

  if (historyForwardStartDate && /^\d{4}-\d{2}-\d{2}$/.test(historyForwardStartDate)) {
    const minCoverageIndex = Math.max(0, closes.length - 126);
    const minCoverageDate = closes[minCoverageIndex]?.date || earliestCloseDate;
    if (historyForwardStartDate <= minCoverageDate) {
      return historyForwardStartDate < earliestCloseDate ? earliestCloseDate : historyForwardStartDate;
    }
  }

  return earliestCloseDate;
}

function isUsableHistoryDataset(dataset: ValuationDataset | null | undefined): dataset is ValuationDataset {
  if (!dataset || !Array.isArray(dataset.indices) || !dataset.indices.length) return false;
  if (!dataset.source || /synthetic/i.test(dataset.source)) return false;

  for (const item of dataset.indices) {
    if (!item?.id || !Array.isArray(item.points) || !item.points.length) {
      return false;
    }
  }
  return true;
}

function isHistorySeriesReliable(indexId: string, points: RawValuationPoint[]): boolean {
  if (!points.length) return false;
  if (indexId === "nasdaq100") {
    const bubbleRange = points.filter((point) => point.date >= "1999-01-01" && point.date <= "2003-12-31");
    if (!bubbleRange.length) return false;
    const bubblePeak = Math.max(...bubbleRange.map((point) => Number(point.pe_ttm) || 0));
    if (!Number.isFinite(bubblePeak) || bubblePeak < 70) {
      return false;
    }
  }
  return true;
}

function buildHistoryFallbackMap(
  dataset: ValuationDataset | null | undefined,
  effectiveEnd: string
): Map<string, RawValuationPoint[]> {
  const result = new Map<string, RawValuationPoint[]>();
  if (!isUsableHistoryDataset(dataset)) return result;

  for (const item of dataset.indices) {
    const points = item.points.filter((point) => point.date <= effectiveEnd);
    if (isHistorySeriesReliable(item.id, points)) {
      result.set(item.id, points);
    }
  }
  return result;
}

function buildLatestForwardMap(
  dataset: ValuationDataset | null | undefined,
  effectiveEnd: string
): Map<string, number> {
  const result = new Map<string, number>();
  if (!isUsableHistoryDataset(dataset)) return result;
  if (/ttm-fpe-fallback-/i.test(dataset.source || "")) {
    return result;
  }

  for (const item of dataset.indices) {
    const latest = [...item.points].reverse().find((point) => point.date <= effectiveEnd);
    if (!latest) continue;
    if (isReasonableForwardPe(latest.pe_forward)) {
      result.set(item.id, Number(latest.pe_forward));
    }
  }

  return result;
}

function buildForwardStartMap(
  dataset: ValuationDataset | null | undefined,
  effectiveEnd: string
): Map<string, string> {
  const result = new Map<string, string>();
  if (!isUsableHistoryDataset(dataset)) return result;

  for (const item of dataset.indices) {
    const rawStart = String(item.forwardStartDate || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(rawStart)) continue;
    if (rawStart > effectiveEnd) continue;
    result.set(item.id, rawStart);
  }

  return result;
}

export async function generateDataset(endDate?: string, options: GenerateDatasetOptions = {}): Promise<ValuationDataset> {
  const effectiveEnd = endDate || new Date().toISOString().slice(0, 10);
  const historyFallbackMap = buildHistoryFallbackMap(options.previousDataset, effectiveEnd);
  const latestForwardMap = buildLatestForwardMap(options.previousDataset, effectiveEnd);
  const historyForwardStartMap = buildForwardStartMap(options.previousDataset, effectiveEnd);
  const sp500ForwardPinnedSeries = await loadLocalMetricSeriesFromCsv(SP500_FORWARD_PE_MM_CSV, 2, 120);
  let wsjPeSnapshot = new Map<string, LatestPeSnapshot>();

  try {
    wsjPeSnapshot = await fetchWsjPeSnapshot();
  } catch {
    wsjPeSnapshot = new Map<string, LatestPeSnapshot>();
  }

  let us10ySeries: YieldPoint[] = [];
  let yieldSource = "fred10y";
  try {
    us10ySeries = await fetchUs10ySeries(effectiveEnd);
  } catch {
    yieldSource = "fallback10y";
    us10ySeries = [];
  }

  const indices: ValuationDataset["indices"] = [];
  let liveSeriesCount = 0;
  let historyFallbackCount = 0;
  let stockAnalysisAnchorCount = 0;
  let guruFocusAnchorCount = 0;
  let macroMicroAnchorCount = 0;
  let macroMicroTrailingSeriesCount = 0;
  let trendonifyTrailingCount = 0;
  let trendonifyForwardCount = 0;
  let wsjSnapshotCount = 0;
  let wsjCuratedTtmCount = 0;
  let stockAnalysisSnapshotCount = 0;
  let siblisTrailingCount = 0;
  let siblisForwardCount = 0;
  let macroMicroForwardCount = 0;
  let ndxForwardBootstrapCount = 0;
  let ndxTtmFactsetBootstrapCount = 0;
  let multplTrailingCount = 0;
  let stockAnalysisForwardCount = 0;
  let finvizForwardCount = 0;
  let historyForwardFallbackCount = 0;
  let localPinnedForwardCount = 0;
  let cachedMultplSp500Series: MonthlyMetricPoint[] | undefined;
  const fetchErrors: string[] = [];

  for (const meta of ALL_INDICES) {
    const startDate = INDEX_START_DATE[meta.id] || "2010-01-04";
    const baseline = BASELINE_BY_INDEX[meta.id];

    try {
      const closes = await fetchStooqCloseSeries(meta.symbol, startDate, effectiveEnd);
      if (closes.length < 120) {
        throw new Error(`insufficient close data: ${meta.id}`);
      }
      const latestCloseDate = closes[closes.length - 1]?.date || effectiveEnd;

      let anchorPe = baseline.pe;
      let resolvedFrom = "";
      const trendRoutes = TRENDONIFY_ROUTES[meta.id];
      const siblisRoute = SIBLIS_ROUTES[meta.id];

      let trailingSeries: MonthlyMetricPoint[] | undefined;
      let forwardSeries: MonthlyMetricPoint[] | undefined;
      let siblisLatestTrailingSnapshot: number | undefined;
      let siblisLatestForwardSnapshot: number | undefined;
      const macroMicroIds = MACROMICRO_CHART_IDS[meta.id];
      const pinnedForwardSeries = meta.id === "sp500" ? sp500ForwardPinnedSeries : undefined;
      const hasPinnedForwardSeries = Boolean(pinnedForwardSeries?.length);

      if (hasPinnedForwardSeries && pinnedForwardSeries) {
        forwardSeries = pinnedForwardSeries;
        localPinnedForwardCount += 1;
      }

      if (siblisRoute?.url) {
        try {
          const siblis = await fetchSiblisSeries(siblisRoute.url);
          if (siblis?.trailing?.length) {
            const latestTrailing = siblis.trailing[siblis.trailing.length - 1]?.value;
            if (isReasonablePe(latestTrailing)) {
              siblisLatestTrailingSnapshot = Number(latestTrailing);
            }

            if (SIBLIS_FULL_HISTORY_INDEX_IDS.has(meta.id)) {
              trailingSeries = siblis.trailing;
              siblisTrailingCount += 1;
              if (isReasonablePe(latestTrailing)) {
                anchorPe = Number(latestTrailing);
                resolvedFrom = "siblis";
              }
            }
          }
          if (siblis?.forward?.length) {
            const latestForward = siblis.forward[siblis.forward.length - 1]?.value;
            if (isReasonableForwardPe(latestForward)) {
              siblisLatestForwardSnapshot = Number(latestForward);
            }

            if (SIBLIS_FULL_HISTORY_INDEX_IDS.has(meta.id)) {
              forwardSeries = siblis.forward;
              siblisForwardCount += 1;
            }
          }
        } catch {
          // ignore
        }
      }

      if (macroMicroIds?.trailing?.length) {
        const macroTrailing = await fetchMacroMicroSeriesByCandidates(macroMicroIds.trailing);
        if (macroTrailing?.length) {
          trailingSeries = trailingSeries?.length ? mergeMonthlySeries(trailingSeries, macroTrailing) : macroTrailing;
          macroMicroTrailingSeriesCount += 1;
          const latestTrailing = trailingSeries[trailingSeries.length - 1]?.value;
          if (isReasonablePe(latestTrailing)) {
            anchorPe = Number(latestTrailing);
            resolvedFrom = "macromicro";
          }
        }
      }

      if (macroMicroIds?.forward?.length) {
        const allowForwardOverlayFromMacroMicro = !FORWARD_LOCKED_INDEX_IDS.has(meta.id);
        if (allowForwardOverlayFromMacroMicro && !hasPinnedForwardSeries) {
          const macroForward = await fetchMacroMicroSeriesByCandidates(macroMicroIds.forward);
          if (macroForward?.length) {
            forwardSeries = forwardSeries?.length ? mergeMonthlySeries(forwardSeries, macroForward) : macroForward;
            macroMicroForwardCount += 1;
          }
        }
      }

      const macroMicroSeriesRoutes = MACROMICRO_SERIES_ROUTES[meta.id];
      if (macroMicroSeriesRoutes?.forward?.length) {
        const allowForwardOverlayFromMacroMicro = !FORWARD_LOCKED_INDEX_IDS.has(meta.id);
        if (allowForwardOverlayFromMacroMicro && !hasPinnedForwardSeries) {
          const macroForwardBySeriesPage = await fetchMacroMicroSeriesByPageRoutes(macroMicroSeriesRoutes.forward);
          if (macroForwardBySeriesPage?.length) {
            forwardSeries = forwardSeries?.length
              ? mergeMonthlySeries(macroForwardBySeriesPage, forwardSeries)
              : macroForwardBySeriesPage;
            macroMicroForwardCount += 1;
          }
        }
      }

      if (trendRoutes) {
        const preferRecentOverride = RECENT_OVERRIDE_INDEX_IDS.has(meta.id);
        const trendPrimaryForTrailing = TRENDONIFY_TRAILING_PRIMARY_INDEX_IDS.has(meta.id);
        const shouldFetchTrendTrailing =
          !trailingSeries?.length ||
          preferRecentOverride ||
          trendPrimaryForTrailing ||
          isSeriesStale(trailingSeries, effectiveEnd, 45);

        if (shouldFetchTrendTrailing) {
          const trendTrailing = await fetchTrendonifySeries(trendRoutes.trailing);
          if (trendTrailing?.length) {
            if (!trailingSeries?.length) {
              trailingSeries = trendTrailing;
            } else if (trendPrimaryForTrailing) {
              trailingSeries = mergeMonthlySeries(trendTrailing, trailingSeries);
            } else if (preferRecentOverride) {
              trailingSeries = mergeMonthlySeriesRecentOverride(trailingSeries, trendTrailing, 900);
            } else {
              trailingSeries = mergeMonthlySeries(trailingSeries, trendTrailing);
            }
            trendonifyTrailingCount += 1;
            const latestTrailing = trailingSeries?.[trailingSeries.length - 1]?.value;
            if (isReasonablePe(latestTrailing)) {
              anchorPe = Number(latestTrailing);
              resolvedFrom = "trendonify";
            }
          }
        }
      }

      if (meta.id === "sp500" && (!trailingSeries?.length || isSeriesStale(trailingSeries, effectiveEnd, 120))) {
        try {
          if (!cachedMultplSp500Series) {
            cachedMultplSp500Series = await fetchMultplSp500PeSeries();
          }
        } catch {
          // ignore
        }

        if (cachedMultplSp500Series?.length) {
          trailingSeries = trailingSeries?.length
            ? mergeMonthlySeries(trailingSeries, cachedMultplSp500Series)
            : cachedMultplSp500Series;
          multplTrailingCount += 1;
          const latestTrailing = trailingSeries[trailingSeries.length - 1]?.value;
          if (isReasonablePe(latestTrailing) && !resolvedFrom) {
            anchorPe = Number(latestTrailing);
            resolvedFrom = "multpl";
          }
        }
      }

      if (trendRoutes) {
        const preferRecentOverride = RECENT_OVERRIDE_INDEX_IDS.has(meta.id);
        const allowForwardOverlayFromTrend = !FORWARD_LOCKED_INDEX_IDS.has(meta.id);
        const shouldFetchTrendForward =
          allowForwardOverlayFromTrend &&
          !hasPinnedForwardSeries &&
          (!forwardSeries?.length || preferRecentOverride || isSeriesStale(forwardSeries, effectiveEnd, 45));
        if (shouldFetchTrendForward) {
          const trendForward = await fetchTrendonifySeries(trendRoutes.forward);
          if (trendForward?.length) {
            if (!forwardSeries?.length) {
              forwardSeries = trendForward;
            } else if (preferRecentOverride) {
              forwardSeries = mergeMonthlySeriesRecentOverride(forwardSeries, trendForward, 900);
            } else {
              forwardSeries = mergeMonthlySeries(forwardSeries, trendForward);
            }
            trendonifyForwardCount += 1;
          }
        }
      }

      if (meta.id === "nasdaq100") {
        const bubbleTrailing = buildBootstrapSeries(NASDAQ100_TTM_BUBBLE_FACTSET_BOOTSTRAP);
        if (bubbleTrailing.length) {
          trailingSeries = trailingSeries?.length ? mergeMonthlySeries(bubbleTrailing, trailingSeries) : bubbleTrailing;
          ndxTtmFactsetBootstrapCount += 1;
        }

        const bootstrapForward = buildBootstrapSeries(NASDAQ100_FORWARD_MM_BOOTSTRAP);
        if (bootstrapForward.length && (!forwardSeries?.length || forwardSeries[0].date > "2001-12-31")) {
          forwardSeries = forwardSeries?.length ? mergeMonthlySeries(forwardSeries, bootstrapForward) : bootstrapForward;
          ndxForwardBootstrapCount += 1;
        }
      }

      if (!SIBLIS_FULL_HISTORY_INDEX_IDS.has(meta.id)) {
        if (isReasonablePe(siblisLatestTrailingSnapshot)) {
          trailingSeries = upsertSeriesValueAtDate(
            trailingSeries,
            latestCloseDate,
            Number(siblisLatestTrailingSnapshot)
          );
          siblisTrailingCount += 1;
          anchorPe = Number(siblisLatestTrailingSnapshot);
          resolvedFrom = "siblis";
        }
        if (
          isReasonableForwardPe(siblisLatestForwardSnapshot) &&
          !FORWARD_LOCKED_INDEX_IDS.has(meta.id) &&
          !hasPinnedForwardSeries
        ) {
          forwardSeries = upsertSeriesValueAtDate(
            forwardSeries,
            latestCloseDate,
            Number(siblisLatestForwardSnapshot)
          );
          siblisForwardCount += 1;
        }
      }

      let wsjSnapshotAppliedForIndex = false;
      const wsjLatest = wsjPeSnapshot.get(meta.id);
      if (wsjLatest) {
        let appliedWsjSnapshot = false;
        const wsjTrailing = isReasonablePe(wsjLatest.trailing) ? Number(wsjLatest.trailing) : undefined;
        const wsjForward = isReasonableForwardPe(wsjLatest.forward) ? Number(wsjLatest.forward) : undefined;

        if (isReasonablePe(wsjTrailing)) {
          trailingSeries = upsertSeriesValueAtDate(trailingSeries, effectiveEnd, wsjTrailing);
          anchorPe = wsjTrailing;
          resolvedFrom = "wsj";
          appliedWsjSnapshot = true;
        }

        if (isPlausibleForwardPair(wsjTrailing, wsjForward)) {
          if (!FORWARD_LOCKED_INDEX_IDS.has(meta.id) && !hasPinnedForwardSeries) {
            forwardSeries = upsertSeriesValueAtDate(forwardSeries, effectiveEnd, Number(wsjForward));
            appliedWsjSnapshot = true;
          }
        }
        if (appliedWsjSnapshot) {
          wsjSnapshotCount += 1;
          wsjSnapshotAppliedForIndex = true;
        }
      }

      let ndxCuratedTtmApplied = false;
      if (meta.id === "nasdaq100") {
        const curatedRef = pickCuratedWsjTtmReference(meta.id, effectiveEnd, 90);
        if (curatedRef) {
          const observedRef = trailingSeries?.length
            ? interpolateMonthlyMetric(curatedRef.date, trailingSeries, Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY)
            : undefined;

          if (isReasonablePe(observedRef)) {
            const factor = clamp(curatedRef.value / Number(observedRef), 0.85, 1.15);
            trailingSeries = scaleSeriesByFactor(trailingSeries, factor, 2.4, 240);
            trailingSeries = upsertSeriesValueAtDate(trailingSeries, curatedRef.date, curatedRef.value);
            const scaledLatest = trailingSeries?.[trailingSeries.length - 1]?.value;
            if (isReasonablePe(scaledLatest)) {
              anchorPe = Number(scaledLatest);
              resolvedFrom = "wsj-curated";
              wsjCuratedTtmCount += 1;
              ndxCuratedTtmApplied = true;
            }
          } else if (isReasonablePe(curatedRef.value)) {
            trailingSeries = upsertSeriesValueAtDate(trailingSeries, latestCloseDate, curatedRef.value);
            trailingSeries = upsertSeriesValueAtDate(trailingSeries, curatedRef.date, curatedRef.value);
            anchorPe = curatedRef.value;
            resolvedFrom = "wsj-curated";
            wsjCuratedTtmCount += 1;
            ndxCuratedTtmApplied = true;
          }
        }
      }

      if (
        (meta.id === "sp500" || meta.id === "nasdaq100" || meta.id === "russell2000") &&
        !wsjSnapshotAppliedForIndex
      ) {
        let appliedStockAnalysisSnapshot = false;
        let stockTrailing: number | undefined;
        let stockForward: number | undefined;

        try {
          stockTrailing = await fetchStockAnalysisPe(meta.symbol);
        } catch {
          // ignore
        }

        try {
          stockForward = await fetchStockAnalysisForwardPe(meta.symbol);
        } catch {
          // ignore
        }

        if (!isReasonableForwardPe(stockForward) && meta.id === "nasdaq100") {
          try {
            stockForward = await fetchMacroMicroNasdaq100ForwardPe();
          } catch {
            // ignore
          }
        }

        if (!isReasonableForwardPe(stockForward)) {
          try {
            stockForward = await fetchFinvizForwardPe(meta.symbol);
          } catch {
            // ignore
          }
        }

        if (isReasonablePe(stockTrailing) && !(meta.id === "nasdaq100" && ndxCuratedTtmApplied)) {
          trailingSeries = upsertSeriesValueAtDate(trailingSeries, effectiveEnd, Number(stockTrailing));
          anchorPe = Number(stockTrailing);
          resolvedFrom = "stockanalysis";
          appliedStockAnalysisSnapshot = true;
        }

        const trailingForPair = isReasonablePe(stockTrailing)
          ? Number(stockTrailing)
          : trailingSeries?.[trailingSeries.length - 1]?.value;
        if (
          !FORWARD_LOCKED_INDEX_IDS.has(meta.id) &&
          !hasPinnedForwardSeries &&
          isPlausibleForwardPair(trailingForPair, stockForward)
        ) {
          forwardSeries = upsertSeriesValueAtDate(forwardSeries, effectiveEnd, Number(stockForward));
          appliedStockAnalysisSnapshot = true;
        }

        if (appliedStockAnalysisSnapshot) {
          stockAnalysisSnapshotCount += 1;
        }
      }

      if (meta.id === "nasdaq100" && !trailingSeries?.length) {
        throw new ReliableSourceError(`missing reliable trailing PE history for ${meta.id}`);
      }
      if (meta.id === "nasdaq100") {
        if (!forwardSeries?.length) {
          throw new ReliableSourceError(`missing reliable forward PE history for ${meta.id}`);
        }
        const forwardStart = forwardSeries[0].date;
        if (forwardStart > "2001-12-31") {
          throw new ReliableSourceError(`insufficient forward PE history for ${meta.id}: starts at ${forwardStart}`);
        }
      }

      if (!resolvedFrom) {
        const anchorCandidates: Array<{ from: string; value?: number }> = [];

        if (meta.id === "sp500") {
          try {
            anchorCandidates.push({ from: "macromicro", value: await fetchMacroMicroSp500Pe() });
          } catch {
            // ignore
          }
        }

        try {
          anchorCandidates.push({ from: "gurufocus", value: await fetchGuruFocusEtfPe(meta.symbol) });
        } catch {
          // ignore
        }

        try {
          anchorCandidates.push({ from: "stockanalysis", value: await fetchStockAnalysisPe(meta.symbol) });
        } catch {
          // ignore
        }

        for (const candidate of anchorCandidates) {
          if (isReasonablePe(candidate.value)) {
            anchorPe = Number(candidate.value);
            resolvedFrom = candidate.from;
            break;
          }
        }
      }

      if (resolvedFrom === "macromicro") macroMicroAnchorCount += 1;
      else if (resolvedFrom === "gurufocus") guruFocusAnchorCount += 1;
      else if (resolvedFrom === "stockanalysis") stockAnalysisAnchorCount += 1;

      const latestForward = forwardSeries?.length ? forwardSeries[forwardSeries.length - 1]?.value : undefined;
      let anchorForwardPe = isReasonableForwardPe(latestForward) ? Number(latestForward) : undefined;

      if (!anchorForwardPe) {
        try {
          const stockAnalysisForward = await fetchStockAnalysisForwardPe(meta.symbol);
          if (isReasonableForwardPe(stockAnalysisForward)) {
            anchorForwardPe = Number(stockAnalysisForward);
            stockAnalysisForwardCount += 1;
          }
        } catch {
          // ignore
        }
      }

      if (!anchorForwardPe) {
        try {
          const finvizForward = await fetchFinvizForwardPe(meta.symbol);
          if (isReasonableForwardPe(finvizForward)) {
            anchorForwardPe = Number(finvizForward);
            finvizForwardCount += 1;
          }
        } catch {
          // ignore
        }
      }

      if (!anchorForwardPe) {
        const latestHistoryForward = latestForwardMap.get(meta.id);
        if (isReasonableForwardPe(latestHistoryForward)) {
          anchorForwardPe = Number(latestHistoryForward);
          historyForwardFallbackCount += 1;
        }
      }

      if (!anchorForwardPe) {
        throw new ReliableSourceError(`missing reliable forward PE source for ${meta.id}`);
      }

      const forwardStartDate = pickForwardStartDate(closes, forwardSeries, historyForwardStartMap.get(meta.id));

      const anchorPb = clamp(baseline.pb * Math.pow(anchorPe / baseline.pe, 0.72), 0.5, 16);
      const proxyPoints = buildProxySeriesFromClose(closes, {
        anchorPe,
        anchorForwardPe,
        anchorPb,
        yields: us10ySeries,
        peSmoothingAlpha: meta.id === "nasdaq100" ? 0.115 : undefined,
        forwardSmoothingAlpha: meta.id === "nasdaq100" ? 0.15 : undefined,
      });
      let points = applyMonthlyPeOverrides(proxyPoints, trailingSeries, forwardSeries, {
        trailingMaxInterpolationSpanDays: meta.id === "nasdaq100" ? 360 : 180,
        trailingMaxScale: meta.id === "nasdaq100" ? 3.8 : 1.4,
        forwardMaxInterpolationSpanDays: meta.id === "nasdaq100" ? 360 : 120,
        forwardMaxForwardFillDays: meta.id === "nasdaq100" ? 40 : 20,
        forwardMaxBackFillDays: meta.id === "nasdaq100" ? 40 : 20,
        forwardMinScale: meta.id === "nasdaq100" ? 0.4 : 0.65,
        forwardMaxScale: meta.id === "nasdaq100" ? 4.8 : 1.45,
      });
      points = applyCloseAnchoredOverrides(points, closes, trailingSeries, forwardSeries, {
        minTtm: 2.4,
        maxTtm: meta.id === "nasdaq100" ? 240 : 180,
        minForward: 2,
        maxForward: meta.id === "nasdaq100" ? 180 : 140,
        maxAnchorLagDays: 5,
      });

      liveSeriesCount += 1;

      indices.push({
        id: meta.id,
        symbol: meta.symbol,
        group: meta.group,
        displayName: meta.displayName,
        description: meta.description,
        forwardStartDate,
        points,
      });
    } catch (error) {
      if (error instanceof ReliableSourceError) {
        fetchErrors.push(`${meta.id}/${meta.symbol}: ${errorMessage(error)}`);
        continue;
      }
      const historyPoints = historyFallbackMap.get(meta.id);
      if (historyPoints?.length) {
        historyFallbackCount += 1;
        indices.push({
          id: meta.id,
          symbol: meta.symbol,
          group: meta.group,
          displayName: meta.displayName,
          description: meta.description,
          forwardStartDate: historyForwardStartMap.get(meta.id) || historyPoints[historyPoints.length - 1]?.date,
          points: historyPoints,
        });
        continue;
      }
      fetchErrors.push(`${meta.id}/${meta.symbol}: ${errorMessage(error)}`);
    }
  }

  if (fetchErrors.length > 0) {
    throw new Error(
      `Real data fetch failed for ${fetchErrors.length} index(es) without historical fallback: ${fetchErrors.join(
        " | "
      )}`
    );
  }

  let source = "free-live-proxy-v6";
  if (historyFallbackCount > 0) {
    source += `+history-fallback-${historyFallbackCount}`;
  }
  if (liveSeriesCount === 0) {
    source += "+no-live-refresh";
  }
  if (stockAnalysisAnchorCount > 0) {
    source += `+stockanalysis-pe-${stockAnalysisAnchorCount}`;
  }
  if (guruFocusAnchorCount > 0) {
    source += `+gurufocus-pe-${guruFocusAnchorCount}`;
  }
  if (macroMicroAnchorCount > 0) {
    source += `+macromicro-pe-${macroMicroAnchorCount}`;
  }
  if (macroMicroTrailingSeriesCount > 0) {
    source += `+macromicro-pe-series-${macroMicroTrailingSeriesCount}`;
  }
  if (multplTrailingCount > 0) {
    source += `+multpl-pe-${multplTrailingCount}`;
  }
  if (trendonifyTrailingCount > 0) {
    source += `+trendonify-pe-${trendonifyTrailingCount}`;
  }
  if (trendonifyForwardCount > 0) {
    source += `+trendonify-fpe-${trendonifyForwardCount}`;
  }
  if (wsjSnapshotCount > 0) {
    source += `+wsj-pe-${wsjSnapshotCount}`;
  }
  if (wsjCuratedTtmCount > 0) {
    source += `+wsj-curated-ttm-${wsjCuratedTtmCount}`;
  }
  if (stockAnalysisSnapshotCount > 0) {
    source += `+stockanalysis-snap-${stockAnalysisSnapshotCount}`;
  }
  if (siblisTrailingCount > 0) {
    source += `+siblis-pe-${siblisTrailingCount}`;
  }
  if (siblisForwardCount > 0) {
    source += `+siblis-fpe-${siblisForwardCount}`;
  }
  if (macroMicroForwardCount > 0) {
    source += `+macromicro-fpe-${macroMicroForwardCount}`;
  }
  if (localPinnedForwardCount > 0) {
    source += `+local-mm-fpe-${localPinnedForwardCount}`;
  }
  if (ndxForwardBootstrapCount > 0) {
    source += `+ndx-fpe-bootstrap-${ndxForwardBootstrapCount}`;
  }
  if (ndxTtmFactsetBootstrapCount > 0) {
    source += `+ndx-ttm-factset-${ndxTtmFactsetBootstrapCount}`;
  }
  if (stockAnalysisForwardCount > 0) {
    source += `+stockanalysis-fpe-${stockAnalysisForwardCount}`;
  }
  if (finvizForwardCount > 0) {
    source += `+finviz-fpe-${finvizForwardCount}`;
  }
  if (historyForwardFallbackCount > 0) {
    source += `+history-fpe-${historyForwardFallbackCount}`;
  }
  source += `+${yieldSource}`;

  return {
    generatedAt: new Date().toISOString(),
    source,
    indices,
  };
}

export function validateDataset(dataset: ValuationDataset): void {
  if (!dataset.indices.length) {
    throw new Error("Dataset contains no indices");
  }

  for (const indexData of dataset.indices) {
    if (!INDEX_MAP[indexData.id]) {
      throw new Error(`Unknown index in dataset: ${indexData.id}`);
    }
    if (!indexData.points.length) {
      throw new Error(`Empty time series: ${indexData.id}`);
    }
    if (
      indexData.forwardStartDate !== undefined &&
      !/^\d{4}-\d{2}-\d{2}$/.test(String(indexData.forwardStartDate || ""))
    ) {
      throw new Error(`Invalid forwardStartDate: ${indexData.id}`);
    }

    let prevDate = "";
    for (const point of indexData.points) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(point.date || "")) {
        throw new Error(`Invalid date format: ${indexData.id}`);
      }
      if (prevDate && point.date < prevDate) {
        throw new Error(`Date order is not ascending: ${indexData.id}`);
      }
      prevDate = point.date;

      const numberFields = [point.pe_ttm, point.pe_forward, point.pb, point.us10y_yield];
      if (numberFields.some((value) => !Number.isFinite(value))) {
        throw new Error(`Invalid numeric value: ${indexData.id}`);
      }
    }
  }
}
