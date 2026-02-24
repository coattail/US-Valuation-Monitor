import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

interface CompanySeed {
  rank: number;
  symbol: string;
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

interface AnchorDenominator {
  date: string;
  ts: number;
  peDen: number | null;
  fwdDen: number | null;
  pbDen: number | null;
}

interface SnapshotPoint {
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  us10y_yield: number;
}

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const OUTPUT_DIR = path.join(ROOT_DIR, "data", "standardized");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "company-valuation-history.json");
const execFileAsync = promisify(execFile);

const TOP_COMPANY_URLS = [
  "https://companiesmarketcap.com/usd/usa/largest-companies-in-the-usa-by-market-cap/",
  "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/",
];

const HISTORY_START_DATE = "2000-01-01";
const CONCURRENCY = 3;
const REQUEST_TIMEOUT_MS = 12000;

const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const DEFAULT_METRICS = {
  pe_ttm: 20,
  pe_forward: 18,
  pb: 3,
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

function sanitizeRatio(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

function isRejectedPayload(text: string): boolean {
  const raw = String(text || "").trim().toLowerCase();
  if (!raw) return true;
  if (raw.includes("exceeded the daily hits limit")) return true;
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

function parseTopCompanies(html: string): CompanySeed[] {
  const rowRegex =
    /<tr><td class="fav">[\s\S]*?<td class="rank-td td-right"[^>]*data-sort="(\d+)"[^>]*>[\s\S]*?<\/td><td class="name-td">[\s\S]*?<div class="company-name">([\s\S]*?)<\/div><div class="company-code">[\s\S]*?<\/span>\s*([^<\s]+)\s*<\/div>[\s\S]*?<\/td><td class="td-right" data-sort="(\d+)"/g;

  const companies: CompanySeed[] = [];
  const seen = new Set<string>();

  for (const match of html.matchAll(rowRegex)) {
    const rank = Number(match[1]);
    const displayName = decodeHtml(stripTags(match[2])).replace(/\s+/g, " ").trim();
    const symbol = decodeHtml(match[3]).replace(/\s+/g, "").trim().toUpperCase();
    const marketCap = Number(match[4]);

    if (!Number.isFinite(rank) || rank <= 0) continue;
    if (!symbol || seen.has(symbol)) continue;
    if (!Number.isFinite(marketCap) || marketCap <= 0) continue;

    seen.add(symbol);
    companies.push({ rank, symbol, displayName, marketCap });
  }

  return companies
    .filter((item) => item.rank <= 100)
    .sort((a, b) => a.rank - b.rank)
    .slice(0, 100);
}

async function fetchTopCompanies(): Promise<CompanySeed[]> {
  for (const url of TOP_COMPANY_URLS) {
    try {
      const html = await fetchText(url, 1, 14000);
      const companies = parseTopCompanies(html);
      if (companies.length >= 90) return companies;
    } catch {
      // try next source
    }
  }

  throw new Error("Unable to parse top US companies from companiesmarketcap");
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

async function fetchCloseHistory(symbol: string): Promise<ClosePoint[]> {
  const candidates = getStooqSymbolCandidates(symbol);

  for (const candidate of candidates) {
    const url = `https://stooq.com/q/d/l/?s=${encodeURIComponent(candidate)}.us&i=d`;
    try {
      await sleep(120);
      const csv = await fetchText(url, 2, 32000, true);
      const points = parseStooqCsv(csv);
      if (points.length >= 200) return points;
    } catch {
      // try next candidate
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
    const pe = sanitizeRatio(peRaw[i]);
    const peForward = sanitizeRatio(fwdRaw[i]);
    const pb = sanitizeRatio(pbRaw[i]);

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
      const quarterlyHtml = await fetchText(quarterlyUrl, 0, 14000);
      const quarterly = parseRatioPayloadFromScript(quarterlyHtml);

      const annualUrl = `https://stockanalysis.com/stocks/${slug}/financials/ratios/`;
      const annualHtml = await fetchText(annualUrl, 0, 12000);
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

function fillMissingSequence(values: Array<number | null>, fallback: number): number[] {
  const out = [...values];

  let lastKnown: number | null = null;
  for (let i = 0; i < out.length; i += 1) {
    if (Number.isFinite(out[i])) {
      lastKnown = out[i] as number;
    } else if (lastKnown !== null) {
      out[i] = lastKnown;
    }
  }

  let nextKnown: number | null = null;
  for (let i = out.length - 1; i >= 0; i -= 1) {
    if (Number.isFinite(out[i])) {
      nextKnown = out[i] as number;
    } else if (nextKnown !== null) {
      out[i] = nextKnown;
    }
  }

  for (let i = 0; i < out.length; i += 1) {
    if (!Number.isFinite(out[i])) out[i] = fallback;
  }

  return out as number[];
}

function buildAnchorDenominators(closePoints: ClosePoint[], ratioPayload: RatioPayload | null): { anchors: AnchorDenominator[]; usedFallback: boolean } {
  const firstDate = closePoints[0]?.date || HISTORY_START_DATE;
  const lastDate = closePoints[closePoints.length - 1]?.date || HISTORY_START_DATE;
  const lastClose = closePoints[closePoints.length - 1]?.close || 1;

  const fallbackMetrics = {
    pe_ttm: DEFAULT_METRICS.pe_ttm,
    pe_forward: DEFAULT_METRICS.pe_forward,
    pb: DEFAULT_METRICS.pb,
  };

  const sourceAnchors = ratioPayload?.anchors || [];
  const latest = ratioPayload?.latest || {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
  };

  const anchored = new Map<string, RatioAnchor>();

  for (const item of sourceAnchors) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(item.date)) continue;
    if (item.date < firstDate || item.date > lastDate) continue;
    anchored.set(item.date, item);
  }

  const latestAnchor: RatioAnchor = {
    date: lastDate,
    pe_ttm: latest.pe_ttm,
    pe_forward: latest.pe_forward,
    pb: latest.pb,
  };

  if (!anchored.has(lastDate)) {
    anchored.set(lastDate, latestAnchor);
  } else {
    const current = anchored.get(lastDate)!;
    anchored.set(lastDate, {
      date: lastDate,
      pe_ttm: current.pe_ttm ?? latest.pe_ttm,
      pe_forward: current.pe_forward ?? latest.pe_forward,
      pb: current.pb ?? latest.pb,
    });
  }

  if (![...anchored.keys()].some((date) => date <= firstDate)) {
    const firstKnown = [...anchored.values()].sort((a, b) => a.date.localeCompare(b.date))[0] || latestAnchor;
    anchored.set(firstDate, {
      date: firstDate,
      pe_ttm: firstKnown.pe_ttm,
      pe_forward: firstKnown.pe_forward,
      pb: firstKnown.pb,
    });
  }

  const orderedAnchors = [...anchored.values()].sort((a, b) => a.date.localeCompare(b.date));

  const denoms: AnchorDenominator[] = orderedAnchors.map((item) => {
    const closeAtAnchor = getCloseAtOrBefore(closePoints, item.date).close;
    return {
      date: item.date,
      ts: toTs(item.date),
      peDen: item.pe_ttm ? closeAtAnchor / item.pe_ttm : null,
      fwdDen: item.pe_forward ? closeAtAnchor / item.pe_forward : null,
      pbDen: item.pb ? closeAtAnchor / item.pb : null,
    };
  });

  const fallbackPeDen = lastClose / fallbackMetrics.pe_ttm;
  const fallbackFwdDen = lastClose / fallbackMetrics.pe_forward;
  const fallbackPbDen = lastClose / fallbackMetrics.pb;

  const peDens = fillMissingSequence(denoms.map((item) => item.peDen), fallbackPeDen);
  const fwdDens = fillMissingSequence(denoms.map((item) => item.fwdDen), fallbackFwdDen);
  const pbDens = fillMissingSequence(denoms.map((item) => item.pbDen), fallbackPbDen);

  for (let i = 0; i < denoms.length; i += 1) {
    denoms[i].peDen = peDens[i];
    denoms[i].fwdDen = fwdDens[i];
    denoms[i].pbDen = pbDens[i];
  }

  const usedFallback = !ratioPayload || !ratioPayload.anchors.length;
  return { anchors: denoms, usedFallback };
}

function interpolate(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

function buildValuationSeries(closePoints: ClosePoint[], anchorDenoms: AnchorDenominator[]): SnapshotPoint[] {
  if (!closePoints.length || !anchorDenoms.length) return [];

  const anchors = [...anchorDenoms].sort((a, b) => a.ts - b.ts);
  let anchorIndex = 0;

  const out: SnapshotPoint[] = [];

  for (const point of closePoints) {
    while (
      anchorIndex < anchors.length - 2 &&
      point.ts > anchors[anchorIndex + 1].ts
    ) {
      anchorIndex += 1;
    }

    const left = anchors[anchorIndex];
    const right = anchors[Math.min(anchorIndex + 1, anchors.length - 1)];

    let t = 0;
    if (right.ts > left.ts) {
      t = clamp((point.ts - left.ts) / (right.ts - left.ts), 0, 1);
    }

    const peDen = interpolate(left.peDen || 1, right.peDen || left.peDen || 1, t);
    const fwdDen = interpolate(left.fwdDen || 1, right.fwdDen || left.fwdDen || 1, t);
    const pbDen = interpolate(left.pbDen || 1, right.pbDen || left.pbDen || 1, t);

    out.push({
      date: point.date,
      pe_ttm: roundTo(clamp(point.close / Math.max(peDen, 1e-6), 0.3, 450), 4),
      pe_forward: roundTo(clamp(point.close / Math.max(fwdDen, 1e-6), 0.2, 420), 4),
      pb: roundTo(clamp(point.close / Math.max(pbDen, 1e-6), 0.1, 120), 4),
      us10y_yield: 0,
    });
  }

  return out;
}

function toCompanyId(symbol: string): string {
  return `company_${symbol.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "")}`;
}

async function main(): Promise<void> {
  console.log("[company] loading top 100 companies...");
  const companies = await fetchTopCompanies();
  if (companies.length < 90) {
    throw new Error(`Top company list is too short: ${companies.length}`);
  }

  console.log(`[company] parsed ${companies.length} companies, building series...`);

  let fallbackAnchorCount = 0;
  let skippedCount = 0;

  const built = await mapLimit(companies, CONCURRENCY, async (company, index) => {
    console.log(`[company] ${String(index + 1).padStart(3, "0")}/${companies.length} ${company.symbol}`);

    const [closePoints, ratioPayload] = await Promise.all([
      fetchCloseHistory(company.symbol),
      fetchQuarterlyRatioPayload(company.symbol),
    ]);

    if (!closePoints.length) {
      skippedCount += 1;
      console.warn(`[company] skip ${company.symbol}: close history unavailable`);
      return null;
    }

    const { anchors, usedFallback } = buildAnchorDenominators(closePoints, ratioPayload);
    if (usedFallback) {
      fallbackAnchorCount += 1;
    }

    const points = buildValuationSeries(closePoints, anchors);
    if (points.length < 120) {
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
      "companiesmarketcap-top100",
      "stooq-daily-close",
      "stockanalysis-quarterly-ratios",
      "daily-return-in-anchor-window",
      `fallback-anchor-${fallbackAnchorCount}`,
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
  console.log(`[company] skipped: ${skippedCount}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
