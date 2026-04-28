import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const CURRENT_FILE = fileURLToPath(import.meta.url);
const REPO_ROOT = path.resolve(path.dirname(CURRENT_FILE), "..");
const DEFAULT_SNAPSHOT_FILE = path.join(REPO_ROOT, "data", "standardized", "company-valuation-snapshot.json");
const DEFAULT_OUTPUT_FILE = path.join(REPO_ROOT, "data", "vendor", "company-forward-pe-history.csv");
const FMP_ANALYST_ESTIMATES_ENDPOINT = "https://financialmodelingprep.com/stable/analyst-estimates";

function csvEscape(value) {
  const text = String(value ?? "");
  if (!/[",\n\r]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function toFinitePositiveNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) && Math.abs(number) > 1e-12 ? number : null;
}

function pickForwardEps(row) {
  return (
    toFinitePositiveNumber(row?.estimatedEpsAvg) ??
    toFinitePositiveNumber(row?.estimatedEpsAverage) ??
    toFinitePositiveNumber(row?.epsAvg) ??
    toFinitePositiveNumber(row?.epsEstimatedAverage) ??
    null
  );
}

function normalizeSymbolList(raw) {
  return String(raw || "")
    .split(/[,\s]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

export function parseCliArgs(argv = process.argv.slice(2)) {
  const options = {
    symbols: [],
    output: DEFAULT_OUTPUT_FILE,
    period: "annual",
    limit: 100,
    page: 0,
    dryRun: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--symbols") {
      options.symbols = normalizeSymbolList(next);
      i += 1;
    } else if (arg === "--output") {
      options.output = path.resolve(next);
      i += 1;
    } else if (arg === "--period") {
      options.period = String(next || "annual").trim() || "annual";
      i += 1;
    } else if (arg === "--limit") {
      options.limit = Math.max(1, Number.parseInt(next || "100", 10) || 100);
      i += 1;
    } else if (arg === "--page") {
      options.page = Math.max(0, Number.parseInt(next || "0", 10) || 0);
      i += 1;
    } else if (arg === "--dry-run") {
      options.dryRun = true;
    }
  }

  return options;
}

export function buildFmpAnalystEstimatesUrl({ symbol, apiKey, period = "annual", page = 0, limit = 100 }) {
  const params = new URLSearchParams();
  params.set("symbol", String(symbol || "").trim().toUpperCase());
  params.set("period", period);
  params.set("page", String(page));
  params.set("limit", String(limit));
  params.set("apikey", apiKey);
  return `${FMP_ANALYST_ESTIMATES_ENDPOINT}?${params.toString()}`;
}

export function estimateRowsToVendorCsvRows(symbol, rows) {
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();
  const out = [];

  for (const row of Array.isArray(rows) ? rows : []) {
    const date = String(row?.date || "").slice(0, 10);
    if (!normalizedSymbol || !/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;

    const eps = pickForwardEps(row);
    if (eps === null) continue;

    out.push({
      symbol: normalizedSymbol,
      date,
      pe_forward: "",
      eps_fy1: String(eps),
      source: "fmp-analyst-estimates-non-pit",
    });
  }

  return out.sort((a, b) => a.date.localeCompare(b.date));
}

async function loadTopCompanySymbols(snapshotFile = DEFAULT_SNAPSHOT_FILE) {
  const payload = JSON.parse(await fs.readFile(snapshotFile, "utf8"));
  return (Array.isArray(payload?.indices) ? payload.indices : [])
    .map((item) => String(item?.symbol || "").trim().toUpperCase())
    .filter(Boolean);
}

async function fetchFmpAnalystEstimates(symbol, options) {
  const url = buildFmpAnalystEstimatesUrl({
    symbol,
    apiKey: options.apiKey,
    period: options.period,
    page: options.page,
    limit: options.limit,
  });
  const response = await fetch(url);
  const text = await response.text();
  let payload = null;
  try {
    payload = JSON.parse(text);
  } catch {
    throw new Error(`FMP returned non-JSON response for ${symbol}: ${text.slice(0, 160)}`);
  }
  if (!response.ok) {
    const message = payload?.["Error Message"] || payload?.message || text.slice(0, 160);
    throw new Error(`FMP request failed for ${symbol}: HTTP ${response.status} ${message}`);
  }
  if (!Array.isArray(payload)) {
    const message = payload?.["Error Message"] || payload?.message || JSON.stringify(payload).slice(0, 160);
    throw new Error(`FMP returned unexpected payload for ${symbol}: ${message}`);
  }
  return payload;
}

function serializeVendorCsv(rows) {
  const header = ["symbol", "date", "pe_forward", "eps_fy1", "source"];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(header.map((key) => csvEscape(row[key])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

export async function importFmpForwardPe(options = {}) {
  const apiKey = options.apiKey || process.env.FMP_API_KEY || process.env.FINANCIALMODELINGPREP_API_KEY || "";
  if (!apiKey) {
    throw new Error("Missing FMP_API_KEY. Create a free FMP key and run with FMP_API_KEY=... npm run import:fmp-forward-pe");
  }

  const symbols = options.symbols?.length ? options.symbols : await loadTopCompanySymbols();
  const allRows = [];
  const errors = [];

  for (const symbol of symbols) {
    try {
      const payload = await fetchFmpAnalystEstimates(symbol, { ...options, apiKey });
      allRows.push(...estimateRowsToVendorCsvRows(symbol, payload));
    } catch (error) {
      errors.push({ symbol, message: error instanceof Error ? error.message : String(error) });
    }
  }

  allRows.sort((a, b) => a.symbol.localeCompare(b.symbol) || a.date.localeCompare(b.date));
  const csvText = serializeVendorCsv(allRows);

  if (!options.dryRun) {
    await fs.mkdir(path.dirname(options.output), { recursive: true });
    await fs.writeFile(options.output, csvText, "utf8");
  }

  return {
    output: options.output,
    dryRun: Boolean(options.dryRun),
    symbols: symbols.length,
    rows: allRows.length,
    errors,
    preview: csvText.split("\n").slice(0, 8).join("\n"),
  };
}

async function main() {
  const options = parseCliArgs();
  const result = await importFmpForwardPe(options);
  console.log(JSON.stringify(result, null, 2));
  if (result.errors.length) {
    process.exitCode = 2;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === CURRENT_FILE) {
  main().catch((error) => {
    console.error("[import-fmp-forward-pe] failed");
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
