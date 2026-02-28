import path from "node:path";
import { fileURLToPath } from "node:url";
import { mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";

interface RawValuationPoint {
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  us10y_yield: number;
}

interface IndexInput {
  id: string;
  symbol: string;
  group: string;
  displayName: string;
  description?: string;
  forwardStartDate?: string;
  points?: RawValuationPoint[];
}

interface IndexDatasetInput {
  generatedAt?: string;
  source?: string;
  indices?: IndexInput[];
}

interface SnapshotIndexRow {
  id: string;
  symbol: string;
  group: string;
  displayName: string;
  description: string;
  forwardStartDate: string;
  startDate: string;
  endDate: string;
  pointCount: number;
  date: string;
  pe_ttm: number;
  pe_forward: number;
  pb: number;
  percentile_5y: number;
  percentile_10y: number;
  percentile_full: number;
  z_score_3y: number;
  pe_ttm_change_1y: number;
  regime: "high" | "low" | "neutral";
}

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const DATA_DIR = path.join(ROOT_DIR, "data", "standardized");
const INPUT_FILE = path.join(DATA_DIR, "valuation-history.json");
const SNAPSHOT_FILE = path.join(DATA_DIR, "valuation-snapshot.json");
const SERIES_DIR = path.join(DATA_DIR, "index-series");
const TRADING_DAYS_PER_YEAR = 252;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function subtractYears(dateText: string, years: number): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) return dateText;
  const date = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return dateText;
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return date.toISOString().slice(0, 10);
}

function percentileWindow(values: number[], startIndex: number, endIndex: number, current: number): number {
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

function zScoreWindow(values: number[], startIndex: number, endIndex: number, current: number): number {
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

  let variance = 0;
  for (let i = start; i <= end; i += 1) {
    const delta = values[i] - avg;
    variance += delta * delta;
  }

  const sigma = Math.sqrt(variance / (length - 1));
  if (sigma === 0) return 0;
  return (current - avg) / sigma;
}

function regimeFromPercentile(percentile: number): "high" | "low" | "neutral" {
  if (percentile >= 0.85) return "high";
  if (percentile <= 0.15) return "low";
  return "neutral";
}

function computeLatestPeStats(points: RawValuationPoint[]) {
  if (!Array.isArray(points) || !points.length) {
    return {
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      regime: "neutral" as const,
      pe_ttm_change_1y: 0,
    };
  }

  const values = points.map((point) => Number(point.pe_ttm)).filter((value) => Number.isFinite(value));
  if (!values.length) {
    return {
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      regime: "neutral" as const,
      pe_ttm_change_1y: 0,
    };
  }

  const latestPoint = points[points.length - 1];
  const latestPe = Number(latestPoint.pe_ttm);
  const latestDate = latestPoint.date;

  let lookbackIndex = points.length - 1;
  const lookbackDate = subtractYears(latestDate, 1);
  while (lookbackIndex > 0 && points[lookbackIndex].date > lookbackDate) {
    lookbackIndex -= 1;
  }
  const peRef = Number(points[lookbackIndex]?.pe_ttm);
  const peChange1y = Number.isFinite(peRef) && Math.abs(peRef) > 1e-12 ? (latestPe - peRef) / Math.abs(peRef) : 0;

  return {
    percentile_5y: percentileWindow(values, values.length - TRADING_DAYS_PER_YEAR * 5, values.length - 1, latestPe),
    percentile_10y: percentileWindow(values, values.length - TRADING_DAYS_PER_YEAR * 10, values.length - 1, latestPe),
    percentile_full: percentileWindow(values, 0, values.length - 1, latestPe),
    z_score_3y: zScoreWindow(values, values.length - TRADING_DAYS_PER_YEAR * 3, values.length - 1, latestPe),
    regime: regimeFromPercentile(percentileWindow(values, 0, values.length - 1, latestPe)),
    pe_ttm_change_1y: peChange1y,
  };
}

function buildSnapshotRow(item: IndexInput): SnapshotIndexRow {
  const points = Array.isArray(item.points) ? item.points : [];
  const latestRaw = points[points.length - 1] || {
    date: "",
    pe_ttm: 0,
    pe_forward: 0,
    pb: 0,
    us10y_yield: 0,
  };
  const latestPe = computeLatestPeStats(points);

  return {
    id: String(item.id || ""),
    symbol: String(item.symbol || "").trim().toUpperCase(),
    group: String(item.group || ""),
    displayName: String(item.displayName || ""),
    description: String(item.description || item.displayName || ""),
    forwardStartDate: String(item.forwardStartDate || points[points.length - 1]?.date || ""),
    startDate: String(points[0]?.date || ""),
    endDate: String(points[points.length - 1]?.date || ""),
    pointCount: points.length,
    date: String(latestRaw.date || ""),
    pe_ttm: Number(latestRaw.pe_ttm || 0),
    pe_forward: Number(latestRaw.pe_forward || 0),
    pb: Number(latestRaw.pb || 0),
    percentile_5y: latestPe.percentile_5y,
    percentile_10y: latestPe.percentile_10y,
    percentile_full: latestPe.percentile_full,
    z_score_3y: latestPe.z_score_3y,
    pe_ttm_change_1y: latestPe.pe_ttm_change_1y,
    regime: latestPe.regime,
  };
}

async function removeStaleSeriesFiles(validFileNames: Set<string>): Promise<void> {
  const existingEntries = await readdir(SERIES_DIR, { withFileTypes: true }).catch(() => []);
  const staleTasks: Promise<unknown>[] = [];
  for (const entry of existingEntries) {
    if (!entry.isFile()) continue;
    if (validFileNames.has(entry.name)) continue;
    staleTasks.push(unlink(path.join(SERIES_DIR, entry.name)).catch(() => undefined));
  }
  await Promise.all(staleTasks);
}

async function main(): Promise<void> {
  const rawText = await readFile(INPUT_FILE, "utf8");
  const dataset = JSON.parse(rawText) as IndexDatasetInput;
  const indicesRaw = Array.isArray(dataset.indices) ? dataset.indices : [];

  const indices = indicesRaw
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      ...item,
      id: String(item.id || "").trim(),
      symbol: String(item.symbol || "").trim().toUpperCase(),
      group: String(item.group || "").trim(),
      displayName: String(item.displayName || "").trim(),
      points: Array.isArray(item.points) ? item.points : [],
      description: String(item.description || "").trim(),
      forwardStartDate: String(item.forwardStartDate || ""),
    }))
    .filter((item) => item.id && item.symbol && item.group && item.displayName);

  if (!indices.length) {
    throw new Error("No valid indices found in valuation-history.json");
  }

  await mkdir(DATA_DIR, { recursive: true });
  await mkdir(SERIES_DIR, { recursive: true });

  const snapshotPayload = {
    generatedAt: String(dataset.generatedAt || ""),
    source: String(dataset.source || "valuation-history"),
    indices: indices.map((item) => buildSnapshotRow(item)),
  };
  await writeFile(SNAPSHOT_FILE, `${JSON.stringify(snapshotPayload)}\n`, "utf8");

  const validSeriesFiles = new Set<string>();
  const writeTasks = indices.map((item) => {
    const fileName = `${item.id}.json`;
    validSeriesFiles.add(fileName);
    const filePath = path.join(SERIES_DIR, fileName);
    const payload = {
      generatedAt: String(dataset.generatedAt || ""),
      source: String(dataset.source || "valuation-history"),
      indexId: item.id,
      id: item.id,
      symbol: item.symbol,
      group: item.group,
      displayName: item.displayName,
      description: item.description || item.displayName,
      forwardStartDate: item.forwardStartDate || item.points[item.points.length - 1]?.date || "",
      points: item.points,
    };
    return writeFile(filePath, `${JSON.stringify(payload)}\n`, "utf8");
  });
  await Promise.all(writeTasks);
  await removeStaleSeriesFiles(validSeriesFiles);

  console.log(`[index] snapshot split written: ${SNAPSHOT_FILE}`);
  console.log(`[index] series directory: ${SERIES_DIR}`);
  console.log(`[index] series count: ${indices.length}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
