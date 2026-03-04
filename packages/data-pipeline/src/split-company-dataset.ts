import path from "node:path";
import { fileURLToPath } from "node:url";
import { mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";

interface CompanyValuationPoint {
  date: string;
  pe_ttm?: number | null;
  pe_forward?: number | null;
  pb?: number | null;
  peg?: number | null;
  close?: number | null;
  [key: string]: unknown;
}

interface CompanyIndexInput {
  id: string;
  symbol: string;
  displayName: string;
  description?: string;
  rank?: number;
  marketCap?: number;
  peg?: number | null;
  forwardStartDate?: string;
  points?: CompanyValuationPoint[];
  quarterlyEps?: Array<{ date: string; eps?: number; source?: string }>;
  quarterlyNetIncome?: Array<{ date: string; netIncome?: number; source?: string }>;
}

interface CompanyDatasetInput {
  generatedAt?: string;
  source?: string;
  indices?: CompanyIndexInput[];
}

interface CompanySnapshotIndex {
  id: string;
  symbol: string;
  displayName: string;
  description: string;
  rank: number;
  marketCap: number;
  forwardStartDate: string;
  startDate: string;
  endDate: string;
  pointCount: number;
  date: string;
  pe_ttm: number | null;
  pe_forward: number | null;
  pb: number | null;
  peg: number | null;
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
const INPUT_FILE = path.join(DATA_DIR, "company-valuation-history.json");
const SNAPSHOT_FILE = path.join(DATA_DIR, "company-valuation-snapshot.json");
const SERIES_DIR = path.join(DATA_DIR, "company-series");
const NEGATIVE_VALUATION_BASE = 1_000_000;
const NEGATIVE_VALUATION_EPSILON = 1e-6;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function toFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && !value.trim()) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function valuationRankValue(value: number): number {
  if (value >= 0) return value;
  const abs = Math.max(Math.abs(value), NEGATIVE_VALUATION_EPSILON);
  return NEGATIVE_VALUATION_BASE + 1 / abs;
}

function subtractYears(dateText: string, years: number): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) return dateText;
  const date = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return dateText;
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return date.toISOString().slice(0, 10);
}

function regimeFromPercentile(percentile: number): "high" | "low" | "neutral" {
  if (percentile >= 0.85) return "high";
  if (percentile <= 0.15) return "low";
  return "neutral";
}

function computeLatestPeStats(points: CompanyValuationPoint[]) {
  const validRows = (Array.isArray(points) ? points : [])
    .map((point) => ({
      date: String(point?.date || ""),
      pe: toFiniteNumber(point?.pe_ttm),
    }))
    .filter((row) => /^\d{4}-\d{2}-\d{2}$/.test(row.date) && row.pe !== null);

  if (!validRows.length) {
    return {
      latestDate: "",
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      pe_ttm_change_1y: 0,
      regime: "neutral" as const,
    };
  }

  const latestRow = validRows[validRows.length - 1];
  const latestDate = latestRow.date;
  const latestPe = Number(latestRow.pe);
  const latestPeRank = valuationRankValue(latestPe);

  const cutoff5 = subtractYears(latestDate, 5);
  const cutoff10 = subtractYears(latestDate, 10);
  const cutoff3 = subtractYears(latestDate, 3);
  const lookbackDate = subtractYears(latestDate, 1);

  const pickRowsByCutoff = (cutoffDate: string) =>
    validRows.filter((row) => row.date >= cutoffDate && row.date <= latestDate);

  const rows5 = pickRowsByCutoff(cutoff5);
  const rows10 = pickRowsByCutoff(cutoff10);
  const rows3 = pickRowsByCutoff(cutoff3);

  const percentileFromRows = (rows: Array<{ date: string; pe: number | null }>) => {
    if (!rows.length) return 0.5;
    const count = rows.filter((row) => valuationRankValue(Number(row.pe)) <= latestPeRank).length;
    return clamp(count / rows.length, 0, 1);
  };

  const percentileFull = percentileFromRows(validRows);
  const percentile5 = percentileFromRows(rows5);
  const percentile10 = percentileFromRows(rows10);

  let zScore3y = 0;
  if (rows3.length > 1) {
    const values = rows3.map((row) => Number(row.pe));
    const sum = values.reduce((acc, value) => acc + value, 0);
    const avg = sum / values.length;
    const variance =
      values.reduce((acc, value) => acc + (value - avg) * (value - avg), 0) / (values.length - 1);
    const sigma = Math.sqrt(variance);
    if (sigma > 1e-12) {
      zScore3y = (latestPe - avg) / sigma;
    }
  }

  const peReference =
    [...validRows].reverse().find((row) => row.date <= lookbackDate) ||
    validRows.find((row) => row.date >= lookbackDate) ||
    null;
  const peRef = Number(peReference?.pe);
  const peChange1y = Number.isFinite(peRef) && Math.abs(peRef) > 1e-12 ? (latestPe - peRef) / Math.abs(peRef) : 0;

  return {
    latestDate,
    percentile_5y: percentile5,
    percentile_10y: percentile10,
    percentile_full: percentileFull,
    z_score_3y: zScore3y,
    pe_ttm_change_1y: peChange1y,
    regime: regimeFromPercentile(percentile10),
  };
}

function buildSnapshotIndexRow(item: CompanyIndexInput): CompanySnapshotIndex {
  const points = Array.isArray(item.points) ? item.points : [];
  const latestPoint = points[points.length - 1] || {};
  const peStats = computeLatestPeStats(points);

  return {
    id: String(item.id || ""),
    symbol: String(item.symbol || "").trim().toUpperCase(),
    displayName: String(item.displayName || ""),
    description: String(item.description || `${item.displayName || ""} (${item.symbol || ""})`),
    rank: Number(item.rank || 9999),
    marketCap: Number(item.marketCap || 0),
    forwardStartDate: String(item.forwardStartDate || points[points.length - 1]?.date || ""),
    startDate: String(points[0]?.date || ""),
    endDate: String(points[points.length - 1]?.date || ""),
    pointCount: points.length,
    date: peStats.latestDate || String(latestPoint.date || ""),
    pe_ttm: toFiniteNumber(latestPoint.pe_ttm),
    pe_forward: toFiniteNumber(latestPoint.pe_forward),
    pb: toFiniteNumber(latestPoint.pb),
    peg: toFiniteNumber(item.peg ?? latestPoint.peg),
    percentile_5y: peStats.percentile_5y,
    percentile_10y: peStats.percentile_10y,
    percentile_full: peStats.percentile_full,
    z_score_3y: peStats.z_score_3y,
    pe_ttm_change_1y: peStats.pe_ttm_change_1y,
    regime: peStats.regime,
  };
}

function stripGrowthOnlyFieldsFromPoints(points: CompanyValuationPoint[]): CompanyValuationPoint[] {
  return points
    .filter((point) => point && typeof point === "object")
    .map((point) => {
      const nextPoint = { ...(point as Record<string, unknown>) };
      delete nextPoint.close;
      return nextPoint as CompanyValuationPoint;
    });
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
  const dataset = JSON.parse(rawText) as CompanyDatasetInput;
  const indicesRaw = Array.isArray(dataset.indices) ? dataset.indices : [];

  const indices = indicesRaw
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      ...item,
      id: String(item.id || "").trim(),
      symbol: String(item.symbol || "").trim().toUpperCase(),
      displayName: String(item.displayName || "").trim(),
      points: Array.isArray(item.points) ? item.points : [],
    }))
    .filter((item) => item.id && item.symbol && item.displayName);

  if (!indices.length) {
    throw new Error("No valid company series found in company-valuation-history.json");
  }

  await mkdir(DATA_DIR, { recursive: true });
  await mkdir(SERIES_DIR, { recursive: true });

  const snapshotIndices = indices
    .map((item) => buildSnapshotIndexRow(item))
    .sort((a, b) => {
      const capDiff = Number(b.marketCap || 0) - Number(a.marketCap || 0);
      if (capDiff !== 0) return capDiff;
      const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
      if (rankDiff !== 0) return rankDiff;
      return String(a.displayName || "").localeCompare(String(b.displayName || ""));
    });

  const snapshotPayload = {
    generatedAt: String(dataset.generatedAt || ""),
    source: String(dataset.source || "company-snapshot"),
    indices: snapshotIndices,
  };
  await writeFile(SNAPSHOT_FILE, `${JSON.stringify(snapshotPayload)}\n`, "utf8");

  const validSeriesFiles = new Set<string>();
  const writeTasks = indices.map((item) => {
    const fileName = `${item.id}.json`;
    validSeriesFiles.add(fileName);
    const filePath = path.join(SERIES_DIR, fileName);
    const payload = {
      generatedAt: String(dataset.generatedAt || ""),
      source: String(dataset.source || "company-snapshot"),
      indexId: item.id,
      symbol: item.symbol,
      displayName: item.displayName,
      description: String(item.description || `${item.displayName} (${item.symbol})`),
      rank: Number(item.rank || 9999),
      marketCap: Number(item.marketCap || 0),
      forwardStartDate: String(item.forwardStartDate || item.points[item.points.length - 1]?.date || ""),
      points: stripGrowthOnlyFieldsFromPoints(item.points),
    };
    return writeFile(filePath, `${JSON.stringify(payload)}\n`, "utf8");
  });
  await Promise.all(writeTasks);
  await removeStaleSeriesFiles(validSeriesFiles);

  console.log(`[company] snapshot split written: ${SNAPSHOT_FILE}`);
  console.log(`[company] series directory: ${SERIES_DIR}`);
  console.log(`[company] series count: ${indices.length}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
