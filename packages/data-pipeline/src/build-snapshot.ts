import path from "node:path";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import {
  assertPublishedIndexHistoryAppendOnly,
  assertValidatedIndexHistoryUnchanged,
  generateDataset,
  loadAuthoritativePublishedMetricCorrections,
  validateDataset,
} from "./generate.ts";
import {
  assertDatasetMatchesIndexHistoryLock,
  buildIndexHistoryLock,
  type IndexHistoryLock,
} from "./index-history-lock.ts";
import type { ValuationDataset } from "../../core/src/types.ts";

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../../..");
const OUTPUT_DIR = path.join(ROOT_DIR, "data", "standardized");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "valuation-history.json");
const HISTORY_LOCK_FILE = path.join(OUTPUT_DIR, "index-history-lock.json");
const PAGES_HISTORY_FILE = path.join(ROOT_DIR, ".pages", "data", "standardized", "valuation-history.json");
const OUTPUT_SERIES_DIR = path.join(OUTPUT_DIR, "index-series");
const PAGES_SERIES_DIR = path.join(ROOT_DIR, ".pages", "data", "standardized", "index-series");

function isSyntheticSource(source: string | undefined): boolean {
  return /synthetic/i.test(source || "");
}

async function readPreviousDataset(): Promise<ValuationDataset | undefined> {
  const candidates: ValuationDataset[] = [];

  for (const file of [OUTPUT_FILE, PAGES_HISTORY_FILE]) {
    try {
      const text = await readFile(file, "utf8");
      const parsed = JSON.parse(text) as ValuationDataset;
      validateDataset(parsed);
      if (isSyntheticSource(parsed.source)) continue;
      candidates.push(parsed);
    } catch {
      // ignore missing/invalid candidate
    }
  }

  for (const dir of [OUTPUT_SERIES_DIR, PAGES_SERIES_DIR]) {
    try {
      const files = (await readdir(dir)).filter((name) => name.endsWith(".json")).sort();
      if (!files.length) continue;

      const indices = [];
      let generatedAt = "";
      let source = "";

      for (const file of files) {
        const text = await readFile(path.join(dir, file), "utf8");
        const parsed = JSON.parse(text) as {
          generatedAt?: string;
          source?: string;
          id?: string;
          indexId?: string;
          symbol?: string;
          group?: string;
          displayName?: string;
          description?: string;
          forwardStartDate?: string;
          points?: unknown[];
        };
        if (!parsed?.id || !Array.isArray(parsed.points) || !parsed.points.length) continue;
        generatedAt = generatedAt || String(parsed.generatedAt || "");
        source = source || String(parsed.source || "split-series-fallback");
        indices.push({
          id: String(parsed.id || parsed.indexId || ""),
          symbol: String(parsed.symbol || ""),
          group: String(parsed.group || ""),
          displayName: String(parsed.displayName || ""),
          description: String(parsed.description || ""),
          forwardStartDate: String(parsed.forwardStartDate || ""),
          points: parsed.points,
        });
      }

      if (!indices.length) continue;
      const candidate = {
        generatedAt: generatedAt || new Date().toISOString(),
        source,
        indices,
      } satisfies ValuationDataset;
      validateDataset(candidate);
      if (isSyntheticSource(candidate.source)) continue;
      candidates.push(candidate);
    } catch {
      // ignore missing/invalid split candidate
    }
  }

  if (!candidates.length) return undefined;

  const scoreSeries = (points: ValuationDataset["indices"][number]["points"]): string => {
    const last = points[points.length - 1]?.date || "";
    return `${last}:${String(points.length).padStart(8, "0")}`;
  };

  const mergedById = new Map<string, ValuationDataset["indices"][number]>();
  for (const candidate of candidates) {
    for (const index of candidate.indices) {
      const existing = mergedById.get(index.id);
      if (!existing || scoreSeries(index.points).localeCompare(scoreSeries(existing.points)) > 0) {
        mergedById.set(index.id, index);
      }
    }
  }

  const merged = {
    generatedAt: candidates.map((item) => String(item.generatedAt || "")).sort().slice(-1)[0] || new Date().toISOString(),
    source: candidates.map((item) => String(item.source || "")).find(Boolean) || "merged-previous-history",
    indices: [...mergedById.values()],
  } satisfies ValuationDataset;

  validateDataset(merged);
  return merged;
}

async function syncPagesHistory(dataset: ValuationDataset): Promise<void> {
  const pagesDir = path.dirname(PAGES_HISTORY_FILE);
  await mkdir(pagesDir, { recursive: true });
  await writeFile(PAGES_HISTORY_FILE, `${JSON.stringify(dataset, null, 2)}\n`, "utf8");
}

async function readIndexHistoryLock(previousDataset: ValuationDataset | undefined): Promise<IndexHistoryLock | undefined> {
  try {
    return JSON.parse(await readFile(HISTORY_LOCK_FILE, "utf8")) as IndexHistoryLock;
  } catch (error) {
    const code = (error as { code?: string })?.code;
    if (!previousDataset?.indices?.length || process.env.BOOTSTRAP_INDEX_HISTORY_LOCK === "1") {
      return undefined;
    }
    if (code === "ENOENT") {
      throw new Error(
        "index history lock is missing; set BOOTSTRAP_INDEX_HISTORY_LOCK=1 only for an audited one-time bootstrap"
      );
    }
    throw error;
  }
}

function getRewriteAllowedIds(dataset: ValuationDataset): Set<string> {
  if (process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE === "1") {
    return new Set(dataset.indices.map((index) => index.id));
  }
  return new Set(
    String(process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE_IDS || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)
  );
}

function getChangedPublishedPrefixIds(
  previousDataset: ValuationDataset | undefined,
  nextDataset: ValuationDataset
): Set<string> {
  const changedIds = new Set<string>();
  if (!previousDataset?.indices?.length) return changedIds;

  const nextById = new Map(nextDataset.indices.map((index) => [index.id, index]));
  const fields = ["date", "pe_ttm", "pe_forward", "pb", "us10y_yield"] as const;
  for (const previousIndex of previousDataset.indices) {
    const nextIndex = nextById.get(previousIndex.id);
    if (!nextIndex) continue;
    const changed = previousIndex.points.some((previousPoint, pointIndex) =>
      fields.some((field) => previousPoint[field] !== nextIndex.points[pointIndex]?.[field])
    );
    if (changed) changedIds.add(previousIndex.id);
  }
  return changedIds;
}

async function main(): Promise<void> {
  const previousDataset = await readPreviousDataset();
  const previousHistoryLock = await readIndexHistoryLock(previousDataset);
  if (previousDataset && previousHistoryLock) {
    assertDatasetMatchesIndexHistoryLock(previousDataset, previousHistoryLock);
  }

  const dataset = await generateDataset(undefined, { previousDataset });
  const authoritativeCorrections = await loadAuthoritativePublishedMetricCorrections();
  validateDataset(dataset);
  assertPublishedIndexHistoryAppendOnly(previousDataset, dataset, authoritativeCorrections);
  if (process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE !== "1") {
    assertValidatedIndexHistoryUnchanged(previousDataset, dataset);
  }
  if (previousHistoryLock) {
    const rewriteAllowedIds = getRewriteAllowedIds(dataset);
    for (const indexId of getChangedPublishedPrefixIds(previousDataset, dataset)) {
      // The strict point-by-point assertion above has already proved that any
      // changed locked field equals a persisted authoritative observation.
      rewriteAllowedIds.add(indexId);
    }
    assertDatasetMatchesIndexHistoryLock(dataset, previousHistoryLock, {
      allowAppendedPoints: true,
      rewriteAllowedIds,
    });
  }

  const nextHistoryLock = buildIndexHistoryLock(dataset);

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(OUTPUT_FILE, `${JSON.stringify(dataset, null, 2)}\n`, "utf8");
  await writeFile(HISTORY_LOCK_FILE, `${JSON.stringify(nextHistoryLock, null, 2)}\n`, "utf8");
  await syncPagesHistory(dataset);

  console.log(`snapshot written: ${OUTPUT_FILE}`);
  console.log(`generatedAt: ${dataset.generatedAt}`);
  console.log(`source: ${dataset.source}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
