import { createHash } from "node:crypto";

import type { RawValuationPoint, ValuationDataset } from "../../core/src/types.ts";

export const INDEX_HISTORY_LOCK_VERSION = 1;
export const INDEX_HISTORY_LOCK_ALGORITHM = "sha256";
export const INDEX_HISTORY_LOCK_FIELDS = ["date", "pe_ttm", "pe_forward", "pb", "us10y_yield"] as const;

export interface IndexHistoryLockEntry {
  pointCount: number;
  firstDate: string;
  lastDate: string;
  sha256: string;
}

export interface IndexHistoryLock {
  version: number;
  algorithm: typeof INDEX_HISTORY_LOCK_ALGORITHM;
  fields: typeof INDEX_HISTORY_LOCK_FIELDS;
  generatedAt: string;
  indices: Record<string, IndexHistoryLockEntry>;
}

function canonicalPoint(point: RawValuationPoint): [string, number | null, number | null, number | null, number] {
  return [point.date, point.pe_ttm, point.pe_forward, point.pb, point.us10y_yield];
}

export function hashIndexHistoryPoints(points: RawValuationPoint[]): string {
  const canonical = points.map(canonicalPoint);
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

export function buildIndexHistoryLock(dataset: ValuationDataset): IndexHistoryLock {
  const indices: Record<string, IndexHistoryLockEntry> = {};
  for (const index of [...dataset.indices].sort((a, b) => a.id.localeCompare(b.id))) {
    const points = index.points || [];
    indices[index.id] = {
      pointCount: points.length,
      firstDate: points[0]?.date || "",
      lastDate: points[points.length - 1]?.date || "",
      sha256: hashIndexHistoryPoints(points),
    };
  }

  return {
    version: INDEX_HISTORY_LOCK_VERSION,
    algorithm: INDEX_HISTORY_LOCK_ALGORITHM,
    fields: INDEX_HISTORY_LOCK_FIELDS,
    generatedAt: dataset.generatedAt,
    indices,
  };
}

export function validateIndexHistoryLock(lock: IndexHistoryLock): void {
  if (!lock || lock.version !== INDEX_HISTORY_LOCK_VERSION) {
    throw new Error(`unsupported index history lock version: ${String(lock?.version)}`);
  }
  if (lock.algorithm !== INDEX_HISTORY_LOCK_ALGORITHM) {
    throw new Error(`unsupported index history lock algorithm: ${String(lock.algorithm)}`);
  }
  if (JSON.stringify(lock.fields) !== JSON.stringify(INDEX_HISTORY_LOCK_FIELDS)) {
    throw new Error("index history lock fields do not match the canonical schema");
  }
  if (!lock.indices || typeof lock.indices !== "object" || !Object.keys(lock.indices).length) {
    throw new Error("index history lock has no index entries");
  }
}

export function assertDatasetMatchesIndexHistoryLock(
  dataset: ValuationDataset,
  lock: IndexHistoryLock,
  options: { allowAppendedPoints?: boolean; rewriteAllowedIds?: Set<string> } = {}
): void {
  validateIndexHistoryLock(lock);
  const allowAppendedPoints = options.allowAppendedPoints === true;
  const rewriteAllowedIds = options.rewriteAllowedIds || new Set<string>();
  const datasetById = new Map(dataset.indices.map((index) => [index.id, index]));

  for (const [indexId, entry] of Object.entries(lock.indices)) {
    if (rewriteAllowedIds.has(indexId)) continue;
    const index = datasetById.get(indexId);
    if (!index) throw new Error(`index history lock missing dataset index: ${indexId}`);
    if (index.points.length < entry.pointCount) {
      throw new Error(
        `index history lock point count shrank for ${indexId}: ${entry.pointCount} -> ${index.points.length}`
      );
    }
    if (!allowAppendedPoints && index.points.length !== entry.pointCount) {
      throw new Error(
        `index history lock point count changed for ${indexId}: ${entry.pointCount} -> ${index.points.length}`
      );
    }

    const lockedPoints = index.points.slice(0, entry.pointCount);
    const firstDate = lockedPoints[0]?.date || "";
    const lastDate = lockedPoints[lockedPoints.length - 1]?.date || "";
    if (firstDate !== entry.firstDate || lastDate !== entry.lastDate) {
      throw new Error(
        `index history lock date boundary changed for ${indexId}: ${entry.firstDate}/${entry.lastDate} -> ${firstDate}/${lastDate}`
      );
    }
    const actualHash = hashIndexHistoryPoints(lockedPoints);
    if (actualHash !== entry.sha256) {
      throw new Error(`index history lock checksum mismatch for ${indexId}`);
    }

    if (allowAppendedPoints) {
      const invalidAppend = index.points.slice(entry.pointCount).find((point) => point.date <= entry.lastDate);
      if (invalidAppend) {
        throw new Error(`index history lock detected a non-appended date for ${indexId}: ${invalidAppend.date}`);
      }
    }
  }
}
