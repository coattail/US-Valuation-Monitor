import test from "node:test";
import assert from "node:assert/strict";

import type { ValuationDataset } from "../../core/src/types.ts";
import {
  assertDatasetMatchesIndexHistoryLock,
  buildIndexHistoryLock,
  hashIndexHistoryPoints,
} from "../src/index-history-lock.ts";

function dataset(): ValuationDataset {
  return {
    generatedAt: "2026-07-16T00:00:00.000Z",
    source: "test",
    indices: [
      {
        id: "nasdaq100",
        symbol: "QQQ",
        group: "core",
        displayName: "Nasdaq 100",
        description: "test",
        points: [
          { date: "2026-07-15", pe_ttm: 34.1305, pe_forward: 25.2269, pb: 8.1053, us10y_yield: 0.0455 },
          { date: "2026-07-16", pe_ttm: 33.5694, pe_forward: 24.8122, pb: 7.972, us10y_yield: 0.0455 },
        ],
      },
    ],
  };
}

test("history lock is deterministic and accepts a strictly appended suffix", () => {
  const previous = dataset();
  const lock = buildIndexHistoryLock(previous);
  assert.equal(lock.indices.nasdaq100.sha256, hashIndexHistoryPoints(previous.indices[0].points));
  assert.doesNotThrow(() => assertDatasetMatchesIndexHistoryLock(previous, lock));

  const appended = structuredClone(previous);
  appended.indices[0].points.push({
    date: "2026-07-17",
    pe_ttm: 33.8,
    pe_forward: 25,
    pb: 8.01,
    us10y_yield: 0.0454,
  });
  assert.doesNotThrow(() =>
    assertDatasetMatchesIndexHistoryLock(appended, lock, { allowAppendedPoints: true })
  );
});

test("history lock rejects edits, deletions, and inserted older dates", () => {
  const previous = dataset();
  const lock = buildIndexHistoryLock(previous);

  const edited = structuredClone(previous);
  edited.indices[0].points[0].pe_ttm = 99;
  assert.throws(() => assertDatasetMatchesIndexHistoryLock(edited, lock), /checksum mismatch.*nasdaq100/);

  const shortened = structuredClone(previous);
  shortened.indices[0].points.pop();
  assert.throws(() => assertDatasetMatchesIndexHistoryLock(shortened, lock), /point count shrank.*nasdaq100/);

  const inserted = structuredClone(previous);
  inserted.indices[0].points.push({
    date: "2026-07-14",
    pe_ttm: 33,
    pe_forward: 24,
    pb: 7.9,
    us10y_yield: 0.045,
  });
  assert.throws(
    () => assertDatasetMatchesIndexHistoryLock(inserted, lock, { allowAppendedPoints: true }),
    /non-appended date.*nasdaq100/
  );
});
