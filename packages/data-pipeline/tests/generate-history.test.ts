import test from "node:test";
import assert from "node:assert/strict";

import {
  getIndexLiveSourceCutoverDateForTest,
  isLatestSnapshotDeviationAcceptableForTest,
  mergeHistoricalSeriesAtCutover,
} from "../src/generate.ts";

function point(date: string, peTtm: number) {
  return {
    date,
    pe_ttm: peTtm,
    pe_forward: peTtm - 4,
    pb: 5,
    us10y_yield: 0.04,
  };
}

test("preserves validated history through the configured index cutover", () => {
  const previous = [point("2020-01-02", 23.04), point("2026-03-26", 30.2)];
  const next = [point("2020-01-02", 41.1), point("2026-03-26", 30.8), point("2026-03-27", 31.4)];

  assert.deepEqual(mergeHistoricalSeriesAtCutover(previous, next), [
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 31.4),
  ]);
  assert.equal(getIndexLiveSourceCutoverDateForTest("russell2000"), "2001-01-03");
});

test("rejects a dated WSJ PE snapshot that jumps away from the existing series", () => {
  const series = [{ date: "2026-03-20", value: 35.6, ts: Date.parse("2026-03-20T00:00:00Z") }];

  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 66.64, 0.35), false);
  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 38.75, 0.35), true);
});
