import test from "node:test";
import assert from "node:assert/strict";

import {
  assertValidatedIndexPointsUnchangedForTest,
  getIndexLiveSourceCutoverDateForTest,
  isLatestSnapshotDeviationAcceptableForTest,
  mergePublishedIndexHistoryAtCutoverForTest,
  mergeHistoricalSeriesAtCutover,
  preserveValidatedIndexHistoryForTest,
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

test("keeps the validated history boundary independent from an index source start date", () => {
  const validated = [
    point("2000-01-31", 58),
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 30.3),
  ];
  const regenerated = [
    point("2000-01-31", 98),
    point("2020-01-02", 41.1),
    point("2026-03-26", 55.8),
    point("2026-03-27", 31.4),
    point("2026-03-30", 31.8),
  ];

  const firstRefresh = preserveValidatedIndexHistoryForTest(validated, regenerated);
  assert.deepEqual(firstRefresh, [
    point("2000-01-31", 58),
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 31.4),
    point("2026-03-30", 31.8),
  ]);

  const secondRefresh = preserveValidatedIndexHistoryForTest(firstRefresh, [
    point("2000-01-31", 120),
    point("2020-01-02", 60),
    point("2026-03-26", 70),
    point("2026-03-27", 32),
    point("2026-03-31", 32.2),
  ]);
  assert.deepEqual(secondRefresh.slice(0, 3), validated.slice(0, 3));
  assert.deepEqual(secondRefresh.slice(3), [point("2026-03-27", 32), point("2026-03-31", 32.2)]);
});

test("fails the build guard if a validated historical valuation drifts", () => {
  const previous = [point("2020-01-02", 23.04), point("2026-03-27", 30.3)];
  const drifted = [point("2020-01-02", 41.1), point("2026-03-27", 31.4)];

  assert.throws(
    () => assertValidatedIndexPointsUnchangedForTest(previous, drifted),
    /validated index history changed.*2020-01-02.*pe_ttm/
  );
  assert.doesNotThrow(() =>
    assertValidatedIndexPointsUnchangedForTest(previous, [point("2020-01-02", 23.04), point("2026-03-27", 31.4)])
  );
});

test("freezes the first published history for new thematic indices", () => {
  const published = [point("2026-04-02", 12), point("2026-07-16", 10.8)];
  const rebased = [point("2026-04-02", 24), point("2026-07-16", 21.6), point("2026-07-17", 11)];

  assert.deepEqual(preserveValidatedIndexHistoryForTest(published, rebased, "dram"), [
    ...published,
    point("2026-07-17", 11),
  ]);
});

test("keeps complete generated history when an index is first published", () => {
  const generated = [point("2001-07-17", 30), point("2026-07-16", 35)];
  assert.deepEqual(preserveValidatedIndexHistoryForTest([], generated, "igv"), generated);
  assert.deepEqual(
    preserveValidatedIndexHistoryForTest([point("2026-03-27", 34)], generated, "igv"),
    generated
  );
  assert.doesNotThrow(() =>
    assertValidatedIndexPointsUnchangedForTest([point("2026-03-27", 34)], generated, "igv")
  );
  assert.deepEqual(
    mergePublishedIndexHistoryAtCutoverForTest(
      "igv",
      [point("2026-03-27", 34)],
      generated,
      "2026-03-27"
    ),
    generated
  );
});

test("rejects a dated WSJ PE snapshot that jumps away from the existing series", () => {
  const series = [{ date: "2026-03-20", value: 35.6, ts: Date.parse("2026-03-20T00:00:00Z") }];

  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 66.64, 0.35), false);
  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 38.75, 0.35), true);
});
