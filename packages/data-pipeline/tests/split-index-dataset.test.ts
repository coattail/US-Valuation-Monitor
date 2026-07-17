import test from "node:test";
import assert from "node:assert/strict";

import { buildSnapshotRowForTest } from "../src/split-index-dataset.ts";

test("snapshot metadata reports metric-specific TTM coverage", () => {
  const row = buildSnapshotRowForTest({
    id: "russell2000",
    symbol: "IWM",
    group: "core",
    displayName: "Russell 2000",
    points: [
      { date: "2001-01-03", pe_ttm: null, pe_forward: null, pb: 2.9, us10y_yield: 0.05 },
      { date: "2022-06-30", pe_ttm: 48.59, pe_forward: 18.29, pb: 1.8, us10y_yield: 0.03 },
      { date: "2026-07-16", pe_ttm: 38.7, pe_forward: 31.48, pb: 2.16, us10y_yield: 0.04 },
    ],
  });

  assert.equal(row.startDate, "2001-01-03");
  assert.equal(row.ttmStartDate, "2022-06-30");
  assert.equal(row.ttmEndDate, "2026-07-16");
  assert.equal(row.ttmPointCount, 2);
});

test("snapshot metadata does not coerce unavailable ratios to zero", () => {
  const row = buildSnapshotRowForTest({
    id: "smh",
    symbol: "SMH",
    group: "theme",
    displayName: "Semiconductors",
    points: [
      { date: "2011-12-20", pe_ttm: null, pe_forward: null, pb: null, us10y_yield: 0.02 },
      { date: "2026-07-16", pe_ttm: null, pe_forward: 24.73, pb: null, us10y_yield: 0.04 },
    ],
  });

  assert.equal(row.pe_ttm, null);
  assert.equal(row.pb, null);
  assert.equal(row.pe_forward, 24.73);
  assert.equal(row.percentile_full, null);
  assert.equal(row.pe_ttm_change_1y, null);
  assert.equal(row.ttmPointCount, 0);
});
