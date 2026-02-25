import test from "node:test";
import assert from "node:assert/strict";

import { buildMetricSeries, buildSnapshot, enrichSeries } from "../src/valuation.ts";

const rawPoints = [
  { date: "2026-01-02", pe_ttm: 18, pe_forward: 16, pb: 3.0, us10y_yield: 0.035 },
  { date: "2026-01-05", pe_ttm: 19, pe_forward: 16.8, pb: 3.1, us10y_yield: 0.0355 },
  { date: "2026-01-06", pe_ttm: 20, pe_forward: 17.6, pb: 3.2, us10y_yield: 0.036 },
  { date: "2026-01-07", pe_ttm: 21, pe_forward: 18.2, pb: 3.3, us10y_yield: 0.0365 },
];

test("enrichSeries appends derived fields", () => {
  const rows = enrichSeries(rawPoints);
  assert.equal(rows.length, 4);
  assert.ok(rows[3].earnings_yield > 0);
  assert.ok(["low", "neutral", "high"].includes(rows[3].regime));
});

test("buildMetricSeries respects date filter", () => {
  const rows = buildMetricSeries(rawPoints, "pe_ttm", "2026-01-05", "2026-01-06");
  assert.equal(rows.length, 2);
  assert.equal(rows[0].date, "2026-01-05");
  assert.equal(rows[1].date, "2026-01-06");
});

test("buildMetricSeries supports pe_forward", () => {
  const rows = buildMetricSeries(rawPoints, "pe_forward", "2026-01-05", "2026-01-07");
  assert.equal(rows.length, 3);
  assert.ok(rows.every((item) => item.value > 0));
});

test("buildMetricSeries percentile uses filtered range only", () => {
  const points = [
    { date: "2026-01-02", pe_ttm: 50, pe_forward: 48, pb: 6, us10y_yield: 0.03 },
    { date: "2026-01-03", pe_ttm: 10, pe_forward: 9, pb: 2, us10y_yield: 0.03 },
    { date: "2026-01-04", pe_ttm: 20, pe_forward: 18, pb: 3, us10y_yield: 0.03 },
  ];

  const rows = buildMetricSeries(points, "pe_ttm", "2026-01-03", "2026-01-04");
  assert.equal(rows.length, 2);
  assert.equal(rows[0].percentile_full, 1);
  assert.equal(rows[1].percentile_full, 1);
});

test("buildSnapshot returns latest row", () => {
  const snapshot = buildSnapshot("sp500", rawPoints);
  assert.equal(snapshot.date, "2026-01-07");
  assert.equal(snapshot.indexId, "sp500");
});
