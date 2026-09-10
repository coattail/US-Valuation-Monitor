import test from "node:test";
import assert from "node:assert/strict";
import { regimeFromPercentile, regimeLabel, snapshotBadge, snapshotPercentile, percentileColor } from "./valuation-policy.js";

test("valuation bands have inclusive outer boundaries and an open middle interval", () => {
  for (const [percentile, expected] of [[0, "low"], [0.2, "low"], [0.20001, "neutral"], [0.79999, "neutral"], [0.8, "high"], [1, "high"]]) {
    assert.equal(regimeFromPercentile(percentile), expected);
  }
  assert.equal(regimeLabel("neutral"), "合理");
});

test("missing or invalid percentiles never produce an investment valuation signal", () => {
  for (const value of [null, undefined, NaN, Infinity, "", "0", -0.1, 1.1]) {
    assert.equal(regimeFromPercentile(value), "unavailable");
    assert.equal(percentileColor(value), "#96a2b3");
  }
});

test("overview classification follows ten-year data even when stored full-history regime disagrees", () => {
  const row = { pe_ttm: 20, percentile_10y: 0.18, percentile_full: 0.95, regime: "high" };
  assert.equal(snapshotPercentile(row), 0.18);
  assert.match(snapshotBadge(row), /class="badge low"/);
  assert.match(snapshotBadge(row), />低估</);
  assert.match(snapshotBadge({ ...row, percentile_10y: 0.6 }), />合理</);
  assert.match(snapshotBadge({ ...row, percentile_10y: 0.82 }), />高估</);
});

test("unavailable or nonpositive PE and absent ten-year history cannot fall back to full history", () => {
  for (const pe_ttm of [null, undefined, NaN, 0, -10]) {
    const row = { pe_ttm, percentile_10y: 0, percentile_full: 0.9 };
    assert.equal(snapshotPercentile(row), null);
    assert.match(snapshotBadge(row), />PE 不适用</);
  }
  const row = { pe_ttm: 20, percentile_10y: null, percentile_full: 0.9 };
  assert.equal(snapshotPercentile(row), null);
  assert.match(snapshotBadge(row), />分位缺失</);
});
