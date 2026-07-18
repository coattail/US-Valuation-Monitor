import test from "node:test";
import assert from "node:assert/strict";

import { midrankPercentileRank, percentileRank, zScore } from "../src/stats.ts";

test("percentileRank returns expected ratio", () => {
  const values = [10, 20, 30, 40, 50];
  assert.equal(percentileRank(values, 40), 0.8);
  assert.equal(percentileRank(values, 5), 0);
  assert.equal(percentileRank([], 1), 0.5);
});

test("midrankPercentileRank gives ties half weight and avoids absolute sample boundaries", () => {
  assert.equal(midrankPercentileRank([10, 20, 30], 30), 5 / 6);
  assert.equal(midrankPercentileRank([10, 10, 10], 10), 0.5);
  assert.equal(midrankPercentileRank([10, 20, 30], 10), 1 / 6);
});

test("zScore stays zero when sigma is zero", () => {
  assert.equal(zScore([10, 10, 10], 10), 0);
});

test("zScore positive for above mean", () => {
  const z = zScore([10, 11, 12, 13, 14], 14);
  assert.ok(z > 0.8);
});
