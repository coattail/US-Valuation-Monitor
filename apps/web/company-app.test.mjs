import test from "node:test";
import assert from "node:assert/strict";

globalThis.window = {
  location: {
    protocol: "http:",
    hostname: "127.0.0.1",
  },
  __USVM_COMPANY_APP_TEST__: true,
  addEventListener() {},
};
globalThis.localStorage = {
  getItem() {
    return null;
  },
  setItem() {},
  removeItem() {},
};
const noopElement = {
  addEventListener() {},
  append() {},
  classList: { toggle() {}, add() {}, remove() {} },
  dataset: {},
  getContext() {
    return null;
  },
  querySelectorAll() {
    return [];
  },
  style: { setProperty() {}, removeProperty() {} },
};
globalThis.document = {
  createElement() {
    return { ...noopElement, dataset: {} };
  },
  getElementById() {
    return { ...noopElement, options: [], value: "", innerHTML: "", textContent: "" };
  },
  head: noopElement,
  querySelectorAll() {
    return [];
  },
};
globalThis.fetch = async () => {
  throw new Error("fetch disabled in unit test");
};

const {
  buildMetricAvailabilityNoteForTest,
  buildZoomedPercentileSeriesDataForTest,
  filterRowsByZoomRangeForTest,
  metricValueFromRawForTest,
  fetchCompanySeriesForTest,
  recomputeRangeRollingStatsForTest,
} = await import("./company-app.js");

test("company chart ignores non-positive PE from stale datasets", () => {
  assert.equal(metricValueFromRawForTest({ pe_ttm: -2_400 }, "pe_ttm"), null);
  assert.equal(metricValueFromRawForTest({ pe_forward: 0 }, "pe_forward"), null);
  assert.equal(metricValueFromRawForTest({ pe_ttm: 25.28 }, "pe_ttm"), 25.28);
  assert.equal(metricValueFromRawForTest({ pb: -1.2 }, "pb"), -1.2);
});

test("buildMetricAvailabilityNoteForTest explains when selected range exceeds available history", () => {
  assert.equal(
    buildMetricAvailabilityNoteForTest({
      metric: "pe_forward",
      range: "10y",
      rows: [{ date: "2021-08-02" }, { date: "2026-04-27" }],
    }),
    "PE(Forward) 最早可用 2021-08-02；10Y 按实际可用区间展示。"
  );
});

test("buildMetricAvailabilityNoteForTest stays quiet when requested range is covered", () => {
  assert.equal(
    buildMetricAvailabilityNoteForTest({
      metric: "pe_forward",
      range: "3y",
      rows: [{ date: "2021-08-02" }, { date: "2026-04-27" }],
    }),
    ""
  );
});


test("fetchCompanySeriesForTest bypasses browser cache for split company series", async () => {
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options });
    return {
      ok: true,
      async json() {
        return {
          indexId: "company_nvda",
          symbol: "NVDA",
          displayName: "NVIDIA",
          points: [{ date: "2026-05-20", pe_ttm: 45.29 }],
        };
      },
    };
  };

  const payload = await fetchCompanySeriesForTest("company_nvda");

  assert.equal(payload.points.at(-1).pe_ttm, 45.29);
  assert.equal(calls[0].options?.cache, "no-store");
  assert.match(calls[0].url, /v=.+/);
});

test("zoomed detail percentile recomputes from the visible window", () => {
  const baseRows = recomputeRangeRollingStatsForTest(
    [
      { date: "2026-01-01", value: 10 },
      { date: "2026-01-02", value: 20 },
      { date: "2026-01-03", value: 30 },
      { date: "2026-01-04", value: 15 },
    ],
    "pe_ttm"
  );

  assert.equal(baseRows.at(-1).percentile_full, 0.5);

  const visibleRows = recomputeRangeRollingStatsForTest(
    filterRowsByZoomRangeForTest(baseRows, { start: 50, end: 100 }),
    "pe_ttm"
  );

  assert.deepEqual(visibleRows.map((row) => row.date), ["2026-01-03", "2026-01-04"]);
  assert.equal(visibleRows.at(-1).percentile_full, 0.5);
  assert.equal(visibleRows.at(-1).percentile_5y, 0.5);
});

test("zoomed detail percentile series blanks rows outside the visible window", () => {
  const baseRows = recomputeRangeRollingStatsForTest(
    [
      { date: "2026-01-01", value: 10 },
      { date: "2026-01-02", value: 20 },
      { date: "2026-01-03", value: 30 },
      { date: "2026-01-04", value: 15 },
    ],
    "pe_ttm"
  );
  const visibleRows = recomputeRangeRollingStatsForTest(
    filterRowsByZoomRangeForTest(baseRows, { start: 50, end: 100 }),
    "pe_ttm"
  );

  assert.deepEqual(
    buildZoomedPercentileSeriesDataForTest(baseRows, visibleRows).map((point) => point[1]),
    [null, null, 100, 50]
  );
});
