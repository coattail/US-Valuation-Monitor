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
  buildLineSeriesDataWithGapsForTest,
  buildMetricAvailabilityNoteForTest,
  buildZoomedPercentileSeriesDataForTest,
  filterRowsByZoomRangeForTest,
  metricValueFromRawForTest,
  fetchCompanySeriesForTest,
  recomputeRangeRollingStatsForTest,
  formatAxisDateForWidthForTest,
  formatPercentileForTest,
} = await import("./company-app.js");

test("company percentiles do not round non-boundary values to absolute 0% or 100%", () => {
  assert.equal(formatPercentileForTest(0.9996022275, 1), "99.9%");
  assert.equal(formatPercentileForTest(0.0003977725, 1), "0.1%");
  assert.equal(formatPercentileForTest(0.5, 1), "50.0%");
});

test("company chart dates shorten progressively for phone and tablet widths", () => {
  const date = Date.UTC(2026, 6, 16);
  assert.equal(formatAxisDateForWidthForTest(date, 390), "2026");
  assert.equal(formatAxisDateForWidthForTest(date, 390, 365), "07-16");
  assert.equal(formatAxisDateForWidthForTest(date, 820), "2026-07");
  assert.equal(formatAxisDateForWidthForTest(date, 1200), "2026-07-16");
});

test("line series inserts a null break across a long unavailable PE period", () => {
  const data = buildLineSeriesDataWithGapsForTest([
    { date: "2009-04-27", value: 18 },
    { date: "2010-04-30", value: 34 },
  ]);

  assert.equal(data.length, 3);
  assert.deepEqual(data[1], [Date.parse("2009-04-28T00:00:00Z"), null]);
  assert.equal(data[0][1], 18);
  assert.equal(data[2][1], 34);
});

test("line series keeps normal market closures connected", () => {
  const data = buildLineSeriesDataWithGapsForTest([
    { date: "2026-07-02", value: 20 },
    { date: "2026-07-06", value: 21 },
  ]);

  assert.equal(data.length, 2);
});

test("line series connects across a PE sign change", () => {
  const data = buildLineSeriesDataWithGapsForTest([
    { date: "2009-04-29", value: -65 },
    { date: "2009-04-30", value: 40 },
  ]);

  assert.equal(data.length, 2);
  assert.deepEqual(data.map((point) => point[1]), [-65, 40]);
});

test("company chart keeps signed PE and ignores only zero", () => {
  assert.equal(metricValueFromRawForTest({ pe_ttm: -2_400 }, "pe_ttm"), -2_400);
  assert.equal(metricValueFromRawForTest({ pe_forward: 0 }, "pe_forward"), null);
  assert.equal(metricValueFromRawForTest({ pe_ttm: 25.28 }, "pe_ttm"), 25.28);
  assert.equal(metricValueFromRawForTest({ pb: -1.2 }, "pb"), -1.2);
});

test("negative PE ranks above every positive PE in percentile stats", () => {
  const rows = recomputeRangeRollingStatsForTest(
    [
      { date: "2026-01-01", value: 10 },
      { date: "2026-01-02", value: 100 },
      { date: "2026-01-03", value: -100 },
    ],
    "pe_ttm"
  );

  assert.equal(rows.at(-1).percentile_full, 5 / 6);
});

test("negative PE closer to zero ranks as more expensive", () => {
  const rows = recomputeRangeRollingStatsForTest(
    [
      { date: "2026-01-01", value: -100 },
      { date: "2026-01-02", value: -10 },
    ],
    "pe_ttm"
  );

  assert.equal(rows.at(-1).percentile_full, 0.75);
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

  assert.equal(baseRows.at(-1).percentile_full, 0.375);

  const visibleRows = recomputeRangeRollingStatsForTest(
    filterRowsByZoomRangeForTest(baseRows, { start: 50, end: 100 }),
    "pe_ttm"
  );

  assert.deepEqual(visibleRows.map((row) => row.date), ["2026-01-03", "2026-01-04"]);
  assert.equal(visibleRows.at(-1).percentile_full, 0.25);
  assert.equal(visibleRows.at(-1).percentile_5y, 0.25);
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
    [null, null, 50, 25]
  );
});
