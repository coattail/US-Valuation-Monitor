import test from "node:test";
import assert from "node:assert/strict";

globalThis.window = {
  location: {
    protocol: "http:",
    hostname: "127.0.0.1",
  },
  __USVM_APP_TEST__: true,
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
  alignCompareSeriesToCommonRangeForTest,
  buildSnapshotRowForTest,
  formatAxisDateForWidthForTest,
  getMetricSeriesForTest,
  toFiniteNumberForTest,
} = await import("./app.js");

test("overview cards use the real TTM coverage instead of the index price range", () => {
  const row = buildSnapshotRowForTest({
    id: "russell2000",
    symbol: "IWM",
    displayName: "Russell 2000",
    group: "core",
    points: [
      { date: "2001-01-03", pe_ttm: null, pe_forward: null, pb: 2.9 },
      { date: "2022-06-30", pe_ttm: 48.59, pe_forward: 18.29, pb: 1.8 },
      { date: "2025-07-16", pe_ttm: 32.8, pe_forward: 25.2, pb: 2.1 },
      { date: "2026-07-16", pe_ttm: 38.7, pe_forward: 31.48, pb: 2.16 },
    ],
  });

  assert.equal(row.ttmStartDate, "2022-06-30");
  assert.equal(row.ttmEndDate, "2026-07-16");
  assert.equal(row.ttmPointCount, 3);
  assert.equal(row.percentile_full, 2 / 3);
});

test("overview cards keep unavailable TTM values missing", () => {
  const row = buildSnapshotRowForTest({
    id: "smh",
    symbol: "SMH",
    displayName: "Semiconductors",
    group: "theme",
    points: [
      { date: "2011-12-20", pe_ttm: null, pe_forward: null, pb: null },
      { date: "2026-07-16", pe_ttm: null, pe_forward: 24.73, pb: null },
    ],
  });

  assert.equal(row.pe_ttm, null);
  assert.equal(row.pe_forward, 24.73);
  assert.equal(row.percentile_full, null);
  assert.equal(row.ttmPointCount, 0);
  assert.equal(row.ttmStartDate, "");
});

test("toFiniteNumberForTest keeps null valuation fields missing", () => {
  assert.equal(toFiniteNumberForTest(null), null);
  assert.equal(toFiniteNumberForTest(undefined), null);
  assert.equal(toFiniteNumberForTest(""), null);
});

test("getMetricSeriesForTest skips missing PE values instead of plotting zeroes", () => {
  const rows = getMetricSeriesForTest(
    {
      forwardStartDate: "2020-01-17",
      points: [
        { date: "1999-12-31", pe_ttm: null, pe_forward: null, pb: 5.19 },
        { date: "2005-02-25", pe_ttm: 19.99, pe_forward: 10.31, pb: 2.92 },
      ],
    },
    "pe_ttm"
  );

  assert.deepEqual(rows.map((row) => [row.date, row.value]), [["2005-02-25", 19.99]]);
});

test("getMetricSeriesForTest keeps real forward PE history before forwardStartDate", () => {
  const rows = getMetricSeriesForTest(
    {
      forwardStartDate: "2020-01-17",
      points: [
        { date: "2005-02-25", pe_ttm: 19.99, pe_forward: 10.31, pb: 2.92 },
        { date: "2020-01-17", pe_ttm: 25.05, pe_forward: 18.7, pb: 3.73 },
      ],
    },
    "pe_forward"
  );

  assert.deepEqual(rows.map((row) => [row.date, row.value]), [
    ["2005-02-25", 10.31],
    ["2020-01-17", 18.7],
  ]);
});

test("compare percentiles are recomputed after series align to their common start", () => {
  const aligned = alignCompareSeriesToCommonRangeForTest([
    {
      indexId: "nasdaq100",
      rows: [
        { date: "2000-01-03", value: 100, percentile_full: 1 },
        { date: "2011-12-20", value: 10, percentile_full: 0.5 },
        { date: "2012-01-03", value: 20, percentile_full: 2 / 3 },
      ],
    },
    {
      indexId: "smh",
      rows: [
        { date: "2011-12-20", value: 15 },
        { date: "2012-01-03", value: 16 },
      ],
    },
  ]);

  assert.deepEqual(aligned.map((item) => item.rows[0].date), ["2011-12-20", "2011-12-20"]);
  assert.equal(aligned[0].rows.at(-1).percentile_full, 1);
  assert.equal(aligned[0].rows.at(-1).regime, "high");
});

test("chart dates shorten progressively for phone and tablet widths", () => {
  const date = Date.UTC(2026, 6, 16);
  assert.equal(formatAxisDateForWidthForTest(date, 390), "2026");
  assert.equal(formatAxisDateForWidthForTest(date, 390, 365), "07-16");
  assert.equal(formatAxisDateForWidthForTest(date, 820), "2026-07");
  assert.equal(formatAxisDateForWidthForTest(date, 1200), "2026-07-16");
});
