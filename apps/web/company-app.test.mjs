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

const { buildMetricAvailabilityNoteForTest } = await import("./company-app.js");

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
