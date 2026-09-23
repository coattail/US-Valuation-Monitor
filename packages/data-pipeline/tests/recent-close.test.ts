import test from "node:test";
import assert from "node:assert/strict";
import { appendRecentCloses, refreshNasdaqPriceTail } from "../src/recent-close.ts";

const response = (rows) => JSON.stringify({ data: { tradesTable: { rows } } });
const history = [{ date: "2026-09-21", close: 227.38 }];

test("recent closes extend missing sessions without rewriting history or including intraday prices", () => {
  const result = appendRecentCloses(history, response([
    { date: "09/23/2026", close: "$225.22" },
    { date: "09/22/2026", close: "$228.87" },
    { date: "09/21/2026", close: "$999.99" },
  ]), "2026-09-22");
  assert.deepEqual(result.map(({date, close}) => ({date, close})), [
    ...history, { date: "2026-09-22", close: 228.87 },
  ]);
  assert.equal(result[1].ts, Date.parse("2026-09-22T00:00:00Z"));
});

test("missing, zero, negative and invalid prices cannot fabricate a new daily point", () => {
  for (const close of [null, "--", "N/A", "", "0", "-1"]) {
    assert.equal(appendRecentCloses(history, response([{ date: "09/22/2026", close }]), "2026-09-22").length, 1);
  }
});

test("stale history uses the correct instrument and a completed, dated window", async () => {
  for (const assetClass of ["stocks", "etf"] as const) {
    let requested = "";
    const result = await refreshNasdaqPriceTail(history, "SPY", "2026-09-22", assetClass, async (url) => {
      requested = url;
      return response([{ date: "09/22/2026", close: "773.38" }]);
    });
    const url = new URL(requested);
    assert.equal(url.searchParams.get("assetclass"), assetClass);
    assert.equal(url.searchParams.get("fromdate"), "2026-09-21");
    assert.ok(url.searchParams.get("fromdate")! < url.searchParams.get("todate")!);
    assert.equal(url.searchParams.get("todate"), "2026-09-22");
    assert.equal(result.at(-1)?.date, "2026-09-22");
  }
});

test("current history needs no extra request; unavailable source preserves the actual last date", async () => {
  await refreshNasdaqPriceTail(history, "NVDA", "2026-09-21", "stocks", async () => {
    assert.fail("unnecessary request");
  });
  for (const raw of ["invalid JSON", response([]), JSON.stringify({ status: { rCode: 400, bCodeMessage: [{ errorMessage: "Symbol not exists." }] } })]) {
    const result = await refreshNasdaqPriceTail(history, "NVDA", "2026-09-22", "stocks", async () => raw);
    assert.equal(result.at(-1)?.date, "2026-09-21");
  }
  const result = await refreshNasdaqPriceTail(history, "NVDA", "2026-09-22", "stocks", async () => { throw new Error("429"); });
  assert.equal(result.at(-1)?.date, "2026-09-21");
});
