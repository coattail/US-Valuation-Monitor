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

test("Nasdaq share-class symbols use dots for Yahoo-style dash tickers", async () => {
  const result = await refreshNasdaqPriceTail(history, "BRK-B", "2026-09-22", "stocks", async (url) => {
    assert.equal(new URL(url).pathname, "/api/quote/BRK.B/historical");
    return response([{ date: "09/22/2026", close: "$503.49" }]);
  });
  assert.equal(result.at(-1)?.close, 503.49);
});

test("delayed historical table falls back to a dated closed primary quote", async () => {
  const calls: string[] = [];
  const original = structuredClone(history);
  const result = await refreshNasdaqPriceTail(history, "SPY", "2026-09-22", "etf", async url => {
    calls.push(url);
    if (url.includes('/historical')) return response([{date:'09/21/2026',close:'999'}]);
    return JSON.stringify({status:{rCode:200},data:{symbol:'SPY',marketStatus:'Closed',primaryData:{
      lastTradeTimestamp:'Sep 22, 2026',lastSalePrice:'$228.87',isRealTime:false,
    },secondaryData:{lastSalePrice:'$999'}}});
  });
  assert.equal(calls.length, 2);
  assert.equal(new URL(calls[1]).searchParams.get('assetclass'), 'etf');
  assert.equal(result.at(-1)?.close, 228.87);
  assert.deepEqual(history, original);
});

test("closed quote rejects stale, realtime, intraday, mismatched and invalid observations", async () => {
  const { parseNasdaqClosedQuote } = await import('../src/recent-close.ts');
  const base = {symbol:'BRK.B',marketStatus:'Closed',primaryData:{lastTradeTimestamp:'Sep 22, 2026',lastSalePrice:'$503.49',isRealTime:false}};
  const parse = data => parseNasdaqClosedQuote(JSON.stringify({status:{rCode:200},data}), 'BRK-B', '2026-09-22');
  assert.equal(parse(base)[0].close, 503.49);
  for (const data of [
    {...base,symbol:'SPY'}, {...base,marketStatus:'Open'}, {...base,marketStatus:'Pre-Market'},
    ...[
      {isRealTime:true}, {lastTradeTimestamp:'Sep 21, 2026'},
      {lastTradeTimestamp:'Sep 22, 2026 4:05 PM ET'}, {lastTradeTimestamp:'Sep 99, 2026'},
      {lastSalePrice:'N/A'}, {lastSalePrice:'0'}, {lastSalePrice:'-1'},
    ].map(change => ({...base,primaryData:{...base.primaryData,...change}})),
  ]) assert.deepEqual(parse(data), []);
});
