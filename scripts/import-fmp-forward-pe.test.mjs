import test from "node:test";
import assert from "node:assert/strict";

import { buildFmpAnalystEstimatesUrl, estimateRowsToVendorCsvRows, parseCliArgs } from "./import-fmp-forward-pe.mjs";

test("estimateRowsToVendorCsvRows maps FMP EPS estimates into vendor CSV rows", () => {
  const rows = estimateRowsToVendorCsvRows("AAPL", [
    {
      symbol: "AAPL",
      date: "2020-12-31",
      estimatedEpsAvg: 3.28,
      numberAnalystsEstimatedEps: 21,
    },
    {
      symbol: "AAPL",
      date: "2019-12-31",
      estimatedEpsAvg: null,
    },
    {
      symbol: "AAPL",
      date: "2018-12-31",
      estimatedEpsHigh: 4.1,
    },
  ]);

  assert.deepEqual(rows, [
    {
      symbol: "AAPL",
      date: "2020-12-31",
      pe_forward: "",
      eps_fy1: "3.28",
      source: "fmp-analyst-estimates-non-pit",
    },
  ]);
});

test("buildFmpAnalystEstimatesUrl uses the stable endpoint", () => {
  const url = buildFmpAnalystEstimatesUrl({
    symbol: "MSFT",
    apiKey: "test-key",
    period: "annual",
    page: 1,
    limit: 50,
  });

  assert.equal(
    url,
    "https://financialmodelingprep.com/stable/analyst-estimates?symbol=MSFT&period=annual&page=1&limit=50&apikey=test-key"
  );
});

test("parseCliArgs accepts symbol and output options", () => {
  assert.deepEqual(parseCliArgs(["--symbols", "AAPL,MSFT", "--output", "/tmp/fmp.csv", "--limit", "20"]), {
    symbols: ["AAPL", "MSFT"],
    output: "/tmp/fmp.csv",
    period: "annual",
    limit: 20,
    page: 0,
    dryRun: false,
  });
});
