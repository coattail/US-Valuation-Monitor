import test from "node:test";
import assert from "node:assert/strict";

import {
  buildVendorCompanyForwardPeSeriesForTest,
  filterVendorForwardPeSeriesBeforeExistingStartForTest,
  parseVendorCompanyForwardPeCsvForTest,
} from "../src/build-company-snapshot.ts";

test("vendor forward PE CSV accepts direct ratios and EPS-derived rows", () => {
  const bySymbol = parseVendorCompanyForwardPeCsvForTest(`
symbol,date,pe_forward,eps_fy1,source
AAPL,2020-01-02,19.75,,factset-pit
AAPL,2020-01-03,,4.25,factset-pit
MSFT,2020-01-02,24.1,,sp-capital-iq
`);

  const points = buildVendorCompanyForwardPeSeriesForTest(bySymbol.get("AAPL") || [], [
    { date: "2020-01-02", ts: Date.parse("2020-01-02T00:00:00Z"), close: 296.24 },
    { date: "2020-01-03", ts: Date.parse("2020-01-03T00:00:00Z"), close: 297.43 },
  ]);

  assert.deepEqual(points, [
    { date: "2020-01-02", ts: Date.parse("2020-01-02T00:00:00Z"), value: 19.75 },
    { date: "2020-01-03", ts: Date.parse("2020-01-03T00:00:00Z"), value: 69.9835 },
  ]);
  assert.equal(bySymbol.get("MSFT")?.[0]?.source, "sp-capital-iq");
});

test("vendor forward PE backfill stops before the existing source start date", () => {
  const vendorSeries = [
    { date: "2020-12-31", ts: Date.parse("2020-12-31T00:00:00Z"), value: 28.1 },
    { date: "2021-06-28", ts: Date.parse("2021-06-28T00:00:00Z"), value: 31.2 },
    { date: "2021-06-29", ts: Date.parse("2021-06-29T00:00:00Z"), value: 31.3 },
  ];

  assert.deepEqual(filterVendorForwardPeSeriesBeforeExistingStartForTest(vendorSeries, "2021-06-28"), [
    { date: "2020-12-31", ts: Date.parse("2020-12-31T00:00:00Z"), value: 28.1 },
  ]);
  assert.deepEqual(filterVendorForwardPeSeriesBeforeExistingStartForTest(vendorSeries, ""), vendorSeries);
});
