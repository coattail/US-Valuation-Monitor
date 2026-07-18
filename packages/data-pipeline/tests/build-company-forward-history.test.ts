import test from "node:test";
import assert from "node:assert/strict";

import {
  applyPreferredForwardPeAnchorsForTest,
  buildPreviousCompanyForwardPeSeriesForTest,
  buildVendorCompanyForwardPeSeriesForTest,
  filterVendorForwardPeSeriesBeforeExistingStartForTest,
  parseVendorCompanyForwardPeCsvForTest,
} from "../src/build-company-snapshot.ts";

test("previously published forward PE history is retained as a fallback series", () => {
  assert.deepEqual(
    buildPreviousCompanyForwardPeSeriesForTest([
      { date: "2020-01-02", pe_forward: 19.75 },
      { date: "2020-01-03", pe_forward: null },
      { date: "2020-01-06", pe_forward: 20.125 },
    ]),
    [
      { date: "2020-01-02", ts: Date.parse("2020-01-02T00:00:00Z"), value: 19.75 },
      { date: "2020-01-06", ts: Date.parse("2020-01-06T00:00:00Z"), value: 20.125 },
    ]
  );
});

test("public forward PE anchors win when several sources align to the same trading day", () => {
  const payload = applyPreferredForwardPeAnchorsForTest(
    {
      anchors: [
        { date: "2015-12-30", pe_ttm: null, pe_forward: 18.9, pb: null, peg: null },
      ],
      latest: { pe_ttm: null, pe_forward: 27, pb: null, peg: null },
      source: "stockanalysis",
    },
    [
      { date: "2015-12-31", ts: Date.parse("2015-12-31T00:00:00Z"), value: 13.97 },
    ],
    [
      { date: "2015-12-29", ts: Date.parse("2015-12-29T00:00:00Z"), close: 1 },
      { date: "2015-12-31", ts: Date.parse("2015-12-31T00:00:00Z"), close: 1 },
      { date: "2016-01-04", ts: Date.parse("2016-01-04T00:00:00Z"), close: 1 },
    ]
  );

  assert.deepEqual(payload?.anchors, [
    { date: "2015-12-31", pe_ttm: null, pe_forward: 13.97, pb: null, peg: null },
  ]);
});

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
