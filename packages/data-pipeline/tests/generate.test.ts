import test from "node:test";
import assert from "node:assert/strict";

import {
  applyMetricPointAnchorsForTest,
  applyMetricCloseCarryWithAnchorsForTest,
  applyCloseAnchoredOverridesForTest,
  applyMetricCloseCarryFromAnchorSeriesForTest,
  applyYahooSnapshotCarryToMetricForTest,
  applyRecentCloseCarryWindowToMetricForTest,
  applyPostCutoverMetricSources,
  applyMetricAnchorSeriesForTest,
  buildEffectiveIndexYahooDailyMetricSnapshotsForTest,
  buildYahooChartUrlsForTest,
  collapseRedundantExplicitLatestSnapshotsForTest,
  extendSeriesWithRebasedPreviousTailForTest,
  getMacroMicroSp500PbRoutesForTest,
  getMultplSp500PbUrlForTest,
  getIndexLiveSourceCutoverDateForTest,
  mergeHistoricalSeriesAtCutover,
  parseIsharesPortfolioMetricsForTest,
  parseMultplTableSeriesForTest,
  parseStockMarketPeRatioCurrentForTest,
  parseStockMarketPeRatioSeriesForTest,
  parseSsgaIndexMetricsForTest,
  parseWsjPeSnapshotFromTextForTest,
  pickAnchorForwardPeForTest,
  parseYahooChartCloseSeries,
  parseYchartsPbSeriesForTest,
  repairHistoryFallbackPointsForTest,
  rebaseHistoryFallbackWithSnapshotsForTest,
  pruneInvalidExplicitIndexSnapshotsForTest,
  pruneImplausibleForwardSeriesForTest,
  prependHistoricalPbPrefixForTest,
  shouldFetchMultplSp500PeFallbackForTest,
  upsertIndexYahooDailyMetricSnapshotForTest,
} from "../src/generate.ts";

test("parseStockMarketPeRatioSeriesForTest parses monthly S&P 500 TTM PE anchors", () => {
  const js = `
    google.visualization.arrayToDataTable([
      ['Month Ending', 'PE Ratio', 'Average Since 1990'],
      [new Date(1990,1,1),15.12,23.58],
      [new Date(1990,12,1),15.41,23.58],
      [new Date(2026,3,1),25.83,23.58],
    ]);
  `;

  const series = parseStockMarketPeRatioSeriesForTest(js);

  assert.deepEqual(series, [
    { date: "1990-01-31", value: 15.12, ts: Date.parse("1990-01-31T00:00:00Z") },
    { date: "1990-12-31", value: 15.41, ts: Date.parse("1990-12-31T00:00:00Z") },
    { date: "2026-03-31", value: 25.83, ts: Date.parse("2026-03-31T00:00:00Z") },
  ]);
});

test("parseStockMarketPeRatioCurrentForTest parses current S&P 500 TTM PE anchor", () => {
  const html = `
    <td id="trailingPE" class="table-important">25.83</td>
    <p id="date" class="footnote">Data as of 2026-03-31 15:00 CST</p>
  `;

  assert.deepEqual(parseStockMarketPeRatioCurrentForTest(html), {
    date: "2026-03-31",
    value: 25.83,
    ts: Date.parse("2026-03-31T00:00:00Z"),
  });
});

test("mergeHistoricalSeriesAtCutover keeps prior history before cutover and uses new points from cutover onward", () => {
  const previous = [
    { date: "2026-03-26", pe_ttm: 28.1, pe_forward: 24.1, pb: 5.1, us10y_yield: 0.041 },
    { date: "2026-03-27", pe_ttm: 28.9, pe_forward: 24.9, pb: 5.2, us10y_yield: 0.0412 },
  ];
  const next = [
    { date: "2026-03-27", pe_ttm: 30.5, pe_forward: 25.5, pb: 5.6, us10y_yield: 0.0412 },
    { date: "2026-03-31", pe_ttm: 31.2, pe_forward: 26.1, pb: 5.8, us10y_yield: 0.0421 },
    { date: "2026-04-01", pe_ttm: 31.6, pe_forward: 26.4, pb: 5.9, us10y_yield: 0.0423 },
  ];

  const merged = mergeHistoricalSeriesAtCutover(previous, next, "2026-03-27");

  assert.deepEqual(
    merged.map((point) => point.date),
    ["2026-03-26", "2026-03-27", "2026-03-31", "2026-04-01"]
  );
  assert.equal(merged[1].pe_ttm, 30.5);
  assert.equal(merged[2].pe_ttm, 31.2);
});

test("applyMetricAnchorSeriesForTest repairs pre-cutover PB from real anchor series", () => {
  const points = [
    { date: "2007-12-31", pe_ttm: 19.28, pe_forward: 29.48, pb: 9.25, us10y_yield: 0.0404 },
    { date: "2008-01-02", pe_ttm: 21.27, pe_forward: 19.28, pb: 5.2, us10y_yield: 0.0391 },
    { date: "2008-03-31", pe_ttm: 22.1, pe_forward: 18.2, pb: 2.75, us10y_yield: 0.035 },
  ];
  const anchors = [
    { date: "2007-12-31", value: 2.82, ts: Date.parse("2007-12-31T00:00:00Z") },
    { date: "2008-03-31", value: 2.75, ts: Date.parse("2008-03-31T00:00:00Z") },
  ];

  const repaired = applyMetricAnchorSeriesForTest(points, "pb", anchors, {
    minValue: 0.2,
    maxValue: 28,
    maxInterpolationSpanDays: 180,
    maxForwardFillDays: 120,
    maxBackFillDays: 120,
  });

  assert.equal(repaired[0].pb, 2.82);
  assert.ok(repaired[1].pb < 3);
});

test("applyMetricAnchorSeriesForTest repairs PB after history fallback merge", () => {
  const previous = [
    { date: "2007-12-31", pe_ttm: 19.28, pe_forward: 29.48, pb: 9.25, us10y_yield: 0.0404 },
  ];
  const next = [
    { date: "2008-01-02", pe_ttm: 21.27, pe_forward: 19.28, pb: 5.2, us10y_yield: 0.0391 },
    { date: "2008-03-31", pe_ttm: 22.1, pe_forward: 18.2, pb: 2.75, us10y_yield: 0.035 },
  ];
  const anchors = [
    { date: "2007-12-31", value: 2.82, ts: Date.parse("2007-12-31T00:00:00Z") },
    { date: "2008-03-31", value: 2.75, ts: Date.parse("2008-03-31T00:00:00Z") },
  ];

  const merged = mergeHistoricalSeriesAtCutover(previous, next, "2008-01-02");
  const repaired = applyMetricAnchorSeriesForTest(merged, "pb", anchors, {
    minValue: 0.2,
    maxValue: 28,
    maxInterpolationSpanDays: 180,
    maxForwardFillDays: 120,
    maxDate: "2008-01-02",
  });

  assert.equal(repaired[0].pb, 2.82);
  assert.ok(repaired[1].pb < 3);
});

test("applyMetricCloseCarryFromAnchorSeriesForTest carries PB by daily close returns between anchors", () => {
  const points = [
    { date: "2026-03-30", pe_ttm: 20, pe_forward: 18, pb: 4, us10y_yield: 0.04 },
    { date: "2026-03-31", pe_ttm: 20, pe_forward: 18, pb: 4, us10y_yield: 0.04 },
    { date: "2026-04-01", pe_ttm: 20, pe_forward: 18, pb: 4, us10y_yield: 0.04 },
    { date: "2026-04-02", pe_ttm: 20, pe_forward: 18, pb: 4, us10y_yield: 0.04 },
  ];
  const closes = [
    { date: "2026-03-30", close: 100 },
    { date: "2026-03-31", close: 102 },
    { date: "2026-04-01", close: 105 },
    { date: "2026-04-02", close: 110 },
  ];
  const anchors = [
    { date: "2026-03-31", value: 2, ts: Date.parse("2026-03-31T00:00:00Z") },
    { date: "2026-04-04", value: 2.1, ts: Date.parse("2026-04-04T00:00:00Z") },
  ];

  const carried = applyMetricCloseCarryFromAnchorSeriesForTest(points, closes, "pb", anchors, {
    minValue: 0.2,
    maxValue: 28,
    maxAnchorLagDays: 5,
    maxForwardFillDays: 10,
  });

  assert.equal(carried[0].pb, 4);
  assert.equal(carried[1].pb, 2);
  assert.equal(carried[2].pb, 2.0588);
  assert.equal(carried[3].pb, 2.1);
});

test("applyMetricCloseCarryFromAnchorSeriesForTest can rebuild S&P 500 TTM PE before cutover", () => {
  const points = [
    { date: "2007-12-31", pe_ttm: 40, pe_forward: 18, pb: 3, us10y_yield: 0.04 },
    { date: "2008-01-02", pe_ttm: 41, pe_forward: 18, pb: 3, us10y_yield: 0.04 },
    { date: "2008-01-03", pe_ttm: 42, pe_forward: 18, pb: 3, us10y_yield: 0.04 },
  ];
  const closes = [
    { date: "2007-12-31", close: 100 },
    { date: "2008-01-02", close: 99 },
    { date: "2008-01-03", close: 98 },
  ];
  const anchors = [{ date: "2007-12-31", value: 21, ts: Date.parse("2007-12-31T00:00:00Z") }];

  const repaired = applyMetricCloseCarryFromAnchorSeriesForTest(points, closes, "pe_ttm", anchors, {
    minValue: 2.4,
    maxValue: 180,
    maxAnchorLagDays: 5,
    maxForwardFillDays: 30,
  });

  assert.equal(repaired[0].pe_ttm, 21);
  assert.equal(repaired[1].pe_ttm, 20.79);
  assert.equal(repaired[2].pe_ttm, 20.58);
});

test("applyMetricCloseCarryFromAnchorSeriesForTest seeds from the latest anchor before the first point", () => {
  const points = [
    { date: "2005-02-25", pe_ttm: 47.5, pe_forward: 18, pb: 3, us10y_yield: 0.04 },
    { date: "2005-02-28", pe_ttm: 48, pe_forward: 18, pb: 3, us10y_yield: 0.04 },
  ];
  const closes = [
    { date: "2005-02-25", close: 100 },
    { date: "2005-02-28", close: 101 },
  ];
  const anchors = [
    { date: "2005-01-31", value: 20, ts: Date.parse("2005-01-31T00:00:00Z") },
    { date: "2005-02-28", value: 20.11, ts: Date.parse("2005-02-28T00:00:00Z") },
  ];

  const repaired = applyMetricCloseCarryFromAnchorSeriesForTest(points, closes, "pe_ttm", anchors, {
    minValue: 2.4,
    maxValue: 180,
    maxAnchorLagDays: 5,
    maxForwardFillDays: 45,
  });

  assert.equal(repaired[0].pe_ttm, 20);
  assert.equal(repaired[1].pe_ttm, 20.11);
});

test("prependHistoricalPbPrefixForTest prepends only the requested pre-2005 S&P 500 PB window", () => {
  const existing = [
    { date: "2005-02-25", pe_ttm: 19.99, pe_forward: 10.31, pb: 2.92, us10y_yield: 0.0427 },
    { date: "2005-02-28", pe_ttm: 20.11, pe_forward: 11.84, pb: 2.9, us10y_yield: 0.0436 },
  ];
  const closes = [
    { date: "1999-12-30", close: 100 },
    { date: "1999-12-31", close: 101 },
    { date: "2000-01-03", close: 102 },
    { date: "2005-02-24", close: 110 },
    { date: "2005-02-25", close: 111 },
  ];
  const pbAnchors = [
    { date: "1999-12-31", value: 4, ts: Date.parse("1999-12-31T00:00:00Z") },
    { date: "2000-03-31", value: 4.1, ts: Date.parse("2000-03-31T00:00:00Z") },
  ];

  const merged = prependHistoricalPbPrefixForTest(existing, closes, pbAnchors, {
    startDate: "1999-12-31",
    endDate: "2005-02-24",
  });

  assert.deepEqual(merged.map((point) => point.date), ["1999-12-31", "2000-01-03", "2005-02-24", "2005-02-25", "2005-02-28"]);
  assert.equal(merged[0].pb, 4);
  assert.equal(merged[0].pe_ttm, null);
  assert.equal(merged[0].pe_forward, null);
  assert.equal(merged[1].pb, 4.0396);
  assert.equal(merged[2].pb, 4.3564);
  assert.equal(merged[3].pb, 2.92);
});

test("applyMetricCloseCarryFromAnchorSeriesForTest rebuilds S&P 500 forward PE from public anchors", () => {
  const points = [
    { date: "2019-12-31", pe_ttm: 22.78, pe_forward: 26.1877, pb: 3.53, us10y_yield: 0.0192 },
    { date: "2020-01-02", pe_ttm: 22.99, pe_forward: 26.6229, pb: 3.56, us10y_yield: 0.0188 },
    { date: "2020-01-17", pe_ttm: 23.49, pe_forward: 27.2094, pb: 3.64, us10y_yield: 0.0183 },
    { date: "2020-01-21", pe_ttm: 23.45, pe_forward: 27.1373, pb: 3.63, us10y_yield: 0.0178 },
  ];
  const closes = [
    { date: "2019-12-31", close: 3230.78 },
    { date: "2020-01-02", close: 3257.85 },
    { date: "2020-01-17", close: 3329.62 },
    { date: "2020-01-21", close: 3320.79 },
  ];
  const anchors = [{ date: "2020-01-17", value: 18.7, ts: Date.parse("2020-01-17T00:00:00Z") }];

  const repaired = applyMetricCloseCarryFromAnchorSeriesForTest(points, closes, "pe_forward", anchors, {
    minValue: 2,
    maxValue: 140,
    maxAnchorLagDays: 5,
    maxForwardFillDays: 45,
    minDate: "2020-01-17",
  });

  assert.equal(repaired[0].pe_forward, 26.1877);
  assert.equal(repaired[1].pe_forward, 26.6229);
  assert.equal(repaired[2].pe_forward, 18.7);
  assert.equal(repaired[3].pe_forward, 18.6504);
});

test("applyCloseAnchoredOverrides keeps missing forward PE as null outside anchor coverage", () => {
  const points = [
    { date: "2022-05-12", pe_ttm: 20, pe_forward: 16.6, pb: 4, us10y_yield: 0.03 },
    { date: "2023-01-03", pe_ttm: 21, pe_forward: null, pb: 4.2, us10y_yield: 0.035 },
    { date: "2026-04-10", pe_ttm: 25, pe_forward: 21.1, pb: 5.3, us10y_yield: 0.04 },
  ];
  const closes = [
    { date: "2022-05-12", close: 100 },
    { date: "2023-01-03", close: 110 },
    { date: "2026-04-10", close: 150 },
  ];
  const anchors = [
    { date: "2022-05-12", value: 16.6, ts: Date.parse("2022-05-12T00:00:00Z") },
    { date: "2026-04-10", value: 21.1, ts: Date.parse("2026-04-10T00:00:00Z") },
  ];

  const repaired = applyCloseAnchoredOverridesForTest(points, closes, undefined, anchors, {
    minForward: 2,
    maxForward: 140,
    maxAnchorLagDays: 5,
    forwardMaxSegmentSpanDays: 900,
    forwardSegmentMode: "daily_return_path",
  });

  assert.equal(repaired[0].pe_forward, 16.6);
  assert.equal(repaired[1].pe_forward, null);
  assert.equal(repaired[2].pe_forward, 21.1);
});

test("applyCloseAnchoredOverrides rebuilds S&P 500 forward PE between FactSet and WSJ anchors", () => {
  const points = [
    { date: "2022-05-12", pe_ttm: 20, pe_forward: 16.6, pb: 4, us10y_yield: 0.03 },
    { date: "2023-01-03", pe_ttm: 21, pe_forward: null, pb: 4.2, us10y_yield: 0.035 },
    { date: "2023-01-20", pe_ttm: 21, pe_forward: null, pb: 4.2, us10y_yield: 0.035 },
    { date: "2024-08-09", pe_ttm: 25, pe_forward: null, pb: 5, us10y_yield: 0.04 },
    { date: "2026-04-10", pe_ttm: 25, pe_forward: 21.1, pb: 5.3, us10y_yield: 0.04 },
  ];
  const closes = [
    { date: "2022-05-12", close: 100 },
    { date: "2023-01-03", close: 106 },
    { date: "2023-01-20", close: 110 },
    { date: "2024-08-09", close: 140 },
    { date: "2026-04-10", close: 155 },
  ];
  const anchors = [
    { date: "2022-05-12", value: 16.6, ts: Date.parse("2022-05-12T00:00:00Z") },
    { date: "2023-01-20", value: 17.0, ts: Date.parse("2023-01-20T00:00:00Z") },
    { date: "2024-08-09", value: 20.2, ts: Date.parse("2024-08-09T00:00:00Z") },
    { date: "2026-04-10", value: 21.1, ts: Date.parse("2026-04-10T00:00:00Z") },
  ];

  const repaired = applyCloseAnchoredOverridesForTest(points, closes, undefined, anchors, {
    minForward: 2,
    maxForward: 140,
    maxAnchorLagDays: 5,
    forwardMaxSegmentSpanDays: 900,
    forwardSegmentMode: "daily_return_path",
  });

  assert.equal(repaired[0].pe_forward, 16.6);
  assert.ok(repaired[1].pe_forward);
  assert.equal(repaired[2].pe_forward, 17);
  assert.equal(repaired[3].pe_forward, 20.2);
  assert.equal(repaired[4].pe_forward, 21.1);
});

test("parseYahooChartCloseSeries returns sorted daily closes from Yahoo chart payload", () => {
  const payload = JSON.stringify({
    chart: {
      result: [
        {
          timestamp: [1774310400, 1774396800, 1774569600],
          indicators: {
            quote: [
              {
                close: [499.12, null, 503.88],
              },
            ],
          },
        },
      ],
    },
  });

  const points = parseYahooChartCloseSeries(payload, "2026-03-20", "2026-03-31");

  assert.deepEqual(points, [
    { date: "2026-03-24", close: 499.12 },
    { date: "2026-03-27", close: 503.88 },
  ]);
});

test("buildYahooChartUrlsForTest uses bounded period queries instead of range=max", () => {
  const urls = buildYahooChartUrlsForTest("SPY", "2026-03-20", "2026-04-16");

  assert.equal(urls.length, 2);
  assert.match(urls[0], /chart\/SPY\?period1=\d+&period2=\d+&interval=1d/);
  assert.ok(urls.every((url) => !url.includes("range=max")));
});

test("parseWsjPeSnapshotFromTextForTest ignores market ticker strips and parses table rows correctly", () => {
  const text = `
    [DJIA 49552.23 2.00%][S&P 500 7131.07 1.28%][Nasdaq 24478.05 1.56%][Russell 2000 2777.03 2.11%]
    ### Other Indexes
    Friday, April 10, 2026
    |  | P/E RATIO | DIV YIELD |
    | --- | --- | --- |
    |  | 4/10/26† | Year ago† | Estimate^ | 4/10/26† | Year ago† |
    | Russell 2000 Index | 36.96 | 32.14 | 25.10 | 1.23 | 1.70 |
    | NASDAQ 100 Index | 30.87 | 28.25 | 23.57 | 0.65 | 0.86 |
    | S&P 500 Index | 24.45 | 22.10 | 21.10 | 1.19 | 1.46 |
  `;

  const parsed = parseWsjPeSnapshotFromTextForTest(text);

  assert.deepEqual(parsed.get("russell2000"), {
    trailing: 36.96,
    forward: 25.1,
    asOfDate: "2026-04-10",
  });
  assert.deepEqual(parsed.get("nasdaq100"), {
    trailing: 30.87,
    forward: 23.57,
    asOfDate: "2026-04-10",
  });
  assert.deepEqual(parsed.get("sp500"), {
    trailing: 24.45,
    forward: 21.1,
    asOfDate: "2026-04-10",
  });
  assert.equal(parsed.get("dow30"), undefined);
});

test("getIndexLiveSourceCutoverDateForTest rebuilds known reliable index histories from source start dates", () => {
  assert.equal(getIndexLiveSourceCutoverDateForTest("russell2000"), "2001-01-03");
  assert.equal(getIndexLiveSourceCutoverDateForTest("dow30"), "1998-01-02");
  assert.equal(getIndexLiveSourceCutoverDateForTest("nasdaq100"), "2000-01-31");
  assert.equal(getIndexLiveSourceCutoverDateForTest("sp500"), "2008-01-02");
  assert.equal(getIndexLiveSourceCutoverDateForTest("sector_technology"), "1999-01-04");
  assert.equal(getIndexLiveSourceCutoverDateForTest("sector_communication"), "2018-06-18");
});

test("applyPostCutoverMetricSources prefers explicit snapshots and real-series interpolation after cutover", () => {
  const points = [
    { date: "2026-03-27", pe_ttm: 28.9, pe_forward: 24.9, pb: 5.2, us10y_yield: 0.0412 },
    { date: "2026-03-30", pe_ttm: 30.4, pe_forward: 25.4, pb: 5.4, us10y_yield: 0.0414 },
    { date: "2026-03-31", pe_ttm: 30.8, pe_forward: 25.8, pb: 5.5, us10y_yield: 0.0415 },
    { date: "2026-04-01", pe_ttm: 31.2, pe_forward: 26.2, pb: 5.6, us10y_yield: 0.0416 },
  ];

  const next = applyPostCutoverMetricSources(
    points,
    "pe_ttm",
    [{ date: "2026-04-01", value: 29.7 }],
    [
      { date: "2026-03-28", value: 28.8, ts: Date.parse("2026-03-28T00:00:00Z") },
      { date: "2026-03-31", value: 29.4, ts: Date.parse("2026-03-31T00:00:00Z") },
    ],
    "2026-03-27",
    { minValue: 2.4, maxValue: 240, maxInterpolationSpanDays: 10, maxForwardFillDays: 3 }
  );

  assert.equal(next[0].pe_ttm, 28.9);
  assert.equal(next[1].pe_ttm, 29.2);
  assert.equal(next[2].pe_ttm, 29.4);
  assert.equal(next[3].pe_ttm, 29.7);
});

test("applyRecentCloseCarryWindowToMetricForTest carries explicit PE snapshots forward by close movement", () => {
  const points = [
    { date: "2026-04-01", pe_ttm: 29.02, pe_forward: 21.1, pb: 5.5445, us10y_yield: 0.0433 },
    { date: "2026-04-02", pe_ttm: 28.5122, pe_forward: 21.0456, pb: 5.5462, us10y_yield: 0.0432 },
    { date: "2026-04-10", pe_ttm: 30.1126, pe_forward: 21.8799, pb: 5.5598, us10y_yield: 0.0429 },
    { date: "2026-04-13", pe_ttm: 27.345, pe_forward: 22.017, pb: 5.5649, us10y_yield: 0.0428 },
    { date: "2026-04-14", pe_ttm: 28.31, pe_forward: 22.2853, pb: 5.6327, us10y_yield: 0.0427 },
    { date: "2026-04-16", pe_ttm: 30.24, pe_forward: 22.5163, pb: 5.6911, us10y_yield: 0.0429 },
  ];
  const closes = [
    { date: "2026-04-01", close: 655.239990234375 },
    { date: "2026-04-02", close: 655.8300170898438 },
    { date: "2026-04-10", close: 679.4600219726562 },
    { date: "2026-04-13", close: 686.0999755859375 },
    { date: "2026-04-14", close: 694.4600219726562 },
    { date: "2026-04-16", close: 701.6599731445312 },
  ];
  const snapshots = [
    { date: "2026-04-10", pe_ttm: 24.45, pe_forward: 21.1, pb: null, source: "wsj-latest", capturedAt: "" },
  ];

  const next = applyRecentCloseCarryWindowToMetricForTest(points, closes, snapshots, "pe_ttm", {
    minValue: 2.4,
    maxValue: 240,
    lookbackPoints: 10,
  });

  assert.equal(next[2].pe_ttm, 24.45);
  assert.equal(next[3].pe_ttm, 24.6889);
  assert.equal(next[4].pe_ttm, 24.9898);
  assert.equal(next[5].pe_ttm, 25.2489);
});

test("applyMetricCloseCarryWithAnchorsForTest smooths sp500 TTM PE between WSJ anchors by daily close moves", () => {
  const points = [
    { date: "2026-03-26", pe_ttm: 24.6943, pe_forward: 21.1, pb: 5.4, us10y_yield: 0.042 },
    { date: "2026-03-27", pe_ttm: 26.2791, pe_forward: 21.2, pb: 5.4, us10y_yield: 0.0421 },
    { date: "2026-03-30", pe_ttm: 22.5862, pe_forward: 21.3, pb: 5.4, us10y_yield: 0.0422 },
    { date: "2026-03-31", pe_ttm: 23.2428, pe_forward: 21.4, pb: 5.4, us10y_yield: 0.0423 },
  ];
  const closes = [
    { date: "2026-03-26", close: 610.0 },
    { date: "2026-03-27", close: 599.6 },
    { date: "2026-03-30", close: 597.5953 },
    { date: "2026-03-31", close: 613.3867 },
  ];
  const next = applyMetricCloseCarryWithAnchorsForTest(
    points,
    closes,
    [],
    "pe_ttm",
    [
      { date: "2026-03-26", value: 24.6943 },
      { date: "2026-03-31", value: 24.8314 },
    ],
    {
      startDate: "2026-03-26",
      minValue: 2.4,
      maxValue: 180,
    }
  );

  assert.equal(next[0].pe_ttm, 24.6943);
  assert.equal(next[1].pe_ttm, 24.2733);
  assert.equal(next[2].pe_ttm, 24.1921);
  assert.equal(next[3].pe_ttm, 24.8314);
});

test("applyMetricCloseCarryWithAnchorsForTest prefers previous rebuilt values after recovery date before resuming carry", () => {
  const points = [
    { date: "2026-04-17", pe_ttm: 25.38, pe_forward: 21.5, pb: 5.5, us10y_yield: 0.042 },
    { date: "2026-04-20", pe_ttm: 25.9, pe_forward: 21.6, pb: 5.5, us10y_yield: 0.0421 },
    { date: "2026-04-21", pe_ttm: 26.1, pe_forward: 21.7, pb: 5.5, us10y_yield: 0.0422 },
  ];
  const closes = [
    { date: "2026-04-17", close: 700.0 },
    { date: "2026-04-20", close: 703.0 },
    { date: "2026-04-21", close: 710.0 },
  ];
  const previousPoints = [
    { date: "2026-04-17", pe_ttm: 25.38, pe_forward: 21.5, pb: 5.5, us10y_yield: 0.042 },
    { date: "2026-04-20", pe_ttm: 25.4888, pe_forward: 21.6, pb: 5.5, us10y_yield: 0.0421 },
  ];
  const next = applyMetricCloseCarryWithAnchorsForTest(
    points,
    closes,
    previousPoints,
    "pe_ttm",
    [{ date: "2026-04-17", value: 25.38 }],
    {
      startDate: "2026-03-26",
      minValue: 2.4,
      maxValue: 180,
      ignorePreviousBeforeDate: "2026-04-17",
    }
  );

  assert.equal(next[0].pe_ttm, 25.38);
  assert.equal(next[1].pe_ttm, 25.4888);
  assert.equal(next[2].pe_ttm, 25.7426);
});

test("buildEffectiveIndexYahooDailyMetricSnapshotsForTest keeps newer explicit snapshots even when value is unchanged", () => {
  const closes = [
    { date: "2026-04-01", close: 249.55999755859375 },
    { date: "2026-04-10", close: 261.29998779296875 },
  ];
  const snapshots = [
    { date: "2026-04-01", pe_ttm: 36.96, pe_forward: 25.1, pb: null, source: "wsj-latest", capturedAt: "" },
    { date: "2026-04-10", pe_ttm: 36.96, pe_forward: 25.1, pb: null, source: "wsj-latest", capturedAt: "" },
  ];

  const effective = buildEffectiveIndexYahooDailyMetricSnapshotsForTest(closes, snapshots);

  assert.deepEqual(
    effective.map((item) => ({ date: item.date, pe_ttm: item.pe_ttm, pe_forward: item.pe_forward })),
    [
      { date: "2026-04-01", pe_ttm: 36.96, pe_forward: 25.1 },
      { date: "2026-04-10", pe_ttm: 36.96, pe_forward: 25.1 },
    ]
  );
});

test("collapseRedundantExplicitLatestSnapshotsForTest drops older duplicate explicit anchors and keeps the newest date", () => {
  const snapshots = [
    { date: "2026-04-01", pe_ttm: 36.96, pe_forward: 25.1, pb: null, source: "wsj-latest", capturedAt: "" },
    { date: "2026-04-10", pe_ttm: 36.96, pe_forward: 25.1, pb: null, source: "wsj-latest", capturedAt: "" },
    { date: "2026-04-17", pe_ttm: 38.05, pe_forward: 25.74, pb: null, source: "wsj-latest", capturedAt: "" },
  ];

  const collapsed = collapseRedundantExplicitLatestSnapshotsForTest(snapshots);

  assert.deepEqual(
    collapsed.map((item) => ({ date: item.date, pe_ttm: item.pe_ttm, pe_forward: item.pe_forward })),
    [
      { date: "2026-04-10", pe_ttm: 36.96, pe_forward: 25.1 },
      { date: "2026-04-17", pe_ttm: 38.05, pe_forward: 25.74 },
    ]
  );
});

test("applyYahooSnapshotCarryToMetricForTest backfills days before the first explicit WSJ anchor by close movement", () => {
  const points = [
    { date: "2026-03-30", pe_ttm: 28.9819, pe_forward: 20.28, pb: 5.5411, us10y_yield: 0.0433 },
    { date: "2026-03-31", pe_ttm: 29.001, pe_forward: 20.8694, pb: 5.5428, us10y_yield: 0.0434 },
    { date: "2026-04-01", pe_ttm: 29.02, pe_forward: 21.1, pb: 5.5445, us10y_yield: 0.0433 },
    { date: "2026-04-02", pe_ttm: 29.0461, pe_forward: 21.119, pb: 5.5462, us10y_yield: 0.0432 },
    { date: "2026-04-09", pe_ttm: 30.1126, pe_forward: 21.8944, pb: 5.5581, us10y_yield: 0.0429 },
    { date: "2026-04-10", pe_ttm: 30.2838, pe_forward: 21.8799, pb: 5.5598, us10y_yield: 0.0429 },
    { date: "2026-04-16", pe_ttm: 30.2489, pe_forward: 22.5948, pb: 5.6911, us10y_yield: 0.0429 },
  ];
  const closes = [
    { date: "2026-03-30", close: 631.969970703125 },
    { date: "2026-03-31", close: 650.3400268554688 },
    { date: "2026-04-01", close: 655.239990234375 },
    { date: "2026-04-02", close: 655.8300170898438 },
    { date: "2026-04-09", close: 679.9099731445312 },
    { date: "2026-04-10", close: 679.4600219726562 },
    { date: "2026-04-16", close: 701.6599731445312 },
  ];
  const snapshots = [
    { date: "2026-04-10", pe_ttm: 24.45, pe_forward: 21.1, pb: null, source: "wsj-latest", capturedAt: "" },
  ];

  const next = applyYahooSnapshotCarryToMetricForTest(points, closes, snapshots, "pe_ttm", {
    minValue: 2.4,
    maxValue: 240,
    backfillLookbackPoints: 10,
  });

  assert.equal(next[0].pe_ttm, 22.7411);
  assert.equal(next[1].pe_ttm, 23.4021);
  assert.equal(next[2].pe_ttm, 23.5785);
  assert.equal(next[3].pe_ttm, 23.5997);
  assert.equal(next[4].pe_ttm, 24.4662);
  assert.equal(next[5].pe_ttm, 24.45);
});

test("pickAnchorForwardPeForTest prefers current WSJ forward PE over implausible historical fallback", () => {
  const picked = pickAnchorForwardPeForTest("dow30", 23.7, {
    latestForwardSeriesValue: null,
    stockAnalysisForward: null,
    finvizForward: null,
    wsjForward: 20.68,
    wsjTrailing: 23.7,
    officialForward: null,
    officialTrailing: null,
    latestHistoryForward: 4.94,
  });

  assert.equal(picked, 20.68);
});

test("pickAnchorForwardPeForTest accepts verified public forward references even without a plausible trailing pair", () => {
  const picked = pickAnchorForwardPeForTest("sector_communication", 17.98, {
    latestForwardSeriesValue: null,
    stockAnalysisForward: null,
    finvizForward: null,
    wsjForward: null,
    wsjTrailing: null,
    officialForward: null,
    officialTrailing: null,
    publicForwardReference: 20.38,
    latestHistoryForward: null,
  });

  assert.equal(picked, 20.38);
});

test("pruneImplausibleForwardSeriesForTest drops Nasdaq 100 forward anchors that sit far above trailing PE", () => {
  const trailingSeries = [
    { date: "2016-03-31", value: 20.25, ts: Date.parse("2016-03-31T00:00:00Z") },
    { date: "2016-06-30", value: 19.36, ts: Date.parse("2016-06-30T00:00:00Z") },
    { date: "2019-12-31", value: 22.66, ts: Date.parse("2019-12-31T00:00:00Z") },
    { date: "2025-12-31", value: 32.32, ts: Date.parse("2025-12-31T00:00:00Z") },
  ];
  const forwardSeries = [
    { date: "2016-03-31", value: 32.19, ts: Date.parse("2016-03-31T00:00:00Z") },
    { date: "2016-06-30", value: 30.84, ts: Date.parse("2016-06-30T00:00:00Z") },
    { date: "2019-12-31", value: 35.74, ts: Date.parse("2019-12-31T00:00:00Z") },
    { date: "2025-12-31", value: 27.44, ts: Date.parse("2025-12-31T00:00:00Z") },
  ];

  const pruned = pruneImplausibleForwardSeriesForTest("nasdaq100", forwardSeries, trailingSeries);

  assert.deepEqual(pruned, [{ date: "2025-12-31", value: 27.44, ts: Date.parse("2025-12-31T00:00:00Z") }]);
});

test("pruneImplausibleForwardSeriesForTest drops sector forward anchors that are far above same-month trailing PE", () => {
  const trailingSeries = [
    { date: "2012-08-31", value: 11.27, ts: Date.parse("2012-08-31T00:00:00Z") },
    { date: "2025-12-31", value: 38.68, ts: Date.parse("2025-12-31T00:00:00Z") },
  ];
  const forwardSeries = [
    { date: "2012-08-31", value: 29.5762, ts: Date.parse("2012-08-31T00:00:00Z") },
    { date: "2025-12-31", value: 27.9013, ts: Date.parse("2025-12-31T00:00:00Z") },
  ];

  const pruned = pruneImplausibleForwardSeriesForTest("sector_technology", forwardSeries, trailingSeries);

  assert.deepEqual(pruned, [{ date: "2025-12-31", value: 27.9013, ts: Date.parse("2025-12-31T00:00:00Z") }]);
});

test("applyMetricPointAnchorsForTest restores public forward PE anchors after carry steps", () => {
  const points = [
    { date: "2026-04-13", pe_ttm: 36, pe_forward: 24.1, pb: 8.8, us10y_yield: 0.041 },
    { date: "2026-04-14", pe_ttm: 36.2, pe_forward: 24.4653, pb: 8.9, us10y_yield: 0.0412 },
    { date: "2026-04-15", pe_ttm: 36.4, pe_forward: 24.7, pb: 9, us10y_yield: 0.0411 },
  ];

  const anchored = applyMetricPointAnchorsForTest(
    points,
    "pe_forward",
    [{ date: "2026-04-14", value: 21.71 }],
    { minValue: 2, maxValue: 140 }
  );

  assert.equal(anchored[0].pe_forward, 24.1);
  assert.equal(anchored[1].pe_forward, 21.71);
  assert.equal(anchored[2].pe_forward, 24.7);
});

test("extendSeriesWithRebasedPreviousTailForTest keeps newer stored dates but rebases them to the new live anchor", () => {
  const previous = [
    { date: "2026-03-27", pe_ttm: 22, pe_forward: 18, pb: 4, us10y_yield: 0.0412 },
    { date: "2026-04-01", pe_ttm: 21, pe_forward: 17, pb: 3.8, us10y_yield: 0.0433 },
    { date: "2026-04-02", pe_ttm: 22, pe_forward: 17.5, pb: 3.9, us10y_yield: 0.0431 },
    { date: "2026-04-03", pe_ttm: 23, pe_forward: 18, pb: 4.0, us10y_yield: 0.0430 },
  ];
  const next = [
    { date: "2026-03-27", pe_ttm: 22, pe_forward: 18, pb: 4, us10y_yield: 0.0412 },
    { date: "2026-04-01", pe_ttm: 30, pe_forward: 20, pb: 5, us10y_yield: 0.0433 },
  ];

  const extended = extendSeriesWithRebasedPreviousTailForTest(previous, next, "2026-03-27");

  assert.deepEqual(
    extended.map((point) => point.date),
    ["2026-03-27", "2026-04-01", "2026-04-02", "2026-04-03"]
  );
  assert.equal(extended[1].pe_ttm, 30);
  assert.equal(extended[2].pe_ttm, 31.4286);
  assert.equal(extended[3].pe_forward, 21.1765);
  assert.equal(extended[3].pb, 5.2632);
});

test("extendSeriesWithRebasedPreviousTailForTest bridges missing dates between sparse new anchors", () => {
  const previous = [
    { date: "2026-03-27", pe_ttm: 22, pe_forward: 18, pb: 4, us10y_yield: 0.0412 },
    { date: "2026-03-30", pe_ttm: 23, pe_forward: 18.5, pb: 4.1, us10y_yield: 0.0413 },
    { date: "2026-03-31", pe_ttm: 24, pe_forward: 19, pb: 4.2, us10y_yield: 0.0415 },
    { date: "2026-04-01", pe_ttm: 21, pe_forward: 17, pb: 3.8, us10y_yield: 0.0433 },
    { date: "2026-04-02", pe_ttm: 22, pe_forward: 17.5, pb: 3.9, us10y_yield: 0.0431 },
    { date: "2026-04-03", pe_ttm: 23, pe_forward: 18, pb: 4.0, us10y_yield: 0.043 },
    { date: "2026-04-16", pe_ttm: 24, pe_forward: 18.5, pb: 4.1, us10y_yield: 0.0429 },
    { date: "2026-04-17", pe_ttm: 25, pe_forward: 19, pb: 4.2, us10y_yield: 0.0428 },
  ];
  const next = [
    { date: "2026-03-27", pe_ttm: 22, pe_forward: 18, pb: 4, us10y_yield: 0.0412 },
    { date: "2026-04-01", pe_ttm: 30, pe_forward: 20, pb: 5, us10y_yield: 0.0433 },
    { date: "2026-04-16", pe_ttm: 36, pe_forward: 24, pb: 6, us10y_yield: 0.0429 },
  ];

  const extended = extendSeriesWithRebasedPreviousTailForTest(previous, next, "2026-03-27");

  assert.deepEqual(
    extended.map((point) => point.date),
    ["2026-03-27", "2026-03-30", "2026-03-31", "2026-04-01", "2026-04-02", "2026-04-03", "2026-04-16", "2026-04-17"]
  );
  assert.equal(extended[1].pe_ttm, 26.2857);
  assert.equal(extended[2].pe_forward, 21.2353);
  assert.equal(extended[4].pe_ttm, 31.9524);
  assert.equal(extended[5].pb, 5.6569);
  assert.equal(extended[7].pe_ttm, 37.5);
});

test("rebaseHistoryFallbackWithSnapshotsForTest repairs fallback tail using explicit snapshots", () => {
  const points = [
    { date: "2026-03-27", pe_ttm: 22.15, pe_forward: 4.23, pb: 10.3275, us10y_yield: 0.0442 },
    { date: "2026-04-01", pe_ttm: 74.4289, pe_forward: 4.2206, pb: 11.2557, us10y_yield: 0.0433 },
    { date: "2026-04-16", pe_ttm: 74, pe_forward: 4.2, pb: 11.2557, us10y_yield: 0.0429 },
  ];
  const snapshots = [
    { date: "2026-04-01", pe_ttm: 23.13, pe_forward: 20.4, pb: null, source: "ssga-official-latest" },
  ];

  const repaired = rebaseHistoryFallbackWithSnapshotsForTest(points, snapshots, "2026-03-27");

  assert.equal(repaired[1].pe_ttm, 23.13);
  assert.equal(repaired[1].pe_forward, 20.4);
  assert.equal(repaired[2].pe_ttm, 22.9967);
  assert.equal(repaired[2].pe_forward, 20.3004);
});

test("repairHistoryFallbackPointsForTest rebases reusable fallback history with explicit latest snapshots", () => {
  const historyPoints = [
    { date: "2026-03-27", pe_ttm: 22.15, pe_forward: 4.23, pb: 10.3275, us10y_yield: 0.0442 },
    { date: "2026-04-01", pe_ttm: 74.4289, pe_forward: 4.2206, pb: 11.2557, us10y_yield: 0.0433 },
    { date: "2026-04-16", pe_ttm: 74, pe_forward: 4.2, pb: 11.2557, us10y_yield: 0.0429 },
  ];
  const snapshots = [
    { date: "2026-04-01", pe_ttm: 23.13, pe_forward: 20.4, pb: null, source: "ssga-official-latest" },
  ];

  const repaired = repairHistoryFallbackPointsForTest(historyPoints, snapshots, "2026-03-27");

  assert.equal(repaired[1].pe_ttm, 23.13);
  assert.equal(repaired[1].pe_forward, 20.4);
  assert.equal(repaired[2].pe_ttm, 22.9967);
  assert.equal(repaired[2].pe_forward, 20.3004);
});

test("upsertIndexYahooDailyMetricSnapshot merges non-null metrics with existing same-day snapshot", () => {
  const store = new Map([
    [
      "QQQ",
      [
        {
          date: "2026-04-16",
          pe_ttm: 30.5,
          pe_forward: null,
          pb: 7.1,
          source: "stockanalysis-latest",
          capturedAt: "2026-04-17T00:00:00.000Z",
        },
      ],
    ],
  ]);

  upsertIndexYahooDailyMetricSnapshotForTest(store, "QQQ", {
    date: "2026-04-16",
    pe_ttm: null,
    pe_forward: 26.2,
    pb: null,
    source: "wsj-latest",
    capturedAt: "2026-04-17T00:01:00.000Z",
  });

  assert.deepEqual(store.get("QQQ"), [
    {
      date: "2026-04-16",
      pe_ttm: 30.5,
      pe_forward: 26.2,
      pb: 7.1,
      source: "wsj-latest",
      capturedAt: "2026-04-17T00:01:00.000Z",
    },
  ]);
});

test("parseMultplTableSeriesForTest parses dated rows from Multpl tables", () => {
  const html = `
    <table>
      <tr><td>Apr 14, 2026</td><td>5.51</td></tr>
      <tr><td>Dec 31, 2025</td><td>5.39</td></tr>
    </table>
  `;

  const series = parseMultplTableSeriesForTest(html, 20);

  assert.deepEqual(series, [
    { date: "2025-12-31", value: 5.39, ts: Date.parse("2025-12-31T00:00:00Z") },
    { date: "2026-04-14", value: 5.51, ts: Date.parse("2026-04-14T00:00:00Z") },
  ]);
});

test("S&P 500 PB uses MacroMicro quarterly anchors before Multpl fallback", () => {
  assert.deepEqual(getMacroMicroSp500PbRoutesForTest(), {
    ids: [6938],
    routes: ["https://en.macromicro.me/series/6938/us-sp500-pb-ratio"],
  });
  assert.equal(getMultplSp500PbUrlForTest(), "https://www.multpl.com/s-p-500-price-to-book/table/by-quarter");
});

test("S&P 500 TTM PE only falls back to Multpl when no historical source exists", () => {
  assert.equal(shouldFetchMultplSp500PeFallbackForTest(undefined), true);
  assert.equal(shouldFetchMultplSp500PeFallbackForTest([]), true);
  assert.equal(
    shouldFetchMultplSp500PeFallbackForTest([
      { date: "2026-03-31", value: 24.7, ts: Date.parse("2026-03-31T00:00:00Z") },
    ]),
    false
  );
});

test("S&P 500 TTM PE does not fall back to Multpl when stockmarketperatio history exists", () => {
  const stockMarketPeRatioSeries = parseStockMarketPeRatioSeriesForTest(`
    [new Date(2025,12,1),31.02,23.58],
    [new Date(2026,3,1),25.83,23.58],
  `);

  assert.equal(shouldFetchMultplSp500PeFallbackForTest(stockMarketPeRatioSeries), false);
});

test("pruneInvalidExplicitIndexSnapshotsForTest removes explicit outliers against anchor series", () => {
  const snapshots = [
    { date: "2026-04-10", pe_ttm: 24.45, pe_forward: 21.1, pb: null, source: "wsj-latest" },
    { date: "2026-04-16", pe_ttm: 75, pe_forward: 4.2, pb: null, source: "wsj-latest" },
  ];
  const trailingSeries = [
    { date: "2026-03-31", value: 24.2, ts: Date.parse("2026-03-31T00:00:00Z") },
    { date: "2026-04-15", value: 24.7, ts: Date.parse("2026-04-15T00:00:00Z") },
  ];
  const forwardSeries = [
    { date: "2026-03-31", value: 20.8, ts: Date.parse("2026-03-31T00:00:00Z") },
    { date: "2026-04-15", value: 21.2, ts: Date.parse("2026-04-15T00:00:00Z") },
  ];

  const pruned = pruneInvalidExplicitIndexSnapshotsForTest(snapshots, trailingSeries, forwardSeries, []);

  assert.deepEqual(pruned, [
    { date: "2026-04-10", pe_ttm: 24.45, pe_forward: 21.1, pb: null, source: "wsj-latest", capturedAt: "" },
  ]);
});

test("parseYchartsPbSeriesForTest parses PB series from YCharts fund_data payload", () => {
  const payload = JSON.stringify({
    chart_data: [
      [
        {
          object_calc: "price_to_book_value",
          raw_data: [
            [1767139200, 5.39],
            [1776124800, 5.51],
          ],
        },
      ],
    ],
  });

  const series = parseYchartsPbSeriesForTest(payload);

  assert.deepEqual(series, [
    { date: "2025-12-31", value: 5.39, ts: Date.parse("2025-12-31T00:00:00Z") },
    { date: "2026-04-14", value: 5.51, ts: Date.parse("2026-04-14T00:00:00Z") },
  ]);
});

test("parseSsgaIndexMetricsForTest extracts TTM PE, forward PE, PB, and date from SSGA HTML", () => {
  const html = `
    <section data-fundComponent="true">
      <h2 class="comp-title">Fund Characteristics <span class="date">as of Apr 16 2026</span></h2>
      <div class="section-content">
        <table class="tb-keyvalue">
          <tr><td class="label">Price/Book Ratio</td><td class="data">5.28</td></tr>
          <tr><td class="label">Price/Earnings Ratio FY1</td><td class="data">20.40</td></tr>
        </table>
      </div>
    </section>
    <section data-fundComponent="true">
      <h2 class="comp-title">Index Characteristics <span class="date">as of Apr 16 2026</span></h2>
      <div class="section-content">
        <table class="tb-keyvalue">
          <tr><td class="label">Price/Earnings</td><td class="data">23.13</td></tr>
          <tr><td class="label">Price/Earnings Ratio FY1</td><td class="data">20.40</td></tr>
        </table>
      </div>
    </section>
  `;

  assert.deepEqual(parseSsgaIndexMetricsForTest(html), {
    date: "2026-04-16",
    pe_ttm: 23.13,
    pe_forward: 20.4,
    pb: 5.28,
    source: "ssga-official-latest",
  });
});

test("parseSsgaIndexMetricsForTest accepts XLC style index characteristics with forward PE only on the index section", () => {
  const html = `
    <section data-fundComponent="true">
      <h2 class="comp-title">Fund Characteristics <span class="date">as of Apr 30 2026</span></h2>
      <div class="section-content">
        <table class="tb-keyvalue">
          <tr><td class="label">Price/Book Ratio</td><td class="data">3.35</td></tr>
        </table>
      </div>
    </section>
    <section data-fundComponent="true">
      <h2 class="comp-title">Index Characteristics <span class="date">as of Apr 30 2026</span></h2>
      <div class="section-content">
        <table class="tb-keyvalue">
          <tr><td class="label">Price/Earnings</td><td class="data">17.92</td></tr>
          <tr><td class="label">Price/Earnings Ratio FY1</td><td class="data">16.07</td></tr>
        </table>
      </div>
    </section>
  `;

  assert.deepEqual(parseSsgaIndexMetricsForTest(html), {
    date: "2026-04-30",
    pe_ttm: 17.92,
    pe_forward: 16.07,
    pb: 3.35,
    source: "ssga-official-latest",
  });
});

test("parseIsharesPortfolioMetricsForTest extracts TTM PE, PB, and date from iShares HTML", () => {
  const html = `
    <div data-componentName="Fundamentals And Risk" class="fund-component fund-component-parent clearfix ppv3" id="fundamentalsAndRisk">
      <h2 class="mobile-hidden" data-link-target=""> Portfolio Characteristics </h2>
      <div class="product-data-item col-priceBook ">
        <div class="caption">P/B Ratio<div class="as-of-date">as of Apr 16, 2026</div></div>
        <div class="data">2.26</div>
      </div>
      <div class="product-data-item col-priceEarnings ">
        <div class="caption">P/E Ratio<div class="as-of-date">as of Apr 16, 2026</div></div>
        <div class="data">19.47</div>
      </div>
    </div>
  `;

  assert.deepEqual(parseIsharesPortfolioMetricsForTest(html), {
    date: "2026-04-16",
    pe_ttm: 19.47,
    pe_forward: null,
    pb: 2.26,
    source: "ishares-official-latest",
  });
});
