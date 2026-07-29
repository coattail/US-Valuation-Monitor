import test from "node:test";
import assert from "node:assert/strict";

import {
  applyAuthoritativePublishedMetricCorrectionsForTest,
  applyValidatedNasdaq100TtmHistoryForTest,
  applyValidatedRussell2000PeHistoryForTest,
  assertPublishedIndexHistoryAppendOnly,
  assertValidatedIndexPointsUnchangedForTest,
  getIndexLiveSourceCutoverDateForTest,
  isLatestSnapshotDeviationAcceptableForTest,
  mergePublishedIndexHistoryAtCutoverForTest,
  mergeHistoricalSeriesAtCutover,
  preservePublishedMetricHistoryFromDateForTest,
  preservePublishedIndexHistoryAppendOnlyForTest,
  preserveValidatedIndexHistoryForTest,
  parseFredIndexCloseSeriesForTest,
  parseHistoryOfMarketNdxForwardSeriesForTest,
  shouldFetchStockAnalysisLatestSnapshotForTest,
} from "../src/generate.ts";

function point(date: string, peTtm: number) {
  return {
    date,
    pe_ttm: peTtm,
    pe_forward: peTtm - 4,
    pb: 5,
    us10y_yield: 0.04,
  };
}

test("preserves validated history through the configured index cutover", () => {
  const previous = [point("2020-01-02", 23.04), point("2026-03-26", 30.2)];
  const next = [point("2020-01-02", 41.1), point("2026-03-26", 30.8), point("2026-03-27", 31.4)];

  assert.deepEqual(mergeHistoricalSeriesAtCutover(previous, next), [
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 31.4),
  ]);
  assert.equal(getIndexLiveSourceCutoverDateForTest("russell2000"), "2001-01-03");
});

test("keeps the validated history boundary independent from an index source start date", () => {
  const validated = [
    point("2000-01-31", 58),
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 30.3),
  ];
  const regenerated = [
    point("2000-01-31", 98),
    point("2020-01-02", 41.1),
    point("2026-03-26", 55.8),
    point("2026-03-27", 31.4),
    point("2026-03-30", 31.8),
  ];

  const firstRefresh = preserveValidatedIndexHistoryForTest(validated, regenerated);
  assert.deepEqual(firstRefresh, [
    point("2000-01-31", 58),
    point("2020-01-02", 23.04),
    point("2026-03-26", 30.2),
    point("2026-03-27", 31.4),
    point("2026-03-30", 31.8),
  ]);

  const secondRefresh = preserveValidatedIndexHistoryForTest(firstRefresh, [
    point("2000-01-31", 120),
    point("2020-01-02", 60),
    point("2026-03-26", 70),
    point("2026-03-27", 32),
    point("2026-03-31", 32.2),
  ]);
  assert.deepEqual(secondRefresh.slice(0, 3), validated.slice(0, 3));
  assert.deepEqual(secondRefresh.slice(3), [point("2026-03-27", 32), point("2026-03-31", 32.2)]);
});

test("routine refreshes preserve every published point and append only newer dates", () => {
  const published = [point("2026-07-15", 31.2), point("2026-07-16", 31.6)];
  const regenerated = [
    point("2026-07-14", 30.1),
    point("2026-07-15", 80),
    point("2026-07-16", 90),
    point("2026-07-17", 32.1),
  ];

  assert.deepEqual(preservePublishedIndexHistoryAppendOnlyForTest(published, regenerated, "nasdaq100"), [
    ...published,
    point("2026-07-17", 32.1),
  ]);
});

test("authoritative delayed snapshots correct only their exact published metrics", () => {
  const published = [
    point("2026-07-23", 33.2282),
    point("2026-07-24", 32.857),
    point("2026-07-27", 32.9481),
  ];

  const corrected = applyAuthoritativePublishedMetricCorrectionsForTest(
    published,
    "nasdaq100",
    [
      {
        date: "2026-07-24",
        pe_ttm: 33.05,
        pe_forward: 25.12,
        pb: null,
        source: "wsj-latest",
      },
      {
        date: "2026-07-27",
        pe_ttm: 99,
        pe_forward: null,
        pb: null,
        source: "stockanalysis-latest",
      },
    ]
  );

  assert.deepEqual(corrected, [
    published[0],
    { ...published[1], pe_ttm: 33.05, pe_forward: 25.12 },
    published[2],
  ]);
});

test("append-only assertion rejects a mutation anywhere in published history", () => {
  const previous = {
    generatedAt: "2026-07-16T00:00:00.000Z",
    source: "test",
    indices: [
      {
        id: "nasdaq100",
        symbol: "QQQ",
        group: "core" as const,
        displayName: "Nasdaq 100",
        description: "test",
        points: [point("2026-07-16", 31.6)],
      },
    ],
  };
  const changed = structuredClone(previous);
  changed.indices[0].points[0].pe_ttm = 99;

  assert.throws(
    () => assertPublishedIndexHistoryAppendOnly(previous, changed),
    /published index history changed.*nasdaq100.*pe_ttm/
  );

  const appended = structuredClone(previous);
  appended.indices[0].points.push(point("2026-07-17", 32.1));
  assert.doesNotThrow(() => assertPublishedIndexHistoryAppendOnly(previous, appended));
});

test("append-only assertion allows only persisted authoritative metric corrections", () => {
  const previous = {
    generatedAt: "2026-07-24T00:00:00.000Z",
    source: "test",
    indices: [
      {
        id: "nasdaq100",
        symbol: "QQQ",
        group: "core" as const,
        displayName: "Nasdaq 100",
        description: "test",
        points: [point("2026-07-24", 32.857)],
      },
    ],
  };
  const corrected = structuredClone(previous);
  corrected.indices[0].points[0].pe_ttm = 33.05;
  const corrections = new Map([
    ["nasdaq100", new Map([["2026-07-24", { pe_ttm: 33.05 }]])],
  ]);

  assert.doesNotThrow(() =>
    assertPublishedIndexHistoryAppendOnly(previous, corrected, corrections)
  );

  corrected.indices[0].points[0].pe_forward = 99;
  assert.throws(
    () => assertPublishedIndexHistoryAppendOnly(previous, corrected, corrections),
    /published index history changed.*nasdaq100.*pe_forward/
  );
});

test("append-only assertion permits a derived PB repair only for an impossible published outlier", () => {
  const previous = {
    generatedAt: "2026-07-24T00:00:00.000Z",
    source: "test",
    indices: [
      {
        id: "russell2000",
        symbol: "IWM",
        group: "core" as const,
        displayName: "Russell 2000",
        description: "test",
        points: [{ ...point("2026-07-24", 37.27), pb: 18.5872 }],
      },
    ],
  };
  const corrected = structuredClone(previous);
  corrected.indices[0].points[0].pb = 2.1745;
  const corrections = new Map([
    [
      "russell2000",
      new Map([
        ["2026-07-15", { pb: 2.17 }],
        ["2026-07-16", { pb: 2.16 }],
        ["2026-07-28", { pb: 2.15 }],
      ]),
    ],
  ]);

  assert.doesNotThrow(() =>
    assertPublishedIndexHistoryAppendOnly(previous, corrected, corrections)
  );

  corrected.indices[0].points[0].pb = 8;
  assert.throws(
    () => assertPublishedIndexHistoryAppendOnly(previous, corrected, corrections),
    /published index history changed.*russell2000.*pb/
  );
});

test("allows an explicit validated-history rewrite to replace a bad frozen baseline", () => {
  const previousFlag = process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE;
  process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE = "1";
  try {
    const frozen = [point("2014-12-31", 13), point("2026-03-27", 30.3)];
    const corrected = [point("2014-12-31", 22), point("2026-03-27", 31.4)];
    assert.deepEqual(preserveValidatedIndexHistoryForTest(frozen, corrected), corrected);
  } finally {
    if (previousFlag === undefined) delete process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE;
    else process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE = previousFlag;
  }
});

test("scopes a validated-history rewrite to the requested index ids", () => {
  const previousFlag = process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE_IDS;
  process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE_IDS = "nasdaq100";
  try {
    const frozen = [point("2014-12-31", 13), point("2026-03-27", 30.3)];
    const corrected = [point("2014-12-31", 21.2), point("2026-03-27", 31.4)];
    assert.deepEqual(preserveValidatedIndexHistoryForTest(frozen, corrected, "nasdaq100"), corrected);
    assert.deepEqual(
      preserveValidatedIndexHistoryForTest(frozen, corrected, "sp500"),
      [frozen[0], corrected[1]]
    );
  } finally {
    if (previousFlag === undefined) delete process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE_IDS;
    else process.env.ALLOW_VALIDATED_INDEX_HISTORY_REWRITE_IDS = previousFlag;
  }
});

test("keeps the published Nasdaq-100 WSJ TTM segment during an earlier-history repair", () => {
  const published = [
    point("2014-12-31", 13),
    point("2026-04-17", 32.41),
    point("2026-04-20", 32.75),
    point("2026-04-24", 33.38),
  ];
  const repaired = [
    point("2014-12-31", 21.2),
    point("2026-04-17", 31.1),
    point("2026-04-20", 31.4),
    point("2026-04-24", 31.8),
    point("2026-04-27", 34.1),
  ];
  repaired[1].pe_forward = 23.1;

  const preserved = preservePublishedMetricHistoryFromDateForTest(
    published,
    repaired,
    "pe_ttm",
    "2026-04-17"
  );

  assert.equal(preserved.find((item) => item.date === "2014-12-31")?.pe_ttm, 21.2);
  assert.equal(preserved.find((item) => item.date === "2026-04-17")?.pe_ttm, 32.41);
  assert.equal(preserved.find((item) => item.date === "2026-04-17")?.pe_forward, 23.1);
  assert.equal(preserved.find((item) => item.date === "2026-04-20")?.pe_ttm, 32.75);
  assert.equal(preserved.find((item) => item.date === "2026-04-24")?.pe_ttm, 33.38);
  assert.equal(preserved.find((item) => item.date === "2026-04-27")?.pe_ttm, 34.1);
});

test("pins Nasdaq-100 TTM history to Nasdaq's published year-end observations", () => {
  const closes = [
    { date: "2013-12-31", close: 100 },
    { date: "2014-06-30", close: 110 },
    { date: "2014-12-31", close: 120 },
    { date: "2015-12-31", close: 140 },
  ];
  const depressed = closes.map(({ date }) => point(date, 13));
  const corrected = applyValidatedNasdaq100TtmHistoryForTest(depressed, closes);

  assert.equal(corrected[0].pe_ttm, 22);
  assert.ok(Number(corrected[1].pe_ttm) > 20);
  assert.equal(corrected[2].pe_ttm, 22.3);
  assert.equal(corrected[3].pe_ttm, 22.9);
});

test("parses the official Nasdaq-100 close series republished by FRED", () => {
  const parsed = parseFredIndexCloseSeriesForTest(
    [
      "observation_date,NASDAQ100",
      "2024-12-27,21372.45",
      "2024-12-30,21197.09",
      "2024-12-31,21012.17",
      "2025-01-02,.",
    ].join("\n"),
    "NASDAQ100",
    "2024-12-30",
    "2024-12-31"
  );

  assert.deepEqual(parsed, [
    { date: "2024-12-30", close: 21197.09 },
    { date: "2024-12-31", close: 21012.17 },
  ]);
});

test("pins Russell 2000 PE history to public Siblis observations", () => {
  const closes = [
    { date: "2022-06-29", close: 1700 },
    { date: "2022-06-30", close: 1707.99 },
    { date: "2022-12-30", close: 1761.25 },
    { date: "2023-06-30", close: 1888.73 },
  ];
  const synthetic = closes.map(({ date }) => point(date, 30));
  const corrected = applyValidatedRussell2000PeHistoryForTest(synthetic, closes);

  assert.equal(corrected[0].pe_ttm, null);
  assert.equal(corrected[0].pe_forward, null);
  assert.equal(corrected[1].pe_ttm, 48.59);
  assert.equal(corrected[1].pe_forward, 18.29);
  assert.equal(corrected[2].pe_ttm, 51.74);
  assert.equal(corrected[2].pe_forward, 21.38);
  assert.equal(corrected[3].pe_ttm, 27.56);
  assert.equal(corrected[3].pe_forward, 24.29);
});

test("daily latest snapshots include every thematic ETF without overriding WSJ-priority indices", () => {
  for (const indexId of ["igv", "soxx", "smh", "dram"]) {
    assert.equal(shouldFetchStockAnalysisLatestSnapshotForTest(indexId), true);
  }
  assert.equal(shouldFetchStockAnalysisLatestSnapshotForTest("sector_energy"), false);
  assert.equal(shouldFetchStockAnalysisLatestSnapshotForTest("nasdaq100", true), false);
});

test("parses the public Bloomberg BEst Nasdaq-100 forward PE history", () => {
  const parsed = parseHistoryOfMarketNdxForwardSeriesForTest(
    JSON.stringify({
      forward: [
        { date: "2001-04-30", value: 75.38 },
        { date: "2014-12-31", value: 17.41 },
        { date: "bad", value: 20 },
        { date: "2020-12-31", value: null },
      ],
    })
  );

  assert.deepEqual(
    parsed.map(({ date, value }) => ({ date, value })),
    [
      { date: "2001-04-30", value: 75.38 },
      { date: "2014-12-31", value: 17.41 },
    ]
  );
});

test("joins the Nasdaq-100 forward bootstrap to the Bloomberg monthly series without a daily scale break", () => {
  const closes = [
    { date: "2001-02-28", close: 100 },
    { date: "2001-03-21", close: 92 },
    { date: "2001-03-30", close: 88 },
    { date: "2001-04-06", close: 89 },
    { date: "2001-04-13", close: 91 },
    { date: "2001-04-20", close: 93 },
    { date: "2001-04-30", close: 95 },
  ];
  const fallback = closes.map(({ date }) => point(date, 60));
  const forward = [
    { date: "2001-04-30", value: 75.38, ts: Date.parse("2001-04-30T00:00:00Z") },
  ];
  const corrected = applyValidatedNasdaq100TtmHistoryForTest(fallback, closes, forward);
  const ratios = corrected.slice(1).map((row, index) =>
    Math.max(
      Number(row.pe_forward) / Number(corrected[index].pe_forward),
      Number(corrected[index].pe_forward) / Number(row.pe_forward)
    )
  );

  assert.ok(ratios.every((ratio) => ratio < 1.5));
  assert.equal(corrected.at(-1)?.pe_forward, 75.38);
});

test("leaves Nasdaq-100 PE unavailable before the first verified observations", () => {
  const closes = [
    { date: "1999-12-30", close: 100 },
    { date: "1999-12-31", close: 105 },
    { date: "2000-01-28", close: 110 },
    { date: "2000-01-31", close: 112 },
  ];
  const fallback = closes.map(({ date }) => point(date, 60));
  const corrected = applyValidatedNasdaq100TtmHistoryForTest(fallback, closes);

  assert.equal(corrected[0].pe_ttm, null);
  assert.equal(corrected[0].pe_forward, null);
  assert.equal(corrected[1].pe_ttm, 104);
  assert.equal(corrected[2].pe_forward, null);
  assert.equal(corrected[3].pe_forward, 95.92);
});

test("fails the build guard if a validated historical valuation drifts", () => {
  const previous = [point("2020-01-02", 23.04), point("2026-03-27", 30.3)];
  const drifted = [point("2020-01-02", 41.1), point("2026-03-27", 31.4)];

  assert.throws(
    () => assertValidatedIndexPointsUnchangedForTest(previous, drifted),
    /validated index history changed.*2020-01-02.*pe_ttm/
  );
  assert.doesNotThrow(() =>
    assertValidatedIndexPointsUnchangedForTest(previous, [point("2020-01-02", 23.04), point("2026-03-27", 31.4)])
  );
});

test("freezes the first published history for new thematic indices", () => {
  const published = [point("2026-04-02", 12), point("2026-07-16", 10.8)];
  const rebased = [point("2026-04-02", 24), point("2026-07-16", 21.6), point("2026-07-17", 11)];

  assert.deepEqual(preserveValidatedIndexHistoryForTest(published, rebased, "dram"), [
    ...published,
    point("2026-07-17", 11),
  ]);
});

test("keeps complete generated history when an index is first published", () => {
  const generated = [point("2001-07-17", 30), point("2026-07-16", 35)];
  assert.deepEqual(preserveValidatedIndexHistoryForTest([], generated, "igv"), generated);
  assert.deepEqual(
    preserveValidatedIndexHistoryForTest([point("2026-03-27", 34)], generated, "igv"),
    generated
  );
  assert.doesNotThrow(() =>
    assertValidatedIndexPointsUnchangedForTest([point("2026-03-27", 34)], generated, "igv")
  );
  assert.deepEqual(
    mergePublishedIndexHistoryAtCutoverForTest(
      "igv",
      [point("2026-03-27", 34)],
      generated,
      "2026-03-27"
    ),
    generated
  );
});

test("rejects a dated WSJ PE snapshot that jumps away from the existing series", () => {
  const series = [{ date: "2026-03-20", value: 35.6, ts: Date.parse("2026-03-20T00:00:00Z") }];

  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 66.64, 0.35), false);
  assert.equal(isLatestSnapshotDeviationAcceptableForTest(series, "2026-03-27", 38.75, 0.35), true);
});
