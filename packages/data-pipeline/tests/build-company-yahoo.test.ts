import test from "node:test";
import assert from "node:assert/strict";

import {
  createYahooDailyMetricSnapshots,
  buildYahooPageForwardPeOverrideForTest,
  applyRecordedYahooForwardPeAnchorsToPointsForTest,
  fillMissingCompanySeriesFromPreviousForTest,
  mergeCurrentYahooSnapshotsIntoHistoryForTest,
  mergeYahooLatestQuotePayloadsForTest,
  mergeCloseSeriesForTest,
  buildEffectiveYahooDailyMetricSnapshotsForTest,
  mergeYahooDrivenRatioPayloadForTest,
  parseYahooValuationMeasuresFromHtml,
  parseYahooQuotePageRatioPayloadForTest,
  preserveExistingPeTtmHistoryForTest,
  reconcileRecordedYahooPeTtmHistoryForTest,
  repairRecentTransientPeTtmPulsesForTest,
  carryForwardLatestYahooPeTtmByCloseForTest,
  carryForwardMissingPeTtmByPreviousCloseForTest,
  assertRecentYahooPeTtmCarryConsistencyForTest,
  selectCompanyCloseHistoryForTest,
  selectLatestYahooRatioOverrideForTest,
  stripGrowthOnlyFieldsFromSnapshotPointsForTest,
} from "../src/build-company-snapshot.ts";

test("dense close overlay removes fallback holiday points and rebases its prefix", () => {
  const day = 86_400_000;
  const merged = mergeCloseSeriesForTest(
    [
      { date: "2001-09-07", close: 24, ts: Date.parse("2001-09-07T00:00:00Z") },
      { date: "2001-09-10", close: 25, ts: Date.parse("2001-09-10T00:00:00Z") },
      { date: "2001-09-11", close: 24.5, ts: Date.parse("2001-09-11T00:00:00Z") },
    ],
    [
      { date: "2001-09-10", close: 40, ts: Date.parse("2001-09-10T00:00:00Z") },
      { date: "2001-09-17", close: 39, ts: Date.parse("2001-09-10T00:00:00Z") + 7 * day },
      { date: "2001-09-18", close: 40, ts: Date.parse("2001-09-10T00:00:00Z") + 8 * day },
    ]
  );

  assert.deepEqual(merged.map((point) => point.date), ["2001-09-07", "2001-09-10", "2001-09-17", "2001-09-18"]);
  assert.ok(Math.abs(merged[0].close - 38.4) < 1e-9);
  assert.equal(merged[1].close, 40);
});

test("Yahoo close history wins on overlapping dates while fallback may extend the tail", () => {
  const start = Date.UTC(2025, 11, 1);
  const makePoint = (index, close) => {
    const date = new Date(start + index * 86_400_000).toISOString().slice(0, 10);
    return { date, close, ts: Date.parse(`${date}T00:00:00Z`) };
  };
  const stooq = Array.from({ length: 201 }, (_, index) => makePoint(index, 100 + index));
  const yahoo = Array.from({ length: 200 }, (_, index) => makePoint(index, 200 + index));

  const selected = selectCompanyCloseHistoryForTest(stooq, yahoo, [], []);

  assert.equal(selected.find((point) => point.date === yahoo[199].date)?.close, yahoo[199].close);
  assert.ok(selected.some((point) => point.date === stooq[200].date));
});

test("a failed new entrant keeps the previous company set at the target size", () => {
  const point = (date, pe_ttm) => ({
    date,
    close: 100,
    pe_ttm,
    pe_forward: 20,
    pb: 5,
    peg: 1,
    us10y_yield: 0,
  });
  const current = {
    id: "company_new",
    symbol: "NEW",
    displayName: "New",
    description: "New (NEW)",
    rank: 1,
    marketCap: 100,
    peg: 1,
    forwardStartDate: "2026-01-01",
    points: Array.from({ length: 24 }, (_, index) => point(`2026-01-${String(index + 1).padStart(2, "0")}`, 20)),
    quarterlyEps: [],
    quarterlyNetIncome: [],
  };
  const previousPoints = Array.from({ length: 24 }, (_, index) =>
    point(`2025-01-${String(index + 1).padStart(2, "0")}`, 18)
  );

  const filled = fillMissingCompanySeriesFromPreviousForTest(
    [current, null],
    new Map([
      [
        "OLD",
        {
          id: "company_old",
          symbol: "OLD",
          displayName: "Old",
          description: "Old (OLD)",
          rank: 2,
          marketCap: 90,
          forwardStartDate: "2025-01-01",
          points: previousPoints,
          quarterlyEps: [],
          quarterlyNetIncome: [],
        },
      ],
    ]),
    2
  );

  assert.deepEqual(filled.map((item) => item.symbol), ["NEW", "OLD"]);
  assert.deepEqual(filled.map((item) => item.rank), [1, 2]);
});

test("a fallback company is rebased to its recent Yahoo TTM anchor and price path", () => {
  const points = Array.from({ length: 24 }, (_, index) => ({
    date: `2026-06-${String(index + 1).padStart(2, "0")}`,
    close: 100 + index,
    pe_ttm: 40 + index,
    pe_forward: 20,
    pb: 5,
    peg: 1,
    us10y_yield: 0,
  }));
  const filled = fillMissingCompanySeriesFromPreviousForTest(
    [null],
    new Map([
      [
        "OLD",
        {
          symbol: "OLD",
          displayName: "Old",
          rank: 1,
          marketCap: 100,
          forwardStartDate: "2026-06-01",
          points,
        },
      ],
    ]),
    1,
    new Map([
      [
        "OLD",
        [
          {
            date: "2026-06-23",
            pe_ttm: 31,
            pe_forward: null,
            pb: null,
            peg: null,
            source: "yahoo-trailing-pe-timeseries",
            capturedAt: "2026-06-24T00:00:00.000Z",
          },
        ],
      ],
    ])
  );

  assert.equal(filled[0].points[22].pe_ttm, 31);
  assert.equal(filled[0].points[23].pe_ttm, 31.5);
});

test("published company series preserves signed PE", () => {
  const points = stripGrowthOnlyFieldsFromSnapshotPointsForTest([
    {
      date: "2003-10-30",
      close: 10,
      pe_ttm: -2_000,
      pe_forward: -40,
      pb: 3,
      peg: null,
      us10y_yield: 0,
    },
    {
      date: "2003-10-31",
      close: 11,
      pe_ttm: 30,
      pe_forward: 25,
      pb: 3.1,
      peg: null,
      us10y_yield: 0,
    },
  ]);

  assert.equal(points[0].close, undefined);
  assert.equal(points[0].pe_ttm, -2_000);
  assert.equal(points[0].pe_forward, -40);
  assert.equal(points[0].pb, 3);
  assert.equal(points[1].pe_ttm, 30);
  assert.equal(points[1].pe_forward, 25);
});


test("Yahoo quote page parser extracts PE Ratio TTM from quote summary", () => {
  const payload = parseYahooQuotePageRatioPayloadForTest(`
    <li><span class="label" title="PE Ratio (TTM)">PE Ratio (TTM)</span>
    <span class="value"><fin-streamer data-value="34.22" data-field="trailingPE">34.22</fin-streamer></span></li>
    <li><span class="label" title="EPS (TTM)">EPS (TTM)</span>
    <span class="value"><fin-streamer data-value="6.53">6.53</fin-streamer></span></li>
  `);

  assert.deepEqual(payload?.latest, {
    pe_ttm: 34.22,
    pe_forward: null,
    pb: null,
    peg: null,
  });
  assert.equal(payload?.source, "yahoo-quote-page-latest");
});

test("Yahoo valuation measures are anchored to their As of date", () => {
  const payload = parseYahooValuationMeasuresFromHtml(`
    <section data-testid="valuation-measures">
      <div class="asofdate">As of 4/23/2026</div>
      <ul>
        <li><p>Trailing P/E</p><p>40.74</p></li>
        <li><p>Forward P/E</p><p>24.51</p></li>
        <li><p>PEG Ratio (5yr expected)</p><p>0.71</p></li>
        <li><p>Price/Book (mrq)</p><p>30.84</p></li>
      </ul>
    </section>
  `);

  assert.deepEqual(payload?.latest, {
    pe_ttm: null,
    pe_forward: null,
    pb: null,
    peg: null,
  });
  assert.deepEqual(payload?.anchors, [
    {
      date: "2026-04-23",
      pe_ttm: 40.74,
      pe_forward: 24.51,
      pb: 30.84,
      peg: 0.71,
    },
  ]);
});

test("Yahoo valuation parser keeps the Current column instead of a historical Forward P/E column", () => {
  const payload = parseYahooValuationMeasuresFromHtml(`
    <section data-testid="valuation-measures">
      <div class="asofdate">As of 7/17/2026</div>
      <table>
        <thead><tr><th></th><th>Current</th><th>6/30/2026</th><th>3/31/2026</th></tr></thead>
        <tbody><tr><td>Forward P/E</td><td>18.31</td><td>21.64</td><td>19.08</td></tr></tbody>
      </table>
    </section>
  `);

  assert.equal(payload?.anchors[0]?.pe_forward, 18.31);
});

test("Yahoo page Current Forward P/E overrides the timeseries latest value when the page is fresh", () => {
  const override = buildYahooPageForwardPeOverrideForTest(
    {
      anchors: [
        { date: "2026-07-16", pe_ttm: 24.17, pe_forward: 18.31, pb: 7.1, peg: 0.96 },
      ],
      latest: { pe_ttm: null, pe_forward: null, pb: null, peg: null },
      source: "yahoo-key-statistics-valuation-measures",
    },
    "2026-07-17"
  );

  assert.deepEqual(override?.latest, { pe_ttm: null, pe_forward: 18.31, pb: null, peg: null });
});

test("stale Yahoo page Forward P/E cannot overwrite a newer fallback", () => {
  const override = buildYahooPageForwardPeOverrideForTest(
    {
      anchors: [
        { date: "2026-06-30", pe_ttm: 24.17, pe_forward: 18.31, pb: 7.1, peg: 0.96 },
      ],
      latest: { pe_ttm: null, pe_forward: null, pb: null, peg: null },
      source: "yahoo-key-statistics-valuation-measures",
    },
    "2026-07-17"
  );

  assert.equal(override, null);
});

test("Yahoo valuation measures do not keep impossible future as-of dates", () => {
  const payload = parseYahooValuationMeasuresFromHtml(`
    <section data-testid="valuation-measures">
      <div class="asofdate">As of 09/04/2026</div>
      <ul>
        <li><p>Forward P/E</p><p>25.00</p></li>
        <li><p>Price/Book (mrq)</p><p>61.81</p></li>
      </ul>
    </section>
  `);

  assert.deepEqual(payload?.anchors, [
    {
      date: "2026-04-09",
      pe_ttm: null,
      pe_forward: 25,
      pb: 61.81,
      peg: null,
    },
  ]);
});

test("Yahoo daily snapshots do not stamp stale dated table values onto the latest close date", () => {
  const payload = parseYahooValuationMeasuresFromHtml(`
    <section data-testid="valuation-measures">
      <div class="asofdate">As of 4/23/2026</div>
      <ul>
        <li><p>Trailing P/E</p><p>40.74</p></li>
        <li><p>Forward P/E</p><p>24.51</p></li>
        <li><p>PEG Ratio (5yr expected)</p><p>0.71</p></li>
        <li><p>Price/Book (mrq)</p><p>30.84</p></li>
      </ul>
    </section>
  `);

  const snapshots = createYahooDailyMetricSnapshots(payload, "2026-04-24", {}, {});

  assert.match(snapshots[0]?.capturedAt || "", /^\d{4}-\d{2}-\d{2}T/);
  assert.deepEqual(
    snapshots.map((item) => ({ ...item, capturedAt: "<captured>" })),
    [
      {
        date: "2026-04-23",
        pe_ttm: 40.74,
        pe_forward: 24.51,
        pb: 30.84,
        peg: 0.71,
        source: "yahoo-key-statistics-valuation-measures",
        capturedAt: "<captured>",
      },
    ]
  );
});

test("Yahoo current valuation measures without As of date use the latest close date", () => {
  const payload = parseYahooValuationMeasuresFromHtml(`
    <section data-testid="valuation-measures">
      <ul>
        <li><p>Trailing P/E</p><p>42.50</p></li>
        <li><p>Forward P/E</p><p>25.58</p></li>
        <li><p>Price/Book (mrq)</p><p>32.18</p></li>
      </ul>
    </section>
  `);

  assert.deepEqual(
    createYahooDailyMetricSnapshots(payload, "2026-04-24", {}, {}).map((item) => ({
      ...item,
      capturedAt: "<captured>",
    })),
    [
      {
        date: "2026-04-24",
        pe_ttm: 42.5,
        pe_forward: 25.58,
        pb: 32.18,
        peg: null,
        source: "yahoo-key-statistics-valuation-measures",
        capturedAt: "<captured>",
      },
    ]
  );
});

test("Yahoo trailing PE timeseries does not replace generated TTM PE history", () => {
  const merged = mergeYahooDrivenRatioPayloadForTest(
    {
      anchors: [
        { date: "2026-04-27", pe_ttm: 32.24, pe_forward: 29.94, pb: 10.15, peg: null },
        { date: "2026-04-28", pe_ttm: 32.5, pe_forward: 30.1, pb: 10.2, peg: null },
      ],
      latest: { pe_ttm: 32.5, pe_forward: 30.1, pb: 10.2, peg: null },
      source: "base",
    },
    [],
    {
      anchors: [
        { date: "2026-04-27", pe_ttm: 26.71, pe_forward: null, pb: null, peg: null },
        { date: "2026-04-28", pe_ttm: 26.9, pe_forward: null, pb: null, peg: null },
      ],
      latest: { pe_ttm: null, pe_forward: null, pb: null, peg: null },
      source: "yahoo-trailing-pe-timeseries",
    },
    null
  );

  assert.deepEqual(merged?.anchors, [
    { date: "2026-04-27", pe_ttm: 32.24, pe_forward: 29.94, pb: 10.15, peg: null },
    { date: "2026-04-28", pe_ttm: 32.5, pe_forward: 30.1, pb: 10.2, peg: null },
  ]);
});

test("recorded Yahoo Forward PE wins after price-path interpolation", () => {
  const points = [
    { date: "2026-07-16", close: 71.4, pe_ttm: 13.252336, pe_forward: 7.59, pb: 1.35, peg: 1.24, us10y_yield: 0 },
    { date: "2026-07-17", close: 69.12, pe_ttm: 13.6, pe_forward: 7.3472, pb: 1.39, peg: -1, us10y_yield: 0 },
    { date: "2026-07-20", close: 64.36, pe_ttm: 13.426791, pe_forward: 6.8409, pb: 1.3722, peg: 1.256, us10y_yield: 0 },
  ];

  const reconciled = applyRecordedYahooForwardPeAnchorsToPointsForTest(points, [
    {
      date: "2026-07-17",
      pe_ttm: 13.6,
      pe_forward: 7.66,
      pb: 1.39,
      peg: -1,
      source: "yahoo-key-statistics-valuation-measures+yahoo-quote-page-latest:finance.yahoo.com",
      capturedAt: "2026-07-21T22:24:47.000Z",
    },
  ]);

  assert.equal(reconciled[0].pe_forward, 7.59);
  assert.equal(reconciled[1].pe_forward, 7.66);
  assert.equal(reconciled[2].pe_forward, 6.8409);
});

test("existing TTM PE history is preserved while new dates remain generated", () => {
  const previous = [
    { date: "2026-04-27", close: 100, pe_ttm: 32.24, pe_forward: 29.94, pb: 10.15, peg: 2.1, us10y_yield: 0 },
  ];
  const generated = [
    { date: "2026-04-27", close: 100, pe_ttm: 26.71, pe_forward: 30.4, pb: 10.2, peg: 1.7, us10y_yield: 0 },
    { date: "2026-04-28", close: 101, pe_ttm: 26.9, pe_forward: 30.6, pb: 10.3, peg: 1.8, us10y_yield: 0 },
  ];

  assert.deepEqual(preserveExistingPeTtmHistoryForTest(generated, previous, "2026-04-28"), [
    { date: "2026-04-27", close: 100, pe_ttm: 32.24, pe_forward: 30.4, pb: 10.2, peg: 2.1, us10y_yield: 0 },
    { date: "2026-04-28", close: 101, pe_ttm: 26.9, pe_forward: 30.6, pb: 10.3, peg: 1.8, us10y_yield: 0 },
  ]);
});

test("explicit Yahoo TTM PE snapshots are kept through earnings resets", () => {
  const effective = buildEffectiveYahooDailyMetricSnapshotsForTest(
    [
      { date: "2026-04-28", close: 100, ts: 1777334400000 },
      { date: "2026-04-29", close: 99, ts: 1777420800000 },
    ],
    [
      {
        date: "2026-04-28",
        pe_ttm: 37,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-key-statistics-valuation-measures",
        capturedAt: "2026-04-30T00:00:00.000Z",
      },
      {
        date: "2026-04-29",
        pe_ttm: 26.49,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
        capturedAt: "2026-04-30T01:00:00.000Z",
      },
    ]
  );

  assert.equal(effective.at(-1)?.pe_ttm, 26.49);
});

test("latest Yahoo TTM PE override uses the unfiltered daily snapshot", () => {
  const override = selectLatestYahooRatioOverrideForTest(
    [
      {
        date: "2025-10-02",
        pe_ttm: 26.271855,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
        capturedAt: "2026-04-30T00:00:00.000Z",
      },
      {
        date: "2026-04-29",
        pe_ttm: 26.491991,
        pe_forward: null,
        pb: 8.78,
        peg: 2.376,
        source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
        capturedAt: "2026-04-30T01:00:00.000Z",
      },
    ],
    [
      {
        date: "2026-04-29",
        pe_ttm: null,
        pe_forward: null,
        pb: 8.78,
        peg: 2.376,
        source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
        capturedAt: "2026-04-30T01:00:00.000Z",
      },
    ],
    "2026-04-29"
  );

  assert.equal(override?.latest.pe_ttm, 26.491991);
});




test("merged Yahoo latest quote payload prioritizes quote API TTM PE over timeseries TTM PE", () => {
  const merged = mergeYahooLatestQuotePayloadsForTest([
    {
      anchors: [{ date: "2026-04-28", pe_ttm: 26.49, pe_forward: null, pb: null, peg: null }],
      latest: { pe_ttm: 26.49, pe_forward: null, pb: null, peg: null },
      source: "yahoo-trailing-pe-timeseries",
    },
    {
      anchors: [],
      latest: { pe_ttm: 40.74, pe_forward: null, pb: null, peg: null },
      source: "yahoo-quote-api-latest",
    },
  ]);

  assert.equal(merged?.latest.pe_ttm, 40.74);
  assert.equal(merged?.source, "yahoo-trailing-pe-timeseries+yahoo-quote-api-latest");
});

test("quote API TTM PE daily snapshot uses the latest close date even when timeseries has an older PE date", () => {
  const snapshots = createYahooDailyMetricSnapshots(
    {
      anchors: [],
      latest: { pe_ttm: 40.74, pe_forward: null, pb: null, peg: null },
      source: "yahoo-quote-api-latest",
    },
    "2026-04-29",
    { pe_ttm: "2026-04-28" },
    { pe_ttm: 26.49 }
  );

  assert.deepEqual(
    snapshots.map((item) => ({ ...item, capturedAt: "<captured>" })),
    [
      {
        date: "2026-04-29",
        pe_ttm: 40.74,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-quote-api-latest",
        capturedAt: "<captured>",
      },
    ]
  );
});

test("latest Yahoo TTM PE override prefers quote API over timeseries on the close date", () => {
  const override = selectLatestYahooRatioOverrideForTest(
    [
      {
        date: "2026-04-29",
        pe_ttm: 26.49,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-trailing-pe-timeseries",
        capturedAt: "2026-04-30T00:00:00.000Z",
      },
      {
        date: "2026-04-29",
        pe_ttm: 40.74,
        pe_forward: null,
        pb: null,
        peg: null,
        source: "yahoo-quote-api-latest",
        capturedAt: "2026-04-30T01:00:00.000Z",
      },
    ],
    [],
    "2026-04-29"
  );

  assert.equal(override?.latest.pe_ttm, 40.74);
  assert.equal(override?.source, "yahoo-quote-api-latest");
});

test("latest Yahoo TTM PE is carried by close when the next Yahoo snapshot lacks TTM", () => {
  const points = [
    { date: "2026-04-28", close: 100, pe_ttm: 37, pe_forward: 25, pb: 10, peg: 2.3, us10y_yield: 0 },
    { date: "2026-04-29", close: 101, pe_ttm: 37.37, pe_forward: 25.2, pb: 10.1, peg: 2.3, us10y_yield: 0 },
    { date: "2026-04-30", close: 102, pe_ttm: 40.83, pe_forward: 30.4, pb: 8.78, peg: 2.3, us10y_yield: 0 },
  ];

  const carried = carryForwardLatestYahooPeTtmByCloseForTest(points, [
    {
      date: "2026-04-29",
      pe_ttm: 26.491991,
      pe_forward: null,
      pb: null,
      peg: 2.376,
      source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
      capturedAt: "2026-05-01T01:00:00.000Z",
    },
    {
      date: "2026-04-30",
      pe_ttm: null,
      pe_forward: 30.4,
      pb: 8.78,
      peg: null,
      source: "yahoo-key-statistics-valuation-measures+yahoo-trailing-pe-timeseries",
      capturedAt: "2026-05-01T01:00:00.000Z",
    },
  ]);

  assert.equal(carried[1].pe_ttm, 26.491991);
  assert.equal(carried[2].pe_ttm, 26.754288);
});

test("TSM earnings-day Yahoo anchor forces the next close onto the same TTM EPS basis", () => {
  const points = [
    { date: "2026-07-16", close: 409.74, pe_ttm: 30.657888, pe_forward: 27.03, pb: 11.84, peg: 1.28, us10y_yield: 0 },
    { date: "2026-07-17", close: 398.37, pe_ttm: 31.982134, pe_forward: 27.03, pb: 11.81, peg: 1.28, us10y_yield: 0 },
  ];
  const snapshots = [
    {
      date: "2026-07-16",
      pe_ttm: 30.657888,
      pe_forward: 27.03,
      pb: 11.84,
      peg: 1.2817,
      source: "yahoo-trailing-pe-timeseries",
      capturedAt: "2026-07-17T22:15:07.406Z",
    },
    {
      date: "2026-07-17",
      pe_ttm: null,
      pe_forward: 27.03,
      pb: 11.81,
      peg: null,
      source: "yahoo-quote-page-latest",
      capturedAt: "2026-07-17T22:15:07.406Z",
    },
  ];

  assert.throws(
    () => assertRecentYahooPeTtmCarryConsistencyForTest(points, snapshots),
    /carry invariant failed/
  );

  const carried = carryForwardLatestYahooPeTtmByCloseForTest(points, snapshots);
  assert.equal(carried[1].pe_ttm, 29.807153);
  assert.doesNotThrow(() => assertRecentYahooPeTtmCarryConsistencyForTest(carried, snapshots));
});

test("a delayed Yahoo TTM PE observation replaces the stale published value and carries forward", () => {
  const previous = [
    { date: "2026-07-07", close: 100, pe_ttm: 41.25, pe_forward: 28, pb: 12, peg: 1.4, us10y_yield: 0 },
    { date: "2026-07-08", close: 102, pe_ttm: 44.71, pe_forward: 28, pb: 12, peg: 1.4, us10y_yield: 0 },
  ];
  const generated = [
    ...previous,
    { date: "2026-07-09", close: 103, pe_ttm: 47.97, pe_forward: 28, pb: 12, peg: 1.4, us10y_yield: 0 },
  ];
  const delayedYahooSnapshot = {
    date: "2026-07-08",
    pe_ttm: 37.978074,
    pe_forward: null,
    pb: null,
    peg: null,
    source: "yahoo-trailing-pe-timeseries",
    capturedAt: "2026-07-09T22:51:22.095Z",
  };

  const reconciled = reconcileRecordedYahooPeTtmHistoryForTest(
    generated,
    previous,
    [delayedYahooSnapshot],
    [delayedYahooSnapshot],
    "2026-07-09"
  );

  assert.equal(reconciled[1].pe_ttm, 37.978074);
  assert.equal(reconciled[2].pe_ttm, 38.350408);
});

test("a later sparse Yahoo response cannot erase a previously recorded TTM anchor", () => {
  const merged = mergeCurrentYahooSnapshotsIntoHistoryForTest(
    [
      {
        date: "2026-07-08",
        pe_ttm: 37.978074,
        pe_forward: 28.41,
        pb: 12.31,
        peg: 1.3684,
        source: "yahoo-trailing-pe-timeseries",
        capturedAt: "2026-07-09T22:51:22.095Z",
      },
    ],
    [
      {
        date: "2026-07-09",
        pe_ttm: 37.976336,
        pe_forward: 28.41,
        pb: 12.31,
        peg: 1.3678,
        source: "yahoo-quote-page-latest",
        capturedAt: "2026-07-10T16:40:28.444Z",
      },
    ],
    { pe_ttm: "2026-07-09" },
    { pe_ttm: ["2026-07-09"] }
  );

  assert.equal(merged.find((item) => item.date === "2026-07-08")?.pe_ttm, 37.978074);
  assert.equal(merged.find((item) => item.date === "2026-07-09")?.pe_ttm, 37.976336);
});

test("a stale Yahoo TTM PE anchor is not carried beyond the short reporting delay window", () => {
  const points = [
    { date: "2026-06-01", close: 100, pe_ttm: 25, pe_forward: 20, pb: 8, peg: 1.2, us10y_yield: 0 },
    { date: "2026-06-15", close: 110, pe_ttm: 30, pe_forward: 22, pb: 9, peg: 1.3, us10y_yield: 0 },
  ];
  const carried = carryForwardLatestYahooPeTtmByCloseForTest(points, [
    {
      date: "2026-06-01",
      pe_ttm: 25,
      pe_forward: null,
      pb: null,
      peg: null,
      source: "yahoo-trailing-pe-timeseries",
      capturedAt: "2026-06-02T00:00:00.000Z",
    },
  ]);

  assert.equal(carried[1].pe_ttm, 30);
});

test("recent transient PE pulses are repaired from the price-implied EPS path", () => {
  const points = [
    { date: "2026-05-28", close: 100, pe_ttm: 35, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-05-29", close: 101, pe_ttm: 45, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-06-01", close: 102, pe_ttm: 46, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-06-02", close: 103, pe_ttm: 36, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
  ];

  const repaired = repairRecentTransientPeTtmPulsesForTest(points);

  assert.equal(repaired.repairedCount, 2);
  assert.ok(Math.abs(repaired.points[1].pe_ttm - 35.340189) < 1e-6);
  assert.ok(Math.abs(repaired.points[2].pe_ttm - 35.660383) < 1e-6);
});

test("transient PE repair tolerates a moderate real EPS change around the bad pulse", () => {
  const points = [
    { date: "2026-05-28", close: 100, pe_ttm: 35, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-05-29", close: 101, pe_ttm: 100, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-06-02", close: 103, pe_ttm: 30, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
  ];

  const repaired = repairRecentTransientPeTtmPulsesForTest(points);

  assert.equal(repaired.repairedCount, 1);
  assert.ok(repaired.points[1].pe_ttm > 30 && repaired.points[1].pe_ttm < 35);
});

test("a multi-fold PE pulse is repaired directly from the previous PE and price path", () => {
  const points = [
    { date: "2026-05-28", close: 100, pe_ttm: 36, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-05-29", close: 105, pe_ttm: 122, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-06-02", close: 50, pe_ttm: 31, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
  ];

  const repaired = repairRecentTransientPeTtmPulsesForTest(points);

  assert.equal(repaired.repairedCount, 1);
  assert.equal(repaired.points[1].pe_ttm, 37.8);
});

test("trusted Yahoo PE dates are never changed by transient-pulse repair", () => {
  const points = [
    { date: "2026-05-28", close: 100, pe_ttm: 35, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-05-29", close: 101, pe_ttm: 45, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
    { date: "2026-06-02", close: 103, pe_ttm: 36, pe_forward: 27, pb: 11, peg: 1.3, us10y_yield: 0 },
  ];

  const repaired = repairRecentTransientPeTtmPulsesForTest(points, new Set(["2026-05-29"]));

  assert.equal(repaired.repairedCount, 0);
  assert.equal(repaired.points[1].pe_ttm, 45);
});

test("missing TTM PE is carried from the preserved previous TTM PE by close", () => {
  const points = [
    { date: "2026-05-20", close: 100, pe_ttm: 34.22, pe_forward: 27.9, pb: 34.8, peg: 0.7, us10y_yield: 0 },
    { date: "2026-05-21", close: 102, pe_ttm: 44.79, pe_forward: 27.55, pb: 34.41, peg: 0.71, us10y_yield: 0 },
  ];

  const carried = carryForwardMissingPeTtmByPreviousCloseForTest(points, "2026-05-21");

  assert.equal(carried[0].pe_ttm, 34.22);
  assert.equal(carried[1].pe_ttm, 34.9044);
});
