import test from "node:test";
import assert from "node:assert/strict";

import {
  createYahooDailyMetricSnapshots,
  mergeYahooLatestQuotePayloadsForTest,
  mergeCloseSeriesForTest,
  buildEffectiveYahooDailyMetricSnapshotsForTest,
  mergeYahooDrivenRatioPayloadForTest,
  parseYahooValuationMeasuresFromHtml,
  parseYahooQuotePageRatioPayloadForTest,
  preserveExistingPeTtmHistoryForTest,
  carryForwardLatestYahooPeTtmByCloseForTest,
  carryForwardMissingPeTtmByPreviousCloseForTest,
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

test("missing TTM PE is carried from the preserved previous TTM PE by close", () => {
  const points = [
    { date: "2026-05-20", close: 100, pe_ttm: 34.22, pe_forward: 27.9, pb: 34.8, peg: 0.7, us10y_yield: 0 },
    { date: "2026-05-21", close: 102, pe_ttm: 44.79, pe_forward: 27.55, pb: 34.41, peg: 0.71, us10y_yield: 0 },
  ];

  const carried = carryForwardMissingPeTtmByPreviousCloseForTest(points, "2026-05-21");

  assert.equal(carried[0].pe_ttm, 34.22);
  assert.equal(carried[1].pe_ttm, 34.9044);
});
