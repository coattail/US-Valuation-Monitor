import test from "node:test";
import assert from "node:assert/strict";

import {
  createYahooDailyMetricSnapshots,
  buildEffectiveYahooDailyMetricSnapshotsForTest,
  mergeYahooDrivenRatioPayloadForTest,
  parseYahooValuationMeasuresFromHtml,
  preserveExistingPeTtmHistoryForTest,
  selectLatestYahooRatioOverrideForTest,
} from "../src/build-company-snapshot.ts";

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
