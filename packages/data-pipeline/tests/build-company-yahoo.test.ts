import test from "node:test";
import assert from "node:assert/strict";

import {
  createYahooDailyMetricSnapshots,
  parseYahooValuationMeasuresFromHtml,
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
