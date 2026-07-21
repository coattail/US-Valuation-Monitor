import test from "node:test";
import assert from "node:assert/strict";

import { validateCompanySeriesPayloads } from "./validate-company-series.mjs";

test("company validation accepts a recent published Yahoo TTM anchor", () => {
  const summary = validateCompanySeriesPayloads(
    [
      {
        symbol: "TSM",
        points: [
          { date: "2026-07-08", pe_ttm: 37.978074 },
          { date: "2026-07-09", pe_ttm: 38.35 },
        ],
      },
    ],
    {
      TSM: [{ date: "2026-07-08", pe_ttm: 37.978074 }],
    }
  );

  assert.equal(summary.companyCount, 1);
  assert.equal(summary.recentYahooAnchorCount, 1);
});

test("company validation rejects stale published PE when Yahoo arrives late", () => {
  assert.throws(
    () =>
      validateCompanySeriesPayloads(
        [
          {
            symbol: "TSM",
            points: [
              { date: "2026-07-08", pe_ttm: 44.70578 },
              { date: "2026-07-09", pe_ttm: 47.965694 },
            ],
          },
        ],
        {
          TSM: [{ date: "2026-07-08", pe_ttm: 37.978074 }],
        }
      ),
    /TSM Yahoo TTM PE mismatch on 2026-07-08/
  );
});

test("company validation rejects a published Forward PE that differs from Yahoo", () => {
  assert.throws(
    () =>
      validateCompanySeriesPayloads(
        [
          {
            symbol: "META",
            points: [
              { date: "2026-07-16", pe_ttm: 24.17, pe_forward: 21.64 },
              { date: "2026-07-17", pe_ttm: 23.49, pe_forward: 21.64 },
            ],
          },
        ],
        {
          META: [{ date: "2026-07-17", pe_ttm: 23.49, pe_forward: 18.31 }],
        }
      ),
    /META Yahoo Forward PE mismatch on 2026-07-17/
  );
});

test("company validation waits for the price series to reach a future Yahoo metric date", () => {
  const summary = validateCompanySeriesPayloads(
    [
      {
        symbol: "TCEHY",
        points: [
          { date: "2026-07-16", pe_ttm: 17.245553, pe_forward: 12.92 },
          { date: "2026-07-17", pe_ttm: 17.1499, pe_forward: 12.92 },
        ],
      },
    ],
    {
      TCEHY: [{ date: "2026-07-20", pe_ttm: 17.248368, pe_forward: null }],
    }
  );

  assert.equal(summary.companyCount, 1);
  assert.equal(summary.recentYahooAnchorCount, 0);
});

test("company validation applies the Yahoo rule to every symbol", () => {
  assert.throws(
    () =>
      validateCompanySeriesPayloads(
        [
          {
            symbol: "ABC",
            points: [
              { date: "2026-07-08", pe_ttm: 30 },
              { date: "2026-07-09", pe_ttm: 30.2 },
            ],
          },
        ],
        {
          ABC: [{ date: "2026-07-08", pe_ttm: 20 }],
        }
      ),
    /ABC Yahoo TTM PE mismatch/
  );
});

test("company validation protects the long TSM forward PE history", () => {
  const summary = validateCompanySeriesPayloads(
    [
      {
        symbol: "TSM",
        points: [
          { date: "2015-12-31", pe_forward: 13.97, pe_ttm: 20 },
          ...Array.from({ length: 500 }, (_, index) => ({
            date: `2016-01-${String((index % 28) + 1).padStart(2, "0")}`,
            pe_forward: 14 + index / 100,
            pe_ttm: 20,
          })),
          { date: "2026-07-17", pe_forward: 27.03, pe_ttm: 30 },
        ],
      },
    ],
    {},
    { requireTsmForwardHistory: true }
  );

  assert.equal(summary.companyCount, 1);
});

test("company validation rejects an extreme short-window TSM forward PE jump", () => {
  assert.throws(
    () =>
      validateCompanySeriesPayloads(
        [
          {
            symbol: "TSM",
            points: [
              { date: "2015-12-31", pe_forward: 20, pe_ttm: 20 },
              ...Array.from({ length: 500 }, (_, index) => ({
                date: "2016-01-01",
                pe_forward: index === 0 ? 200 : 20 + index / 100,
                pe_ttm: 20,
              })),
              { date: "2026-07-17", pe_forward: 27.03, pe_ttm: 30 },
            ],
          },
        ],
        {},
        { requireTsmForwardHistory: true }
      ),
    /TSM forward PE validation failed:/
  );
});
