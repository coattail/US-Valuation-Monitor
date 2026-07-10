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
