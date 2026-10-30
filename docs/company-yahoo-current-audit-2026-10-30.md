# Company Yahoo valuation field audit — 2026-10-30

Policy update: the user subsequently requested the original quote-field TTM PE priority. TTM now prefers Yahoo quote-page/API `PE Ratio (TTM)` again; Current-table synchronization does not overwrite it. Forward PE, PB and PEG retain their independent source policy. The following audit describes the earlier Current-table rollout and its captured observations.

The reported Alphabet value, 14.74 on 2026-10-29, is present verbatim in `company-yahoo-daily-metrics.json` under **GOOG**, captured at `2026-10-30T07:16:21.609Z`, with source `yahoo-quote-page-latest:finance.yahoo.com`. The UI did not miscalculate it.

The supplied comparison is Yahoo's **GOOGL** key-statistics **Trailing P/E / Current** value, 16.97. GOOG and GOOGL are different share classes. Neither the ticker difference nor a presumed formula error alone explains the difference: the previous pipeline explicitly preferred the quote page's `PE Ratio (TTM)` and could stop HTML fetching before visiting the independent statistics page. Yahoo fields and page dates can differ around earnings updates. A cache-to-output check could pass while the independent statistics page disagreed.

## Shared source policy

- Fetch each company's own `/key-statistics/` before `/quote/`; reject explicit canonical redirects to another symbol.
- Prefer structurally identified `Current` cells for TTM PE, Forward PE, PB and PEG. Missing Current cells never borrow a historical column, a neighboring row or an embedded script value.
- Keep dated observations on their own dates. New publication dates require a Current observation; a quote-only source outage preserves the last verified valuation date. A fresh explicit Current observation takes precedence over rolling timeseries date hints and same-date quote fallbacks.
- Preserve metric-level sources and differences from fallback fields. Apply stored observations after every transformation, including fallback/reuse branches.
- Validate TTM PE, Forward PE and PB, use strict equality tolerance for Current observations, and verify snapshot/series identity, dates and values before publishing.
- Show the selected symbol, source, date and capture time in company details. Historical or fallback points without Current evidence are identified as such.

## Independent verification and repair

`npm run audit:yahoo:company` independently reads Current tables for the full company universe and reports unavailable sources, date mismatches and differences. It fails if coverage is incomplete or values differ by more than 2%.

`npm run refresh:yahoo:company` applies independently fetched Current values only when the published series and provider observation both match the last completed session, then recomputes all snapshot statistics and runs publication validation. It never extends a price series without prices or writes GOOGL data into GOOG. An additional GOOGL read is recorded solely as a share-class comparison. The report records exact coverage; a partial refresh is never described as complete verification.

GitHub workflow dispatch: `mode=company`, `company_current_only=true`. Branch dispatches produce reviewable artifacts without publishing to main. Normal scheduled builds use the shared policy above.

## Verification

The regression suite covers conflicting quote versus Current values, nested HTML, superscripts, unavailable Current cells, dated observations, sparse timeseries hints, repeated refreshes, wrong-symbol responses, PB agreement, and snapshot/series divergence. The complete local test suite passed (313 tests); static site construction and validation of the existing 100-company publication also passed.

Local Yahoo HTTP responses were region-blocked, so no current numerical correction was inferred from cached search results or the supplied GOOGL screenshot. Independent runner results are retained in `company-valuation-audit.json` and the workflow artifacts.

## Independent runner results

The [branch source audit](https://github.com/coattail/US-Valuation-Monitor/actions/runs/41037834102) read all 100 companies successfully on 2026-10-30. At least one metric differed by over 2% for 98 companies. Finite Current observations were available for TTM PE on 97 companies, Forward PE on 99, PB on 96, and PEG on 95. Explicit unavailable Current cells are published as null; CRWD, GILD and INTC have no current TTM PE. Their old PE values do not produce a current valuation judgment.

| Same-run Yahoo Current field | GOOG (platform) | GOOGL (comparison only) |
| --- | ---: | ---: |
| Trailing P/E | 16.81 | 16.97 |
| Forward P/E | 20.57 | 20.76 |
| Price/Book | 5.97 | 6.02 |
| PEG | 0.56 | 0.57 |

GOOG was captured at 2026-10-30T10:03:49.484Z; GOOGL at 10:03:55.411Z. The source report and reconciled dataset were downloaded from that run. Snapshot statistics were regenerated after applying those observations, including explicit nulls. Earlier dates were preserved. Browser verification confirmed GOOG PE 16.81, its Current-source link and conflict notice, and a missing-value state for CRWD, without JavaScript errors.
