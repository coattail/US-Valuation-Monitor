# Nasdaq-100 Forward P/E audit

Audit date: 2026-09-26. Baseline: `faf3d882`.

The April–May spikes were a publication bug. The final
`applyAuthoritativePublishedMetricCorrections` pass inserted dated WSJ estimated
P/E observations into a daily curve that had already been reconstructed from a
different forward-P/E source. The append-only lock preserved the surrounding
estimates. Each weekly observation could therefore jump up and immediately fall
back to the old estimate on the next session.

Examples from the published dataset:

| Date | Published Forward P/E | Provenance |
| --- | ---: | --- |
| 2026-04-16 | 22.6557 | reconstructed daily estimate |
| 2026-04-17 | 24.62 | cached WSJ observation, captured 2026-04-24 |
| 2026-04-20 | 22.7560 | reconstructed daily estimate |
| 2026-04-23 | 22.7388 | reconstructed daily estimate |
| 2026-04-24 | 25.15 | cached WSJ observation, captured 2026-04-30 |
| 2026-04-27 | 23.0602 | reconstructed daily estimate |

The verified issue is source mixing, not evidence that the WSJ observations
are incorrect. Different forecasts, constituent treatments and averaging
methods can produce different forward P/E levels. A price history alone cannot
establish the daily consensus earnings denominator.

## Evidence and source limits

- The persisted observations, dates, `source: wsj-latest` and capture timestamps
  are in `data/standardized/index-yahoo-daily-metrics.json`, under `symbols.QQQ`.
- The source URLs used by the collector are
  [WSJ P/E & Yields](https://www.wsj.com/market-data/stocks/peyields) and
  [WSJ's legacy table](https://online.wsj.com/mdc/public/page/2_3021-peyield.html).
- The older history overlay fetched
  [History of Market's forward history](https://historyofmarket.com/api/ndx/forward-pe.json).
  The response retrieved during this audit describes its `forward` field as
  weekly 12-month blended-forward terminal consensus, and explicitly separates
  it from its own holdings-based `forwardOwn` calculation. Its 2026-04-17 value
  is 23.18, not the cached WSJ 24.62. The current response is not an immutable
  archive of the previously downloaded monthly history and was not used to
  silently rewrite that history.
- The live WSJ page returned HTTP 403 during this audit. The retained 25 WSJ
  values are the project's previously captured observations, not newly
  independently reverified historical quotes. No replacement quote was invented
  or inferred from visual smoothness.

## Current correction: daily market-based estimates

The initial repair in PR #9 kept only observed WSJ dates. The user then
requested a daily series driven by market movements instead of empty days.
The current policy retains the same 25 original WSJ quotes and reconstructs
all 92 intervening trading days using actual unadjusted Nasdaq-100 closes:

`estimated_forward_PE(t) = WSJ_PE(anchor) × NDX_close(t) / NDX_close(anchor)`

The anchor is the latest WSJ observation dated on or before the session. This
holds implied forward earnings constant until the next observation, which then
becomes the new anchor. No future quote is interpolated backward, no QQQ or
adjusted ETF price is substituted, and no other provider's P/E is mixed in.
A new WSJ estimate may cause an earnings-related level change; following days
continue from that new level instead of snapping back to the old series.

For example, the April 17 WSJ quote is unchanged at 24.62. NDX closed at
26,672.4296875 that day and 26,590.33984375 on April 20 in the Yahoo history,
so April 20 is estimated at **24.5442**, replacing the old mixed-source 22.7560
(and the temporary null from PR #9). The September 25 quote remains **24.15**.
All other valuation metrics and all other indices retain their values.

Each derived point carries `pe_forward_estimate` metadata containing the
formula, quote date/value, both closes, and their source URLs. An observed
quote carries no estimate marker. The UI identifies the estimates in tooltips,
marks original quotes, and explains the constant-earnings assumption. Earlier
history uses a different basis and remains separated at the source transition.
Percentiles and range changes now include all comparable daily observations
and estimates within the current WSJ basis.

## Price provenance and refresh

The initial 117 NDX closes, April 10–September 25, were parsed from the
`^NDX` Yahoo chart response retrieved during this audit:

https://query2.finance.yahoo.com/v8/finance/chart/%5ENDX?range=1y&interval=1d

The importer validates the symbol and uses `indicators.quote.close`, never
`adjclose`. These inputs are committed in
`data/standardized/nasdaq100-forward-closes.json`. Refresh fills missing dates
from FRED's `NASDAQ100` series first, then Yahoo's `^NDX` chart endpoints.
Previously persisted closes remain fixed for reproducibility. The cache is
included in the daily refresh artifact and published alongside the dataset.
If an actual index close or anchor is unavailable, coverage validation stops
publication of an incomplete Nasdaq forward series instead of inventing a
price. A network fallback may retain the already complete published history.

The final publication policy and gap recovery both use this same calculation.
A delayed WSJ quote updates its date and the affected subsequent estimates;
the history guard accepts only results reproducible from the persisted
quotes and closes. An estimate cannot overwrite a previously observed quote.

## Reproduce and validate

```sh
npm run repair:data:nasdaq100-forward          # dry run
npm run repair:data:nasdaq100-forward -- --write
node packages/data-pipeline/src/split-index-dataset.ts
npm run build:site
npm test
```

The repair validates the existing lock, changes only Nasdaq Forward P/E and
its estimate metadata, refreshes the lock, and is idempotent. Tests cover up
and down market days, anchor changes, unchanged observed quotes, absence of
look-ahead, delayed-quote propagation, tamper rejection, unadjusted NDX prices,
gap recovery, complete/reproducible committed daily history, artifact cache
publication, and tooltip labels.
