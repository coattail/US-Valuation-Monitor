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

## Correction

From the first persisted WSJ observation, 2026-04-10, publish only the dated WSJ
Forward P/E observations. Keep all 25 values through 2026-09-25 unchanged,
including 24.62 on April 17 and 24.15 on September 25. Set the other 92 daily
Forward P/E estimates in that range to `null`. Do not interpolate, rescale or
price-carry missing forward earnings. Earlier history, all other valuation
fields, and all other indices retain their values.

The detail chart marks observed points and breaks the line at the source
transition. Its note explains the weekly frequency and earlier estimated
history. Percentiles restart at the source boundary; the detail range summary
uses the latest source's comparable observations. Lines within the weekly
segment connect observations visually, without adding daily records.

The same policy runs at the final publication boundary, on the history fallback,
and after gap recovery (including replayed gap records). A future dated WSJ
observation may fill an empty date via the existing exact-observation exception;
the append-only lock still rejects arbitrary historical rewrites.

## Reproduce and validate

```sh
npm run repair:data:nasdaq100-forward          # dry run
npm run repair:data:nasdaq100-forward -- --write
node packages/data-pipeline/src/split-index-dataset.ts
npm run build:site
npm test
```

The repair validates the existing lock before writing, changes only Nasdaq
Forward P/E, refreshes the lock, and is idempotent. Regression tests cover exact
observations, null days, alternate-provider rejection, delayed observations,
gap recovery, committed history/split-series agreement, source-break chart data,
and source-consistent percentiles.
