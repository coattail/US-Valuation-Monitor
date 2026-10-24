# ETF TTM daily update audit

The DRAM chart showed a current value of 13.30 and long flat segments despite
complete daily dates. Inspection of the source observation cache found two
causes:

1. `stockanalysis-latest` ETF multiples were repeatedly recorded against new
   scrape dates without a dated fundamental observation. For example, DRAM
   repeatedly showed 14.93 from September 18 through October 13, 2026.
2. Changed source snapshots were excluded from the authoritative correction
   allowlist. From October 14 onward, the raw cache contained changed DRAM/SMH
   multiples, but append-only publication kept earlier price-based recoveries.
   On October 23 the DRAM source snapshot was 22.48 while publication was 13.296855.

## Publication policy

`scripts/repair-etf-ttm-history.mjs` reconciles DRAM, SMH, SOXX, IGV, IJH and VTI
after generation/gap recovery and before splitting publication files. It runs
in full, close-only and WSJ/overnight index refresh commands.

- Preserve all rows before July 17, 2026, and all non-TTM fields.
- Preserve dated official observations, including equal consecutive values.
- Treat consecutive equal undated Stock Analysis values as one anchor at the
  first sighting. A scrape date does not establish the underlying valuation date.
- Between anchors compute `anchor PE × current unadjusted ETF close / anchor close`.
  This holds the earnings denominator fixed; it is an estimate, not an issuer's
  historical daily fundamental multiple.
- Adopt a changed source value even when its date already has a published
  estimate. Never borrow a future snapshot to rebase an earlier interval.
- Persist exact price dates and source URLs in `etf-ttm-closes.json`. Source
  snapshots remain unchanged. Point-level estimate/snapshot metadata feeds the
  chart tooltip. Recovery ledger points are reconciled too.
- Verify the old history lock before changes; verify every non-TTM field stays
  equal; then rebuild the lock for the targeted repair. Missing prices abort
  reconciliation without changing publication data.

The initial repair changed 182 rows (values and/or provenance). The October 23
DRAM and SMH source snapshots now publish as 22.48 and 29.13 respectively. Other
indices and all Forward PE/PB values retain their existing calculation policies.

## Reproduction and validation

```bash
# Dry run; fetches missing prices without writing publication data.
node scripts/repair-etf-ttm-history.mjs
# Apply the targeted reconciliation and regenerate chart/percentile files.
npm run refresh:etf-ttm:index
node packages/data-pipeline/src/split-index-dataset.ts
npm run validate:data:freshness
npm run build:site
```

Freshness validation now rejects stale TTM values/provenance even if every
trading date exists. Regression cases cover repeated undated values, equal
dated official quotes, delayed changed snapshots, price-cache persistence,
history locks, idempotent retries, artifact publication, and tooltip labels.
