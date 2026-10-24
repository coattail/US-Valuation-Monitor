# Index daily valuation update audit — 2026-10-25

## Findings

Audited PE TTM, Forward PE and PB for all 21 indices from March 27, 2026.
Long exact-value plateaus remained in Dow Forward PE and the IJH/XLRE forward
series. Other failures included delayed forward observations not replacing
published estimates and a blanket assumption that Forward PE must be below TTM.

- [WSJ](https://www.wsj.com/market-data/stocks/peyields) explicitly says the
  Forward 12-month field is updated weekly on Friday. The Dow table's daily TTM
  date therefore cannot refresh the date of the forward observation.
- [SSGA XLRE](https://www.ssga.com/us/en/institutional/etfs/state-street-real-estate-select-sector-spdr-etf-xlre)
  reports FY1 PE 33.90 and trailing PE 31.63 as of October 22. This valid official
  forward quote was rejected because FY1 was above TTM. PB labels also contained
  tooltip text that made the parser miss the metric.
- The six thematic/core ETFs previously repaired for TTM had additional forward
  snapshots that could not replace old estimates after their dates were frozen.

## Daily forward publication

`reconcile-index-forward.mjs` now runs in every standard index refresh mode,
after source synchronization and before splitting chart files. It covers 18
series. The existing independently audited SPX/NDX rules and DRAM's separate
forward estimate basis are retained.

Actual weekly WSJ anchors stay intact, including equal Friday observations.
Between them use the actual unadjusted index close (`^DJI` / `^RUT`), never the
tracking ETF. A changed weekday quote is retained at its first observed date;
repeated copies of that weekly field are not new anchors. Official ETF daily
observations remain intact, including equal daily numbers. Undated duplicates
keep their first sighting.

`PE(day) = PE(anchor) × close(day) / close(anchor)` between observations. The
result is explicitly an estimate holding the implied earnings denominator fixed.
Where no observed forward anchor exists yet, a fixed previously published
estimate is retained as the seed, labeled as an estimated historical basis.
It is not represented as a newly verified source quote. Latest official source
observations and original price URLs are persisted in `index-forward-inputs.json`.
Missing prices prevent publication of the repair. Source snapshots remain unchanged.

The initial repair removes every exact-value plateau of five trading sessions
in the audited index window. This is an audit result, not a rule that all true
daily quotes must differ. Validation replays the field-specific price policy;
it does not reject legitimate equal dated official observations.

## DRAM October 14 reconstruction

No issuer-certified daily historical TTM series was found. The previous 13.2344
on October 13 was a September snapshot carried forward, not an observed ratio.
The user authorized reconstruction from real price and source data while keeping
the chart continuous.

The fixed interval uses recorded Stock Analysis snapshots: July 16 (32.38) and
October 14 (23.95), with their actual unadjusted ETF closes. The October 23
snapshot (22.48) and close corroborate the October 14 implied denominator within
0.1%. Snapshots have first-seen dates; their fund-valuation effective dates were
not independently certified.

For each endpoint compute implied earnings `E = price / PE`. For July 17 through
October 13 interpolate implied earnings in calendar time, then compute
`PE(day) = actual close(day) / interpolated E(day)`. This is a retrospective model,
not a recovered actual earnings statement. It uses a later endpoint, so metadata
and tooltips explicitly mark the use of hindsight. It must not be treated as
point-in-time information for an investment backtest.

October 13 becomes 25.3038; October 14 remains 23.95. The earlier and later
published endpoints, Forward PE and PB remain unchanged. Raw observations are
retained. Routine refreshes replay the same fixed interval and cannot shift its
endpoint forward or rebase it to each new current quote.

## Verification

Regression coverage includes weekly versus daily field dates, actual index
prices, equal genuine quotes, delayed source reconciliation, FY1 above TTM,
tooltip parsing, missing-price failures, immutable other fields, history locks,
recovery-ledger consistency, artifact cache propagation and idempotent retries.

All 100 company histories were also scanned and checked against their separate
Yahoo publication rules. Repeated provider fields were found for DHR, GILD,
INTC, PBR and SPCX. Those source-side repetitions are recorded separately from
the index forward-fill defect; company source values were not replaced with
invented observations or subjected to the index estimation policy.
