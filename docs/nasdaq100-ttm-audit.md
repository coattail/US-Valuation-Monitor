# Nasdaq-100 TTM valuation audit

Audit date: 2026-08-27

## Finding

The published Nasdaq-100 `pe_ttm` history mixed incompatible source definitions and then converted sparse annual observations into daily values by assuming a smoothly changing earnings denominator. The interpolation looked precise but was materially wrong in periods when aggregate earnings changed quickly.

Four issues were confirmed:

1. The 1999 year-end 104x and 2000 year-end 113x anchors had no traceable primary-source support. An archived Nasdaq-provided table instead described a positive-earners-only series whose month-end peak was 89.5x in February 2000 and whose January 2001 value was about 45x. That definition is not comparable with Nasdaq's later aggregate headline P/E series, whose 2001 year-end value was 208.3x.
2. From 2001 through 2022, one observation per year was used to infer every trading day. This suppressed real earnings changes. Against Nasdaq's later monthly headline P/E chart, the old series understated September-November 2009 by about 22%.
3. The 2023-2026 bridge used Siblis observations even though Nasdaq/Bloomberg's headline TTM series was available. The old series understated May-July 2023 by roughly 16-18% and many 2024-2025 month ends by 8-13%.
4. The append-only history lock correctly prevented routine drift, but it also made earlier bad observations permanent unless an explicit audited rewrite was performed.
5. A final delayed-snapshot pass ran after the historical overlay. Ten WSJ TTM observations inside the official 2026 monthly range could therefore punch provider-definition holes back into the corrected curve on the next refresh.
6. An attempted continuity bridge incorrectly multiplied every post-June WSJ value by `38.17 / 35.49`. That changed traceable WSJ observations—for example 2026-08-21 from the published 34.12x to 36.6965x—and violated the declared source hierarchy.
7. The detail-series request used an unchanged dataset timestamp as its browser cache key and did not disable HTTP caching. A repaired snapshot could therefore coexist with an old chart curve after a normal page refresh.

## Corrected source hierarchy

- Before 2001-12-31: unavailable (`null`). The project no longer joins the incompatible positive-earners-only series to headline aggregate P/E.
- 2001-12-31 through 2005-12-31: annual Nasdaq/FactSet/Bloomberg headline P/E observations from Nasdaq's long-run valuation presentation, with NDX closes used only between those sparse verified points.
- 2006-01-31 through 2026-06-30: 246 monthly headline TTM P/E observations from Nasdaq's current NDX extended presentation, sourced to Nasdaq and Bloomberg.
- After 2026-06-30: dated WSJ public P/E observations on their original published basis, with the existing short price-carry logic between observations. The source transition is visible rather than altering either provider's values to manufacture continuity.

The audited monthly observations are committed in `data/bootstrap/nasdaq100-ttm-nasdaq-bloomberg-monthly.csv`. The build now fails closed if the file does not contain exactly 246 observations with the expected boundaries.

## Extraction and reproducibility

Primary PDF:

`https://indexes.nasdaq.com/docs/NDX%20Extended%20Presentation.pdf`

- PDF creation date: 2026-08-05
- Data as of: 2026-06-30
- Chart: PDF page 24 / slide 23, "What Powers NDX Returns? Earnings."
- Source label: Nasdaq, Bloomberg; headline trailing P/E
- Downloaded PDF SHA-256: `fc9af9db6745dd5f66bf89e003fa59f7881481f9cc5f033e31d0f4657b4cb711`
- The yellow P/E vector contains 246 monthly vertices, January 2006 through June 2026. Values were recovered from the right-hand 0-40x axis and rounded to two decimals.

Annual-prefix PDF:

`https://indexes.nasdaq.com/docs/NDX_Extended%20Presentation_December%202022.pdf`

Archived methodology warning:

`https://www.purebytes.com/archives/realtraders/2001/msg01014.html`

## Regression checks

The tests now verify that:

- 1999-2000 TTM P/E remains unavailable rather than mixing definitions;
- official monthly anchors override annual interpolation;
- the monthly bootstrap has the exact expected point count and date boundaries;
- post-June WSJ anchors remain exactly equal to their published values, including 34.12x on 2026-08-21;
- delayed provider snapshots cannot overwrite TTM inside the Nasdaq/Bloomberg coverage period, while their other metrics remain usable;
- the existing history-lock and explicit-rewrite protections still work.
