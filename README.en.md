# US Valuation Monitor

US Valuation Monitor is a valuation platform for major US indices, S&P 500 sectors, and Top 100 US companies. It combines a production-oriented Web interface, a WeChat Mini Program client, a lightweight API service, and a daily-refresh data pipeline.

The project is built to be practical in real operations:
- daily-refresh capable (GitHub Actions)
- robust fallback behavior when one source degrades
- unified standardized dataset for both Web and API consumers

## 1) What You Get

### Product capabilities
- Cross-index valuation monitoring for 17 US market proxies (core indices + 11 sectors)
- Multi-metric coverage: `pe_ttm`, `pe_forward`, `pb`, `earnings_yield`
- Historical context: full-history / 10Y / 5Y percentiles, valuation regime, and z-score
- Comparison workflow for key indices (default: S&P 500, Nasdaq 100, Dow 30)
- Company valuation board (Top 100) with company detail pages (valuation series + percentile sub-chart)
- Mini Program support for both index and company boards, detail charts, alerts, and settings
- Watchlist + alert-state persistence via API runtime store (index); company watchlist persisted locally in Mini Program

### Technical capabilities
- One-command dataset build from source adapters and normalization logic
- Shared core package for metric/statistical consistency across clients
- API endpoints for metadata, snapshots, time series, heatmap, watchlist, alerts, company valuation, and daily job triggering
- Scheduled daily data refresh with automatic commit-and-push on data change
- Mini Program UX upgrades including custom navigation, custom tab bar, and lifecycle-safe async rendering

## 2) Repository Layout

```text
us-valuation-monitor/
├─ apps/
│  ├─ web/                         # Web client (HTML/CSS/JS)
│  └─ miniprogram/                 # WeChat Mini Program (boards + detail + alerts + settings)
├─ cloudfunctions/                 # Node HTTP API server
│  ├─ server.ts                    # entry point (HOST/PORT)
│  └─ src/app.ts                   # routes + runtime stores
├─ packages/
│  ├─ core/                        # shared types, metrics, percentiles, rules
│  └─ data-pipeline/               # fetch/merge/normalize/build dataset
├─ data/
│  ├─ bootstrap/                   # curated bootstrap datasets (CSV)
│  ├─ runtime/                     # watchlist / alert runtime JSON files
│  └─ standardized/
│     └─ valuation-history.json    # main generated dataset
├─ .github/workflows/
│  └─ daily-data-refresh.yml       # scheduled refresh workflow
└─ package.json
```

## 3) Data Strategy and Quality Controls

This project follows a **multi-source merge with reliability guardrails** strategy.

### Main source categories
- Price and market proxy series: Stooq
- US 10Y yield: FRED (`DGS10`)
- Valuation history supplements: MacroMicro, Trendonify, and other public references used by adapters

### Important implementation notes
- S&P 500 forward PE includes a pinned bootstrap series from MacroMicro in `data/bootstrap/sp500-forward-pe-macromicro.csv`.
- Forward PE availability is tracked per index via `forwardStartDate` and enforced in API responses.
- Latest index snapshots include an anti-spike deviation guard to reduce one-day source-regime jumps.
- Company `PE(FWD)` latest values prefer Yahoo trusted sources; when a daily Yahoo snapshot is untrusted for `pe_forward`, that day is stored as `null` (no manual fill).
- For source-regime transitions (previous trading day unavailable, latest day available), historical `forward PE` is rebased using a fixed factor that includes the latest-day price move:
  - factor = `latestYahooForward / (previousForward × latestClose/previousClose)`
  - this keeps historical forward PE and latest Yahoo forward PE connected under the same basis.
- TTM PE reconstruction between sparse anchors is **close-aware**, not pure long-span linear interpolation:
  - within valid anchor ranges, daily valuation path is reconstructed against actual trading-day close movements
  - this preserves realistic day-to-day fluctuation instead of producing unnaturally smooth declines

## 4) Prerequisites

- Git
- Node.js (recommended: **v25**, aligned with CI workflow)
- Python 3 (used by `npm run start:web` to serve static files)

## 5) Quick Start

```bash
git clone https://github.com/coattail/US-Valuation-Monitor.git
cd us-valuation-monitor
npm run build:data
npm run start:web
```

Open Web app:
- `http://127.0.0.1:9030/apps/web/`

GitHub-hosted Web (GitHub Pages):
- `https://coattail.github.io/US-Valuation-Monitor/`
- Direct index board: `https://coattail.github.io/US-Valuation-Monitor/apps/web/index.html`
- Direct company board: `https://coattail.github.io/US-Valuation-Monitor/apps/web/companies.html`

Optional API server:

```bash
npm run start:api
```

Default API base URL:
- `http://127.0.0.1:9040`

## 5.1) Run the WeChat Mini Program Locally

1. Start API service (recommended):

```bash
npm run start:api
```

2. Open WeChat DevTools and import:
   - `apps/miniprogram`
3. In the app, go to `Profile -> Connection Settings` to verify/test API base (default `http://localhost:9040`).

Notes:
- Index watchlist is persisted via backend `/api/watchlist`.
- Company watchlist is currently persisted in Mini Program local storage (`usvm-company-watchlist`).

## 6) Script Reference

| Command | Purpose |
| --- | --- |
| `npm run build:data` | Fetch/merge/normalize data and write `data/standardized/valuation-history.json` |
| `npm run build:site` | Assemble a Cloudflare Pages / GitHub Pages static site into `.pages/` |
| `npm run start:web` | Start static web server at port `9030` |
| `npm run start:api` | Start API server (default `127.0.0.1:9040`) |
| `npm test` | Run core + API test suites |

## 6.1) Deploy to Cloudflare Pages

The web app is a static site. On Cloudflare, deploy it with **Cloudflare Pages** instead of running `npx wrangler deploy` from the workspace root.

Recommended settings:

| Setting | Value |
| --- | --- |
| Framework preset | `None` |
| Root directory | repository root |
| Build command | `npm run build:site` |
| Build output directory | `.pages` |

Notes:
- The repository root is an npm workspace, so a root-level `wrangler deploy` can fail with workspace application detection errors.
- `npm run build:site` copies the required files from `apps/web` and `data/standardized` into `.pages/`, which Cloudflare can publish directly.
- The deployed Cloudflare/GitHub Pages site is static, so the in-app "hot refresh data" action is intentionally unavailable unless you deploy `cloudfunctions` separately.

## 6.2) Automatic updates on Cloudflare

If your Cloudflare Pages project is connected to this repository through the **GitHub integration**, the repository already has the main pieces needed for automatic updates:

1. GitHub Actions runs [`daily-data-refresh.yml`](./.github/workflows/daily-data-refresh.yml) on a schedule and rebuilds the latest datasets.
2. When files under `data/standardized` change, the workflow commits and pushes them to the default branch.
3. Cloudflare Pages detects the new commit on the production branch and automatically rebuilds and deploys the site.

It is worth checking two settings in Cloudflare:
- Production branch should be `main` (or your actual release branch).
- Build watch paths should include only site-related paths, so unrelated changes in the repo do not trigger extra builds.

Suggested include paths:
- `apps/web/*`
- `data/standardized/*`
- `scripts/build-static-site.mjs`
- `package.json`
- `.github/workflows/daily-data-refresh.yml`

If you are not using GitHub integration and deploy manually instead, create a Pages Deploy Hook and call it from GitHub Actions after the scheduled refresh finishes.

## 7) API Reference

Base URL: `http://127.0.0.1:9040`

### Health and metadata
- `GET /healthz` — liveness check
- `GET /api/meta` — dataset meta, indices, ranges, and `forwardStartDate`

### Snapshot and series
- `GET /api/snapshot?group=core|sector|all`
- `GET /api/series?indexId=<id>&metric=pe_ttm|pe_forward|pb|earnings_yield&from=YYYY-MM-DD&to=YYYY-MM-DD`
- `GET /api/heatmap?group=core|sector|all`

### Company valuation (Top 100)
- `GET /api/company/meta`
- `GET /api/company/snapshot`
- `GET /api/company/series?indexId=<id>&metric=pe_ttm|pe_forward|pb|earnings_yield&from=YYYY-MM-DD&to=YYYY-MM-DD`

### Watchlist and alerts
- `GET /api/watchlist`
- `POST /api/watchlist`
- `GET /api/alerts`
- `POST /api/alerts/ack`

### Jobs and auth
- `POST /api/jobs/daily-update` — trigger one full refresh in API runtime
- `POST /api/jobs/company-refresh` — trigger company rebuild (optionally with symbol filter)
- `POST /api/auth/dev-login` — returns a development token
- `POST /api/auth/wechat-login` — currently returns `501` placeholder until Mini Program AppID integration

For user-scoped endpoints, pass header:
- `X-Dev-Token: dev-token:<userId>`

## 8) Daily Auto Refresh (GitHub Actions)

Workflow file:
- `.github/workflows/daily-data-refresh.yml`

Current behavior:
- Runs on schedule (`cron: 30 21 * * 1-5`, 21:30 UTC on US trading weekdays, after market close year-round) and manual dispatch
- Executes `npm run build:data`
- Index valuation keeps the historical non-Yahoo source chain to avoid short-term source-regime shocks
- Company valuation series are capped to Yahoo's latest available trading day and appended into history
- Commits and pushes standardized dataset outputs **only when changed** (including company snapshot and split series files)

To trigger manually:
1. Open GitHub repository → **Actions**
2. Select **Daily Data Refresh**
3. Click **Run workflow**

## 9) Dataset and Runtime Files

- Main dataset: `data/standardized/valuation-history.json`
- Lightweight index snapshot (for Web first paint): `data/standardized/valuation-snapshot.json`
- Split index series (loaded on demand): `data/standardized/index-series/<index_id>.json`
- Local company intermediate build file (not committed): `data/standardized/company-valuation-history.json`
- Lightweight company snapshot (for Web first paint): `data/standardized/company-valuation-snapshot.json`
- Split company series (loaded on demand): `data/standardized/company-series/<company_id>.json`
- Runtime watchlists: `data/runtime/watchlists.json`
- Runtime alerts: `data/runtime/alerts.json`
- Alert states: `data/runtime/alert-state.json`
- S&P 500 forward PE bootstrap: `data/bootstrap/sp500-forward-pe-macromicro.csv`

## 10) Testing and Validation

Run tests:

```bash
npm test
```

Recommended smoke checks after rebuilding data:

```bash
curl -sS http://127.0.0.1:9040/healthz
curl -sS http://127.0.0.1:9040/api/meta
curl -sS "http://127.0.0.1:9040/api/series?indexId=sp500&metric=pe_ttm"
```

## 11) Troubleshooting

- `ERR_UNKNOWN_FILE_EXTENSION .ts`:
  - use a newer Node.js version (recommended v25)
- Web loads but no fresh data:
  - run `npm run build:data` again and verify `data/standardized/valuation-history.json`
- API returns `Invalid indexId` / `Invalid metric`:
  - check request parameters against supported index IDs and metrics
- Unexpectedly flat valuation segments:
  - verify source anchor coverage and rebuild dataset; the pipeline uses close-aware reconstruction inside valid ranges
- Company `PE(TTM)` / `PE(FWD)` differs from Yahoo:
  - latest override now prioritizes Yahoo timeseries (`trailingPeRatio` / `forwardPeRatio`) plus quote API to stay close to Yahoo Valuation Measures
  - check whether `data/standardized/company-yahoo-daily-metrics.json` is being appended (use `yahoo-market-latest-date-*` and `yahoo-latest-override-*` source tags to verify date alignment and coverage)
  - when a source transition happens (missing previous day, available latest day), the pipeline rebases historical `PE(FWD)` by a price-adjusted fixed factor to connect to Yahoo latest value
  - latest Yahoo override is enabled for all symbols by default (`YAHOO_LATEST_OVERRIDE_SYMBOLS=*`); exclude special cases with `YAHOO_LATEST_OVERRIDE_EXCLUDE_SYMBOLS=SYM1,SYM2`
  - Yahoo is often blocked from mainland China; run `npm run build:data:company` in a Yahoo-reachable environment (for example GitHub Actions)
- Index/company latest date is behind:
  - index pipeline intentionally does not use Yahoo; recency follows the index source chain availability
  - for company pipeline, use `yahoo-market-latest-date-*` and `yahoo-latest-override-*` tags to verify alignment

## 12) Roadmap

- Continue hardening historical valuation coverage for all tracked indices
- Continue improving Mini Program/Web visual consistency and interaction details
- Add backend persistence + cross-device sync for company watchlists
- Add richer alert center and operational monitoring
