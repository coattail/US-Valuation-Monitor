# US Valuation Monitor

[![Daily Data Refresh](https://img.shields.io/github/actions/workflow/status/Sunny-1991/us-valuation-monitor/daily-data-refresh.yml?branch=main&label=Daily%20Data%20Refresh)](https://github.com/Sunny-1991/us-valuation-monitor/actions/workflows/daily-data-refresh.yml)
[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Live-blue)](https://sunny-1991.github.io/us-valuation-monitor/)
[![Node](https://img.shields.io/badge/Node.js-v25-brightgreen)](https://nodejs.org/)

A production-oriented valuation monitoring project for:
- US major indices + S&P sector proxies
- Top 100 US-listed companies
- Web + WeChat Mini Program + lightweight API
- automated multi-source daily data refresh

**Docs:** [中文文档](./README.zh-CN.md) | [English Documentation](./README.en.md) | [Investor README](./README.investor.md)

## Highlights

- Daily valuation snapshot + long history series for both indices and companies.
- Unified metric stack: `pe_ttm`, `pe_forward`, `pb`, `earnings_yield`.
- Company board + detail APIs: `/api/company/meta`, `/api/company/snapshot`, `/api/company/series`.
- Runtime APIs for watchlist, alerts, auth, and manual refresh jobs.
- GitHub Actions pipeline that rebuilds and commits dataset changes automatically.

## Data Integrity Policy (Important)

- **Index valuation data** stays on its historical non-Yahoo source chain to avoid short-term regime noise.
- **Company latest valuation** is aligned to Yahoo trusted metrics when available.
- If a Yahoo daily snapshot has an untrusted source for `pe_forward`, that day is stored as `null` (not forced/interpolated).
- For symbols with source-regime transition (e.g., yesterday unavailable, latest day available), historical `forward PE` is rebased by a fixed factor using latest-day price movement to connect to Yahoo latest smoothly and transparently.

## Quick Start

### 1) Build dataset

```bash
git clone https://github.com/Sunny-1991/us-valuation-monitor.git
cd us-valuation-monitor
npm run build:data
```

### 2) Run Web

```bash
npm run start:web
```

Open: `http://127.0.0.1:9030/apps/web/`

### 3) Run API (optional)

```bash
npm run start:api
```

Base URL: `http://127.0.0.1:9040`

## Online Preview

- Home: [https://sunny-1991.github.io/us-valuation-monitor/](https://sunny-1991.github.io/us-valuation-monitor/)
- Index board: [https://sunny-1991.github.io/us-valuation-monitor/apps/web/index.html](https://sunny-1991.github.io/us-valuation-monitor/apps/web/index.html)
- Company board: [https://sunny-1991.github.io/us-valuation-monitor/apps/web/companies.html](https://sunny-1991.github.io/us-valuation-monitor/apps/web/companies.html)

## Repository Structure

```text
us-valuation-monitor/
├─ apps/
│  ├─ web/                 # Web client
│  └─ miniprogram/         # WeChat Mini Program
├─ cloudfunctions/         # API server
├─ packages/
│  ├─ core/                # shared metrics/stat rules/types
│  └─ data-pipeline/       # data fetch/merge/normalize/build
├─ data/
│  ├─ bootstrap/           # curated bootstrap CSV
│  ├─ runtime/             # watchlist/alert runtime JSON
│  └─ standardized/        # generated datasets
└─ .github/workflows/
   └─ daily-data-refresh.yml
```

## API Quick Reference

- `GET /healthz`
- `GET /api/meta`
- `GET /api/snapshot`, `GET /api/series`, `GET /api/heatmap`
- `GET /api/company/meta`, `GET /api/company/snapshot`, `GET /api/company/series`
- `GET/POST /api/watchlist`
- `GET /api/alerts`, `POST /api/alerts/ack`
- `POST /api/jobs/daily-update`
- `POST /api/jobs/company-refresh`
- `POST /api/auth/dev-login`, `POST /api/auth/wechat-login`

## Notes

- Recommended runtime: Node.js `v25` (same as CI workflow).
- Mini Program project path: `apps/miniprogram`.
- For detailed setup, troubleshooting, and full endpoint docs, see:
  - [README.zh-CN.md](./README.zh-CN.md)
  - [README.en.md](./README.en.md)
