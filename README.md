# US Valuation Monitor

[![Daily Data Refresh](https://img.shields.io/github/actions/workflow/status/coattail/US-Valuation-Monitor/daily-data-refresh.yml?branch=main&label=Daily%20Data%20Refresh)](https://github.com/coattail/US-Valuation-Monitor/actions/workflows/daily-data-refresh.yml)
[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Live-blue)](https://coattail.github.io/US-Valuation-Monitor/)
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
- Core index TTM/forward P/E observations are reconciled with dated WSJ quotes after close and during overnight company runs, even when every trading date already exists. Run `npm run refresh:wsj:index` to retry only this synchronization. Source freshness and published-value equality are checked separately from trading-date coverage; see [the WSJ audit](./docs/wsj-reconciliation-2026-10-17.md).
- ETF TTM reconciliation retains the first sighting of repeated undated page snapshots, applies actual daily close returns between anchors, and replaces earlier estimates when a changed source snapshot arrives. All index refresh modes run this policy; tooltips distinguish dated quotes, undated snapshots and estimates. See [the ETF TTM audit](./docs/etf-ttm-daily-audit.md).
- Forward PE follows field-specific publication dates: weekly WSJ quotes use daily actual-index closes between observations; official ETF quotes replace delayed estimates. DRAM's disputed historical TTM interval uses a fixed, explicitly labeled retrospective reconstruction. See [the full daily update audit](./docs/index-daily-update-audit-2026-10-25.md).
- **Company latest valuation** is aligned to Yahoo trusted metrics when available.
- If a Yahoo daily snapshot has an untrusted source for `pe_forward`, that day is stored as `null` (not forced/interpolated).
- For symbols with source-regime transition (e.g., yesterday unavailable, latest day available), historical `forward PE` is rebased by a fixed factor using latest-day price movement to connect to Yahoo latest smoothly and transparently.

## Quick Start

### 1) Build dataset

```bash
git clone https://github.com/coattail/US-Valuation-Monitor.git
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

- Home: [https://coattail.github.io/US-Valuation-Monitor/](https://coattail.github.io/US-Valuation-Monitor/)
- Index board: [https://coattail.github.io/US-Valuation-Monitor/apps/web/index.html](https://coattail.github.io/US-Valuation-Monitor/apps/web/index.html)
- Company board: [https://coattail.github.io/US-Valuation-Monitor/apps/web/companies.html](https://coattail.github.io/US-Valuation-Monitor/apps/web/companies.html)

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

### 官方财报补录（2026-10-28）

`data/earnings/official-quarterly.json` 保存 Visa FY2026 Q4／全年及 SK 海力士 FY2026 Q3 官方财报。公司详情展示原币营收、营业利润、净利润、已披露稀释 EPS 和官方来源；Visa 为美元，SK 海力士为韩元。季度基本面与发布日期在构建、拆分后保留，后续自动刷新会再次应用官方记录。

SK 海力士美股代码为 SKHY，公司列表根据 `sk-hynix` 标识映射，避免韩股 `000660.KS` 被美股过滤规则排除。未取得可靠行情观察时，SKHY 仅发布财报，估值显示等待同步；不推算历史倍数，不把普通股韩元 EPS 当作 ADR 美元 EPS。新财报不会重写发布前的历史收盘估值。
