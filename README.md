# US Valuation Monitor

[中文文档](./README.zh-CN.md) | [English Documentation](./README.en.md)

US Valuation Monitor is a US equity valuation monitoring platform with:
- a production-oriented Web client
- a WeChat Mini Program client
- a lightweight API service
- a multi-source daily-refresh data pipeline

## Recent Updates

- Added company valuation board (Top 100) for both Web and Mini Program.
- Added company detail API endpoints: `/api/company/meta`, `/api/company/snapshot`, `/api/company/series`.
- Upgraded Mini Program UX: custom nav bar, custom tab bar, compact company cards, and dual-chart detail views.
- Enhanced Profile page with both index and company watchlist selectors (with independent scroll areas).
- Optimized company Web loading: lightweight snapshot first, company series loaded on demand.

## Quick Start

```bash
git clone https://github.com/Sunny-1991/us-valuation-monitor.git
cd us-valuation-monitor
npm run build:data
npm run start:web
```

Open Web:
- `http://127.0.0.1:9030/apps/web/`

Online Web (GitHub Pages):
- `https://sunny-1991.github.io/us-valuation-monitor/`
- Direct index board: `https://sunny-1991.github.io/us-valuation-monitor/apps/web/index.html`
- Direct company board: `https://sunny-1991.github.io/us-valuation-monitor/apps/web/companies.html`

Start API (optional):

```bash
npm run start:api
```

API base URL:
- `http://127.0.0.1:9040`

Mini Program project path:
- `apps/miniprogram`

## Full Documentation

- Chinese: [README.zh-CN.md](./README.zh-CN.md)
- English: [README.en.md](./README.en.md)
