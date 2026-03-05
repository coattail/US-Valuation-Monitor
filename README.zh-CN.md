# US Valuation Monitor（美国股市估值监测）

US Valuation Monitor 是一个面向美股主要指数、标普行业板块与 Top100 企业的估值监测平台，包含 Web 展示端、微信小程序端、轻量 API 服务和可日更的数据管线。

项目强调“可长期运行”的工程化能力：
- 可自动日更（GitHub Actions）
- 多数据源融合与降级回退
- 一份标准化数据同时供 Web/API 使用

## 1）你将获得什么

### 功能能力
- 覆盖 17 个美股监测对象（核心指数 + 标普 11 行业代理）
- 多指标跟踪：`pe_ttm`、`pe_forward`、`pb`、`earnings_yield`
- 历史分位和状态识别：全历史 / 10年 / 5年分位、估值区间、Z-Score
- 多指数对比分析（默认：标普500、纳指100、道琼斯30）
- 企业估值看板（Top 100）与公司详情页（时序估值 + 分位子图）
- 微信小程序端支持指数与企业双看板、详情图表、提醒中心、配置中心
- 自选与提醒状态后端持久化（指数），企业自选支持小程序本地持久化

### 技术能力
- 一条命令构建标准化快照数据
- `packages/core` 统一指标与统计口径，前后端一致
- API 提供元信息、快照、时序、热力、自选、提醒、企业估值接口、日更触发接口
- GitHub Actions 每日自动刷新并在数据变更时自动提交
- 小程序端引入自定义导航栏 / 自定义 TabBar、生命周期防护（避免页面销毁时异步渲染报错）

## 2）仓库结构

```text
us-valuation-monitor/
├─ apps/
│  ├─ web/                         # Web 页面（HTML/CSS/JS）
│  └─ miniprogram/                 # 微信小程序（指数/企业看板 + 详情 + 提醒 + 配置）
├─ cloudfunctions/                 # Node HTTP API 服务
│  ├─ server.ts                    # 服务入口（HOST/PORT）
│  └─ src/app.ts                   # 路由与运行时存储
├─ packages/
│  ├─ core/                        # types、指标计算、统计规则
│  └─ data-pipeline/               # 抓取、融合、清洗、快照生成
├─ data/
│  ├─ bootstrap/                   # 人工校验过的引导数据（CSV）
│  ├─ runtime/                     # 自选/提醒运行时 JSON
│  └─ standardized/
│     └─ valuation-history.json    # 主输出数据文件
├─ .github/workflows/
│  └─ daily-data-refresh.yml       # 每日自动更新工作流
└─ package.json
```

## 3）数据策略与质量控制

项目采用“**多源融合 + 可靠性约束**”的数据策略。

### 主要来源类别
- 指数/ETF 价格代理：Stooq
- 美国 10Y 国债收益率：FRED（`DGS10`）
- 估值历史补充：MacroMicro、Trendonify 及其他公开可用来源

### 关键实现说明
- 标普500前瞻市盈率（Forward PE）包含 MacroMicro 引导序列，文件为 `data/bootstrap/sp500-forward-pe-macromicro.csv`。
- 每个指数的前瞻估值起始可用日由 `forwardStartDate` 标记，API 会在查询时严格处理。
- 指数最新快照值有“防跳变阈值”校验，避免单日源口径切换造成异常尖刺。
- TTM PE 在锚点区间内不是“长区间纯线性插值”，而是**结合交易日收盘路径重建**：
  - 在有效锚点之间按每日价格波动推导估值路径
  - 避免出现不符合市场节奏的“过度平滑下滑线条”

## 4）环境要求

- Git
- Node.js（建议 **v25**，与 CI 保持一致）
- Python 3（`npm run start:web` 通过 Python 启动静态服务）

## 5）快速开始

```bash
git clone https://github.com/Sunny-1991/us-valuation-monitor.git
cd us-valuation-monitor
npm run build:data
npm run start:web
```

打开 Web：
- `http://127.0.0.1:9030/apps/web/`

GitHub 线上访问（GitHub Pages）：
- `https://sunny-1991.github.io/us-valuation-monitor/`
- 指数估值页：`https://sunny-1991.github.io/us-valuation-monitor/apps/web/index.html`
- 企业估值页：`https://sunny-1991.github.io/us-valuation-monitor/apps/web/companies.html`

可选启动 API：

```bash
npm run start:api
```

默认 API 地址：
- `http://127.0.0.1:9040`

## 5.1）微信小程序本地运行

1. 启动 API 服务（建议）：

```bash
npm run start:api
```

2. 打开微信开发者工具，导入目录：
   - `apps/miniprogram`
3. 首次进入小程序后，可在「我的 -> 连接设置」检查/测试 API 地址（默认 `http://localhost:9040`）。

说明：
- 指数自选由后端 `/api/watchlist` 持久化。
- 企业自选当前由小程序本地缓存（`usvm-company-watchlist`）持久化。

## 6）脚本说明

| 命令 | 用途 |
| --- | --- |
| `npm run build:data` | 抓取并融合数据，输出 `data/standardized/valuation-history.json` |
| `npm run start:web` | 启动 9030 端口静态服务 |
| `npm run start:api` | 启动 API（默认 `127.0.0.1:9040`） |
| `npm test` | 运行 core + API 测试 |

## 7）API 接口

基地址：`http://127.0.0.1:9040`

### 健康检查与元信息
- `GET /healthz`：服务存活检查
- `GET /api/meta`：数据元信息、指数列表、可用区间、`forwardStartDate`

### 快照与时序
- `GET /api/snapshot?group=core|sector|all`
- `GET /api/series?indexId=<id>&metric=pe_ttm|pe_forward|pb|earnings_yield&from=YYYY-MM-DD&to=YYYY-MM-DD`
- `GET /api/heatmap?group=core|sector|all`

### 企业估值（Top100）
- `GET /api/company/meta`
- `GET /api/company/snapshot`
- `GET /api/company/series?indexId=<id>&metric=pe_ttm|pe_forward|pb|earnings_yield&from=YYYY-MM-DD&to=YYYY-MM-DD`

### 自选与提醒
- `GET /api/watchlist`
- `POST /api/watchlist`
- `GET /api/alerts`
- `POST /api/alerts/ack`

### 任务与认证
- `POST /api/jobs/daily-update`：手动触发一次完整日更
- `POST /api/auth/dev-login`：获取开发态 token
- `POST /api/auth/wechat-login`：当前返回 `501`（待小程序 AppID 打通）

用户维度接口可携带请求头：
- `X-Dev-Token: dev-token:<userId>`

## 8）每日自动更新（GitHub Actions）

工作流文件：
- `.github/workflows/daily-data-refresh.yml`

当前策略：
- 定时触发（`cron: 30 6 * * *`，美股收盘后）+ 手动触发
- 执行 `npm run build:data`
- 指数估值沿用既有历史同源链路（不引入 Yahoo，降低短期口径波动）
- 企业估值时序按 Yahoo 可用最新交易日截断并同步写入历史
- 仅在标准化数据文件有变化时提交并推送（含企业快照与分公司时序文件）

手动运行步骤：
1. 打开仓库的 **Actions**
2. 选择 **Daily Data Refresh**
3. 点击 **Run workflow**

## 9）数据文件与运行时文件

- 标准化主数据：`data/standardized/valuation-history.json`
- 指数轻量快照（Web 首屏使用）：`data/standardized/valuation-snapshot.json`
- 指数分指数时序（按需加载）：`data/standardized/index-series/<index_id>.json`
- 企业全量历史：`data/standardized/company-valuation-history.json`
- 企业轻量快照（Web 首屏使用）：`data/standardized/company-valuation-snapshot.json`
- 企业分公司时序（按需加载）：`data/standardized/company-series/<company_id>.json`
- 自选存储：`data/runtime/watchlists.json`
- 提醒存储：`data/runtime/alerts.json`
- 提醒状态：`data/runtime/alert-state.json`
- 标普500前瞻 PE 引导数据：`data/bootstrap/sp500-forward-pe-macromicro.csv`

## 10）测试与验证

运行测试：

```bash
npm test
```

建议在重建数据后做一次接口冒烟：

```bash
curl -sS http://127.0.0.1:9040/healthz
curl -sS http://127.0.0.1:9040/api/meta
curl -sS "http://127.0.0.1:9040/api/series?indexId=sp500&metric=pe_ttm"
```

## 11）常见问题排查

- 出现 `ERR_UNKNOWN_FILE_EXTENSION .ts`：
  - 升级 Node.js（建议 v25）
- Web 可打开但数据不是最新：
  - 重新执行 `npm run build:data`，检查 `data/standardized/valuation-history.json` 更新时间
- API 返回 `Invalid indexId` / `Invalid metric`：
  - 核对参数是否在支持列表中
- 某段估值看起来异常平滑：
  - 检查该区间锚点覆盖和源数据可用性；当前管线会在有效锚点区间使用收盘路径进行重建
- 企业卡片 `PE(TTM)` / `PE(FWD)` 与 Yahoo 页面不一致：
  - 最新覆盖优先使用 Yahoo timeseries（`trailingPeRatio` / `forwardPeRatio`）+ quote API，尽量对齐 Yahoo Valuation Measures 口径
  - 检查 `data/standardized/company-yahoo-daily-metrics.json` 是否持续有数据写入（可结合 `yahoo-market-latest-date-*` 与 `yahoo-latest-override-*` 源标签核对日期和覆盖率）
  - 默认对全部公司启用 Yahoo 最新值覆盖（`YAHOO_LATEST_OVERRIDE_SYMBOLS=*`）；如个别标的口径特殊，可用 `YAHOO_LATEST_OVERRIDE_EXCLUDE_SYMBOLS=SYM1,SYM2` 排除
  - 中国大陆网络通常会被 Yahoo 拒绝页拦截，建议在可访问 Yahoo 的环境（如 GitHub Actions）执行 `npm run build:data:company`
- 指数/企业最新日期落后：
  - 指数链路默认不使用 Yahoo，日期取决于指数既有数据源的可用日
  - 企业链路可结合 `yahoo-market-latest-date-*` 与 `yahoo-latest-override-*` 检查是否已对齐到 Yahoo 最新交易日

## 12）后续方向

- 继续增强各指数长期估值历史覆盖质量
- 继续提升小程序与 Web 的视觉一致性与交互细节（图表、筛选、配置体验）
- 推进企业自选服务端持久化与多端同步
- 补强提醒中心与运行监控能力
