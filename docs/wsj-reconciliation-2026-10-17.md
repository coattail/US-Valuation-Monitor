# WSJ 指数估值核查：2026-10-17

对照来源：https://www.wsj.com/market-data/stocks/peyields 。源表日期为美国交易日 **2026-10-16**，不是北京时间 10 月 17 日。TTM 与 Estimate 两列分别代表过去 12 个月和未来 12 个月；不可取 Year ago 或股息率列。

| 指数 | 原 TTM | WSJ TTM | 原 Forward | WSJ Forward |
|---|---:|---:|---:|---:|
| S&P 500 | 23.1209 | 23.51 | 19.175085 | 19.25 |
| Nasdaq 100 | 29.770609 | 30.27 | 20.9339 | 21.45 |
| Dow Jones Industrial Average | 20.896269 | 20.87 | 19.501857 | 19.78 |
| Russell 2000 | 33.574729 | 34.62 | 25.120425 | 26.32 |

## 原因与证据

- 仓库 `4dd595a` 的指数数据已经有 10 月 16 日记录，但四个指数最新行带有 `recovery.method=observed-close-ratio-with-previous-published-fundamentals`。标普 TTM 和纳指 Forward 的专用估算字段仍使用 10 月 9 日 WSJ 锚点。
- `index-yahoo-daily-metrics.json` 中，SPY、QQQ、IWM 的最新 WSJ 行为 10 月 9 日，DIA 为 10 月 15 日，没有本次源表更新。
- `refresh-market-close.mjs` 按缺失交易日筛选任务，日期齐全后返回；它并不抓取 WSJ。UTC 22:17 和 01:17 的夜间任务此前只运行公司更新，完整指数源同步要等到 UTC 05:17/09:17。
- 多入口 WSJ 抓取此前直接按遍历顺序覆盖，旧镜像有覆盖新结果的风险。此次未将该隐患认定为当天差异的已证实直接原因。
- 原始表格解析先过滤数值再定位列，缺失或异常的 Year ago 单元格可能改变列偏移，属于同时修复的解析隐患。

## 修复机制

1. 每次指数收盘刷新之后独立同步 WSJ；夜间公司模式也安排独立的指数同步任务，跳过昂贵的完整指数历史重建。
2. 首先请求 WSJ 当前正式地址；按每张表自己的日期解析，跨入口按日期选最新值，同日保留优先来源。拒绝无日期和未来日期的结果。
3. 固定 TTM/Year ago/Forward 列位置，缺失值不能导致股息率或其他列移入 Forward。
4. 已发布交易日允许按同日 WSJ 直接观测修正 TTM/Forward，清除对应专用估算标记。P/B、国债收益率、日期和更早历史保持不变；SPX TTM / NDX Forward 的后续估算按新锚点重算。
5. 同步更新 WSJ 缓存、历史锁、缺口修复台账，再重新生成快照、分位和拆分序列。后续常规刷新可从缓存继续使用新观测。
6. 独立检查 WSJ 源日期及源表行与已发布值的一致性。Dow 按日检查，其他三个按最近周五检查，休市周五退回前一交易日，允许源站 12 小时更新窗口。
7. 抓取失败保留已有有效数据；工作流先上传有效进展，再将源同步失败/过期/不一致报告为失败，由现有后续计划重试。

## 验证与操作

```sh
npm run refresh:wsj:index
node scripts/sync-wsj-valuations.mjs --check
node --test scripts/sync-wsj-valuations.test.mjs scripts/refresh-market-close.test.mjs packages/data-pipeline/tests/generate-wsj.test.ts
npm test
```

测试包含真实表格布局、各表日期不同、旧镜像覆盖、未来数据、缺失 Year ago/Forward、已齐全日期的延迟修正、历史与其他字段保护、专用价格估算续接、重试幂等性、失败时文件不变及历史锁一致性。

本次执行：265 项全量测试中 264 项通过；唯一失败为既存 `PBR.png` 公司 Logo 缺失（`origin/main` 同样没有该文件），不属于指数估值修改。21 条指数序列的交易日完整性、WSJ 一致性、静态站构建均通过；逐点比较确认仅四个核心指数的 10 月 16 日 TTM/Forward 及相应专用估算标记发生变化。
