const DATA_PATH_CANDIDATES = [
  "/data/standardized/valuation-history.json",
  "../../data/standardized/valuation-history.json",
  "./valuation-history.json",
];

const STORAGE_KEYS = {
  watchlist: "usvm-watchlist",
  overviewGroup: "usvm-overview-group",
  compareRange: "usvm-compare-range",
  compareMetric: "usvm-compare-metric",
  compareStartDate: "usvm-compare-start-date",
  compareEndDate: "usvm-compare-end-date",
};

const METRIC_CONFIG = {
  pe_ttm: { label: "PE (TTM)", digits: 2 },
  pe_forward: { label: "PE (Forward)", digits: 2 },
  pb: { label: "PB", digits: 2 },
  earnings_yield: { label: "Earnings Yield", digits: 2, percentage: true },
};

const COMPARE_LINE_COLORS = [
  "#6b90ff",
  "#95db70",
  "#ffd35a",
  "#ff7478",
  "#66d6ff",
  "#bc9cff",
  "#5fd2b8",
  "#f5a5c8",
];

const CORE_ATTENTION_ORDER = ["sp500", "nasdaq100", "dow30", "russell2000", "us_total_market", "sp400"];
const SECTOR_ATTENTION_ORDER = [
  "sector_technology",
  "sector_financials",
  "sector_healthcare",
  "sector_energy",
  "sector_consumer_discretionary",
  "sector_communication",
  "sector_industrials",
  "sector_consumer_staples",
  "sector_utilities",
  "sector_real_estate",
  "sector_materials",
];
const DEFAULT_COMPARE_INDEX_IDS = ["sp500", "dow30", "nasdaq100"];

const TRADING_DAYS_PER_YEAR = 252;

const state = {
  dataset: null,
  metaRows: [],
  snapshotRows: [],
  watchlist: safeJsonParse(localStorage.getItem(STORAGE_KEYS.watchlist), []),

  overview: {
    group: localStorage.getItem(STORAGE_KEYS.overviewGroup) || "all",
    search: "",
    sort: "attention",
  },

  detail: {
    indexId: "",
    metric: "pe_ttm",
    range: "max",
  },

  compare: {
    metric: localStorage.getItem(STORAGE_KEYS.compareMetric) || "pe_ttm",
    range: localStorage.getItem(STORAGE_KEYS.compareRange) || "max",
    startDate: localStorage.getItem(STORAGE_KEYS.compareStartDate) || "",
    endDate: localStorage.getItem(STORAGE_KEYS.compareEndDate) || "",
    indexIds: [],
    watchlistOnly: false,
  },

  settings: {
    defaultGroup: localStorage.getItem(STORAGE_KEYS.overviewGroup) || "all",
    defaultCompareRange: localStorage.getItem(STORAGE_KEYS.compareRange) || "max",
  },

  caches: {
    metricSeries: new Map(),
  },
};

const elements = {
  dataModeChip: document.getElementById("data-mode-chip"),
  updatedChip: document.getElementById("updated-chip"),
  enterCompanyBoardBtn: document.getElementById("enter-company-board-btn"),
  tabButtons: [...document.querySelectorAll(".tab")],
  viewPanels: [...document.querySelectorAll(".view-panel")],

  snapshotDate: document.getElementById("snapshot-date"),
  overviewGroupFilter: document.getElementById("overview-group-filter"),
  overviewSortSelect: document.getElementById("overview-sort-select"),
  overviewSearch: document.getElementById("overview-search"),
  snapshotGrid: document.getElementById("snapshot-grid"),

  detailIndex: document.getElementById("detail-index"),
  detailMetric: document.getElementById("detail-metric"),
  detailRefresh: document.getElementById("detail-refresh"),
  detailRange: document.getElementById("detail-range"),
  detailRangeChips: [...document.querySelectorAll("#detail-range-chips .chip")],
  detailStats: document.getElementById("detail-stats"),
  detailPercentileTrack: document.getElementById("detail-percentile-track"),
  detailChart: document.getElementById("detail-chart"),
  detailPercentileChart: document.getElementById("detail-percentile-chart"),

  compareMetric: document.getElementById("compare-metric"),
  compareRange: document.getElementById("compare-range"),
  compareStartDate: document.getElementById("compare-start-date"),
  compareEndDate: document.getElementById("compare-end-date"),
  compareResetDate: document.getElementById("compare-reset-date"),
  compareWatchlistOnly: document.getElementById("compare-watchlist-only"),
  compareApply: document.getElementById("compare-apply"),
  compareIndexPicker: document.getElementById("compare-index-picker"),
  compareSummary: document.getElementById("compare-summary"),
  compareChart: document.getElementById("compare-chart"),
  compareChartTitle: document.getElementById("compare-chart-title"),
  compareTableBody: document.getElementById("compare-table-body"),

  settingsSave: document.getElementById("settings-save"),
  settingsReset: document.getElementById("settings-reset"),
  settingsDefaultGroup: document.getElementById("settings-default-group"),
  settingsDefaultCompareRange: document.getElementById("settings-default-compare-range"),
  watchlistBox: document.getElementById("watchlist-box"),

  toast: document.getElementById("toast"),
};

const charts = {
  detail: null,
  detailPercentile: null,
  compare: null,
};

const detailZoomSyncState = {
  syncing: false,
  rafId: 0,
  pending: null,
};

function safeJsonParse(rawValue, fallback) {
  if (!rawValue) return fallback;
  try {
    return JSON.parse(rawValue);
  } catch {
    return fallback;
  }
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function roundTo(value, digits = 2) {
  const ratio = 10 ** digits;
  return Math.round(Number(value) * ratio) / ratio;
}

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toFixed(digits);
}

function fmtSigned(value, digits = 2, asPct = false) {
  if (!Number.isFinite(Number(value))) return "--";
  const n = Number(value);
  const text = `${n >= 0 ? "+" : ""}${n.toFixed(digits)}`;
  return asPct ? `${text}%` : text;
}

function fmtPct(value, digits = 1) {
  return `${fmt(Number(value) * 100, digits)}%`;
}

function getLineEndLabelLayout(chartWidth, seriesCount = 1) {
  const width = Number(chartWidth) || 0;
  const compact = width > 0 && width < 900;
  const dense = seriesCount >= 6;
  const extraSeriesSpace = Math.min(44, Math.max(0, (seriesCount - 3) * 6));

  return {
    fontSize: compact ? (dense ? 9 : 10) : (dense ? 10 : 12),
    padding: compact ? [1, 4] : [2, 6],
    rightSpace: (compact ? 92 : 112) + extraSeriesSpace,
  };
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function percentileColor(percentile) {
  const p = clamp(Number(percentile), 0, 1);
  const hue = (1 - p) * 130;
  return `hsl(${hue}, 68%, 44%)`;
}

function parseDate(dateText) {
  return new Date(`${dateText}T00:00:00Z`);
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function formatAxisDate(value) {
  if (value === null || value === undefined) return "--";

  if (typeof value === "string") {
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? value : formatDate(parsed);
  }

  const parsed = new Date(Number(value));
  return Number.isNaN(parsed.getTime()) ? String(value) : formatDate(parsed);
}

function subtractYears(dateText, years) {
  const d = parseDate(dateText);
  d.setUTCFullYear(d.getUTCFullYear() - years);
  return formatDate(d);
}

function percentileWindow(values, startIndex, endIndex, current) {
  const start = Math.max(0, startIndex);
  const end = Math.min(values.length - 1, endIndex);
  if (end < start) return 0.5;
  let count = 0;
  const length = end - start + 1;
  for (let i = start; i <= end; i += 1) {
    if (values[i] <= current) count += 1;
  }
  return clamp(count / length, 0, 1);
}

function zScoreWindow(values, startIndex, endIndex, current) {
  const start = Math.max(0, startIndex);
  const end = Math.min(values.length - 1, endIndex);
  if (end < start) return 0;

  const length = end - start + 1;
  if (length <= 1) return 0;

  let sum = 0;
  for (let i = start; i <= end; i += 1) {
    sum += values[i];
  }
  const avg = sum / length;

  let variance = 0;
  for (let i = start; i <= end; i += 1) {
    const delta = values[i] - avg;
    variance += delta * delta;
  }

  const sigma = Math.sqrt(variance / (length - 1));
  if (sigma === 0) return 0;
  return (current - avg) / sigma;
}

function attentionRankFor(indexId, group) {
  const coreRank = CORE_ATTENTION_ORDER.indexOf(indexId);
  const sectorRank = SECTOR_ATTENTION_ORDER.indexOf(indexId);

  if (group === "core") return coreRank >= 0 ? coreRank : 999;
  if (group === "sector") return sectorRank >= 0 ? sectorRank : 999;
  return 999;
}

function compareByAttention(a, b) {
  const ga = a.group === "core" ? 0 : 1;
  const gb = b.group === "core" ? 0 : 1;
  if (ga !== gb) return ga - gb;

  const ra = attentionRankFor(a.indexId || a.id, a.group);
  const rb = attentionRankFor(b.indexId || b.id, b.group);
  if (ra !== rb) return ra - rb;

  return (a.displayName || "").localeCompare(b.displayName || "");
}

function getDefaultCompareSelection() {
  const valid = DEFAULT_COMPARE_INDEX_IDS.filter((id) => state.metaRows.some((item) => item.id === id));
  return valid.length ? valid : state.watchlist.slice(0, 4);
}

function snapshotToneVars(percentile) {
  const p = clamp(Number(percentile), 0, 1);
  const hue = 232 + p * 8;
  const hue2 = hue + 8;
  const sat = 46 + p * 5;
  const edgeLight = 60 - Math.abs(p - 0.5) * 6;
  const topLight = 40 + p * 2.5;
  const bottomLight = 25 + p * 2.5;
  const sweepLight = 72 - Math.abs(p - 0.5) * 5;
  const sweepDelay = (-p * 5.4).toFixed(2);

  const edge = `hsla(${hue.toFixed(1)}, ${sat.toFixed(1)}%, ${edgeLight.toFixed(1)}%, 0.36)`;
  const glow = `hsla(${hue.toFixed(1)}, 70%, 60%, 0.11)`;
  const top = `hsla(${hue.toFixed(1)}, 52%, ${topLight.toFixed(1)}%, 0.84)`;
  const bottom = `hsla(${hue2.toFixed(1)}, 48%, ${bottomLight.toFixed(1)}%, 0.92)`;
  const flare = `hsla(${hue.toFixed(1)}, 78%, 84%, 0.1)`;
  const glass = `hsla(${hue.toFixed(1)}, 56%, 72%, 0.08)`;
  const pin = `hsl(${(220 + p * 10).toFixed(1)}, 80%, 62%)`;
  const textGlow = `hsla(${hue.toFixed(1)}, 84%, 80%, 0.14)`;
  const sweep = `hsla(${hue.toFixed(1)}, 86%, ${sweepLight.toFixed(1)}%, 0.12)`;

  return `--card-edge:${edge};--card-glow:${glow};--card-top:${top};--card-bottom:${bottom};--card-flare:${flare};--card-glass:${glass};--card-pin:${pin};--card-text-glow:${textGlow};--card-sweep:${sweep};--card-sweep-delay:${sweepDelay}s;`;
}

function regimeFromPercentile(percentile) {
  if (percentile >= 0.85) return "high";
  if (percentile <= 0.15) return "low";
  return "neutral";
}

function computeLatestPeStats(points) {
  if (!Array.isArray(points) || !points.length) {
    return {
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      regime: "neutral",
      pe_ttm_change_1y: 0,
    };
  }

  const latest = points[points.length - 1];
  const latestPe = Number(latest?.pe_ttm);
  if (!Number.isFinite(latestPe)) {
    return {
      percentile_5y: 0.5,
      percentile_10y: 0.5,
      percentile_full: 0.5,
      z_score_3y: 0,
      regime: "neutral",
      pe_ttm_change_1y: 0,
    };
  }

  const total = points.length;
  const start5 = Math.max(0, total - TRADING_DAYS_PER_YEAR * 5);
  const start10 = Math.max(0, total - TRADING_DAYS_PER_YEAR * 10);
  const start3 = Math.max(0, total - TRADING_DAYS_PER_YEAR * 3);
  const lookbackIndex = Math.max(0, total - TRADING_DAYS_PER_YEAR);

  let fullCount = 0;
  let count5 = 0;
  let count10 = 0;
  let sum3 = 0;
  let sumSq3 = 0;
  let len3 = 0;

  for (let i = 0; i < total; i += 1) {
    const pe = Number(points[i]?.pe_ttm);
    if (!Number.isFinite(pe)) continue;

    if (pe <= latestPe) {
      fullCount += 1;
      if (i >= start5) count5 += 1;
      if (i >= start10) count10 += 1;
    }

    if (i >= start3) {
      sum3 += pe;
      sumSq3 += pe * pe;
      len3 += 1;
    }
  }

  const len5 = Math.max(1, total - start5);
  const len10 = Math.max(1, total - start10);
  const pctFull = clamp(fullCount / Math.max(1, total), 0, 1);
  const pct5 = clamp(count5 / len5, 0, 1);
  const pct10 = clamp(count10 / len10, 0, 1);

  let zScore3y = 0;
  if (len3 > 1) {
    const avg = sum3 / len3;
    const variance = Math.max(0, (sumSq3 - (sum3 * sum3) / len3) / (len3 - 1));
    const sigma = Math.sqrt(variance);
    if (sigma > 1e-12) {
      zScore3y = (latestPe - avg) / sigma;
    }
  }

  const peRef = Number(points[lookbackIndex]?.pe_ttm);
  const peChange1y = Number.isFinite(peRef) && Math.abs(peRef) > 1e-12 ? (latestPe - peRef) / Math.abs(peRef) : 0;

  return {
    percentile_5y: pct5,
    percentile_10y: pct10,
    percentile_full: pctFull,
    z_score_3y: zScore3y,
    regime: regimeFromPercentile(pctFull),
    pe_ttm_change_1y: peChange1y,
  };
}

function regimeLabel(regime) {
  if (regime === "high") return "高估";
  if (regime === "low") return "低估";
  return "中性";
}

function metricValueFromRaw(point, metric) {
  if (metric === "earnings_yield") {
    return point.pe_ttm > 0 ? 1 / point.pe_ttm : 0;
  }
  return Number(point[metric]);
}

async function fetchDataset() {
  const cacheBust = `v=${Date.now()}`;
  for (const basePath of DATA_PATH_CANDIDATES) {
    const path = `${basePath}${basePath.includes("?") ? "&" : "?"}${cacheBust}`;
    try {
      const response = await fetch(path, { cache: "no-store" });
      if (!response.ok) continue;
      const payload = await response.json();
      if (!payload || !Array.isArray(payload.indices) || !payload.indices.length) continue;
      return payload;
    } catch {
      // continue
    }
  }
  throw new Error("无法读取本地数据快照，请先在项目根目录运行 npm run build:data");
}

function ensureWatchlistDefaults() {
  const validIds = new Set(state.metaRows.map((item) => item.id));
  const sanitized = (Array.isArray(state.watchlist) ? state.watchlist : []).filter((id) => validIds.has(id));

  if (!sanitized.length) {
    state.watchlist = state.metaRows.slice(0, 6).map((item) => item.id);
  } else {
    state.watchlist = sanitized;
  }

  localStorage.setItem(STORAGE_KEYS.watchlist, JSON.stringify(state.watchlist));
}

function buildMetaRows() {
  state.metaRows = state.dataset.indices
    .map((item) => ({
      id: item.id,
      symbol: item.symbol,
      displayName: item.displayName,
      group: item.group,
      description: item.description,
      startDate: item.points[0]?.date || "",
      endDate: item.points[item.points.length - 1]?.date || "",
      pointCount: item.points.length,
      forwardStartDate: item.forwardStartDate || item.points[item.points.length - 1]?.date || "",
      order: 999,
    }))
    .sort((a, b) => compareByAttention(a, b));

  const orderMap = Object.fromEntries(state.metaRows.map((item, index) => [item.id, index + 1]));

  state.metaRows = state.metaRows.map((item) => ({
    ...item,
    order: orderMap[item.id] || 999,
  }));
}

function getIndexData(indexId) {
  return state.dataset.indices.find((item) => item.id === indexId);
}

function getMetricSeries(indexId, metric) {
  const cacheKey = `${indexId}:${metric}`;
  if (state.caches.metricSeries.has(cacheKey)) {
    return state.caches.metricSeries.get(cacheKey);
  }

  const indexData = getIndexData(indexId);
  if (!indexData) return [];

  const forwardStartDate = metric === "pe_forward" ? (indexData.forwardStartDate || indexData.points[indexData.points.length - 1]?.date || "") : "";
  const values = [];
  const result = [];

  for (let i = 0; i < indexData.points.length; i += 1) {
    const point = indexData.points[i];
    if (metric === "pe_forward" && forwardStartDate && point.date < forwardStartDate) {
      continue;
    }
    const value = metricValueFromRaw(point, metric);
    values.push(value);
    const valueIndex = values.length - 1;

    const pct5 = percentileWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 5 + 1, valueIndex, value);
    const pct10 = percentileWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 10 + 1, valueIndex, value);
    const pctFull = percentileWindow(values, 0, valueIndex, value);

    result.push({
      date: point.date,
      value,
      percentile_5y: pct5,
      percentile_10y: pct10,
      percentile_full: pctFull,
      z_score_3y: zScoreWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 3 + 1, valueIndex, value),
      regime: regimeFromPercentile(pctFull),
    });
  }

  state.caches.metricSeries.set(cacheKey, result);
  return result;
}

function buildSnapshotRows() {
  state.snapshotRows = state.dataset.indices.map((indexData) => {
    const points = Array.isArray(indexData.points) ? indexData.points : [];
    const latestRaw = points[points.length - 1] || {};
    const latestPe = computeLatestPeStats(points);

    return {
      indexId: indexData.id,
      symbol: indexData.symbol,
      displayName: indexData.displayName,
      group: indexData.group,
      date: latestRaw.date,
      pe_ttm: latestRaw.pe_ttm,
      pe_forward: latestRaw.pe_forward,
      pb: latestRaw.pb,
      percentile_5y: latestPe.percentile_5y,
      percentile_10y: latestPe.percentile_10y,
      percentile_full: latestPe.percentile_full,
      z_score_3y: latestPe.z_score_3y,
      pe_ttm_change_1y: latestPe.pe_ttm_change_1y,
      regime: latestPe.regime,
      startDate: points[0]?.date || "",
      endDate: points[points.length - 1]?.date || "",
      pointCount: points.length,
    };
  });
}

function getOverviewFilteredRows() {
  const keyword = state.overview.search.trim().toLowerCase();

  const rows = state.snapshotRows.filter((row) => {
    const groupOk =
      state.overview.group === "all"
        ? true
        : state.overview.group === "watchlist"
          ? state.watchlist.includes(row.indexId)
          : row.group === state.overview.group;

    if (!groupOk) return false;
    if (!keyword) return true;

    const text = `${row.displayName} ${row.symbol} ${row.indexId}`.toLowerCase();
    return text.includes(keyword);
  });

  rows.sort((a, b) => {
    switch (state.overview.sort) {
      case "attention":
        return compareByAttention(a, b);
      case "percentile_asc":
        return a.percentile_full - b.percentile_full;
      case "pe_desc":
        return b.pe_ttm - a.pe_ttm;
      case "pb_desc":
        return b.pb - a.pb;
      case "name":
        return a.displayName.localeCompare(b.displayName);
      case "percentile_desc":
      default:
        return b.percentile_full - a.percentile_full;
    }
  });

  return rows;
}

function openDetailIndex(indexId) {
  if (!indexId) return;
  state.detail.indexId = indexId;
  syncDetailSelectors();
  switchView("detail");
  renderDetail();
}

function snapshotBadge(row) {
  if (row.regime === "high") {
    return '<span class="badge high">高估</span>';
  }
  if (row.regime === "low") {
    return '<span class="badge low">低估</span>';
  }
  return '<span class="badge neutral">中性</span>';
}

function renderSnapshotGrid(rows) {
  elements.snapshotDate.textContent = rows[0]?.date ? `更新到 ${rows[0].date}` : "--";
  const isSearching = state.overview.search.trim().length > 0;

  elements.snapshotGrid.innerHTML = rows
    .map((row) => {
      const rawPct = clamp(row.percentile_full * 100, 0, 100);
      const pinLeft = rawPct;
      const peChangeTone = row.pe_ttm_change_1y >= 0 ? "up" : "down";
      const toneVars = snapshotToneVars(row.percentile_full);
      const searchCardLayoutStyle = isSearching ? "max-width:320px;width:100%;justify-self:start;" : "";
      const nameLength = String(row.displayName || "").length;
      const nameClass = nameLength >= 28 ? "name name--tight" : nameLength >= 20 ? "name name--compact" : "name";
      return `
      <article class="snapshot-card" data-index-id="${row.indexId}" style="${toneVars}${searchCardLayoutStyle}">
        <div class="name-row">
          <div>
            <div class="${nameClass}" title="${row.displayName}">${row.displayName}</div>
            <div class="symbol">${row.symbol} · ${row.group === "core" ? "核心" : "行业"}</div>
          </div>
          ${snapshotBadge(row)}
        </div>
        <div class="line"><span>PE(TTM)</span><strong>${fmt(row.pe_ttm, 2)}</strong></div>
        <div class="line"><span>PE(FWD)</span><strong>${fmt(row.pe_forward, 2)}</strong></div>
        <div class="line"><span>PB</span><strong>${fmt(row.pb, 2)}</strong></div>
        <div class="line"><span>1Y PE变化</span><strong class="${peChangeTone}">${fmtSigned(row.pe_ttm_change_1y * 100, 1, true)}</strong></div>
        <div class="line"><span>百分位</span><strong style="color:${percentileColor(row.percentile_full)}">${fmtPct(row.percentile_full, 1)}</strong></div>
        <div class="line line-muted"><span>数据区间</span><strong>${row.startDate} ~ ${row.endDate}</strong></div>
        <div class="percent-track-mini"><span class="pin" style="left:${pinLeft.toFixed(2)}%"></span></div>
      </article>`;
    })
    .join("");

  for (const card of elements.snapshotGrid.querySelectorAll(".snapshot-card")) {
    card.addEventListener("click", () => openDetailIndex(card.dataset.indexId));
  }
}

function ensureChart(name, element) {
  if (!element) return null;

  const width = element.clientWidth;
  const height = element.clientHeight;
  const canRender = width > 32 && height > 32;

  if (!canRender) return null;

  if (!charts[name]) {
    charts[name] = echarts.init(element);
    window.addEventListener("resize", () => charts[name]?.resize());
  }

  charts[name].resize();

  return charts[name];
}

function extractZoomRange(chart, payload) {
  const batchItem = Array.isArray(payload?.batch) ? payload.batch[payload.batch.length - 1] : payload;
  const optionZoom = chart?.getOption?.()?.dataZoom?.[0] || {};

  const rawStart = batchItem?.start ?? optionZoom.start ?? 0;
  const rawEnd = batchItem?.end ?? optionZoom.end ?? 100;

  return {
    start: clamp(Number(rawStart), 0, 100),
    end: clamp(Number(rawEnd), 0, 100),
  };
}

function applyZoomRange(chart, range) {
  if (!chart || !range) return;
  chart.dispatchAction({
    type: "dataZoom",
    dataZoomIndex: [0, 1],
    start: range.start,
    end: range.end,
  });
}

function resolveYAxisRangeFromSeriesData(seriesData, startPercent, endPercent) {
  if (!Array.isArray(seriesData) || !seriesData.length) return null;

  const values = seriesData
    .map((item) => (Array.isArray(item) ? Number(item[1]) : Number(item?.value?.[1])))
    .filter((value) => Number.isFinite(value));
  if (!values.length) return null;

  const total = values.length;
  const lo = clamp(Math.floor((clamp(startPercent, 0, 100) / 100) * (total - 1)), 0, total - 1);
  const hi = clamp(Math.ceil((clamp(endPercent, 0, 100) / 100) * (total - 1)), 0, total - 1);
  const from = Math.min(lo, hi);
  const to = Math.max(lo, hi);

  const visible = values.slice(from, to + 1).filter((value) => Number.isFinite(value));
  if (!visible.length) return null;

  let min = Math.min(...visible);
  let max = Math.max(...visible);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;

  const span = Math.max(max - min, 1e-6);
  const pad = Math.max(span * 0.12, 0.45);

  min -= pad * 0.65;
  max += pad * 0.35;

  if (min === max) {
    min -= pad;
    max += pad;
  }

  return {
    min: roundTo(min, 3),
    max: roundTo(max, 3),
  };
}

function bindDetailZoomSync() {
  const mainChart = charts.detail;
  const percentileChart = charts.detailPercentile;
  if (!mainChart || !percentileChart) return;

  mainChart.off("datazoom");
  percentileChart.off("datazoom");

  const applyMainYAxisRange = (range) => {
    const seriesData = mainChart.getOption?.()?.series?.[0]?.data || [];
    const yRange = resolveYAxisRangeFromSeriesData(seriesData, range?.start ?? 0, range?.end ?? 100);
    if (!yRange) return;

    mainChart.setOption(
      {
        yAxis: {
          min: yRange.min,
          max: yRange.max,
        },
      },
      false
    );
  };

  const scheduleSync = (sourceChart, targetChart, payload) => {
    if (detailZoomSyncState.syncing) return;

    detailZoomSyncState.pending = {
      targetChart,
      range: extractZoomRange(sourceChart, payload),
    };

    if (detailZoomSyncState.rafId) return;

    detailZoomSyncState.rafId = requestAnimationFrame(() => {
      const task = detailZoomSyncState.pending;
      detailZoomSyncState.pending = null;
      detailZoomSyncState.rafId = 0;
      if (!task) return;

      detailZoomSyncState.syncing = true;
      applyZoomRange(task.targetChart, task.range);
      applyMainYAxisRange(task.range);
      detailZoomSyncState.syncing = false;
    });
  };

  mainChart.on("datazoom", (payload) => scheduleSync(mainChart, percentileChart, payload));
  percentileChart.on("datazoom", (payload) => scheduleSync(percentileChart, mainChart, payload));

  applyMainYAxisRange(extractZoomRange(mainChart));
}

function renderOverview() {
  const rows = getOverviewFilteredRows();
  const isSearching = state.overview.search.trim().length > 0;
  elements.snapshotGrid.classList.toggle("is-searching", isSearching);
  renderSnapshotGrid(rows);
}

function sanitizeLegacyStaticMetricOption() {
  const cleanSelect = (selectEl) => {
    if (!selectEl) return;
    [...selectEl.options].forEach((option) => {
      if (String(option.value || "").trim() === "pe_static") {
        option.remove();
      }
    });
  };

  cleanSelect(elements.detailMetric);
  cleanSelect(elements.compareMetric);
}

function filterRowsByRange(rows, rangeCode) {
  if (!rows.length || rangeCode === "max") return rows;
  const years = Number(rangeCode.replace("y", ""));
  if (!Number.isFinite(years) || years <= 0) return rows;

  const endDate = rows[rows.length - 1].date;
  const threshold = subtractYears(endDate, years);
  const filtered = rows.filter((row) => row.date >= threshold);

  if (filtered.length < 60) {
    return rows;
  }
  return filtered;
}

function normalizeCompareDateRange(startDate = "", endDate = "") {
  const start = /^\d{4}-\d{2}-\d{2}$/.test(String(startDate || "")) ? String(startDate) : "";
  const end = /^\d{4}-\d{2}-\d{2}$/.test(String(endDate || "")) ? String(endDate) : "";
  if (start && end && start > end) {
    return { startDate: end, endDate: start };
  }
  return { startDate: start, endDate: end };
}

function filterRowsByCustomDateRange(rows, startDate = "", endDate = "") {
  if (!rows.length) return rows;
  const { startDate: start, endDate: end } = normalizeCompareDateRange(startDate, endDate);
  if (!start && !end) return rows;

  return rows.filter((row) => {
    if (start && row.date < start) return false;
    if (end && row.date > end) return false;
    return true;
  });
}

function recomputeRangeRollingStats(rows) {
  if (!rows.length) return rows;

  const values = [];
  const result = [];

  for (const row of rows) {
    const value = Number(row.value);
    values.push(value);
    const valueIndex = values.length - 1;

    const pct5 = percentileWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 5 + 1, valueIndex, value);
    const pct10 = percentileWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 10 + 1, valueIndex, value);
    const pctFull = percentileWindow(values, 0, valueIndex, value);

    result.push({
      ...row,
      percentile_5y: pct5,
      percentile_10y: pct10,
      percentile_full: pctFull,
      z_score_3y: zScoreWindow(values, valueIndex - TRADING_DAYS_PER_YEAR * 3 + 1, valueIndex, value),
      regime: regimeFromPercentile(pctFull),
    });
  }

  return result;
}

function resolveCompareDateBounds() {
  const indexIds = state.compare.indexIds.length
    ? state.compare.indexIds
    : state.watchlist.slice(0, 4);

  let minDate = "";
  let maxDate = "";

  for (const indexId of indexIds) {
    const fullRows = getMetricSeries(indexId, state.compare.metric);
    if (!fullRows.length) continue;

    const rangedRows = filterRowsByRange(fullRows, state.compare.range);
    if (!rangedRows.length) continue;

    const firstDate = rangedRows[0].date;
    const lastDate = rangedRows[rangedRows.length - 1].date;

    if (!minDate || firstDate < minDate) minDate = firstDate;
    if (!maxDate || lastDate > maxDate) maxDate = lastDate;
  }

  return { minDate, maxDate };
}

function syncCompareDateInputBounds() {
  const { minDate, maxDate } = resolveCompareDateBounds();

  if (elements.compareStartDate) {
    elements.compareStartDate.min = minDate;
    elements.compareStartDate.max = maxDate;
  }

  if (elements.compareEndDate) {
    elements.compareEndDate.min = minDate;
    elements.compareEndDate.max = maxDate;
  }

  let nextStart = state.compare.startDate;
  let nextEnd = state.compare.endDate;

  if (minDate && nextStart && nextStart < minDate) nextStart = minDate;
  if (maxDate && nextStart && nextStart > maxDate) nextStart = maxDate;
  if (minDate && nextEnd && nextEnd < minDate) nextEnd = minDate;
  if (maxDate && nextEnd && nextEnd > maxDate) nextEnd = maxDate;

  if (nextStart !== state.compare.startDate || nextEnd !== state.compare.endDate) {
    setCompareDateRange(nextStart, nextEnd);
  }
}

function setCompareDateRange(startDate = "", endDate = "", shouldPersist = true) {
  const normalized = normalizeCompareDateRange(startDate, endDate);
  state.compare.startDate = normalized.startDate;
  state.compare.endDate = normalized.endDate;

  if (elements.compareStartDate) elements.compareStartDate.value = normalized.startDate;
  if (elements.compareEndDate) elements.compareEndDate.value = normalized.endDate;

  if (shouldPersist) {
    if (normalized.startDate) localStorage.setItem(STORAGE_KEYS.compareStartDate, normalized.startDate);
    else localStorage.removeItem(STORAGE_KEYS.compareStartDate);

    if (normalized.endDate) localStorage.setItem(STORAGE_KEYS.compareEndDate, normalized.endDate);
    else localStorage.removeItem(STORAGE_KEYS.compareEndDate);
  }
}

function syncDetailSelectors() {
  elements.detailIndex.value = state.detail.indexId;
  elements.detailMetric.value = state.detail.metric;
  for (const chip of elements.detailRangeChips) {
    chip.classList.toggle("is-active", chip.dataset.range === state.detail.range);
  }
}

function renderDetailPercentileTrack(latest) {
  const full = clamp(latest.percentile_full, 0, 1);
  const left = `${(full * 100).toFixed(1)}%`;

  elements.detailPercentileTrack.innerHTML = `
    <div class="title">当前百分位位置（当前区间）</div>
    <div class="bar"><span class="pin" style="left:${left}"></span></div>
    <div class="labels">
      <span>0% 低估</span>
      <span>15%</span>
      <span>50%</span>
      <span>85%</span>
      <span>100% 高估</span>
    </div>
  `;
}

function renderDetailStats(fullRows, viewRows) {
  const latest = viewRows[viewRows.length - 1];
  const latestFull = fullRows[fullRows.length - 1] || latest;
  const metricCfg = METRIC_CONFIG[state.detail.metric];
  const values = viewRows.map((row) => row.value);

  const min = Math.min(...values);
  const max = Math.max(...values);
  const change = values.length > 1 ? ((values[values.length - 1] - values[0]) / Math.abs(values[0] || 1)) * 100 : 0;

  const valueText = metricCfg.percentage
    ? fmtSigned(latest.value * 100, metricCfg.digits, true)
    : fmt(latest.value, metricCfg.digits);

  elements.detailStats.innerHTML = [
    ["当前值", valueText],
    ["百分位(当前区间)", fmtPct(latest.percentile_full, 1)],
    ["百分位(全历史)", fmtPct(latestFull.percentile_full ?? latest.percentile_full, 1)],
    ["滚动百分位(5Y)", fmtPct(latest.percentile_5y, 1)],
    ["滚动百分位(10Y)", fmtPct(latest.percentile_10y, 1)],
    ["区间变动", fmtSigned(change, 2, true)],
    ["区间最低", metricCfg.percentage ? fmtSigned(min * 100, metricCfg.digits, true) : fmt(min, metricCfg.digits)],
    ["区间最高", metricCfg.percentage ? fmtSigned(max * 100, metricCfg.digits, true) : fmt(max, metricCfg.digits)],
    ["估值状态", regimeLabel(latest.regime)],
    ["数据区间", `${viewRows[0].date} ~ ${viewRows[viewRows.length - 1].date}`],
  ]
    .map(
      ([k, v]) => `
      <div class="stat-pill">
        <div class="k">${k}</div>
        <div class="v">${v}</div>
      </div>`
    )
    .join("");

  renderDetailPercentileTrack(latest);
}

function renderDetailChart(indexMeta, rows) {
  const chart = ensureChart("detail", elements.detailChart);
  if (!chart) return;
  const metricCfg = METRIC_CONFIG[state.detail.metric];
  const detailChartWidth = elements.detailChart?.clientWidth || 0;
  const endLabelLayout = getLineEndLabelLayout(elements.detailChart?.clientWidth, 1);
  const detailLabelFontSize = Math.max(endLabelLayout.fontSize + 4, 16);
  const detailRightPadding = Math.round(
    Math.max(detailChartWidth * 0.035, Math.min(endLabelLayout.rightSpace + 18, 102))
  );

  const metricFormatter = (value) =>
    metricCfg.percentage ? `${fmt(value * 100, metricCfg.digits)}%` : fmt(value, metricCfg.digits);

  chart.setOption(
    {
      animationDuration: 500,
      legend: {
        top: 4,
        textStyle: { color: "#88a4b8" },
      },
      grid: { left: 58, right: detailRightPadding, top: 46, bottom: 94 },
      tooltip: {
        trigger: "axis",
        formatter(params) {
          const price = params.find((item) => item.seriesName === metricCfg.label);
          const axisDate = formatAxisDate(price?.axisValue ?? params?.[0]?.axisValue);

          return [
            axisDate,
            `${metricCfg.label}: <strong>${price ? metricFormatter(price.data[1]) : "--"}</strong>`,
          ].join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        axisLabel: { color: "#8aa5b8" },
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
        },
        {
          type: "slider",
          xAxisIndex: 0,
          filterMode: "none",
          height: 24,
          bottom: 12,
          brushSelect: false,
          showDetail: false,
          borderColor: "rgba(159, 184, 236, 0.4)",
          backgroundColor: "rgba(29, 45, 85, 0.7)",
          fillerColor: "rgba(105, 182, 255, 0.25)",
          handleSize: 24,
          handleStyle: {
            color: "#7ab7ff",
            borderColor: "#d7e8ff",
            borderWidth: 1,
          },
          moveHandleStyle: {
            color: "rgba(149, 203, 255, 0.65)",
          },
          moveHandleSize: 20,
          textStyle: {
            color: "#8aa5b8",
          },
        },
      ],
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          color: "#8aa5b8",
          formatter(value) {
            return metricCfg.percentage ? `${value}%` : value;
          },
        },
        splitLine: { lineStyle: { color: "rgba(120,150,170,0.16)" } },
      },
      series: [
        {
          name: metricCfg.label,
          type: "line",
          smooth: false,
          showSymbol: false,
          lineStyle: { width: 2.2, color: "#1ba596" },
          areaStyle: {
            color: {
              type: "linear",
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(27,165,150,0.35)" },
                { offset: 1, color: "rgba(27,165,150,0.02)" },
              ],
            },
          },
          endLabel: {
            show: true,
            position: "right",
            distance: 8,
            formatter(params) {
              return metricFormatter(params.value?.[1]);
            },
            color: "#1ba596",
            fontSize: detailLabelFontSize,
            fontWeight: 900,
            padding: 0,
            borderRadius: 0,
            backgroundColor: "transparent",
          },
          labelLayout: {
            moveOverlap: "shiftY",
          },
          data: rows.map((row) => [row.date, metricCfg.percentage ? row.value * 100 : row.value]),
        },
      ],
    },
    true
  );

  const start = rows[0]?.date || "--";
  const end = rows[rows.length - 1]?.date || "--";
  elements.detailRange.textContent = `${indexMeta.displayName} · ${start} ~ ${end}`;
}

function renderDetailPercentileChart(rows) {
  const chart = ensureChart("detailPercentile", elements.detailPercentileChart);
  if (!chart) return;
  const percentileChartWidth = elements.detailPercentileChart?.clientWidth || 0;
  const endLabelLayout = getLineEndLabelLayout(elements.detailPercentileChart?.clientWidth, 1);
  const detailLabelFontSize = Math.max(endLabelLayout.fontSize + 4, 16);
  const percentileRightPadding = Math.round(
    Math.max(percentileChartWidth * 0.04, Math.min(endLabelLayout.rightSpace + 14, 92))
  );

  chart.setOption(
    {
      animationDuration: 420,
      grid: { left: 48, right: percentileRightPadding, top: 24, bottom: 88 },
      tooltip: {
        trigger: "axis",
        formatter(params) {
          const point = params?.[0];
          if (!point) return "--";
          return `${formatAxisDate(point.axisValue)}<br/>百分位: <strong>${fmt(point.data[1], 1)}%</strong>`;
        },
      },
      xAxis: {
        type: "time",
        axisLabel: { color: "#9ab3d3", fontSize: 11 },
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
        },
        {
          type: "slider",
          xAxisIndex: 0,
          filterMode: "none",
          height: 22,
          bottom: 10,
          brushSelect: false,
          showDetail: false,
          borderColor: "rgba(163, 186, 233, 0.38)",
          backgroundColor: "rgba(35, 52, 95, 0.68)",
          fillerColor: "rgba(132, 171, 255, 0.25)",
          handleSize: 22,
          handleStyle: {
            color: "#9ab8ff",
            borderColor: "#dce7ff",
            borderWidth: 1,
          },
          moveHandleStyle: {
            color: "rgba(166, 188, 255, 0.62)",
          },
          moveHandleSize: 18,
          textStyle: {
            color: "#9ab3d3",
          },
        },
      ],
      yAxis: {
        type: "value",
        min: 0,
        max: 100,
        axisLabel: { color: "#9ab3d3", formatter: "{value}%" },
        splitLine: { lineStyle: { color: "rgba(140,165,210,0.18)" } },
      },
      series: [
        {
          name: "百分位",
          type: "line",
          smooth: false,
          showSymbol: false,
          lineStyle: { width: 2, color: "rgba(123, 164, 255, 0.95)" },
          areaStyle: {
            color: {
              type: "linear",
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(124, 164, 255, 0.34)" },
                { offset: 1, color: "rgba(124, 164, 255, 0.04)" },
              ],
            },
          },
          endLabel: {
            show: true,
            position: "right",
            distance: 8,
            formatter(params) {
              return `${fmt(params.value?.[1], 1)}%`;
            },
            color: "rgba(123, 164, 255, 0.95)",
            fontSize: detailLabelFontSize,
            fontWeight: 900,
            padding: 0,
            borderRadius: 0,
            backgroundColor: "transparent",
          },
          labelLayout: {
            moveOverlap: "shiftY",
          },
          data: rows.map((row) => [row.date, row.percentile_full * 100]),
        },
      ],
    },
    true
  );
}

function renderDetail() {
  const indexId = state.detail.indexId;
  const metric = state.detail.metric;

  if (!indexId) return;

  const fullRows = getMetricSeries(indexId, metric);
  const rangedRows = filterRowsByRange(fullRows, state.detail.range);
  const viewRows = recomputeRangeRollingStats(rangedRows);

  const indexMeta = state.metaRows.find((item) => item.id === indexId);
  if (!indexMeta || !viewRows.length) return;

  renderDetailChart(indexMeta, viewRows);
  renderDetailPercentileChart(viewRows);
  bindDetailZoomSync();
  renderDetailStats(fullRows, viewRows);
}

function populateDetailOptions() {
  elements.detailIndex.innerHTML = state.metaRows
    .map((item) => `<option value="${item.id}">${item.displayName}</option>`)
    .join("");

  if (!state.detail.indexId || !state.metaRows.some((item) => item.id === state.detail.indexId)) {
    state.detail.indexId = state.watchlist[0] || state.metaRows[0]?.id || "";
  }

  syncDetailSelectors();
}

function buildCompareIndexList() {
  const list = state.compare.watchlistOnly
    ? state.metaRows.filter((item) => state.watchlist.includes(item.id))
    : state.metaRows;

  elements.compareIndexPicker.innerHTML = list
    .map((item) => {
      const checked = state.compare.indexIds.includes(item.id) ? "checked" : "";
      return `
      <label class="compare-item">
        <input type="checkbox" data-index-id="${item.id}" ${checked} />
        <span>${item.displayName}</span>
      </label>`;
    })
    .join("");
}

function collectCompareSelection() {
  const checked = [...elements.compareIndexPicker.querySelectorAll("input[type=checkbox]")]
    .filter((input) => input.checked)
    .map((input) => input.dataset.indexId);

  state.compare.indexIds = checked.slice(0, 8);
}

function buildCompareRows() {
  const indexIds = state.compare.indexIds.length
    ? state.compare.indexIds
    : state.watchlist.slice(0, 4);

  const metric = state.compare.metric;
  const range = state.compare.range;
  const customStart = state.compare.startDate;
  const customEnd = state.compare.endDate;

  const seriesList = indexIds
    .map((indexId) => {
      const full = getMetricSeries(indexId, metric);
      const ranged = filterRowsByRange(full, range);
      const filtered = filterRowsByCustomDateRange(ranged, customStart, customEnd);
      const normalized = recomputeRangeRollingStats(filtered);
      return {
        indexId,
        full,
        rows: normalized,
      };
    })
    .filter((item) => item.rows.length >= 2);

  if (!seriesList.length) return [];

  const commonStart = seriesList
    .map((item) => item.rows[0].date)
    .sort()
    .at(-1);

  return seriesList.map((item) => {
    const rows = item.rows.filter((row) => row.date >= commonStart);
    return {
      ...item,
      rows,
    };
  }).filter((item) => item.rows.length >= 2);
}

function getSeriesPointAtOrBefore(seriesRows, targetDate) {
  if (!Array.isArray(seriesRows) || !seriesRows.length) return null;
  if (!targetDate) return seriesRows[seriesRows.length - 1];

  let left = 0;
  let right = seriesRows.length - 1;
  let ans = -1;

  while (left <= right) {
    const mid = (left + right) >> 1;
    const date = seriesRows[mid].date;
    if (date <= targetDate) {
      ans = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return ans >= 0 ? seriesRows[ans] : seriesRows[0];
}

function buildCompareCrossSection(rows, targetDate = "") {
  return rows
    .map((item) => {
      const point = getSeriesPointAtOrBefore(item.rows, targetDate);
      const meta = state.metaRows.find((m) => m.id === item.indexId);
      if (!point) return null;
      return {
        indexId: item.indexId,
        name: meta?.displayName || item.indexId,
        latest: point,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.latest.value - a.latest.value);
}

function measureVisualNameLength(text) {
  const raw = String(text || "");
  if (!raw) return 0;
  let size = 0;
  for (const ch of raw) {
    size += /[^\x00-\xff]/.test(ch) ? 2 : 1;
  }
  return size;
}

function tuneCompareTableLayout(latestRows) {
  const sidePanel = elements.compareTableBody?.closest(".compare-side-panel");
  if (!sidePanel) return;

  if (!latestRows.length) {
    sidePanel.style.removeProperty("--compare-name-col");
    sidePanel.style.removeProperty("--compare-rest-col");
    sidePanel.style.removeProperty("--compare-head-font");
    sidePanel.style.removeProperty("--compare-cell-font");
    sidePanel.style.removeProperty("--compare-name-font");
    return;
  }

  const panelWidth = sidePanel.clientWidth || 420;
  const maxNameLen = latestRows.reduce((max, item) => Math.max(max, measureVisualNameLength(item.name)), 0);
  const rowCount = latestRows.length;

  let nameCol = 48;
  if (maxNameLen <= 16) nameCol = 44;
  else if (maxNameLen <= 20) nameCol = 47;
  else if (maxNameLen <= 26) nameCol = 51;
  else nameCol = 54;

  if (rowCount <= 4) {
    nameCol = Math.max(nameCol, 56);
  } else if (rowCount <= 6) {
    nameCol = Math.max(nameCol, 52);
  }

  if (panelWidth >= 520 && rowCount <= 4) {
    nameCol += 2;
  }
  if (panelWidth <= 420) nameCol += 2;
  if (panelWidth <= 380) nameCol += 2;
  nameCol = clamp(nameCol, 44, 62);

  const restCol = (100 - nameCol) / 3;
  const compactNames = maxNameLen >= 24;

  let headFont = panelWidth >= 470 ? 0.82 : panelWidth >= 420 ? 0.8 : 0.77;
  let cellFont = panelWidth >= 470 ? 0.95 : panelWidth >= 420 ? 0.92 : 0.88;
  let nameFont = panelWidth >= 470 ? 0.97 : panelWidth >= 420 ? 0.93 : 0.88;

  if (rowCount >= 10) {
    headFont -= 0.03;
    cellFont -= 0.04;
    nameFont -= 0.05;
  } else if (rowCount <= 5) {
    cellFont += 0.02;
    nameFont += 0.03;
  }

  if (compactNames) {
    nameFont -= 0.05;
  }

  sidePanel.style.setProperty("--compare-name-col", `${nameCol.toFixed(2)}%`);
  sidePanel.style.setProperty("--compare-rest-col", `${restCol.toFixed(2)}%`);
  sidePanel.style.setProperty("--compare-head-font", `${clamp(headFont, 0.72, 0.88).toFixed(3)}rem`);
  sidePanel.style.setProperty("--compare-cell-font", `${clamp(cellFont, 0.82, 1.0).toFixed(3)}rem`);
  sidePanel.style.setProperty("--compare-name-font", `${clamp(nameFont, 0.82, 1.02).toFixed(3)}rem`);
}

function renderCompareLatestViews(latestRows, metricCfg, lineColorByIndexId) {
  tuneCompareTableLayout(latestRows);

  if (!latestRows.length) {
    elements.compareTableBody.innerHTML = '<tr><td colspan="4" class="hint">当前区间无可用样本</td></tr>';
    return;
  }

  elements.compareTableBody.innerHTML = latestRows
    .map((item) => {
      const regime = regimeLabel(item.latest.regime);
      const valueText = metricCfg.percentage
        ? `${fmt(item.latest.value * 100, metricCfg.digits)}%`
        : fmt(item.latest.value, metricCfg.digits);
      const lineColor = lineColorByIndexId?.get(item.indexId) || "#9bb6ff";
      return `
      <tr>
        <td class="compare-name-cell" style="color:${lineColor}">
          <div class="compare-name-content">
            <span class="line-dot" style="background:${lineColor}"></span>
            <span class="compare-name-text" title="${item.name}">${item.name}</span>
          </div>
        </td>
        <td>${valueText}</td>
        <td style="color:${percentileColor(item.latest.percentile_full)}">${fmtPct(item.latest.percentile_full, 1)}</td>
        <td>${regime}</td>
      </tr>`;
    })
    .join("");
}

function renderCompareSummary(rows, metricCfg, crossSectionRows = null) {
  if (!elements.compareSummary) return;
  if (!rows.length) {
    elements.compareSummary.innerHTML = "";
    return;
  }

  const alignedStart = rows[0].rows[0]?.date || "--";
  const alignedEnd = rows[0].rows[rows[0].rows.length - 1]?.date || "--";
  const points = Array.isArray(crossSectionRows) && crossSectionRows.length
    ? crossSectionRows.map((item) => item.latest)
    : rows.map((item) => item.rows[item.rows.length - 1]).filter(Boolean);
  const medianPct = median(points.map((point) => point?.percentile_full || 0));

  elements.compareSummary.innerHTML = [
    { label: "对比指数", value: `${rows.length} 个` },
    { label: "对齐区间", value: `${alignedStart} ~ ${alignedEnd}` },
    { label: "中位分位", value: fmtPct(medianPct, 1) },
  ]
    .map(
      (item) => `
      <div class="summary-pill">
        <div class="k">${item.label}</div>
        <div class="v">${item.value}</div>
      </div>`
    )
    .join("");
}

function resolveCompareYAxisRange(rows, metricCfg, startPercent, endPercent) {
  if (!rows.length) return null;

  const timelineLength = rows[0]?.rows?.length || 0;
  if (!timelineLength) return null;

  const startIdx = clamp(Math.floor((clamp(startPercent, 0, 100) / 100) * (timelineLength - 1)), 0, timelineLength - 1);
  const endIdx = clamp(Math.ceil((clamp(endPercent, 0, 100) / 100) * (timelineLength - 1)), 0, timelineLength - 1);
  const lo = Math.min(startIdx, endIdx);
  const hi = Math.max(startIdx, endIdx);

  const values = [];
  for (const item of rows) {
    for (let i = lo; i <= hi; i += 1) {
      const point = item.rows[i];
      if (!point) continue;
      const value = metricCfg.percentage ? point.value * 100 : point.value;
      if (Number.isFinite(value)) values.push(value);
    }
  }

  if (!values.length) return null;

  let min = Math.min(...values);
  let max = Math.max(...values);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;

  const span = Math.max(max - min, 1e-6);
  const pad = Math.max(span * 0.12, metricCfg.percentage ? 0.35 : 0.55);

  min -= pad * 0.68;
  max += pad * 0.38;

  if (min === max) {
    min -= pad;
    max += pad;
  }

  return {
    min: roundTo(min, 3),
    max: roundTo(max, 3),
  };
}

function renderCompareCharts() {
  const metricCfg = METRIC_CONFIG[state.compare.metric];
  syncCompareDateInputBounds();

  if (elements.compareChartTitle) {
    const hasCustomDate = Boolean(state.compare.startDate || state.compare.endDate);
    const dateTag = hasCustomDate
      ? ` · ${state.compare.startDate || "最早"} ~ ${state.compare.endDate || "最新"}`
      : "";
    elements.compareChartTitle.textContent = `对比走势 · ${metricCfg.label}${dateTag}`;
  }

  const chart = ensureChart("compare", elements.compareChart);
  const rows = buildCompareRows();
  const compareChartWidth = elements.compareChart?.clientWidth || 0;
  const endLabelLayout = getLineEndLabelLayout(compareChartWidth, rows.length);
  const compareLabelFontSize = Math.max(endLabelLayout.fontSize + 3, 15);
  const compareRightPadding = Math.round(
    Math.max(24, Math.min(endLabelLayout.rightSpace * 0.38 + 10, compareChartWidth * 0.06, 50))
  );

  if (!rows.length) {
    const noDataMessage =
      state.compare.startDate || state.compare.endDate
        ? "当前日期范围样本不足（每条曲线至少需要 2 个点）"
        : "请至少选择一个有效指数";
    elements.compareTableBody.innerHTML = `<tr><td colspan="4" class="hint">${noDataMessage}</td></tr>`;
    renderCompareSummary([], metricCfg);
    chart?.clear();
    return;
  }

  const legend = [];
  const lineSeries = [];
  const lineColorByIndexId = new Map();

  for (const [seriesIndex, item] of rows.entries()) {
    const meta = state.metaRows.find((m) => m.id === item.indexId);
    if (!meta) continue;
    legend.push(meta.displayName);
    const lineColor = COMPARE_LINE_COLORS[seriesIndex % COMPARE_LINE_COLORS.length];
    lineColorByIndexId.set(item.indexId, lineColor);

    const data = item.rows.map((row) => {
      const raw = metricCfg.percentage ? row.value * 100 : row.value;
      return [row.date, raw];
    });

    lineSeries.push({
      name: meta.displayName,
      type: "line",
      smooth: false,
      showSymbol: false,
      lineStyle: {
        width: 2.5,
        color: lineColor,
      },
      itemStyle: {
        color: lineColor,
      },
      endLabel: {
        show: true,
        position: "right",
        distance: 6,
        align: "left",
        verticalAlign: "middle",
        formatter(params) {
          const value = params.value?.[1];
          return metricCfg.percentage ? `${fmt(value, metricCfg.digits)}%` : fmt(value, metricCfg.digits);
        },
        color: lineColor,
        fontSize: compareLabelFontSize,
        fontWeight: 900,
        padding: 0,
      },
      labelLayout: {
        moveOverlap: "shiftY",
      },
      data,
    });
  }

  if (chart) {
    chart.setOption(
      {
        animationDuration: 500,
        legend: {
          data: legend,
          top: 4,
          textStyle: { color: "#88a4b8" },
        },
        grid: { left: 60, right: compareRightPadding, top: 46, bottom: 94 },
        tooltip: {
          trigger: "axis",
        },
        xAxis: {
          type: "time",
          axisLabel: { color: "#8aa5b8" },
        },
        dataZoom: [
          {
            type: "inside",
            xAxisIndex: 0,
            filterMode: "filter",
          },
          {
            type: "slider",
            xAxisIndex: 0,
            filterMode: "filter",
            height: 24,
            bottom: 12,
            brushSelect: false,
            showDetail: false,
            borderColor: "rgba(159, 184, 236, 0.4)",
            backgroundColor: "rgba(29, 45, 85, 0.7)",
            fillerColor: "rgba(105, 182, 255, 0.25)",
            handleSize: 24,
            handleStyle: {
              color: "#7ab7ff",
              borderColor: "#d7e8ff",
              borderWidth: 1,
            },
            moveHandleStyle: {
              color: "rgba(149, 203, 255, 0.65)",
            },
            moveHandleSize: 20,
            textStyle: {
              color: "#8aa5b8",
            },
          },
        ],
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: {
            color: "#8aa5b8",
            formatter(value) {
              return metricCfg.percentage ? `${fmt(value, metricCfg.digits)}%` : fmt(value, metricCfg.digits);
            },
          },
          splitLine: { lineStyle: { color: "rgba(120,150,170,0.16)" } },
        },
        series: lineSeries,
      },
      true
    );

    chart.off("datazoom");
  }

  const timeline = rows[0]?.rows?.map((point) => point.date) || [];
  const defaultFocusDate = timeline[timeline.length - 1] || "";
  let activeFocusDate = defaultFocusDate;
  let rafSyncId = 0;
  let pendingZoomRange = { start: 0, end: 100 };

  const applyCompareYAxisRange = (startPercent, endPercent) => {
    if (!chart) return;
    const range = resolveCompareYAxisRange(rows, metricCfg, startPercent, endPercent);
    if (!range) return;

    chart.setOption(
      {
        yAxis: {
          min: range.min,
          max: range.max,
        },
      },
      false
    );
  };

  const syncCrossSection = (focusDate) => {
    const finalDate = focusDate || defaultFocusDate;
    const crossSectionRows = buildCompareCrossSection(rows, finalDate);
    renderCompareSummary(rows, metricCfg, crossSectionRows);
    renderCompareLatestViews(crossSectionRows, metricCfg, lineColorByIndexId);
  };

  syncCrossSection(defaultFocusDate);
  applyCompareYAxisRange(0, 100);

  if (!chart || timeline.length < 2) return;

  chart.on("datazoom", () => {
    const dz = chart.getOption()?.dataZoom?.[0];
    const rawEndValue = dz?.endValue;
    const fromEndValue = rawEndValue !== undefined && rawEndValue !== null ? formatAxisDate(rawEndValue) : "";
    const endPercent = clamp(Number(dz?.end ?? 100), 0, 100);
    const index = clamp(Math.round((endPercent / 100) * (timeline.length - 1)), 0, timeline.length - 1);
    const nextFocusDate = fromEndValue || timeline[index] || defaultFocusDate;
    pendingZoomRange = {
      start: clamp(Number(dz?.start ?? 0), 0, 100),
      end: endPercent,
    };

    if (rafSyncId) cancelAnimationFrame(rafSyncId);
    rafSyncId = requestAnimationFrame(() => {
      applyCompareYAxisRange(pendingZoomRange.start, pendingZoomRange.end);
      if (nextFocusDate) {
        activeFocusDate = nextFocusDate;
      }
      syncCrossSection(activeFocusDate);
      rafSyncId = 0;
    });
  });
}

function renderSettings() {
  elements.settingsDefaultGroup.value = state.settings.defaultGroup;
  elements.settingsDefaultCompareRange.value = state.settings.defaultCompareRange;

  elements.watchlistBox.innerHTML = state.metaRows
    .map((item) => {
      const checked = state.watchlist.includes(item.id) ? "checked" : "";
      const groupLabel = item.group === "core" ? "核心" : "行业";
      return `
      <label class="watch-item">
        <span class="watch-item-main">
          <span class="watch-item-name">${item.displayName}</span>
          <span class="watch-item-meta">${groupLabel} · ${item.symbol}</span>
        </span>
        <input class="watch-item-check" type="checkbox" data-index-id="${item.id}" ${checked} />
      </label>`;
    })
    .join("");
}

function saveSettings() {
  const pickedWatchlist = [...elements.watchlistBox.querySelectorAll("input[type=checkbox]")]
    .filter((input) => input.checked)
    .map((input) => input.dataset.indexId);

  state.watchlist = pickedWatchlist.length ? pickedWatchlist : state.metaRows.slice(0, 6).map((item) => item.id);
  state.settings.defaultGroup = elements.settingsDefaultGroup.value;
  state.settings.defaultCompareRange = elements.settingsDefaultCompareRange.value;

  state.overview.group = state.settings.defaultGroup;
  state.compare.range = state.settings.defaultCompareRange;

  localStorage.setItem(STORAGE_KEYS.watchlist, JSON.stringify(state.watchlist));
  localStorage.setItem(STORAGE_KEYS.overviewGroup, state.settings.defaultGroup);
  localStorage.setItem(STORAGE_KEYS.compareRange, state.settings.defaultCompareRange);

  elements.overviewGroupFilter.value = state.overview.group;
  elements.compareRange.value = state.compare.range;

  state.compare.indexIds = getDefaultCompareSelection();

  buildCompareIndexList();
  renderOverview();
  renderCompareCharts();
  renderSettings();

  showToast("偏好已保存");
}

function resetSettings() {
  state.settings.defaultGroup = "all";
  state.settings.defaultCompareRange = "max";
  state.overview.group = "all";
  state.compare.range = "max";
  state.compare.startDate = "";
  state.compare.endDate = "";
  state.watchlist = state.metaRows.slice(0, 6).map((item) => item.id);
  state.compare.indexIds = getDefaultCompareSelection();

  localStorage.removeItem(STORAGE_KEYS.watchlist);
  localStorage.removeItem(STORAGE_KEYS.overviewGroup);
  localStorage.removeItem(STORAGE_KEYS.compareRange);
  localStorage.removeItem(STORAGE_KEYS.compareStartDate);
  localStorage.removeItem(STORAGE_KEYS.compareEndDate);

  elements.overviewGroupFilter.value = state.overview.group;
  elements.compareRange.value = state.compare.range;
  setCompareDateRange("", "", false);

  buildCompareIndexList();
  renderOverview();
  renderCompareCharts();
  renderSettings();

  showToast("已恢复默认设置");
}

let toastTimer = null;

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.classList.add("show");

  if (toastTimer) {
    clearTimeout(toastTimer);
  }

  toastTimer = setTimeout(() => {
    elements.toast.classList.remove("show");
  }, 1800);
}

function switchView(view) {
  for (const button of elements.tabButtons) {
    button.classList.toggle("is-active", button.dataset.view === view);
  }

  for (const panel of elements.viewPanels) {
    panel.classList.toggle("is-active", panel.id === `view-${view}`);
  }

  if (view === "detail") {
    renderDetail();
  }
  if (view === "compare") {
    renderCompareCharts();
  }
  if (view === "settings") {
    renderSettings();
  }

  const resizeActiveCharts = () => {
    if (view === "detail") {
      charts.detail?.resize();
      charts.detailPercentile?.resize();
      return;
    }
    if (view === "compare") {
      charts.compare?.resize();
    }
  };

  requestAnimationFrame(resizeActiveCharts);
  setTimeout(resizeActiveCharts, 80);
  setTimeout(resizeActiveCharts, 260);
}

function applyTheme() {
  document.body.dataset.theme = "terminal";
}

function applyDataSourceBadge(sourceText = "") {
  if (!elements.dataModeChip) return;

  const source = String(sourceText || "").toLowerCase();
  elements.dataModeChip.style.background = "";
  elements.dataModeChip.style.borderColor = "";

  if (!source) {
    elements.dataModeChip.textContent = "数据源未知";
    return;
  }

  if (source.includes("synthetic")) {
    elements.dataModeChip.textContent = "仿真回退数据";
    elements.dataModeChip.style.background = "rgba(193,79,98,0.2)";
    elements.dataModeChip.style.borderColor = "rgba(193,79,98,0.5)";
    return;
  }

  if (source.includes("history-fallback")) {
    elements.dataModeChip.textContent = "真实源 + 历史兜底";
    elements.dataModeChip.style.background = "rgba(197,152,42,0.16)";
    elements.dataModeChip.style.borderColor = "rgba(197,152,42,0.45)";
    return;
  }

  elements.dataModeChip.textContent = "真实免费数据";
}

function bindEvents() {
  elements.enterCompanyBoardBtn?.addEventListener("click", (event) => {
    const href = event.currentTarget?.getAttribute("href") || "./companies.html";
    event.preventDefault();

    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      window.location.href = href;
      return;
    }

    document.body.classList.add("is-board-flipping");
    window.setTimeout(() => {
      window.location.href = href;
    }, 320);
  });

  elements.tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      switchView(button.dataset.view);
    });
  });

  elements.overviewGroupFilter.addEventListener("change", (event) => {
    state.overview.group = event.target.value;
    renderOverview();
  });

  elements.overviewSortSelect.addEventListener("change", (event) => {
    state.overview.sort = event.target.value;
    renderOverview();
  });

  elements.overviewSearch.addEventListener("input", (event) => {
    state.overview.search = event.target.value;
    renderOverview();
  });

  elements.detailIndex.addEventListener("change", (event) => {
    state.detail.indexId = event.target.value;
    renderDetail();
  });

  elements.detailMetric.addEventListener("change", (event) => {
    state.detail.metric = event.target.value;
    renderDetail();
  });

  elements.detailRangeChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      state.detail.range = chip.dataset.range;
      syncDetailSelectors();
      renderDetail();
    });
  });

  elements.detailRefresh.addEventListener("click", () => {
    renderDetail();
    showToast("详情图表已刷新");
  });

  elements.compareMetric.addEventListener("change", (event) => {
    state.compare.metric = event.target.value;
    localStorage.setItem(STORAGE_KEYS.compareMetric, state.compare.metric);
    renderCompareCharts();
  });

  elements.compareRange.addEventListener("change", (event) => {
    state.compare.range = event.target.value;
    localStorage.setItem(STORAGE_KEYS.compareRange, state.compare.range);
    renderCompareCharts();
  });

  const handleCompareDateInput = () => {
    setCompareDateRange(elements.compareStartDate?.value || "", elements.compareEndDate?.value || "");
    renderCompareCharts();
  };

  elements.compareStartDate?.addEventListener("change", handleCompareDateInput);
  elements.compareEndDate?.addEventListener("change", handleCompareDateInput);
  elements.compareResetDate?.addEventListener("click", () => {
    setCompareDateRange("", "");
    renderCompareCharts();
  });

  elements.compareWatchlistOnly.addEventListener("change", (event) => {
    state.compare.watchlistOnly = event.target.checked;
    buildCompareIndexList();
  });

  elements.compareApply.addEventListener("click", () => {
    setCompareDateRange(elements.compareStartDate?.value || "", elements.compareEndDate?.value || "");
    collectCompareSelection();
    renderCompareCharts();
    showToast("对比配置已应用");
  });

  elements.settingsSave.addEventListener("click", saveSettings);
  elements.settingsReset.addEventListener("click", resetSettings);
}

function initSelections() {
  if (!METRIC_CONFIG[state.detail.metric]) {
    state.detail.metric = "pe_ttm";
  }
  if (!METRIC_CONFIG[state.compare.metric]) {
    state.compare.metric = "pe_ttm";
  }

  if (![...elements.overviewSortSelect.options].some((option) => option.value === state.overview.sort)) {
    state.overview.sort = "attention";
  }

  elements.overviewGroupFilter.value = state.overview.group;
  elements.overviewSortSelect.value = state.overview.sort;
  elements.overviewSearch.value = state.overview.search;

  elements.compareMetric.value = state.compare.metric;
  elements.compareRange.value = state.compare.range;
  setCompareDateRange(state.compare.startDate, state.compare.endDate, false);
  elements.compareWatchlistOnly.checked = state.compare.watchlistOnly;

  if (!state.compare.indexIds.length) {
    state.compare.indexIds = getDefaultCompareSelection();
  }
}

async function bootstrap() {
  applyTheme();
  sanitizeLegacyStaticMetricOption();
  bindEvents();

  try {
    state.dataset = await fetchDataset();
    buildMetaRows();
    ensureWatchlistDefaults();
    buildSnapshotRows();

    initSelections();
    populateDetailOptions();
    buildCompareIndexList();

    renderOverview();
    renderSettings();

    applyDataSourceBadge(state.dataset.source);
    elements.updatedChip.textContent = `数据更新: ${state.dataset.generatedAt?.slice(0, 19).replace("T", " ") || "--"}`;
  } catch (error) {
    console.error(error);
    if (elements.dataModeChip) {
      elements.dataModeChip.textContent = "数据加载失败";
      elements.dataModeChip.style.background = "rgba(193,79,98,0.2)";
    }
    elements.updatedChip.textContent = "请先执行 npm run build:data";
    elements.snapshotGrid.innerHTML = `<div class="hint">${error.message}</div>`;
  }
}

bootstrap();
