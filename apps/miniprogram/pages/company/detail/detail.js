const { request } = require("../../../utils/api");

const WATCHLIST_STORAGE_KEY = "usvm-company-watchlist";

const METRIC_OPTIONS = [
  { label: "PE(TTM)", value: "pe_ttm" },
  { label: "PE(Forward)", value: "pe_forward" },
  { label: "PB", value: "pb" },
];

const RANGE_OPTIONS = [
  { label: "MAX", value: "max" },
  { label: "20Y", value: "20y" },
  { label: "10Y", value: "10y" },
  { label: "5Y", value: "5y" },
  { label: "3Y", value: "3y" },
  { label: "1Y", value: "1y" },
];
const DEFAULT_RANGE_INDEX = RANGE_OPTIONS.findIndex((item) => item.value === "10y");

function toPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function toNumberText(value, digits) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "--";
}

function toMetricText(value, metric) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "--";
  return n.toFixed(2);
}

function toMarketCapText(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "--";
  if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  return `$${n.toFixed(0)}`;
}

function subtractYears(dateText, years) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateText || ""))) return dateText;
  const date = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return dateText;
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return date.toISOString().slice(0, 10);
}

function filterRowsByRange(rows, rangeCode) {
  if (!Array.isArray(rows) || !rows.length) return [];
  if (rangeCode === "max") return rows;

  const years = Number(String(rangeCode || "").replace("y", ""));
  if (!Number.isFinite(years) || years <= 0) return rows;

  const endDate = rows[rows.length - 1].date;
  const threshold = subtractYears(endDate, years);
  const filtered = rows.filter((row) => row.date >= threshold);
  return filtered.length ? filtered : rows;
}

function buildRenderableRows(rows, maxPoints) {
  const cleanRows = (Array.isArray(rows) ? rows : [])
    .map((item) => ({
      ...item,
      value: Number(item.value),
    }))
    .filter((item) => Number.isFinite(item.value));

  if (!cleanRows.length) return [];
  if (!Number.isFinite(maxPoints) || maxPoints < 16 || cleanRows.length <= maxPoints) return cleanRows;

  const sampled = [];
  const step = (cleanRows.length - 1) / (maxPoints - 1);
  let lastIndex = -1;
  for (let i = 0; i < maxPoints; i += 1) {
    const idx = i === maxPoints - 1 ? cleanRows.length - 1 : Math.floor(i * step);
    if (idx === lastIndex) continue;
    sampled.push(cleanRows[idx]);
    lastIndex = idx;
  }
  if (sampled[sampled.length - 1] !== cleanRows[cleanRows.length - 1]) {
    sampled.push(cleanRows[cleanRows.length - 1]);
  }
  return sampled;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function formatAxisDate(dateText) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateText || ""))) return "--";
  return `${dateText.slice(0, 4)}-${dateText.slice(5, 7)}`;
}

function buildAxisLabels(rows) {
  if (!Array.isArray(rows) || !rows.length) return ["--", "--", "--"];
  const start = rows[0].date;
  const mid = rows[Math.floor((rows.length - 1) / 2)].date;
  const end = rows[rows.length - 1].date;
  return [formatAxisDate(start), formatAxisDate(mid), formatAxisDate(end)];
}

function normalizeWatchlist(raw) {
  if (Array.isArray(raw)) {
    return raw.filter((item) => typeof item === "string" && item.trim()).map((item) => item.trim());
  }

  if (typeof raw === "string" && raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.filter((item) => typeof item === "string" && item.trim()).map((item) => item.trim());
      }
    } catch (error) {
      console.error(error);
    }
  }
  return [];
}

Page({
  data: {
    indexId: "",
    displayName: "",
    symbol: "",
    marketCapText: "--",
    rankText: "--",
    isWatched: false,
    metricOptions: METRIC_OPTIONS,
    metricIndex: 0,
    rangeOptions: RANGE_OPTIONS,
    rangeIndex: DEFAULT_RANGE_INDEX >= 0 ? DEFAULT_RANGE_INDEX : 0,
    availableRangeText: "",
    rangeSummaryText: "",
    mainAxisLabels: ["--", "--", "--"],
    latest: null,
    loading: true,
  },

  cache: {
    canvasRects: {},
    isAlive: false,
    loadToken: 0,
  },

  onLoad(query) {
    this.cache.isAlive = true;
    const patch = {};
    if (query.indexId) patch.indexId = query.indexId;
    if (query.displayName) patch.displayName = decodeURIComponent(query.displayName);
    if (query.symbol) patch.symbol = decodeURIComponent(query.symbol);
    if (Object.keys(patch).length) this.setData(patch);
    this.syncWatchState();
  },

  onShow() {
    this.cache.isAlive = true;
    this.loadPage();
  },

  onHide() {
    this.cache.isAlive = false;
  },

  onUnload() {
    this.cache.isAlive = false;
  },

  nextLoadToken() {
    this.cache.loadToken += 1;
    return this.cache.loadToken;
  },

  isLoadActive(token) {
    return this.cache.isAlive && token === this.cache.loadToken;
  },

  safeSetData(patch, callback) {
    if (!this.cache.isAlive) return;
    this.setData(patch, callback);
  },

  onPullDownRefresh() {
    this.loadPage(true);
  },

  getWatchlist() {
    return normalizeWatchlist(wx.getStorageSync(WATCHLIST_STORAGE_KEY));
  },

  saveWatchlist(ids) {
    wx.setStorageSync(WATCHLIST_STORAGE_KEY, ids);
  },

  syncWatchState() {
    const watchlist = this.getWatchlist();
    this.safeSetData({ isWatched: watchlist.indexOf(this.data.indexId) >= 0 });
  },

  async loadPage(fromPullDown = false) {
    const loadToken = this.nextLoadToken();
    this.safeSetData({ loading: true });
    try {
      await Promise.all([this.loadMeta(loadToken), this.loadSeries(loadToken)]);
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    } finally {
      if (this.isLoadActive(loadToken)) this.safeSetData({ loading: false });
      if (fromPullDown && this.cache.isAlive) wx.stopPullDownRefresh();
    }
  },

  async loadMeta(loadToken) {
    const token = Number.isInteger(loadToken) ? loadToken : this.cache.loadToken;
    const payload = await request("/api/company/meta");
    if (!this.isLoadActive(token)) return;
    const target = (payload.indices || []).find((item) => item.id === this.data.indexId);
    if (!target) return;

    this.safeSetData({
      displayName: target.displayName || this.data.displayName,
      symbol: target.symbol || this.data.symbol,
      rankText: toNumberText(target.rank, 0),
      marketCapText: toMarketCapText(target.marketCap),
    });
  },

  async loadSeries(loadToken) {
    const hasToken = Number.isInteger(loadToken);
    const token = hasToken ? loadToken : this.nextLoadToken();
    if (!hasToken) this.safeSetData({ loading: true });

    const metric = this.data.metricOptions[this.data.metricIndex].value;
    const rangeCode = this.data.rangeOptions[this.data.rangeIndex].value;
    try {
      const payload = await request("/api/company/series", "GET", null, {
        indexId: this.data.indexId,
        metric,
      });
      if (!this.isLoadActive(token)) return;

      const rangedRows = filterRowsByRange(payload.rows || [], rangeCode);
      const latestRaw = rangedRows[rangedRows.length - 1] || null;
      const latest = latestRaw
        ? {
            ...latestRaw,
            valueText: toMetricText(latestRaw.value, metric),
            zScoreText: toNumberText(latestRaw.z_score_3y, 2),
            percentile5yText: toPercent(latestRaw.percentile_5y),
            percentile10yText: toPercent(latestRaw.percentile_10y),
            percentileFullText: toPercent(latestRaw.percentile_full),
          }
        : null;

      this.safeSetData({
        latest,
        availableRangeText: `${payload.availableRange.startDate} ~ ${payload.availableRange.endDate} (${payload.availableRange.pointCount})`,
        rangeSummaryText: `${rangedRows[0] ? rangedRows[0].date : "--"} ~ ${
          rangedRows[rangedRows.length - 1] ? rangedRows[rangedRows.length - 1].date : "--"
        } (${rangedRows.length})`,
        mainAxisLabels: buildAxisLabels(rangedRows),
      });
      await Promise.all([this.drawMainChart(rangedRows, token), this.drawPercentileChart(rangedRows, token)]);
    } catch (error) {
      if (this.isLoadActive(token)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    } finally {
      if (!hasToken && this.isLoadActive(token)) this.safeSetData({ loading: false });
    }
  },

  onMetricChange(event) {
    this.safeSetData({ metricIndex: Number(event.detail.value) }, () => this.loadSeries());
  },

  onMetricTap(event) {
    const nextIndex = Number(event.currentTarget.dataset.index);
    if (nextIndex === this.data.metricIndex) return;
    this.safeSetData({ metricIndex: nextIndex }, () => this.loadSeries());
  },

  onRangeTap(event) {
    const nextIndex = Number(event.currentTarget.dataset.index);
    if (nextIndex === this.data.rangeIndex) return;
    this.safeSetData({ rangeIndex: nextIndex }, () => this.loadSeries());
  },

  toggleWatch() {
    if (!this.data.indexId) return;

    const next = new Set(this.getWatchlist());
    const existed = next.has(this.data.indexId);
    if (existed) next.delete(this.data.indexId);
    else next.add(this.data.indexId);

    const nowWatched = !existed;
    this.saveWatchlist(Array.from(next));
    this.syncWatchState();
    wx.showToast({
      title: nowWatched ? "已关注" : "已取消关注",
      icon: "none",
    });
  },

  resolveCanvasRect(selector, fallbackHeightRatio) {
    if (!this.cache.isAlive) {
      const info = wx.getWindowInfo ? wx.getWindowInfo() : wx.getSystemInfoSync();
      const width = (Number(info.windowWidth || 375) * 700) / 750;
      return Promise.resolve({
        width,
        height: width * Number(fallbackHeightRatio || 320 / 700),
      });
    }

    if (
      this.cache.canvasRects[selector] &&
      this.cache.canvasRects[selector].width > 0 &&
      this.cache.canvasRects[selector].height > 0
    ) {
      return Promise.resolve(this.cache.canvasRects[selector]);
    }

    return new Promise((resolve) => {
      const query = wx.createSelectorQuery().in(this);
      query.select(selector).boundingClientRect((rect) => {
        if (!this.cache.isAlive) {
          const info = wx.getWindowInfo ? wx.getWindowInfo() : wx.getSystemInfoSync();
          const width = (Number(info.windowWidth || 375) * 700) / 750;
          resolve({
            width,
            height: width * Number(fallbackHeightRatio || 320 / 700),
          });
          return;
        }
        if (rect && rect.width > 0 && rect.height > 0) {
          this.cache.canvasRects[selector] = rect;
          resolve(rect);
          return;
        }
        const info = wx.getWindowInfo ? wx.getWindowInfo() : wx.getSystemInfoSync();
        const width = (Number(info.windowWidth || 375) * 700) / 750;
        const fallback = {
          width,
          height: (width * Number(fallbackHeightRatio || 320 / 700)),
        };
        this.cache.canvasRects[selector] = fallback;
        resolve(fallback);
      });
      query.exec();
    });
  },

  async drawMainChart(rows, loadToken) {
    const token = Number.isInteger(loadToken) ? loadToken : this.cache.loadToken;
    if (!this.isLoadActive(token)) return;
    const ctx = wx.createCanvasContext("lineCanvas", this);
    const rect = await this.resolveCanvasRect("#lineCanvas", 320 / 700);
    if (!this.isLoadActive(token)) return;
    const width = Math.max(240, Number(rect.width || 0));
    const height = Math.max(120, Number(rect.height || 0));
    const padding = Math.max(10, (width * 30) / 700);

    ctx.clearRect(0, 0, width, height);

    if (!Array.isArray(rows) || !rows.length) {
      ctx.draw();
      return;
    }

    const drawRows = buildRenderableRows(rows, Math.max(260, Math.floor(width * 1.8)));
    if (!drawRows.length) {
      ctx.draw();
      return;
    }
    const values = drawRows.map((item) => item.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(max - min, 1e-6);

    ctx.setStrokeStyle("#1ca294");
    ctx.setLineWidth(2);
    ctx.beginPath();

    drawRows.forEach((row, i) => {
      const x = padding + (i / Math.max(drawRows.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - ((row.value - min) / span) * (height - padding * 2);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    ctx.setStrokeStyle("rgba(70, 120, 150, 0.2)");
    ctx.setLineWidth(1);
    ctx.beginPath();
    ctx.moveTo(padding, height - padding);
    ctx.lineTo(width - padding, height - padding);
    ctx.stroke();

    const labelFont = Math.max(10, Math.round((width / 700) * 20));
    ctx.setFontSize(labelFont);
    ctx.setFillStyle("#8fb2c8");
    const minLabel = `min ${min.toFixed(2)}`;
    const maxLabel = `max ${max.toFixed(2)}`;
    ctx.fillText(minLabel, padding * 0.8, labelFont + 4);
    ctx.fillText(maxLabel, width - padding - maxLabel.length * labelFont * 0.52, labelFont + 4);

    if (!this.isLoadActive(token)) return;
    ctx.draw();
  },

  async drawPercentileChart(rows, loadToken) {
    const token = Number.isInteger(loadToken) ? loadToken : this.cache.loadToken;
    if (!this.isLoadActive(token)) return;
    const ctx = wx.createCanvasContext("percentileCanvas", this);
    const rect = await this.resolveCanvasRect("#percentileCanvas", 220 / 700);
    if (!this.isLoadActive(token)) return;
    const width = Math.max(240, Number(rect.width || 0));
    const height = Math.max(120, Number(rect.height || 0));
    const paddingX = Math.max(10, (width * 30) / 700);
    const paddingY = Math.max(12, (height * 18) / 220);

    ctx.clearRect(0, 0, width, height);

    if (!Array.isArray(rows) || !rows.length) {
      ctx.draw();
      return;
    }

    const drawRows = buildRenderableRows(rows, Math.max(220, Math.floor(width * 1.6))).filter((item) =>
      Number.isFinite(Number(item.percentile_full))
    );
    if (!drawRows.length) {
      ctx.draw();
      return;
    }

    const chartWidth = width - paddingX * 2;
    const chartHeight = height - paddingY * 2;
    const yByPercentile = (p) => paddingY + (1 - clamp(Number(p || 0), 0, 1)) * chartHeight;

    const yHigh = yByPercentile(0.85);
    const yLow = yByPercentile(0.15);
    const yTop = yByPercentile(1);
    const yBottom = yByPercentile(0);

    ctx.setFillStyle("rgba(255, 129, 152, 0.09)");
    ctx.fillRect(paddingX, yTop, chartWidth, Math.max(0, yHigh - yTop));
    ctx.setFillStyle("rgba(117, 160, 255, 0.08)");
    ctx.fillRect(paddingX, yHigh, chartWidth, Math.max(0, yLow - yHigh));
    ctx.setFillStyle("rgba(89, 211, 159, 0.09)");
    ctx.fillRect(paddingX, yLow, chartWidth, Math.max(0, yBottom - yLow));

    ctx.setStrokeStyle("rgba(179, 204, 242, 0.26)");
    ctx.setLineWidth(1);
    ctx.beginPath();
    ctx.moveTo(paddingX, yHigh);
    ctx.lineTo(width - paddingX, yHigh);
    ctx.moveTo(paddingX, yLow);
    ctx.lineTo(width - paddingX, yLow);
    ctx.stroke();

    ctx.setStrokeStyle("#7fa9ff");
    ctx.setLineWidth(2);
    ctx.beginPath();
    drawRows.forEach((row, i) => {
      const x = paddingX + (i / Math.max(drawRows.length - 1, 1)) * chartWidth;
      const y = yByPercentile(row.percentile_full);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    const last = drawRows[drawRows.length - 1];
    const lastY = yByPercentile(last.percentile_full);
    ctx.setFillStyle("#cbe0ff");
    ctx.beginPath();
    ctx.arc(width - paddingX, lastY, 3, 0, Math.PI * 2);
    ctx.fill();

    const labelFont = Math.max(9, Math.round((width / 700) * 18));
    ctx.setFontSize(labelFont);
    ctx.setFillStyle("#9db9d4");
    ctx.fillText("100%", paddingX, labelFont);
    ctx.fillText("50%", paddingX, yByPercentile(0.5) - 2);
    ctx.fillText("0%", paddingX, yBottom - 2);
    ctx.fillText("85%", width - paddingX - labelFont * 2.3, yHigh - 2);
    ctx.fillText("15%", width - paddingX - labelFont * 2.3, yLow - 2);
    if (!this.isLoadActive(token)) return;
    ctx.draw();
  },
});
