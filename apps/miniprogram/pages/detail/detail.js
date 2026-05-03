const { request } = require("../../utils/api");

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

function regimeLabel(regime) {
  if (regime === "high") return "高估";
  if (regime === "low") return "低估";
  return "中性";
}

function regimeByPercentile(percentile) {
  const p = Number(percentile);
  if (!Number.isFinite(p)) return "中性";
  if (p >= 0.85) return "高估";
  if (p <= 0.15) return "低估";
  return "中性";
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

function attachRangePercentiles(rows) {
  const values = rows.map((row) => Number(row.value)).filter((value) => Number.isFinite(value));
  if (!values.length) {
    return rows.map((row) => ({
      ...row,
      percentile_range: 0.5,
    }));
  }

  const sorted = Array.from(new Set(values)).sort((a, b) => a - b);
  const rankMap = new Map();
  sorted.forEach((value, index) => rankMap.set(value, index + 1));

  const bit = new Array(sorted.length + 2).fill(0);
  const update = (index, delta) => {
    let i = index;
    while (i < bit.length) {
      bit[i] += delta;
      i += i & -i;
    }
  };
  const query = (index) => {
    let i = index;
    let sum = 0;
    while (i > 0) {
      sum += bit[i];
      i -= i & -i;
    }
    return sum;
  };

  let seen = 0;
  return rows.map((row) => {
    const value = Number(row.value);
    if (!Number.isFinite(value)) {
      return {
        ...row,
        percentile_range: 0.5,
      };
    }
    const rank = rankMap.get(value) || 1;
    const lessOrEqualBefore = query(rank);
    const percentile = clamp((lessOrEqualBefore + 1) / (seen + 1), 0, 1);
    update(rank, 1);
    seen += 1;
    return {
      ...row,
      percentile_range: percentile,
    };
  });
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

Page({
  data: {
    indexId: "sp500",
    displayName: "",
    metricIndex: 0,
    metricOptions: METRIC_OPTIONS,
    rangeIndex: DEFAULT_RANGE_INDEX >= 0 ? DEFAULT_RANGE_INDEX : 0,
    rangeOptions: RANGE_OPTIONS,
    availableRangeText: "",
    rangeSummaryText: "",
    mainAxisLabels: ["--", "--", "--"],
    rows: [],
    latest: null,
    focusCard: null,
    focusActive: false,
    loading: true,
  },

  cache: {
    canvasRects: {},
    rows: [],
    mainDrawRows: [],
    mainChartGeom: null,
    focusDate: "",
    touchBusy: false,
    isAlive: false,
    loadToken: 0,
  },

  onLoad(query) {
    this.cache.isAlive = true;
    const patch = {};
    if (query.indexId) patch.indexId = query.indexId;
    if (query.displayName) patch.displayName = decodeURIComponent(query.displayName);
    if (Object.keys(patch).length) this.setData(patch);
  },

  onShow() {
    this.cache.isAlive = true;
    this.loadSeries();
  },

  onHide() {
    this.cache.isAlive = false;
    this.cache.touchBusy = false;
  },

  onUnload() {
    this.cache.isAlive = false;
    this.cache.touchBusy = false;
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

  buildFocusCard(row, metric) {
    if (!row) return null;
    return {
      date: row.date || "--",
      valueText: toMetricText(row.value, metric),
      percentileText: toPercent(row.percentile_range),
      zScoreText: toNumberText(row.z_score_3y, 2),
      regimeText: regimeByPercentile(row.percentile_range),
    };
  },

  async loadSeries() {
    const loadToken = this.nextLoadToken();
    this.safeSetData({ loading: true });
    const metric = this.data.metricOptions[this.data.metricIndex].value;
    const rangeCode = this.data.rangeOptions[this.data.rangeIndex].value;

    try {
      const payload = await request("/api/series", "GET", null, {
        indexId: this.data.indexId,
        metric,
      });
      if (!this.isLoadActive(loadToken)) return;

      const rangedRows = attachRangePercentiles(filterRowsByRange(payload.rows || [], rangeCode));
      const latestRaw = rangedRows[rangedRows.length - 1] || null;
      const latest = latestRaw
        ? {
            ...latestRaw,
            valueText: toMetricText(latestRaw.value, metric),
            zScoreText: toNumberText(latestRaw.z_score_3y, 2),
            percentileRangeText: toPercent(latestRaw.percentile_range),
            percentile5yText: toPercent(latestRaw.percentile_5y),
            percentile10yText: toPercent(latestRaw.percentile_10y),
            percentileFullText: toPercent(latestRaw.percentile_full),
          }
        : null;

      this.cache.rows = rangedRows;
      this.cache.focusDate = latestRaw ? latestRaw.date : "";
      this.safeSetData({
        rows: rangedRows,
        latest,
        focusCard: this.buildFocusCard(latestRaw, metric),
        focusActive: false,
        availableRangeText: `${payload.availableRange.startDate} ~ ${payload.availableRange.endDate} (${payload.availableRange.pointCount})`,
        rangeSummaryText: `${rangedRows[0] ? rangedRows[0].date : "--"} ~ ${
          rangedRows[rangedRows.length - 1] ? rangedRows[rangedRows.length - 1].date : "--"
        } (${rangedRows.length})`,
        mainAxisLabels: buildAxisLabels(rangedRows),
      });

      await Promise.all([this.drawMainChart(rangedRows, loadToken), this.drawPercentileChart(rangedRows, loadToken)]);
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    } finally {
      if (this.isLoadActive(loadToken)) this.safeSetData({ loading: false });
    }
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

  resetFocusToLatest() {
    const rows = this.cache.rows || [];
    if (!rows.length) return;
    const metric = this.data.metricOptions[this.data.metricIndex].value;
    const latest = rows[rows.length - 1];
    this.cache.focusDate = latest.date;
    this.safeSetData(
      {
        focusCard: this.buildFocusCard(latest, metric),
        focusActive: false,
      },
      () => {
        this.drawMainChart(rows, this.cache.loadToken);
        this.drawPercentileChart(rows, this.cache.loadToken);
      }
    );
  },

  onMainChartTouchStart(event) {
    this.updateFocusFromTouch(event);
  },

  onMainChartTouchMove(event) {
    this.updateFocusFromTouch(event);
  },

  async updateFocusFromTouch(event) {
    if (this.cache.touchBusy) return;
    const touch = (event.touches && event.touches[0]) || (event.changedTouches && event.changedTouches[0]);
    if (!touch) return;

    const rows = this.cache.mainDrawRows;
    const geom = this.cache.mainChartGeom;
    if (!rows || !rows.length || !geom) return;

    this.cache.touchBusy = true;
    try {
      const rect = await this.resolveCanvasRect("#lineCanvas", 320 / 700);
      if (!this.cache.isAlive) return;
      const localX = Number(touch.x || 0) - Number(rect.left || 0);
      const ratio = clamp((localX - geom.padding) / Math.max(1, geom.width - geom.padding * 2), 0, 1);
      const idx = Math.round(ratio * (rows.length - 1));
      const picked = rows[idx] || rows[rows.length - 1];
      if (!picked) return;
      if (this.cache.focusDate === picked.date && this.data.focusActive) return;

      this.cache.focusDate = picked.date;
      const metric = this.data.metricOptions[this.data.metricIndex].value;
      this.safeSetData(
        {
          focusCard: this.buildFocusCard(picked, metric),
          focusActive: true,
        },
        () => {
          this.drawMainChart(this.cache.rows, this.cache.loadToken);
          this.drawPercentileChart(this.cache.rows, this.cache.loadToken);
        }
      );
    } finally {
      this.cache.touchBusy = false;
    }
  },

  resolveCanvasRect(selector, fallbackHeightRatio) {
    if (!this.cache.isAlive) {
      const info = wx.getWindowInfo ? wx.getWindowInfo() : wx.getSystemInfoSync();
      const width = (Number(info.windowWidth || 375) * 700) / 750;
      return Promise.resolve({
        left: 0,
        top: 0,
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
            left: 0,
            top: 0,
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
          left: 0,
          top: 0,
          width,
          height: width * Number(fallbackHeightRatio || 320 / 700),
        };
        this.cache.canvasRects[selector] = fallback;
        resolve(fallback);
      });
      query.exec();
    });
  },

  resolveFocusedRowOnDraw(drawRows) {
    if (!Array.isArray(drawRows) || !drawRows.length) return null;
    const focusDate = this.cache.focusDate;
    if (!focusDate) return drawRows[drawRows.length - 1];
    const byDate = drawRows.find((item) => item.date === focusDate);
    return byDate || drawRows[drawRows.length - 1];
  },

  async drawMainChart(rows, loadToken) {
    const token = Number.isInteger(loadToken) ? loadToken : this.cache.loadToken;
    if (!this.isLoadActive(token)) return;
    const ctx = wx.createCanvasContext("lineCanvas", this);
    const rect = await this.resolveCanvasRect("#lineCanvas", 320 / 700);
    if (!this.isLoadActive(token)) return;
    const width = Math.max(240, Number(rect.width || 0));
    const height = Math.max(120, Number(rect.height || 0));
    const padding = Math.max(12, (width * 32) / 700);

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

    this.cache.mainDrawRows = drawRows;
    this.cache.mainChartGeom = { width, height, padding };

    const values = drawRows.map((item) => item.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(max - min, 1e-6);

    const xOf = (index) => padding + (index / Math.max(drawRows.length - 1, 1)) * (width - padding * 2);
    const yOfValue = (value) => height - padding - ((value - min) / span) * (height - padding * 2);

    ctx.setStrokeStyle("#1ca294");
    ctx.setLineWidth(2);
    ctx.beginPath();
    drawRows.forEach((row, i) => {
      const x = xOf(i);
      const y = yOfValue(row.value);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    ctx.setStrokeStyle("rgba(70, 120, 150, 0.22)");
    ctx.setLineWidth(1);
    ctx.beginPath();
    ctx.moveTo(padding, height - padding);
    ctx.lineTo(width - padding, height - padding);
    ctx.stroke();

    const tickIndices = [0, Math.floor((drawRows.length - 1) / 2), drawRows.length - 1];
    ctx.setStrokeStyle("rgba(120, 156, 196, 0.4)");
    tickIndices.forEach((idx) => {
      const x = xOf(idx);
      ctx.beginPath();
      ctx.moveTo(x, height - padding);
      ctx.lineTo(x, height - padding + 4);
      ctx.stroke();
    });

    const focused = this.resolveFocusedRowOnDraw(drawRows);
    if (focused) {
      const focusIndex = drawRows.findIndex((item) => item.date === focused.date);
      const focusX = xOf(focusIndex >= 0 ? focusIndex : drawRows.length - 1);
      const focusY = yOfValue(focused.value);

      ctx.setStrokeStyle("rgba(169, 206, 255, 0.45)");
      ctx.setLineWidth(1);
      ctx.beginPath();
      ctx.moveTo(focusX, padding * 0.55);
      ctx.lineTo(focusX, height - padding);
      ctx.stroke();

      ctx.setFillStyle("#b2d8ff");
      ctx.beginPath();
      ctx.arc(focusX, focusY, 3.5, 0, Math.PI * 2);
      ctx.fill();
    }

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
    const paddingX = Math.max(12, (width * 32) / 700);
    const paddingY = Math.max(12, (height * 18) / 220);

    ctx.clearRect(0, 0, width, height);
    if (!Array.isArray(rows) || !rows.length) {
      ctx.draw();
      return;
    }

    const drawRows = buildRenderableRows(rows, Math.max(220, Math.floor(width * 1.6))).filter((item) =>
      Number.isFinite(Number(item.percentile_range))
    );
    if (!drawRows.length) {
      ctx.draw();
      return;
    }

    const chartWidth = width - paddingX * 2;
    const chartHeight = height - paddingY * 2;
    const yByPercentile = (p) => paddingY + (1 - clamp(Number(p || 0), 0, 1)) * chartHeight;
    const xOf = (index) => paddingX + (index / Math.max(drawRows.length - 1, 1)) * chartWidth;

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

    ctx.setStrokeStyle("rgba(179, 204, 242, 0.28)");
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
      const x = xOf(i);
      const y = yByPercentile(row.percentile_range);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    const focused = this.resolveFocusedRowOnDraw(drawRows);
    if (focused) {
      const focusIndex = drawRows.findIndex((item) => item.date === focused.date);
      const focusX = xOf(focusIndex >= 0 ? focusIndex : drawRows.length - 1);
      const focusY = yByPercentile(focused.percentile_range);

      ctx.setStrokeStyle("rgba(169, 206, 255, 0.45)");
      ctx.setLineWidth(1);
      ctx.beginPath();
      ctx.moveTo(focusX, paddingY);
      ctx.lineTo(focusX, yBottom);
      ctx.stroke();

      ctx.setFillStyle("#cbe0ff");
      ctx.beginPath();
      ctx.arc(focusX, focusY, 3.2, 0, Math.PI * 2);
      ctx.fill();
    }

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
