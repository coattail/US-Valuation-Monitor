const { request } = require("../../utils/api");

const METRIC_OPTIONS = [
  { label: "PE(TTM)", value: "pe_ttm" },
  { label: "PE(Forward)", value: "pe_forward" },
  { label: "PB", value: "pb" },
  { label: "Earnings Yield", value: "earnings_yield" },
];

function toPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

Page({
  data: {
    indexId: "sp500",
    metricIndex: 0,
    metricOptions: METRIC_OPTIONS,
    availableRangeText: "",
    rows: [],
    latest: null,
  },

  onLoad(query) {
    if (query.indexId) {
      this.setData({ indexId: query.indexId });
    }
  },

  onShow() {
    this.loadSeries();
  },

  async loadSeries() {
    const metric = this.data.metricOptions[this.data.metricIndex].value;
    try {
      const payload = await request(`/api/series?indexId=${this.data.indexId}&metric=${metric}`);
      const latestRaw = payload.rows[payload.rows.length - 1] || null;
      const latest = latestRaw
        ? {
            ...latestRaw,
            percentile5yText: toPercent(latestRaw.percentile_5y),
            percentile10yText: toPercent(latestRaw.percentile_10y),
            percentileFullText: toPercent(latestRaw.percentile_full),
          }
        : null;

      this.setData({
        rows: payload.rows,
        latest,
        availableRangeText: `${payload.availableRange.startDate} ~ ${payload.availableRange.endDate} (${payload.availableRange.pointCount})`,
      });
      this.drawChart(payload.rows);
    } catch (error) {
      wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    }
  },

  onMetricChange(event) {
    this.setData({ metricIndex: Number(event.detail.value) }, () => {
      this.loadSeries();
    });
  },

  drawChart(rows) {
    if (!rows.length) return;

    const ctx = wx.createCanvasContext("lineCanvas", this);
    const width = 700;
    const height = 320;
    const padding = 30;

    const values = rows.map((item) => Number(item.value));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(max - min, 1e-6);

    ctx.setStrokeStyle("#1ca294");
    ctx.setLineWidth(3);

    rows.forEach((row, i) => {
      const x = padding + (i / Math.max(rows.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - ((row.value - min) / span) * (height - padding * 2);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    ctx.setStrokeStyle("rgba(70, 120, 150, 0.2)");
    ctx.setLineWidth(1);
    ctx.moveTo(padding, height - padding);
    ctx.lineTo(width - padding, height - padding);
    ctx.stroke();

    ctx.setFontSize(20);
    ctx.setFillStyle("#48677c");
    ctx.fillText(`min ${min.toFixed(2)}`, 24, 24);
    ctx.fillText(`max ${max.toFixed(2)}`, width - 130, 24);

    ctx.draw();
  },
});
