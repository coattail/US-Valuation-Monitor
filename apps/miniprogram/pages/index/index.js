const { request } = require("../../utils/api");

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
const THEME_ATTENTION_ORDER = ["igv", "soxx", "smh", "dram"];
const DEFAULT_VISIBLE_COUNT = 8;

const GROUP_OPTIONS = [
  { label: "全部", value: "all" },
  { label: "核心指数", value: "core" },
  { label: "行业指数", value: "sector" },
  { label: "主题指数", value: "theme" },
  { label: "仅自选", value: "watchlist" },
];

const SORT_OPTIONS = [
  { label: "市场热度", value: "attention" },
  { label: "分位从高到低", value: "percentile_desc" },
  { label: "分位从低到高", value: "percentile_asc" },
  { label: "PE从高到低", value: "pe_desc" },
  { label: "PB从高到低", value: "pb_desc" },
  { label: "名称A-Z", value: "name" },
];

function toPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function toNumberText(value, digits) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : "--";
}

function attentionRank(row) {
  const groupRank = { core: 0, sector: 1, theme: 2 };
  const group = groupRank[row.group] === undefined ? 999 : groupRank[row.group];
  const coreRank = CORE_ATTENTION_ORDER.indexOf(row.indexId);
  const sectorRank = SECTOR_ATTENTION_ORDER.indexOf(row.indexId);
  const themeRank = THEME_ATTENTION_ORDER.indexOf(row.indexId);
  const withinGroupRank = row.group === "core" ? coreRank : row.group === "sector" ? sectorRank : themeRank;
  const rank = withinGroupRank >= 0 ? withinGroupRank : 999;
  return group * 1000 + rank;
}

function groupLabel(group) {
  if (group === "core") return "核心指数";
  if (group === "sector") return "行业指数";
  if (group === "theme") return "主题指数";
  return "其他指数";
}

function regimeLabel(regime) {
  if (regime === "high") return "高估";
  if (regime === "low") return "低估";
  return "中性";
}

function regimeClass(regime) {
  if (regime === "high") return "danger";
  if (regime === "low") return "safe";
  return "normal";
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function toneClassByPercentile(percentile) {
  const p = Number(percentile || 0);
  if (p >= 0.85) return "tone-hot";
  if (p <= 0.15) return "tone-low";
  return "";
}

function percentileColor(percentile) {
  const p = clamp(Number(percentile || 0), 0, 1);
  const hue = (1 - p) * 130;
  return `hsl(${hue}, 68%, 74%)`;
}

Page({
  data: {
    generatedAt: "",
    loading: true,
    groupIndex: 0,
    sortIndex: 0,
    keyword: "",
    groupOptions: GROUP_OPTIONS,
    sortOptions: SORT_OPTIONS,
    rows: [],
    totalCount: 0,
    shownCount: 0,
    canLoadMore: false,
  },

  cache: {
    allRows: [],
    watchlistIds: [],
    visibleCount: DEFAULT_VISIBLE_COUNT,
    prefetchingCompanySnapshot: false,
    prefetchedCompanySnapshot: false,
    isAlive: false,
    loadToken: 0,
  },

  onLoad() {
    this.cache.isAlive = true;
  },

  syncTabBarSelected() {
    const tabBar = this.getTabBar && this.getTabBar();
    if (tabBar && tabBar.setData) {
      tabBar.setData({ selected: 0 });
    }
  },

  onShow() {
    this.cache.isAlive = true;
    this.syncTabBarSelected();
    this.loadData();
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
    this.loadData(true);
  },

  async loadData(fromPullDown = false) {
    const loadToken = this.nextLoadToken();
    this.safeSetData({ loading: true });
    try {
      const [snapshot, watchlist] = await Promise.all([request("/api/snapshot"), request("/api/watchlist")]);
      if (!this.isLoadActive(loadToken)) return;
      const allRows = (snapshot.rows || []).map((item) => ({
        ...item,
        peTtmText: toNumberText(item.pe_ttm, 2),
        peForwardText: toNumberText(item.pe_forward, 2),
        pbText: toNumberText(item.pb, 2),
        percentileText: toPercent(item.percentile_full),
        percentilePinStyle: `left:${clamp(Number(item.percentile_full || 0) * 100, 0, 100).toFixed(2)}%;`,
        percentileStyle: `color:${percentileColor(item.percentile_full)};`,
        groupText: groupLabel(item.group),
        regimeText: regimeLabel(item.regime),
        regimeClass: regimeClass(item.regime),
        cardToneClass: toneClassByPercentile(item.percentile_full),
        watermarkText: String(item.symbol || "").toUpperCase(),
      }));

      this.cache.allRows = allRows;
      this.cache.watchlistIds = Array.isArray(watchlist.watchIndexIds) ? watchlist.watchIndexIds : [];
      this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
      this.safeSetData({
        generatedAt: snapshot.generatedAt || "",
      });
      this.applyFilters();
      this.prefetchCompanySnapshot();
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    } finally {
      if (this.isLoadActive(loadToken)) this.safeSetData({ loading: false });
      if (fromPullDown && this.cache.isAlive) {
        wx.stopPullDownRefresh();
      }
    }
  },

  applyFilters() {
    if (!this.cache.isAlive) return;
    const groupValue = this.data.groupOptions[this.data.groupIndex].value;
    const sortValue = this.data.sortOptions[this.data.sortIndex].value;
    const keyword = String(this.data.keyword || "").trim().toLowerCase();
    const isSearching = keyword.length > 0;

    const rows = this.cache.allRows.filter((row) => {
      if (groupValue === "core" && row.group !== "core") return false;
      if (groupValue === "sector" && row.group !== "sector") return false;
      if (groupValue === "theme" && row.group !== "theme") return false;
      if (groupValue === "watchlist" && this.cache.watchlistIds.indexOf(row.indexId) < 0) return false;

      if (!keyword) return true;
      const text = `${row.displayName} ${row.symbol} ${row.indexId}`.toLowerCase();
      return text.indexOf(keyword) >= 0;
    });

    rows.sort((a, b) => {
      if (sortValue === "attention") {
        const rankDiff = attentionRank(a) - attentionRank(b);
        if (rankDiff !== 0) return rankDiff;
        return String(a.displayName || "").localeCompare(String(b.displayName || ""));
      }
      if (sortValue === "percentile_asc") return Number(a.percentile_full || 0) - Number(b.percentile_full || 0);
      if (sortValue === "pe_desc") return Number(b.pe_ttm || 0) - Number(a.pe_ttm || 0);
      if (sortValue === "pb_desc") return Number(b.pb || 0) - Number(a.pb || 0);
      if (sortValue === "name") return String(a.displayName || "").localeCompare(String(b.displayName || ""));
      return Number(b.percentile_full || 0) - Number(a.percentile_full || 0);
    });

    const shownRows = isSearching ? rows : rows.slice(0, this.cache.visibleCount);
    this.safeSetData({
      rows: shownRows,
      totalCount: rows.length,
      shownCount: shownRows.length,
      canLoadMore: !isSearching && shownRows.length < rows.length,
    });
  },

  onGroupChange(event) {
    this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
    this.safeSetData({ groupIndex: Number(event.detail.value) }, () => this.applyFilters());
  },

  onSortChange(event) {
    this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
    this.safeSetData({ sortIndex: Number(event.detail.value) }, () => this.applyFilters());
  },

  onKeywordInput(event) {
    this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
    this.safeSetData({ keyword: event.detail.value || "" }, () => this.applyFilters());
  },

  clearKeyword() {
    this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
    this.safeSetData({ keyword: "" }, () => this.applyFilters());
  },

  loadMore() {
    this.cache.visibleCount += DEFAULT_VISIBLE_COUNT;
    this.applyFilters();
  },

  async prefetchCompanySnapshot() {
    if (this.cache.prefetchedCompanySnapshot || this.cache.prefetchingCompanySnapshot) return;
    this.cache.prefetchingCompanySnapshot = true;
    try {
      await request("/api/company/snapshot");
      this.cache.prefetchedCompanySnapshot = true;
    } catch (error) {
      console.error(error);
    } finally {
      this.cache.prefetchingCompanySnapshot = false;
    }
  },

  openDetail(event) {
    const indexId = event.currentTarget.dataset.indexId;
    const displayName = encodeURIComponent(String(event.currentTarget.dataset.displayName || ""));
    wx.navigateTo({
      url: `/pages/detail/detail?indexId=${indexId}&displayName=${displayName}`,
    });
  },
});
