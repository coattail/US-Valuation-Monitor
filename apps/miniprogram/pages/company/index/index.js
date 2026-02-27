const { request } = require("../../../utils/api");

const WATCHLIST_STORAGE_KEY = "usvm-company-watchlist";
const DEFAULT_VISIBLE_COUNT = 20;

const GROUP_OPTIONS = [
  { label: "全部公司", value: "all" },
  { label: "仅关注", value: "watchlist" },
];

const SORT_OPTIONS = [
  { label: "市值从高到低", value: "market_cap_desc" },
  { label: "10Y分位从高到低", value: "percentile_desc" },
  { label: "10Y分位从低到高", value: "percentile_asc" },
  { label: "PE从高到低", value: "pe_desc" },
  { label: "PB从高到低", value: "pb_desc" },
  { label: "名称A-Z", value: "name" },
];

function toPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function toNumberText(value, digits) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "--";
}

function toMarketCapText(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "--";
  if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  return `$${n.toFixed(0)}`;
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

function formatSignedPercent(value) {
  const n = Number(value || 0) * 100;
  if (!Number.isFinite(n)) return "--";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(1)}%`;
}

function toCompanyLogoUrl(symbol) {
  const normalized = String(symbol || "").trim().toUpperCase();
  if (!normalized) return "";
  return `https://companiesmarketcap.com/img/company-logos/64/${encodeURIComponent(normalized)}.png`;
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
    generatedAt: "",
    loading: true,
    groupOptions: GROUP_OPTIONS,
    sortOptions: SORT_OPTIONS,
    groupIndex: 0,
    sortIndex: 0,
    keyword: "",
    rows: [],
    shownCount: 0,
    totalCount: 0,
    watchCount: 0,
    canLoadMore: false,
  },

  cache: {
    allRows: [],
    watchlistIds: [],
    logoErrorIds: new Set(),
    visibleCount: DEFAULT_VISIBLE_COUNT,
    loadedAt: 0,
    loading: false,
    isAlive: false,
    loadToken: 0,
  },

  onLoad() {
    this.cache.isAlive = true;
  },

  syncTabBarSelected() {
    const tabBar = this.getTabBar && this.getTabBar();
    if (tabBar && tabBar.setData) {
      tabBar.setData({ selected: 1 });
    }
  },

  onShow() {
    this.cache.isAlive = true;
    this.syncTabBarSelected();
    const cacheAge = Date.now() - Number(this.cache.loadedAt || 0);
    if (this.cache.allRows.length && cacheAge < 60_000) {
      this.applyFilters();
      return;
    }
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

  getWatchlist() {
    return normalizeWatchlist(wx.getStorageSync(WATCHLIST_STORAGE_KEY));
  },

  saveWatchlist(ids) {
    wx.setStorageSync(WATCHLIST_STORAGE_KEY, ids);
  },

  async loadData(fromPullDown = false) {
    if (this.cache.loading) return;
    const loadToken = this.nextLoadToken();
    this.cache.loading = true;
    this.safeSetData({ loading: true });
    try {
      const payload = await request("/api/company/snapshot");
      if (!this.isLoadActive(loadToken)) return;
      const watchlistIds = this.getWatchlist();
      const watchSet = new Set(watchlistIds);

      this.cache.allRows = (payload.rows || []).map((item) => ({
        ...item,
        marketCapText: toMarketCapText(item.marketCap),
        peTtmText: toNumberText(item.pe_ttm, 2),
        peForwardText: toNumberText(item.pe_forward, 2),
        pbText: toNumberText(item.pb, 2),
        percentile10yText: toPercent(item.percentile_10y),
        percentilePinStyle: `left:${clamp(Number(item.percentile_10y || 0) * 100, 0, 100).toFixed(2)}%;`,
        percentileStyle: `color:${percentileColor(item.percentile_10y)};`,
        peChangeText: formatSignedPercent(item.pe_ttm_change_1y),
        peChangeClass: Number(item.pe_ttm_change_1y || 0) >= 0 ? "up" : "down",
        regimeText: regimeLabel(item.regime),
        regimeClass: regimeClass(item.regime),
        cardToneClass: toneClassByPercentile(item.percentile_10y),
        logoUrl: toCompanyLogoUrl(item.symbol),
        logoFailed: this.cache.logoErrorIds.has(item.indexId),
        watermarkText: String(item.symbol || "").toUpperCase(),
        watched: watchSet.has(item.indexId),
      }));
      this.cache.watchlistIds = watchlistIds;
      this.cache.visibleCount = DEFAULT_VISIBLE_COUNT;
      this.cache.loadedAt = Date.now();
      this.safeSetData({
        generatedAt: payload.generatedAt || "",
        watchCount: watchlistIds.length,
      });
      this.applyFilters();
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    } finally {
      this.cache.loading = false;
      if (this.isLoadActive(loadToken)) this.safeSetData({ loading: false });
      if (fromPullDown && this.cache.isAlive) wx.stopPullDownRefresh();
    }
  },

  applyFilters() {
    if (!this.cache.isAlive) return;
    const groupValue = this.data.groupOptions[this.data.groupIndex].value;
    const sortValue = this.data.sortOptions[this.data.sortIndex].value;
    const keyword = String(this.data.keyword || "").trim().toLowerCase();
    const isSearching = keyword.length > 0;

    const rows = this.cache.allRows.filter((row) => {
      if (groupValue === "watchlist" && this.cache.watchlistIds.indexOf(row.indexId) < 0) return false;
      if (!keyword) return true;
      const text = `${row.displayName} ${row.symbol} ${row.indexId}`.toLowerCase();
      return text.indexOf(keyword) >= 0;
    });

    rows.sort((a, b) => {
      if (sortValue === "percentile_asc") return Number(a.percentile_10y || 0) - Number(b.percentile_10y || 0);
      if (sortValue === "pe_desc") return Number(b.pe_ttm || 0) - Number(a.pe_ttm || 0);
      if (sortValue === "pb_desc") return Number(b.pb || 0) - Number(a.pb || 0);
      if (sortValue === "name") return String(a.displayName || "").localeCompare(String(b.displayName || ""));
      if (sortValue === "percentile_desc") return Number(b.percentile_10y || 0) - Number(a.percentile_10y || 0);
      return Number(b.marketCap || 0) - Number(a.marketCap || 0);
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

  onLogoError(event) {
    const indexId = String((event.currentTarget && event.currentTarget.dataset && event.currentTarget.dataset.indexId) || "");
    if (!indexId || this.cache.logoErrorIds.has(indexId)) return;

    this.cache.logoErrorIds.add(indexId);
    this.cache.allRows = this.cache.allRows.map((row) => {
      if (row.indexId !== indexId) return row;
      return {
        ...row,
        logoFailed: true,
      };
    });
    this.applyFilters();
  },

  toggleWatch(event) {
    const indexId = event.currentTarget.dataset.indexId;
    if (!indexId) return;

    const next = new Set(this.cache.watchlistIds);
    if (next.has(indexId)) next.delete(indexId);
    else next.add(indexId);
    this.cache.watchlistIds = Array.from(next);
    this.saveWatchlist(this.cache.watchlistIds);

    const watchSet = new Set(this.cache.watchlistIds);
    this.cache.allRows = this.cache.allRows.map((row) => ({
      ...row,
      watched: watchSet.has(row.indexId),
    }));
    this.safeSetData({ watchCount: this.cache.watchlistIds.length });
    this.applyFilters();
  },

  openDetail(event) {
    const indexId = event.currentTarget.dataset.indexId;
    const displayName = encodeURIComponent(String(event.currentTarget.dataset.displayName || ""));
    const symbol = encodeURIComponent(String(event.currentTarget.dataset.symbol || ""));
    wx.navigateTo({
      url: `/pages/company/detail/detail?indexId=${indexId}&displayName=${displayName}&symbol=${symbol}`,
    });
  },
});
