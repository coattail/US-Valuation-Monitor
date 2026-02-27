const { request, setApiBase, getApiBaseConfig, probeApiConnection } = require("../../utils/api");
const COMPANY_WATCHLIST_STORAGE_KEY = "usvm-company-watchlist";

function markChecked(allIndices, selectedIds) {
  const selected = new Set(selectedIds || []);
  return (allIndices || []).map((item) => ({
    ...item,
    checked: selected.has(item.id),
  }));
}

function normalizeStringIdList(raw) {
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

function normalizeCompanyItemsFromMeta(payload) {
  const indices = payload && Array.isArray(payload.indices) ? payload.indices : [];
  return indices.map((item) => ({
    id: item.id,
    displayName: item.displayName,
    symbol: item.symbol,
    rank: Number(item.rank || 9999),
  }));
}

function normalizeCompanyItemsFromSnapshot(payload) {
  const rows = payload && Array.isArray(payload.rows) ? payload.rows : [];
  return rows.map((item) => ({
    id: item.indexId,
    displayName: item.displayName,
    symbol: item.symbol,
    rank: Number(item.rank || 9999),
  }));
}

Page({
  data: {
    upper: 85,
    lower: 15,
    cooldownTradingDays: 5,
    allIndices: [],
    allCompanies: [],
    selectedIds: [],
    selectedCompanyIds: [],
    apiBaseInput: "",
    apiBaseDefault: "",
  },

  cache: {
    isAlive: false,
    loadToken: 0,
  },

  onLoad() {
    this.cache.isAlive = true;
  },

  syncTabBarSelected() {
    const tabBar = this.getTabBar && this.getTabBar();
    if (tabBar && tabBar.setData) {
      tabBar.setData({ selected: 3 });
    }
  },

  onShow() {
    this.cache.isAlive = true;
    this.syncTabBarSelected();
    this.loadConfig();
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

  async loadConfig() {
    const loadToken = this.nextLoadToken();
    try {
      const [metaResult, watchlistResult, companyMetaResult] = await Promise.allSettled([
        request("/api/meta"),
        request("/api/watchlist"),
        request("/api/company/meta"),
      ]);

      if (metaResult.status !== "fulfilled") throw metaResult.reason;
      if (watchlistResult.status !== "fulfilled") throw watchlistResult.reason;

      if (!this.isLoadActive(loadToken)) return;

      const meta = metaResult.value || {};
      const watchlist = watchlistResult.value || {};
      const companyMeta = companyMetaResult.status === "fulfilled" ? companyMetaResult.value || {} : { indices: [] };
      if (companyMetaResult.status !== "fulfilled") {
        console.error(companyMetaResult.reason);
      }

      const selectedIds = normalizeStringIdList(watchlist.watchIndexIds);
      const selectedCompanyIds = normalizeStringIdList(wx.getStorageSync(COMPANY_WATCHLIST_STORAGE_KEY));
      let companyItems = normalizeCompanyItemsFromMeta(companyMeta);
      if (!companyItems.length) {
        try {
          const companySnapshot = await request("/api/company/snapshot");
          companyItems = normalizeCompanyItemsFromSnapshot(companySnapshot);
        } catch (fallbackError) {
          console.error(fallbackError);
        }
      }

      if (!this.isLoadActive(loadToken)) return;

      const allCompanies = companyItems
        .sort((a, b) => {
          const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
          if (rankDiff !== 0) return rankDiff;
          return String(a.displayName || "").localeCompare(String(b.displayName || ""));
        });

      const apiBaseConfig = getApiBaseConfig();
      this.safeSetData({
        allIndices: markChecked(meta.indices, selectedIds),
        selectedIds,
        allCompanies: markChecked(allCompanies, selectedCompanyIds),
        selectedCompanyIds,
        upper: Number(watchlist.alertRule && watchlist.alertRule.upper) || 85,
        lower: Number(watchlist.alertRule && watchlist.alertRule.lower) || 15,
        cooldownTradingDays: Number(watchlist.alertRule && watchlist.alertRule.cooldownTradingDays) || 5,
        apiBaseInput: apiBaseConfig.apiBase,
        apiBaseDefault: apiBaseConfig.defaultApiBase,
      });
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    }
  },

  onUpperChange(event) {
    this.safeSetData({ upper: Number(event.detail.value) });
  },

  onLowerChange(event) {
    this.safeSetData({ lower: Number(event.detail.value) });
  },

  onCooldownInput(event) {
    this.safeSetData({ cooldownTradingDays: Number(event.detail.value) || 1 });
  },

  onWatchlistChange(event) {
    const selectedIds = event.detail.value;
    this.safeSetData({
      selectedIds,
      allIndices: markChecked(this.data.allIndices, selectedIds),
    });
  },

  onCompanyWatchlistChange(event) {
    const selectedCompanyIds = event.detail.value;
    this.safeSetData({
      selectedCompanyIds,
      allCompanies: markChecked(this.data.allCompanies, selectedCompanyIds),
    });
  },

  onApiBaseInput(event) {
    this.safeSetData({ apiBaseInput: event.detail.value || "" });
  },

  resetApiBase() {
    const config = getApiBaseConfig();
    this.safeSetData({ apiBaseInput: config.defaultApiBase });
  },

  async testApiConnection() {
    try {
      setApiBase(this.data.apiBaseInput);
      const result = await probeApiConnection();
      if (result.ok) {
        wx.showModal({
          title: "连接成功",
          content: `当前可用地址：${result.apiBase}`,
          showCancel: false,
        });
      } else {
        wx.showModal({
          title: "连接失败",
          content: `当前地址：${result.apiBase}\n错误：${result.detail}`,
          showCancel: false,
        });
      }
    } catch (error) {
      wx.showModal({
        title: "连接失败",
        content: String(error && error.message ? error.message : error),
        showCancel: false,
      });
    }
  },

  async saveConfig() {
    const selectedIds = normalizeStringIdList(this.data.selectedIds);
    const selectedCompanyIds = normalizeStringIdList(this.data.selectedCompanyIds);
    wx.setStorageSync(COMPANY_WATCHLIST_STORAGE_KEY, selectedCompanyIds);

    try {
      setApiBase(this.data.apiBaseInput);
      await request("/api/watchlist", "POST", {
        watchIndexIds: selectedIds,
        alertRule: {
          upper: this.data.upper,
          lower: this.data.lower,
          cooldownTradingDays: this.data.cooldownTradingDays,
        },
      });
      wx.showToast({ title: "已保存", icon: "success" });
    } catch (error) {
      wx.showToast({ title: "指数保存失败，公司自选已本地保存", icon: "none", duration: 2800 });
      console.error(error);
    }
  },
});
