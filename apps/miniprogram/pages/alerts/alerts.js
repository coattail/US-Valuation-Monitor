const { request } = require("../../utils/api");

function formatPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

Page({
  data: {
    unreadCount: 0,
    rows: [],
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
      tabBar.setData({ selected: 2 });
    }
  },

  onShow() {
    this.cache.isAlive = true;
    this.syncTabBarSelected();
    this.loadAlerts();
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

  async loadAlerts() {
    const loadToken = this.nextLoadToken();
    try {
      const payload = await request("/api/alerts");
      if (!this.isLoadActive(loadToken)) return;
      const rows = (payload.rows || []).map((item) => ({
        ...item,
        percentileText: formatPercent(item.percentile),
        directionText: item.direction === "high" ? "高估" : "低估",
      }));
      this.safeSetData({
        unreadCount: payload.unreadCount,
        rows,
      });
    } catch (error) {
      if (this.isLoadActive(loadToken)) wx.showToast({ title: "加载失败", icon: "none" });
      console.error(error);
    }
  },

  async markAllRead() {
    try {
      await request("/api/alerts/ack", "POST", {});
      wx.showToast({ title: "已标记", icon: "success" });
      this.loadAlerts();
    } catch (error) {
      wx.showToast({ title: "操作失败", icon: "none" });
      console.error(error);
    }
  },
});
