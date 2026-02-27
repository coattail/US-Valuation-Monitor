const { devLogin, getApiBaseConfig, probeApiConnection } = require("./utils/api");

App({
  globalData: {
    token: "",
    userId: "demo-user",
    theme: "fresh",
  },

  async onLaunch() {
    try {
      const login = await devLogin("mini-user");
      this.globalData.token = login.token;
      this.globalData.userId = login.userId;
      wx.setStorageSync("usvm-dev-token", login.token);
      wx.setStorageSync("usvm-user-id", login.userId);
    } catch (error) {
      const probe = await probeApiConnection();
      if (probe.ok) {
        try {
          const login = await devLogin("mini-user");
          this.globalData.token = login.token;
          this.globalData.userId = login.userId;
          wx.setStorageSync("usvm-dev-token", login.token);
          wx.setStorageSync("usvm-user-id", login.userId);
          return;
        } catch (retryError) {
          console.error(retryError);
        }
      }

      const config = getApiBaseConfig();
      wx.showToast({
        title: "API连接失败，请先启动9040服务",
        icon: "none",
        duration: 2800,
      });
      console.error(`[usvm] api base=${config.apiBase}`);
      console.error(error);
    }
  },
});
