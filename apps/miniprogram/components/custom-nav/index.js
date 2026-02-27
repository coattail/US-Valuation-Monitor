Component({
  properties: {
    title: {
      type: String,
      value: "",
    },
  },

  data: {
    statusBarHeight: 20,
    titleBarHeight: 44,
    navHeight: 64,
    sideWidth: 96,
  },

  lifetimes: {
    attached() {
      this.initLayout();
    },
  },

  methods: {
    initLayout() {
      try {
        const info = wx.getWindowInfo ? wx.getWindowInfo() : wx.getSystemInfoSync();
        const statusBarHeight = Math.max(20, Number(info.statusBarHeight || 20));

        let titleBarHeight = 44;
        let sideWidth = 96;
        if (wx.getMenuButtonBoundingClientRect) {
          const menu = wx.getMenuButtonBoundingClientRect();
          if (menu && menu.top && menu.height && menu.width) {
            titleBarHeight = Math.max(36, Math.round((menu.top - statusBarHeight) * 2 + menu.height));
            const rightSafe = Math.max(8, Number(info.windowWidth || 375) - Number(menu.right || 0));
            sideWidth = Math.max(92, Math.round(menu.width + rightSafe * 2));
          }
        }

        this.setData({
          statusBarHeight,
          titleBarHeight,
          navHeight: statusBarHeight + titleBarHeight,
          sideWidth,
        });
      } catch (error) {
        console.error(error);
        this.setData({
          statusBarHeight: 20,
          titleBarHeight: 44,
          navHeight: 64,
          sideWidth: 96,
        });
      }
    },
  },
});
