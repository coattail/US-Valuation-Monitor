Component({
  data: {
    selected: 0,
    color: "#8fa7c7",
    selectedColor: "#63d4c7",
    list: [
      {
        pagePath: "/pages/index/index",
        text: "指数"
      },
      {
        pagePath: "/pages/company/index/index",
        text: "公司"
      },
      {
        pagePath: "/pages/alerts/alerts",
        text: "提醒"
      },
      {
        pagePath: "/pages/profile/profile",
        text: "我的"
      }
    ]
  },

  methods: {
    updateSelected() {
      const pages = getCurrentPages();
      const current = pages[pages.length - 1];
      const route = current && current.route ? `/${current.route}` : "";
      const selected = this.data.list.findIndex((item) => item.pagePath === route);
      this.setData({ selected: selected >= 0 ? selected : 0 });
    },

    switchTab(event) {
      const index = Number(event.currentTarget.dataset.index || 0);
      const item = this.data.list[index];
      if (!item || !item.pagePath) return;
      wx.switchTab({ url: item.pagePath });
    }
  },

  lifetimes: {
    attached() {
      this.updateSelected();
    }
  },

  pageLifetimes: {
    show() {
      this.updateSelected();
    }
  }
});
