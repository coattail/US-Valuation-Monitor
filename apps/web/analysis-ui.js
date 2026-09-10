// Shared presentation behavior for index and company analysis pages.
export function resolveDetailRange(rows, requestedRange = "10y") {
  if (requestedRange !== "10y" || !rows.length) return requestedRange;
  const end = new Date(`${rows.at(-1).date}T00:00:00Z`);
  end.setUTCFullYear(end.getUTCFullYear() - 10);
  return rows[0].date > end.toISOString().slice(0, 10) ? "max" : "10y";
}

export function detailRangeCaption(requestedRange, effectiveRange) {
  if (requestedRange === "10y" && effectiveRange === "max") return "全部可用历史（不足十年）";
  return effectiveRange === "max" ? "全部历史" : `近 ${parseInt(effectiveRange, 10)} 年`;
}

export function formatAxisTick(value, percentage = false) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  const absolute = Math.abs(number);
  const scale = absolute >= 1e9 ? 1e9 : absolute >= 1e6 ? 1e6 : absolute >= 1e4 ? 1e3 : 1;
  const suffix = scale === 1e9 ? "B" : scale === 1e6 ? "M" : scale === 1e3 ? "k" : "";
  const digits = absolute > 0 && absolute < 0.01 ? 4 : 2;
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(number / scale)}${suffix}${percentage ? "%" : ""}`;
}

export function createSeriesColors(palette) {
  const assignments = new Map();
  return (id, selectedIds) => {
    // Release removed selections without recoloring any series that remains selected.
    for (const assigned of assignments.keys()) {
      if (!selectedIds.includes(assigned)) assignments.delete(assigned);
    }
    for (const selected of selectedIds) {
      if (!assignments.has(selected)) {
        const used = new Set(assignments.values());
        assignments.set(selected, palette.find((color) => !used.has(color)) || palette[0]);
      }
    }
    return assignments.get(id) || palette[0];
  };
}

export function initAnalysisUI({ state, charts, compareColor, renderCompareCharts, toggleCompareSelection, setCompareDateRange, logoUrl }) {
  const byId = (id) => document.getElementById(id);
  const detailSelect = byId("detail-index");
  const search = byId("detail-search");
  let compareTimer;
  const scheduleCompare = () => {
    clearTimeout(compareTimer);
    if (byId("view-compare").classList.contains("is-active")) {
      // Invalidate any in-flight result as soon as selection changes.
      state.runtime.compareRenderToken++;
      compareTimer = setTimeout(renderCompareCharts, 140);
    }
  };
  const syncControls = () => {
    for (const group of document.querySelectorAll("[data-select]")) {
      const select = byId(group.dataset.select);
      group.querySelectorAll("button").forEach((button) => {
        const active = button.dataset.value === select.value;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", String(active));
      });
    }
    document.querySelectorAll("#detail-range-chips button").forEach((button) => {
      const active = button.dataset.range === (state.detail.effectiveRange || state.detail.range);
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    const custom = byId("compare-custom-dates");
    const hasDates = Boolean(state.compare.startDate || state.compare.endDate);
    custom.classList.toggle("has-dates", hasDates);
    custom.querySelector("summary").textContent = hasDates ? "自定义区间 · 已启用" : "自定义区间";
  };
  for (const group of document.querySelectorAll("[data-select]")) {
    const select = byId(group.dataset.select);
    for (const option of select.options) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = group.classList.contains("range-chip-group") ? "chip" : "";
      button.dataset.value = option.value;
      button.textContent = option.value === "pe_ttm" ? "PE TTM" : option.value === "pe_forward" ? "Forward PE" : option.textContent;
      button.addEventListener("click", () => {
        if (select.id === "compare-range") setCompareDateRange("", "");
        select.value = option.value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
        syncControls();
      });
      group.append(button);
    }
    select.addEventListener("change", syncControls);
  }
  const filterDetailOptions = () => {
    const keyword = search.value.trim().toLowerCase();
    const matches = state.metaRows.filter((item) => `${item.displayName} ${item.symbol || ""}`.toLowerCase().includes(keyword));
    detailSelect.replaceChildren(...matches.map((item) => new Option(`${item.displayName} · ${item.symbol || item.id}`, item.id, false, item.id === state.detail.indexId)));
    // Never silently select the first search result.
    detailSelect.value = state.detail.indexId;
    byId("detail-search-status").textContent = matches.length ? `${matches.length} 个匹配结果` : "没有匹配结果，请尝试其他名称或代码";
  };
  search.addEventListener("input", filterDetailOptions);
  search.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") { event.preventDefault(); detailSelect.focus(); }
    if (event.key === "Enter" && detailSelect.options.length === 1) {
      event.preventDefault();
      detailSelect.value = detailSelect.options[0].value;
      detailSelect.dispatchEvent(new Event("change", { bubbles: true }));
    }
  });
  byId("detail-switcher").addEventListener("toggle", () => {
    if (byId("detail-switcher").open) { filterDetailOptions(); search.focus(); }
  });
  byId("detail-switcher").addEventListener("keydown", (event) => {
    if (event.key === "Escape") { byId("detail-switcher").open = false; byId("detail-switcher").querySelector("summary").focus(); }
  });
  detailSelect.addEventListener("change", () => {
    byId("detail-switcher").open = false;
    search.value = "";
    byId("detail-switcher").querySelector("summary").focus();
  });
  const syncDetail = () => {
    const meta = state.metaRows.find((item) => item.id === state.detail.indexId);
    if (!meta) return;
    const identity = byId("detail-identity");
    identity.replaceChildren();
    const mark = document.createElement("span");
    mark.className = "identity-mark";
    mark.textContent = (meta.symbol || meta.id).replace(/[^a-z0-9]/gi, "").slice(0, 3).toUpperCase();
    if (logoUrl) {
      const logo = document.createElement("img");
      logo.src = logoUrl(meta.symbol); logo.alt = "";
      logo.addEventListener("error", () => logo.remove(), { once: true });
      mark.append(logo);
    }
    const text = document.createElement("div");
    const caption = document.createElement("span"); caption.className = "eyebrow"; caption.textContent = `${meta.symbol || meta.id} · 估值详情`;
    const heading = document.createElement("h2"); heading.textContent = meta.displayName;
    text.append(caption, heading); identity.append(mark, text);
    byId("detail-metric").value = state.detail.metric;
    syncControls();
  };
  byId("compare-picker-toggle").addEventListener("click", () => {
    const selector = byId("compare-selector");
    selector.hidden = !selector.hidden;
    byId("compare-picker-toggle").setAttribute("aria-expanded", String(!selector.hidden));
    byId("compare-picker-toggle").textContent = selector.hidden ? `＋ 添加${logoUrl ? "公司" : "指数"}` : "收起选择";
    if (!selector.hidden) byId("compare-search").focus();
  });
  const syncSelection = () => {
    const container = byId("compare-selected");
    const focusedId = document.activeElement?.dataset?.removeId;
    const focusedIndex = [...container.children].indexOf(document.activeElement);
    container.replaceChildren();
    for (const id of state.compare.indexIds) {
      const meta = state.metaRows.find((item) => item.id === id);
      if (!meta) continue;
      const chip = document.createElement("button");
      chip.type = "button"; chip.className = "selected-chip"; chip.dataset.removeId = id;
      chip.style.setProperty("--series-color", compareColor(id, state.compare.indexIds));
      chip.textContent = `${meta.displayName} ×`;
      chip.setAttribute("aria-label", `移除 ${meta.displayName}`);
      chip.addEventListener("click", () => toggleCompareSelection(id, false));
      container.append(chip);
    }
    if (!container.children.length) container.textContent = "添加对象，开始估值对比";
    byId("compare-picker-count").textContent = `已选 ${state.compare.indexIds.length} / 8`;
    if (focusedId) (container.querySelector(`[data-remove-id="${CSS.escape(focusedId)}"]`) || container.children[Math.min(focusedIndex, container.children.length - 1)] || byId("compare-picker-toggle")).focus();
  };
  for (const button of document.querySelectorAll("[data-chart]")) {
    button.addEventListener("click", () => {
      document.querySelector(".detail-visual-grid").dataset.activeChart = button.dataset.chart;
      document.querySelectorAll("[data-chart]").forEach((tab) => {
        const active = tab === button;
        tab.classList.toggle("is-active", active); tab.setAttribute("aria-pressed", String(active));
      });
      charts.detail?.resize(); charts.detailPercentile?.resize();
    });
  }
  const highlight = (event) => {
    const row = event.target.closest("tr[data-series-id]");
    if (row && !row.contains(event.relatedTarget)) {
      charts.compare?.dispatchAction({ type: "downplay" });
      charts.compare?.dispatchAction({ type: "highlight", seriesId: row.dataset.seriesId });
    }
  };
  const clearHighlight = (event) => {
    const row = event.target.closest("tr[data-series-id]");
    if (row && !row.contains(event.relatedTarget)) charts.compare?.dispatchAction({ type: "downplay" });
  };
  byId("compare-table-body").addEventListener("pointerover", highlight);
  byId("compare-table-body").addEventListener("focusin", highlight);
  byId("compare-table-body").addEventListener("pointerout", clearHighlight);
  byId("compare-table-body").addEventListener("focusout", clearHighlight);
  if (state.compare.startDate || state.compare.endDate) byId("compare-custom-dates").open = true;
  syncControls();
  return { syncDetail, syncSelection, syncControls, scheduleCompare };
}
