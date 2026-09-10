// Shared presentation policy. Recompute from the displayed percentile rather
// than trusting stored regime fields, which may use a different history window.
export const VALUATION_THRESHOLDS = Object.freeze({ low: 0.2, high: 0.8 });

export function regimeFromPercentile(percentile) {
  if (typeof percentile !== "number" || !Number.isFinite(percentile) || percentile < 0 || percentile > 1) return "unavailable";
  if (percentile <= VALUATION_THRESHOLDS.low) return "low";
  if (percentile >= VALUATION_THRESHOLDS.high) return "high";
  return "neutral";
}

export function regimeLabel(regime) {
  return { low: "低估", neutral: "合理", high: "高估" }[regime] || "不适用";
}

export function percentileColor(percentile) {
  return { low: "#62d5b0", neutral: "#ddc086", high: "#f28b94", unavailable: "#96a2b3" }[regimeFromPercentile(percentile)];
}

export function snapshotPercentile(row) {
  return typeof row.pe_ttm === "number" && Number.isFinite(row.pe_ttm) && row.pe_ttm > 0 && regimeFromPercentile(row.percentile_10y) !== "unavailable"
    ? row.percentile_10y : null;
}

export function snapshotBadge(row) {
  const percentile = snapshotPercentile(row);
  if (percentile === null) {
    const label = typeof row.pe_ttm !== "number" || !Number.isFinite(row.pe_ttm) || row.pe_ttm <= 0 ? "PE 不适用" : "分位缺失";
    return `<span class="badge unavailable" title="缺少有效 PE 或十年百分位，暂不判断估值状态">${label}</span>`;
  }
  const regime = regimeFromPercentile(percentile);
  return `<span class="badge ${regime}" title="按 PE 近十年百分位判断：≤20% 低估，≥80% 高估，其余合理；不足十年使用可用历史">${regimeLabel(regime)}</span>`;
}
