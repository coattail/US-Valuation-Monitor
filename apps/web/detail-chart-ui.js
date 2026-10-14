// Keep the navigator on the same frame as the plot, including its end-label gutter.
export function detailChartFrame(right, tone = "value") {
  const rgb = tone === "percentile" ? "133, 160, 218" : "112, 193, 173";
  const accent = `rgb(${rgb})`;
  const left = 50;

  return {
    grid: { containLabel: false, left, right, top: 16, bottom: 70 },
    dataZoom: [{
      type: "slider",
      xAxisIndex: 0,
      filterMode: "none",
      left,
      right,
      height: 24,
      bottom: 8,
      brushSelect: false,
      showDetail: false,
      borderColor: "rgba(139, 157, 178, 0.16)",
      backgroundColor: "rgba(8, 14, 22, 0.4)",
      fillerColor: `rgba(${rgb}, 0.10)`,
      dataBackground: {
        lineStyle: { color: "#687789", opacity: 0.35, width: 1 },
        areaStyle: { color: "#687789", opacity: 0.06 },
      },
      selectedDataBackground: {
        lineStyle: { color: accent, opacity: 0.6, width: 1 },
        areaStyle: { color: accent, opacity: 0.10 },
      },
      // A compact rounded grip avoids the default projecting white handle stems.
      handleIcon: "path://M3,0H5Q8,0 8,3V21Q8,24 5,24H3Q0,24 0,21V3Q0,0 3,0Z",
      handleSize: "100%",
      handleStyle: { color: "#263741", borderColor: accent, borderWidth: 1 },
      moveHandleSize: 0,
      emphasis: {
        handleStyle: { color: accent, borderColor: accent },
      },
    }],
  };
}

export function updateDetailWindow(chart) {
  const zoom = chart?.getOption?.()?.dataZoom?.[0];
  for (const [id, value] of [["detail-window-start", zoom?.startValue], ["detail-window-end", zoom?.endValue]]) {
    const element = document.getElementById(id);
    if (!element) continue;
    const date = value == null ? null : new Date(value);
    const label = date && Number.isFinite(date.getTime()) ? date.toISOString().slice(0, 10) : "—";
    element.textContent = label;
    if (label !== "—") element.dateTime = label;
    else element.removeAttribute("datetime");
  }
}
