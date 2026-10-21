// ECharts uses elapsed time on a time axis, not the number of observations.
export function filterDetailRowsByZoom(rows, range = {}) {
  if (!rows.length) return [];
  const start = Number(range.start ?? 0);
  const end = Number(range.end ?? 100);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return rows;
  const first = Date.parse(rows[0].date);
  const span = Date.parse(rows.at(-1).date) - first;
  const from = first + span * Math.max(0, Math.min(100, Math.min(start, end))) / 100;
  const to = first + span * Math.max(0, Math.min(100, Math.max(start, end))) / 100;
  return rows.filter(row => {
    const time = Date.parse(row.date);
    return time >= from && time <= to;
  });
}

// A Fenwick tree counts values <= each observation in O(n log n). Re-scanning
// every prefix on every drag frame makes long daily histories visibly stutter.
export function recomputeDetailPercentiles(rows) {
  const sorted = [...new Set(rows.map(row => row.value))].sort((a, b) => a - b);
  const ranks = new Map(sorted.map((value, index) => [value, index + 1]));
  const tree = new Uint32Array(sorted.length + 1);
  return rows.map((row, index) => {
    const rank = ranks.get(row.value);
    for (let i = rank; i < tree.length; i += i & -i) tree[i]++;
    let count = 0;
    for (let i = rank; i > 0; i -= i & -i) count += tree[i];
    return { ...row, percentile_full: count / (index + 1) };
  });
}

export function buildDetailPercentileData(baseRows, visibleRows) {
  const percentiles = new Map(visibleRows.map(row => [row.date, row.percentile_full * 100]));
  // Preserve the full time domain so replacing the curve cannot move a handle.
  return baseRows.map(row => [Date.parse(row.date), percentiles.get(row.date) ?? null]);
}

export function bindDetailRangeSync(mainChart, percentileChart, onRangeChange) {
  let frame = 0;
  let pending;
  let syncing = false;
  const schedule = (source, target, payload) => {
    if (syncing) return;
    const event = payload?.batch?.at(-1) || payload;
    const zoom = event?.start != null && event?.end != null
      ? event : source.getOption().dataZoom[0];
    const range = { start: zoom.start, end: zoom.end };
    pending = { target, range };
    if (frame) return;
    frame = requestAnimationFrame(() => {
      const task = pending;
      pending = null;
      frame = 0;
      syncing = true;
      try {
        task.target.dispatchAction({ type: "dataZoom", dataZoomIndex: [0], ...task.range });
        onRangeChange(task.range);
      } finally {
        syncing = false;
      }
    });
  };
  const onMain = payload => schedule(mainChart, percentileChart, payload);
  const onPercentile = payload => schedule(percentileChart, mainChart, payload);
  mainChart.on("datazoom", onMain);
  percentileChart.on("datazoom", onPercentile);
  return () => {
    if (frame) cancelAnimationFrame(frame);
    mainChart.off("datazoom", onMain);
    percentileChart.off("datazoom", onPercentile);
    pending = null;
  };
}
