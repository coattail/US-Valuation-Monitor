import test from "node:test";
import assert from "node:assert/strict";
import { filterDetailRowsByZoom, recomputeDetailPercentiles, buildDetailPercentileData, bindDetailRangeSync } from "./detail-range.js";

const rows = [
  { date: "2026-01-01", value: 10 },
  { date: "2026-01-02", value: 20 },
  { date: "2026-01-09", value: 30 },
  { date: "2026-01-11", value: 15 },
];

test("zoom follows elapsed dates, including sparse, empty and single-point windows", () => {
  assert.deepEqual(filterDetailRowsByZoom(rows, { start: 50, end: 100 }), rows.slice(2));
  assert.deepEqual(filterDetailRowsByZoom(rows, { start: 100, end: 50 }), rows.slice(2));
  assert.deepEqual(filterDetailRowsByZoom(rows, { start: 20, end: 70 }), []);
  assert.deepEqual(filterDetailRowsByZoom(rows, { start: 80, end: 80 }), [rows[2]]);
  assert.deepEqual(filterDetailRowsByZoom(rows, { start: -20, end: 120 }), rows);
});

test("visible-window ranks include ties and rebuild after moving either endpoint", () => {
  for (const range of [{ start: 0, end: 100 }, { start: 50, end: 100 }, { start: 0, end: 80 }]) {
    const selected = filterDetailRowsByZoom(rows, range);
    const computed = recomputeDetailPercentiles(selected);
    computed.forEach((row, index) => {
      const prefix = selected.slice(0, index + 1);
      assert.equal(row.percentile_full, prefix.filter(item => item.value <= row.value).length / prefix.length);
    });
  }
  assert.deepEqual(recomputeDetailPercentiles([rows[0], rows[0]]).map(row => row.percentile_full), [1, 1]);
  const visible = recomputeDetailPercentiles(rows.slice(2));
  assert.equal(visible.at(-1).percentile_full, 0.5);
  const data = buildDetailPercentileData(rows, visible);
  assert.deepEqual(data.map(point => point[1]), [null, null, 100, 50]);
  assert.deepEqual(data.map(point => point[0]), rows.map(row => Date.parse(row.date)));
  assert.deepEqual(buildDetailPercentileData(rows, []).map(point => point[1]), [null, null, null, null]);
});

test("large histories keep the same rank convention as a direct prefix count", () => {
  const large = Array.from({ length: 12000 }, (_, i) => ({ value: ((i * 7919) % 101) - 50 }));
  const computed = recomputeDetailPercentiles(large);
  for (const index of [0, 1, 200, 11000, 11999]) {
    assert.equal(computed[index].percentile_full,
      large.slice(0, index + 1).filter(row => row.value <= large[index].value).length / (index + 1));
  }
});

test("both sliders coalesce bursts, avoid feedback and cancel stale frames", () => {
  let nextFrame = 0;
  const frames = new Map();
  globalThis.requestAnimationFrame = callback => { frames.set(++nextFrame, callback); return nextFrame; };
  globalThis.cancelAnimationFrame = id => frames.delete(id);
  const flush = () => {
    const callbacks = [...frames.values()];
    frames.clear();
    callbacks.forEach(callback => callback());
  };
  const chart = () => {
    const listeners = new Set();
    return {
      actions: [],
      on(type, callback) { listeners.add(callback); },
      off(type, callback) { listeners.delete(callback); },
      emit(payload) { listeners.forEach(callback => callback(payload)); },
      dispatchAction(action) { this.actions.push(action); this.emit(action); },
      getOption() { return { dataZoom: [{ start: 10, end: 80 }] }; },
    };
  };
  const main = chart(), percentile = chart(), updates = [];
  const cleanup = bindDetailRangeSync(main, percentile, range => updates.push(range));
  main.emit({ start: 20, end: 100 });
  main.emit({ batch: [{ start: 50, end: 90 }] });
  assert.equal(frames.size, 1);
  flush();
  assert.deepEqual(updates, [{ start: 50, end: 90 }]);
  assert.equal(percentile.actions.length, 1);
  assert.equal(frames.size, 0);
  percentile.emit({ startValue: 123, endValue: 456 });
  flush();
  assert.deepEqual(updates.at(-1), { start: 10, end: 80 });
  assert.equal(main.actions.length, 1);
  main.emit({ start: 80, end: 100 });
  cleanup();
  flush();
  main.emit({ start: 0, end: 100 });
  assert.equal(updates.length, 2);
  assert.equal(frames.size, 0);
});
