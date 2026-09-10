import test from 'node:test';
import assert from 'node:assert/strict';
import { createSeriesColors, formatAxisTick } from './analysis-ui.js';

test('axis labels stay concise for negative extrema, small ratios and large values', () => {
  assert.equal(formatAxisTick(-159.123456), '-159.12');
  assert.equal(formatAxisTick(0.00456), '0.0046');
  assert.equal(formatAxisTick(1234567), '1.23M');
  assert.equal(formatAxisTick(15.001, true), '15%');
  assert.equal(formatAxisTick(Infinity), '—');
});
test('removing or reordering a selected series preserves the remaining colors', () => {
  const color = createSeriesColors(['red', 'blue', 'green']);
  assert.equal(color('A', ['A','B','C']), 'red');
  assert.equal(color('C', ['A','B','C']), 'green');
  assert.equal(color('C', ['C','B']), 'green');
  assert.equal(color('B', ['C','B','D']), 'blue');
  assert.equal(color('D', ['C','B','D']), 'red');
  assert.equal(new Set(['C','B','D'].map(id=>color(id,['C','B','D']))).size,3);
});

test('details default to ten years and use all available history for newer objects', async () => {
  const { resolveDetailRange, detailRangeCaption } = await import('./analysis-ui.js');
  const history = [{date:'2001-01-01'}, {date:'2026-09-09'}];
  assert.equal(resolveDetailRange(history), '10y');
  assert.equal(resolveDetailRange([{date:'2016-09-09'}, history[1]]), '10y');
  assert.equal(resolveDetailRange([{date:'2016-09-10'}, history[1]]), 'max');
  assert.equal(resolveDetailRange([{date:'2026-06-12'}, history[1]]), 'max');
  assert.equal(resolveDetailRange([history[1]]), 'max');
  assert.equal(resolveDetailRange([]), '10y');
  assert.equal(detailRangeCaption('10y', 'max'), '全部可用历史（不足十年）');
  // Explicit selections remain in force; a short object's automatic fallback
  // does not replace the requested range when the user switches objects.
  assert.equal(resolveDetailRange(history, 'max'), 'max');
  assert.equal(resolveDetailRange(history, '5y'), '5y');
  assert.equal(resolveDetailRange(history, '10y'), '10y');
});
