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
