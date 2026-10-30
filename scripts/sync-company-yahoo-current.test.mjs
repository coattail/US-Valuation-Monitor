import test from 'node:test';
import assert from 'node:assert/strict';
import { readCurrentValuation, readQuoteTtm } from './sync-company-yahoo-current.mjs';
const table = '<table><tr><th></th><th>Current</th></tr><tr><td>Trailing P/E</td><td>17.03</td></tr></table>';
test('independent audit requires the requested share class and an explicit Current table', async () => {
  let calls = 0;
  const result = await readCurrentValuation('GOOG', async url => {
    calls++;
    assert.match(url, /\/GOOG\/key-statistics\//);
    return calls === 1 ? `<link href="https://finance.yahoo.com/quote/GOOGL/key-statistics/" rel="canonical">${table}` : table;
  });
  assert.equal(calls, 2);
  assert.equal(result.latest.pe_ttm, 17.03);
  assert.equal(await readCurrentValuation('GOOG', async () => '<p>PE Ratio (TTM)</p><p>14.74</p>'), null);
  assert.equal(await readCurrentValuation('GOOG', async () => { throw new Error('429'); }), null);
});


test('quote TTM synchronization reads the requested quote field instead of a Current cell', async () => {
  const result = await readQuoteTtm('GOOG', async url => {
    assert.match(url, /\/GOOG\/$/);
    return `${table}<span title="PE Ratio (TTM)">PE Ratio (TTM)</span><fin-streamer data-value="14.74" data-field="trailingPE">14.74</fin-streamer>`;
  });
  assert.equal(result.latest.pe_ttm, 14.74);
  assert.match(result.source, /yahoo-quote-page-latest/);
  assert.equal(await readQuoteTtm('GOOG', async () => table), null);
});
