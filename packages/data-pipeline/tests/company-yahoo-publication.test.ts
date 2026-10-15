import test from 'node:test';
import assert from 'node:assert/strict';
import { capCompanyPublicationToYahoo, restoreCompanyPublishedArchive } from '../src/company-yahoo-publication.ts';

const previous = [{ date: '2026-10-13', pe_ttm: 20 }];
const points = [...previous, { date: '2026-10-14', pe_ttm: 21 }];
const yahoo = (date: string) => ({ date, pe_ttm: 22, source: 'yahoo-quote-page-latest:finance.yahoo.com' });

test('company prices cannot advance publication beyond Yahoo valuations', () => {
  assert.deepEqual(capCompanyPublicationToYahoo(points, previous, [yahoo('2026-10-13')], '2026-10-14'), previous);
  assert.deepEqual(capCompanyPublicationToYahoo(points, previous, [], '2026-10-14'), previous);
});

test('a dated Yahoo update permits the new company session and never an open session', () => {
  assert.deepEqual(capCompanyPublicationToYahoo(points, previous, [yahoo('2026-10-14')], '2026-10-14'), points);
  assert.deepEqual(capCompanyPublicationToYahoo(points, previous, [yahoo('2026-10-14')], '2026-10-13'), previous);
});

test('previous close estimates do not become settled company history on the next build', () => {
  const provisional = [...previous, { ...points[1], recovery: { estimated: true } }];
  assert.deepEqual(capCompanyPublicationToYahoo(points, provisional, [yahoo('2026-10-13')], '2026-10-14'), previous);
  assert.deepEqual(capCompanyPublicationToYahoo(points, provisional, [yahoo('2026-10-14')], '2026-10-14'), points);
});

test('settled company history survives a source outage, but non-Yahoo and empty metrics cannot advance it', () => {
  assert.deepEqual(capCompanyPublicationToYahoo(points, points, [], '2026-10-14'), points);
  for (const row of [
    { date: '2026-10-14', pe_ttm: 30, source: 'other-provider' },
    { date: '2026-10-14', pe_ttm: null, pb: null, source: 'yahoo-quote-page-latest' },
  ]) assert.deepEqual(capCompanyPublicationToYahoo(points, previous, [row], '2026-10-14'), previous);
});

test('a sparse rebuild preserves published historical gaps without extending the unobserved tail', () => {
  const retained = { date: '2026-09-21', pe_ttm: 19, recovery: { estimated: true } };
  const before = [retained, ...previous, { ...points[1], recovery: { estimated: true } }];
  const result = capCompanyPublicationToYahoo(points, before, [yahoo('2026-10-13')], '2026-10-14');
  assert.deepEqual(result, [retained, ...previous]);
  assert.deepEqual(result[0], retained);
});

test('the frozen company archive restores only interior dates and cannot replay a future close estimate', () => {
  const retained = { date: '2026-09-21', pe_ttm: 19, recovery: { estimated: true } };
  const restored = restoreCompanyPublishedArchive(previous, [
    { date: retained.date, point: retained },
    { date: previous[0].date, point: { ...previous[0], pe_ttm: 99 } },
    { date: points[1].date, point: points[1] },
    { date: '2026-09-22', point: { date: '2026-09-23', pe_ttm: 99 } },
  ]);
  assert.deepEqual(restored, [retained, ...previous]);
});
