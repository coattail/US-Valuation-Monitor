import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { applyOfficialEarnings, normalizeListedCompany } from '../packages/data-pipeline/src/official-earnings.mjs';
import { validateCompanySeriesPayloads } from './validate-company-series.mjs';

test('Visa Q4 overrides stale EPS without altering pre-release valuations and reconciles to FY2026', () => {
  const points = [{date:'2026-10-27', pe_ttm:34.2}];
  const c = applyOfficialEarnings({symbol:'V', points, quarterlyEps:[{date:'2026-09-30',eps:1.708025,source:'actual'}]});
  assert.equal(c.points, points);
  assert.equal(c.quarterlyEps[0].eps,3.19);
  assert.equal(c.quarterlyEps[0].availableDate,'2026-10-27');
  assert.equal(c.latestEarnings.netIncome,6002000000);
  assert.equal(c.latestEarnings.revenue,12139000000);
  assert.equal(c.annualFinancials[0].eps,12.32);
  assert.equal(c.quarterlyNetIncome.reduce((sum,row)=>sum+row.netIncome,0),c.annualFinancials[0].netIncome);
  assert.deepEqual(applyOfficialEarnings(c),c);
});

test('SK hynix maps to its ADR ticker and retains KRW statements without ordinary-share EPS', () => {
  assert.equal(normalizeListedCompany({symbol:'000660.KS',slug:'sk-hynix'}).symbol,'SKHY');
  const c = applyOfficialEarnings({symbol:'SKHY',points:[],quarterlyEps:[{date:'2026-09-30',eps:83560,source:'expected'}]});
  assert.equal(c.latestEarnings.currency,'KRW');
  assert.equal(c.latestEarnings.netIncome,39775600000000);
  assert.equal(c.latestEarnings.reportDate,'2026-10-28');
  assert.deepEqual(c.quarterlyEps,[]);
  assert.equal(c.valuationStatus,'awaiting-provider');
  assert.equal(c.peg,null);
  assert.equal(c.forwardStartDate,'');
  assert.doesNotThrow(()=>validateCompanySeriesPayloads([c],{}));
  assert.throws(()=>validateCompanySeriesPayloads([{...c,latestEarnings:null}],{}),/invalid company/);
});

test('published records preserve official quarterly amounts and keep old Visa closing PE', async () => {
  const root = new URL('../data/standardized/company-series/',import.meta.url);
  const visa = JSON.parse(await readFile(new URL('company_v.json',root)));
  const sk = JSON.parse(await readFile(new URL('company_skhy.json',root)));
  assert.equal(visa.latestEarnings.eps,3.19);
  assert.equal(visa.points.find(p=>p.date==='2026-10-27').pe_ttm,34.2);
  assert.equal(sk.latestEarnings.revenue,104615500000000);
  assert.equal(sk.quarterlyNetIncome.find(q=>q.date==='2026-09-30').source,'actual');
});
