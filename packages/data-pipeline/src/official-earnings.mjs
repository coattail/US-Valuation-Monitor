import reports from '../../../data/earnings/official-quarterly.json' with { type: 'json' };

// Issuer financials are independent of daily provider valuation observations.
// Retain availability dates; never rewrite an earlier closing PE with later earnings.
export function applyOfficialEarnings(company) {
  const report = reports.companies[company.symbol];
  if (!report) return company;
  const merge = (prior, key) => {
    const rows = new Map((prior || []).map(row => [row.date, row]));
    for (const quarter of report.quarters) {
      if (!Number.isFinite(quarter[key])) continue;
      rows.set(quarter.periodEnd, {
        date: quarter.periodEnd, [key]: quarter[key], source: 'actual',
        availableDate: quarter.reportDate, currency: report.currency,
        sourceUrl: quarter.sourceUrl,
      });
    }
    return [...rows.values()].sort((a, b) => a.date.localeCompare(b.date));
  };
  return {
    ...company,
    quarterlyEps: company.symbol === 'SKHY' ? (company.quarterlyEps || []).filter(row => row.currency === 'USD') : merge(company.quarterlyEps, 'eps'),
    quarterlyNetIncome: merge(company.quarterlyNetIncome, 'netIncome'),
    valuationStatus: company.points?.length ? 'available' : 'awaiting-provider',
    ...(!company.points?.length ? { peg: null, forwardStartDate: '' } : {}),
    latestEarnings: { ...report.quarters.at(-1), currency: report.currency, accountingStandard: report.accountingStandard },
    annualFinancials: report.annualFinancials || [],
  };
}

export function normalizeListedCompany(company) {
  // CMC still labels the Korean primary listing 000660.KS after the July ADR IPO.
  return company.slug === 'sk-hynix' ? { ...company, symbol: 'SKHY', displayName: 'SK hynix' } : company;
}
