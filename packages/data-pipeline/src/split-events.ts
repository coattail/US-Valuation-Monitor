import { createTextFetcher } from './fetch-text.ts';
import { isTradingDate } from './market-calendar.ts';

export interface SplitEvent { symbol: string; date: string; ratio: number; source: string }
const request = createTextFetcher({ userAgent: 'Mozilla/5.0', requestBudgetMs: 10000 });

export function yahooChartUrl(symbol: string, from: string, to: string): string {
  return `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol.replace(/\./g, '-'))}` +
    `?period1=${Date.parse(from + 'T00:00:00Z') / 1000}&period2=${Date.parse(to + 'T00:00:00Z') / 1000 + 86400}&interval=1d&events=splits`;
}

export function parseYahooSplitEvents(raw: string, source: string, symbol: string): SplitEvent[] {
  const result = JSON.parse(raw)?.chart?.result?.[0];
  if (result?.meta?.symbol !== symbol.replace(/\./g, '-')) throw new Error(`Unexpected Yahoo split symbol for ${symbol}`);
  return Object.values(result.events?.splits || {}).map((event: any) => {
    if (!Number.isSafeInteger(event.date) || !Number.isFinite(event.numerator) || event.numerator <= 0 ||
      !Number.isFinite(event.denominator) || event.denominator <= 0) throw new Error(`Invalid ${symbol} split event`);
    const date = new Date(event.date * 1000).toISOString().slice(0, 10);
    const ratio = event.numerator / event.denominator;
    if (!isTradingDate(date) || !Number.isFinite(ratio) || ratio <= 0 || ratio === 1) throw new Error(`Invalid ${symbol} split ratio/date`);
    if (event.splitRatio && event.splitRatio !== `${event.numerator}:${event.denominator}`) throw new Error(`Inconsistent ${symbol} split ratio`);
    return { symbol, date, ratio, source };
  }).sort((a, b) => a.date.localeCompare(b.date));
}

export async function fetchYahooSplitEvents(symbol: string, from: string, to: string,
  fetchText = (url: string) => request(url, 1, 10000)): Promise<SplitEvent[]> {
  const url = yahooChartUrl(symbol, from, to);
  return parseYahooSplitEvents(await fetchText(url), url, symbol).filter(event => event.date >= from && event.date <= to);
}
