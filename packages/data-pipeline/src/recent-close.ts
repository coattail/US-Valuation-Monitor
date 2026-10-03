import { createTextFetcher } from "./fetch-text.ts";
import { isTradingDate } from "./market-calendar.ts";

interface PricePoint { date: string; close: number }
interface DailyClose extends PricePoint { ts: number }
const fetchText = createTextFetcher({ userAgent: "Mozilla/5.0", requestBudgetMs: 10000 });

// Nasdaq's historical table can lag its dated, closed-market primary quote.
// Date-only, non-real-time primary data is required: never use secondary
// after-hours quotes or an intraday timestamp as a completed daily close.
export function parseNasdaqClosedQuote(raw: string, symbol: string, endDate: string): PricePoint[] {
  const payload = JSON.parse(raw);
  const data = payload?.data;
  const quote = data?.primaryData;
  if (Number(payload?.status?.rCode) !== 200 || data?.symbol !== symbol.replace(/-/g, ".") ||
      data?.marketStatus !== "Closed" || quote?.isRealTime !== false) return [];
  const match = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{1,2}), (\d{4})$/.exec(quote.lastTradeTimestamp || "");
  if (!match) return [];
  const month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].indexOf(match[1]) + 1;
  const date = `${match[3]}-${String(month).padStart(2, "0")}-${match[2].padStart(2, "0")}`;
  const close = Number(String(quote.lastSalePrice || "").replace(/[$,\s]/g, ""));
  const ts = Date.parse(`${date}T00:00:00Z`);
  if (date !== endDate || !Number.isFinite(ts) || !isTradingDate(date) || new Date(ts).toISOString().slice(0, 10) !== date ||
      !Number.isFinite(close) || close <= 0) return [];
  return [{ date, close }];
}

export function nasdaqClosedQuoteUrl(symbol: string, assetClass: "stocks" | "etf"): string {
  return `https://api.nasdaq.com/api/quote/${encodeURIComponent(symbol.replace(/-/g, "."))}/info?assetclass=${assetClass}`;
}

export function appendRecentCloses(
  history: readonly PricePoint[], raw: string, endDate: string
): DailyClose[] {
  const original = history.map((point) => ({ ...point, ts: Date.parse(`${point.date}T00:00:00Z`) }));
  const latestDate = original.reduce((latest, point) => point.date > latest ? point.date : latest, "");
  const payload = JSON.parse(raw);
  if (Number(payload?.status?.rCode) >= 400) {
    throw new Error(`Nasdaq response: ${JSON.stringify(payload.status.bCodeMessage || payload.status.rCode)}`);
  }
  const rows = payload?.data?.tradesTable?.rows;
  if (!Array.isArray(rows)) return original;
  const extra = new Map<string, DailyClose>();
  for (const row of rows) {
    const match = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(String(row?.date || ""));
    if (!match) continue;
    const date = `${match[3]}-${match[1]}-${match[2]}`;
    const ts = Date.parse(`${date}T00:00:00Z`);
    if (!Number.isFinite(ts) || new Date(ts).toISOString().slice(0, 10) !== date) continue;
    const close = Number(String(row?.close || "").replace(/[$,\s]/g, ""));
    // Do not overwrite established price paths or publish intraday observations.
    if (date <= latestDate || date > endDate || !Number.isFinite(close) || close <= 0) continue;
    extra.set(date, { date, close, ts });
  }
  return [...original, ...extra.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export async function refreshNasdaqPriceTail(
  history: readonly PricePoint[], symbol: string, endDate: string,
  assetClass: "stocks" | "etf",
  request: (url: string) => Promise<string> = (url) => fetchText(url, 1, 10000)
): Promise<DailyClose[]> {
  const original = history.map((point) => ({ ...point, ts: Date.parse(`${point.date}T00:00:00Z`) }));
  const latestDate = original.reduce((latest, point) => point.date > latest ? point.date : latest, "");
  if (!latestDate || latestDate >= endDate) return original;
  // Query a dated, small window independently of the long-history response.
  // The latter can omit a recent session even when Nasdaq already publishes it.
  const fromDate = new Date(Math.max(
    // Nasdaq rejects equal from/to dates. Include the preceding observation;
    // appendRecentCloses discards the overlap without altering history.
    Date.parse(`${latestDate}T00:00:00Z`),
    Date.parse(`${endDate}T00:00:00Z`) - 14 * 86400000
  )).toISOString().slice(0, 10);
  const nasdaqSymbol = symbol.replace(/-/g, ".");
  const url = `https://api.nasdaq.com/api/quote/${encodeURIComponent(nasdaqSymbol)}/historical` +
    `?assetclass=${assetClass}&fromdate=${fromDate}&todate=${endDate}&limit=100`;
  let updated = [...original];
  try {
    updated = appendRecentCloses(original, await request(url), endDate);
  } catch (error) {
    // Preserve source dates when no verified newer observation is available.
    console.warn(`[prices] ${symbol}: recent close unavailable: ${String(error)}`);
  }
  if (updated.at(-1)?.date !== endDate) {
    try {
      for (const point of parseNasdaqClosedQuote(await request(nasdaqClosedQuoteUrl(symbol, assetClass)), symbol, endDate)) {
        updated.push({ ...point, ts: Date.parse(`${point.date}T00:00:00Z`) });
        console.log(`[prices] ${symbol}: recovered dated Nasdaq closed quote ${point.date}`);
      }
    } catch (error) { console.warn(`[prices] ${symbol}: closed quote unavailable: ${String(error)}`); }
  }
  if (updated.length > original.length) {
    console.log(`[prices] ${symbol}: appended ${updated.length - original.length} Nasdaq close(s), latest=${updated.at(-1)?.date}`);
  }
  return updated;
}
