import { createTextFetcher } from "./fetch-text.ts";

interface PricePoint { date: string; close: number }
interface DailyClose extends PricePoint { ts: number }
const fetchText = createTextFetcher({ userAgent: "Mozilla/5.0", requestBudgetMs: 10000 });

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
  try {
    const updated = appendRecentCloses(original, await request(url), endDate);
    if (updated.length > original.length) {
      console.log(`[prices] ${symbol}: appended ${updated.length - original.length} Nasdaq close(s), latest=${updated.at(-1)?.date}`);
    }
    return updated;
  } catch (error) {
    // Preserve source dates when no verified newer observation is available.
    console.warn(`[prices] ${symbol}: recent close unavailable: ${String(error)}`);
    return original;
  }
}
