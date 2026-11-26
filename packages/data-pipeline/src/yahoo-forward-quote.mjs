import { isTradingDate } from './market-calendar.ts';
import { valuationMatchesClose } from './yahoo-valuation-policy.mjs';

export const YAHOO_FORWARD_QUOTE_SOURCE = 'yahoo-quote-api-forward';
export const YAHOO_FORWARD_QUOTE_URL = 'https://query1.finance.yahoo.com/v7/finance/quote';
const marketDate = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
});
const closedStates = new Set(['CLOSED', 'POST', 'POSTPOST', 'PRE', 'PREPRE']);

// Use the reported multiple. Price and EPS only check whether the quote has
// moved in sync with the rest of the regular-session response.
export function parseYahooForwardQuote(quote) {
  if (!quote || typeof quote.symbol !== 'string' || !closedStates.has(quote.marketState) ||
      !Number.isFinite(quote.regularMarketTime) || quote.regularMarketTime <= 0 ||
      !valuationMatchesClose(quote.forwardPE, quote.regularMarketPrice, quote.epsForward)) return null;
  const date = marketDate.format(new Date(quote.regularMarketTime * 1000));
  return isTradingDate(date)
    ? { symbol: quote.symbol.toUpperCase(), date, close: quote.regularMarketPrice, value: quote.forwardPE }
    : null;
}

// Yahoo's quote endpoint uses an anonymous session cookie and crumb. Share one
// session and make a bounded number of requests for the whole company universe.
export async function fetchYahooForwardQuotes(symbols, request = fetch) {
  const normalized = [...new Set(symbols.map(symbol => symbol.replaceAll('.', '-').toUpperCase()))];
  if (!normalized.length) return new Map();
  const userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36';
  const headers = { 'User-Agent': userAgent };
  const get = (url, init = {}) => request(url, { ...init, signal: AbortSignal.timeout(10_000) });
  let crumb;
  try {
    const session = await get('https://fc.yahoo.com/', { headers, redirect: 'manual' });
    const cookie = session.headers.getSetCookie().map(value => value.split(';', 1)[0]).join('; ');
    await session.body?.cancel();
    if (!cookie) return new Map();
    headers.Cookie = cookie;
    const response = await get('https://query1.finance.yahoo.com/v1/test/getcrumb', { headers });
    if (!response.ok) return new Map();
    crumb = (await response.text()).trim();
    if (!crumb || !/^[\w./+~-]{4,100}$/.test(crumb)) return new Map();
  } catch {
    return new Map();
  }

  const quotes = new Map();
  const batchSize = 30;
  const batches = Array.from({ length: Math.ceil(normalized.length / batchSize) }, (_, index) =>
    normalized.slice(index * batchSize, (index + 1) * batchSize)
  );
  await Promise.all(batches.map(async batch => {
    const url = new URL(YAHOO_FORWARD_QUOTE_URL);
    url.search = new URLSearchParams({ symbols: batch.join(','), crumb, formatted: 'false' });
    try {
      const response = await get(url, { headers });
      if (!response.ok) return;
      const payload = await response.json();
      for (const raw of payload.quoteResponse?.result || []) {
        const quote = parseYahooForwardQuote(raw);
        if (quote && batch.includes(quote.symbol)) quotes.set(quote.symbol, quote);
      }
    } catch {
      // Continue with the dated statistics and timeseries when quotes are unavailable.
    }
  }));
  return quotes;
}
