import { isTradingDate } from './market-calendar.ts';
import { valuationMatchesClose } from './yahoo-valuation-policy.mjs';

export const YAHOO_FORWARD_QUOTE_SOURCE = 'yahoo-quote-api-forward';
export const YAHOO_PEG_QUOTE_SOURCE = 'yahoo-quote-summary-peg';
export const YAHOO_FORWARD_QUOTE_URL = 'https://query1.finance.yahoo.com/v7/finance/quote';
const marketDate = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
});
const closedStates = new Set(['CLOSED', 'POST', 'POSTPOST', 'PRE', 'PREPRE']);

const raw = value => value?.raw ?? value;

function parseYahooRegularClose(quote) {
  if (!quote || typeof quote.symbol !== 'string' || !closedStates.has(quote.marketState)) return null;
  const time = raw(quote.regularMarketTime);
  const close = raw(quote.regularMarketPrice);
  if (!Number.isFinite(time) || time <= 0 || !Number.isFinite(close) || close <= 0) return null;
  const instant = new Date(time * 1000);
  if (!Number.isFinite(instant.getTime())) return null;
  const date = marketDate.format(instant);
  return isTradingDate(date) ? { symbol: quote.symbol.toUpperCase(), date, close } : null;
}

// The price and EPS check the reported P/E. Publication uses the reported value.
export function parseYahooForwardQuote(quote) {
  const regular = parseYahooRegularClose(quote);
  if (!regular || !valuationMatchesClose(quote.forwardPE, regular.close, quote.epsForward)) return null;
  return { ...regular, value: quote.forwardPE };
}

export function parseYahooPegQuote(result) {
  const regular = parseYahooRegularClose(result?.price);
  const value = raw(result?.defaultKeyStatistics?.pegRatio);
  return regular && Number.isFinite(value) && value !== 0 ? { ...regular, value } : null;
}

// Yahoo's quote endpoint uses an anonymous session cookie and crumb. Share one
// session and make a bounded number of requests for the whole company universe.
export async function openYahooQuoteSession(request = fetch) {
  const userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36';
  const headers = { 'User-Agent': userAgent };
  const get = (url, init = {}) => request(url, { ...init, signal: AbortSignal.timeout(10_000) });
  let crumb;
  try {
    const session = await get('https://fc.yahoo.com/', { headers, redirect: 'manual' });
    const cookie = session.headers.getSetCookie().map(value => value.split(';', 1)[0]).join('; ');
    await session.body?.cancel();
    if (!cookie) return null;
    headers.Cookie = cookie;
    const response = await get('https://query1.finance.yahoo.com/v1/test/getcrumb', { headers });
    if (!response.ok) return null;
    crumb = (await response.text()).trim();
    if (!crumb || !/^[\w./+~-]{4,100}$/.test(crumb)) return null;
  } catch {
    return null;
  }

  return async (endpoint, params) => {
    const url = new URL(endpoint);
    url.search = new URLSearchParams({ ...params, crumb, formatted: 'false' });
    try {
      const response = await get(url, { headers });
      return response.ok ? await response.json() : null;
    } catch {
      return null;
    }
  };
}

export async function fetchYahooForwardQuotes(symbols, session) {
  const normalized = [...new Set(symbols.map(symbol => symbol.replaceAll('.', '-').toUpperCase()))];
  const read = await session;
  if (!read || !normalized.length) return new Map();
  const quotes = new Map();
  const batchSize = 30;
  const batches = Array.from({ length: Math.ceil(normalized.length / batchSize) }, (_, index) =>
    normalized.slice(index * batchSize, (index + 1) * batchSize)
  );
  await Promise.all(batches.map(async batch => {
    const payload = await read(YAHOO_FORWARD_QUOTE_URL, { symbols: batch.join(',') });
    for (const rawQuote of payload?.quoteResponse?.result || []) {
      const quote = parseYahooForwardQuote(rawQuote);
      if (quote && batch.includes(quote.symbol)) quotes.set(quote.symbol, quote);
    }
  }));
  return quotes;
}

export async function fetchYahooPegQuote(symbol, session) {
  const read = await session;
  if (!read) return null;
  const ticker = symbol.replaceAll('.', '-').toUpperCase();
  const quote = await read(`https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(ticker)}`,
    { modules: 'price,defaultKeyStatistics' });
  const parsed = parseYahooPegQuote(quote?.quoteSummary?.result?.[0]);
  return parsed?.symbol === ticker ? parsed : null;
}
