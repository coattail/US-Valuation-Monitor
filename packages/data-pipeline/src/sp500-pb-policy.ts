import type { RawValuationPoint } from '../../core/src/types.ts';
import {readFile} from 'node:fs/promises';

export const SP500_BOOK_VALUE_URL = 'https://www.multpl.com/s-p-500-book-value/table/by-quarter';
export interface Sp500PbInputs {
  version: 1;
  symbol: '^GSPC';
  bookValueSource: string;
  bookValues: Array<{date: string; value: number}>;
  priceSources: string[];
  closes: Array<{date: string; close: number}>;
}

export async function loadSp500PbInputs(): Promise<Sp500PbInputs> {
  const inputs = JSON.parse(await readFile(new URL('../../../data/standardized/sp500-pb-inputs.json',import.meta.url),'utf8'));
  validateSp500PbInputs(inputs);
  return inputs;
}

// Keep the denominator's own date. A current quote date must never make an
// old balance sheet look current. Estimated PB rows are not book-value data.
export function parseSp500BookValues(html: string): Sp500PbInputs['bookValues'] {
  if (!/S&P 500 Book Value|S&amp;P 500 Book Value/i.test(html)) throw new Error('Expected S&P 500 book-value table');
  const values = new Map<string, number>();
  for (const row of html.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(cell => cell[1]);
    if (cells.length !== 2 || /estimate|dagger|2020;/i.test(cells[1])) continue;
    const dateText = cells[0].replace(/<[^>]*>/g, '').trim();
    if (!/^[A-Za-z]{3,9} \d{1,2}, \d{4}$/.test(dateText)) continue;
    const time = Date.parse(`${dateText} UTC`);
    const number = cells[1].replace(/<[^>]*>/g, '').replace(/&[^;]+;/g, '').trim().replace(/,/g, '');
    if (!/^\d+(?:\.\d+)?$/.test(number) || !Number.isFinite(time) || Number(number) <= 0) continue;
    values.set(new Date(time).toISOString().slice(0, 10), Number(number));
  }
  if (!values.size) throw new Error('No reported S&P 500 book values');
  return [...values].sort(([a], [b]) => a.localeCompare(b)).map(([date, value]) => ({date, value}));
}

export function validateSp500PbInputs(inputs: Sp500PbInputs): void {
  if (inputs.version !== 1 || inputs.symbol !== '^GSPC' || inputs.bookValueSource !== SP500_BOOK_VALUE_URL ||
      !inputs.bookValues?.length || !inputs.priceSources?.length || !inputs.closes?.length) throw new Error('Invalid S&P 500 PB inputs');
  for (const [rows, key] of [[inputs.bookValues, 'value'], [inputs.closes, 'close']] as const) {
    let previous = '';
    for (const row of rows) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(row.date) || row.date <= previous ||
          !Number.isFinite(row[key]) || row[key] <= 0) throw new Error(`Invalid S&P 500 PB ${key}: ${row.date}`);
      previous = row.date;
    }
  }
  if (!inputs.priceSources.every(source => /^https:\/\/query[12]\.finance\.yahoo\.com\/v8\/finance\/chart\/%5EGSPC\?/i.test(source))) {
    throw new Error('PB requires unadjusted S&P 500 index prices');
  }
}

export function applySp500PbPolicy(points: RawValuationPoint[], inputs: Sp500PbInputs, complete = true): RawValuationPoint[] {
  validateSp500PbInputs(inputs);
  const prices = new Map(inputs.closes.map(row => [row.date, row.close]));
  // Quarter-end ratios refer to that quarter's final market close, even when
  // the calendar quarter ends on a weekend. This is a retrospective series,
  // not a claim that balance sheets were already published on these dates.
  const books = inputs.bookValues.map(row => {
    const trade = inputs.closes.findLast(close => close.date <= row.date);
    const lag = trade ? (Date.parse(row.date) - Date.parse(trade.date)) / 86400000 : Infinity;
    return {...row, effectiveDate: lag <= 4 ? trade!.date : row.date};
  });
  return points.map(point => {
    const book = books.findLast(row => row.effectiveDate <= point.date);
    const close = prices.get(point.date);
    if (!book || !close) {
      if (complete) throw new Error(`Missing S&P 500 PB input: ${point.date}`);
      return point;
    }
    return {...point, pb: Number((close / book.value).toFixed(6)),
      pb_basis: {method: 'index-price-to-book', bookValueDate: book.date, bookValue: book.value}};
  });
}
