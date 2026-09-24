// NYSE regular sessions: https://www.nyse.com/markets/hours-calendars
// Conservatively wait until 16:00 ET even on early-close sessions.
const DAY = 86400000;
export const shiftDate = (date: string, days: number) => new Date(Date.parse(`${date}T00:00:00Z`) + days * DAY).toISOString().slice(0, 10);
const iso = (year: number, month: number, day: number) => new Date(Date.UTC(year, month - 1, day)).toISOString().slice(0, 10);
const weekday = (date: string) => new Date(`${date}T00:00:00Z`).getUTCDay();
const nthMonday = (y: number, m: number, n: number) => shiftDate(iso(y, m, 1), (8 - weekday(iso(y, m, 1))) % 7 + (n - 1) * 7);
function observed(date: string) { return shiftDate(date, weekday(date) === 6 ? -1 : weekday(date) === 0 ? 1 : 0); }
function goodFriday(y: number) {
  const a = y % 19, b = Math.floor(y / 100), c = y % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30, i = Math.floor(c / 4), k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451);
  return shiftDate(iso(y, Math.floor((h + l - 7 * m + 114) / 31), (h + l - 7 * m + 114) % 31 + 1), -2);
}
export function isTradingDate(date: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || weekday(date) % 6 === 0) return false;
  const y = Number(date.slice(0, 4));
  const jan1 = iso(y, 1, 1);
  // NYSE does not observe a Saturday New Year's Day on the preceding Friday.
  const newYear = weekday(jan1) === 0 ? shiftDate(jan1, 1) : jan1;
  const may31 = iso(y, 5, 31);
  const thanksgiving = shiftDate(iso(y, 11, 1), (11 - weekday(iso(y, 11, 1))) % 7 + 21);
  return !new Set([newYear, nthMonday(y, 1, 3), nthMonday(y, 2, 3), goodFriday(y),
    shiftDate(may31, -(weekday(may31) + 6) % 7), ...(y >= 2022 ? [observed(iso(y, 6, 19))] : []),
    observed(iso(y, 7, 4)), nthMonday(y, 9, 1), thanksgiving, observed(iso(y, 12, 25)),
    '2025-01-09', // National day of mourning; add future extraordinary closures here.
  ]).has(date);
}
export function lastCompletedSession(now = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23' }).formatToParts(now);
  const get = (key: string) => parts.find(p => p.type === key)!.value;
  let date = `${get('year')}-${get('month')}-${get('day')}`;
  if (Number(get('hour')) < 16) date = shiftDate(date, -1);
  while (!isTradingDate(date)) date = shiftDate(date, -1);
  return date;
}
export function tradingDates(from: string, to: string): string[] {
  const dates: string[] = [];
  for (let date = from; date <= to; date = shiftDate(date, 1)) if (isTradingDate(date)) dates.push(date);
  return dates;
}
