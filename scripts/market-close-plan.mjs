import { appendFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isTradingDate, lastCompletedSession } from '../packages/data-pipeline/src/market-calendar.ts';

export function marketClosePlan({ event = 'schedule', schedule = '', mode = 'full', now = new Date() } = {}) {
  const refreshMode = event === 'schedule' ? (['17 5 * * 2-6', '17 9 * * 2-6'].includes(schedule) ? 'full' : 'close') : mode;
  if (!['close', 'full'].includes(refreshMode)) throw new Error('Expected close or full refresh mode');
  const target = lastCompletedSession(now);
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
  // UTC schedules cover both DST offsets; the earlier winter invocation skips
  // instead of deriving from a session that is still open.
  const run = event !== 'schedule' || refreshMode === 'full' || (isTradingDate(parts) && target === parts);
  return { mode: refreshMode, target, run: String(run) };
}

if (path.resolve(process.argv[1] || '') === fileURLToPath(import.meta.url)) {
  const plan = marketClosePlan({ event: process.env.REFRESH_EVENT, schedule: process.env.REFRESH_SCHEDULE, mode: process.env.REFRESH_MODE || 'full' });
  const output = Object.entries(plan).map(([key, value]) => `${key}=${value}`).join('\n') + '\n';
  console.log(output);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, output);
}
