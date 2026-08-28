import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  applyNasdaq100OfficialCloseRepairsForTest,
  applyValidatedNasdaq100TtmHistoryForTest,
  applyYahooSnapshotCarryToMetricForTest,
  parseFredIndexCloseSeriesForTest,
  validateNasdaq100OfficialMonthlyTtmSeriesForTest,
} from "../packages/data-pipeline/src/generate.ts";
import { buildIndexHistoryLock } from "../packages/data-pipeline/src/index-history-lock.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HISTORY_FILE = path.join(ROOT, "data", "standardized", "valuation-history.json");
const LOCK_FILE = path.join(ROOT, "data", "standardized", "index-history-lock.json");
const DAILY_METRICS_FILE = path.join(ROOT, "data", "standardized", "index-yahoo-daily-metrics.json");
const OFFICIAL_TTM_FILE = path.join(
  ROOT,
  "data",
  "bootstrap",
  "nasdaq100-ttm-nasdaq-bloomberg-monthly.csv"
);
const FRED_NDX_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQ100";

function parseOfficialTtmCsv(text) {
  const series = text
    .replace(/\r/g, "")
    .trim()
    .split("\n")
    .slice(1)
    .map((line) => {
      const [date, rawValue] = line.split(",");
      return {
        date: String(date || "").trim(),
        value: Number(rawValue),
        ts: Date.parse(`${date}T00:00:00Z`),
      };
    });
  return validateNasdaq100OfficialMonthlyTtmSeriesForTest(series);
}

function changedTtmStats(previous, corrected) {
  const previousByDate = new Map(previous.map((point) => [point.date, point.pe_ttm]));
  const changes = corrected
    .map((point) => {
      const before = previousByDate.get(point.date);
      const after = point.pe_ttm;
      return {
        date: point.date,
        before,
        after,
        absoluteChange:
          Number.isFinite(before) && Number.isFinite(after)
            ? Math.abs(Number(after) - Number(before))
            : before === after
              ? 0
              : Number.POSITIVE_INFINITY,
      };
    })
    .filter((change) => change.before !== change.after);

  const finiteChanges = changes.filter((change) => Number.isFinite(change.absoluteChange));
  finiteChanges.sort((a, b) => b.absoluteChange - a.absoluteChange);
  return {
    changedPointCount: changes.length,
    largestFiniteChanges: finiteChanges.slice(0, 10),
  };
}

async function main() {
  const shouldWrite = process.argv.includes("--write");
  const dataset = JSON.parse(await readFile(HISTORY_FILE, "utf8"));
  const ndx = dataset.indices.find((index) => index.id === "nasdaq100");
  if (!ndx?.points?.length) throw new Error("Nasdaq-100 history is missing");

  const officialMonthlyTtm = parseOfficialTtmCsv(await readFile(OFFICIAL_TTM_FILE, "utf8"));
  const firstDate = ndx.points[0].date;
  const lastDate = ndx.points.at(-1).date;
  const fredResponse = await fetch(FRED_NDX_URL);
  if (!fredResponse.ok) throw new Error(`FRED NDX download failed: HTTP ${fredResponse.status}`);
  const fredCsv = await fredResponse.text();
  const closes = applyNasdaq100OfficialCloseRepairsForTest(
    parseFredIndexCloseSeriesForTest(fredCsv, "NASDAQ100", firstDate, lastDate),
    firstDate,
    lastDate
  );
  if (closes.length < 6_000) throw new Error(`FRED NDX history is unexpectedly short: ${closes.length}`);

  const dailyMetrics = JSON.parse(await readFile(DAILY_METRICS_FILE, "utf8"));
  const postOfficialWsjSnapshots = (dailyMetrics.symbols?.QQQ || []).filter(
    (snapshot) =>
      snapshot.source === "wsj-latest" &&
      snapshot.date > "2026-06-30" &&
      Number.isFinite(snapshot.pe_ttm)
  );
  if (!postOfficialWsjSnapshots.length) {
    throw new Error("post-official Nasdaq-100 WSJ TTM snapshots are missing");
  }

  const previousForwardByDate = new Map(ndx.points.map((point) => [point.date, point.pe_forward]));
  let corrected = applyValidatedNasdaq100TtmHistoryForTest(
    ndx.points,
    closes,
    undefined,
    officialMonthlyTtm
  );
  corrected = applyYahooSnapshotCarryToMetricForTest(
    corrected,
    closes,
    postOfficialWsjSnapshots,
    "pe_ttm",
    {
      minValue: 2.4,
      maxValue: 240,
      backfillLookbackPoints: 5,
      minDate: "2026-06-30",
    }
  ).map((point) => ({
    ...point,
    pe_forward: previousForwardByDate.get(point.date) ?? null,
  }));

  for (let index = 0; index < ndx.points.length; index += 1) {
    const before = ndx.points[index];
    const after = corrected[index];
    for (const field of ["date", "pe_forward", "pb", "us10y_yield"]) {
      if (before[field] !== after[field]) {
        throw new Error(`unexpected Nasdaq-100 ${field} change on ${before.date}`);
      }
    }
  }

  const stats = changedTtmStats(ndx.points, corrected);
  console.log(JSON.stringify(stats, null, 2));
  if (!shouldWrite) {
    console.log("dry run only; pass --write to update standardized history and its lock");
    return;
  }

  ndx.points = corrected;
  const sourceWithoutOfficialTag = String(dataset.source || "").replace(
    /\+ndx-official-ttm-monthly-\d+/,
    ""
  );
  dataset.source = /\+ndx-ttm-factset-\d+/.test(sourceWithoutOfficialTag)
    ? sourceWithoutOfficialTag.replace(/\+ndx-ttm-factset-\d+/, "+ndx-official-ttm-monthly-1")
    : sourceWithoutOfficialTag.includes("+ndx-official-close-")
      ? sourceWithoutOfficialTag.replace(
          "+ndx-official-close-",
          "+ndx-official-ttm-monthly-1+ndx-official-close-"
        )
      : sourceWithoutOfficialTag.concat("+ndx-official-ttm-monthly-1");
  await writeFile(HISTORY_FILE, `${JSON.stringify(dataset, null, 2)}\n`, "utf8");
  await writeFile(LOCK_FILE, `${JSON.stringify(buildIndexHistoryLock(dataset), null, 2)}\n`, "utf8");
  console.log(`updated ${HISTORY_FILE}`);
  console.log(`updated ${LOCK_FILE}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
