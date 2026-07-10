import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const companySeriesDir = path.join(repoRoot, "data", "standardized", "company-series");
const yahooDailyMetricsPath = path.join(
  repoRoot,
  "data",
  "standardized",
  "company-yahoo-daily-metrics.json"
);
const RECENT_YAHOO_ANCHOR_MAX_AGE_DAYS = 14;
const YAHOO_ANCHOR_MAX_RELATIVE_ERROR = 0.02;

function finiteNonZero(value) {
  const number = Number(value);
  return Number.isFinite(number) && Math.abs(number) > 1e-8 ? number : null;
}

function ratioDistance(left, right) {
  if (left === null || right === null || left * right <= 0) return Number.POSITIVE_INFINITY;
  return Math.max(Math.abs(left / right), Math.abs(right / left));
}

function daysBetween(leftDate, rightDate) {
  const left = Date.parse(`${leftDate}T00:00:00Z`);
  const right = Date.parse(`${rightDate}T00:00:00Z`);
  if (!Number.isFinite(left) || !Number.isFinite(right)) return Number.POSITIVE_INFINITY;
  return Math.abs(right - left) / 86_400_000;
}

export function validateCompanySeriesPayloads(
  companyPayloads,
  yahooMetricsBySymbol,
  {
    requireNvdaHistory = false,
    recentYahooAnchorMaxAgeDays = RECENT_YAHOO_ANCHOR_MAX_AGE_DAYS,
    yahooAnchorMaxRelativeError = YAHOO_ANCHOR_MAX_RELATIVE_ERROR,
  } = {}
) {
  const errors = [];
  let recentYahooAnchorCount = 0;
  const payloadBySymbol = new Map();

  for (const payload of companyPayloads || []) {
    const symbol = String(payload?.symbol || "").trim().toUpperCase();
    const points = Array.isArray(payload?.points) ? payload.points : [];
    if (!symbol || !points.length) {
      errors.push(`invalid company series payload: symbol=${symbol || "missing"} points=${points.length}`);
      continue;
    }
    payloadBySymbol.set(symbol, payload);

    const latestDate = String(points.at(-1)?.date || "");
    const yahooRows = Array.isArray(yahooMetricsBySymbol?.[symbol])
      ? yahooMetricsBySymbol[symbol]
      : [];
    const latestYahooTtm = yahooRows
      .map((row) => ({ ...row, pe_ttm: finiteNonZero(row?.pe_ttm) }))
      .filter((row) => row.pe_ttm !== null && /^\d{4}-\d{2}-\d{2}$/.test(String(row?.date || "")))
      .sort((left, right) => String(left.date).localeCompare(String(right.date)))
      .at(-1);

    if (
      !latestYahooTtm ||
      daysBetween(String(latestYahooTtm.date), latestDate) > recentYahooAnchorMaxAgeDays
    ) {
      continue;
    }

    recentYahooAnchorCount += 1;
    const published = points.find((point) => point?.date === latestYahooTtm.date);
    const publishedPe = finiteNonZero(published?.pe_ttm);
    const factor = ratioDistance(publishedPe, latestYahooTtm.pe_ttm);
    if (factor > 1 + yahooAnchorMaxRelativeError) {
      errors.push(
        `${symbol} Yahoo TTM PE mismatch on ${latestYahooTtm.date}: ` +
          `published=${publishedPe ?? "missing"} yahoo=${latestYahooTtm.pe_ttm} factor=${
            Number.isFinite(factor) ? factor.toFixed(4) : "infinite"
          }`
      );
    }
  }

  if (requireNvdaHistory) {
    const nvda = payloadBySymbol.get("NVDA");
    const historical = (Array.isArray(nvda?.points) ? nvda.points : [])
      .filter((point) => point?.date >= "2001-01-01" && point?.date <= "2011-12-31")
      .map((point) => ({ date: point.date, value: Number(point.pe_ttm) }))
      .filter((point) => Number.isFinite(point.value));

    if (!historical.length) {
      errors.push("NVDA historical PE validation failed: no finite 2001-2011 observations");
    } else {
      const extreme = historical.find((point) => Math.abs(point.value) > 300);
      if (extreme) {
        errors.push(
          `NVDA historical PE validation failed: extreme value ${extreme.value} on ${extreme.date}`
        );
      }

      for (let index = 1; index < historical.length; index += 1) {
        const previous = historical[index - 1];
        const current = historical[index];
        const change = Math.abs(current.value - previous.value);
        if (change > 200) {
          errors.push(
            `NVDA historical PE validation failed: ${change.toFixed(4)} jump from ${previous.date} to ${current.date}`
          );
          break;
        }
      }
    }
  }

  if (errors.length) {
    const preview = errors.slice(0, 20).map((error) => `- ${error}`).join("\n");
    const suffix = errors.length > 20 ? `\n- ... ${errors.length - 20} more` : "";
    throw new Error(`Company PE validation failed (${errors.length}):\n${preview}${suffix}`);
  }

  return {
    companyCount: payloadBySymbol.size,
    recentYahooAnchorCount,
  };
}

async function main() {
  const names = (await readdir(companySeriesDir))
    .filter((name) => /^company_[a-z0-9_]+\.json$/i.test(name))
    .sort();
  const companyPayloads = await Promise.all(
    names.map(async (name) => JSON.parse(await readFile(path.join(companySeriesDir, name), "utf8")))
  );
  const yahooPayload = JSON.parse(await readFile(yahooDailyMetricsPath, "utf8"));
  const summary = validateCompanySeriesPayloads(
    companyPayloads,
    yahooPayload?.symbols || {},
    { requireNvdaHistory: true }
  );

  console.log(
    `[company] validated ${summary.companyCount} series; ` +
      `recent Yahoo TTM anchors=${summary.recentYahooAnchorCount}`
  );
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
