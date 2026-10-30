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
    requireTsmForwardHistory = false,
    recentYahooAnchorMaxAgeDays = RECENT_YAHOO_ANCHOR_MAX_AGE_DAYS,
    yahooAnchorMaxRelativeError = YAHOO_ANCHOR_MAX_RELATIVE_ERROR,
  } = {}
) {
  const errors = [];
  let recentYahooAnchorCount = 0;
  let recentYahooForwardAnchorCount = 0;
  let recentYahooPbAnchorCount = 0;
  const payloadBySymbol = new Map();

  for (const payload of companyPayloads || []) {
    const symbol = String(payload?.symbol || "").trim().toUpperCase();
    const points = Array.isArray(payload?.points) ? payload.points : [];
    const report = payload?.latestEarnings;
    const awaitingProvider = symbol && !points.length && payload?.valuationStatus === "awaiting-provider"
      && report?.sourceUrl?.startsWith("https://") && /^\d{4}-\d{2}-\d{2}$/.test(report?.reportDate || "")
      && Number.isFinite(report?.revenue) && Number.isFinite(report?.netIncome);
    if (awaitingProvider) continue;
    if (!symbol || !points.length) {
      errors.push(`invalid company series payload: symbol=${symbol || "missing"} points=${points.length}`);
      continue;
    }
    payloadBySymbol.set(symbol, payload);

    const latestDate = String(points.at(-1)?.date || "");
    const publishedByDate = new Map(points.map((point) => [String(point?.date || ""), point]));
    const yahooRows = Array.isArray(yahooMetricsBySymbol?.[symbol])
      ? yahooMetricsBySymbol[symbol]
      : [];
    for (const row of yahooRows) {
      if (row.date !== latestDate) continue;
      for (const key of ['pe_ttm', 'pe_forward', 'pb', 'peg']) {
        if (row.metricSources?.[key]?.includes(':current-table') && row[key] == null && points.at(-1)?.[key] != null) errors.push(`${symbol} unavailable Current ${key} was filled by fallback on ${row.date}`);
      }
    }
    for (const { key, label } of [
      { key: "pe_ttm", label: "TTM PE" },
      { key: "pe_forward", label: "Forward PE" },
      { key: "pb", label: "PB" },
    ]) {
      const latestYahooMetric = yahooRows
        .map((row) => ({ ...row, value: finiteNonZero(row?.[key]) }))
        .filter((row) => {
          const date = String(row?.date || "");
          return (
            row.value !== null &&
            /^\d{4}-\d{2}-\d{2}$/.test(date) &&
            date <= latestDate &&
            publishedByDate.has(date)
          );
        })
        .sort((left, right) => String(left.date).localeCompare(String(right.date)))
        .at(-1);

      if (
        !latestYahooMetric ||
        daysBetween(String(latestYahooMetric.date), latestDate) > recentYahooAnchorMaxAgeDays
      ) {
        continue;
      }

      if (key === "pe_ttm") recentYahooAnchorCount += 1;
      else if (key === "pe_forward") recentYahooForwardAnchorCount += 1;
      else recentYahooPbAnchorCount += 1;

      const published = publishedByDate.get(String(latestYahooMetric.date));
      const publishedPe = finiteNonZero(published?.[key]);
      const factor = ratioDistance(publishedPe, latestYahooMetric.value);
      const source = latestYahooMetric.metricSources?.[key] || latestYahooMetric.source || "";
      const tolerance = source.includes(":current-table") ? 1e-6 : yahooAnchorMaxRelativeError;
      if (factor > 1 + tolerance) {
        errors.push(
          `${symbol} Yahoo ${label} mismatch on ${latestYahooMetric.date}: ` +
            `published=${publishedPe ?? "missing"} yahoo=${latestYahooMetric.value} factor=${
              Number.isFinite(factor) ? factor.toFixed(4) : "infinite"
            }`
        );
      }
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

  if (requireTsmForwardHistory) {
    const tsm = payloadBySymbol.get("TSM");
    const forwardRows = (Array.isArray(tsm?.points) ? tsm.points : [])
      .map((point) => ({ date: String(point?.date || ""), value: finiteNonZero(point?.pe_forward) }))
      .filter((point) => /^\d{4}-\d{2}-\d{2}$/.test(point.date) && point.value !== null);

    if (!forwardRows.length) {
      errors.push("TSM forward PE validation failed: no finite historical observations");
    } else {
      forwardRows.sort((left, right) => left.date.localeCompare(right.date));
      const earliest = forwardRows[0]?.date || "";
      if (earliest > "2016-01-15" || forwardRows.length < 500) {
        errors.push(
          `TSM forward PE validation failed: earliest=${earliest || "missing"} ` +
            `observations=${forwardRows.length}`
        );
      }

      const extreme = forwardRows.find((point) => Math.abs(point.value) > 100);
      if (extreme) {
        errors.push(
          `TSM forward PE validation failed: extreme value ${extreme.value} on ${extreme.date}`
        );
      }

      for (let index = 1; index < forwardRows.length; index += 1) {
        const previous = forwardRows[index - 1];
        const current = forwardRows[index];
        const factor = ratioDistance(previous.value, current.value);
        if (daysBetween(previous.date, current.date) <= 7 && factor > 2.5) {
          errors.push(
            `TSM forward PE validation failed: ${factor.toFixed(4)} jump ` +
              `from ${previous.date} to ${current.date}`
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
    recentYahooForwardAnchorCount,
    recentYahooPbAnchorCount,
  };
}

export function validateCompanySnapshotAgreement(companyPayloads, snapshot) {
  const byId = new Map(companyPayloads.map(payload => [payload.indexId, payload]));
  if (snapshot.indices?.length !== byId.size) throw new Error('Company snapshot/series universe mismatch');
  const seen = new Set();
  for (const row of snapshot.indices || []) {
    const payload = byId.get(row.id);
    if (seen.has(row.id) || !payload || payload.symbol !== row.symbol) throw new Error(`Company identity mismatch: ${row.id}`);
    seen.add(row.id);
    const point = payload.points.at(-1);
    if (!point && payload.valuationStatus === 'awaiting-provider') continue;
    if (!point || row.date !== point.date) throw new Error(`Company snapshot date mismatch: ${row.symbol}`);
    for (const key of ['pe_ttm', 'pe_forward', 'pb']) {
      if (finiteNonZero(row[key]) !== finiteNonZero(point[key])) throw new Error(`Company snapshot ${key} mismatch: ${row.symbol}`);
      const evidence = point.valuationSources?.[key];
      if (evidence && (evidence.symbol !== payload.symbol || evidence.date !== point.date)) throw new Error(`Company provenance mismatch: ${row.symbol} ${key}`);
    }
  }
}

async function main() {
  const names = (await readdir(companySeriesDir))
    .filter((name) => /^company_[a-z0-9_]+\.json$/i.test(name))
    .sort();
  const companyPayloads = await Promise.all(
    names.map(async (name) => JSON.parse(await readFile(path.join(companySeriesDir, name), "utf8")))
  );
  const yahooPayload = JSON.parse(await readFile(yahooDailyMetricsPath, "utf8"));
  validateCompanySnapshotAgreement(companyPayloads, JSON.parse(await readFile(path.join(repoRoot, "data/standardized/company-valuation-snapshot.json"), "utf8")));
  const summary = validateCompanySeriesPayloads(
    companyPayloads,
    yahooPayload?.symbols || {},
    { requireNvdaHistory: true, requireTsmForwardHistory: true }
  );

  console.log(
    `[company] validated ${summary.companyCount} series; ` +
      `recent Yahoo TTM anchors=${summary.recentYahooAnchorCount}; ` +
      `recent Yahoo Forward PE anchors=${summary.recentYahooForwardAnchorCount}`
  );
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
