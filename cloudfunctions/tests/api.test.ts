import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  applyDailyUpdate,
  buildMeta,
  buildHeatmapPayload,
  buildCompanyMeta,
  buildCompanySeriesPayload,
  buildCompanySnapshotPayload,
  buildSeriesPayload,
  buildSnapshotPayload,
  loadCompanyDataset,
  loadDataset,
  normalizeWatchlist,
  type RuntimeStores,
} from "../src/app.ts";

const CURRENT_FILE = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(CURRENT_FILE), "../..");

test("meta payload exposes complete index catalog", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const meta = buildMeta(dataset);

  assert.equal(meta.indices.length, 21);
  assert.ok(meta.indices.filter((item) => item.id !== "dram").every((item) => item.pointCount > 100));
  assert.ok((meta.indices.find((item) => item.id === "dram")?.pointCount || 0) >= 40);
});

test("theme group filters expose all thematic indices", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const snapshot = buildSnapshotPayload(dataset, "theme");
  const heatmap = buildHeatmapPayload(dataset, "theme");

  assert.equal(snapshot.rows.length, 4);
  assert.equal(heatmap.rows.length, 4);
  assert.deepEqual(
    snapshot.rows.map((item) => item.indexId).sort(),
    ["dram", "igv", "smh", "soxx"]
  );
  assert.ok(snapshot.rows.every((item) => item.group === "theme"));
});

test("series payload defaults to index full history", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const payload = buildSeriesPayload(dataset, "sp500", "pe_ttm");

  assert.equal(payload.rows.length, payload.availableRange.pointCount);
  assert.equal(payload.rows[0].date, payload.availableRange.startDate);
  assert.equal(payload.rows[payload.rows.length - 1].date, payload.availableRange.endDate);
});

test("series payload exposes metric-specific PB history", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const payload = buildSeriesPayload(dataset, "sp500", "pb");

  assert.equal(payload.rows.length, payload.availableRange.pointCount);
  assert.equal(payload.availableRange.startDate, "1999-12-31");
  assert.equal(payload.rows[0].date, "1999-12-31");
  assert.equal(payload.rows[0].value, 5.19);
});

test("series payload starts S&P 500 forward PE at Yardeni/Refinitiv prefix", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const payload = buildSeriesPayload(dataset, "sp500", "pe_forward");

  assert.equal(payload.rows.length, payload.availableRange.pointCount);
  assert.equal(payload.availableRange.startDate, "1999-12-31");
  assert.equal(payload.rows[0].date, "1999-12-31");
  assert.ok(Number(payload.rows[0].value) > 25);
  assert.ok(Number(payload.rows[0].value) < 26);
  assert.ok(Number(payload.rows.find((row) => row.date === "2020-01-16")?.value) > 18);
  assert.ok(Number(payload.rows.find((row) => row.date === "2020-01-16")?.value) < 19);
  assert.equal(payload.rows.find((row) => row.date === "2020-01-17")?.value, 18.7);
});

test("company dataset exposes top100 meta and snapshot", async () => {
  const dataset = await loadCompanyDataset(ROOT_DIR);
  const meta = buildCompanyMeta(dataset);
  const snapshot = buildCompanySnapshotPayload(dataset);

  assert.equal(meta.indices.length, 100);
  assert.equal(snapshot.rows.length, 100);
  assert.ok(snapshot.rows.every((item) => item.indexId && item.symbol));
});

test("company series payload supports default full history", async () => {
  const dataset = await loadCompanyDataset(ROOT_DIR);
  const first = dataset.indices[0];
  assert.ok(first?.id);

  const payload = buildCompanySeriesPayload(dataset, first.id, "pe_ttm");
  assert.ok(payload.rows.length > 100);
  assert.equal(payload.rows[payload.rows.length - 1].date, payload.availableRange.endDate);
});

test("company snapshot payload prefers latest point peg over stale top-level peg", () => {
  const dataset = {
    generatedAt: "2026-03-27T00:00:00.000Z",
    source: "test",
    indices: [
      {
        id: "company_test",
        symbol: "TEST",
        displayName: "Test Inc",
        description: "Test Inc (TEST)",
        rank: 1,
        marketCap: 100,
        peg: 9.99,
        points: [
          { date: "2025-03-26", pe_ttm: 20, pe_forward: 18, pb: 4, peg: 1.1 },
          { date: "2026-03-26", pe_ttm: 25, pe_forward: 19, pb: 5, peg: 1.8 },
        ],
      },
    ],
  };

  const payload = buildCompanySnapshotPayload(dataset);
  assert.equal(payload.rows[0]?.peg, 1.8);
});

test("watchlist normalization applies defaults and clamps invalid input", () => {
  const watchlist = normalizeWatchlist(
    {
      watchIndexIds: ["sp500", "invalid", "nasdaq100"],
      alertRule: {
        metric: "percentile_full",
        upper: 88,
        lower: 12,
        cooldownTradingDays: 4,
      },
      themePreference: "terminal",
    },
    "test-user"
  );

  assert.deepEqual(watchlist.watchIndexIds, ["sp500", "nasdaq100"]);
  assert.equal(watchlist.alertRule.upper, 88);
  assert.equal(watchlist.themePreference, "terminal");
});

test("daily update returns refreshed dataset and alert count field", async () => {
  const dataset = await loadDataset(ROOT_DIR);
  const stores: RuntimeStores = {
    watchlists: {
      users: {
        "test-user": normalizeWatchlist(
          {
            watchIndexIds: ["sp500", "sector_technology"],
            alertRule: {
              metric: "percentile_full",
              upper: 85,
              lower: 15,
              cooldownTradingDays: 5,
            },
            themePreference: "fresh",
          },
          "test-user"
        ),
      },
    },
    alerts: { users: { "test-user": [] } },
    alertState: { users: { "test-user": {} } },
  };

  const result = await applyDailyUpdate(dataset, stores, {
    generateDatasetFn: async () => dataset,
  });
  assert.ok(result.dataset.generatedAt);
  assert.equal(result.dataset.indices.length, 21);
  assert.ok(Number.isInteger(result.createdAlerts));
});
