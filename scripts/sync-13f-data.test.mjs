import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { sync13fData } from "./sync-13f-data.mjs";

async function makeTempDir(prefix) {
  return await fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

test("sync13fData copies and minifies upstream datasets", async () => {
  const rootDir = await makeTempDir("usvm-13f-root-");
  const sourceDir = await makeTempDir("usvm-13f-source-");

  await fs.mkdir(path.join(rootDir, "apps", "13f", "data"), { recursive: true });

  await fs.writeFile(
    path.join(sourceDir, "sec-13f-history.json"),
    JSON.stringify({ generated_at_utc: "2026-04-09T00:00:00Z", managers: [{ id: "buffett" }] }, null, 2),
    "utf8",
  );
  await fs.writeFile(
    path.join(sourceDir, "sec-13f-latest.json"),
    JSON.stringify({ generated_at_utc: "2026-04-09T00:00:00Z", managers: [{ id: "buffett" }] }, null, 2),
    "utf8",
  );

  const result = await sync13fData({ rootDir, sourceDir });

  assert.deepEqual(result.changedFiles.sort(), ["sec-13f-history.json", "sec-13f-latest.json"]);

  const historyText = await fs.readFile(path.join(rootDir, "apps", "13f", "data", "sec-13f-history.json"), "utf8");
  const latestText = await fs.readFile(path.join(rootDir, "apps", "13f", "data", "sec-13f-latest.json"), "utf8");

  assert.equal(historyText, '{"generated_at_utc":"2026-04-09T00:00:00Z","managers":[{"id":"buffett"}]}\n');
  assert.equal(latestText, '{"generated_at_utc":"2026-04-09T00:00:00Z","managers":[{"id":"buffett"}]}\n');
});

test("sync13fData reports no changes when target files already match", async () => {
  const rootDir = await makeTempDir("usvm-13f-root-");
  const sourceDir = await makeTempDir("usvm-13f-source-");
  const targetDir = path.join(rootDir, "apps", "13f", "data");

  await fs.mkdir(targetDir, { recursive: true });

  const payload = JSON.stringify({ generated_at_utc: "2026-04-09T00:00:00Z", managers: [{ id: "soros" }] }, null, 2);
  await fs.writeFile(path.join(sourceDir, "sec-13f-history.json"), payload, "utf8");
  await fs.writeFile(path.join(sourceDir, "sec-13f-latest.json"), payload, "utf8");

  await sync13fData({ rootDir, sourceDir });
  const second = await sync13fData({ rootDir, sourceDir });

  assert.deepEqual(second.changedFiles, []);
  assert.deepEqual(second.unchangedFiles.sort(), ["sec-13f-history.json", "sec-13f-latest.json"]);
});
