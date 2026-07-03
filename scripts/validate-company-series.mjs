import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const nvdaPath = path.join(
  repoRoot,
  "data",
  "standardized",
  "company-series",
  "company_nvda.json"
);

const payload = JSON.parse(await readFile(nvdaPath, "utf8"));
const historical = (Array.isArray(payload.points) ? payload.points : [])
  .filter((point) => point?.date >= "2001-01-01" && point?.date <= "2011-12-31")
  .map((point) => ({ date: point.date, value: Number(point.pe_ttm) }))
  .filter((point) => Number.isFinite(point.value));

if (!historical.length) {
  throw new Error("NVDA historical PE validation failed: no finite 2001-2011 observations");
}

const extreme = historical.find((point) => Math.abs(point.value) > 300);
if (extreme) {
  throw new Error(
    `NVDA historical PE validation failed: extreme value ${extreme.value} on ${extreme.date}`
  );
}

for (let index = 1; index < historical.length; index += 1) {
  const previous = historical[index - 1];
  const current = historical[index];
  const change = Math.abs(current.value - previous.value);
  if (change > 200) {
    throw new Error(
      `NVDA historical PE validation failed: ${change.toFixed(4)} jump from ${previous.date} to ${current.date}`
    );
  }
}

console.log(`[company] NVDA historical PE validated: ${historical.length} observations`);
