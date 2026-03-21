import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentFile = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(currentFile), "..");
const outputDir = path.join(repoRoot, ".pages");

const filesToCopy = [
  ["data", "standardized", "valuation-snapshot.json"],
  ["data", "standardized", "valuation-history.json"],
  ["data", "standardized", "company-valuation-snapshot.json"],
];

const directoriesToCopy = [
  ["apps", "web"],
  ["data", "standardized", "index-series"],
  ["data", "standardized", "company-series"],
];

const redirectHtml = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta http-equiv="refresh" content="0; url=./apps/web/index.html" />
    <title>US Valuation Monitor</title>
    <script>
      window.location.replace("./apps/web/index.html");
    </script>
  </head>
  <body>
    <p>Redirecting to <a href="./apps/web/index.html">US Valuation Monitor</a>...</p>
  </body>
</html>
`;

async function copyRelativePath(segments) {
  const sourcePath = path.join(repoRoot, ...segments);
  const targetPath = path.join(outputDir, ...segments);
  await cp(sourcePath, targetPath, { recursive: true });
}

async function main() {
  await rm(outputDir, { recursive: true, force: true });
  await mkdir(path.join(outputDir, "apps"), { recursive: true });
  await mkdir(path.join(outputDir, "data", "standardized"), { recursive: true });

  await Promise.all(directoriesToCopy.map((segments) => copyRelativePath(segments)));
  await Promise.all(filesToCopy.map((segments) => copyRelativePath(segments)));

  await writeFile(path.join(outputDir, ".nojekyll"), "", "utf8");
  await writeFile(path.join(outputDir, "index.html"), redirectHtml, "utf8");
  await writeFile(path.join(outputDir, "404.html"), redirectHtml, "utf8");
}

main().catch((error) => {
  console.error("[build-static-site] failed");
  console.error(error);
  process.exitCode = 1;
});
