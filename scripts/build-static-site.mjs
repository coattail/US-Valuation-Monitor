import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentFile = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(currentFile), "..");
const outputDir = path.join(repoRoot, ".pages");

const fileCopies = [
  {
    source: ["data", "standardized", "valuation-snapshot.json"],
    target: ["data", "standardized", "valuation-snapshot.json"],
  },
  {
    source: ["data", "standardized", "valuation-history.json"],
    target: ["data", "standardized", "valuation-history.json"],
  },
  {
    source: ["data", "standardized", "company-valuation-snapshot.json"],
    target: ["data", "standardized", "company-valuation-snapshot.json"],
  },
];

const directoryCopies = [
  {
    source: ["apps", "web"],
    target: ["apps", "web"],
  },
  {
    source: ["apps", "13f"],
    target: ["13f"],
  },
  {
    source: ["apps", "13f"],
    target: ["apps", "13f"],
  },
  {
    source: ["data", "standardized", "index-series"],
    target: ["data", "standardized", "index-series"],
  },
  {
    source: ["data", "standardized", "company-series"],
    target: ["data", "standardized", "company-series"],
  },
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
  const sourcePath = path.join(repoRoot, ...segments.source);
  const targetPath = path.join(outputDir, ...segments.target);
  await cp(sourcePath, targetPath, { recursive: true });
}

const redirects = `
/13F /13f/ 301
/apps/13f /13f/ 301
/apps/13F /13f/ 301
/apps/13f/* /13f/:splat 301
/apps/13F/* /13f/:splat 301
`.trimStart();

async function main() {
  await rm(outputDir, { recursive: true, force: true });
  await mkdir(path.join(outputDir, "apps"), { recursive: true });
  await mkdir(path.join(outputDir, "data", "standardized"), { recursive: true });

  await Promise.all(directoryCopies.map((segments) => copyRelativePath(segments)));
  await Promise.all(fileCopies.map((segments) => copyRelativePath(segments)));

  await writeFile(path.join(outputDir, ".nojekyll"), "", "utf8");
  await writeFile(path.join(outputDir, "_redirects"), redirects, "utf8");
  await writeFile(path.join(outputDir, "index.html"), redirectHtml, "utf8");
  await writeFile(path.join(outputDir, "404.html"), redirectHtml, "utf8");
}

main().catch((error) => {
  console.error("[build-static-site] failed");
  console.error(error);
  process.exitCode = 1;
});
