import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const CURRENT_FILE = fileURLToPath(import.meta.url);
const REPO_ROOT = path.resolve(path.dirname(CURRENT_FILE), "..");
const DEFAULT_TARGET_DIR = path.join(REPO_ROOT, "apps", "13f", "data");
const DEFAULT_SOURCE_DIR = process.env.THIRTEEN_F_SOURCE_DIR
  ? path.resolve(process.env.THIRTEEN_F_SOURCE_DIR)
  : path.join(REPO_ROOT, "..", "13F-Tracker", "data");

export const THIRTEEN_F_FILENAMES = ["sec-13f-history.json", "sec-13f-latest.json"];

export function minifyJsonText(rawText) {
  return `${JSON.stringify(JSON.parse(rawText))}\n`;
}

async function readFileIfExists(filePath) {
  try {
    return await fs.readFile(filePath, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

export async function sync13fData(options = {}) {
  const rootDir = path.resolve(options.rootDir ?? REPO_ROOT);
  const sourceDir = path.resolve(options.sourceDir ?? DEFAULT_SOURCE_DIR);
  const targetDir = path.join(rootDir, "apps", "13f", "data");
  const changedFiles = [];
  const unchangedFiles = [];

  await fs.mkdir(targetDir, { recursive: true });

  for (const filename of THIRTEEN_F_FILENAMES) {
    const sourcePath = path.join(sourceDir, filename);
    const targetPath = path.join(targetDir, filename);
    const nextText = minifyJsonText(await fs.readFile(sourcePath, "utf8"));
    const currentText = await readFileIfExists(targetPath);

    if (currentText === nextText) {
      unchangedFiles.push(filename);
      continue;
    }

    await fs.writeFile(targetPath, nextText, "utf8");
    changedFiles.push(filename);
  }

  return {
    sourceDir,
    targetDir,
    changedFiles,
    unchangedFiles,
  };
}

async function main() {
  const result = await sync13fData();
  console.log(JSON.stringify(result, null, 2));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error("[sync-13f-data] failed");
    console.error(error);
    process.exitCode = 1;
  });
}
