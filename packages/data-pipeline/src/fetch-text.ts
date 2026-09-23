import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const STATUS_MARKER = "\n__USVM_HTTP_STATUS__:";

export function isRejectedPayload(text: string): boolean {
  const raw = String(text || "").trim().toLowerCase();
  return !raw || [
    "exceeded the daily hits limit", "too many requests", "sad-panda-201402200631.png",
    "no longer be accessible from mainland china", "enable javascript and cookies to continue",
    "<title>just a moment", "cf-chl", "<title>access denied", "<title>verifying",
    "requires javascript to verify your browser",
  ].some((message) => raw.includes(message));
}

interface FetchOptions { headers?: string[]; userAgent?: string }
interface FetcherOptions {
  userAgent: string;
  directFallback?: boolean;
  requestBudgetMs?: number;
  failureThreshold?: number;
  run?: (args: string[], timeoutMs: number) => Promise<string>;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  warn?: (message: string) => void;
}

// State belongs to one build. A persistently unavailable source is skipped for
// the rest of that build; the next daily run probes it again normally.
export function createTextFetcher(options: FetcherOptions) {
  const failures = new Map<string, number>();
  const disabled = new Set<string>();
  const now = options.now || Date.now;
  const sleep = options.sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const warn = options.warn || console.warn;
  const run = options.run || (async (args, timeoutMs) => {
    const { stdout } = await execFileAsync("curl", args, {
      maxBuffer: 24 * 1024 * 1024,
      timeout: timeoutMs + 1000,
      killSignal: "SIGKILL",
    });
    return stdout;
  });

  return async function fetchText(
    url: string, retries = 1, timeoutMs = 12000, directFirst = false, request: FetchOptions = {}
  ): Promise<string> {
    const origin = new URL(url).origin;
    const started = now();
    // Retries and proxy/direct attempts share a single deadline, rather than
    // each multiplying a 20–32 second timeout across 100 companies.
    const budgetMs = Math.min(timeoutMs, options.requestBudgetMs ?? 15000);
    const deadline = started + budgetMs;
    const routes = options.directFallback ? (directFirst ? [true, false] : [false, true]) : [false];
    let lastError = "request budget exhausted";
    let sourceFailure = false;
    let stop = false;
    for (let attempt = 0; attempt <= retries && !stop; attempt += 1) {
      for (const direct of routes) {
        if (disabled.has(origin)) throw new Error(`Source unavailable for this build: ${origin}`);
        const remaining = deadline - now();
        if (remaining <= 0) { stop = true; break; }
        const args = [
          ...(direct ? ["--noproxy", "*"] : []),
          "-4", "-sSL", "--compressed", "--connect-timeout", String(Math.min(5, remaining / 1000)),
          "--max-time", String(remaining / 1000), "-A", request.userAgent || options.userAgent,
          "-H", "accept-language: en-US,en;q=0.9",
          ...(request.headers || []).flatMap((header) => ["-H", header]),
          "--write-out", `${STATUS_MARKER}%{http_code}`, url,
        ];
        try {
          const stdout = await run(args, remaining);
          const marker = stdout.lastIndexOf(STATUS_MARKER);
          const status = marker >= 0 ? Number(stdout.slice(marker + STATUS_MARKER.length)) : 0;
          const body = (marker >= 0 ? stdout.slice(0, marker) : "").trim();
          if (status >= 200 && status < 300 && !isRejectedPayload(body)) {
            failures.delete(origin);
            return body;
          }
          lastError = `HTTP ${status}${isRejectedPayload(body) ? " rejected/empty payload" : ""}`;
          sourceFailure = status === 429 || status >= 500 || status === 0 ||
            (status >= 200 && status < 300 && isRejectedPayload(body));
          // Missing symbols/auth-only endpoints must not disable a whole host.
          if (status >= 400 && status < 500) { stop = true; break; }
          if (sourceFailure && body) { stop = true; break; }
        } catch (error) {
          // Never accept a partial body from a timed-out curl as valid data.
          const code = (error as { code?: unknown }).code;
          lastError = `transport failure (${String(code || "timeout")})`;
          sourceFailure = true;
        }
      }
      if (!stop && attempt < retries) {
        const remaining = deadline - now();
        if (remaining > 0) await sleep(Math.min(280 * (attempt + 1), remaining));
      }
    }
    if (sourceFailure) {
      const count = (failures.get(origin) || 0) + 1;
      failures.set(origin, count);
      if (count >= (options.failureThreshold ?? 3) && !disabled.has(origin)) {
        disabled.add(origin);
        warn(`[fetch] disabled ${origin} for this build after ${count} consecutive failures; using fallback sources`);
      }
    }
    if (sourceFailure || now() - started >= 5000) {
      warn(`[fetch] ${new URL(url).host}${new URL(url).pathname}: ${lastError}; elapsed=${now() - started}ms`);
    }
    throw new Error(`Failed to fetch ${url}: ${lastError}`);
  };
}
