import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { createTextFetcher } from "../src/fetch-text.ts";

const response = (body: string, status = 200) => `${body}\n__USVM_HTTP_STATUS__:${status}`;

test("one deadline covers retries and direct fallback; partial timeout bodies are discarded", async () => {
  let clock = 0;
  const budgets: number[] = [];
  const fetch = createTextFetcher({
    userAgent: "test", directFallback: true, requestBudgetMs: 100,
    now: () => clock, sleep: async (ms) => { clock += ms; }, warn: () => {},
    run: async (_args, budget) => {
      budgets.push(budget);
      clock += Math.min(60, budget);
      throw Object.assign(new Error("timeout"), { code: 28, stdout: "Date,Close\n2026-09-22,100" });
    },
  });
  await assert.rejects(fetch("https://slow.test/history", 3, 32000, true));
  assert.deepEqual(budgets, [100, 40]);
  assert.equal(clock, 100);
});

test("rate-limited source stops retrying and opens circuit while other sources still work", async () => {
  let calls = 0;
  const fetch = createTextFetcher({
    userAgent: "test", warn: () => {},
    run: async (args) => { calls++; return args.at(-1)!.includes("slow.test") ? response("Too Many Requests", 429) : response("data"); },
  });
  for (let i = 0; i < 3; i++) await assert.rejects(fetch(`https://slow.test/${i}`, 3));
  await assert.rejects(fetch("https://slow.test/next"), /Source unavailable/);
  assert.equal(calls, 3);
  assert.equal(await fetch("https://healthy.test/next"), "data");
});

test("symbol-specific HTTP errors do not disable the host or retry", async () => {
  let calls = 0;
  const fetch = createTextFetcher({
    userAgent: "test", warn: () => {},
    run: async (args) => { calls++; return args.at(-1)!.endsWith("valid") ? response("data") : response("missing", 404); },
  });
  for (let i = 0; i < 4; i++) await assert.rejects(fetch(`https://source.test/${i}`, 3));
  assert.equal(calls, 4);
  assert.equal(await fetch("https://source.test/valid"), "data");
});

test("a successful request resets consecutive failures and headers survive", async () => {
  const results = [503, 503, 200, 503, 503, 200];
  const fetch = createTextFetcher({
    userAgent: "test", warn: () => {},
    run: async (args) => {
      assert.ok(args.includes("x-test: value"));
      return response("body", results.shift());
    },
  });
  for (const succeeds of [false, false, true, false, false, true]) {
    const result = fetch("https://source.test/data", 0, 1000, false, { headers: ["x-test: value"] });
    if (succeeds) assert.equal(await result, "body");
    else await assert.rejects(result);
  }
  assert.equal(results.length, 0);
});

test("a quota error returned with HTTP 200 is rejected and does not consume retries", async () => {
  let calls = 0;
  const fetch = createTextFetcher({ userAgent: "test", warn: () => {}, run: async () => {
    calls++; return response("You have exceeded the daily hits limit");
  } });
  await assert.rejects(fetch("https://source.test/data", 3));
  assert.equal(calls, 1);
});

test("HTTP 200 browser verification pages fall back without treating HTML as data", async () => {
  let calls = 0;
  const fetch = createTextFetcher({ userAgent: "test", warn: () => {}, run: async () => {
    calls++; return response("<noscript>This site requires JavaScript to verify your browser.</noscript>");
  } });
  for (let i = 0; i < 4; i++) await assert.rejects(fetch(`https://stooq.test/${i}`, 3));
  assert.equal(calls, 3);
});

test("real curl validates HTTP status and enforces deadline on a hanging response", async (t) => {
  const server = createServer((req, res) => {
    if (req.url === "/hang") { res.writeHead(200); res.write("partial data"); return; }
    if (req.url === "/missing") { res.writeHead(404); res.end("Not found"); return; }
    res.end("complete data");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); server.close(); });
  const address = server.address() as { port: number };
  const base = `http://127.0.0.1:${address.port}`;
  const fetch = createTextFetcher({ userAgent: "test", requestBudgetMs: 200, warn: () => {} });
  assert.equal(await fetch(`${base}/ok`), "complete data");
  await assert.rejects(fetch(`${base}/missing`), /HTTP 404/);
  const started = Date.now();
  await assert.rejects(fetch(`${base}/hang`, 3), /transport failure/);
  assert.ok(Date.now() - started < 2000);
});
