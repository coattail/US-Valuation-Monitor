import test from "node:test";
import assert from "node:assert/strict";

import { parseWsjRowPeValues } from "../src/generate.ts";

test("parseWsjRowPeValues ignores index label numbers in WSJ table cells", () => {
  assert.deepEqual(parseWsjRowPeValues(["NASDAQ 100 Index", "33.38", "28.18", "25.15"], "nasdaq100"), {
    trailing: 33.38,
    forward: 25.15,
  });

  assert.deepEqual(parseWsjRowPeValues(["Dow Jones Industrial Average", "23.4", "21.5", "19.8"], "dow30"), {
    trailing: 23.4,
    forward: 19.8,
  });
});

test("parseWsjRowPeValues handles single-line WSJ text rows", () => {
  assert.deepEqual(parseWsjRowPeValues(["NASDAQ 100 Index 33.38 28.18 25.15"], "nasdaq100"), {
    trailing: 33.38,
    forward: 25.15,
  });

  assert.deepEqual(parseWsjRowPeValues(["Russell 2000 Index 38.75 33.01 27.27"], "russell2000"), {
    trailing: 38.75,
    forward: 27.27,
  });
});
