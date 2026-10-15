import { expect, it } from "vitest";
import { readFileSync } from "node:fs";
it.skipIf(!process.env.P0_FEED_BENCH_OUTPUT)("finite production normalizer and PAPER Engine capacity benchmark", async () => {
  await import("../scripts/p0-feed-capacity.js");
  const result=JSON.parse(readFileSync(process.env.P0_FEED_BENCH_OUTPUT!,"utf8"));
  expect(result.snapshots).toBe(120000);
  expect(result.fills).toBe(0);
},60000);
