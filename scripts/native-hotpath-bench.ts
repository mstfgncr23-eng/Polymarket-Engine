import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { benchmark, economicFixture } from "./native-hotpath-fixture.js";

// Same harness must be copied into the exact base worktree before comparison.
// node node_modules/tsx/dist/cli.mjs --tsconfig tsconfig.tests.json scripts/native-hotpath-bench.ts result.json 2048 0 [baseline.json]
const economics = await economicFixture();
const result = {
  sha: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  benchmark: await benchmark(Number(process.argv[3] ?? 2048), 8, Number(process.argv[4] ?? 0)),
  economicDigest: createHash("sha256").update(JSON.stringify(economics)).digest("hex"), economics,
  harnessDigest: createHash("sha256").update(readFileSync(new URL("./native-hotpath-fixture.ts", import.meta.url))).digest("hex"),
};
if (process.argv[5]) {
  const baseline = JSON.parse(readFileSync(process.argv[5], "utf8"));
  assert.deepEqual(economics, baseline.economics, "Native decision, risk, order, fill, ledger and trailing semantics changed");
  assert.equal(result.harnessDigest, baseline.harnessDigest, "baseline must use the identical harness");
  for (const key of ["node", "fixture", "markets", "eventCount", "journalDelayMs", "journalBackend"] as const)
    assert.equal(result.benchmark[key], baseline.benchmark[key], `benchmark ${key} differs`);
}
writeFileSync(process.argv[2] ?? "native-hotpath-result.json", JSON.stringify(result, null, 2));
console.log(JSON.stringify({ ...result, economics: undefined }, null, 2));
