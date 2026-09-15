import { readFile, writeFile } from "node:fs/promises";

const path = new URL("../apps/engine/src/engine.ts", import.meta.url);
const source = await readFile(path, "utf8");
const needle = "experimentAllowsFirstLeg(this.experiment)";
const matches = source.split(needle).length - 1;

if (matches !== 3) {
  throw new Error(`EXPECTED_3_FIRST_LEG_CALLS_FOUND_${matches}`);
}

const next = source.replaceAll(needle, "experimentAllowsFirstLeg(this.experiment, this.nowFn())");
await writeFile(path, next, "utf8");
console.log(`updated ${matches} native first-leg call sites to use EngineService.nowFn()`);
