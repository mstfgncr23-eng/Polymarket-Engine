import { existsSync, readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("P0-02 PAPER observer removal", () => {
  it("removes the standalone soak process and leaves deployment using the real Engine and Guardian", () => {
    expect(existsSync(resolve("scripts/soak-market-observer.py"))).toBe(false);
    const compose = readFileSync(resolve("deploy/guardian/compose.yml"), "utf8");
    expect(compose).toContain("TRADING_MODE: paper");
    expect(compose).toContain("dist/guardian-main.js");
    expect(compose).not.toContain("soak-market-observer");
    expect(compose).toContain("/data/archive:/data/archive");
    for (const name of readdirSync(resolve("scripts"))) expect(name).not.toBe("soak-market-observer.py");
  });
});
