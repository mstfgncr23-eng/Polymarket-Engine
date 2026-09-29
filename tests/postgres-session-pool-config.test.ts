import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

describe("postgres journal session-pool stability", () => {
  it("keeps long-lived VPS connections warm and retains a bounded connect timeout", () => {
    const source = readFileSync(resolve("packages/db/src/journal.ts"), "utf8");
    expect(source).toMatch(/max:\s*10/);
    expect(source).toMatch(/idle_timeout:\s*0/);
    expect(source).toMatch(/connect_timeout:\s*30/);
    expect(source).not.toMatch(/idle_timeout:\s*20/);
    expect(source).not.toMatch(/connect_timeout:\s*10/);
  });
});
