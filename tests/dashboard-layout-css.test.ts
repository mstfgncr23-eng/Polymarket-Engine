import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

describe("dashboard layout CSS contract", () => {
  it("locks html/body overflow hidden at min-width 1200px", () => {
    const css = readFileSync(resolve("apps/dashboard/app/globals.css"), "utf8");
    expect(css).toMatch(/@media\s*\(\s*min-width:\s*1200px\s*\)\s*\{[\s\S]*?html,\s*body\s*\{[^}]*overflow:\s*hidden/);
  });
});