import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("guardian shadow harness", () => {
  it("python sources compile", () => {
    const files = [
      resolve("research/guardian-shadow/guardian_shadow_24h.py"),
      resolve("research/guardian-shadow/analyze_shadow.py"),
    ];
    const result = spawnSync("python3", ["-m", "py_compile", ...files], {
      encoding: "utf8",
    });
    expect(result.status, result.stderr || result.stdout).toBe(0);
  });

  it("observer stays read-only", () => {
    const text = readFileSync(
      resolve("research/guardian-shadow/guardian_shadow_24h.py"),
      "utf8",
    );
    expect(text).toContain("TWO_STAGE_GUARDIAN_V1_LOCKED");
    expect(text).toContain("wss://ws-subscriptions-clob.polymarket.com/ws/market");
    expect(text).not.toContain("py_clob_client");
    expect(text).not.toContain("create_order");
    expect(text).not.toContain("post(");
    expect(text).not.toContain("/api/operator/");
  });
});
