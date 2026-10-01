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
    expect(text).toContain("NORMAL_HEDGE_PRIORITY");
    expect(text).toContain('NORMAL_MIN_LOCKED_EDGE = Decimal("0.015")');
    expect(text).toContain('TRIGGER_LOSS = Decimal("0.35")');
    expect(text).toContain('PAIR_LOCK_CAP = Decimal("0.60")');
    expect(text).toContain("book.received_mono_ns >= target_ns");
    expect(text).toContain("book.received_mono_ns <= deadline_ns");
    expect(text).toContain('status = "KNOWN" if observed and sell_executable else "UNKNOWN"');
    expect(text).toContain("wss://ws-subscriptions-clob.polymarket.com/ws/market");
    expect(text).not.toContain("py_clob_client");
    expect(text).not.toContain("create_order");
    expect(text).not.toContain("post(");
    expect(text).not.toContain("/api/operator/");
  });
});
