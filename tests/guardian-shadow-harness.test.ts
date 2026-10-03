import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("guardian shadow harness", () => {
  it("python sources compile", () => {
    const files = [
      resolve("research/guardian-shadow/guardian_shadow_24h.py"),
      resolve("research/guardian-shadow/analyze_shadow.py"),
      resolve("research/guardian-shadow/test_guardian_shadow.py"),
    ];
    const result = spawnSync("python3", ["-m", "py_compile", ...files], {
      encoding: "utf8",
    });
    expect(result.status, result.stderr || result.stdout).toBe(0);
  });

  it("passive recorder unit tests pass without network access", () => {
    const result = spawnSync("python3", [
      resolve("research/guardian-shadow/test_guardian_shadow.py"),
    ], { encoding: "utf8" });
    expect(result.status, result.stderr || result.stdout).toBe(0);
  });

  it("vendors the exact user TEST_CONTRACT bytes", () => {
    const contract = readFileSync(resolve("research/guardian-shadow/TEST_CONTRACT.md"));
    const digest = createHash("sha256").update(contract).digest("hex");
    expect(digest).toBe("545fd28c6452d2b5af6ed69c74b1cb4aff1423562311e91f88b44650a83aa9a9");
  });

  it("analyzer preserves UNKNOWN and refuses incomplete numeric conclusions", () => {
    const text = readFileSync(
      resolve("research/guardian-shadow/analyze_shadow.py"),
      "utf8",
    );
    expect(text).toContain('run_complete.get("status") != "COMPLETE"');
    expect(text).toContain("full-policy delayed PnL = UNKNOWN; delayed meteor result = UNKNOWN");
    expect(text).toContain("no numeric conclusion is emitted from an incomplete run");
  });

  it("recorder has no independent Guardian policy or execution authority", () => {
    const text = readFileSync(
      resolve("research/guardian-shadow/guardian_shadow_24h.py"),
      "utf8",
    );
    expect(text).toContain("PASSIVE_ENGINE_GUARDIAN_RECORDER_V1");
    expect(text).toContain('"policy_authority": "Engine Guardian"');
    expect(text).toContain('"counterfactual_policy": False');
    expect(text).toContain('READ_PATHS = ("/api/dashboard", "/api/shadow/guardian-state", "/events")');
    expect(text).toContain('"request_latency_ns"');
    for (const removedPolicy of ["evaluate_market", "TRIGGER_LOSS", "PAIR_LOCK_CAP", "shadow_closed", "rescue_error_for_episode", "guaranteed_locked_pnl"]) {
      expect(text).not.toContain(removedPolicy);
    }
    expect(text).toContain("wss://ws-subscriptions-clob.polymarket.com/ws/market");
    expect(text).not.toContain("py_clob_client");
    expect(text).not.toContain("create_order");
    expect(text).not.toContain("post(");
    expect(text).not.toContain("session.request(");
    expect(text).not.toContain("/api/operator/");
  });
});
