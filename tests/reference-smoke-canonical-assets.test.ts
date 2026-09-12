import { describe, expect, it } from "vitest";
import { canonicalAssetIdentity, extractUnderlyingAsset } from "@pm/core";
import { evaluateLiveFeedSmokeGate } from "../apps/engine/src/live-feed-smoke-gate.js";

describe("canonical economic asset identity", () => {
  it("canonicalizes the observed long-form crypto aliases at discovery boundaries", () => {
    expect(canonicalAssetIdentity("BITCOIN")).toBe("BTC");
    expect(canonicalAssetIdentity("ethereum")).toBe("ETH");
    expect(canonicalAssetIdentity("SOLANA")).toBe("SOL");
    expect(canonicalAssetIdentity("dogecoin")).toBe("DOGE");
    expect(canonicalAssetIdentity("ZEC")).toBe("ZEC");
  });

  it("applies canonical identity to both directional slugs and injected asset tags", () => {
    expect(extractUnderlyingAsset("bitcoin-up-or-down-5m", "Bitcoin Up or Down", ["crypto"])).toBe("BTC");
    expect(extractUnderlyingAsset("up-or-down-now", "Up or Down", ["crypto", "ETHEREUM"])).toBe("ETH");
  });
});

describe("live-feed smoke completion gate", () => {
  it("does not pass when Polymarket arrives before a supported reference tick", () => {
    const beforeReference = evaluateLiveFeedSmokeGate({
      polymarketMessages: 1,
      referenceMessages: { BTC: 0 },
      referenceStatuses: ["SUPPORTED"],
    });
    expect(beforeReference).toMatchObject({ settled: false, ok: false, referenceExpected: true, referenceMessageCount: 0 });

    const afterReference = evaluateLiveFeedSmokeGate({
      polymarketMessages: 1,
      referenceMessages: { BTC: 1 },
      referenceStatuses: ["SUPPORTED"],
    });
    expect(afterReference).toMatchObject({ settled: true, ok: true, referenceMessageCount: 1 });
  });

  it("fails immediately for transient/provider resolution failures but preserves definitive unsupported fallback", () => {
    expect(evaluateLiveFeedSmokeGate({
      polymarketMessages: 1,
      referenceMessages: {},
      referenceStatuses: ["TRANSIENT_FAILURE"],
    })).toMatchObject({ settled: true, ok: false, referenceResolutionBlocked: true });

    expect(evaluateLiveFeedSmokeGate({
      polymarketMessages: 1,
      referenceMessages: {},
      referenceStatuses: ["UNSUPPORTED_DEFINITIVE"],
    })).toMatchObject({ settled: true, ok: true, referenceExpected: false });
  });
});
