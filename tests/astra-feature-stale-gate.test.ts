import { describe, expect, it } from "vitest";
import { buildAstraFeaturePacket, evaluateAstraOpportunityGate, type TriggerEvent } from "@pm/core";
import { now, snapshot } from "./fixtures.js";

function pair() {
  const up = snapshot({
    marketId: "stale-gate-market", tokenId: "stale-up", outcome: "UP", asset: "BTC",
    bids: [{ price: "0.47", quantity: "300" }], asks: [{ price: "0.48", quantity: "300" }],
  });
  const down = snapshot({
    marketId: "stale-gate-market", tokenId: "stale-down", outcome: "DOWN", asset: "BTC",
    bids: [{ price: "0.48", quantity: "300" }], asks: [{ price: "0.49", quantity: "300" }],
  });
  return { up, down };
}

function trigger(expiresAt: string): TriggerEvent {
  const { up } = pair();
  return {
    id: "stale-trigger", traceId: "stale-trace", marketId: up.marketId, asset: "BTC",
    source: "polymarket", kind: "SPREAD_CHANGE", severity: 1, score: 1,
    dedupeKey: "stale-dedupe", evidence: {}, sourceEventIds: [up.eventId], evidenceIds: [up.eventId],
    createdAt: new Date(now.getTime() - 40_000).toISOString(), expiresAt,
  };
}

describe("Astra stale trigger admission", () => {
  it("builds an expired packet and rejects it at the deterministic gate before provider dispatch", () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({
      trigger: trigger(new Date(now.getTime() - 1).toISOString()),
      up, down, reference: null, now,
    });

    expect(packet).not.toBeNull();
    expect(packet?.timeToExpiryMs).toBe(0);
    expect(evaluateAstraOpportunityGate({
      featurePacket: packet!,
      now,
      marketSynchronized: true,
      triggerProvenanceValid: true,
      minDepthUsd: 25,
      maxSpreadBps: 1_200,
    })).toMatchObject({ accepted: false, reason: "STALE" });
  });

  it("still fails feature extraction for an invalid expiry timestamp", () => {
    const { up, down } = pair();
    expect(buildAstraFeaturePacket({
      trigger: trigger("not-a-date"),
      up, down, reference: null, now,
    })).toBeNull();
  });
});
