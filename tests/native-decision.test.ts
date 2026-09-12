import { describe, expect, it } from "vitest";
import {
  evaluateNativeDecision,
  marketSnapshotSchema,
  triggerEventSchema,
  type MarketSnapshot,
  type NativeReferenceFeatures,
} from "@pm/core";
import { snapshot } from "./fixtures.js";

const now = new Date("2026-08-31T12:00:00.000Z");

function trigger() {
  return triggerEventSchema.parse({
    id: "trigger-native", traceId: "trace-native", marketId: "market-native", asset: "BTC",
    source: "polymarket", kind: "BOOK_IMBALANCE", severity: 2, score: 0.9,
    dedupeKey: "native-test", evidence: { test: true }, sourceEventIds: ["source-native"],
    evidenceIds: ["evidence-native"], createdAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 30_000).toISOString(),
  });
}

function book(outcome: "UP" | "DOWN", bidQuantity: string, askQuantity: string, overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return marketSnapshotSchema.parse(snapshot({
    marketId: "market-native", tokenId: outcome === "UP" ? "native-up" : "native-down", outcome,
    bids: [{ price: "0.47", quantity: bidQuantity }],
    asks: [{ price: "0.49", quantity: askQuantity }],
    ...overrides,
  }));
}

function reference(recentMoveBps: number, freshness: NativeReferenceFeatures["freshness"] = "fresh"): NativeReferenceFeatures {
  return {
    asset: "BTC", venue: "binance", freshness, freshnessMs: 100,
    recentMoveBps, recentVolatilityBps: Math.abs(recentMoveBps) / 2,
    referenceWindowMs: 60_000, referenceSourceTimestamp: now.toISOString(),
  };
}

function evaluate(up: MarketSnapshot, down: MarketSnapshot, ref: NativeReferenceFeatures | null = reference(0)) {
  return evaluateNativeDecision({
    trigger: trigger(), up, down, reference: ref, referenceRequired: true,
    marketSynchronized: true, triggerProvenanceValid: true, now,
  });
}

describe("native deterministic decision model", () => {
  it("returns UP for strong aligned UP evidence", () => {
    const result = evaluate(book("UP", "100", "1"), book("DOWN", "1", "100"), reference(30));
    expect(result?.direction).toBe("UP");
    expect(result?.confidence).toBeGreaterThan(0.9);
  });

  it("returns DOWN for strong aligned DOWN evidence", () => {
    const result = evaluate(book("UP", "1", "100"), book("DOWN", "100", "1"), reference(-30));
    expect(result?.direction).toBe("DOWN");
    expect(result?.confidence).toBeGreaterThan(0.9);
  });

  it("returns FLAT for weak evidence", () => {
    const result = evaluate(book("UP", "50", "50"), book("DOWN", "50", "50"), reference(0));
    expect(result?.direction).toBe("FLAT");
  });

  it("reduces confidence and avoids forcing a direction on disagreement", () => {
    const result = evaluate(book("UP", "100", "1"), book("DOWN", "1", "100"), reference(-30));
    expect(result?.direction).toBe("FLAT");
    expect(result?.confidence).toBeLessThan(0.9);
  });

  it("keeps fair probability bounded", () => {
    const result = evaluate(book("UP", "100", "1", { bids: [{ price: "0", quantity: "100" }], asks: [{ price: "1", quantity: "1" }] }), book("DOWN", "1", "100", { bids: [{ price: "0", quantity: "1" }], asks: [{ price: "1", quantity: "100" }] }), reference(30));
    expect(result?.fairProbability).toBeGreaterThanOrEqual(0.01);
    expect(result?.fairProbability).toBeLessThanOrEqual(0.99);
    expect(result?.featureVector.fairProbabilityUp).toBeGreaterThanOrEqual(0.01);
    expect(result?.featureVector.fairProbabilityUp).toBeLessThanOrEqual(0.99);
  });

  it("is deterministic for identical input", () => {
    const up = book("UP", "100", "1");
    const down = book("DOWN", "1", "100");
    expect(evaluate(up, down, reference(12))).toEqual(evaluate(up, down, reference(12)));
  });

  it("returns no actionable decision when a book side is missing", () => {
    const up = book("UP", "100", "1", { asks: [] });
    expect(evaluate(up, book("DOWN", "1", "100"))).toBeNull();
  });

  it("returns no actionable decision for malformed or crossed top-of-book data", () => {
    const crossed = book("UP", "100", "1", {
      bids: [{ price: "0.60", quantity: "100" }],
      asks: [{ price: "0.40", quantity: "1" }],
    });
    const nonfinite = book("UP", "100", "1");
    nonfinite.bids[0]!.price = "NaN" as never;
    expect(evaluate(crossed, book("DOWN", "1", "100"))).toBeNull();
    expect(evaluate(nonfinite, book("DOWN", "1", "100"))).toBeNull();
  });

  it("returns no actionable decision for a stale required reference", () => {
    expect(evaluate(book("UP", "100", "1"), book("DOWN", "1", "100"), reference(30, "stale"))).toBeNull();
  });
});
