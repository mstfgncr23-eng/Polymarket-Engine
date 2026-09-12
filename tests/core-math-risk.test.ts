import { describe, expect, it } from "vitest";
import { PaperLedger, calculateFee, calculatePairMetrics, calculateSlippageUsd, calculateUnrealizedPnl, createId, evaluateNativeDecision, evaluateRisk, fairProbabilityOutputSchema, riskLimitsSchema, triggerEventSchema, walkBook } from "@pm/core";
import { fill, feeSchedule, now, safeRisk } from "./fixtures.js";

describe("deterministic math and accounting", () => {
  it("walks depth and computes VWAP/slippage", () => {
    const result = walkBook([{ price: "0.48", quantity: "5" }, { price: "0.50", quantity: "10" }], "10", "0.51");
    expect(result).toMatchObject({ filledQuantity: "10", vwap: "0.49", sufficientDepth: true, levelsConsumed: 2 });
    expect(calculateSlippageUsd("0.48", result.vwap, "10")).toBe("0.1");
    expect(calculateSlippageUsd("0.50", "0.49", "10", "BUY")).toBe("0");
  });

  it("uses the market fee curve and taker-only maker rule", () => {
    expect(calculateFee("10", "0.5", "TAKER", feeSchedule)).toBe("0.17500");
    expect(calculateFee("10", "0.5", "MAKER", feeSchedule)).toBe("0");
  });

  it("tracks a first leg, completed pair, unrealized and realized PnL", () => {
    const ledger = new PaperLedger(1000, now);
    let position = ledger.applyFill(fill("UP", "0.45"));
    expect(position.unpairedQuantity).toBe("10");
    position = ledger.applyFill(fill("DOWN", "0.48"));
    expect(calculatePairMetrics(position)).toMatchObject({ pairedQuantity: "10", combinedAcquisitionCostPerPair: "0.93", netPairEdge: "0.7" });
    expect(calculateUnrealizedPnl(position, "0.5", "0.5")).toBe("0.7");
    ledger.applyFill(fill("UP", "0.55", "5", "SELL"));
    expect(ledger.snapshot().realizedPnl).toBe("0.5");
  });
});

describe("native decisions and hard risk", () => {
  it("uses deterministic book/reference evidence instead of weighted agents", () => {
    const upBook = {
      eventId: createId("up"), traceId: "trace", marketId: "native-math", tokenId: "up", outcome: "UP" as const,
      bids: [{ price: "0.48", quantity: "80" }], asks: [{ price: "0.49", quantity: "20" }], lastTradePrice: "0.49",
      tickSize: "0.01", minOrderSize: "1", feeSchedule, sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "up",
    };
    const downBook = { ...upBook, eventId: createId("down"), tokenId: "down", outcome: "DOWN" as const, bids: [{ price: "0.50", quantity: "20" }], asks: [{ price: "0.51", quantity: "80" }], hash: "down" };
    const trigger = triggerEventSchema.parse({
      id: createId("trigger"), traceId: "trace", marketId: "native-math", asset: "BTC", source: "polymarket",
      kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "native-math", evidence: {}, sourceEventIds: [upBook.eventId],
      evidenceIds: [upBook.eventId], createdAt: now.toISOString(), expiresAt: new Date(now.getTime() + 60_000).toISOString(),
    });
    const result = evaluateNativeDecision({
      trigger, up: upBook, down: downBook,
      reference: { asset: "BTC", venue: "binance", freshness: "fresh", freshnessMs: 0, recentMoveBps: 15, recentVolatilityBps: 2 },
      referenceRequired: true, marketSynchronized: true, triggerProvenanceValid: true, now,
    });
    expect(result?.direction).toBe("UP");
    expect(result?.featureVector.referenceStrength).toBe(0.5);
    expect(result?.confidence).toBeGreaterThan(0.5);
  });

  it.each([
    [{ marketFeedAt: new Date(now.getTime() - 20_000) }, "MARKET_FEED_STALE"],
    [{ signalExpiresAt: now }, "SIGNAL_EXPIRED"],
    [{ unhedgedExposureUsd: "49", orderSizeUsd: "5" }, "MAX_UNHEDGED_EXPOSURE"],
    [{ databaseAvailable: false }, "DATABASE_UNAVAILABLE"],
    [{ duplicateTrade: true }, "DUPLICATE_TRADE"],
  ] as const)("vetoes unsafe state %#", (change, code) => {
    expect(evaluateRisk(safeRisk(change), riskLimitsSchema.parse({})).vetoes).toContain(code);
  });

  it("enforces the external signal-rate veto", () => {
    const limits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: 2 });
    expect(evaluateRisk(safeRisk({ externalSignalsLastMinute: 2 }), limits).vetoes).toContain("MAXIMUM_EXTERNAL_SIGNAL_RATE");
  });

  it("rejects malicious model-shaped output before deterministic risk", () => {
    const malicious = { direction: "UP", fairProbability: 3, marketProbability: 0.5, estimatedEdge: 99, confidence: 2, evidenceIds: [], expiresAt: "never", reasonCodes: [], placeLiveOrder: true };
    expect(fairProbabilityOutputSchema.safeParse(malicious).success).toBe(false);
    expect(evaluateRisk(safeRisk({ externalRiskRecommendedVeto: true }), riskLimitsSchema.parse({})).accepted).toBe(false);
  });
});
