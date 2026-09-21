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

  it("walks SELL depth from best bid down to the sell limit", () => {
    const result = walkBook(
      [{ price: "0.41", quantity: "5" }, { price: "0.40", quantity: "5" }, { price: "0.39", quantity: "50" }],
      "10",
      "0.40",
      "SELL",
    );
    expect(result).toMatchObject({
      filledQuantity: "10",
      vwap: "0.405",
      sufficientDepth: true,
      levelsConsumed: 2,
      worstPrice: "0.4",
    });
    expect(calculateSlippageUsd("0.40", result.vwap, "10", "SELL")).toBe("0");
  });

  it("tracks each unhedged episode independently and removes a fully unwound position", () => {
    const ledger = new PaperLedger(1000, now);
    const first = { ...fill("UP", "0.45", "10"), timestamp: "2026-08-31T12:00:00.000Z" };
    let position = ledger.applyFill(first);
    expect(position.unhedgedStartedAt).toBe(first.timestamp);

    const sameEpisode = { ...fill("UP", "0.46", "2"), timestamp: "2026-08-31T12:01:00.000Z" };
    position = ledger.applyFill(sameEpisode);
    expect(position.unhedgedStartedAt).toBe(first.timestamp);

    const pair = { ...fill("DOWN", "0.50", "12"), timestamp: "2026-08-31T12:02:00.000Z" };
    position = ledger.applyFill(pair);
    expect(position.unpairedQuantity).toBe("0");
    expect(position.unhedgedStartedAt).toBeNull();

    const reopen = { ...fill("DOWN", "0.49", "2", "SELL"), timestamp: "2026-08-31T12:03:00.000Z" };
    position = ledger.applyFill(reopen);
    expect(position.unpairedOutcome).toBe("UP");
    expect(position.unpairedQuantity).toBe("2");
    expect(position.unhedgedStartedAt).toBe(reopen.timestamp);

    const closeResidual = { ...fill("UP", "0.44", "2", "SELL"), timestamp: "2026-08-31T12:04:00.000Z" };
    position = ledger.applyFill(closeResidual);
    expect(position.unpairedQuantity).toBe("0");
    expect(position.unhedgedStartedAt).toBeNull();
    expect(ledger.positions.get("btc-15m")?.pairedQuantity).toBe("10");

    const sellPairedUp = { ...fill("UP", "0.44", "10", "SELL"), timestamp: "2026-08-31T12:05:00.000Z" };
    ledger.applyFill(sellPairedUp);
    const sellPairedDown = { ...fill("DOWN", "0.44", "10", "SELL"), timestamp: "2026-08-31T12:06:00.000Z" };
    ledger.applyFill(sellPairedDown);
    expect(ledger.positions.has("btc-15m")).toBe(false);
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
      reference: { asset: "BTC", venue: "binance", freshness: "fresh", freshnessMs: 0, recentMoveBps: 15, recentVolatilityBps: 2, referenceWindowMs: 60_000, referenceSourceTimestamp: now.toISOString() },
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

  it("requires at least 2% expected ROI for a fresh first leg", () => {
    const limits = riskLimitsSchema.parse({});
    expect(limits.minExpectedROI).toBe(0.02);
    const rejected = evaluateRisk(safeRisk({ intent: "OPEN_FIRST_LEG", expectedROI: 0.019 }), limits);
    expect(rejected.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    const accepted = evaluateRisk(safeRisk({ intent: "OPEN_FIRST_LEG", expectedROI: 0.02 }), limits);
    expect(accepted.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
  });

  it("does not apply the first-leg ROI floor to a risk-reducing hedge", () => {
    const decision = evaluateRisk(safeRisk({
      intent: "HEDGE_SECOND_LEG",
      expectedROI: 0,
      expectedEdge: 0.05,
    }), riskLimitsSchema.parse({}));
    expect(decision.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
  });

  it("applies strategy cooldown to fresh first legs but not risk-reducing hedges", () => {
    const limits = riskLimitsSchema.parse({ strategyCooldownMs: 60_000 });
    const recentTradeAt = new Date(now.getTime() - 1_000);

    const firstLeg = evaluateRisk(safeRisk({
      intent: "OPEN_FIRST_LEG",
      lastStrategyTradeAt: recentTradeAt,
    }), limits);
    expect(firstLeg.vetoes).toContain("STRATEGY_COOLDOWN");

    const hedge = evaluateRisk(safeRisk({
      intent: "HEDGE_SECOND_LEG",
      lastStrategyTradeAt: recentTradeAt,
    }), limits);
    expect(hedge.vetoes).not.toContain("STRATEGY_COOLDOWN");
  });

  it("allows a monotonic protective unwind through alpha gates but keeps hard safety", () => {
    const limits = riskLimitsSchema.parse({});
    const protective = safeRisk({
      intent: "PROTECTIVE_DE_RISK",
      marketExposureUsd: "50",
      assetExposureUsd: "50",
      totalExposureUsd: "80",
      directionalExposureUsd: "20",
      unhedgedExposureUsd: "20",
      projectedMarketExposureUsd: "45",
      projectedAssetExposureUsd: "45",
      projectedTotalExposureUsd: "75",
      projectedDirectionalExposureUsd: "15",
      projectedUnhedgedExposureUsd: "15",
      unhedgedDurationMs: limits.maxUnhedgedDurationMs + 1,
      dailyPnlUsd: "-9999",
      drawdownUsd: "9999",
      availableLiquidityUsd: "0.01",
      expectedEdge: -1,
      expectedROI: -1,
      spread: 1,
      signalConfidence: 0,
      signalExpiresAt: now,
      externalSignalsLastMinute: 999,
      lastStrategyTradeAt: now,
      duplicateTrade: true,
      experimentEntryOpen: false,
      marketEntryQualified: false,
      externalRiskRecommendedVeto: true,
      referenceRequired: true,
      referenceProviderBlocked: true,
      referenceFeedAt: new Date(now.getTime() - 60_000),
      availableCashUsd: "0",
      requiredCashUsd: "0",
    });
    expect(evaluateRisk(protective, limits).vetoes).toEqual([]);

    expect(evaluateRisk({
      ...protective,
      projectedUnhedgedExposureUsd: protective.unhedgedExposureUsd,
    }, limits).vetoes).toContain("PROTECTIVE_DE_RISK_NOT_REDUCING");

    expect(evaluateRisk({
      ...protective,
      marketFeedAt: new Date(now.getTime() - limits.staleFeedTimeoutMs - 1),
    }, limits).vetoes).toContain("MARKET_FEED_STALE");

    expect(evaluateRisk({
      ...protective,
      feeScheduleKnown: false,
    }, limits).vetoes).toContain("UNKNOWN_FEE_SCHEDULE");

    for (const [change, veto] of [
      [{ databaseAvailable: false }, "DATABASE_UNAVAILABLE"],
      [{ timestampDisorder: true }, "EVENT_TIMESTAMP_DISORDER"],
      [{ riskStateAvailable: false }, "RISK_STATE_UNAVAILABLE"],
      [{ executionErrorsLastHour: limits.maxExecutionErrorsPerHour }, "ABNORMAL_EXECUTION_ERRORS"],
      [{ shuttingDown: true }, "SHUTTING_DOWN"],
    ] as const) {
      expect(evaluateRisk({ ...protective, ...change }, limits).vetoes).toContain(veto);
    }
  });

  it("rejects malicious model-shaped output before deterministic risk", () => {
    const malicious = { direction: "UP", fairProbability: 3, marketProbability: 0.5, estimatedEdge: 99, confidence: 2, evidenceIds: [], expiresAt: "never", reasonCodes: [], placeLiveOrder: true };
    expect(fairProbabilityOutputSchema.safeParse(malicious).success).toBe(false);
    expect(evaluateRisk(safeRisk({ externalRiskRecommendedVeto: true }), riskLimitsSchema.parse({})).accepted).toBe(false);
  });
});
