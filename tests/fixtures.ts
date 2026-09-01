import { createId, feeScheduleSchema, marketSnapshotSchema, paperOrderSchema, type MarketSnapshot, type PaperFill, type RiskInput } from "@pm/core";

export const now = new Date("2026-08-31T12:00:00.000Z");
export const feeSchedule = feeScheduleSchema.parse({ rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0.2", source: "configured_test_fixture", effectiveAt: now.toISOString() });

export function snapshot(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return marketSnapshotSchema.parse({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-15m", tokenId: "up", outcome: "UP",
    bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "5" }, { price: "0.50", quantity: "10" }],
    lastTradePrice: "0.48", tickSize: "0.01", minOrderSize: "1", feeSchedule,
    sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "h1", ...overrides,
  });
}

export function order(type: "MAKER" | "TAKER" = "TAKER", overrides: Record<string, unknown> = {}) {
  return paperOrderSchema.parse({ id: createId("order"), traceId: createId("trace"), triggerId: createId("trigger"), strategyId: "temporal-pair-hedge", marketId: "btc-15m", tokenId: "up", outcome: "UP", side: "BUY", orderType: type, requestedPrice: type === "TAKER" ? "0.51" : "0.47", quantity: "10", status: "PENDING", createdAt: now.toISOString(), ...overrides });
}

export function fill(outcome: "UP" | "DOWN", price: string, quantity = "10", side: "BUY" | "SELL" = "BUY"): PaperFill {
  return { id: createId("fill"), orderId: createId("order"), traceId: createId("trace"), marketId: "btc-15m", tokenId: outcome.toLowerCase(), outcome, side, requestedPrice: price, fillPrice: price, quantity, liquidityRole: "TAKER", latencyMs: 100, slippageUsd: "0", feeUsd: "0", timestamp: now.toISOString(), sourceMarketEventId: createId("event"), fillConfidence: 1, fillModel: "test", feeSchedule };
}

export function safeRisk(overrides: Partial<RiskInput> = {}): RiskInput {
  return { traceId: createId("trace"), now, marketFeedAt: now, btcFeedAt: now, databaseAvailable: true, timestampDisorder: false, riskStateAvailable: true, dailyPnlUsd: "0", drawdownUsd: "0", orderSizeUsd: "5", marketExposureUsd: "0", totalExposureUsd: "0", directionalExposureUsd: "0", unhedgedExposureUsd: "0", unhedgedDurationMs: 0, availableLiquidityUsd: "1000", expectedEdge: 0.05, spread: 0.01, signalConfidence: 0.9, signalExpiresAt: new Date(now.getTime() + 60_000), externalSignalsLastMinute: 0, lastStrategyTradeAt: null, duplicateTrade: false, feeScheduleKnown: true, experimentEntryOpen: true, executionErrorsLastHour: 0, externalRiskRecommendedVeto: false, shuttingDown: false, ...overrides };
}
