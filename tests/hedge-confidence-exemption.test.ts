import { describe, expect, it } from "vitest";
import {
  createId,
  decidePaperOrder,
  evaluateRisk,
  riskLimitsSchema,
  verifiedReducingHedge,
  type ConsensusDecision,
  type RiskInput,
  type StrategyCandidate,
} from "@pm/core";
import { now, safeRisk, snapshot } from "./fixtures.js";

const limits = riskLimitsSchema.parse({});

// Historical 4886202 fixture (eth-updown-5m-1790274900):
// existing first leg DOWN qty 5.88235294 @ 0.85; opposite UP ask 0.12 with
// materially deeper visible book; signalConfidence 0.5191637586 < 0.60.
const MARKET = "4886202";
const RESIDUAL = "5.88235294";
const HEDGE_UP_ASK = "0.12";
const LOW_CONFIDENCE = 0.5191637586;
const HEDGE_NOTIONAL = "0.705883"; // 5.88235294 * 0.12

const hedgeCandidate: StrategyCandidate = {
  strategyId: "temporal-pair-hedge",
  strategyVersion: "v1",
  marketId: MARKET,
  tokenId: "yes-token",
  outcome: "UP",
  side: "BUY",
  quantity: RESIDUAL,
  limitPrice: HEDGE_UP_ASK,
  intent: "HEDGE_SECOND_LEG",
  rawExpectedEdge: 0.03,
  reasonCodes: ["UNPAIRED_INVENTORY", "LOCKABLE_PAIR_EDGE"],
};

const hedgeFacts = { marketId: MARKET, outcome: "UP" as const, side: "BUY" as const, quantity: RESIDUAL };
const positionFacts = { marketId: MARKET, unpairedOutcome: "DOWN" as const, unpairedQuantity: RESIDUAL };

function hedgeRisk(overrides: Partial<BaseHedgeRisk>): BaseHedgeRisk {
  return {
    ...safeRisk({ intent: "HEDGE_SECOND_LEG" }),
    orderSizeUsd: HEDGE_NOTIONAL,
    requiredCashUsd: HEDGE_NOTIONAL,
    availableCashUsd: "10",
    directionalExposureUsd: "0.5",
    unhedgedExposureUsd: "4.9995",
    projectedDirectionalExposureUsd: "0",
    projectedUnhedgedExposureUsd: "0",
    hedgeCandidate: hedgeFacts,
    hedgePosition: positionFacts,
    ...overrides,
  };
}

interface BaseHedgeRisk extends RiskInput {
  hedgeCandidate: typeof hedgeFacts | null;
  hedgePosition: typeof positionFacts | null;
}

function consensus(confidence: number): ConsensusDecision {
  return {
    id: createId("consensus"), traceId: "t", triggerId: "g", consensusDirection: "UP",
    consensusConfidence: confidence, agentAgreement: 1, signalFreshness: 1, conflicts: [],
    estimatedEdge: 0.03, evidenceIds: ["e1"], calculatedAt: now.toISOString(),
    authority: "NATIVE_DETERMINISTIC_V1", fairProbability: 0.54, marketProbability: 0.51,
    featureVector: {},
  };
}

const decisionBaseRisk = {
  now, marketFeedAt: now, btcFeedAt: now, databaseAvailable: true, timestampDisorder: false,
  riskStateAvailable: true, dailyPnlUsd: "0", drawdownUsd: "0", marketExposureUsd: "0",
  totalExposureUsd: "0", directionalExposureUsd: "0.5", unhedgedExposureUsd: "4.9995",
  unhedgedDurationMs: 0, intent: "HEDGE_SECOND_LEG" as const,
  projectedMarketExposureUsd: "8", projectedTotalExposureUsd: "8",
  projectedDirectionalExposureUsd: "0", projectedUnhedgedExposureUsd: "0",
  projectedAssetExposureUsd: "8", availableCashUsd: "10", marketEntryQualified: true,
  externalSignalsLastMinute: 0, lastStrategyTradeAt: null, duplicateTrade: false,
  experimentEntryOpen: true, executionErrorsLastHour: 0, externalRiskRecommendedVeto: false,
  shuttingDown: false, hedgePosition: positionFacts,
};

describe("hedge confidence exemption (verified HEDGE_SECOND_LEG)", () => {
  it("A: 4886202 regression — verified hedge is not vetoed by MINIMUM_SIGNAL_CONFIDENCE", () => {
    const risk = evaluateRisk(hedgeRisk({ signalConfidence: LOW_CONFIDENCE }), limits);
    expect(risk.vetoes).not.toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(risk.accepted).toBe(true);
  });

  it("B: the same low confidence still vetoes a fresh OPEN_FIRST_LEG entry", () => {
    const risk = evaluateRisk({
      ...safeRisk({ intent: "OPEN_FIRST_LEG", signalConfidence: LOW_CONFIDENCE }),
      orderSizeUsd: "5",
    }, limits);
    expect(risk.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(risk.accepted).toBe(false);
  });

  it("C: a HEDGE_SECOND_LEG label without verified position facts gets no exemption", () => {
    const noPosition = evaluateRisk(
      hedgeRisk({ signalConfidence: LOW_CONFIDENCE, hedgePosition: null }),
      limits,
    );
    expect(noPosition.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");

    const noCandidate = evaluateRisk(
      hedgeRisk({ signalConfidence: LOW_CONFIDENCE, hedgeCandidate: null }),
      limits,
    );
    expect(noCandidate.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");

    // No projected facts means the position cannot be verified → fail closed.
    const noProjections = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      projectedDirectionalExposureUsd: undefined,
      projectedUnhedgedExposureUsd: undefined,
    }), limits);
    expect(noProjections.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
  });

  it("D: a same-side BUY (same outcome as the unpaired residual) gets no exemption", () => {
    const sameSide = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      hedgeCandidate: { ...hedgeFacts, outcome: "DOWN" },
    }), limits);
    expect(sameSide.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
  });

  it("E: wrong market or wrong outcome gets no exemption", () => {
    const wrongMarket = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      hedgeCandidate: { ...hedgeFacts, marketId: "OTHER-MARKET" },
    }), limits);
    expect(wrongMarket.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");

    // Position unpaired UP requires a DOWN hedge.
    const wrongOutcome = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      hedgePosition: { ...positionFacts, unpairedOutcome: "UP" },
    }), limits);
    expect(wrongOutcome.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
  });
it("F: a candidate quantity larger than the residual is never exempt and never silently creates inventory", () => {
    const over = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      hedgeCandidate: { ...hedgeFacts, quantity: "6.0" },
    }), limits);
    expect(over.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(over.accepted).toBe(false);

    // Even with high confidence, the exemption is refused by the verifier.
    expect(verifiedReducingHedge({
      hedgeIntent: true,
      candidate: { ...hedgeFacts, quantity: "6.0" },
      position: positionFacts,
      projectedDirectionalExposureUsd: "0",
      directionalExposureUsd: "0.5",
      projectedUnhedgedExposureUsd: "0",
      unhedgedExposureUsd: "4.9995",
    })).toBe(false);
  });

  it("F2: a zero/negative quantity is not exempt", () => {
    const zero = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      hedgeCandidate: { ...hedgeFacts, quantity: "0" },
    }), limits);
    expect(zero.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
  });

  it("G: insufficient depth still rejects the decision even when confidence is exempted", () => {
    const thinBook = snapshot({
      marketId: MARKET, tokenId: "yes-token", outcome: "UP",
      asks: [{ price: "0.12", quantity: "3" }],
      bids: [{ price: "0.11", quantity: "50" }],
    });
    const decision = decidePaperOrder({
      candidate: hedgeCandidate,
      consensus: consensus(LOW_CONFIDENCE),
      snapshotAfterDecisionLatency: thinBook,
      limits: riskLimitsSchema.parse({ minExpectedEdge: 0, minSignalConfidence: 0.6 }),
      triggerId: "g",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: decisionBaseRisk,
    });
    expect(decision.risk.vetoes).toContain("INSUFFICIENT_DEPTH");
    expect(decision.risk.vetoes).not.toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(decision.accepted).toBe(false);
  });

  it("G2: a verified hedge with adequate depth is accepted through the full decision path", () => {
    const deepBook = snapshot({
      marketId: MARKET, tokenId: "yes-token", outcome: "UP",
      asks: [{ price: "0.12", quantity: "30" }],
      bids: [{ price: "0.11", quantity: "50" }],
    });
    const decision = decidePaperOrder({
      candidate: hedgeCandidate,
      consensus: consensus(LOW_CONFIDENCE),
      snapshotAfterDecisionLatency: deepBook,
      limits: riskLimitsSchema.parse({ minExpectedEdge: 0, minSignalConfidence: 0.6, minLiquidityUsd: 1, maxSpread: 1 }),
      triggerId: "g",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: decisionBaseRisk,
    });
    expect(decision.accepted, JSON.stringify(decision.risk.vetoes)).toBe(true);
    expect(decision.risk.vetoes).toEqual([]);
  });

  it("H: insufficient cash still rejects a verified hedge", () => {
    const risk = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      availableCashUsd: "0",
      requiredCashUsd: HEDGE_NOTIONAL,
    }), limits);
    expect(risk.vetoes).toContain("INSUFFICIENT_CASH");
    expect(risk.accepted).toBe(false);
  });

  it("I: stale market data still rejects a verified hedge", () => {
    const risk = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      marketFeedAt: new Date(now.getTime() - limits.staleFeedTimeoutMs - 1),
    }), limits);
    expect(risk.vetoes).toContain("MARKET_FEED_STALE");
    expect(risk.accepted).toBe(false);
  });

  it("I2: confidence bypass never softens other hedge gates (fee/depth/lifecycle)", () => {
    const unknownFees = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      feeScheduleKnown: false,
    }), limits);
    expect(unknownFees.vetoes).toContain("UNKNOWN_FEE_SCHEDULE");

    const duplicate = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      duplicateTrade: true,
    }), limits);
    expect(duplicate.vetoes).toContain("DUPLICATE_TRADE");

    const shuttingDown = evaluateRisk(hedgeRisk({
      signalConfidence: LOW_CONFIDENCE,
      shuttingDown: true,
    }), limits);
    expect(shuttingDown.vetoes).toContain("SHUTTING_DOWN");
  });
});

describe("verifiedReducingHedge predicate", () => {
  const base = {
    hedgeIntent: true,
    candidate: hedgeFacts,
    position: positionFacts,
    projectedDirectionalExposureUsd: "0" as string | null | undefined,
    directionalExposureUsd: "0.5",
    projectedUnhedgedExposureUsd: "0" as string | null | undefined,
    unhedgedExposureUsd: "4.9995",
  };

  it("accepts the historical 4886202 shape", () => {
    expect(verifiedReducingHedge(base)).toBe(true);
  });

  it("refuses non-hedge intents, missing facts, and non-opposite outcomes", () => {
    expect(verifiedReducingHedge({ ...base, hedgeIntent: false })).toBe(false);
    expect(verifiedReducingHedge({ ...base, candidate: null })).toBe(false);
    expect(verifiedReducingHedge({ ...base, position: null })).toBe(false);
    expect(verifiedReducingHedge({ ...base, candidate: { ...hedgeFacts, side: "SELL" } })).toBe(false);
    expect(verifiedReducingHedge({ ...base, candidate: { ...hedgeFacts, outcome: "DOWN" } })).toBe(false);
    expect(verifiedReducingHedge({ ...base, position: { ...positionFacts, unpairedOutcome: null } })).toBe(false);
    expect(verifiedReducingHedge({ ...base, position: { ...positionFacts, unpairedQuantity: "0" } })).toBe(false);
  });

  it("refuses candidates that would increase directional or unhedged exposure", () => {
    expect(verifiedReducingHedge({ ...base, projectedDirectionalExposureUsd: "1" })).toBe(false);
    expect(verifiedReducingHedge({ ...base, projectedUnhedgedExposureUsd: "5" })).toBe(false);
  });

  it("fails closed when projections are absent", () => {
    expect(verifiedReducingHedge({ ...base, projectedDirectionalExposureUsd: undefined })).toBe(false);
    expect(verifiedReducingHedge({ ...base, projectedUnhedgedExposureUsd: null })).toBe(false);
  });
});