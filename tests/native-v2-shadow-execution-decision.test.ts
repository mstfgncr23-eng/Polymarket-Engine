import { describe, expect, it } from "vitest";
import {
  buildNativeV2ShadowExecutionIntent,
  decidePaperOrder,
  riskLimitsSchema,
  type MarketSnapshot,
  type NativeV2ShadowFairProbability,
  type NativeV2ShadowOpportunityCandidate,
} from "@pm/core";

const NOW = new Date("2026-09-16T18:00:00.000Z");
const EXPIRES = new Date("2026-09-16T18:01:00.000Z");

function fair(evidenceFamilyCount = 3, directionalEvidenceScore = 0.6): NativeV2ShadowFairProbability {
  return {
    version: "native-v2-shadow-fair-probability-v1",
    calibrationStatus: "UNVALIDATED_SHADOW",
    fairProbabilityUp: 0.65,
    fairProbabilityDown: 0.35,
    marketPriorUp: 0.55,
    micropricePriorUp: 0.56,
    baseProbabilityUp: 0.555,
    directionalEvidenceScore,
    probabilityAdjustment: 0.03,
    evidenceFamilyCount,
    evidenceFamilyWeighting: "equal_available",
    familyScores: { bookPressure: directionalEvidenceScore, referenceMomentum: null, orderPressureVelocity: null },
    diagnostics: {
      evidenceRange: 0,
      freshReferenceUsed: false,
      binaryComplementBasis: 0,
      pairSourceSkewMs: 0,
      timeToExpiryMs: 60_000,
      signalPersistenceMs: 3_000,
      signalPersistenceSamples: 4,
    },
  };
}

function selected(): NativeV2ShadowOpportunityCandidate {
  return {
    marketId: "m1",
    outcome: "UP",
    fairProbability: 0.65,
    targetNotionalUsd: 5,
    bestAsk: 0.54,
    executableVwap: 0.55,
    worstPrice: 0.56,
    levelsConsumed: 2,
    quantity: "9.09090909",
    executableNotionalUsd: "5",
    requiredCashUsd: "5",
    availableAskLiquidityUsd: "114",
    feeUsd: "0",
    feeUsdPerShare: 0,
    slippageUsdPerShare: 0.01,
    grossEdgeUsdPerShare: 0.10,
    netEvUsdPerShare: 0.10,
    expectedEvUsdTotal: 0.909090909,
    expectedROI: 0.1818181818,
    actionable: true,
    minimumNetEvUsdPerShare: 0.015,
  };
}

function snapshot(): MarketSnapshot {
  const iso = NOW.toISOString();
  return {
    eventId: "event-m1-up",
    traceId: "trace-1",
    marketId: "m1",
    tokenId: "token-up",
    outcome: "UP",
    asset: "BTC",
    bids: [{ price: "0.53", quantity: "200" }],
    asks: [{ price: "0.54", quantity: "4" }, { price: "0.56", quantity: "200" }],
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: {
      rate: "0",
      exponent: 1,
      takerOnly: true,
      rebateRate: "0",
      source: "configured_test_fixture",
      effectiveAt: iso,
    },
    sourceTimestamp: iso,
    receivedAt: iso,
    sequence: 1,
    hash: null,
  };
}

function intent(evidenceFamilyCount = 3, directionalEvidenceScore = 0.6) {
  const result = buildNativeV2ShadowExecutionIntent({
    triggerId: "trigger-1",
    traceId: "trace-1",
    expiresAt: EXPIRES.toISOString(),
    evidenceIds: ["e1"],
    fair: fair(evidenceFamilyCount, directionalEvidenceScore),
    selected: selected(),
    snapshot: snapshot(),
    now: NOW,
  });
  if (!result) throw new Error("expected shadow intent");
  return result;
}

function baseRisk(consensusConfidence: number) {
  return {
    now: NOW,
    marketFeedAt: NOW,
    btcFeedAt: NOW,
    referenceFeedAt: NOW,
    referenceRequired: false,
    asset: "BTC",
    assetExposureUsd: "0",
    databaseAvailable: true,
    timestampDisorder: false,
    riskStateAvailable: true,
    dailyPnlUsd: "0",
    drawdownUsd: "0",
    marketExposureUsd: "0",
    totalExposureUsd: "0",
    directionalExposureUsd: "0",
    unhedgedExposureUsd: "0",
    unhedgedDurationMs: 0,
    intent: "OPEN_FIRST_LEG" as const,
    availableCashUsd: "1000",
    referenceProviderBlocked: false,
    marketEntryQualified: true,
    externalSignalsLastMinute: 0,
    lastStrategyTradeAt: null,
    duplicateTrade: false,
    experimentEntryOpen: true,
    executionErrorsLastHour: 0,
    externalRiskRecommendedVeto: false,
    shuttingDown: false,
    // Keep the parameter visible in the fixture so the test documents the
    // exact confidence fed through the synthetic consensus below.
    _consensusConfidence: consensusConfidence,
  };
}

describe("native v2 full shadow execution evaluation", () => {
  it("passes a strong V2 intent through the real deterministic order/risk evaluator without mutating anything", () => {
    const shadow = intent();
    const risk = baseRisk(shadow.confidence);
    const { _consensusConfidence: _ignored, ...riskInput } = risk;
    const decision = decidePaperOrder({
      candidate: shadow.candidate,
      consensus: shadow.consensus,
      snapshotAfterDecisionLatency: snapshot(),
      baseRisk: riskInput,
      limits: riskLimitsSchema.parse({}),
      triggerId: "trigger-1",
      signalExpiresAt: EXPIRES,
      orderType: "TAKER",
    });

    expect(shadow.confidence).toBeCloseTo(0.8, 8);
    expect(decision.accepted).toBe(true);
    expect(decision.risk.vetoes).toEqual([]);
    expect(decision.order?.requestedPrice).toBe("0.56");
    expect(decision.bookVwap).not.toBeNull();
    expect(decision.netEvUsdPerShare).toBeGreaterThan(0.015);
  });

  it("rejects a high-price first leg whose net edge passes but expected ROI is below 2%", () => {
    const shadow = intent();
    const risk = baseRisk(shadow.confidence);
    const { _consensusConfidence: _ignored, ...riskInput } = risk;
    const expensiveSnapshot: MarketSnapshot = {
      ...snapshot(),
      bids: [{ price: "0.96", quantity: "200" }],
      asks: [{ price: "0.97", quantity: "200" }],
      feeSchedule: {
        rate: "0.07",
        exponent: 1,
        takerOnly: true,
        rebateRate: "0.2",
        source: "configured_test_fixture",
        effectiveAt: NOW.toISOString(),
      },
    };
    const candidate = {
      ...shadow.candidate,
      quantity: "5.15463918",
      limitPrice: "0.97",
      rawEdgeUsdPerShare: 0.02,
      rawExpectedEdge: 0.02,
    };

    const decision = decidePaperOrder({
      candidate,
      consensus: shadow.consensus,
      snapshotAfterDecisionLatency: expensiveSnapshot,
      baseRisk: riskInput,
      limits: riskLimitsSchema.parse({}),
      triggerId: "trigger-1",
      signalExpiresAt: EXPIRES,
      orderType: "TAKER",
    });

    expect(decision.netEvUsdPerShare).toBeGreaterThan(0.015);
    expect(decision.expectedROI).not.toBeNull();
    expect(decision.expectedROI!).toBeLessThan(0.02);
    expect(decision.risk.vetoes).not.toContain("MINIMUM_EXPECTED_EDGE");
    expect(decision.risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(decision.accepted).toBe(false);
    expect(decision.order).toBeNull();
  });

  it("uses the unchanged risk gate to veto weak V2 evidence on minimum signal confidence", () => {
    const shadow = intent(1, 0.3);
    const risk = baseRisk(shadow.confidence);
    const { _consensusConfidence: _ignored, ...riskInput } = risk;
    const decision = decidePaperOrder({
      candidate: shadow.candidate,
      consensus: shadow.consensus,
      snapshotAfterDecisionLatency: snapshot(),
      baseRisk: riskInput,
      limits: riskLimitsSchema.parse({}),
      triggerId: "trigger-1",
      signalExpiresAt: EXPIRES,
      orderType: "TAKER",
    });

    expect(shadow.confidence).toBeCloseTo(0.55, 8);
    expect(decision.accepted).toBe(false);
    expect(decision.risk.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(decision.order).toBeNull();
  });
});
