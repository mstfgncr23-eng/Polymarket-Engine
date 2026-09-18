import { describe, expect, it } from "vitest";
import {
  buildNativeV2ShadowExecutionIntent,
  nativeV2ShadowExecutionConfidence,
  type MarketSnapshot,
  type NativeV2ShadowFairProbability,
  type NativeV2ShadowOpportunityCandidate,
} from "@pm/core";

const NOW = "2026-09-16T18:00:00.000Z";

function fair(overrides: Partial<NativeV2ShadowFairProbability> = {}): NativeV2ShadowFairProbability {
  return {
    version: "native-v2-shadow-fair-probability-v1",
    calibrationStatus: "UNVALIDATED_SHADOW",
    fairProbabilityUp: 0.65,
    fairProbabilityDown: 0.35,
    marketPriorUp: 0.55,
    micropricePriorUp: 0.56,
    baseProbabilityUp: 0.555,
    directionalEvidenceScore: 0.6,
    probabilityAdjustment: 0.03,
    evidenceFamilyCount: 3,
    evidenceFamilyWeighting: "equal_available",
    familyScores: { bookPressure: 0.6, referenceMomentum: 0.6, orderPressureVelocity: 0.6 },
    diagnostics: {
      evidenceRange: 0,
      freshReferenceUsed: true,
      binaryComplementBasis: 0,
      pairSourceSkewMs: 0,
      timeToExpiryMs: 60_000,
      signalPersistenceMs: 3_000,
      signalPersistenceSamples: 4,
    },
    ...overrides,
  };
}

function candidate(): NativeV2ShadowOpportunityCandidate {
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
    requiredCashUsd: "5.01",
    availableAskLiquidityUsd: "200",
    feeUsd: "0.01",
    feeUsdPerShare: 0.0011,
    slippageUsdPerShare: 0.01,
    grossEdgeUsdPerShare: 0.10,
    netEvUsdPerShare: 0.0989,
    expectedEvUsdTotal: 0.899,
    expectedROI: 0.1794,
    actionable: true,
    minimumNetEvUsdPerShare: 0.015,
  };
}

function snapshot(): MarketSnapshot {
  return {
    eventId: "event-m1-up",
    traceId: "trace-1",
    marketId: "m1",
    tokenId: "token-up",
    outcome: "UP",
    asset: "BTC",
    bids: [{ price: "0.53", quantity: "100" }],
    asks: [{ price: "0.54", quantity: "4" }, { price: "0.56", quantity: "20" }],
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: {
      rate: "0.02",
      exponent: 1,
      takerOnly: true,
      rebateRate: "0",
      source: "configured_test_fixture",
      effectiveAt: NOW,
    },
    sourceTimestamp: NOW,
    receivedAt: NOW,
    sequence: 1,
    hash: null,
  };
}

describe("native v2 shadow execution intent", () => {
  it("discounts confidence when evidence coverage is incomplete", () => {
    const full = nativeV2ShadowExecutionConfidence(fair());
    const partial = nativeV2ShadowExecutionConfidence(fair({ evidenceFamilyCount: 1 }));
    expect(full.evidenceCoverage).toBe(1);
    expect(full.confidence).toBeCloseTo(0.8, 8);
    expect(partial.evidenceCoverage).toBeCloseTo(1 / 3, 8);
    expect(partial.confidence).toBeCloseTo(0.6, 8);
  });

  it("builds a deterministic current-book BUY intent with the walked worst price as limit", () => {
    const intent = buildNativeV2ShadowExecutionIntent({
      triggerId: "trigger-1",
      traceId: "trace-1",
      expiresAt: "2026-09-16T18:01:00.000Z",
      evidenceIds: ["e1", "e1", "e2"],
      fair: fair(),
      selected: candidate(),
      snapshot: snapshot(),
      now: new Date(NOW),
    });
    expect(intent).not.toBeNull();
    expect(intent?.candidate.marketId).toBe("m1");
    expect(intent?.candidate.outcome).toBe("UP");
    expect(intent?.candidate.quantity).toBe("9.09090909");
    expect(intent?.candidate.limitPrice).toBe("0.56");
    expect(intent?.candidate.intent).toBe("OPEN_FIRST_LEG");
    expect(intent?.consensus.consensusDirection).toBe("UP");
    expect(intent?.consensus.consensusConfidence).toBeCloseTo(0.8, 8);
    expect(intent?.consensus.fairProbability).toBe(0.65);
    expect(intent?.consensus.marketProbability).toBe(0.55);
    expect(intent?.consensus.evidenceIds).toEqual(["e1", "e2"]);
  });

  it("rejects a directly supplied loser-side longshot even when its economics are positive", () => {
    const selected = {
      ...candidate(),
      outcome: "DOWN" as const,
      fairProbability: 0.35,
      bestAsk: 0.20,
      executableVwap: 0.20,
      worstPrice: 0.20,
      grossEdgeUsdPerShare: 0.15,
      netEvUsdPerShare: 0.14,
      actionable: true,
    };
    const downSnapshot = {
      ...snapshot(),
      eventId: "event-m1-down",
      tokenId: "token-down",
      outcome: "DOWN" as const,
      bids: [{ price: "0.19", quantity: "100" }],
      asks: [{ price: "0.20", quantity: "100" }],
    };
    expect(buildNativeV2ShadowExecutionIntent({
      triggerId: "trigger-1",
      traceId: "trace-1",
      expiresAt: "2026-09-16T18:01:00.000Z",
      evidenceIds: ["e1"],
      fair: fair(),
      selected,
      snapshot: downSnapshot,
      now: new Date(NOW),
    })).toBeNull();
  });

  it("fails closed for a stale signal expiry or mismatched selected snapshot", () => {
    const base = {
      triggerId: "trigger-1",
      traceId: "trace-1",
      evidenceIds: ["e1"],
      fair: fair(),
      selected: candidate(),
      now: new Date(NOW),
    };
    expect(buildNativeV2ShadowExecutionIntent({
      ...base,
      expiresAt: "2026-09-16T17:59:59.000Z",
      snapshot: snapshot(),
    })).toBeNull();

    expect(buildNativeV2ShadowExecutionIntent({
      ...base,
      expiresAt: "2026-09-16T18:01:00.000Z",
      snapshot: { ...snapshot(), outcome: "DOWN", tokenId: "token-down" },
    })).toBeNull();
  });
});
