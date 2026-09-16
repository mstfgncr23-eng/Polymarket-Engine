import { describe, expect, it } from "vitest";
import { NativeV2ShadowStore } from "../apps/engine/src/native-v2-shadow-store.js";
import type {
  NativeV2ShadowFairProbability,
  NativeV2ShadowFeatureVector,
  NativeV2ShadowOpportunityCandidate,
} from "@pm/core";

const featureVector: NativeV2ShadowFeatureVector = {
  version: "native-v2-shadow-features-v1",
  marketId: "m1",
  asset: "BTC",
  observedAt: "2026-09-16T18:00:00.000Z",
  up: {
    mid: 0.55, spread: 0.02, spreadBps: 363.63, microprice: 0.56, micropriceOffsetBps: 181.8,
    imbalanceL1: 0.2, imbalanceL3: 0.2, imbalanceL5: 0.2,
    bidDepthUsdL5: 100, askDepthUsdL5: 100, depthAsymmetryL5: 0,
    bidDepthSlope: null, askDepthSlope: null, depthSlopeAsymmetry: null,
    bidLiquidityConcentrationL1InL5: 0.5, askLiquidityConcentrationL1InL5: 0.5,
    spreadVelocityBpsPerSecond: null, midVelocityBpsPerSecond: null,
    sourceTimestamp: "2026-09-16T18:00:00.000Z", receivedAt: "2026-09-16T18:00:00.000Z", sequence: 1,
  },
  down: {
    mid: 0.45, spread: 0.02, spreadBps: 444.44, microprice: 0.44, micropriceOffsetBps: -222.2,
    imbalanceL1: -0.2, imbalanceL3: -0.2, imbalanceL5: -0.2,
    bidDepthUsdL5: 100, askDepthUsdL5: 100, depthAsymmetryL5: 0,
    bidDepthSlope: null, askDepthSlope: null, depthSlopeAsymmetry: null,
    bidLiquidityConcentrationL1InL5: 0.5, askLiquidityConcentrationL1InL5: 0.5,
    spreadVelocityBpsPerSecond: null, midVelocityBpsPerSecond: null,
    sourceTimestamp: "2026-09-16T18:00:00.000Z", receivedAt: "2026-09-16T18:00:00.000Z", sequence: 1,
  },
  pair: {
    marketPriorUp: 0.55, pairAskCost: 1.02, binaryComplementBasis: 0,
    bookPressureL1: 0.2, bookPressureL3: 0.2, bookPressureL5: 0.2,
    orderPressureVelocityPerSecond: null, signalDirection: 1, signalPersistenceMs: 1000, signalPersistenceSamples: 2,
  },
  reference: {
    latestPrice: 60_000, shortMomentumBps: 4, mediumMomentumBps: 8,
    realizedVolatilityBps: 5, volatilityNormalizedMovement: 0.8,
    sourceTimestamp: "2026-09-16T18:00:00.000Z", sourceAgeMs: 0, marketReferenceSourceSkewMs: 0,
  },
  timing: { timeToExpiryMs: 60_000, upSourceAgeMs: 0, downSourceAgeMs: 0, pairSourceSkewMs: 0 },
  synchronization: { marketSynchronized: true, referenceFreshness: "fresh", referenceFreshnessMs: 0 },
};

const fairProbability: NativeV2ShadowFairProbability = {
  version: "native-v2-shadow-fair-probability-v1",
  calibrationStatus: "UNVALIDATED_SHADOW",
  fairProbabilityUp: 0.62,
  fairProbabilityDown: 0.38,
  marketPriorUp: 0.55,
  micropricePriorUp: 0.56,
  baseProbabilityUp: 0.555,
  directionalEvidenceScore: 0.6,
  probabilityAdjustment: 0.03,
  evidenceFamilyCount: 3,
  evidenceFamilyWeighting: "equal_available",
  familyScores: { bookPressure: 0.2, referenceMomentum: 0.8, orderPressureVelocity: 0.8 },
  diagnostics: {
    evidenceRange: 0.6, freshReferenceUsed: true, binaryComplementBasis: 0,
    pairSourceSkewMs: 0, timeToExpiryMs: 60_000, signalPersistenceMs: 1_000, signalPersistenceSamples: 2,
  },
};

const selected: NativeV2ShadowOpportunityCandidate = {
  marketId: "m1", outcome: "UP", fairProbability: 0.62, targetNotionalUsd: 5,
  bestAsk: 0.54, executableVwap: 0.55, worstPrice: 0.56, levelsConsumed: 2,
  quantity: "9.09090909", executableNotionalUsd: "5", requiredCashUsd: "5.01",
  availableAskLiquidityUsd: "200", feeUsd: "0.01", feeUsdPerShare: 0.0011,
  slippageUsdPerShare: 0.01, grossEdgeUsdPerShare: 0.07, netEvUsdPerShare: 0.0589,
  expectedEvUsdTotal: 0.535, expectedROI: 0.1067, actionable: true, minimumNetEvUsdPerShare: 0.015,
};

function record(i: number, execution: Record<string, unknown>) {
  return {
    recordedAt: `2026-09-16T18:00:0${i}.000Z`,
    experimentId: "experiment-1",
    experimentStatus: "RUNNING",
    gitSha: "abc123",
    triggerId: `trigger-${i}`,
    traceId: `trace-${i}`,
    marketId: `m${i}`,
    asset: "BTC",
    outcome: "UP" as const,
    v1: { direction: "UP" as const, confidence: 0.7, fairProbability: 0.58, marketProbability: 0.55 },
    featureVector: { ...featureVector, marketId: `m${i}` },
    fairProbability,
    opportunity: {
      marketRank: 1,
      rankedMarketCount: 5,
      actionableMarketCount: 2,
      selected: { ...selected, marketId: `m${i}` },
      globalTop: {
        marketId: `m${i}`, outcome: "UP" as const, expectedROI: selected.expectedROI,
        expectedEvUsdTotal: selected.expectedEvUsdTotal, netEvUsdPerShare: selected.netEvUsdPerShare,
        executableVwap: selected.executableVwap, fairProbability: selected.fairProbability, actionable: true,
      },
    },
    execution,
  };
}

describe("NativeV2ShadowStore", () => {
  it("keeps a bounded chronological ring and exports only retained RAM records", () => {
    const store = new NativeV2ShadowStore(2);
    store.noteObserved();
    store.append(record(1, { preflightBlockers: [], riskVetoes: [], wouldSubmitOrder: true }));
    store.noteObserved();
    store.append(record(2, { preflightBlockers: [], riskVetoes: ["MAXIMUM_SPREAD"], wouldSubmitOrder: false }));
    store.noteObserved();
    store.append(record(3, { preflightBlockers: ["MARKET_UNSYNCHRONIZED"], riskVetoes: [], wouldSubmitOrder: false }));

    const exported = store.export({
      generatedAt: "2026-09-16T18:10:00.000Z",
      experimentId: "experiment-1",
      experimentStatus: "RUNNING",
      gitSha: "abc123",
    });
    expect(exported.summary).toMatchObject({
      observed: 3, evaluated: 3, wouldSubmit: 1, vetoed: 1, preflightBlocked: 1,
      retained: 2, capacity: 2,
    });
    expect(exported.summary.topRiskVetoes).toEqual({ MAXIMUM_SPREAD: 1 });
    expect(exported.summary.topPreflightBlockers).toEqual({ MARKET_UNSYNCHRONIZED: 1 });
    expect(exported.evaluations.map((item) => item.triggerId)).toEqual(["trigger-2", "trigger-3"]);
    expect(exported.evaluations.map((item) => item.sequence)).toEqual([2, 3]);
  });

  it("tracks busy drops without fabricating an evaluation", () => {
    const store = new NativeV2ShadowStore(4);
    store.noteObserved();
    store.noteDroppedBusy();
    const snapshot = store.snapshot();
    expect(snapshot.summary).toMatchObject({ observed: 1, evaluated: 0, droppedBusy: 1, retained: 0 });
    expect(snapshot.latest).toBeNull();
  });
});
