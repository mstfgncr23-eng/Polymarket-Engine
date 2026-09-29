import { describe, expect, it } from "vitest";
import {
  NATIVE_V2_SHADOW_CALIBRATION_STATUS,
  NATIVE_V2_SHADOW_FAIR_PROBABILITY_VERSION,
  evaluateNativeV2ShadowFairProbability,
  type NativeV2ShadowFeatureVector,
} from "@pm/core";

function vector(overrides: {
  marketPriorUp?: number;
  upMicro?: number;
  downMicro?: number;
  bookPressure?: number;
  referenceZ?: number | null;
  referenceFreshness?: "fresh" | "stale" | "unknown";
  flowVelocity?: number | null;
  synchronized?: boolean;
} = {}): NativeV2ShadowFeatureVector {
  const book = (microprice: number) => ({
    mid: 0.5,
    spread: 0.02,
    spreadBps: 400,
    microprice,
    micropriceOffsetBps: 0,
    imbalanceL1: 0,
    imbalanceL3: 0,
    imbalanceL5: 0,
    bidDepthUsdL5: 100,
    askDepthUsdL5: 100,
    depthAsymmetryL5: 0,
    bidDepthSlope: 0,
    askDepthSlope: 0,
    depthSlopeAsymmetry: 0,
    bidLiquidityConcentrationL1InL5: 0.2,
    askLiquidityConcentrationL1InL5: 0.2,
    spreadVelocityBpsPerSecond: null,
    midVelocityBpsPerSecond: null,
    sourceTimestamp: "2026-09-16T17:00:00.000Z",
    receivedAt: "2026-09-16T17:00:00.000Z",
    sequence: 1,
  });

  return {
    version: "native-v2-shadow-features-v2",
    marketId: "m1",
    asset: "BTC",
    observedAt: "2026-09-16T17:00:01.000Z",
    up: book(overrides.upMicro ?? 0.5),
    down: book(overrides.downMicro ?? 0.5),
    pair: {
      marketPriorUp: overrides.marketPriorUp ?? 0.5,
      pairAskCost: 1.02,
      binaryComplementBasis: 0,
      bookPressureL1: overrides.bookPressure ?? 0,
      bookPressureL3: overrides.bookPressure ?? 0,
      bookPressureL5: overrides.bookPressure ?? 0,
      orderPressureVelocityPerSecond: overrides.flowVelocity ?? null,
      signalDirection: 0,
      signalPersistenceMs: 2_000,
      signalPersistenceSamples: 3,
    },
    reference: {
      latestPrice: 100,
      shortMomentumBps: 5,
      mediumMomentumBps: 10,
      realizedVolatilityBps: 10,
      volatilityNormalizedMovement: overrides.referenceZ ?? null,
      sourceTimestamp: "2026-09-16T17:00:00.000Z",
      sourceAgeMs: 1_000,
      marketReferenceSourceSkewMs: 0,
    },
    timing: {
      timeToExpiryMs: 60_000,
      upSourceAgeMs: 1_000,
      downSourceAgeMs: 1_000,
      pairSourceSkewMs: 0,
    },
    synchronization: {
      marketSynchronized: overrides.synchronized ?? true,
      referenceFreshness: overrides.referenceFreshness ?? "fresh",
      referenceFreshnessMs: 1_000,
    },
    eventFlow: {
      available: false,
      reason: "CLOB_TRANSPORT_LACKS_ADD_CANCEL_TRADE_DISCRIMINATION",
      windowMs: null,
      orderFlowImbalance: null,
      bidAddImbalance: null,
      askAddImbalance: null,
      bidCancelImbalance: null,
      askCancelImbalance: null,
      touchDepletion: null,
      touchReplenishment: null,
      aggressiveBuyVolume: null,
      aggressiveSellVolume: null,
      eventArrivalRatePerSecond: null,
      ofiSlope: null,
      ofiVelocity: null,
    },
    resolutionAligned: {
      timeToExpiryMs: 60_000,
      invSqrtTimeToExpiryMinutes: 1,
      bookPressureL5xInvSqrtTte: 0,
      shortMomentumBpsxInvSqrtTte: 0,
      volNormMovementxInvSqrtTte: 0,
      upMidMinusMarketPrior: 0,
      downMidMinusMarketPriorComplement: 0,
      binaryComplementBasis: 0,
      referenceStrikeDistanceBps: null,
      referenceStrikeDistanceAvailable: false,
      referenceVenue: "binance",
      referenceSymbol: "BTCUSDT",
      referenceSourceAgeMs: 1_000,
      marketReferenceSourceSkewMs: 0,
      referenceFreshness: "fresh",
      marketSynchronized: true,
    },
    crossVenueOracle: {
      binance: {
        available: true,
        price: 100,
        sourceAgeMs: 1_000,
        symbol: "BTCUSDT",
        venue: "binance",
      },
      chainlink: {
        available: false,
        price: null,
        sourceAgeMs: null,
        reason: "NO_EXISTING_INTEGRATION",
      },
      coinbase: {
        available: false,
        price: null,
        sourceAgeMs: null,
        reason: "NO_EXISTING_INTEGRATION",
      },
    },
  };
}

describe("evaluateNativeV2ShadowFairProbability", () => {
  it("anchors neutral synchronized books at 0.5 and remains explicitly unvalidated", () => {
    const result = evaluateNativeV2ShadowFairProbability(vector());
    expect(result).not.toBeNull();
    expect(result!.version).toBe(NATIVE_V2_SHADOW_FAIR_PROBABILITY_VERSION);
    expect(result!.calibrationStatus).toBe(NATIVE_V2_SHADOW_CALIBRATION_STATUS);
    expect(result!.marketPriorUp).toBe(0.5);
    expect(result!.micropricePriorUp).toBe(0.5);
    expect(result!.baseProbabilityUp).toBe(0.5);
    expect(result!.fairProbabilityUp).toBe(0.5);
    expect(result!.fairProbabilityDown).toBe(0.5);
    expect(result!.evidenceFamilyCount).toBe(1);
  });

  it("combines book, fresh normalized reference and flow evidence with a bounded adjustment", () => {
    const result = evaluateNativeV2ShadowFairProbability(vector({
      marketPriorUp: 0.52,
      upMicro: 0.56,
      downMicro: 0.46,
      bookPressure: 0.6,
      referenceZ: 1,
      flowVelocity: 0.5,
    }));
    expect(result).not.toBeNull();
    expect(result!.micropricePriorUp).toBeCloseTo(0.55, 12);
    expect(result!.baseProbabilityUp).toBeCloseTo(0.535, 12);
    expect(result!.familyScores.referenceMomentum).toBeCloseTo(Math.tanh(1), 12);
    expect(result!.familyScores.orderPressureVelocity).toBeCloseTo(Math.tanh(0.5), 12);
    expect(result!.evidenceFamilyCount).toBe(3);
    expect(result!.probabilityAdjustment).toBeGreaterThan(0);
    expect(result!.probabilityAdjustment).toBeLessThanOrEqual(0.05);
    expect(result!.fairProbabilityUp).toBeGreaterThan(result!.baseProbabilityUp);
    expect(result!.fairProbabilityUp + result!.fairProbabilityDown).toBeCloseTo(1, 12);
  });

  it("omits stale reference evidence instead of treating it as neutral", () => {
    const stale = evaluateNativeV2ShadowFairProbability(vector({
      bookPressure: -0.6,
      referenceZ: 5,
      referenceFreshness: "stale",
      flowVelocity: -0.5,
    }));
    const fresh = evaluateNativeV2ShadowFairProbability(vector({
      bookPressure: -0.6,
      referenceZ: 5,
      referenceFreshness: "fresh",
      flowVelocity: -0.5,
    }));
    expect(stale).not.toBeNull();
    expect(fresh).not.toBeNull();
    expect(stale!.familyScores.referenceMomentum).toBeNull();
    expect(stale!.diagnostics.freshReferenceUsed).toBe(false);
    expect(stale!.evidenceFamilyCount).toBe(2);
    expect(fresh!.evidenceFamilyCount).toBe(3);
    expect(fresh!.fairProbabilityUp).toBeGreaterThan(stale!.fairProbabilityUp);
  });

  it("fails closed for unsynchronized books and clamps extreme shadow probabilities", () => {
    expect(evaluateNativeV2ShadowFairProbability(vector({ synchronized: false }))).toBeNull();

    const high = evaluateNativeV2ShadowFairProbability(vector({
      marketPriorUp: 0.99,
      upMicro: 0.99,
      downMicro: 0.01,
      bookPressure: 1,
      referenceZ: 20,
      flowVelocity: 20,
    }));
    expect(high).not.toBeNull();
    expect(high!.fairProbabilityUp).toBe(0.99);
    expect(high!.fairProbabilityDown).toBeCloseTo(0.01, 12);
  });
});
