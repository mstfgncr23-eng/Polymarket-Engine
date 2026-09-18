import { describe, expect, it } from "vitest";
import {
  NativeV2ShadowOpportunityRanker,
  evaluateNativeV2ShadowMarketOpportunity,
  type MarketSnapshot,
  type NativeV2ShadowFairProbability,
} from "@pm/core";

const T0 = "2026-09-16T17:00:00.000Z";

function fair(up: number): NativeV2ShadowFairProbability {
  return {
    version: "native-v2-shadow-fair-probability-v1",
    calibrationStatus: "UNVALIDATED_SHADOW",
    fairProbabilityUp: up,
    fairProbabilityDown: 1 - up,
    marketPriorUp: 0.5,
    micropricePriorUp: 0.5,
    baseProbabilityUp: 0.5,
    directionalEvidenceScore: 0,
    probabilityAdjustment: up - 0.5,
    evidenceFamilyCount: 1,
    evidenceFamilyWeighting: "equal_available",
    familyScores: { bookPressure: 0, referenceMomentum: null, orderPressureVelocity: null },
    diagnostics: {
      evidenceRange: 0,
      freshReferenceUsed: false,
      binaryComplementBasis: 0,
      pairSourceSkewMs: 0,
      timeToExpiryMs: 60_000,
      signalPersistenceMs: 1_000,
      signalPersistenceSamples: 2,
    },
  };
}

function snapshot(
  marketId: string,
  outcome: "UP" | "DOWN",
  asks: Array<[number, number]>,
  rate = "0",
): MarketSnapshot {
  return {
    eventId: `event-${marketId}-${outcome}`,
    traceId: `trace-${marketId}`,
    marketId,
    tokenId: `token-${marketId}-${outcome}`,
    outcome,
    asset: "BTC",
    bids: [{ price: outcome === "UP" ? "0.49" : "0.51", quantity: "100" }],
    asks: asks.map(([price, quantity]) => ({ price: String(price), quantity: String(quantity) })),
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: {
      rate,
      exponent: 1,
      takerOnly: true,
      rebateRate: "0",
      source: "configured_test_fixture",
      effectiveAt: T0,
    },
    sourceTimestamp: T0,
    receivedAt: T0,
    sequence: 1,
    hash: null,
  };
}

function opportunity(
  marketId: string,
  fairUp: number,
  observedAt: string,
  upAsk = 0.55,
  downAsk = 0.55,
) {
  const result = evaluateNativeV2ShadowMarketOpportunity({
    fair: fair(fairUp),
    snapshots: {
      UP: snapshot(marketId, "UP", [[upAsk, 100]]),
      DOWN: snapshot(marketId, "DOWN", [[downAsk, 100]]),
    },
    now: new Date(observedAt),
    minimumNetEvUsdPerShare: 0.015,
  });
  if (!result) throw new Error("expected opportunity");
  return result;
}

describe("native v2 shadow opportunity ranking", () => {
  it("selects the side with the best executable fee-aware capital efficiency", () => {
    const result = opportunity("m1", 0.65, T0, 0.55, 0.45);
    expect(result.best?.outcome).toBe("UP");
    expect(result.best?.executableVwap).toBeCloseTo(0.55, 8);
    expect(result.best?.netEvUsdPerShare).toBeCloseTo(0.10, 8);
    expect(result.best?.expectedROI).toBeCloseTo(0.18181818, 7);
    expect(result.best?.actionable).toBe(true);
    expect(result.candidates.DOWN?.netEvUsdPerShare).toBeCloseTo(-0.10, 8);
  });

  it("walks multiple ask levels to the target notional and subtracts taker fees", () => {
    const result = evaluateNativeV2ShadowMarketOpportunity({
      fair: fair(0.70),
      snapshots: {
        UP: snapshot("m-depth", "UP", [[0.50, 4], [0.60, 5]], "0.02"),
        DOWN: snapshot("m-depth", "DOWN", [[0.70, 100]], "0.02"),
      },
      now: new Date(T0),
      targetNotionalUsd: 5,
      minimumNetEvUsdPerShare: 0.015,
    });
    const up = result?.candidates.UP;
    expect(up).not.toBeNull();
    expect(up?.levelsConsumed).toBe(2);
    expect(Number(up?.executableNotionalUsd)).toBeCloseTo(5, 8);
    expect(Number(up?.quantity)).toBeCloseTo(9, 8);
    expect(up?.executableVwap).toBeCloseTo(5 / 9, 7);
    expect(up?.slippageUsdPerShare).toBeCloseTo((5 / 9) - 0.50, 7);
    expect(Number(up?.feeUsd)).toBeGreaterThan(0);
    expect(up?.netEvUsdPerShare).toBeLessThan((0.70 - (5 / 9)));
    expect(up?.actionable).toBe(true);
  });

  it("does not substitute the loser side when the fair-probability winner is not executable", () => {
    const result = evaluateNativeV2ShadowMarketOpportunity({
      fair: fair(0.75),
      snapshots: {
        UP: snapshot("m-thin", "UP", [[0.50, 2]]),
        DOWN: snapshot("m-thin", "DOWN", [[0.30, 100]]),
      },
      now: new Date(T0),
    });
    expect(result?.candidates.UP).toBeNull();
    expect(result?.candidates.DOWN?.actionable).toBe(false);
    expect(result?.best).toBeNull();
  });

  it("keeps a cheap longshot diagnostic but never selects it over the absolute winner", () => {
    const result = opportunity("m-longshot", 0.90, T0, 0.95, 0.05);
    expect(result.candidates.DOWN?.netEvUsdPerShare).toBeCloseTo(0.05, 8);
    expect(result.candidates.DOWN?.actionable).toBe(false);
    expect(result.best?.outcome).toBe("UP");
    expect(result.best?.fairProbability).toBeCloseTo(0.90, 8);
    expect(result.best?.actionable).toBe(false);
  });

  it("ranks markets deterministically by ROI and evicts stale latest values", () => {
    const ranker = new NativeV2ShadowOpportunityRanker();
    const low = opportunity("m-low", 0.60, "2026-09-16T17:00:00.000Z");
    const high = opportunity("m-high", 0.70, "2026-09-16T17:00:10.000Z");

    ranker.upsertAndRank(low, new Date("2026-09-16T17:00:00.000Z"), 15_000);
    const rankedHigh = ranker.upsertAndRank(high, new Date("2026-09-16T17:00:10.000Z"), 15_000);
    expect(rankedHigh.marketRank).toBe(1);
    expect(rankedHigh.rankedMarketCount).toBe(2);
    expect(rankedHigh.top?.marketId).toBe("m-high");

    const refreshedHigh = opportunity("m-high", 0.70, "2026-09-16T17:00:20.001Z");
    const afterEviction = ranker.upsertAndRank(refreshedHigh, new Date("2026-09-16T17:00:20.001Z"), 15_000);
    expect(afterEviction.rankedMarketCount).toBe(1);
    expect(ranker.size()).toBe(1);
  });

  it("uses stable identifiers as the final tie break instead of insertion order", () => {
    const ranker = new NativeV2ShadowOpportunityRanker();
    const m2 = opportunity("m2", 0.65, T0);
    const m1 = opportunity("m1", 0.65, T0);
    ranker.upsertAndRank(m2, new Date(T0), 15_000);
    const result = ranker.upsertAndRank(m1, new Date(T0), 15_000);
    expect(result.top?.marketId).toBe("m1");
    expect(result.marketRank).toBe(1);
  });
});
