import { describe, expect, it } from "vitest";
import {
  NativeV2ShadowFeatureExtractor,
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  type MarketSnapshot,
  type Outcome,
  type ReferenceTick,
} from "@pm/core";

const baseMs = Date.parse("2026-09-16T16:00:00.000Z");

function snapshot(input: {
  outcome: Outcome;
  sequence: number;
  seconds: number;
  bid: number;
  ask: number;
  bidQuantities: number[];
  askQuantities: number[];
}): MarketSnapshot {
  const at = new Date(baseMs + input.seconds * 1_000).toISOString();
  return {
    eventId: `market-${input.outcome}-${input.sequence}`,
    traceId: `trace-${input.outcome}-${input.sequence}`,
    marketId: "m1",
    tokenId: `token-${input.outcome}`,
    outcome: input.outcome,
    asset: "BTC",
    bids: input.bidQuantities.map((quantity, index) => ({
      price: Math.max(0.01, input.bid - index * 0.01).toString(),
      quantity: quantity.toString(),
    })),
    asks: input.askQuantities.map((quantity, index) => ({
      price: Math.min(0.99, input.ask + index * 0.01).toString(),
      quantity: quantity.toString(),
    })),
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: null,
    sourceTimestamp: at,
    receivedAt: at,
    sequence: input.sequence,
    hash: null,
  };
}

function reference(seconds: number, price: number): ReferenceTick {
  const at = new Date(baseMs + seconds * 1_000).toISOString();
  return {
    eventId: `ref-${seconds}`,
    traceId: `ref-trace-${seconds}`,
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "binance",
    price: price.toString(),
    quantity: "1",
    sourceTimestamp: at,
    receivedAt: at,
    tradeId: `trade-${seconds}`,
  };
}

describe("NativeV2ShadowFeatureExtractor", () => {
  it("extracts multi-level microstructure without changing economic authority", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 1, bid: 0.4, ask: 0.6,
      bidQuantities: [30, 20, 10, 5, 5], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 1, bid: 0.3, ask: 0.5,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [30, 20, 10, 5, 5],
    }));

    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 2_000),
      endDate: new Date(baseMs + 60_000).toISOString(),
      marketSynchronized: true,
      referenceFreshness: "unknown",
      referenceFreshnessMs: null,
    });

    expect(vector).not.toBeNull();
    expect(vector!.version).toBe(NATIVE_V2_SHADOW_FEATURE_VERSION);
    expect(vector!.up.microprice).toBeCloseTo(0.55, 10);
    expect(vector!.up.imbalanceL1).toBeCloseTo(0.5, 10);
    expect(vector!.down.imbalanceL1).toBeCloseTo(-0.5, 10);
    expect(vector!.pair.bookPressureL1).toBeCloseTo(0.5, 10);
    expect(vector!.pair.marketPriorUp).toBeCloseTo(0.55, 10);
    expect(vector!.pair.pairAskCost).toBeCloseTo(1.1, 10);
    expect(vector!.pair.binaryComplementBasis).toBeCloseTo(-0.1, 10);
    expect(vector!.timing.timeToExpiryMs).toBe(58_000);
    expect(vector!.synchronization.marketSynchronized).toBe(true);
  });

  it("tracks spread and order-pressure velocity plus signal persistence from accepted snapshots", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 1, bid: 0.4, ask: 0.6,
      bidQuantities: [30, 30, 30, 30, 30], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 1, bid: 0.3, ask: 0.5,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [30, 30, 30, 30, 30],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 2, seconds: 3, bid: 0.45, ask: 0.55,
      bidQuantities: [40, 40, 40, 40, 40], askQuantities: [10, 10, 10, 10, 10],
    }));

    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 4_000),
      endDate: null,
      marketSynchronized: true,
      referenceFreshness: "unknown",
      referenceFreshnessMs: null,
    });

    expect(vector).not.toBeNull();
    expect(vector!.up.spreadVelocityBpsPerSecond).not.toBeNull();
    expect(vector!.up.midVelocityBpsPerSecond).not.toBeNull();
    expect(vector!.pair.orderPressureVelocityPerSecond).not.toBeNull();
    expect(vector!.pair.signalDirection).toBe(1);
    expect(vector!.pair.signalPersistenceSamples).toBe(2);
    expect(vector!.pair.signalPersistenceMs).toBe(2_000);
  });

  it("uses bounded one-second reference buckets for 5s/30s momentum and volatility", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 35, bid: 0.45, ask: 0.55,
      bidQuantities: [20, 20, 20, 20, 20], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 35, bid: 0.4, ask: 0.5,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [20, 20, 20, 20, 20],
    }));
    for (let second = 0; second <= 35; second += 1) {
      expect(extractor.ingestReference(reference(second, 100 + second * 0.1))).toBe(true);
      extractor.ingestReference(reference(second, 100 + second * 0.1 + 0.01));
    }

    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 36_000),
      endDate: null,
      marketSynchronized: true,
      referenceFreshness: "fresh",
      referenceFreshnessMs: 1_000,
    });

    expect(vector).not.toBeNull();
    expect(vector!.reference.shortMomentumBps).not.toBeNull();
    expect(vector!.reference.mediumMomentumBps).not.toBeNull();
    expect(vector!.reference.realizedVolatilityBps).toBeGreaterThan(0);
    expect(vector!.reference.volatilityNormalizedMovement).not.toBeNull();
    expect(vector!.reference.sourceAgeMs).toBe(1_000);
    expect(extractor.footprint().referenceBuckets).toBeLessThanOrEqual(64);

    extractor.resetReference("BTC");
    expect(extractor.footprint().referenceBuckets).toBe(0);
    extractor.dropMarket("m1");
    expect(extractor.footprint().markets).toBe(0);
  });
});
