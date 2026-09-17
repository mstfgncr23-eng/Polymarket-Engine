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

  it("marks event-flow/OFI unavailable without inventing snapshot-derived flow (Part A BLOCKED)", () => {
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
    expect(vector!.version).toBe("native-v2-shadow-features-v2");
    expect(vector!.eventFlow.available).toBe(false);
    expect(vector!.eventFlow.reason).toBe("CLOB_TRANSPORT_LACKS_ADD_CANCEL_TRADE_DISCRIMINATION");
    expect(vector!.eventFlow.windowMs).toBeNull();
    expect(vector!.eventFlow.orderFlowImbalance).toBeNull();
    expect(vector!.eventFlow.bidAddImbalance).toBeNull();
    expect(vector!.eventFlow.askCancelImbalance).toBeNull();
    expect(vector!.eventFlow.touchDepletion).toBeNull();
    expect(vector!.eventFlow.aggressiveBuyVolume).toBeNull();
    expect(vector!.eventFlow.ofiSlope).toBeNull();
    expect(vector!.eventFlow.ofiVelocity).toBeNull();
  });

  it("exposes resolution-aligned TTE interactions and keeps strike distance unavailable (Part B)", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 1, bid: 0.4, ask: 0.6,
      bidQuantities: [30, 20, 10, 5, 5], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 1, bid: 0.3, ask: 0.5,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [30, 20, 10, 5, 5],
    }));
    for (let second = 0; second <= 30; second += 1) {
      extractor.ingestReference(reference(second, 100 + second * 0.2));
    }
    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 31_000),
      endDate: new Date(baseMs + 91_000).toISOString(),
      marketSynchronized: true,
      referenceFreshness: "fresh",
      referenceFreshnessMs: 500,
    });
    expect(vector).not.toBeNull();
    expect(vector!.resolutionAligned.timeToExpiryMs).toBe(60_000);
    expect(vector!.resolutionAligned.invSqrtTimeToExpiryMinutes).toBeCloseTo(1, 8);
    expect(vector!.resolutionAligned.bookPressureL5xInvSqrtTte).not.toBeNull();
    expect(vector!.resolutionAligned.shortMomentumBpsxInvSqrtTte).not.toBeNull();
    expect(vector!.resolutionAligned.volNormMovementxInvSqrtTte).not.toBeNull();
    expect(vector!.resolutionAligned.upMidMinusMarketPrior).toBeCloseTo(
      vector!.up.mid - vector!.pair.marketPriorUp, 10,
    );
    expect(vector!.resolutionAligned.binaryComplementBasis).toBeCloseTo(
      vector!.pair.binaryComplementBasis, 10,
    );
    expect(vector!.resolutionAligned.referenceStrikeDistanceAvailable).toBe(false);
    expect(vector!.resolutionAligned.referenceStrikeDistanceBps).toBeNull();
    expect(vector!.resolutionAligned.referenceVenue).toBe("binance");
    expect(vector!.resolutionAligned.referenceSymbol).toBe("BTCUSDT");
    expect(vector!.resolutionAligned.referenceFreshness).toBe("fresh");
    expect(vector!.resolutionAligned.marketSynchronized).toBe(true);
  });

  it("exposes existing Binance reference as cross-venue oracle and keeps Chainlink/Coinbase unavailable (Part C)", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 1, bid: 0.45, ask: 0.55,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 1, bid: 0.45, ask: 0.55,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestReference(reference(1, 42_000));
    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 2_000),
      endDate: null,
      marketSynchronized: true,
      referenceFreshness: "fresh",
      referenceFreshnessMs: 1_000,
    });
    expect(vector).not.toBeNull();
    expect(vector!.crossVenueOracle.binance.available).toBe(true);
    expect(vector!.crossVenueOracle.binance.price).toBe(42_000);
    expect(vector!.crossVenueOracle.binance.venue).toBe("binance");
    expect(vector!.crossVenueOracle.binance.symbol).toBe("BTCUSDT");
    expect(vector!.crossVenueOracle.chainlink.available).toBe(false);
    expect(vector!.crossVenueOracle.chainlink.price).toBeNull();
    expect(vector!.crossVenueOracle.chainlink.reason).toBe("NO_EXISTING_INTEGRATION");
    expect(vector!.crossVenueOracle.coinbase.available).toBe(false);
    expect(vector!.crossVenueOracle.coinbase.price).toBeNull();
    expect(vector!.crossVenueOracle.coinbase.reason).toBe("NO_EXISTING_INTEGRATION");
  });

  it("keeps reference freshness unknown and oracle unavailable when reference is absent", () => {
    const extractor = new NativeV2ShadowFeatureExtractor();
    extractor.ingestMarket(snapshot({
      outcome: "UP", sequence: 1, seconds: 1, bid: 0.45, ask: 0.55,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [10, 10, 10, 10, 10],
    }));
    extractor.ingestMarket(snapshot({
      outcome: "DOWN", sequence: 1, seconds: 1, bid: 0.45, ask: 0.55,
      bidQuantities: [10, 10, 10, 10, 10], askQuantities: [10, 10, 10, 10, 10],
    }));
    const vector = extractor.extract({
      marketId: "m1",
      asset: "BTC",
      now: new Date(baseMs + 2_000),
      endDate: null,
      marketSynchronized: false,
      referenceFreshness: "unknown",
      referenceFreshnessMs: null,
    });
    expect(vector).not.toBeNull();
    expect(vector!.crossVenueOracle.binance.available).toBe(false);
    expect(vector!.crossVenueOracle.binance.price).toBeNull();
    expect(vector!.resolutionAligned.referenceVenue).toBeNull();
    expect(vector!.resolutionAligned.shortMomentumBpsxInvSqrtTte).toBeNull();
    expect(vector!.resolutionAligned.referenceFreshness).toBe("unknown");
    expect(vector!.resolutionAligned.marketSynchronized).toBe(false);
    expect(vector!.eventFlow.available).toBe(false);
  });

});
