import { describe, expect, it } from "vitest";
import {
  TriggerEngine,
  createRestingMakerOrder,
  evolveMakerOrder,
  executePaperTaker,
  triggerConfigSchema,
  type MarketSnapshot,
  type TriggerEvent,
} from "@pm/core";
import { PolymarketMarketDataAdapter, type PolymarketFeedAnomaly } from "@pm/market-data";
import { feeSchedule, order } from "./fixtures.js";
import { ReferencePolymarketNormalizer } from "./normalization-reference.js";

interface ReplayPair {
  optimized: ReturnType<typeof createOptimized>;
  reference: ReferencePolymarketNormalizer;
}

function createOptimized() {
  return new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
}

function wirePair(tokenId = "tok-up", marketId = "mkt", outcome: "UP" | "DOWN" = "UP", asset: string | null = "BTC"): ReplayPair {
  const optimized = createOptimized();
  const reference = new ReferencePolymarketNormalizer();
  for (const adapter of [optimized, reference]) {
    adapter.registerManualToken(tokenId, marketId, outcome, asset);
    const market = adapter.getMarket(tokenId);
    if (market) market.feeSchedule = feeSchedule;
    adapter.setTokenIds([tokenId]);
  }
  return { optimized, reference };
}

function bookEvent(
  tokenId: string,
  timestamp: number,
  bids: Array<{ price: string; size: string }>,
  asks: Array<{ price: string; size: string }>,
  extra: Record<string, unknown> = {},
) {
  return {
    type: "book",
    payload: { tokenId, market: "mkt", timestamp, bids, asks, ...extra },
  };
}

function priceChangeEvent(
  timestamp: number,
  changes: Array<Record<string, unknown>>,
  extra: Record<string, unknown> = {},
) {
  return { type: "price_change", payload: { timestamp, priceChanges: changes, ...extra } };
}

function comparableSnapshot(snapshot: MarketSnapshot) {
  return {
    marketId: snapshot.marketId,
    tokenId: snapshot.tokenId,
    outcome: snapshot.outcome,
    asset: snapshot.asset,
    bids: snapshot.bids,
    asks: snapshot.asks,
    lastTradePrice: snapshot.lastTradePrice,
    tickSize: snapshot.tickSize,
    minOrderSize: snapshot.minOrderSize,
    feeSchedule: snapshot.feeSchedule,
    sourceTimestamp: snapshot.sourceTimestamp,
    sequence: snapshot.sequence,
    hash: snapshot.hash,
  };
}

function comparableTrigger(event: TriggerEvent) {
  return {
    marketId: event.marketId,
    asset: event.asset,
    source: event.source,
    kind: event.kind,
    severity: event.severity,
    score: event.score,
    dedupeKey: event.dedupeKey,
    evidence: event.evidence,
  };
}

function replay(pair: ReplayPair, events: unknown[]) {
  const optimizedAnomalies: PolymarketFeedAnomaly[] = [];
  const referenceAnomalies: PolymarketFeedAnomaly[] = [];
  const optimized: MarketSnapshot[] = [];
  const reference: MarketSnapshot[] = [];
  for (const event of events) {
    optimized.push(...pair.optimized.normalizeSourceEvent(event, { onAnomaly: (item) => optimizedAnomalies.push(item) }));
    reference.push(...pair.reference.normalizeSourceEvent(event, { onAnomaly: (item) => referenceAnomalies.push(item) }));
  }
  return { optimized, reference, optimizedAnomalies, referenceAnomalies };
}

function expectEquivalent(pair: ReplayPair, events: unknown[]) {
  const result = replay(pair, events);
  expect(result.optimized).toHaveLength(result.reference.length);
  for (let index = 0; index < result.reference.length; index += 1) {
    expect(comparableSnapshot(result.optimized[index]!), `snapshot ${index}`).toEqual(comparableSnapshot(result.reference[index]!));
  }
  expect(result.optimizedAnomalies.map((item) => ({
    kind: item.kind, tokenId: item.tokenId, incomingMs: item.incomingMs, lastAcceptedMs: item.lastAcceptedMs, regressionMs: item.regressionMs,
  }))).toEqual(result.referenceAnomalies.map((item) => ({
    kind: item.kind, tokenId: item.tokenId, incomingMs: item.incomingMs, lastAcceptedMs: item.lastAcceptedMs, regressionMs: item.regressionMs,
  })));
  return result;
}

function levels(count: number, start: number, step: number, size = "1"): Array<{ price: string; size: string }> {
  return Array.from({ length: count }, (_, index) => ({
    price: (start + index * step).toFixed(4),
    size,
  }));
}

function deepBook(levelsPerSide: number) {
  return bookEvent("tok-up", 1_000, levels(levelsPerSide, 0.1, 0.001).reverse(), levels(levelsPerSide, 0.6, 0.001));
}

describe("Polymarket normalize replay equivalence vs main reference", () => {
  it("1 initial full book event", () => {
    expectEquivalent(wirePair(), [bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }], [{ price: "0.60", size: "3" }, { price: "0.61", size: "1" }], { lastTradePrice: "0.50", hash: "h0", tickSize: "0.01", minOrderSize: "1" })]);
  });

  it("2 BUY insertion", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" }]),
    ]);
  });

  it("3 SELL insertion", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.59", size: "7" }]),
    ]);
  });

  it("4 existing level quantity update", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "22" }]),
    ]);
  });

  it("5 level deletion / zero size", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }]),
    ]);
  });

  it("6 best bid change", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.42", size: "5" }]),
    ]);
  });

  it("7 best ask change", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }, { price: "0.61", size: "1" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.58", size: "8" }]),
    ]);
  });

  it("8 deep book update", () => {
    const bids = levels(20, 0.20, 0.01).reverse();
    const asks = levels(20, 0.60, 0.01);
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, bids, asks),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: bids[15]!.price, size: "99" }]),
    ]);
  });

  it("9 multiple price_change entries in one source event", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [
        { tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" },
        { tokenId: "tok-up", side: "SELL", price: "0.59", size: "1" },
        { tokenId: "tok-up", side: "BUY", price: "0.40", size: "9" },
      ]),
    ]);
  });

  it("10 changes affecting alternating sides", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "1" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.59", size: "2" }]),
      priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "BUY", price: "0.39", size: "3" }]),
      priceChangeEvent(1_004, [{ tokenId: "tok-up", side: "SELL", price: "0.61", size: "4" }]),
    ]);
  });

  it("11 repeated updates to same price", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "11" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "12" }]),
      priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }]),
      priceChangeEvent(1_004, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "5" }]),
    ]);
  });

  it("12 empty side", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }]),
    ]);
  });

  it("13 20 levels/side", () => {
    expectEquivalent(wirePair(), [
      deepBook(20),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.1050", size: "8" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.6100", size: "0" }]),
    ]);
  });

  it("14 100 levels/side", () => {
    expectEquivalent(wirePair(), [
      deepBook(100),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.1500", size: "8" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.6500", size: "2" }]),
    ]);
  });

  it("15 500 levels/side", () => {
    expectEquivalent(wirePair(), [
      deepBook(500),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.2500", size: "8" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.7000", size: "0" }]),
    ]);
  });

  it("16 stale timestamp event", () => {
    const pair = wirePair();
    const result = expectEquivalent(pair, [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(999, [{ tokenId: "tok-up", side: "BUY", price: "0.99", size: "99" }]),
      bookEvent("tok-up", 998, [{ price: "0.11", size: "1" }], [{ price: "0.90", size: "1" }]),
    ]);
    expect(result.optimized).toHaveLength(1);
    expect(result.optimized[0]!.bids[0]!.price).toBe("0.40");
    expect(result.optimizedAnomalies).toHaveLength(2);
    expect(pair.optimized.lastAcceptedTimestamp("tok-up")).toBe(1_000);
    expect(pair.reference.lastAcceptedTimestamp("tok-up")).toBe(1_000);
  });

  it("17 out-of-order event then later valid update", () => {
    const result = expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(999, [{ tokenId: "tok-up", side: "BUY", price: "0.12", size: "1" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.42", size: "8" }]),
    ]);
    expect(result.optimized.map((item) => item.bids[0]?.price)).toEqual(["0.40", "0.42"]);
  });

  it("18 hash update", () => {
    const result = expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }], { hash: "h0" }),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "11", hash: "h1" }]),
    ]);
    expect(result.optimized.map((item) => item.hash)).toEqual(["h0", "h1"]);
  });

  it("19 lastTradePrice preservation", () => {
    const result = expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }], { lastTradePrice: "0.50" }),
      { type: "last_trade_price", payload: { tokenId: "tok-up", price: "0.51" } },
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.60", size: "4" }]),
    ]);
    expect(result.optimized[0]!.lastTradePrice).toBe("0.50");
    expect(result.optimized[1]!.lastTradePrice).toBe("0.51");
  });

  it("20 previously emitted snapshot immutability", () => {
    const pair = wirePair();
    const first = pair.optimized.normalizeSourceEvent(bookEvent(
      "tok-up",
      1_000,
      [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }],
      [{ price: "0.60", size: "3" }, { price: "0.61", size: "1" }],
      { hash: "h0", lastTradePrice: "0.50" },
    ))[0]!;
    const firstCopy = structuredClone(comparableSnapshot(first));
    const firstBids = first.bids;
    const firstAsks = first.asks;
    const later = [
      ...pair.optimized.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" }])),
      ...pair.optimized.normalizeSourceEvent(priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.59", size: "9" }])),
      ...pair.optimized.normalizeSourceEvent(priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }])),
    ];
    expect(later).toHaveLength(3);
    expect(comparableSnapshot(first)).toEqual(firstCopy);
    expect(first.bids).toBe(firstBids);
    expect(first.asks).toBe(firstAsks);
    expect(first.bids).toEqual(firstCopy.bids);
    expect(first.asks).toEqual(firstCopy.asks);
    expect(later[0]!.bids[0]!.price).toBe("0.41");
    expect(later[2]!.bids.some((level) => level.price === "0.40")).toBe(false);
    expect(first.bids.some((level) => level.price === "0.40")).toBe(true);
    expect(first.bids[0]).not.toBe(later[0]!.bids[0]);
  });
});

describe("one-snapshot-per-accepted-change contract", () => {
  it("emits independently for every accepted price_change entry and does not batch", () => {
    const result = expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]),
      priceChangeEvent(1_001, [
        { tokenId: "tok-up", side: "BUY", price: "0.41", size: "1" },
        { tokenId: "tok-up", side: "BUY", price: "0.42", size: "2" },
      ]),
    ]);
    expect(result.optimized).toHaveLength(3);
    expect(result.optimized[1]!.bids[0]!.price).toBe("0.41");
    expect(result.optimized[2]!.bids[0]!.price).toBe("0.42");
    expect(result.optimized[1]!.sequence + 1).toBe(result.optimized[2]!.sequence);
  });

  it("keeps equal-numeric distinct price strings as separate levels in Map insertion order", () => {
    expectEquivalent(wirePair(), [
      bookEvent("tok-up", 1_000, [{ price: "0.40", size: "1" }], [{ price: "0.60", size: "1" }]),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.400", size: "2" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "BUY", price: "0.4", size: "3" }]),
    ]);
  });
});

describe("trigger and paper execution equivalence on replayed snapshots", () => {
  it("TriggerEngine results stay equivalent for representative fixtures", () => {
    const pair = wirePair();
    const events = [
      bookEvent("tok-up", 1_000, [{ price: "0.47", size: "100" }], [{ price: "0.48", size: "1" }], { lastTradePrice: "0.48" }),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.46", size: "80" }]),
      priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.49", size: "20" }]),
      priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "SELL", price: "0.48", size: "0" }]),
    ];
    const { optimized, reference } = replay(pair, events);
    const now = new Date("2026-09-03T00:00:00.000Z");
    const config = triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.5, spreadChangeThreshold: 0.005, depthChangeThreshold: 0.2 });
    const optimizedEngine = new TriggerEngine(config, () => now);
    const referenceEngine = new TriggerEngine(config, () => now);
    const optimizedTriggers = optimized.flatMap((snapshot) => optimizedEngine.ingestMarket(snapshot));
    const referenceTriggers = reference.flatMap((snapshot) => referenceEngine.ingestMarket(snapshot));
    expect(optimizedTriggers.map(comparableTrigger)).toEqual(referenceTriggers.map(comparableTrigger));
  });

  it("paper taker/maker fill behavior stays unchanged for equivalent books", () => {
    const pair = wirePair();
    const { optimized, reference } = replay(pair, [
      bookEvent("tok-up", 1_000, [{ price: "0.47", size: "100" }], [{ price: "0.48", size: "5" }, { price: "0.50", size: "10" }], { lastTradePrice: "0.48" }),
      priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.48", size: "4" }]),
    ]);
    const taker = order("TAKER", { quantity: "3", requestedPrice: "0.51" });
    const optimizedFill = executePaperTaker({ order: taker, snapshotAfterLatency: optimized[1]!, measuredLatencyMs: 150 });
    const referenceFill = executePaperTaker({ order: taker, snapshotAfterLatency: reference[1]!, measuredLatencyMs: 150 });
    expect(optimizedFill.fillPrice).toBe(referenceFill.fillPrice);
    expect(optimizedFill.quantity).toBe(referenceFill.quantity);
    expect(optimizedFill.slippageUsd).toBe(referenceFill.slippageUsd);
    expect(optimizedFill.feeUsd).toBe(referenceFill.feeUsd);

    const maker = order("MAKER", { quantity: "10", requestedPrice: "0.47" });
    const optimizedResting = createRestingMakerOrder(maker, optimized[0]!);
    const referenceResting = createRestingMakerOrder(maker, reference[0]!);
    expect(optimizedResting.queueAhead).toBe(referenceResting.queueAhead);
    const optimizedEvolved = evolveMakerOrder({ ...optimizedResting, queueAhead: "2" }, optimized[0]!, {
      ...optimized[1]!,
      bids: [{ price: "0.47", quantity: "2" }],
      sequence: optimized[1]!.sequence,
      receivedAt: "2026-08-31T12:00:01.000Z",
    });
    const referenceEvolved = evolveMakerOrder({ ...referenceResting, queueAhead: "2" }, reference[0]!, {
      ...reference[1]!,
      bids: [{ price: "0.47", quantity: "2" }],
      sequence: reference[1]!.sequence,
      receivedAt: "2026-08-31T12:00:01.000Z",
    });
    expect(optimizedEvolved.fill?.quantity).toBe(referenceEvolved.fill?.quantity);
    expect(optimizedEvolved.fill?.fillPrice).toBe(referenceEvolved.fill?.fillPrice);
  });
});
