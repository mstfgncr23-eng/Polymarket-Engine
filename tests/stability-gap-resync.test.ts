import { describe, expect, it } from "vitest";
import {
  TriggerEngine,
  createRestingMakerOrder,
  evolveMakerOrder,
  triggerConfigSchema,
} from "@pm/core";
import { BoundedIngressQueue, IngressOverflowError, POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS, PolymarketMarketDataAdapter } from "@pm/market-data";
import { order, snapshot } from "./fixtures.js";
import { startQualifiedEngine } from "./paper-engine-testkit.js";

function createAdapter() {
  const adapter = new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
  adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
  adapter.setTokenIds(["tok-up"]);
  return adapter;
}

function bookEvent(timestamp: number, bidQty = "10") {
  return {
    type: "book",
    payload: {
      tokenId: "tok-up", market: "mkt", timestamp,
      bids: [{ price: "0.47", size: bidQty }],
      asks: [{ price: "0.48", size: "5" }],
      lastTradePrice: "0.48",
    },
  };
}

function deltaEvent(timestamp: number, size = "9") {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId: "tok-up", side: "BUY", price: "0.47", size }],
    },
  };
}

describe("SDK subscribe findings + owned bounds", () => {
  it("default adapter (no injected client) uses the owned bounded CLOB transport", () => {
    const adapter = new PolymarketMarketDataAdapter();
    expect(adapter.usesBoundedMarketTransport()).toBe(true);
    expect(adapter.usesOwnedBoundedTransport()).toBe(true);
    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.bounded).toBe(false);
  });

  it("injected PublicClient is discovery/REST only — market stream stays bounded", () => {
    const adapter = createAdapter();
    expect(adapter.usesBoundedMarketTransport()).toBe(true);
  });

  it("documents that @polymarket/client subscribe is an unbounded it-pushable FIFO", () => {
    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.bounded).toBe(false);
    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.configurableBound).toBe(false);
    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.observableDepth).toBe(false);
    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.queue).toContain("it-pushable");
  });

  it("owned ingress is dual-bounded and overflow is GAPPED not silent drop", () => {
    const queue = new BoundedIngressQueue(2, 64);
    expect(queue.tryEnqueue({ a: 1 }).ok).toBe(true);
    expect(queue.tryEnqueue({ b: 2 }).ok).toBe(true);
    const overflow = queue.tryEnqueue({ c: 3 });
    expect(overflow.ok).toBe(false);
    if (!overflow.ok) expect(overflow.reason).toBe("count_limit");
    expect(queue.depth).toBe(2);
    expect(queue.snapshot().accepted).toBe(2);
    expect(queue.snapshot().gapTotal).toBeGreaterThan(0);
    expect(() => { throw new IngressOverflowError("byte_limit"); }).toThrow(/MARKET_INGRESS_OVERFLOW:byte_limit/);
  });

  it("below-bound accepted==processed; beyond-bound is not success", () => {
    const queue = new BoundedIngressQueue(3, 10_000);
    queue.tryEnqueue("a");
    queue.tryEnqueue("b");
    expect(queue.dequeue()).toBe("a");
    expect(queue.dequeue()).toBe("b");
    expect(queue.snapshot().accepted).toBe(2);
    expect(queue.snapshot().processed).toBe(2);
    queue.tryEnqueue("c");
    queue.tryEnqueue("d");
    queue.tryEnqueue("e");
    expect(queue.tryEnqueue("f").ok).toBe(false);
    expect(queue.snapshot().accepted).toBe(5);
  });
});

describe("Part 8 token sync FSM + timestamp epoch", () => {
  it("rejects price_change before the first trusted full book", () => {
    const adapter = createAdapter();
    expect(adapter.tokenSyncState("tok-up")).toBe("AWAITING_BOOK");
    expect(adapter.normalizeSourceEvent(deltaEvent(1_001))).toHaveLength(0);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(false);
  });

  it("accepts price_change only after a trusted full book and resets the timestamp baseline", () => {
    const adapter = createAdapter();
    expect(adapter.normalizeSourceEvent(bookEvent(1_000))).toHaveLength(1);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(true);
    expect(adapter.normalizeSourceEvent(deltaEvent(1_001))).toHaveLength(1);
    expect(adapter.normalizeSourceEvent(deltaEvent(999))).toHaveLength(0);
  });

  it("old-epoch deltas after reconnect/gap do not mutate the book", () => {
    const adapter = createAdapter();
    adapter.normalizeSourceEvent(bookEvent(1_000));
    adapter.normalizeSourceEvent(deltaEvent(1_001, "8"));
    adapter.beginStreamEpoch(["tok-up"], "reconnect");
    expect(adapter.isTokenSynchronized("tok-up")).toBe(false);
    expect(adapter.normalizeSourceEvent(deltaEvent(1_002, "1"))).toHaveLength(0);
    const resynced = adapter.normalizeSourceEvent(bookEvent(2_000, "12"));
    expect(resynced).toHaveLength(1);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(true);
    expect(adapter.normalizeSourceEvent(deltaEvent(1_500))).toHaveLength(0);
    expect(adapter.normalizeSourceEvent(deltaEvent(2_001, "11"))).toHaveLength(1);
  });
});

describe("Parts 9-10 maker and trigger gap safety", () => {
  it("maker does not fill from a gapped book and resumes after a crossing full-book epoch", () => {
    const first = snapshot({ bids: [{ price: "0.47", quantity: "12" }], sequence: 1 });
    const resting = createRestingMakerOrder(order("MAKER"), first);
    const gapped = snapshot({ bids: [{ price: "0.47", quantity: "2" }], sequence: 2, receivedAt: "2026-08-31T12:00:01.000Z" });
    expect(evolveMakerOrder({ ...resting, queueAhead: "2" }, first, gapped).fill?.quantity).toBe("4");
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.01 }));
    engine.ingestMarket(first);
    engine.forgetBook(first.marketId, first.outcome);
    const kinds = engine.ingestMarket(gapped).map((item) => item.kind);
    expect(kinds).not.toContain("SPREAD_CHANGE");
    expect(kinds).not.toContain("DEPTH_CHANGE");
  });

  it("engine skips maker evolution and new entries while the token is GAPPED", async () => {
    const { engine, market } = await startQualifiedEngine();
    const book = snapshot({
      marketId: "btc-mkt", tokenId: "btc-up",
      bids: [{ price: "0.47", quantity: "12" }], asks: [{ price: "0.48", quantity: "5" }],
    });
    await market.emitSnapshot(book);
    await engine.restMakerForTest(order("MAKER", { marketId: "btc-mkt", tokenId: "btc-up" }), book);
    expect(engine.restingOrderCount()).toBe(1);
    market.markTokenSync("btc-up", "GAPPED", 2);
    await market.emitSnapshot(snapshot({
      marketId: "btc-mkt", tokenId: "btc-up", eventId: "later", sequence: 2,
      receivedAt: "2026-08-31T12:00:01.000Z",
      bids: [{ price: "0.47", quantity: "1" }], asks: [{ price: "0.48", quantity: "5" }],
    }));
    expect(engine.restingOrdersForTest().get([...engine.restingOrdersForTest().keys()][0]!)?.remaining).toBe("10");
    expect(engine.isTokenSynchronized("btc-up")).toBe(false);
    await engine.stop();
  });

  it("first full book after gap is a baseline, next valid delta resumes triggers", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 1, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.1,
    }));
    engine.ingestMarket(snapshot({ bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.42", quantity: "10" }] }));
    engine.forgetBook("btc-15m", "UP");
    const baseline = engine.ingestMarket(snapshot({
      eventId: "base", sequence: 3,
      bids: [{ price: "0.10", quantity: "1" }], asks: [{ price: "0.90", quantity: "1" }],
    }));
    expect(baseline.map((item) => item.kind)).not.toContain("SPREAD_CHANGE");
    const next = engine.ingestMarket(snapshot({
      eventId: "delta", sequence: 4,
      bids: [{ price: "0.10", quantity: "1" }], asks: [{ price: "0.50", quantity: "1" }],
    }));
    expect(next.some((item) => item.kind === "SPREAD_CHANGE")).toBe(true);
  });
});
