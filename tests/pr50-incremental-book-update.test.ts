import { describe, expect, it } from "vitest";
import { PolymarketMarketDataAdapter, MarketIngressMalformedError, type PolymarketFeedAnomaly } from "@pm/market-data";
import { feeSchedule } from "./fixtures.js";
import { ReferencePolymarketNormalizer } from "./normalization-reference.js";

function createAdapter() {
  return new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
}

function wire(tokenIds: Array<{ tokenId: string; marketId?: string; outcome?: "UP" | "DOWN" }> = [{ tokenId: "tok-up" }]) {
  const adapter = createAdapter();
  for (const token of tokenIds) {
    adapter.registerManualToken(token.tokenId, token.marketId ?? "mkt", token.outcome ?? "UP", "BTC");
    const market = adapter.getMarket(token.tokenId);
    if (market) market.feeSchedule = feeSchedule;
  }
  adapter.setTokenIds(tokenIds.map((token) => token.tokenId));
  return adapter;
}

function bookEvent(
  tokenId: string,
  timestamp: number,
  bids: Array<{ price: string; size: string }>,
  asks: Array<{ price: string; size: string }>,
  extra: Record<string, unknown> = {},
) {
  return { type: "book", payload: { tokenId, market: "mkt", timestamp, bids, asks, lastTradePrice: "0.50", hash: "h0", tickSize: "0.01", minOrderSize: "1", ...extra } };
}

function priceChangeEvent(timestamp: number, changes: Array<Record<string, unknown>>, extra: Record<string, unknown> = {}) {
  return { type: "price_change", payload: { timestamp, tickSize: "0.01", minOrderSize: "1", priceChanges: changes, ...extra } };
}

function levels(count: number, start: number, step: number, size = "1"): Array<{ price: string; size: string }> {
  return Array.from({ length: count }, (_, index) => ({ price: (start + index * step).toFixed(4), size }));
}

function comparable(snapshot: { marketId: string; tokenId: string; outcome: string; bids: unknown; asks: unknown; lastTradePrice: unknown; tickSize: unknown; minOrderSize: unknown; sequence: number; hash: unknown }) {
  return {
    marketId: snapshot.marketId,
    tokenId: snapshot.tokenId,
    outcome: snapshot.outcome,
    bids: snapshot.bids,
    asks: snapshot.asks,
    lastTradePrice: snapshot.lastTradePrice,
    tickSize: snapshot.tickSize,
    minOrderSize: snapshot.minOrderSize,
    sequence: snapshot.sequence,
    hash: snapshot.hash,
  };
}

describe("PR50 incremental price_change correctness", () => {
  it("applies a valid single BUY replacement after full-book sync", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    const [snapshot] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "22" }]));
    expect(snapshot!.bids).toEqual([{ price: "0.40", quantity: "22" }]);
    expect(snapshot!.asks).toEqual([{ price: "0.60", quantity: "3" }]);
    expect(adapter.bookState("tok-up")?.bids.get("0.40")).toBe("22");
    expect(adapter.isTokenSynchronized("tok-up")).toBe(true);
  });

  it("applies a valid single SELL replacement after full-book sync", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    const [snapshot] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.60", size: "8" }]));
    expect(snapshot!.asks).toEqual([{ price: "0.60", quantity: "8" }]);
    expect(snapshot!.bids).toEqual([{ price: "0.40", quantity: "10" }]);
  });

  it("inserts a new BUY level into sorted descending bids", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }, { price: "0.38", size: "2" }], [{ price: "0.60", size: "3" }]));
    const [snapshot] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.39", size: "4" }]));
    expect(snapshot!.bids.map((level) => level.price)).toEqual(["0.40", "0.39", "0.38"]);
    expect(snapshot!.asks.map((level) => level.price)).toEqual(["0.60"]);
  });

  it("inserts a new SELL level into sorted ascending asks", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }, { price: "0.62", size: "1" }]));
    const [snapshot] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "SELL", price: "0.61", size: "7" }]));
    expect(snapshot!.asks.map((level) => level.price)).toEqual(["0.60", "0.61", "0.62"]);
  });

  it("deletes a level when size is 0", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }], [{ price: "0.60", size: "3" }]));
    const [snapshot] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }]));
    expect(snapshot!.bids).toEqual([{ price: "0.39", quantity: "2" }]);
    expect(adapter.bookState("tok-up")?.bids.has("0.40")).toBe(false);
  });

  it("emits one snapshot per accepted same-token change in source order", () => {
    const adapter = wire();
    const initial = adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    const later = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "BUY", price: "0.41", size: "1" },
      { tokenId: "tok-up", side: "BUY", price: "0.42", size: "2" },
      { tokenId: "tok-up", side: "SELL", price: "0.59", size: "4" },
    ]));
    expect(later).toHaveLength(3);
    expect(later[0]!.bids[0]!.price).toBe("0.41");
    expect(later[1]!.bids[0]!.price).toBe("0.42");
    expect(later[2]!.asks[0]!.price).toBe("0.59");
    expect(later[0]!.sequence).toBe(initial[0]!.sequence + 1);
    expect(later[1]!.sequence).toBe(later[0]!.sequence + 1);
    expect(later[2]!.sequence).toBe(later[1]!.sequence + 1);
  });

  it("applies multiple tokens in one payload without cross-token mutation", () => {
    const adapter = wire([
      { tokenId: "tok-up", outcome: "UP" },
      { tokenId: "tok-down", outcome: "DOWN" },
    ]);
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    adapter.normalizeSourceEvent(bookEvent("tok-down", 1_000, [{ price: "0.41", size: "8" }], [{ price: "0.58", size: "2" }]));
    const snapshots = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "BUY", price: "0.42", size: "5" },
      { tokenId: "tok-down", side: "SELL", price: "0.57", size: "9" },
    ]));
    expect(snapshots.map((item) => item.tokenId)).toEqual(["tok-up", "tok-down"]);
    expect(snapshots[0]!.bids[0]).toEqual({ price: "0.42", quantity: "5" });
    expect(snapshots[1]!.asks[0]).toEqual({ price: "0.57", quantity: "9" });
    expect(adapter.bookState("tok-up")?.asks.get("0.60")).toBe("3");
    expect(adapter.bookState("tok-down")?.bids.get("0.41")).toBe("8");
  });

  it("treats a malformed second entry as zero trusted mutation from the whole payload", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    const before = adapter.bookState("tok-up")!;
    expect(() => adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" },
      { tokenId: "tok-up", side: "SELL", price: "0.59", size: "bad" },
    ]))).toThrow(MarketIngressMalformedError);
    expect(adapter.bookState("tok-up")).toBeUndefined();
    expect(adapter.isTokenSynchronized("tok-up")).toBe(false);
    expect(adapter.tokenSyncState("tok-up")).toBe("GAPPED");
    expect(adapter.lastAcceptedTimestamp("tok-up")).toBeUndefined();
    expect(before.bids.get("0.40")).toBe("10");
  });

  it("safe-drops a stale payload without gapping or mutating", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    const anomalies: PolymarketFeedAnomaly[] = [];
    const result = adapter.normalizeSourceEvent(
      priceChangeEvent(999, [{ tokenId: "tok-up", side: "BUY", price: "0.99", size: "99" }]),
      { onAnomaly: (item) => anomalies.push(item) },
    );
    expect(result).toEqual([]);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(true);
    expect(adapter.bookState("tok-up")?.bids.get("0.40")).toBe("10");
    expect(adapter.lastAcceptedTimestamp("tok-up")).toBe(1_000);
    expect(anomalies[0]?.kind).toBe("stale");
  });

  it("rejects a malformed decimal price with zero trusted mutation", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    expect(() => adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "BUY", price: "0.4x", size: "1" },
    ]))).toThrow(MarketIngressMalformedError);
    expect(adapter.bookState("tok-up")).toBeUndefined();
    expect(adapter.tokenSyncState("tok-up")).toBe("GAPPED");
  });

  it("rejects a negative size with zero trusted mutation", () => {
    const adapter = wire();
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    expect(() => adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "SELL", price: "0.60", size: "-1" },
    ]))).toThrow(MarketIngressMalformedError);
    expect(adapter.bookState("tok-up")).toBeUndefined();
  });

  it("still fully rejects a malformed full book and does not SYNC", () => {
    const adapter = wire();
    expect(() => adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "nope" }], [{ price: "0.60", size: "3" }]))).toThrow(MarketIngressMalformedError);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(false);
    expect(adapter.bookState("tok-up")).toBeUndefined();
  });

  it("accepts incremental updates only after a trusted full book and after resync", () => {
    const adapter = wire();
    expect(adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "1" }]))).toEqual([]);
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, [{ price: "0.40", size: "10" }], [{ price: "0.60", size: "3" }]));
    expect(adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "1" }]))).toHaveLength(1);
    expect(() => adapter.normalizeSourceEvent(priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "HOLD", price: "0.41", size: "1" }]))).toThrow(MarketIngressMalformedError);
    expect(adapter.tokenSyncState("tok-up")).toBe("GAPPED");
    expect(adapter.normalizeSourceEvent(priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "BUY", price: "0.42", size: "1" }]))).toEqual([]);
    const resynced = adapter.normalizeSourceEvent(bookEvent("tok-up", 2_000, [{ price: "0.33", size: "12" }], [{ price: "0.66", size: "4" }]));
    expect(resynced).toHaveLength(1);
    expect(adapter.isTokenSynchronized("tok-up")).toBe(true);
    const [delta] = adapter.normalizeSourceEvent(priceChangeEvent(2_001, [{ tokenId: "tok-up", side: "SELL", price: "0.65", size: "2" }]));
    expect(delta!.asks[0]).toEqual({ price: "0.65", quantity: "2" });
    expect(delta!.bids[0]).toEqual({ price: "0.33", quantity: "12" });
  });

  it("keeps the unchanged side intact across BUY and SELL updates", () => {
    const adapter = wire();
    const [initial] = adapter.normalizeSourceEvent(bookEvent(
      "tok-up",
      1_000,
      [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }],
      [{ price: "0.60", size: "3" }, { price: "0.61", size: "1" }],
    ));
    const askCopy = structuredClone(initial!.asks);
    const bidCopy = structuredClone(initial!.bids);
    const [buy] = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" }]));
    expect(buy!.asks).toEqual(askCopy);
    expect(initial!.asks).toEqual(askCopy);
    const [sell] = adapter.normalizeSourceEvent(priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.59", size: "9" }]));
    expect(sell!.bids[0]).toEqual({ price: "0.41", quantity: "4" });
    expect(buy!.asks).toEqual(askCopy);
    expect(initial!.bids).toEqual(bidCopy);
    expect(initial!.asks).toEqual(askCopy);
  });

  it("keeps previously published snapshots immutable after later price_change", () => {
    const adapter = wire();
    const [first] = adapter.normalizeSourceEvent(bookEvent(
      "tok-up",
      1_000,
      [{ price: "0.40", size: "10" }, { price: "0.39", size: "2" }],
      [{ price: "0.60", size: "3" }, { price: "0.61", size: "1" }],
      { hash: "h0" },
    ));
    const firstCopy = structuredClone(comparable(first!));
    const firstBids = first!.bids;
    const firstAsks = first!.asks;
    const later = [
      ...adapter.normalizeSourceEvent(priceChangeEvent(1_001, [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" }])),
      ...adapter.normalizeSourceEvent(priceChangeEvent(1_002, [{ tokenId: "tok-up", side: "SELL", price: "0.59", size: "9" }])),
      ...adapter.normalizeSourceEvent(priceChangeEvent(1_003, [{ tokenId: "tok-up", side: "BUY", price: "0.40", size: "0" }])),
    ];
    expect(comparable(first!)).toEqual(firstCopy);
    expect(first!.bids).toBe(firstBids);
    expect(first!.asks).toBe(firstAsks);
    expect(Object.isFrozen(first!.bids)).toBe(true);
    expect(Object.isFrozen(first!.asks)).toBe(true);
    expect(Object.isFrozen(first!.bids[0])).toBe(true);
    expect(() => { (first!.bids as Array<{ price: string; quantity: string }>).push({ price: "0.10", quantity: "1" }); }).toThrow();
    expect(first!.bids).toHaveLength(2);
    expect(later[0]!.bids[0]!.price).toBe("0.41");
    expect(later[2]!.bids.some((level) => level.price === "0.40")).toBe(false);
    expect(first!.bids.some((level) => level.price === "0.40")).toBe(true);
    expect(later[1]!.asks[0]!.price).toBe("0.59");
    expect(first!.asks[0]!.price).toBe("0.60");
  });

  it("keeps deterministic bid/ask ordering on a 45-level book after mid-book insert and delete", () => {
    const adapter = wire();
    const bids = levels(45, 0.10, 0.001).reverse();
    const asks = levels(45, 0.60, 0.001);
    adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000, bids, asks));
    const snapshots = adapter.normalizeSourceEvent(priceChangeEvent(1_001, [
      { tokenId: "tok-up", side: "BUY", price: "0.1225", size: "8" },
      { tokenId: "tok-up", side: "SELL", price: "0.6100", size: "0" },
    ]));
    const inserted = snapshots.at(-1)!;
    const bidPrices = inserted.bids.map((level) => Number(level.price));
    const askPrices = inserted.asks.map((level) => Number(level.price));
    expect(snapshots).toHaveLength(2);
    expect(bidPrices).toEqual([...bidPrices].sort((left, right) => right - left));
    expect(askPrices).toEqual([...askPrices].sort((left, right) => left - right));
    expect(inserted.bids.some((level) => level.price === "0.1225" && level.quantity === "8")).toBe(true);
    expect(inserted.asks.some((level) => level.price === "0.6100")).toBe(false);
  });
});

describe("PR50 equivalence vs reference replay", () => {
  function pair() {
    const optimized = wire();
    const reference = new ReferencePolymarketNormalizer();
    reference.registerManualToken("tok-up", "mkt", "UP", "BTC");
    const market = reference.getMarket("tok-up");
    if (market) market.feeSchedule = feeSchedule;
    reference.setTokenIds(["tok-up"]);
    return { optimized, reference };
  }

  it("matches reference snapshots across a deterministic mixed incremental sequence", () => {
    const { optimized, reference } = pair();
    const bids = levels(45, 0.10, 0.001).reverse();
    const asks = levels(45, 0.60, 0.001);
    const events: unknown[] = [bookEvent("tok-up", 1_000, bids, asks, { hash: "h0" })];
    for (let index = 0; index < 80; index += 1) {
      const buy = index % 2 === 0;
      const offset = index % 45;
      const op = index % 5;
      const price = (buy ? 0.10 + offset * 0.001 : 0.60 + offset * 0.001).toFixed(4);
      const insert = (buy ? 0.10 + (45 + (index % 3)) * 0.001 : 0.60 + (45 + (index % 3)) * 0.001).toFixed(4);
      const size = op === 4 ? "0" : String(1 + (index % 7));
      events.push(priceChangeEvent(1_001 + index, [{
        tokenId: "tok-up",
        side: buy ? "BUY" : "SELL",
        price: op === 2 ? insert : price,
        size,
        hash: `h${index + 1}`,
      }]));
    }
    events.push(priceChangeEvent(1_090, [
      { tokenId: "tok-up", side: "BUY", price: "0.1120", size: "9", hash: "hm1" },
      { tokenId: "tok-up", side: "SELL", price: "0.6220", size: "3", hash: "hm2" },
      { tokenId: "tok-up", side: "BUY", price: "0.1000", size: "0", hash: "hm3" },
    ]));
    events.push(priceChangeEvent(999, [{ tokenId: "tok-up", side: "BUY", price: "0.99", size: "1" }]));

    const optimizedOut = [];
    const referenceOut = [];
    for (const event of events) {
      optimizedOut.push(...optimized.normalizeSourceEvent(event));
      referenceOut.push(...reference.normalizeSourceEvent(event));
    }
    expect(optimizedOut).toHaveLength(referenceOut.length);
    for (let index = 0; index < referenceOut.length; index += 1) {
      expect(comparable(optimizedOut[index]!), `snapshot ${index}`).toEqual(comparable(referenceOut[index]!));
    }
    expect(optimized.lastAcceptedTimestamp("tok-up")).toBe(reference.lastAcceptedTimestamp("tok-up"));
  });
});
