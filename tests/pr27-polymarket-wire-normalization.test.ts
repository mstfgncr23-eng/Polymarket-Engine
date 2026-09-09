import { describe, expect, it } from "vitest";
import { normalizeRawClobMarketEvent, PolymarketMarketDataAdapter } from "@pm/market-data";

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

function rawBook(timestamp = "1757908892351") {
  return {
    event_type: "book",
    asset_id: "tok-up",
    market: "mkt",
    bids: [{ price: "0.40", size: "10" }],
    asks: [{ price: "0.60", size: "3" }],
    timestamp,
    hash: "h0",
    min_order_size: "1",
    tick_size: "0.01",
  };
}

describe("raw Polymarket CLOB websocket compatibility", () => {
  it("maps the official raw book wire shape into the canonical adapter event", () => {
    const normalized = normalizeRawClobMarketEvent(rawBook());
    expect(normalized).toEqual({
      topic: "market",
      type: "book",
      payload: {
        market: "mkt",
        tokenId: "tok-up",
        bids: [{ price: "0.40", size: "10" }],
        asks: [{ price: "0.60", size: "3" }],
        hash: "h0",
        timestamp: "1757908892351",
        minOrderSize: "1",
        tickSize: "0.01",
        negRisk: undefined,
        lastTradePrice: undefined,
      },
    });

    const snapshots = createAdapter().normalizeSourceEvent(normalized);
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]).toMatchObject({
      marketId: "mkt",
      tokenId: "tok-up",
      asset: "BTC",
      tickSize: "0.01",
      minOrderSize: "1",
      hash: "h0",
    });
  });

  it("maps raw price_changes asset_id entries and updates the trusted book", () => {
    const adapter = createAdapter();
    adapter.normalizeSourceEvent(normalizeRawClobMarketEvent(rawBook()));

    const snapshots = adapter.normalizeSourceEvent(normalizeRawClobMarketEvent({
      event_type: "price_change",
      market: "mkt",
      price_changes: [{
        asset_id: "tok-up",
        price: "0.41",
        size: "4",
        side: "BUY",
        hash: "h1",
        best_bid: "0.41",
        best_ask: "0.60",
      }],
      timestamp: "1757908892352",
    }));

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.bids[0]).toEqual({ price: "0.41", quantity: "4" });
    expect(snapshots[0]?.hash).toBe("h1");
  });

  it("maps raw last_trade_price and preserves it on the next published snapshot", () => {
    const adapter = createAdapter();
    adapter.normalizeSourceEvent(normalizeRawClobMarketEvent(rawBook()));

    expect(adapter.normalizeSourceEvent(normalizeRawClobMarketEvent({
      event_type: "last_trade_price",
      asset_id: "tok-up",
      market: "mkt",
      price: "0.55",
      size: "2",
      fee_rate_bps: "0",
      side: "BUY",
      timestamp: "1757908892352",
      transaction_hash: "0xabc",
    }))).toEqual([]);

    const snapshots = adapter.normalizeSourceEvent(normalizeRawClobMarketEvent({
      event_type: "price_change",
      market: "mkt",
      price_changes: [{ asset_id: "tok-up", price: "0.40", size: "11", side: "BUY", hash: "h2" }],
      timestamp: "1757908892353",
    }));

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.lastTradePrice).toBe("0.55");
  });

  it("leaves PublicClient-style type/payload events unchanged", () => {
    const canonical = {
      topic: "market",
      type: "book",
      payload: {
        market: "mkt",
        tokenId: "tok-up",
        bids: [{ price: "0.40", size: "10" }],
        asks: [{ price: "0.60", size: "3" }],
        timestamp: "1757908892351",
      },
    };
    expect(normalizeRawClobMarketEvent(canonical)).toBe(canonical);
  });
});
