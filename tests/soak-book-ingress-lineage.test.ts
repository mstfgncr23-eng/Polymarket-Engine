import { describe, expect, it } from "vitest";
import { BoundedClobMarketTransport, PolymarketMarketDataAdapter } from "@pm/market-data";
import { marketSnapshotSchema } from "@pm/core";

class Socket extends EventTarget {
  static current: Socket | null = null;
  readyState = 0;
  constructor(readonly url: string) { super(); Socket.current = this; }
  send(_raw: string): void {}
  close(): void { this.readyState = 3; }
  open(): void { this.readyState = 1; this.dispatchEvent(new Event("open")); }
  message(payload: unknown): void {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(payload) }));
  }
}

function adapter() {
  const client = new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
  client.registerManualToken("tok-up", "market-one", "UP", "BTC");
  client.setTokenIds(["tok-up"]);
  return client;
}

describe("P0 soak book ingress lineage", () => {
  it("captures owned WebSocket callback time and carries it through trusted book and price-change normalization", async () => {
    const transport = new BoundedClobMarketTransport({
      webSocket: Socket as unknown as typeof WebSocket,
      pingIntervalMs: 600_000,
      staleMs: 600_000,
    });
    const pending = transport.subscribe(["tok-up"]);
    const socket = Socket.current!;
    socket.open();
    const stream = await pending;
    const iter = stream[Symbol.asyncIterator]();
    const client = adapter();
    const before = Date.now();
    socket.message({
      event_type: "book",
      asset_id: "tok-up", market: "market-one",
      bids: [{ price: "0.40", size: "10" }],
      asks: [{ price: "0.60", size: "5" }],
      hash: "h0", timestamp: "1757908892351",
      min_order_size: "1", tick_size: "0.01",
    });
    const bookEvent = (await iter.next()).value;
    const full = client.normalizeSourceEvent(bookEvent)[0]!;
    expect(full.wsCallbackAt).toBeDefined();
    expect(Date.parse(full.wsCallbackAt!)).toBeGreaterThanOrEqual(before);
    expect(Date.parse(full.wsCallbackAt!)).toBeLessThanOrEqual(Date.now());
    expect(Date.parse(full.receivedAt)).toBeGreaterThanOrEqual(Date.parse(full.wsCallbackAt!));
    expect(marketSnapshotSchema.parse(full).wsCallbackAt).toBe(full.wsCallbackAt);

    socket.message({
      event_type: "price_change", market: "market-one",
      timestamp: "1757908892352",
      price_changes: [{ asset_id: "tok-up", price: "0.41", size: "2", side: "BUY", hash: "h1" }],
    });
    const priceEvent = (await iter.next()).value;
    const delta = client.normalizeSourceEvent(priceEvent)[0]!;
    expect(delta.wsCallbackAt).toBeDefined();
    expect(Date.parse(delta.receivedAt)).toBeGreaterThanOrEqual(Date.parse(delta.wsCallbackAt!));
    expect(delta.hash).toBe("h1");
    await stream.close();
  });

  it("does not pretend SDK/replayed data passed through owned WebSocket callback", () => {
    const client = adapter();
    const event = { type: "book", payload: {
      tokenId: "tok-up", market: "market-one",
      bids: [{ price: "0.40", size: "10" }],
      asks: [{ price: "0.60", size: "5" }],
      hash: "sdk", timestamp: "1757908892351",
      minOrderSize: "1", tickSize: "0.01",
    } };
    const value = client.normalizeSourceEvent(event)[0]!;
    expect(value.wsCallbackAt).toBeUndefined();
  });
});
