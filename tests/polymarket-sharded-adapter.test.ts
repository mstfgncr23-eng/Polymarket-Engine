import { describe, expect, it } from "vitest";
import {
  DEFAULT_POLYMARKET_MARKET_SHARD_COUNT,
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
} from "@pm/market-data";

async function waitFor(predicate: () => boolean, timeoutMs = 2_500): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

class RecordingTransport extends ScriptedBoundedMarketTransport {
  readonly tokenCalls: string[][] = [];
  readonly customFeatureCalls: boolean[] = [];

  override async subscribe(tokenIds: readonly string[], customFeatureEnabled = true, signal?: AbortSignal) {
    this.tokenCalls.push([...tokenIds]);
    this.customFeatureCalls.push(customFeatureEnabled);
    return super.subscribe(tokenIds, customFeatureEnabled, signal);
  }
}

function book(tokenId: string, marketId: string, timestamp: number) {
  return {
    type: "book",
    payload: {
      tokenId,
      market: marketId,
      timestamp,
      bids: [{ price: "0.47", size: "10" }],
      asks: [{ price: "0.48", size: "10" }],
      lastTradePrice: "0.475",
    },
  };
}

function fourTransports() {
  return [
    new RecordingTransport(),
    new RecordingTransport(),
    new RecordingTransport(),
    new RecordingTransport(),
  ] as const;
}

function registerMarket(adapter: PolymarketMarketDataAdapter, market: number): string[] {
  const id = `m${market}`;
  const asset = ["BTC", "ETH", "SOL", "XRP", "DOGE"][market - 1] ?? "BTC";
  adapter.registerManualToken(`${id}-up`, id, "UP", asset);
  adapter.registerManualToken(`${id}-down`, id, "DOWN", asset);
  return [`${id}-up`, `${id}-down`];
}

describe("pair-aware sharded Polymarket market feed", () => {
  it("uses four shards by default in the zero-argument production path", () => {
    const adapter = new PolymarketMarketDataAdapter();
    expect(DEFAULT_POLYMARKET_MARKET_SHARD_COUNT).toBe(4);
    expect(adapter.marketShardCount()).toBe(4);
  });

  it("rejects an empty explicit transport set", () => {
    expect(() => new PolymarketMarketDataAdapter(undefined, { transports: [] }))
      .toThrow("MARKET_SHARD_TRANSPORT_COUNT:0");
  });

  it("keeps both outcomes together across four shards, serializes Engine delivery, and disables the custom feature", async () => {
    const transports = fourTransports();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports,
      stableAfterMs: 0,
    });

    const tokenIds = [1, 2, 3, 4].flatMap((market) => registerMarket(adapter, market));
    adapter.setTokenIds(tokenIds);

    const abort = new AbortController();
    const sequences: number[] = [];
    let handlersInFlight = 0;
    let maxHandlersInFlight = 0;

    const running = adapter.run(tokenIds, {
      onSnapshot: async (snapshot) => {
        handlersInFlight += 1;
        maxHandlersInFlight = Math.max(maxHandlersInFlight, handlersInFlight);
        await new Promise((resolve) => setTimeout(resolve, 1));
        sequences.push(snapshot.sequence);
        handlersInFlight -= 1;
      },
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => transports.every((transport) => transport.subscribeCalls === 1));

    for (let index = 0; index < transports.length; index += 1) {
      const market = index + 1;
      expect(transports[index]!.tokenCalls[0]).toEqual([`m${market}-up`, `m${market}-down`]);
      expect(transports[index]!.customFeatureCalls).toEqual([false]);
    }
    expect(adapter.marketShardCount()).toBe(4);
    expect(adapter.shardSnapshots().map((snapshot) => snapshot.tokenCount)).toEqual([2, 2, 2, 2]);

    transports[0].push(book("m1-up", "m1", 1_000));
    transports[1].push(book("m2-up", "m2", 1_001));
    transports[2].push(book("m3-up", "m3", 1_002));
    transports[3].push(book("m4-up", "m4", 1_003));
    transports[0].push(book("m1-down", "m1", 1_004));
    transports[1].push(book("m2-down", "m2", 1_005));
    transports[2].push(book("m3-down", "m3", 1_006));
    transports[3].push(book("m4-down", "m4", 1_007));

    await waitFor(() => sequences.length === 8);
    expect(sequences).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(maxHandlersInFlight).toBe(1);
    expect(tokenIds.every((tokenId) => adapter.isTokenSynchronized(tokenId))).toBe(true);

    abort.abort("test");
    await running;
  });

  it("reconnects and gaps only the failed shard while the other three stay trusted", async () => {
    const transports = fourTransports();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports,
      stableAfterMs: 0,
    });

    const tokenIds = [1, 2, 3, 4].flatMap((market) => registerMarket(adapter, market));
    adapter.setTokenIds(tokenIds);

    const abort = new AbortController();
    const gaps: Array<{ reason: string; tokenIds: string[] }> = [];
    const running = adapter.run(tokenIds, {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
      onGap: (event) => gaps.push({ reason: event.reason, tokenIds: [...event.tokenIds] }),
    }, abort.signal);

    await waitFor(() => transports.every((transport) => transport.subscribeCalls === 1));

    for (let index = 0; index < transports.length; index += 1) {
      const market = index + 1;
      transports[index]!.push(book(`m${market}-up`, `m${market}`, 1_000 + index * 2));
      transports[index]!.push(book(`m${market}-down`, `m${market}`, 1_001 + index * 2));
    }

    await waitFor(() => tokenIds.every((tokenId) => adapter.isTokenSynchronized(tokenId)));
    const healthyStreams = transports.slice(1).map((transport) => transport.current());

    transports[0].current().endRemote();

    await waitFor(() => transports[0].subscribeCalls === 2);
    for (let index = 1; index < transports.length; index += 1) {
      expect(transports[index]!.subscribeCalls).toBe(1);
      expect(transports[index]!.current()).toBe(healthyStreams[index - 1]);
      expect(healthyStreams[index - 1]!.alive).toBe(true);
      expect(adapter.isTokenSynchronized(`m${index + 1}-up`)).toBe(true);
      expect(adapter.isTokenSynchronized(`m${index + 1}-down`)).toBe(true);
    }
    expect(gaps).toContainEqual({ reason: "reconnect", tokenIds: ["m1-up", "m1-down"] });
    expect(gaps.some((gap) => gap.tokenIds.some((tokenId) => !tokenId.startsWith("m1-")))).toBe(false);

    abort.abort("test");
    await running;
  });

  it("keeps unchanged shards alive when discovery adds a market to the least-loaded shard", async () => {
    const transports = fourTransports();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports,
      stableAfterMs: 0,
    });

    const initial = [1, 2, 3, 4].flatMap((market) => registerMarket(adapter, market));
    adapter.setTokenIds(initial);

    const abort = new AbortController();
    const running = adapter.run(initial, {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => transports.every((transport) => transport.subscribeCalls === 1));
    const unchanged = transports.slice(1).map((transport) => transport.current());

    const added = registerMarket(adapter, 5);
    adapter.setTokenIds([...initial, ...added]);

    await waitFor(() => transports[0].subscribeCalls === 2);
    expect(transports[0].tokenCalls[1]).toEqual(["m1-up", "m1-down", "m5-up", "m5-down"]);
    for (let index = 1; index < transports.length; index += 1) {
      expect(transports[index]!.subscribeCalls).toBe(1);
      expect(transports[index]!.current()).toBe(unchanged[index - 1]);
      expect(unchanged[index - 1]!.alive).toBe(true);
    }

    abort.abort("test");
    await running;
  });

  it("keeps shard observability side-effect free", async () => {
    const transports = fourTransports();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transports, stableAfterMs: 0 });
    const tokenIds = [1, 2, 3, 4].flatMap((market) => registerMarket(adapter, market));

    adapter.setShardObserver({
      onChange: () => { throw new Error("observer failure"); },
      onGap: () => { throw new Error("observer failure"); },
      onResync: () => { throw new Error("observer failure"); },
      onError: () => { throw new Error("observer failure"); },
    });
    adapter.setTokenIds(tokenIds);

    const abort = new AbortController();
    const snapshots: string[] = [];
    const running = adapter.run(tokenIds, {
      onSnapshot: (snapshot) => snapshots.push(snapshot.tokenId),
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => transports.every((transport) => transport.subscribeCalls === 1));
    transports[0].push(book("m1-up", "m1", 1_000));
    await waitFor(() => snapshots.length === 1);
    expect(snapshots).toEqual(["m1-up"]);

    abort.abort("test");
    await running;
  });
});
