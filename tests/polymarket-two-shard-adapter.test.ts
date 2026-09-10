import { describe, expect, it } from "vitest";
import {
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

describe("pair-aware two-shard Polymarket market feed", () => {
  it("keeps both outcomes together, serializes Engine delivery, and uses custom_feature_enabled=false", async () => {
    const shardA = new RecordingTransport();
    const shardB = new RecordingTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports: [shardA, shardB],
      stableAfterMs: 0,
    });

    adapter.registerManualToken("m1-up", "m1", "UP", "BTC");
    adapter.registerManualToken("m1-down", "m1", "DOWN", "BTC");
    adapter.registerManualToken("m2-up", "m2", "UP", "ETH");
    adapter.registerManualToken("m2-down", "m2", "DOWN", "ETH");

    const tokenIds = ["m1-up", "m1-down", "m2-up", "m2-down"];
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

    await waitFor(() => shardA.subscribeCalls === 1 && shardB.subscribeCalls === 1);

    expect(shardA.tokenCalls[0]).toEqual(["m1-up", "m1-down"]);
    expect(shardB.tokenCalls[0]).toEqual(["m2-up", "m2-down"]);
    expect(shardA.customFeatureCalls).toEqual([false]);
    expect(shardB.customFeatureCalls).toEqual([false]);
    expect(adapter.marketShardCount()).toBe(2);

    shardA.push(book("m1-up", "m1", 1_000));
    shardB.push(book("m2-up", "m2", 1_001));
    shardA.push(book("m1-down", "m1", 1_002));
    shardB.push(book("m2-down", "m2", 1_003));

    await waitFor(() => sequences.length === 4);
    expect(sequences).toEqual([1, 2, 3, 4]);
    expect(maxHandlersInFlight).toBe(1);
    expect(tokenIds.every((tokenId) => adapter.isTokenSynchronized(tokenId))).toBe(true);

    abort.abort("test");
    await running;
  });

  it("reconnects and gaps only the failed shard while the healthy shard stays trusted", async () => {
    const shardA = new RecordingTransport();
    const shardB = new RecordingTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports: [shardA, shardB],
      stableAfterMs: 0,
    });

    adapter.registerManualToken("m1-up", "m1", "UP", "BTC");
    adapter.registerManualToken("m1-down", "m1", "DOWN", "BTC");
    adapter.registerManualToken("m2-up", "m2", "UP", "ETH");
    adapter.registerManualToken("m2-down", "m2", "DOWN", "ETH");

    const tokenIds = ["m1-up", "m1-down", "m2-up", "m2-down"];
    adapter.setTokenIds(tokenIds);

    const abort = new AbortController();
    const gaps: Array<{ reason: string; tokenIds: string[] }> = [];
    const running = adapter.run(tokenIds, {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
      onGap: (event) => gaps.push({ reason: event.reason, tokenIds: [...event.tokenIds] }),
    }, abort.signal);

    await waitFor(() => shardA.subscribeCalls === 1 && shardB.subscribeCalls === 1);

    shardA.push(book("m1-up", "m1", 1_000));
    shardA.push(book("m1-down", "m1", 1_001));
    shardB.push(book("m2-up", "m2", 1_002));
    shardB.push(book("m2-down", "m2", 1_003));

    await waitFor(() => tokenIds.every((tokenId) => adapter.isTokenSynchronized(tokenId)));
    const healthyStream = shardB.current();

    shardA.current().endRemote();

    await waitFor(() => shardA.subscribeCalls === 2);
    expect(shardB.subscribeCalls).toBe(1);
    expect(shardB.current()).toBe(healthyStream);
    expect(healthyStream.alive).toBe(true);
    expect(adapter.isTokenSynchronized("m2-up")).toBe(true);
    expect(adapter.isTokenSynchronized("m2-down")).toBe(true);
    expect(gaps).toContainEqual({ reason: "reconnect", tokenIds: ["m1-up", "m1-down"] });
    expect(gaps.some((gap) => gap.tokenIds.includes("m2-up") || gap.tokenIds.includes("m2-down"))).toBe(false);

    abort.abort("test");
    await running;
  });

  it("keeps an unchanged shard alive when discovery adds a market to the other shard", async () => {
    const shardA = new RecordingTransport();
    const shardB = new RecordingTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transports: [shardA, shardB],
      stableAfterMs: 0,
    });

    adapter.registerManualToken("m1-up", "m1", "UP", "BTC");
    adapter.registerManualToken("m1-down", "m1", "DOWN", "BTC");
    adapter.registerManualToken("m2-up", "m2", "UP", "ETH");
    adapter.registerManualToken("m2-down", "m2", "DOWN", "ETH");

    const initial = ["m1-up", "m1-down", "m2-up", "m2-down"];
    adapter.setTokenIds(initial);

    const abort = new AbortController();
    const running = adapter.run(initial, {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => shardA.subscribeCalls === 1 && shardB.subscribeCalls === 1);
    const unchangedB = shardB.current();

    adapter.registerManualToken("m3-up", "m3", "UP", "SOL");
    adapter.registerManualToken("m3-down", "m3", "DOWN", "SOL");
    adapter.setTokenIds([...initial, "m3-up", "m3-down"]);

    await waitFor(() => shardA.subscribeCalls === 2);
    expect(shardA.tokenCalls[1]).toEqual(["m1-up", "m1-down", "m3-up", "m3-down"]);
    expect(shardB.subscribeCalls).toBe(1);
    expect(shardB.current()).toBe(unchangedB);
    expect(unchangedB.alive).toBe(true);

    abort.abort("test");
    await running;
  });
});
