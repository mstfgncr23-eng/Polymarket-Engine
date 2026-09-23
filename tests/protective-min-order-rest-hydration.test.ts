import { describe, expect, it } from "vitest";
import { evaluateProtectiveDeRiskCandidate, positionSchema, type MarketSnapshot } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  MarketIngressMalformedError,
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
  ShardedPolymarketMarketDataAdapter,
  type MinOrderSizeFetcher,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";

function liveBook(tokenId: string, timestamp: number, minOrderSize?: string, qty = "10") {
  return {
    type: "book",
    payload: {
      tokenId, market: tokenId, timestamp,
      bids: [{ price: "0.47", size: qty }], asks: [{ price: "0.48", size: "5" }],
      lastTradePrice: "0.48",
      ...(minOrderSize !== undefined ? { minOrderSize } : {}),
    },
  };
}

function liveDelta(tokenId: string, timestamp: number, fields: Record<string, unknown> = {}, price = "0.47", size = "9") {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId, side: "BUY", price, size }],
      ...fields,
    },
  };
}

function deferredFetcher(): {
  fetcher: MinOrderSizeFetcher;
  resolvers: Array<(value: string | null) => void>;
  calls: () => number;
} {
  const resolvers: Array<(value: string | null) => void> = [];
  const counters = { count: 0 };
  const fetcher: MinOrderSizeFetcher = () => {
    counters.count += 1;
    return new Promise<string | null>((resolve) => { resolvers.push(resolve); });
  };
  return { fetcher, resolvers, calls: () => counters.count };
}

function instanceWithFetcher(fetcher?: MinOrderSizeFetcher, options: {
  minOrderSizeClock?: () => number;
  minOrderSizeRetryIntervalMs?: number;
} = {}) {
  const instance = new PolymarketMarketDataAdapter(undefined, {
    minOrderSizeFetcher: fetcher,
    minOrderSizeClock: options.minOrderSizeClock,
    minOrderSizeRetryIntervalMs: options.minOrderSizeRetryIntervalMs,
  });
  instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
  instance.registerManualToken("tok-down", "mkt", "DOWN", "BTC");
  instance.setTokenIds(["tok-up", "tok-down"]);
  return instance;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_500): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function tick(count = 1): Promise<void> {
  for (let i = 0; i < count; i += 1) await new Promise((resolve) => setTimeout(resolve, 5));
}

async function metricValue(metrics: EngineMetrics, name: string): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const json = await item.get();
  const values = "values" in json ? json.values as Array<{ value: string | number }> : [];
  return values.reduce((sum, entry) => sum + Number(entry.value), 0);
}

type MetadataEvent = { tokenId: string; minOrderSize: string; tokenEpoch: number };

describe("REST min_order_size hydration for trusted WS books that omit the field", () => {
it("1: WS full book WITH min_order_size never invokes REST and stays authoritative", async () => {
    let calls = 0;
    const instance = instanceWithFetcher(async () => { calls += 1; return "bad"; });
    const first = instance.normalizeSourceEvent(liveBook("tok-up", 1_000, "5"));
    expect(first[0]?.minOrderSize).toBe("5");
    const delta = instance.normalizeSourceEvent(liveDelta("tok-up", 1_001));
    expect(delta[0]?.minOrderSize).toBe("5");
    await tick();
    expect(calls).toBe(0);
    expect(instance.minOrderSizeFetcherCallCountForTest()).toBe(0);
  });

  it("2: WS full book WITHOUT min_order_size is hydrated from REST and preserved by later omit-books", async () => {
    let calls = 0;
    const instance = instanceWithFetcher(async (tokenId) => {
      calls += 1;
      expect(tokenId).toBe("tok-up");
      return "3";
    });
    const first = instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(first[0]?.minOrderSize).toBeNull();
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "3");
    const second = instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    expect(second[0]?.minOrderSize).toBe("3");
    await tick();
    expect(calls).toBe(1);
  });

  it("3: subsequent price_change retains the hydrated value without further REST calls", async () => {
    const instance = instanceWithFetcher(async () => "3");
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "3");
    const before = instance.minOrderSizeFetcherCallCountForTest();
    const delta = instance.normalizeSourceEvent(liveDelta("tok-up", 1_001));
    expect(delta[0]?.minOrderSize).toBe("3");
    expect(instance.minOrderSizeFetcherCallCountForTest()).toBe(before);
  });

  it("4: REST availability failure leaves the field null, bounded, and protective path reports MIN_ORDER_SIZE_UNKNOWN", async () => {
    let calls = 0;
    const instance = instanceWithFetcher(async () => { calls += 1; throw new Error("REST_UNAVAILABLE"); });
    const first = instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(first[0]?.minOrderSize).toBeNull();
    await waitFor(() => calls === 1);
    // Repeated full books within the 60s backoff do not generate unbounded REST.
    for (let seq = 2; seq <= 6; seq += 1) {
      const snapshot = instance.normalizeSourceEvent(liveBook("tok-up", seq * 1_000));
      expect(snapshot[0]?.minOrderSize).toBeNull();
    }
    instance.flushMinOrderSizeHydrationRetriesForTest();
    expect(calls).toBe(1);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();

    const position = positionSchema.parse({
      id: "pos-rest-unknown", marketId: "mkt", upQuantity: "10", downQuantity: "0",
      averageUpCost: "0.40", averageDownCost: "0",
      pairedQuantity: "0", unpairedOutcome: "UP", unpairedQuantity: "10",
      unhedgedStartedAt: "2026-01-01T00:00:00.000Z", combinedPairCost: "0",
      totalFees: "0", totalSlippage: "0", openedAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
    const evaluated = evaluateProtectiveDeRiskCandidate({
      position,
      snapshot: { ...first[0]!, bids: [{ price: "0.90", quantity: "1585.55" }] },
      maxOrderSizeUsd: 25,
    });
    expect(evaluated.candidate).toBeNull();
    expect(evaluated.reason).toBe("MIN_ORDER_SIZE_UNKNOWN");
  });

  it("5: stale completion cannot apply across an epoch advance; a fresh request resolves", async () => {
    const { fetcher, resolvers, calls } = deferredFetcher();
    const instance = instanceWithFetcher(fetcher);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => calls() === 1 && resolvers.length === 1);
    instance.bumpTokenEpochForTest("tok-up");
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    expect(calls()).toBe(1); // one physical flight: no concurrent P2
    resolvers[0]!("99");
    await tick();
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    await waitFor(() => calls() === 2 && resolvers.length === 2); // auto resume after stale settle
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
  });
it("6: real malformed-delta invalidation: stale P1 applies to nothing; auto resume starts exactly one P2; no P3 while pending", async () => {
    const { fetcher, resolvers, calls } = deferredFetcher();
    const instance = instanceWithFetcher(fetcher);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => calls() === 1 && resolvers.length === 1);
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_005, { tickSize: "nope" }))).toThrow(MarketIngressMalformedError);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    await tick();
    expect(calls()).toBe(1); // one physical flight; P1 still running
    resolvers[0]!("99");
    await waitFor(() => calls() === 2 && resolvers.length === 2); // auto resume
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    instance.normalizeSourceEvent(liveBook("tok-up", 3_000));
    await tick();
    expect(calls()).toBe(2); // no P3 while P2 pending
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
  });

  it("7: same-token removal/re-add starts a fresh lifecycle a stale pre-removal request cannot touch", async () => {
    const { fetcher, resolvers, calls } = deferredFetcher();
    const instance = instanceWithFetcher(fetcher);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => calls() === 1 && resolvers.length === 1);
    instance.setTokenIds(["tok-down"]); // removal while flight pending -> deferred deletion + lifecycle bump
    instance.setTokenIds(["tok-up", "tok-down"]); // re-add
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    await tick();
    expect(calls()).toBe(1); // one physical flight
    resolvers[0]!("99"); // stale: lifecycle bumped on removal even though epoch resets to 1
    await waitFor(() => calls() === 2 && resolvers.length === 2);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
    expect(instance.hydratedMinOrderSizeForTest("tok-down")).toBeNull();
  });

  it("8: real run consumer: hydration emits metadata only Ã¢â‚¬â€ no extra snapshot, no sequence change", async () => {
    const { fetcher, resolvers } = deferredFetcher();
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, minOrderSizeFetcher: fetcher });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const snapshots: MarketSnapshot[] = [];
    const metadata: MetadataEvent[] = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: (snapshot) => { snapshots.push(snapshot); },
      onMinOrderSizeHydrated: (event) => { metadata.push(event); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    try {
      await waitFor(() => adapter.activeStreamCount() === 1);
      transport.push(liveBook("tok-up", 1_000));
      await waitFor(() => snapshots.length === 1 && resolvers.length === 1);
      expect(snapshots[0]!.minOrderSize).toBeNull();
      expect(snapshots[0]!.sequence).toBe(1);
      resolvers[0]!("3");
      await waitFor(() => metadata.length === 1);
      await tick();
      // The metadata must report the adapter epoch that was current at apply.
      expect(metadata[0]).toMatchObject({ tokenId: "tok-up", minOrderSize: "3", tokenEpoch: adapter.tokenEpoch("tok-up") });
      expect(snapshots.length).toBe(1); // no synthetic market snapshot
      expect(snapshots[0]!.sequence).toBe(1); // sequence never advanced by metadata
      expect(adapter.hydratedMinOrderSizeForTest("tok-up")).toBe("3");
    } finally {
      controller.abort();
      await running.catch(() => undefined);
    }
  });
it("9: engine metadata-only update patches snapshots/pair without market-event side effects", async () => {
    const market = qualified("btc-live", "BTC", "btc-live-up", "btc-live-down");
    const transport = new ScriptedBoundedMarketTransport();
    const { fetcher, resolvers } = deferredFetcher();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, minOrderSizeFetcher: fetcher });
    adapter.registerManualToken("btc-live-up", market.id, "UP", "BTC");
    adapter.registerManualToken("btc-live-down", market.id, "DOWN", "BTC");
    (adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<ReturnType<typeof discoveryReport>> }).discoverCryptoMarkets = async () => discoveryReport([market]);
    const engine = new EngineService(qualifiedPaperEnv(), new MemoryEventJournal(), { error: () => undefined }, {
      market: adapter,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      await waitFor(() => transport.streams.length >= 1 && engine.state.connections.polymarket.state === "CONNECTED");
      transport.push({ type: "book", payload: { tokenId: "btc-live-up", market: market.id, timestamp: 1_000, bids: [{ price: "0.47", size: "10" }], asks: [{ price: "0.48", size: "10" }] } });
      transport.push({ type: "book", payload: { tokenId: "btc-live-down", market: market.id, timestamp: 1_001, bids: [{ price: "0.52", size: "10" }], asks: [{ price: "0.53", size: "10" }] } });
      await waitFor(() => engine.state.marketSnapshots.has("btc-live-up") && engine.state.marketSnapshots.has("btc-live-down"));
      const before = engine.state.marketSnapshots.get("btc-live-up")!;
      expect(before.minOrderSize).toBeNull();
      const marketMessagesBefore = await metricValue(engine.metrics, "market_messages_total");
      const triggerCandidatesBefore = await metricValue(engine.metrics, "trigger_candidates_total");
      const marketEventsBefore = engine.state.snapshot().decisionFeed.filter((item) => item.type === "MARKET_EVENT").length;
      resolvers[0]!("3");
      await waitFor(() => engine.state.marketSnapshots.get("btc-live-up")?.minOrderSize === "3");
      const after = engine.state.marketSnapshots.get("btc-live-up")!;
      expect(after.sequence).toBe(before.sequence);
      expect(after.bids).toEqual(before.bids);
      expect(after.asks).toEqual(before.asks);
      expect(after.sourceTimestamp).toBe(before.sourceTimestamp);
      expect(after.receivedAt).toBe(before.receivedAt);
      expect(after.eventId).toBe(before.eventId);
      expect(after.minOrderSize).toBe("3");
      const pair = (engine as unknown as { snapshots: Map<string, { UP?: { minOrderSize: string | null } }> }).snapshots.get("btc-live");
      expect(pair?.UP?.minOrderSize).toBe("3");
      // No market-event side effects from hydration metadata.
      expect(await metricValue(engine.metrics, "market_messages_total")).toBe(marketMessagesBefore);
      expect(await metricValue(engine.metrics, "trigger_candidates_total")).toBe(triggerCandidatesBefore);
      expect(engine.state.snapshot().decisionFeed.filter((item) => item.type === "MARKET_EVENT").length).toBe(marketEventsBefore);
      expect(engine.restingOrderCount()).toBe(0);
      expect(engine.ledger.positions.size).toBe(0);
    } finally {
      await engine.stop();
    }
  });

  it("10: production four-shard hydration forwards metadata without dispatchSnapshot/globalSequence", async () => {
    const transports = Array.from({ length: 4 }, () => new ScriptedBoundedMarketTransport());
    let resolveFetch: ((value: string | null) => void) | null = null;
    const adapter = new ShardedPolymarketMarketDataAdapter(undefined, {
      transports,
      minOrderSizeFetcher: () => new Promise<string | null>((resolve) => { resolveFetch = resolve; }),
    });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    expect(adapter.marketShardCount()).toBe(4);
    const snapshots: MarketSnapshot[] = [];
    const metadata: MetadataEvent[] = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: (snapshot) => { snapshots.push(snapshot); },
      onMinOrderSizeHydrated: (event) => { metadata.push(event); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    try {
      await waitFor(() => transports.some((transport) => transport.streams.length >= 1));
      // Only the owning shard subscribes a stream; push the omit-book there.
      const ownerTransport = transports.find((transport) => transport.streams.length >= 1)!;
      ownerTransport.push(liveBook("tok-up", 1_000));
      await waitFor(() => snapshots.length === 1 && resolveFetch != null);
      expect(snapshots[0]!.sequence).toBe(1); // facade global sequence assigned to the one market snapshot
      resolveFetch!("3");
      await waitFor(() => metadata.length === 1);
      await tick(2);
      expect(metadata[0]).toMatchObject({ tokenId: "tok-up", minOrderSize: "3" });
      expect(snapshots.length).toBe(1); // metadata did NOT produce another market snapshot
      expect(snapshots[0]!.sequence).toBe(1); // globalSequence did NOT advance
      expect(adapter.hydratedMinOrderSizeForTest("tok-up")).toBe("3");
      expect(adapter.minOrderSizeHydrationStateCountForTest()).toBe(1);
    } finally {
      controller.abort();
      await running.catch(() => undefined);
    }
  });
it("11: intervening price_change Ã¢â‚¬â€ metadata patches the LATEST snapshot, never the old full book", async () => {
    const market = qualified("btc-live", "BTC", "btc-live-up", "btc-live-down");
    const transport = new ScriptedBoundedMarketTransport();
    const { fetcher, resolvers } = deferredFetcher();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, minOrderSizeFetcher: fetcher });
    adapter.registerManualToken("btc-live-up", market.id, "UP", "BTC");
    adapter.registerManualToken("btc-live-down", market.id, "DOWN", "BTC");
    (adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<ReturnType<typeof discoveryReport>> }).discoverCryptoMarkets = async () => discoveryReport([market]);
    const engine = new EngineService(qualifiedPaperEnv(), new MemoryEventJournal(), { error: () => undefined }, {
      market: adapter,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      await waitFor(() => transport.streams.length >= 1 && engine.state.connections.polymarket.state === "CONNECTED");
      transport.push({ type: "book", payload: { tokenId: "btc-live-up", market: market.id, timestamp: 1_000, bids: [{ price: "0.47", size: "10" }], asks: [{ price: "0.48", size: "10" }] } });
      await waitFor(() => engine.state.marketSnapshots.has("btc-live-up") && resolvers.length === 1);
      // Intervening price_change moves the book to a new level/sequence while
      // REST is still pending.
      transport.push(liveDelta("btc-live-up", 1_010, {}, "0.48", "8"));
      await waitFor(() => engine.state.marketSnapshots.get("btc-live-up")?.bids[0]?.price === "0.48");
      const current = engine.state.marketSnapshots.get("btc-live-up")!;
      expect(current.bids[0]).toMatchObject({ price: "0.48", quantity: "8" });
      resolvers[0]!("3");
      await waitFor(() => engine.state.marketSnapshots.get("btc-live-up")?.minOrderSize === "3");
      const patched = engine.state.marketSnapshots.get("btc-live-up")!;
      expect(patched.sequence).toBe(current.sequence); // the price-change snapshot, not the old book
      expect(patched.bids[0]).toMatchObject({ price: "0.48", quantity: "8" });
      expect(patched.minOrderSize).toBe("3");
    } finally {
      await engine.stop();
    }
  });

  it("12: REAL scripted reconnect Ã¢â‚¬â€ stale pre-reconnect request cannot apply and auto-resume hydrates the new epoch", async () => {
    const { fetcher, resolvers, calls } = deferredFetcher();
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, minOrderSizeFetcher: fetcher });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const metadata: MetadataEvent[] = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onMinOrderSizeHydrated: (event) => { metadata.push(event); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    try {
      await waitFor(() => adapter.activeStreamCount() === 1);
      transport.push(liveBook("tok-up", 1_000));
      await waitFor(() => calls() === 1 && resolvers.length === 1);
      // REAL reconnect path: the stream dies; the run loop reconnects and calls
      // beginStreamEpoch internally (epoch change + lifecycle invalidation).
      transport.current().endRemote();
      await waitFor(() => transport.subscribeCalls >= 2);
      // First omit-book of the new epoch: no new physical flight while P1 runs.
      transport.current().push(liveBook("tok-up", 2_000));
      await tick();
      expect(calls()).toBe(1);
      // Stale P1 resolves: cannot apply, and its settle auto-resumes the fresh request.
      resolvers[0]!("99");
      await waitFor(() => calls() === 2 && resolvers.length === 2);
      expect(adapter.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
      resolvers[1]!("3");
      await waitFor(() => adapter.hydratedMinOrderSizeForTest("tok-up") === "3");
      await waitFor(() => metadata.length === 1);
      expect(metadata[0]?.minOrderSize).toBe("3");
    } finally {
      controller.abort();
      await running.catch(() => undefined);
    }
  });

  it("13: physical flight bound Ã¢â‚¬â€ reconnect/invalidate flapping never exceeds one REST flight", async () => {
    const { fetcher, resolvers, calls } = deferredFetcher();
    const instance = instanceWithFetcher(fetcher);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => calls() === 1 && resolvers.length === 1);
    for (let i = 0; i < 5; i += 1) {
      instance.beginStreamEpoch(["tok-up"], "reconnect");
      instance.normalizeSourceEvent(liveBook("tok-up", (i + 2) * 1_000));
      await tick();
      expect(calls()).toBe(1); // hard bound: never exceeds one physical flight
      expect(instance.minOrderSizeHydrationInFlightForTest()).toBe(1);
    }
    resolvers[0]!("99"); // stale after repeated epoch flapping
    await waitFor(() => calls() === 2 && resolvers.length === 2); // exactly one fresh resume
    expect(instance.minOrderSizeHydrationInFlightForTest()).toBe(1);
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
    expect(instance.minOrderSizeHydrationInFlightForTest()).toBe(0);
  });
it("14: autonomous retry after failure (no WS event) with the injected clock", async () => {
    let clockMs = 1_000_000;
    const responses = [null, "7"]; // attempt 1 fails (unknown), attempt 2 succeeds
    const callsByInstance = { count: 0 };
    const fetcher: MinOrderSizeFetcher = async () => {
      callsByInstance.count += 1;
      return responses.shift() ?? null;
    };
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transport,
      minOrderSizeFetcher: fetcher,
      minOrderSizeClock: () => clockMs,
      minOrderSizeRetryIntervalMs: 60_000,
    });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const metadata: MetadataEvent[] = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onMinOrderSizeHydrated: (event) => { metadata.push(event); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    try {
      await waitFor(() => adapter.activeStreamCount() === 1);
      transport.push(liveBook("tok-up", 1_000));
      await waitFor(() => callsByInstance.count === 1);
      await tick();
      expect(adapter.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
      // Backoff still active: an immediate scan must not fire another request.
      adapter.flushMinOrderSizeHydrationRetriesForTest();
      expect(callsByInstance.count).toBe(1);
      // 60s elapses (no WS event at all) -> autonomous retry succeeds.
      clockMs += 60_001;
      adapter.flushMinOrderSizeHydrationRetriesForTest();
      await waitFor(() => callsByInstance.count === 2);
      await waitFor(() => adapter.hydratedMinOrderSizeForTest("tok-up") === "7");
      await waitFor(() => metadata.length === 1);
      expect(metadata[0]).toMatchObject({ tokenId: "tok-up", minOrderSize: "7" });
      expect(adapter.minOrderSizeHydrationStateCountForTest()).toBe(1);
    } finally {
      controller.abort();
      await running.catch(() => undefined);
    }
  });

  it("15: token churn keeps hydration bookkeeping bounded to managed tokens", async () => {
    const { fetcher, resolvers } = deferredFetcher();
    const instance = instanceWithFetcher(fetcher);
    for (let i = 0; i < 30; i += 1) {
      const token = `t${i}`;
      instance.registerManualToken(token, "mkt", "UP", "BTC");
      instance.setTokenIds([token]);
      instance.normalizeSourceEvent(liveBook(token, 1_000));
      await waitFor(() => resolvers.length === i + 1);
      resolvers[i]!(String(i));
      await waitFor(() => instance.hydratedMinOrderSizeForTest(token) === String(i));
      instance.setTokenIds([]); // no in-flight on removal -> immediate deletion
      expect(instance.minOrderSizeHydrationStateCountForTest()).toBe(0);
    }
    // Deferred stale-flight removal: entry is released once the stale flight settles.
    instance.registerManualToken("t-stale", "mkt", "UP", "BTC");
    instance.setTokenIds(["t-stale"]);
    instance.normalizeSourceEvent(liveBook("t-stale", 1_000));
    await waitFor(() => resolvers.length === 31);
    instance.setTokenIds([]);
    expect(instance.minOrderSizeHydrationStateCountForTest()).toBe(1); // released on settle
    resolvers[30]!("5");
    await waitFor(() => instance.minOrderSizeHydrationStateCountForTest() === 0);
    expect(instance.hydratedMinOrderSizeForTest("t-stale")).toBeNull();
  });
});
