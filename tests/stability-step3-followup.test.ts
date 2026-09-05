import { describe, expect, it } from "vitest";
import {
  createId,
  isMarketDataDerivedTrigger,
  MARKET_DATA_TRIGGER_KINDS,
  NON_SUPPRESSIBLE_ON_MARKET_GAP_KINDS,
  TRIGGER_STABILITY_BOUNDS,
  TriggerEngine,
  triggerConfigSchema,
  type MarketSnapshot,
  type ReferenceTick,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  BoundedClobMarketTransport,
  BoundedIngressQueue,
  CLOB_MARKET_CLOSED_BEFORE_OPEN,
  CLOB_MARKET_CONNECT_ABORTED,
  CLOB_MARKET_CONNECT_TIMEOUT,
  INGRESS_OBSERVER_BOUNDS,
  POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS,
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { ALLOWED_MCP_READ_TOOLS, ALLOWED_MCP_TOOLS, mcpVisibleMarketSnapshots, mcpVisibleOrderbook } from "../apps/engine/src/mcp.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { order, snapshot } from "./fixtures.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function drainRuntime(engine: EngineService): Promise<void> {
  await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0);
}

function marketTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  return {
    id: createId("trigger"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    asset: "BTC",
    source: "polymarket",
    kind: "BOOK_IMBALANCE",
    severity: 2,
    score: 1,
    dedupeKey: "book",
    evidence: {},
    sourceEventIds: ["e1"],
    evidenceIds: ["e1"],
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 30_000).toISOString(),
    ...overrides,
  };
}

function riskTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  return marketTrigger({
    kind: "DIRECTIONAL_EXPOSURE",
    source: "risk",
    severity: 2,
    ...overrides,
  });
}

function book(outcome: "UP" | "DOWN", ask: string, extra: Record<string, unknown> = {}) {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return {
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId,
    outcome,
    asset: "BTC",
    bids: [{ price: "0.40", quantity: "10" }],
    asks: [{ price: ask, quantity: "10" }],
    lastTradePrice: ask,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: null,
    sourceTimestamp: new Date().toISOString(),
    receivedAt: new Date().toISOString(),
    sequence: 1,
    hash: null,
    ...extra,
  };
}

function refTick(asset: string, price: string, atMs: number, extra: Partial<ReferenceTick> = {}): ReferenceTick {
  const at = new Date(atMs).toISOString();
  return {
    eventId: createId("ref"),
    traceId: createId("trace"),
    asset,
    symbol: `${asset}USDT`,
    venue: "binance",
    price,
    quantity: "1",
    sourceTimestamp: at,
    receivedAt: at,
    tradeId: createId("trade"),
    ...extra,
  };
}

async function metricValue(metrics: EngineMetrics, name: string, labels?: Record<string, string>): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const data = await item.get();
  const points = data.values.filter((point) => {
    if (point.metricName?.endsWith("_bucket") || point.metricName?.endsWith("_sum")) return false;
    if (labels) return Object.entries(labels).every(([key, value]) => point.labels[key] === value);
    return true;
  });
  if (labels) return points[0]?.value ?? 0;
  return points.reduce((sum, point) => {
    if (point.metricName?.endsWith("_count")) return sum + point.value;
    if (!point.metricName || point.metricName === name) return sum + point.value;
    return sum;
  }, 0);
}

async function startPaper(journal: MemoryEventJournal) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
  const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
  });
  await engine.start();
  return { engine, market };
}

describe("Blocker 1 — SDK subscribe is not a market-stream runtime path", () => {
  it("every supported runtime adapter config selects a bounded transport", () => {
    const def = new PolymarketMarketDataAdapter();
    expect(def.usesBoundedMarketTransport()).toBe(true);

    const injected = new PolymarketMarketDataAdapter({
      subscribe: async () => {
        throw new Error("SDK_SUBSCRIBE_MUST_NOT_RUN");
      },
      closeSubscriptions: async () => undefined,
      fetchMarket: async () => ({}),
      listEvents: async function* () {},
    } as never);
    expect(injected.usesBoundedMarketTransport()).toBe(true);

    const scripted = new PolymarketMarketDataAdapter(undefined, { transport: new ScriptedBoundedMarketTransport() });
    expect(scripted.usesBoundedMarketTransport()).toBe(true);

    expect(POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS.bounded).toBe(false);
  });

  it("rejects an unbounded injected transport and never calls PublicClient.subscribe", async () => {
    expect(() => new PolymarketMarketDataAdapter(undefined, {
      transport: { bounded: false, subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }) } as never,
    })).toThrow(/MARKET_TRANSPORT_MUST_BE_BOUNDED/);

    let sdkSubscribe = 0;
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter({
      subscribe: async () => {
        sdkSubscribe += 1;
        throw new Error("SDK_SUBSCRIBE_MUST_NOT_RUN");
      },
      closeSubscriptions: async () => undefined,
      fetchMarket: async () => ({}),
      listEvents: async function* () {},
    } as never, { transport });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    const ac = new AbortController();
    const connections: string[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => connections.includes("CONNECTED"));
    ac.abort();
    await running;
    expect(sdkSubscribe).toBe(0);
    expect(transport.subscribeCalls).toBeGreaterThan(0);
    expect(adapter.usesBoundedMarketTransport()).toBe(true);
  });
});

describe("Blockers 2-3 — gap invalidates projections and forbids mixed-epoch pair cost", () => {
  it("keeps the MCP tool allowlist unchanged", () => {
    expect(ALLOWED_MCP_READ_TOOLS).toEqual(expect.arrayContaining([
      "get_market_snapshot", "get_orderbook", "get_reference_context",
    ]));
    expect(ALLOWED_MCP_TOOLS.has("get_orderbook")).toBe(true);
    expect(ALLOWED_MCP_TOOLS.has("place_order")).toBe(false);
  });

  it("sync UP+DOWN → gap both → fresh UP only → no PAIR_COST and stale DOWN not via MCP", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.60", { sequence: 1 }));
    await market.emitSnapshot(book("DOWN", "0.47", { sequence: 1, eventId: createId("market") }));
    await drainRuntime(engine);
    expect(mcpVisibleOrderbook(engine.state, "btc-down", (id) => engine.isTokenSynchronized(id))?.asks[0]?.price).toBe("0.47");

    market.emitGap(["btc-up", "btc-down"]);
    expect(engine.isMarketSynchronized("btc-mkt")).toBe(false);
    expect(engine.state.marketSnapshots.get("btc-up")).toBeUndefined();
    expect(engine.state.marketSnapshots.get("btc-down")).toBeUndefined();
    expect(mcpVisibleOrderbook(engine.state, "btc-up", (id) => engine.isTokenSynchronized(id))).toBeNull();
    expect(mcpVisibleOrderbook(engine.state, "btc-down", (id) => engine.isTokenSynchronized(id))).toBeNull();
    expect(mcpVisibleMarketSnapshots(engine.state, "btc-mkt", (id) => engine.isTokenSynchronized(id))).toEqual([]);

    market.markTokenSync("btc-up", "SYNCED");
    await market.emitSnapshot(book("UP", "0.47", { sequence: 2, eventId: createId("market") }));
    await drainRuntime(engine);
    const afterUp = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterUp.some((event) => event.payload.kind === "PAIR_COST_OPPORTUNITY")).toBe(false);
    expect(engine.state.marketSnapshots.get("btc-down")).toBeUndefined();
    expect(mcpVisibleOrderbook(engine.state, "btc-down", (id) => engine.isTokenSynchronized(id))).toBeNull();
    expect(mcpVisibleMarketSnapshots(engine.state, "btc-mkt", (id) => engine.isTokenSynchronized(id)).map((item) => item.outcome)).toEqual(["UP"]);

    market.markTokenSync("btc-down", "SYNCED");
    const downBaseline = await market.emitSnapshot(book("DOWN", "0.60", { sequence: 2, eventId: createId("market") }));
    void downBaseline;
    await drainRuntime(engine);
    const afterDown = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterDown.some((event) => event.payload.kind === "SPREAD_CHANGE")).toBe(false);
    expect(afterDown.some((event) => event.payload.kind === "DEPTH_CHANGE")).toBe(false);
    expect(afterDown.some((event) => event.payload.kind === "PAIR_COST_OPPORTUNITY")).toBe(false);
    expect(engine.state.marketSnapshots.get("btc-up")?.asks[0]?.price).toBe("0.47");
    expect(engine.state.marketSnapshots.get("btc-down")?.asks[0]?.price).toBe("0.60");

    await market.emitSnapshot(book("DOWN", "0.47", { sequence: 3, eventId: createId("market") }));
    await drainRuntime(engine);
    const afterEvolve = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterEvolve.some((event) => event.payload.kind === "PAIR_COST_OPPORTUNITY")).toBe(true);
    await engine.stop();
  });
});

describe("Blocker 4 — pre-gap queued market triggers do not activate after gap", () => {
  it("classifies a finite suppress vs never-suppress set", () => {
    for (const kind of MARKET_DATA_TRIGGER_KINDS) {
      expect(isMarketDataDerivedTrigger({ kind, source: "polymarket" })).toBe(true);
    }
    for (const kind of NON_SUPPRESSIBLE_ON_MARKET_GAP_KINDS) {
      expect(isMarketDataDerivedTrigger({ kind, source: "risk" })).toBe(false);
      expect(isMarketDataDerivedTrigger({ kind, source: "polymarket" })).toBe(false);
    }
  });

  it("holds a Polymarket market trigger, gaps, then drops it without TRIGGER_EVENT or Grok", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    try {
      engine.setProcessTriggerHoldForTest(hold);
      const stale = marketTrigger({ id: "stale-book" });
      const admitted = engine.enqueueTriggerStormForTest([stale]);
      expect(admitted.admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      market.emitGap(["btc-up", "btc-down"]);
      expect(engine.isMarketSynchronized("btc-mkt")).toBe(false);
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT"] });
      expect(events.some((event) => event.payload.id === "stale-book" || event.payload.triggerId === "stale-book")).toBe(false);
      expect(engine.state.recentTriggers.some((item) => item.id === "stale-book")).toBe(false);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });

  it("risk P0 still journals after the market is GAPPED", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    market.emitGap(["btc-up", "btc-down"]);
    const risk = riskTrigger({ id: "risk-p0" });
    engine.enqueueTriggerStormForTest([risk]);
    await drainRuntime(engine);
    const events = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(events.some((event) => event.payload.id === "risk-p0")).toBe(true);
    await engine.stop();
  });
});

describe("Blocker 5 — resting token index matches authoritative orders after tail replay", () => {
  it("A: checkpoint before reserve + durable reserve in the tail rebuilds the index", async () => {
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal);
    await first.engine.checkpointForTest();
    const maker = order("MAKER", { id: "maker-tail-a", marketId: "btc-mkt", tokenId: "btc-up" });
    await first.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    expect(first.engine.restingIndexOrderIdsForTest("btc-up")).toContain("maker-tail-a");
    first.engine.abandonWithoutCheckpointForTest();
    await first.engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.restingOrdersForTest().has("maker-tail-a")).toBe(true);
    expect(recovered.engine.restingIndexOrderIdsForTest("btc-up")).toEqual(["maker-tail-a"]);
    expect(recovered.engine.restingOrderTokenIndexEntries()).toBe(1);
    await recovered.engine.stop();
  });

  it("B: checkpoint with maker + durable cancel leaves no orphan index entry", async () => {
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal);
    const maker = order("MAKER", { id: "maker-tail-b", marketId: "btc-mkt", tokenId: "btc-up" });
    await first.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    await first.engine.checkpointForTest();
    expect(first.engine.restingIndexOrderIdsForTest("btc-up")).toContain("maker-tail-b");
    await first.engine.cancelMakerForTest("maker-tail-b");
    first.engine.abandonWithoutCheckpointForTest();
    await first.engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.restingOrdersForTest().has("maker-tail-b")).toBe(false);
    expect(recovered.engine.restingIndexOrderIdsForTest("btc-up")).toEqual([]);
    expect(recovered.engine.restingOrderTokenIndexEntries()).toBe(0);
    await recovered.engine.stop();
  });

  it("C: no checkpoint + durable maker where valid rebuilds the index from the tail", async () => {
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal);
    const maker = order("MAKER", { id: "maker-tail-c", marketId: "btc-mkt", tokenId: "btc-up" });
    await first.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    first.engine.abandonWithoutCheckpointForTest();
    await first.engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.restingOrdersForTest().has("maker-tail-c")).toBe(true);
    expect(recovered.engine.restingIndexOrderIdsForTest("btc-up")).toEqual(["maker-tail-c"]);
    await recovered.engine.stop();
  });
});

describe("Blockers 6-7 — reference overflow is sticky and CONNECTED is not trusted", () => {
  it("overflow blocks entry; CONNECTED stays unreliable; first tick is baseline; ETH does not disable BTC", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([
      qualified("btc-mkt", "BTC", "btc-up", "btc-down"),
      qualified("eth-mkt", "ETH", "eth-up", "eth-down"),
    ]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    });
    engine.attachReferenceWindowForTest("ETH", 4);
    engine.attachReferenceWindowForTest("BTC", 8);
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 4; i += 1) {
      await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + i * 100));
    }
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 400));
    expect(engine.referenceUnreliableForTest("ETH")).toBe(true);
    expect(engine.referenceWindowOverflowedForTest("ETH")).toBe(true);
    expect(engine.openFirstLegBlockedByUnreliableReferenceForTest("eth-mkt")).toBe(true);
    expect(referenceRegistry.stopCounts.get("ETH")).toBe(1);

    await engine.ingestReferenceTickForTest(refTick("ETH", "9000", t0 + 500));
    expect(referenceRegistry.stopCounts.get("ETH")).toBe(1);
    const afterOverflow = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterOverflow.some((event) => String(event.payload.kind).includes("REFERENCE"))).toBe(false);

    await engine.applyReferenceConnectionForTest("ETH", "CONNECTED", 1);
    expect(engine.referenceUnreliableForTest("ETH")).toBe(true);
    expect(engine.openFirstLegBlockedByUnreliableReferenceForTest("eth-mkt")).toBe(true);
    const context = engine.state.getReferenceContext("ETH", t0 + 600);
    expect(context.latestPrice).toBeNull();
    expect(context.freshness).not.toBe("fresh");
    expect(context.lastMessageAt).toBeNull();

    await engine.ingestReferenceTickForTest(refTick("ETH", "9000", t0 + 1_000));
    expect(engine.referenceUnreliableForTest("ETH")).toBe(false);
    const afterBaseline = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterBaseline.some((event) => event.payload.kind === "CRYPTO_REFERENCE_RAPID_MOVE")).toBe(false);

    await engine.ingestReferenceTickForTest(refTick("ETH", "9030", t0 + 1_100));
    await drainRuntime(engine);
    const afterSecond = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterSecond.some((event) => event.payload.kind === "CRYPTO_REFERENCE_RAPID_MOVE")).toBe(true);

    await engine.ingestReferenceTickForTest(refTick("BTC", "100", t0));
    await engine.ingestReferenceTickForTest(refTick("BTC", "100.20", t0 + 200));
    await drainRuntime(engine);
    expect(engine.referenceUnreliableForTest("BTC")).toBe(false);
    const btcMoves = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(btcMoves.some((event) => event.payload.kind === "BTC_RAPID_MOVE")).toBe(true);
    await engine.stop();
  });
});

describe("Blocker 10 — live ingress metrics and one-logical-overflow gap accounting", () => {
  it("updates depth/bytes/high-water on enqueue/dequeue without a gap", () => {
    const seen: Array<{ depth: number; bytes: number; highWaterEvents: number; gapTotal: number }> = [];
    const queue = new BoundedIngressQueue(4, 10_000);
    queue.setObserver({
      onChange: (snapshot) => {
        seen.push({
          depth: snapshot.depth,
          bytes: snapshot.bytes,
          highWaterEvents: snapshot.highWaterEvents,
          gapTotal: snapshot.gapTotal,
        });
      },
    });
    expect(queue.tryEnqueue("aa").ok).toBe(true);
    expect(queue.tryEnqueue("bbbb").ok).toBe(true);
    queue.flushObserver();
    expect(seen.at(-1)?.depth).toBe(2);
    expect(seen.at(-1)?.bytes).toBe(6);
    expect(seen.at(-1)?.highWaterEvents).toBe(2);
    expect(seen.at(-1)?.gapTotal).toBe(0);
    expect(queue.dequeue()).toBe("aa");
    queue.flushObserver();
    expect(seen.at(-1)?.depth).toBe(1);
    expect(seen.at(-1)?.gapTotal).toBe(0);
    expect(queue.snapshot().accepted).toBe(2);
    expect(queue.snapshot().processed).toBe(1);
  });

  it("counts one logical overflow once and clear() does not double-count", () => {
    const queue = new BoundedIngressQueue(2, 64);
    expect(queue.tryEnqueue("a").ok).toBe(true);
    expect(queue.tryEnqueue("b").ok).toBe(true);
    expect(queue.tryEnqueue("c").ok).toBe(false);
    expect(queue.gapTotal).toBe(1);
    queue.clear();
    expect(queue.gapTotal).toBe(1);
    expect(queue.depth).toBe(0);
  });

  it("engine ingress gauges move on owned-queue changes and first AWAITING_BOOK subscribe is not an overflow gap", async () => {
    const transport = new ScriptedBoundedMarketTransport(new BoundedIngressQueue(8, 10_000));
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    const metrics = new EngineMetrics();
    adapter.setIngressObserver({ onChange: (snapshot) => metrics.noteIngressSnapshot(snapshot) });
    const ac = new AbortController();
    const gaps: string[] = [];
    const connections: string[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
      onGap: (event) => { gaps.push(event.reason); },
    }, ac.signal);
    await waitFor(() => connections.includes("CONNECTED"));
    expect(gaps).toEqual([]);
    expect(adapter.ingressSnapshot().gapTotal).toBe(0);
    expect(adapter.tokenSyncState("tok-up")).toBe("AWAITING_BOOK");

    expect(transport.push({ type: "book", payload: { tokenId: "tok-up" } }).ok).toBe(true);
    transport.queue.flushObserver();
    expect(await metricValue(metrics, "market_ingress_high_water_total")).toBeGreaterThanOrEqual(1);
    ac.abort();
    await running;
    expect(await metricValue(metrics, "market_ingress_gap_total")).toBe(0);
  });

  it("reference_window_entries is the true total across assets", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([
      qualified("btc-mkt", "BTC", "btc-up", "btc-down"),
      qualified("eth-mkt", "ETH", "eth-up", "eth-down"),
    ]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    engine.attachReferenceWindowForTest("BTC", 8);
    engine.attachReferenceWindowForTest("ETH", 8);
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    await engine.ingestReferenceTickForTest(refTick("BTC", "100", t0));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0));
    expect(engine.referenceWindowTotalEntriesForTest()).toBe(2);
    expect(await metricValue(engine.metrics, "reference_window_entries")).toBe(2);
    const help = await engine.metrics.registry.getSingleMetric("reference_window_entries");
    const exposition = help ? await help.get() : { help: "" };
    expect(String((exposition as { help?: string }).help ?? "")).toMatch(/across all assets/i);
    await engine.stop();
  });
});

function liveBookEvent(tokenId: string, timestamp: number) {
  return {
    type: "book",
    payload: {
      tokenId, market: tokenId, timestamp,
      bids: [{ price: "0.47", size: "10" }],
      asks: [{ price: "0.48", size: "5" }],
      lastTradePrice: "0.48",
    },
  };
}

async function startLiveAdapter(tokenId = "tok-up") {
  const transport = new ScriptedBoundedMarketTransport();
  const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
  adapter.registerManualToken(tokenId, tokenId, "UP", "BTC");
  const ac = new AbortController();
  const gaps: string[] = [];
  const connections: string[] = [];
  const running = adapter.run([tokenId], {
    onSnapshot: () => undefined,
    onConnection: (event) => { connections.push(event.state); },
    onError: () => undefined,
    onGap: (event) => { gaps.push(event.reason); },
  }, ac.signal);
  await waitFor(() => connections.includes("CONNECTED"));
  return { adapter, transport, ac, running, gaps, connections };
}

describe("Step-3 follow-up 1 — unexpected market stream loss gaps before backoff", () => {
  it("A: synchronized books then unexpected stream end gaps before reconnect delay", async () => {
    const journal = new MemoryEventJournal();
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    const engine = new EngineService(qualifiedPaperEnv({ POLYMARKET_TOKEN_IDS: "tok-up" }), journal, { error: () => undefined }, {
      market: adapter,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await waitFor(() => transport.subscribeCalls >= 1 && adapter.tokenSyncState("tok-up") === "AWAITING_BOOK");
    expect(transport.push(liveBookEvent("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up") && engine.state.marketSnapshots.has("tok-up"));
    expect(mcpVisibleOrderbook(engine.state, "tok-up", (id) => engine.isTokenSynchronized(id))?.asks[0]?.price).toBe("0.48");

    const before = Date.now();
    transport.current().endRemote();
    await waitFor(() => adapter.tokenSyncState("tok-up") === "GAPPED");
    expect(Date.now() - before).toBeLessThan(400);
    expect(adapter.bookState("tok-up")).toBeUndefined();
    expect(engine.state.marketSnapshots.get("tok-up")).toBeUndefined();
    expect(mcpVisibleOrderbook(engine.state, "tok-up", (id) => engine.isTokenSynchronized(id))).toBeNull();
    await engine.stop();
  });

  it("B: malformed JSON gaps immediately with malformed reason before retry", async () => {
    const { adapter, transport, ac, running, gaps } = await startLiveAdapter();
    expect(transport.push(liveBookEvent("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    const before = Date.now();
    transport.current().fail(new Error("MARKET_INGRESS_MALFORMED"));
    await waitFor(() => adapter.tokenSyncState("tok-up") === "GAPPED");
    expect(Date.now() - before).toBeLessThan(400);
    expect(gaps).toEqual(["malformed"]);
    expect(adapter.bookState("tok-up")).toBeUndefined();
    ac.abort();
    await running;
  });

  it("C: stale WebSocket gaps immediately before retry", async () => {
    const { adapter, transport, ac, running, gaps } = await startLiveAdapter();
    expect(transport.push(liveBookEvent("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    const before = Date.now();
    transport.current().fail(new Error("CLOB_MARKET_WS_STALE"));
    await waitFor(() => adapter.tokenSyncState("tok-up") === "GAPPED");
    expect(Date.now() - before).toBeLessThan(400);
    expect(gaps).toEqual(["reconnect"]);
    ac.abort();
    await running;
  });

  it("D: one logical disconnect emits one gap, not two, and initial connect failure is not a fake gap", async () => {
    const { adapter, transport, ac, running, gaps } = await startLiveAdapter();
    expect(transport.push(liveBookEvent("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => adapter.tokenSyncState("tok-up") === "GAPPED");
    expect(gaps).toEqual(["reconnect"]);
    await waitFor(() => transport.subscribeCalls >= 2);
    expect(gaps).toEqual(["reconnect"]);
    ac.abort();
    await running;

    const second = await startLiveAdapter("tok-init");
    second.transport.current().fail(new Error("POLYMARKET_STREAM_ENDED"));
    await waitFor(() => second.connections.includes("DISCONNECTED"));
    expect(second.gaps).toEqual([]);
    expect(second.adapter.tokenSyncState("tok-init")).toBe("AWAITING_BOOK");
    second.ac.abort();
    await second.running;
  });
});

describe("Step-3 follow-up 2 — queued market triggers are epoch-fenced", () => {
  it("A: epoch N held through gap/resync does not journal after epoch N+1", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot(book("DOWN", "0.51"));
    try {
      engine.setProcessTriggerHoldForTest(hold);
      const stale = marketTrigger({ id: "epoch-n-stale" });
      expect(engine.enqueueTriggerStormForTest([stale]).admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      market.emitGap(["btc-up", "btc-down"]);
      market.markTokenSync("btc-up", "SYNCED");
      market.markTokenSync("btc-down", "SYNCED");
      await market.emitSnapshot(book("UP", "0.47", { sequence: 2, eventId: createId("market") }));
      await market.emitSnapshot(book("DOWN", "0.52", { sequence: 2, eventId: createId("market") }));
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION"] });
      expect(events.some((event) => event.payload.id === "epoch-n-stale" || event.payload.triggerId === "epoch-n-stale")).toBe(false);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });

  it("B: current-epoch trigger after resync does process", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot(book("DOWN", "0.51"));
    market.emitGap(["btc-up", "btc-down"]);
    market.markTokenSync("btc-up", "SYNCED");
    market.markTokenSync("btc-down", "SYNCED");
    await market.emitSnapshot(book("UP", "0.47", { sequence: 2, eventId: createId("market") }));
    await market.emitSnapshot(book("DOWN", "0.52", { sequence: 2, eventId: createId("market") }));
    const fresh = marketTrigger({ id: "epoch-n1-fresh" });
    engine.enqueueTriggerStormForTest([fresh]);
    await drainRuntime(engine);
    const events = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(events.some((event) => event.payload.id === "epoch-n1-fresh")).toBe(true);
    await engine.stop();
  });

  it("C: risk P0 survives the same epoch change", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    try {
      engine.setProcessTriggerHoldForTest(hold);
      const risk = riskTrigger({ id: "risk-epoch" });
      expect(engine.enqueueTriggerStormForTest([risk]).admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      market.emitGap(["btc-up", "btc-down"]);
      market.markTokenSync("btc-up", "SYNCED");
      market.markTokenSync("btc-down", "SYNCED");
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
      expect(events.some((event) => event.payload.id === "risk-epoch")).toBe(true);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });

  it("D: AWAITING_BOOK after reconnect cannot validate an old epoch trigger", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot(book("DOWN", "0.51"));
    try {
      engine.setProcessTriggerHoldForTest(hold);
      const stale = marketTrigger({ id: "awaiting-old" });
      expect(engine.enqueueTriggerStormForTest([stale]).admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      market.emitGap(["btc-up", "btc-down"]);
      market.markTokenSync("btc-up", "AWAITING_BOOK");
      market.markTokenSync("btc-down", "AWAITING_BOOK");
      expect(market.tokenSyncState("btc-up")).toBe("AWAITING_BOOK");
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION"] });
      expect(events.some((event) => event.payload.id === "awaiting-old" || event.payload.triggerId === "awaiting-old")).toBe(false);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });
});

describe("Step-3 follow-up 3 — reference overflow fail-closed before trusted publish", () => {
  it("overflow-causing tick is never trusted while telemetry persist is held", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    } as MarketSnapshot);
    engine.attachReferenceWindowForTest("ETH", 4);
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 4; i += 1) {
      await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + i * 100));
    }
    expect(engine.state.getReferenceContext("ETH", t0 + 400).latestPrice).toBe("2000");

    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    engine.setReferenceTelemetryHoldForTest(hold);
    const pending = engine.ingestReferenceTickForTest(refTick("ETH", "9999", t0 + 500));
    await Promise.resolve();
    expect(engine.referenceWindowOverflowedForTest("ETH")).toBe(true);
    expect(engine.referenceUnreliableForTest("ETH")).toBe(true);
    expect(engine.openFirstLegBlockedByUnreliableReferenceForTest("eth-mkt")).toBe(true);
    expect(engine.state.getReferenceContext("ETH", t0 + 500).latestPrice).not.toBe("9999");
    expect(engine.state.getReferenceContext("ETH", t0 + 500).latestPrice).toBeNull();
    engine.setReferenceTelemetryHoldForTest(null);
    release();
    await pending;
    expect(engine.state.getReferenceContext("ETH", t0 + 600).latestPrice).not.toBe("9999");
    await engine.stop();
  });
});

describe("Step-3 follow-up 4 — reference resync recovers from transient start failure", () => {
  it("A: overflow → transient start → later retry restores on first trusted tick", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    } as MarketSnapshot);
    engine.attachReferenceWindowForTest("ETH", 4);
    referenceRegistry.queueEnsureOutcome("ETH", "transient");
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 5; i += 1) {
      await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + i * 100));
    }
    expect(engine.referenceUnreliableForTest("ETH")).toBe(true);
    await waitFor(() => (referenceRegistry.ensureCalls.get("ETH") ?? 0) >= 2);
    referenceRegistry.queueEnsureOutcome("ETH", "started");
    await engine.flushReferenceRetryForTest("ETH");
    expect(engine.referenceResyncRequestedForTest("ETH")).toBe(true);
    await engine.applyReferenceConnectionForTest("ETH", "CONNECTED", 1);
    await engine.ingestReferenceTickForTest(refTick("ETH", "2100", t0 + 1_000));
    expect(engine.referenceUnreliableForTest("ETH")).toBe(false);
    expect(engine.state.getReferenceContext("ETH", t0 + 1_000).latestPrice).toBe("2100");
    await engine.stop();
  });

  it("B: overflow → ensureRunning throws → bounded retry remains possible", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    } as MarketSnapshot);
    engine.attachReferenceWindowForTest("ETH", 4);
    referenceRegistry.queueEnsureOutcome("ETH", "throw");
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 5; i += 1) {
      await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + i * 100));
    }
    expect(engine.referenceUnreliableForTest("ETH")).toBe(true);
    await waitFor(() => (referenceRegistry.ensureCalls.get("ETH") ?? 0) >= 2);
    referenceRegistry.queueEnsureOutcome("ETH", "started");
    await engine.flushReferenceRetryForTest("ETH");
    expect(engine.referenceResyncRequestedForTest("ETH")).toBe(true);
    await engine.stop();
  });

  it("C: multiple overflow/resync requests do not create parallel sessions or timers", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    } as MarketSnapshot);
    engine.attachReferenceWindowForTest("ETH", 2);
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 100));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 200));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 300));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 400));
    expect(referenceRegistry.maxConcurrentEnsure.value).toBe(1);
    expect(engine.referenceRetryTimerCountForTest()).toBeLessThanOrEqual(1);
    expect(referenceRegistry.stopCounts.get("ETH")).toBe(1);
    await engine.stop();
  });

  it("D: asset removed while retry pending does not restart", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([
      qualified("btc-mkt", "BTC", "btc-up", "btc-down"),
      qualified("eth-mkt", "ETH", "eth-up", "eth-down"),
    ]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot({
      ...book("UP", "0.48", { marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", eventId: createId("market") }),
      outcome: "UP",
    } as MarketSnapshot);
    engine.attachReferenceWindowForTest("ETH", 2);
    referenceRegistry.queueEnsureOutcome("ETH", "transient");
    const t0 = Date.parse("2026-08-31T12:00:00.000Z");
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 100));
    await engine.ingestReferenceTickForTest(refTick("ETH", "2000", t0 + 200));
    await waitFor(() => (referenceRegistry.ensureCalls.get("ETH") ?? 0) >= 2);
    const callsAfterOverflow = referenceRegistry.ensureCalls.get("ETH") ?? 0;
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    await engine.reconcileDiscovery();
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(referenceRegistry.ensureCalls.get("ETH")).toBe(callsAfterOverflow);
    await engine.stop();
  });
});

describe("Step-3 follow-up 5 — coalesced ingress metrics and O(1) dedupe count", () => {
  it("A: live ingress gauges eventually reflect enqueue/dequeue/high-water under coalescing", async () => {
    const metrics = new EngineMetrics();
    const queue = new BoundedIngressQueue(64, 100_000);
    queue.setObserver({ onChange: (snapshot) => metrics.noteIngressSnapshot(snapshot) });
    for (let i = 0; i < 20; i += 1) expect(queue.tryEnqueue(`e${i}`)).toEqual({ ok: true });
    expect(queue.dequeue()).toBe("e0");
    await new Promise((resolve) => setTimeout(resolve, INGRESS_OBSERVER_BOUNDS.coalesceMinIntervalMs + 5));
    expect(queue.tryEnqueue("late").ok).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, INGRESS_OBSERVER_BOUNDS.coalesceMinIntervalMs + 5));
    expect(await metricValue(metrics, "market_ingress_queue_depth")).toBe(queue.snapshot().depth);
    expect(await metricValue(metrics, "market_ingress_high_water_total")).toBe(queue.snapshot().highWaterEvents);
    queue.flushObserver();
    expect(await metricValue(metrics, "market_ingress_queue_depth")).toBe(queue.depth);
  });

  it("B: overflow metrics publish immediately", () => {
    let lastGap = 0;
    const queue = new BoundedIngressQueue(2, 64);
    queue.setObserver({ onChange: (snapshot) => { lastGap = snapshot.gapTotal; } });
    expect(queue.tryEnqueue("a").ok).toBe(true);
    expect(queue.tryEnqueue("b").ok).toBe(true);
    const before = queue.observerPublishCount;
    expect(queue.tryEnqueue("c").ok).toBe(false);
    expect(queue.observerPublishCount).toBeGreaterThan(before);
    expect(lastGap).toBe(1);
    expect(queue.snapshot().gapTotal).toBe(1);
  });

  it("C: observer invocations stay far below 100k queue mutations", () => {
    const queue = new BoundedIngressQueue(200_000, 10_000_000);
    let observerCalls = 0;
    queue.setObserver({ onChange: () => { observerCalls += 1; } });
    for (let i = 0; i < 50_000; i += 1) queue.tryEnqueue(i);
    for (let i = 0; i < 50_000; i += 1) queue.dequeue();
    expect(queue.accepted + queue.processed).toBe(100_000);
    expect(observerCalls).toBeLessThan(4_000);
    expect(queue.observerPublishCount).toBe(observerCalls);
  });

  it("D: incremental dedupe counter equals a map scan under churn/expiry/removal", () => {
    let now = Date.parse("2026-08-31T12:00:00.000Z");
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 0.01, spreadChangeThreshold: 0.0001, depthChangeThreshold: 0.01,
    }), () => new Date(now));
    for (let i = 0; i < 40; i += 1) {
      engine.ingestMarket({
        ...book("UP", "0.40", { marketId: `m${i}`, tokenId: `u${i}`, eventId: createId("market"), sequence: 1 }),
        outcome: "UP",
      } as MarketSnapshot);
    }
    expect(engine.dedupeEntryCount()).toBe(engine.dedupeEntryCountScanForTest());
    now += TRIGGER_STABILITY_BOUNDS.dedupeRetentionMs + 1;
    engine.ingestMarket({
      ...book("UP", "0.40", { marketId: "m-late", tokenId: "u-late", eventId: createId("market"), sequence: 1 }),
      outcome: "UP",
    } as MarketSnapshot);
    expect(engine.dedupeEntryCount()).toBe(engine.dedupeEntryCountScanForTest());
    engine.dropMarket("m-late");
    expect(engine.dedupeEntryCount()).toBe(engine.dedupeEntryCountScanForTest());
  });

  it("E: onMarketSnapshot does not scan all dedupe buckets", async () => {
    const { engine, market } = await startQualifiedEngine();
    await market.waitForHandlers();
    const before = engine.triggerDedupeScanCallsForTest();
    await market.emitSnapshot(book("UP", "0.48"));
    await market.emitSnapshot(book("DOWN", "0.51"));
    await market.emitSnapshot(book("UP", "0.30", { sequence: 2, eventId: createId("market") }));
    await drainRuntime(engine);
    expect(engine.triggerDedupeScanCallsForTest()).toBe(before);
    await engine.stop();
  });

  it("F: quiet queue auto-flushes coalesced depth to 0 without flushObserver", async () => {
    const metrics = new EngineMetrics();
    const queue = new BoundedIngressQueue(16, 10_000);
    queue.setObserver({ onChange: (snapshot) => metrics.noteIngressSnapshot(snapshot) });
    expect(queue.tryEnqueue("a").ok).toBe(true);
    expect(queue.tryEnqueue("b").ok).toBe(true);
    expect(queue.dequeue()).toBe("a");
    expect(queue.dequeue()).toBe("b");
    expect(queue.depth).toBe(0);
    await new Promise((resolve) => setTimeout(resolve, INGRESS_OBSERVER_BOUNDS.coalesceMinIntervalMs + 20));
    expect(await metricValue(metrics, "market_ingress_queue_depth")).toBe(0);
  });
});

describe("Step-3 review — partial-sync epoch fence stays attached", () => {
  it("A: UP SYNCED + DOWN AWAITING admits an UP-derived trigger with a non-null fence", async () => {
    const { engine, market } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.30"));
    expect(market.tokenSyncState("btc-up")).toBe("SYNCED");
    expect(market.tokenSyncState("btc-down")).toBe("AWAITING_BOOK");
    const admitted = engine.enqueueTriggerStormForTest([marketTrigger({ id: "partial-up" })]);
    expect(admitted.admitted).toBe(1);
    const fence = engine.lastAdmittedMarketEpochFenceForTest();
    expect(fence).not.toBeNull();
    expect(fence?.["btc-up"]).toBe(market.tokenEpoch("btc-up"));
    expect(fence?.["btc-down"]).toBeUndefined();
    await engine.stop();
  });

  it("B: held partial-sync trigger is stale after UP epoch advances and full resync", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.30"));
    try {
      engine.setProcessTriggerHoldForTest(hold);
      expect(engine.enqueueTriggerStormForTest([marketTrigger({ id: "partial-stale" })]).admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      expect(engine.lastAdmittedMarketEpochFenceForTest()).not.toBeNull();
      market.emitGap(["btc-up", "btc-down"]);
      market.markTokenSync("btc-up", "SYNCED");
      market.markTokenSync("btc-down", "SYNCED");
      await market.emitSnapshot(book("UP", "0.31", { sequence: 2, eventId: createId("market") }));
      await market.emitSnapshot(book("DOWN", "0.52", { sequence: 2, eventId: createId("market") }));
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION"] });
      expect(events.some((event) => event.payload.id === "partial-stale" || event.payload.triggerId === "partial-stale")).toBe(false);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });

  it("C: same partial-sync trigger with no epoch change remains actionable", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.30"));
    try {
      engine.setProcessTriggerHoldForTest(hold);
      expect(engine.enqueueTriggerStormForTest([marketTrigger({ id: "partial-live" })]).admitted).toBe(1);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      engine.setProcessTriggerHoldForTest(null);
      release();
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
      expect(events.some((event) => event.payload.id === "partial-live")).toBe(true);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
      await engine.stop();
    }
  });

  it("D: PAIR_COST still requires a current trusted pair", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(book("UP", "0.47"));
    await drainRuntime(engine);
    const afterUp = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterUp.some((event) => event.payload.kind === "PAIR_COST_OPPORTUNITY")).toBe(false);
    market.markTokenSync("btc-down", "SYNCED");
    await market.emitSnapshot(book("DOWN", "0.47"));
    await drainRuntime(engine);
    const afterPair = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    expect(afterPair.some((event) => event.payload.kind === "PAIR_COST_OPPORTUNITY")).toBe(true);
    await engine.stop();
  });
});

class SilentWebSocket extends EventTarget {
  static created = 0;
  readyState = 0;
  constructor(readonly url: string) {
    super();
    SilentWebSocket.created += 1;
  }
  send(): void { /* never opens */ }
  close(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}

class ImmediateCloseWebSocket extends EventTarget {
  readyState = 0;
  constructor(readonly url: string) {
    super();
    queueMicrotask(() => {
      this.readyState = 3;
      this.dispatchEvent(new Event("close"));
    });
  }
  send(): void { /* closed before open */ }
  close(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}

describe("Step-3 review — owned market connect is finitely bounded", () => {
  it("A: WebSocket never opens -> bounded connect timeout -> adapter retries with no hung promise", async () => {
    SilentWebSocket.created = 0;
    const timed = new BoundedClobMarketTransport({
      webSocket: SilentWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 40,
    });
    await expect(timed.subscribe(["tok-up"])).rejects.toThrow(CLOB_MARKET_CONNECT_TIMEOUT);
    SilentWebSocket.created = 0;
    const transport = new BoundedClobMarketTransport({
      webSocket: SilentWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 40,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const gaps: string[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
      onGap: (event) => { gaps.push(event.reason); },
    }, ac.signal);
    await waitFor(() => SilentWebSocket.created >= 2, 3_000);
    expect(gaps).toEqual([]);
    ac.abort();
    await running;
    expect(gaps).toEqual([]);
  });

  it("B: close before open rejects subscribe and retry proceeds", async () => {
    const transport = new BoundedClobMarketTransport({
      webSocket: ImmediateCloseWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 500,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const connections: string[] = [];
    const gaps: string[] = [];
    const pending = transport.subscribe(["tok-up"], true);
    await expect(pending).rejects.toThrow(CLOB_MARKET_CLOSED_BEFORE_OPEN);
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
      onGap: (event) => { gaps.push(event.reason); },
    }, ac.signal);
    await waitFor(() => connections.filter((state) => state === "DISCONNECTED").length >= 1);
    expect(gaps).toEqual([]);
    ac.abort();
    await running;
  });

  it("C: abort during CONNECTING settles the connect promise promptly", async () => {
    const direct = new BoundedClobMarketTransport({
      webSocket: SilentWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 5_000,
    });
    const connectAbort = new AbortController();
    const pending = direct.subscribe(["tok-up"], true, connectAbort.signal);
    await Promise.resolve();
    const before = Date.now();
    connectAbort.abort();
    await expect(pending).rejects.toThrow(CLOB_MARKET_CONNECT_ABORTED);
    expect(Date.now() - before).toBeLessThan(200);

    const transport = new ScriptedBoundedMarketTransport();
    transport.holdNextSubscribe();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const connections: string[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1 && connections.includes("CONNECTING"));
    const started = Date.now();
    ac.abort();
    await running;
    expect(Date.now() - started).toBeLessThan(200);
  });

  it("D-F: trusted loss is one gap; failed reconnects add none; later trusted loss adds one", async () => {
    const { adapter, transport, ac, running, gaps } = await startLiveAdapter();
    expect(transport.push(liveBookEvent("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => gaps.length === 1);
    expect(gaps).toEqual(["reconnect"]);
    transport.failNextSubscribeCount = 3;
    await waitFor(() => transport.subscribeCalls >= 5, 15_000);
    expect(gaps).toEqual(["reconnect"]);
    expect(transport.push(liveBookEvent("tok-up", 2_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => gaps.length === 2);
    expect(gaps).toEqual(["reconnect", "reconnect"]);
    ac.abort();
    await running;
  }, 20_000);
});
