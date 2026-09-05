import { describe, expect, it } from "vitest";
import {
  createId,
  isMarketDataDerivedTrigger,
  MARKET_DATA_TRIGGER_KINDS,
  NON_SUPPRESSIBLE_ON_MARKET_GAP_KINDS,
  TRIGGER_STABILITY_BOUNDS,
  type ReferenceTick,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  BoundedIngressQueue,
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
    queue.setObserver((snapshot) => {
      seen.push({
        depth: snapshot.depth,
        bytes: snapshot.bytes,
        highWaterEvents: snapshot.highWaterEvents,
        gapTotal: snapshot.gapTotal,
      });
    });
    expect(queue.tryEnqueue("aa").ok).toBe(true);
    expect(queue.tryEnqueue("bbbb").ok).toBe(true);
    expect(seen.at(-1)?.depth).toBe(2);
    expect(seen.at(-1)?.bytes).toBe(6);
    expect(seen.at(-1)?.highWaterEvents).toBe(2);
    expect(seen.at(-1)?.gapTotal).toBe(0);
    expect(queue.dequeue()).toBe("aa");
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
    expect(await metricValue(metrics, "market_ingress_high_water_total")).toBeGreaterThanOrEqual(1);
    ac.abort();
    await running;
    expect(await metricValue(metrics, "market_ingress_gap_total")).toBe(0);
  });

  it("reference_window_entries is the true total across assets", async () => {
    const { engine } = await startQualifiedEngine();
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
