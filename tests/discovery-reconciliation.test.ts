import { describe, expect, it } from "vitest";
import { createId, engineEnvSchema, grokSignalSubmissionSchema, type MarketSnapshot, type ReferenceTick, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { parseBinanceAggTrade, type DiscoveryReport, type DiscoveredMarket, type PolymarketFeedHandlers, type ReferenceFeedHandlers, type ReferenceResolution } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { feeSchedule, snapshot } from "./fixtures.js";

const future = () => new Date(Date.now() + 60_000).toISOString();

function qualified(id: string, asset: string, up: string, down: string): DiscoveredMarket {
  return {
    id, slug: `${asset.toLowerCase()}-updown-15m`, question: `${asset} Up or Down`, title: `${asset} Up or Down`,
    asset, endDate: new Date(Date.now() + 15 * 60_000).toISOString(), active: true,
    tokens: [{ tokenId: up, outcome: "UP", price: "0.48" }, { tokenId: down, outcome: "DOWN", price: "0.51" }],
    feeSchedule, liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null },
    raw: {},
  };
}

function report(markets: DiscoveredMarket[]): DiscoveryReport {
  return { discovered: markets, qualified: markets, rejected: [] };
}

function tick(asset: string, price: string, tradeId = 1): ReferenceTick {
  return parseBinanceAggTrade(JSON.stringify({ s: `${asset}USDT`, p: price, q: "1", T: Date.now(), a: tradeId }), `${asset}USDT`, asset);
}

function researchTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {},
    sourceEventIds: ["e1"], evidenceIds: ["e1"], createdAt: new Date().toISOString(), expiresAt: future(), ...overrides,
  };
}

class ScriptedMarketAdapter {
  readonly reports: DiscoveryReport[] = [];
  tokenIds: string[] = [];
  handlers: PolymarketFeedHandlers | null = null;
  readonly setTokenCalls: string[][] = [];
  closeCount = 0;
  private tokenWaiters: Array<() => void> = [];
  private streamClosed: (() => void) | null = null;

  queue(next: DiscoveryReport): void { this.reports.push(next); }
  registerManualToken(): void { /* discovery tests do not use manual tokens */ }
  async discoverCryptoMarkets(): Promise<DiscoveryReport> {
    return this.reports.shift() ?? { discovered: [], qualified: [], rejected: [] };
  }

  setTokenIds(tokenIds: readonly string[]): { changed: boolean; previous: string[]; next: string[] } {
    const previous = [...this.tokenIds];
    const next = [...new Set(tokenIds.filter(Boolean))];
    const changed = previous.length !== next.length || previous.some((id) => !next.includes(id)) || next.some((id) => !previous.includes(id));
    this.tokenIds = next;
    this.setTokenCalls.push([...next]);
    if (next.length > 0) {
      const waiters = this.tokenWaiters.splice(0);
      for (const waiter of waiters) waiter();
    }
    if (changed) {
      this.closeCount += 1;
      this.streamClosed?.();
    }
    return { changed, previous, next };
  }

  async run(_tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    this.handlers = handlers;
    while (!signal.aborted) {
      if (this.tokenIds.length === 0) {
        await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount: 0, message: "NO_QUALIFIED_TOKENS" });
        try {
          await new Promise<void>((resolve, reject) => {
            const finish = () => { cleanup(); resolve(); };
            const onAbort = () => { cleanup(); reject(signal.reason); };
            const cleanup = () => {
              signal.removeEventListener("abort", onAbort);
              this.tokenWaiters = this.tokenWaiters.filter((waiter) => waiter !== finish);
            };
            if (this.tokenIds.length > 0) { resolve(); return; }
            signal.addEventListener("abort", onAbort, { once: true });
            this.tokenWaiters.push(finish);
          });
        } catch { break; }
        continue;
      }
      await handlers.onConnection({ feed: "polymarket", state: "CONNECTING", at: new Date().toISOString(), reconnectCount: 0 });
      await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount: 0 });
      await new Promise<void>((resolve) => {
        this.streamClosed = () => resolve();
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
      this.streamClosed = null;
    }
  }

  async emitSnapshot(next: MarketSnapshot): Promise<void> {
    await this.handlers?.onSnapshot(next);
  }
}

class ScriptedReferenceRegistry {
  readonly startCounts = new Map<string, number>();
  readonly stopCounts = new Map<string, number>();
  readonly failNext = new Set<string>();
  private readonly running = new Map<string, ReferenceFeedHandlers>();

  async resolve(asset: string): Promise<ReferenceResolution> {
    return { asset, supported: true, symbol: `${asset}USDT`, url: `wss://example/${asset.toLowerCase()}`, reason: "RESOLVED" };
  }
  adapterFor(): null { return null; }
  async ensureRunning(asset: string, handlers: ReferenceFeedHandlers, _signal: AbortSignal): Promise<"started" | "already" | "unsupported"> {
    if (this.running.has(asset)) return "already";
    if (this.failNext.has(asset)) {
      this.failNext.delete(asset);
      throw new Error("FEED_RESTART_ERROR");
    }
    this.running.set(asset, handlers);
    this.startCounts.set(asset, (this.startCounts.get(asset) ?? 0) + 1);
    await handlers.onConnection({ feed: "reference", asset, state: "CONNECTED", at: new Date().toISOString(), reconnectCount: 0 });
    return "started";
  }
  stop(asset: string): boolean {
    if (!this.running.has(asset)) return false;
    this.running.delete(asset);
    this.stopCounts.set(asset, (this.stopCounts.get(asset) ?? 0) + 1);
    return true;
  }
  runningAssets(): string[] { return [...this.running.keys()]; }
  async emitTick(next: ReferenceTick): Promise<void> {
    await this.running.get(next.asset)?.onTick(next);
  }
}

async function startDiscoveringEngine(seed: DiscoveryReport = report([])) {
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const references = new ScriptedReferenceRegistry();
  market.queue(seed);
  const env = engineEnvSchema.parse({
    NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
    DISCOVERY_INTERVAL_MS: "3600000",
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never, referenceRegistry: references as never,
  });
  await engine.start();
  await flush();
  return { engine, journal, market, references };
}

function flush(): Promise<void> { return new Promise((resolve) => setTimeout(resolve, 20)); }

describe("zero qualified markets keep the engine alive", () => {
  it("startup succeeds with zero qualified markets, stays ready, and never fabricates a paper order", async () => {
    const { engine, journal, market, references } = await startDiscoveringEngine();
    try {
      expect(engine.state.ready).toBe(true);
      expect(engine.state.engineAlive()).toBe(true);
      expect(engine.state.discoveryStatus).toBe("ZERO_QUALIFIED");
      expect(engine.state.discoveryLoopActive).toBe(true);
      expect(engine.state.marketMonitoring).toBe(false);
      expect(engine.state.qualifiedMarketCount).toBe(0);
      expect(engine.subscribedTokenIds()).toEqual([]);
      expect(engine.runningReferenceAssets()).toEqual([]);
      expect(market.tokenIds).toEqual([]);
      expect(references.runningAssets()).toEqual([]);
      expect(engine.ledger.positions.size).toBe(0);
      expect(engine.state.feed.some((item) => item.type === "PAPER_ORDER_CREATED")).toBe(false);
      const snap = engine.state.snapshot();
      expect(snap.engine.alive).toBe(true);
      expect(snap.engine.discoveryStatus).toBe("ZERO_QUALIFIED");
      expect(snap.engine.marketMonitoring).toBe(false);
      expect(snap.health.polymarketWs.state).toBe("STOPPED");
      expect((await journal.readAll()).some((event) => event.type === "DISCOVERY_RECONCILED")).toBe(true);

      const server = await createApiServer({
        state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
        dashboardOrigin: "http://localhost:3000", logger: false,
        mcp: { bearerTokens: "fair_probability_analyst:test-token", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
        submitGrokSignal: async () => ({ status: "ignored" }),
      });
      const ready = await server.inject({ method: "GET", url: "/ready" });
      const health = await server.inject({ method: "GET", url: "/health" });
      expect(ready.statusCode).toBe(200);
      expect(ready.json()).toMatchObject({
        status: "ready", mode: "paper", engineAlive: true, discoveryStatus: "ZERO_QUALIFIED",
        marketMonitoring: false, qualifiedMarketCount: 0, polymarketWs: "STOPPED",
      });
      expect(health.statusCode).toBe(200);
      await server.close();
    } finally {
      await engine.stop();
    }
  });

  it("later discovery of a qualifying market starts monitoring automatically", async () => {
    const { engine, market, references } = await startDiscoveringEngine();
    try {
      const eth = qualified("eth-mkt", "ETH", "eth-up", "eth-down");
      market.queue(report([eth]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.subscribedTokenIds()).toEqual(["eth-up", "eth-down"]);
      expect(engine.state.discoveryStatus).toBe("MONITORING");
      expect(engine.state.qualifiedMarketCount).toBe(1);
      expect(engine.state.marketMonitoring).toBe(true);
      expect(engine.runningReferenceAssets()).toEqual(["ETH"]);
      expect(references.startCounts.get("ETH")).toBe(1);
      expect(market.closeCount).toBeGreaterThan(0);
      await market.emitSnapshot(snapshot({ marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH" }));
      expect(engine.state.marketSnapshots.has("eth-up")).toBe(true);
      expect(engine.ledger.positions.size).toBe(0);
    } finally {
      await engine.stop();
    }
  });
});

describe("dynamic discovery reconciles live subscriptions", () => {
  it("A) 0 markets → ETH market appears", async () => {
    const { engine, market, references, journal } = await startDiscoveringEngine();
    try {
      market.queue(report([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.subscribedTokenIds()).toEqual(["eth-up", "eth-down"]);
      expect(engine.runningReferenceAssets()).toEqual(["ETH"]);
      const events = await journal.readAll();
      expect(events.some((event) => event.type === "DISCOVERY_RECONCILED" && JSON.stringify(event.payload).includes("eth-mkt"))).toBe(true);
      expect(events.some((event) => event.type === "REFERENCE_FEED_STARTED" && (event.payload as { asset?: string }).asset === "ETH")).toBe(true);
      expect(references.startCounts.get("ETH")).toBe(1);
    } finally {
      await engine.stop();
    }
  });

  it("B) BTC → BTC + SOL", async () => {
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const { engine, market, references } = await startDiscoveringEngine(report([btc]));
    try {
      await flush();
      expect(engine.runningReferenceAssets()).toEqual(["BTC"]);
      const tokensBefore = market.setTokenCalls.length;
      market.queue(report([btc, qualified("sol-mkt", "SOL", "sol-up", "sol-down")]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.subscribedTokenIds()).toEqual(["btc-up", "btc-down", "sol-up", "sol-down"]);
      expect(engine.runningReferenceAssets().sort()).toEqual(["BTC", "SOL"]);
      expect(references.startCounts.get("BTC")).toBe(1);
      expect(references.startCounts.get("SOL")).toBe(1);
      expect(market.setTokenCalls.length).toBeGreaterThan(tokensBefore);
    } finally {
      await engine.stop();
    }
  });

  it("C) BTC + SOL → SOL only", async () => {
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const sol = qualified("sol-mkt", "SOL", "sol-up", "sol-down");
    const { engine, market, references } = await startDiscoveringEngine(report([btc, sol]));
    try {
      market.queue(report([sol]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.subscribedTokenIds()).toEqual(["sol-up", "sol-down"]);
      expect(engine.runningReferenceAssets()).toEqual(["SOL"]);
      expect(references.stopCounts.get("BTC")).toBe(1);
      expect(engine.state.snapshot().health.referenceFeeds.some((feed) => feed.asset === "BTC")).toBe(false);
    } finally {
      await engine.stop();
    }
  });

  it("D) two BTC markets still use one BTC reference feed", async () => {
    const first = qualified("btc-a", "BTC", "btc-a-up", "btc-a-down");
    const second = qualified("btc-b", "BTC", "btc-b-up", "btc-b-down");
    const { engine, market, references } = await startDiscoveringEngine(report([first]));
    try {
      market.queue(report([first, second]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.subscribedTokenIds()).toEqual(["btc-a-up", "btc-a-down", "btc-b-up", "btc-b-down"]);
      expect(engine.runningReferenceAssets()).toEqual(["BTC"]);
      expect(references.startCounts.get("BTC")).toBe(1);
      market.queue(report([first, second]));
      await engine.reconcileDiscovery();
      expect(engine.runningReferenceAssets()).toEqual(["BTC"]);
      expect(references.startCounts.get("BTC")).toBe(1);
    } finally {
      await engine.stop();
    }
  });

  it("E) a newly discovered asset actually receives reference ticks", async () => {
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const { engine, market, references } = await startDiscoveringEngine(report([btc]));
    try {
      market.queue(report([btc, qualified("sol-mkt", "SOL", "sol-up", "sol-down")]));
      await engine.reconcileDiscovery();
      await flush();
      await references.emitTick(tick("SOL", "140", 11));
      expect(engine.state.referenceLastMessageAt("SOL")).toBeTruthy();
      expect(engine.state.snapshot().health.referenceFeeds.some((feed) => feed.asset === "SOL" && feed.lastMessageAt)).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("F) a removed market stops receiving and processing snapshots", async () => {
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const sol = qualified("sol-mkt", "SOL", "sol-up", "sol-down");
    const { engine, market, references } = await startDiscoveringEngine(report([btc, sol]));
    try {
      await market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC" }));
      expect(engine.state.marketSnapshots.has("btc-up")).toBe(true);
      market.queue(report([sol]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.state.marketSnapshots.has("btc-up")).toBe(false);
      await market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC", eventId: createId("late") }));
      expect(engine.state.marketSnapshots.has("btc-up")).toBe(false);
      await references.emitTick(tick("BTC", "65000", 99));
      expect(engine.state.referenceLastMessageAt("BTC")).toBeNull();
      await market.emitSnapshot(snapshot({ marketId: "sol-mkt", tokenId: "sol-up", asset: "SOL" }));
      expect(engine.state.marketSnapshots.has("sol-up")).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("one feed restart failure does not crash the engine or duplicate loops", async () => {
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const { engine, market, references } = await startDiscoveringEngine(report([btc]));
    try {
      references.failNext.add("SOL");
      market.queue(report([btc, qualified("sol-mkt", "SOL", "sol-up", "sol-down")]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.state.ready).toBe(true);
      expect(engine.runningReferenceAssets()).toEqual(["BTC"]);
      market.queue(report([btc, qualified("sol-mkt", "SOL", "sol-up", "sol-down")]));
      await engine.reconcileDiscovery();
      await flush();
      expect(engine.runningReferenceAssets().sort()).toEqual(["BTC", "SOL"]);
      expect(references.startCounts.get("SOL")).toBe(1);
      expect(references.startCounts.get("BTC")).toBe(1);
    } finally {
      await engine.stop();
    }
  });
});

describe("per-trigger MCP latency attribution", () => {
  it("an MCP request with triggerId marks only that trigger; two pending triggers stay isolated", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down", GROK_ACTIVATION_PROVIDER: "noop" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    const first = researchTrigger({ marketId: "btc-mkt", asset: "BTC" });
    const second = researchTrigger({ marketId: "eth-mkt", asset: "ETH" });
    await engine.registerTriggerForResearch(first);
    await engine.registerTriggerForResearch(second);

    engine.noteMcpRequest("get_recent_triggers");
    const afterPoll = engine.activation.snapshot();
    expect(afterPoll.find((item) => item.triggerId === first.id)?.firstMcpRequestAt).toBeNull();
    expect(afterPoll.find((item) => item.triggerId === second.id)?.firstMcpRequestAt).toBeNull();
    expect(engine.activation.unattributedSnapshot().count).toBe(1);

    engine.noteMcpRequest("get_recent_triggers", first.id);
    const afterFirst = engine.activation.snapshot();
    expect(afterFirst.find((item) => item.triggerId === first.id)?.firstMcpRequestAt).toBeTruthy();
    expect(afterFirst.find((item) => item.triggerId === second.id)?.firstMcpRequestAt).toBeNull();

    engine.noteMcpRequest("submit_analysis", second.id);
    const afterSecond = engine.activation.snapshot();
    expect(afterSecond.find((item) => item.triggerId === first.id)?.firstMcpRequestAt).toBeTruthy();
    expect(afterSecond.find((item) => item.triggerId === second.id)?.firstMcpRequestAt).toBeTruthy();
    expect(afterSecond.find((item) => item.triggerId === first.id)?.firstAcceptedSignalAt).toBeNull();
    expect(afterSecond.find((item) => item.triggerId === second.id)?.firstAcceptedSignalAt).toBeNull();
  });

  it("accepted signal still records firstAcceptedSignalAt for that trigger only", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down", GROK_ACTIVATION_PROVIDER: "noop" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    const first = researchTrigger({ marketId: "btc-mkt", asset: "BTC" });
    const second = researchTrigger({ marketId: "eth-mkt", asset: "ETH" });
    await engine.registerTriggerForResearch(first);
    await engine.registerTriggerForResearch(second);
    engine.noteMcpRequest("submit_analysis", first.id);
    const result = await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("sub"), triggerId: first.id, traceId: first.traceId, correlationId: createId("c"),
      marketId: "btc-mkt", asset: "BTC", timestamp: new Date().toISOString(),
      agentId: "fair_probability_analyst", role: "fair_probability_analyst",
      output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: future(), reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
    }), "fair_probability_analyst");
    expect(result.status).toBe("accepted");
    const latency = engine.activation.snapshot();
    expect(latency.find((item) => item.triggerId === first.id)?.firstAcceptedSignalAt).toBeTruthy();
    expect(latency.find((item) => item.triggerId === second.id)?.firstAcceptedSignalAt).toBeNull();
    expect(engine.state.snapshot().activationLatency.find((item) => item.triggerId === first.id)?.firstAcceptedSignalAt).toBeTruthy();
  });

  it("generic MCP HTTP get_recent_triggers does not stamp pending triggers", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      GROK_ACTIVATION_PROVIDER: "noop", GROK_MCP_BEARER_TOKENS: "fair_probability_analyst:fair-secret",
    });
    const engine = new EngineService(env, journal, { error: () => undefined });
    const event = researchTrigger();
    await engine.registerTriggerForResearch(event);
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "fair_probability_analyst:fair-secret", allowedOrigins: "https://grok.com,https://x.ai", rateLimitPerMinute: 30 },
      submitGrokSignal: (signal, clientId) => engine.submitGrokSignal(signal, clientId),
      noteMcpRequest: (tool, triggerId) => engine.noteMcpRequest(tool, triggerId),
    });
    await server.listen({ host: "127.0.0.1", port: 0 });
    try {
      const address = server.server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      const headers = {
        authorization: "Bearer fair-secret",
        accept: "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": "2025-11-25",
      };
      await fetch(`http://127.0.0.1:${port}/mcp`, {
        method: "POST", headers,
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "get_recent_triggers", arguments: { limit: 10 } } }),
      });
      expect(engine.activation.snapshot().find((item) => item.triggerId === event.id)?.firstMcpRequestAt).toBeNull();
      expect(engine.activation.unattributedSnapshot().lastTool).toBe("get_recent_triggers");
      await fetch(`http://127.0.0.1:${port}/mcp`, {
        method: "POST", headers,
        body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "get_recent_triggers", arguments: { limit: 10, triggerId: event.id } } }),
      });
      expect(engine.activation.snapshot().find((item) => item.triggerId === event.id)?.firstMcpRequestAt).toBeTruthy();
    } finally {
      await server.close();
    }
  });
});
