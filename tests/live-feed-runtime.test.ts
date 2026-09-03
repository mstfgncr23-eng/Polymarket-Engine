import { afterEach, describe, expect, it, vi } from "vitest";
import { PaperLedger, engineEnvSchema, evaluateRisk, riskLimitsSchema } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  PolymarketMarketDataAdapter,
  REFERENCE_STABLE_CONNECTION_MS,
  ReferenceFeedRegistry,
  ReferencePriceFeedAdapter,
  classifyBinanceExchangeInfo,
  classifyReferenceClose,
  type FeedConnectionEvent,
  type PolymarketFeedAnomaly,
  type ReferenceFeedHandlers,
  type WebSocketLike,
} from "@pm/market-data";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { safeRisk } from "./fixtures.js";

async function waitFor(predicate: () => boolean, timeoutMs = 1_500): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

class ControllableMarketStream {
  private queue: unknown[] = [];
  private waiters: Array<(result: IteratorResult<unknown>) => void> = [];
  closed = false;
  closeCount = 0;

  push(event: unknown): void {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: event, done: false });
    else this.queue.push(event);
  }

  async close(): Promise<void> {
    this.closeCount += 1;
    this.closed = true;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: undefined, done: true });
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<unknown> {
    while (true) {
      if (this.queue.length > 0) {
        yield this.queue.shift();
        continue;
      }
      if (this.closed) return;
      const result = await new Promise<IteratorResult<unknown>>((resolve) => this.waiters.push(resolve));
      if (result.done) return;
      yield result.value;
    }
  }
}

class FakePolymarketClient {
  readonly streams: ControllableMarketStream[] = [];
  subscribeCalls = 0;
  closeSubscriptionsCalls = 0;
  concurrent = 0;
  maxConcurrent = 0;

  async subscribe(): Promise<ControllableMarketStream> {
    this.subscribeCalls += 1;
    this.concurrent += 1;
    this.maxConcurrent = Math.max(this.maxConcurrent, this.concurrent);
    const stream = new ControllableMarketStream();
    const original = stream.close.bind(stream);
    stream.close = async () => {
      if (!stream.closed) this.concurrent = Math.max(0, this.concurrent - 1);
      await original();
    };
    this.streams.push(stream);
    return stream;
  }

  async closeSubscriptions(): Promise<void> { this.closeSubscriptionsCalls += 1; }
  current(): ControllableMarketStream { return this.streams.at(-1)!; }
}

function bookEvent(tokenId: string, timestamp: number | string | null, bidPrice: string) {
  return {
    type: "book",
    payload: {
      tokenId, market: "mkt", timestamp,
      bids: [{ price: bidPrice, size: "10" }],
      asks: [{ price: "0.60", size: "5" }],
    },
  };
}

function priceChangeEvent(tokenId: string, timestamp: number, price: string, size: string) {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId, side: "BUY", price, size }],
    },
  };
}

function dispatchSocketClose(target: EventTarget, code: number, reason: string, wasClean: boolean): void {
  const event = new Event("close") as Event & { code: number; reason: string; wasClean: boolean };
  Object.assign(event, { code, reason, wasClean });
  target.dispatchEvent(event);
}

class ScriptedSocket extends EventTarget implements WebSocketLike {
  readyState = 0;
  closeCalls: Array<{ code?: number; reason?: string }> = [];
  close(code?: number, reason?: string): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.closeCalls.push({ code, reason });
    dispatchSocketClose(this, code ?? 1005, reason ?? "", code === 1000);
  }
  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  remoteClose(code: number, reason: string, wasClean: boolean): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    dispatchSocketClose(this, code, reason, wasClean);
  }
}

class DelayedCloseSocket extends EventTarget implements WebSocketLike {
  readyState = 0;
  private pending: { code?: number; reason?: string } | null = null;
  constructor(private readonly onClosed?: () => void) { super(); }
  close(code?: number, reason?: string): void {
    if (this.readyState === 3) return;
    this.readyState = 2;
    this.pending = { code, reason };
  }
  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  error(): void { this.dispatchEvent(new Event("error")); }
  completeClose(): void {
    if (this.readyState === 3) return;
    const pending = this.pending ?? { code: 1006, reason: "" };
    this.readyState = 3;
    this.pending = null;
    this.onClosed?.();
    dispatchSocketClose(this, pending.code ?? 1006, pending.reason ?? "", pending.code === 1000);
  }
}

function silentHandlers(extra: Partial<ReferenceFeedHandlers> = {}): ReferenceFeedHandlers {
  return { onTick: () => undefined, onConnection: () => undefined, onError: () => undefined, ...extra };
}

describe("A 1ms stale Polymarket event does not tear down the stream", () => {
  it("accepts 1000, drops 999, accepts 1001 without reconnect", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt", "UP", "BTC");
    const snapshots: Array<{ timestamp: string; bid: string | undefined }> = [];
    const connections: FeedConnectionEvent[] = [];
    const errors: string[] = [];
    const anomalies: PolymarketFeedAnomaly[] = [];
    const abort = new AbortController();
    const running = adapter.run(["up"], {
      onSnapshot: (snapshot) => {
        snapshots.push({ timestamp: snapshot.sourceTimestamp, bid: snapshot.bids[0]?.price });
      },
      onConnection: (event) => { connections.push(event); },
      onError: (error) => { errors.push(error.message); },
      onAnomaly: (anomaly) => { anomalies.push(anomaly); },
    }, abort.signal);
    await waitFor(() => client.streams.length === 1 && connections.some((event) => event.state === "CONNECTED"));
    client.current().push(bookEvent("up", 1000, "0.40"));
    await waitFor(() => snapshots.length === 1);
    client.current().push(bookEvent("up", 999, "0.99"));
    await waitFor(() => anomalies.length === 1);
    client.current().push(bookEvent("up", 1001, "0.41"));
    await waitFor(() => snapshots.length === 2);

    expect(errors).toEqual([]);
    expect(connections.filter((event) => event.state === "DISCONNECTED")).toHaveLength(0);
    expect(client.current().closeCount).toBe(0);
    expect(client.subscribeCalls).toBe(1);
    expect(adapter.activeStreamCount()).toBe(1);
    expect(adapter.lastAcceptedTimestamp("up")).toBe(1001);
    expect(snapshots.map((item) => item.bid)).toEqual(["0.40", "0.41"]);
    expect(anomalies[0]).toMatchObject({ kind: "stale", tokenId: "up", incomingMs: 999, lastAcceptedMs: 1000, regressionMs: 1 });
    abort.abort("test");
    await running;
  });
});

describe("B stale event does not mutate the book", () => {
  it("ignores a stale full book and a stale price_change", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt");
    const abort = new AbortController();
    const running = adapter.run(["up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => { throw new Error("unexpected feed error"); },
    }, abort.signal);
    await waitFor(() => client.streams.length === 1);
    client.current().push(bookEvent("up", 1000, "0.40"));
    await waitFor(() => adapter.bookState("up")?.bids.get("0.40") === "10");
    client.current().push(bookEvent("up", 999, "0.11"));
    client.current().push(priceChangeEvent("up", 998, "0.40", "99"));
    await waitFor(() => adapter.timestampStats().staleDropped >= 2);
    const book = adapter.bookState("up");
    expect(book?.bids.get("0.40")).toBe("10");
    expect(book?.bids.get("0.11")).toBeUndefined();
    expect(adapter.lastAcceptedTimestamp("up")).toBe(1000);
    abort.abort("test");
    await running;
  });
});

describe("C stale then newer valid event updates state", () => {
  it("applies a later price_change after dropping a 1ms stale book", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt");
    const bids: string[] = [];
    const abort = new AbortController();
    const running = adapter.run(["up"], {
      onSnapshot: (snapshot) => { bids.push(snapshot.bids[0]?.price ?? ""); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => client.streams.length === 1);
    client.current().push(bookEvent("up", 1000, "0.40"));
    await waitFor(() => bids.length === 1);
    client.current().push(bookEvent("up", 999, "0.12"));
    client.current().push(priceChangeEvent("up", 1001, "0.42", "8"));
    await waitFor(() => bids.length === 2);
    expect(bids).toEqual(["0.40", "0.42"]);
    expect(adapter.bookState("up")?.bids.get("0.42")).toBe("8");
    expect(adapter.bookState("up")?.bids.get("0.12")).toBeUndefined();
    abort.abort("test");
    await running;
  });
});

describe("D stale-event diagnostic is bounded", () => {
  it("counts every drop but samples only a bounded number of diagnostics", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt");
    const sampled: PolymarketFeedAnomaly[] = [];
    const abort = new AbortController();
    const running = adapter.run(["up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
      onAnomaly: (anomaly) => { if (anomaly.sampled) sampled.push(anomaly); },
    }, abort.signal);
    await waitFor(() => client.streams.length === 1);
    client.current().push(bookEvent("up", 5_000, "0.40"));
    await waitFor(() => adapter.lastAcceptedTimestamp("up") === 5_000);
    for (let i = 0; i < 1_000; i += 1) client.current().push(bookEvent("up", 4_999, "0.01"));
    await waitFor(() => adapter.timestampStats().staleDropped === 1_000);
    const stats = adapter.timestampStats();
    expect(stats.staleDropped).toBe(1_000);
    expect(stats.diagnosticsEmitted).toBeLessThanOrEqual(16);
    expect(sampled.length).toBe(stats.diagnosticsEmitted);
    expect(sampled.length).toBeGreaterThan(0);
    expect(client.subscribeCalls).toBe(1);
    expect(adapter.lastAcceptedTimestamp("up")).toBe(5_000);
    abort.abort("test");
    await running;
  });
});

describe("E reference unexpected close reconnects with backoff", () => {
  it("reconnects after a network close and does not spin", async () => {
    const sockets: ScriptedSocket[] = [];
    const factory = () => {
      const socket = new ScriptedSocket();
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    };
    const adapter = new ReferencePriceFeedAdapter({
      asset: "SOL", symbol: "SOLUSDT", url: "wss://example/sol", factory,
      reconnect: { initialDelayMs: 40, maxDelayMs: 80, multiplier: 2, jitterRatio: 0 },
    });
    const connections: FeedConnectionEvent[] = [];
    const errors: string[] = [];
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: (event) => { connections.push(event); },
      onError: (error) => { errors.push(error.message); },
    }, abort.signal);
    await waitFor(() => sockets.length === 1 && connections.some((event) => event.state === "CONNECTED"));
    const closedAt = Date.now();
    sockets[0]!.remoteClose(1006, "abnormal", false);
    await waitFor(() => sockets.length === 2 && connections.filter((event) => event.state === "CONNECTED").length >= 2);
    expect(Date.now() - closedAt).toBeGreaterThanOrEqual(30);
    expect(errors.some((message) => message.includes("kind=network_error") && message.includes("code=1006"))).toBe(true);
    expect(connections.some((event) => event.state === "DISCONNECTED" && event.message?.includes("network_error"))).toBe(true);
    abort.abort("test");
    await running;
  });
});

describe("F reference intentional abort does not failure-loop", () => {
  it("stop/abort is operator shutdown, not REFERENCE_STREAM_ENDED", async () => {
    const sockets: ScriptedSocket[] = [];
    const factory = () => {
      const socket = new ScriptedSocket();
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    };
    const adapter = new ReferencePriceFeedAdapter({
      asset: "ETH", symbol: "ETHUSDT", url: "wss://example/eth", factory,
      reconnect: { initialDelayMs: 10, maxDelayMs: 10, jitterRatio: 0 },
    });
    const connections: FeedConnectionEvent[] = [];
    const errors: string[] = [];
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: (event) => { connections.push(event); },
      onError: (error) => { errors.push(error.message); },
    }, abort.signal);
    await waitFor(() => connections.some((event) => event.state === "CONNECTED"));
    abort.abort("operator-stop");
    await running;
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(errors).toEqual([]);
    expect(connections.filter((event) => event.state === "DISCONNECTED")).toHaveLength(0);
    expect(connections.filter((event) => event.state === "STOPPED")).toHaveLength(1);
    expect(sockets.length).toBe(1);
    expect(classifyReferenceClose({
      abortInitiated: true, abortReason: "REFERENCE_ASSET_UNNEEDED", code: 1000, reason: "shutdown",
      wasClean: true, sawError: false, staleForced: false,
    })).toBe("asset_unneeded");
    expect(classifyReferenceClose({
      abortInitiated: true, abortReason: "SIGTERM", code: 1000, reason: "shutdown",
      wasClean: true, sawError: false, staleForced: false,
    })).toBe("operator_shutdown");
  });
});

describe("G only one running reference feed per asset", () => {
  it("serializes start/close/reconnect/stop and concurrent ensureRunning", async () => {
    let open = 0;
    let maxOpen = 0;
    const factory = () => {
      const socket = new ScriptedSocket();
      open += 1;
      maxOpen = Math.max(maxOpen, open);
      const original = socket.close.bind(socket);
      socket.close = (code?: number, reason?: string) => {
        if (socket.readyState !== 3) open = Math.max(0, open - 1);
        original(code, reason);
      };
      queueMicrotask(() => socket.open());
      return socket;
    };
    const registry = new ReferenceFeedRegistry(
      "wss://example.invalid/ws", "https://example.invalid", 15_000, factory,
      async () => ({ kind: "supported", detail: "trading" }),
      { negativeCacheTtlMs: 0 },
    );
    const parent = new AbortController();
    const handlers = silentHandlers();
    const results = await Promise.all([
      registry.ensureRunning("BNB", handlers, parent.signal),
      registry.ensureRunning("BNB", handlers, parent.signal),
      registry.ensureRunning("BNB", handlers, parent.signal),
    ]);
    expect(results.filter((result) => result === "started")).toHaveLength(1);
    expect(results.filter((result) => result === "already")).toHaveLength(2);
    expect(registry.runningAssets()).toEqual(["BNB"]);
    await waitFor(() => maxOpen === 1);

    registry.stop("BNB");
    const restarted = await registry.ensureRunning("BNB", handlers, parent.signal);
    expect(restarted).toBe("started");
    expect(registry.runningAssets()).toEqual(["BNB"]);
    registry.stop("BNB");
    await waitFor(() => registry.runningAssets().length === 0);
    expect(maxOpen).toBe(1);
    parent.abort("test");
  });
});

describe("H /health and /ready remain state-only", () => {
  it("does not touch journal, fetch, discovery, or websocket on health routes", async () => {
    const journal = new MemoryEventJournal();
    let healthChecks = 0;
    let appends = 0;
    let queries = 0;
    const wrapped = {
      healthCheck: async () => { healthChecks += 1; return journal.healthCheck(); },
      append: async (event: Parameters<MemoryEventJournal["append"]>[0]) => { appends += 1; return journal.append(event); },
      appendMany: async (events: Parameters<MemoryEventJournal["appendMany"]>[0]) => { appends += 1; return journal.appendMany(events); },
      readTrace: async (traceId: string) => { queries += 1; return journal.readTrace(traceId); },
      queryEvents: async (query: Parameters<MemoryEventJournal["queryEvents"]>[0]) => { queries += 1; return journal.queryEvents(query); },
      readAll: () => journal.readAll(),
      initialize: () => journal.initialize(),
      close: () => journal.close(),
      saveCheckpoint: (input: Parameters<MemoryEventJournal["saveCheckpoint"]>[0]) => journal.saveCheckpoint(input),
      loadLatestCheckpoint: () => journal.loadLatestCheckpoint(),
    };
    const ledger = new PaperLedger(1_000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "UP";
    state.setDiscoveryStatus("MONITORING", 40);
    state.setMarketMonitoring(true);
    state.connections.polymarket.state = "CONNECTED";
    let fetches = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (...args: Parameters<typeof fetch>) => {
      fetches += 1;
      return originalFetch(...args);
    }) as typeof fetch;
    const server = await createApiServer({
      state, journal: wrapped as never, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
    });
    try {
      const health = await server.inject({ method: "GET", url: "/health" });
      const ready = await server.inject({ method: "GET", url: "/ready" });
      expect(health.statusCode).toBe(200);
      expect(health.json()).toMatchObject({ status: "ok", mode: "paper", draining: false, engineAlive: true });
      expect(ready.statusCode).toBe(200);
      expect(ready.json()).toMatchObject({
        status: "ready", mode: "paper", engineAlive: true, database: "UP",
        discoveryStatus: "MONITORING", marketMonitoring: true, qualifiedMarketCount: 40, polymarketWs: "CONNECTED",
      });
      expect(healthChecks).toBe(0);
      expect(appends).toBe(0);
      expect(queries).toBe(0);
      expect(fetches).toBe(0);
      expect(state.readiness()).toEqual(ready.json());
    } finally {
      globalThis.fetch = originalFetch;
      await server.close();
    }
  });
});

describe("stale-event storm does not reconnect or grow maps", () => {
  it("keeps one stream and bounded per-token state under mixed valid/stale load", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt");
    const abort = new AbortController();
    let accepted = 0;
    const running = adapter.run(["up"], {
      onSnapshot: () => { accepted += 1; },
      onConnection: () => undefined,
      onError: () => { throw new Error("reconnect from stale events"); },
    }, abort.signal);
    await waitFor(() => client.streams.length === 1);
    for (let i = 0; i < 400; i += 1) {
      client.current().push(bookEvent("up", 10_000 + i, "0.40"));
      if (i % 5 === 0) client.current().push(bookEvent("up", 10_000 + i - 1, "0.01"));
    }
    await waitFor(() => accepted === 400);
    expect(adapter.timestampStats().staleDropped).toBe(80);
    expect(client.subscribeCalls).toBe(1);
    expect(adapter.activeStreamCount()).toBe(1);
    expect(adapter.lastAcceptedTimestamp("up")).toBe(10_399);
    abort.abort("test");
    await running;
  });

  it("changing qualified tokens resubscribes without overlapping streams", async () => {
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.registerManualToken("up", "mkt");
    adapter.registerManualToken("down", "mkt", "DOWN");
    const abort = new AbortController();
    const running = adapter.run(["up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => client.subscribeCalls === 1);
    adapter.setTokenIds(["up", "down"]);
    await waitFor(() => client.subscribeCalls === 2);
    expect(client.maxConcurrent).toBe(1);
    abort.abort("test");
    await running;
  });
});

describe("provider/region block detection remains intact", () => {
  it("still classifies HTTP 451/403 as blocked, not a reconnectable transient", () => {
    expect(classifyBinanceExchangeInfo({ ok: false, status: 451, body: null, symbol: "ETHUSDT" })).toEqual({
      kind: "provider_blocked", detail: "http_451",
    });
    expect(classifyBinanceExchangeInfo({ ok: false, status: 403, body: null, symbol: "ETHUSDT" })).toEqual({
      kind: "provider_blocked", detail: "http_403",
    });
    expect(evaluateRisk(safeRisk({ referenceProviderBlocked: true }), riskLimitsSchema.parse({})).vetoes).toContain("REFERENCE_PROVIDER_BLOCKED");
  });
});

describe("I paper safety remains paper-only after feed stabilization", () => {
  it("boots IDLE paper with noop Grok and does not start the experiment", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper",
      POLYMARKET_TOKEN_IDS: "up,down", GROK_ACTIVATION_PROVIDER: "noop",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: {
        registerManualToken: () => undefined,
        setTokenIds: (tokenIds: readonly string[]) => ({ changed: false, previous: [...tokenIds], next: [...tokenIds] }),
        run: async (_ids: unknown, _handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })),
      } as never,
    });
    await engine.start();
    try {
      expect(env.TRADING_MODE).toBe("paper");
      expect(env.GROK_ACTIVATION_PROVIDER).toBe("noop");
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      expect(engine.state.engineAlive()).toBe(true);
    } finally {
      await engine.stop();
    }
  });
});

describe("reference-feed lifecycle closure", () => {
  afterEach(() => { vi.useRealTimers(); });

  it("documents the stable-connection backoff reset threshold", () => {
    expect(REFERENCE_STABLE_CONNECTION_MS).toBe(5_000);
  });

  it("grows reconnect delay across consecutive short-lived open/close flaps and caps at maxDelay", async () => {
    vi.useFakeTimers();
    const createdAt: number[] = [];
    const sockets: ScriptedSocket[] = [];
    const factory = () => {
      createdAt.push(Date.now());
      const socket = new ScriptedSocket();
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    };
    const adapter = new ReferencePriceFeedAdapter({
      asset: "XRP", symbol: "XRPUSDT", url: "wss://example/xrp", factory, staleTimeoutMs: 60_000,
      reconnect: { initialDelayMs: 50, maxDelayMs: 200, multiplier: 2, jitterRatio: 0, stableAfterMs: 10_000 },
    });
    const abort = new AbortController();
    const running = adapter.run(silentHandlers(), abort.signal);
    const gaps: number[] = [];
    for (let i = 0; i < 5; i += 1) {
      await vi.advanceTimersByTimeAsync(0);
      await Promise.resolve();
      await Promise.resolve();
      expect(sockets.length).toBe(i + 1);
      sockets[i]!.remoteClose(1006, "flap", false);
      if (i === 4) break;
      const expected = Math.min(200, 50 * 2 ** i);
      const before = Date.now();
      await vi.advanceTimersByTimeAsync(expected);
      gaps.push(Date.now() - before);
    }
    expect(gaps).toEqual([50, 100, 200, 200]);
    const createdGaps = createdAt.slice(1).map((at, index) => at - createdAt[index]!);
    expect(createdGaps).toEqual([50, 100, 200, 200]);
    abort.abort("test");
    await vi.advanceTimersByTimeAsync(0);
    await running;
  });

  it("resets backoff only after a connection stays open for stableAfterMs", async () => {
    vi.useFakeTimers();
    const createdAt: number[] = [];
    const sockets: ScriptedSocket[] = [];
    const factory = () => {
      createdAt.push(Date.now());
      const socket = new ScriptedSocket();
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    };
    const adapter = new ReferencePriceFeedAdapter({
      asset: "SOL", symbol: "SOLUSDT", url: "wss://example/sol", factory, staleTimeoutMs: 60_000,
      reconnect: { initialDelayMs: 50, maxDelayMs: 200, multiplier: 2, jitterRatio: 0, stableAfterMs: 30 },
    });
    const abort = new AbortController();
    const running = adapter.run(silentHandlers(), abort.signal);
    await vi.advanceTimersByTimeAsync(0);
    await Promise.resolve();
    sockets[0]!.remoteClose(1006, "short", false);
    await vi.advanceTimersByTimeAsync(50);
    await Promise.resolve();
    expect(sockets.length).toBe(2);
    await vi.advanceTimersByTimeAsync(30);
    const stableClosedAt = Date.now();
    sockets[1]!.remoteClose(1006, "stable-then-close", false);
    await vi.advanceTimersByTimeAsync(49);
    expect(sockets.length).toBe(2);
    await vi.advanceTimersByTimeAsync(1);
    await Promise.resolve();
    expect(sockets.length).toBe(3);
    expect(createdAt[1]! - createdAt[0]!).toBe(50);
    expect(Date.now() - stableClosedAt).toBe(50);
    abort.abort("test");
    await vi.advanceTimersByTimeAsync(0);
    await running;
  });

  it("does not open a new socket until the previous error socket has closed", async () => {
    vi.useFakeTimers();
    let live = 0;
    let maxLive = 0;
    const sockets: DelayedCloseSocket[] = [];
    const factory = () => {
      const socket = new DelayedCloseSocket(() => {
        live = Math.max(0, live - 1);
      });
      live += 1;
      maxLive = Math.max(maxLive, live);
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    };
    const adapter = new ReferencePriceFeedAdapter({
      asset: "ETH", symbol: "ETHUSDT", url: "wss://example/eth", factory, staleTimeoutMs: 60_000,
      reconnect: { initialDelayMs: 20, maxDelayMs: 20, jitterRatio: 0, stableAfterMs: 10_000 },
    });
    const abort = new AbortController();
    const running = adapter.run(silentHandlers(), abort.signal);
    await vi.advanceTimersByTimeAsync(0);
    await Promise.resolve();
    expect(sockets.length).toBe(1);
    sockets[0]!.error();
    await vi.advanceTimersByTimeAsync(5_000);
    await Promise.resolve();
    expect(sockets.length).toBe(1);
    expect(live).toBe(1);
    expect(maxLive).toBe(1);
    sockets[0]!.completeClose();
    await vi.advanceTimersByTimeAsync(20);
    await Promise.resolve();
    expect(sockets.length).toBe(2);
    expect(maxLive).toBe(1);
    expect(live).toBe(1);
    abort.abort("test");
    sockets.at(-1)?.completeClose();
    await vi.advanceTimersByTimeAsync(0);
    await running;
  });

  it("stop during an in-flight resolve does not start the feed", async () => {
    let probes = 0;
    let factoryCalls = 0;
    let release!: (outcome: { kind: "supported"; detail: string }) => void;
    const factory = () => {
      factoryCalls += 1;
      const socket = new ScriptedSocket();
      queueMicrotask(() => socket.open());
      return socket;
    };
    const registry = new ReferenceFeedRegistry(
      "wss://example.invalid/ws", "https://example.invalid", 15_000, factory,
      () => {
        probes += 1;
        return new Promise((resolve) => { release = resolve; });
      },
      { negativeCacheTtlMs: 0 },
    );
    const parent = new AbortController();
    const pending = registry.ensureRunning("SOL", silentHandlers(), parent.signal);
    await waitFor(() => probes === 1);
    expect(registry.runningAssets()).toEqual([]);
    registry.stop("SOL");
    release({ kind: "supported", detail: "trading" });
    expect(await pending).toBe("stopped");
    expect(factoryCalls).toBe(0);
    expect(registry.runningAssets()).toEqual([]);

    const restarted = await registry.ensureRunning("SOL", silentHandlers(), parent.signal);
    expect(restarted).toBe("started");
    expect(factoryCalls).toBe(1);
    expect(registry.runningAssets()).toEqual(["SOL"]);
    registry.stop("SOL");
    parent.abort("test");
  });
});
