import { describe, expect, it } from "vitest";
import {
  classifyTriggerProvenance,
  createId,
  grokSignalSubmissionSchema,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokActivationResult,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  BoundedClobMarketTransport,
  CLOB_MARKET_WS_ERROR,
  CLOB_MARKET_WS_STALE,
  MARKET_INGRESS_MALFORMED,
  MarketIngressMalformedError,
  PolymarketMarketDataAdapter,
  REFERENCE_INGRESS_BOUNDS,
  REFERENCE_INGRESS_OVERFLOW,
  REFERENCE_WS_CONNECT_TIMEOUT,
  ReferencePriceFeedAdapter,
  ScriptedBoundedMarketTransport,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { mcpVisibleOrderbook, mcpVisibleRecentTriggers } from "../apps/engine/src/mcp.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedReferenceRegistry,
  qualifiedPaperEnv,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";

async function waitFor(predicate: () => boolean, timeoutMs = 3_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function drainRuntime(engine: EngineService): Promise<void> {
  await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0);
}

class ControllableWebSocket extends EventTarget {
  static instances: ControllableWebSocket[] = [];
  readyState = 0;
  sent: string[] = [];
  pingSends = 0;
  constructor(readonly url: string) {
    super();
    ControllableWebSocket.instances.push(this);
  }
  send(data: string): void {
    this.sent.push(data);
    if (data === "PING") this.pingSends += 1;
  }
  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  message(data: string): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
  error(): void {
    this.dispatchEvent(new Event("error"));
  }
}

class SilentRefSocket extends EventTarget {
  readyState = 0;
  close(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}

/** close() returns normally and never emits a close event. */
class CloseNeverAcksSocket extends EventTarget {
  readyState = 0;
  added = 0;
  removed = 0;
  get liveListeners(): number { return this.added - this.removed; }
  addEventListener(type: string, callback: EventListenerOrEventListenerObject | null, options?: boolean | AddEventListenerOptions): void {
    this.added += 1;
    super.addEventListener(type, callback, options);
  }
  removeEventListener(type: string, callback: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions): void {
    this.removed += 1;
    super.removeEventListener(type, callback, options);
  }
  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  close(): void {
    this.readyState = 2;
  }
  message(data: string): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
}

class ControllableRefSocket extends EventTarget {
  readyState = 0;
  messages: string[] = [];
  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event("close"), { code: 1000, reason: "", wasClean: true }));
  }
  message(data: string): void {
    this.messages.push(data);
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
}

function liveBook(tokenId: string, timestamp: number, qty = "10") {
  return {
    type: "book",
    payload: {
      tokenId, market: tokenId, timestamp,
      bids: [{ price: "0.47", size: qty }],
      asks: [{ price: "0.48", size: "5" }],
      lastTradePrice: "0.48",
    },
  };
}

function liveDelta(tokenId: string, timestamp: number, extras: Array<Record<string, unknown>> = [], fields: Record<string, unknown> = {}) {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId, side: "BUY", price: "0.47", size: "9" }, ...extras],
      ...fields,
    },
  };
}

function bookSnapshot(outcome: "UP" | "DOWN", ask: string, extra: Record<string, unknown> = {}) {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return {
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId,
    outcome,
    asset: "BTC",
    bids: [{ price: "0.40", quantity: "200" }],
    asks: [{ price: ask, quantity: "1" }],
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

function refTick(asset: string, price: string, atMs: number) {
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
  };
}

function binanceTrade(price: string, qty = "1", atMs = Date.now()) {
  return JSON.stringify({ e: "aggTrade", s: "BTCUSDT", p: price, q: qty, T: atMs, E: atMs, a: atMs });
}

class HeldActivationBridge implements GrokActivationBridge {
  readonly provider = "held";
  calls = 0;
  hold: Promise<void> | null = null;
  async activate(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    this.calls += 1;
    if (this.hold) await this.hold;
    const requestedAt = new Date().toISOString();
    return request.targetRoles.map((targetRole) => ({
      provider: this.provider,
      ok: true,
      skipped: false,
      targetRole,
      requestedAt,
      sentAt: requestedAt,
      errorCode: null,
      httpStatus: 200,
      grokWakeObserved: false as const,
      idempotencyKey: `${request.triggerId}:${targetRole}`,
    }));
  }
}

function submission(trigger: TriggerEvent, agentId: "fair_probability_analyst" | "market_microstructure_scanner" = "fair_probability_analyst") {
  return grokSignalSubmissionSchema.parse({
    submissionId: createId("sub"),
    triggerId: trigger.id,
    traceId: trigger.traceId,
    correlationId: createId("corr"),
    marketId: trigger.marketId ?? "btc-mkt",
    timestamp: new Date().toISOString(),
    agentId,
    role: agentId,
    output: {
      confidence: 0.8,
      evidenceIds: ["e1"],
      expiresAt: new Date(Date.now() + 30_000).toISOString(),
      reasonCodes: ["TEST"],
      direction: "UP",
      fairProbability: 0.61,
      marketProbability: 0.48,
      estimatedEdge: 0.04,
    },
  });
}

describe("A — CLOB stream terminalization is total", () => {
  async function openStream(options: ConstructorParameters<typeof BoundedClobMarketTransport>[0] = {}) {
    ControllableWebSocket.instances = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: ControllableWebSocket as unknown as typeof WebSocket,
      pingIntervalMs: 20,
      staleMs: 40,
      ...options,
    });
    const pending = transport.subscribe(["tok-up"]);
    await waitFor(() => ControllableWebSocket.instances.length === 1);
    const socket = ControllableWebSocket.instances[0]!;
    socket.open();
    const stream = await pending;
    return { transport, stream, socket };
  }

  it("pending next + malformed settles the waiter with the terminal error", async () => {
    const { stream } = await openStream();
    const next = stream[Symbol.asyncIterator]().next();
    await waitFor(() => stream.pendingWaiterCount() === 1);
    ControllableWebSocket.instances[0]!.message("{not-json");
    await expect(next).rejects.toThrow(MARKET_INGRESS_MALFORMED);
    expect(stream.pendingWaiterCount()).toBe(0);
  });

  it("pending next + stale timeout rejects and clears the ping timer", async () => {
    const { stream } = await openStream({ pingIntervalMs: 15, staleMs: 25 });
    const next = stream[Symbol.asyncIterator]().next();
    await waitFor(() => stream.pendingWaiterCount() === 1);
    await expect(next).rejects.toThrow(CLOB_MARKET_WS_STALE);
    expect(stream.pendingWaiterCount()).toBe(0);
    expect(stream.pingTimerActive()).toBe(false);
  });

  it("pending next + runtime socket error rejects immediately", async () => {
    const { stream, socket } = await openStream();
    const next = stream[Symbol.asyncIterator]().next();
    await waitFor(() => stream.pendingWaiterCount() === 1);
    socket.error();
    await expect(next).rejects.toThrow(CLOB_MARKET_WS_ERROR);
    expect(stream.pendingWaiterCount()).toBe(0);
    expect(stream.pingTimerActive()).toBe(false);
  });

  it("pending next + remote close completes cleanly and clears ping", async () => {
    const { stream, socket } = await openStream();
    const next = stream[Symbol.asyncIterator]().next();
    await waitFor(() => stream.pendingWaiterCount() === 1);
    socket.close();
    await expect(next).resolves.toEqual({ value: undefined, done: true });
    expect(stream.pendingWaiterCount()).toBe(0);
    expect(stream.pingTimerActive()).toBe(false);
  });

  it("explicit close is a clean iterator completion", async () => {
    const { stream } = await openStream();
    const next = stream[Symbol.asyncIterator]().next();
    await waitFor(() => stream.pendingWaiterCount() === 1);
    await stream.close();
    await expect(next).resolves.toEqual({ value: undefined, done: true });
    expect(stream.pendingWaiterCount()).toBe(0);
  });

  it("old socket late message cannot enter a newer stream generation", async () => {
    const { transport, socket, stream } = await openStream();
    const firstGen = stream.streamGeneration();
    await stream.close();
    const pending = transport.subscribe(["tok-up"]);
    await waitFor(() => ControllableWebSocket.instances.length === 2);
    const second = ControllableWebSocket.instances[1]!;
    second.open();
    const next = await pending;
    expect(next.streamGeneration()).not.toBe(firstGen);
    socket.message(JSON.stringify(liveBook("tok-up", 1_000)));
    expect(transport.queue.depth).toBe(0);
    second.message(JSON.stringify(liveBook("tok-up", 2_000)));
    const item = await next[Symbol.asyncIterator]().next();
    expect(item.done).toBe(false);
    expect((item.value as { payload?: { timestamp?: number } }).payload?.timestamp).toBe(2_000);
  });

  it("old generation iterator cannot dequeue a newer stream event", async () => {
    const { transport, stream } = await openStream();
    const oldIter = stream[Symbol.asyncIterator]();
    await stream.close();
    const pending = transport.subscribe(["tok-up"]);
    await waitFor(() => ControllableWebSocket.instances.length === 2);
    ControllableWebSocket.instances[1]!.open();
    const stream2 = await pending;
    const newIter = stream2[Symbol.asyncIterator]();
    ControllableWebSocket.instances[1]!.message(JSON.stringify(liveBook("tok-up", 2_000)));
    expect(transport.queue.depth).toBe(1);
    const oldNext = await oldIter.next();
    expect(oldNext.done).toBe(true);
    expect(oldNext.value).toBeUndefined();
    expect(transport.queue.depth).toBe(1);
    const oldAgain = await oldIter.next();
    expect(oldAgain.done).toBe(true);
    expect(transport.queue.depth).toBe(1);
    const newNext = await newIter.next();
    expect(newNext.done).toBe(false);
    expect((newNext.value as { payload?: { timestamp?: number } }).payload?.timestamp).toBe(2_000);
    expect(transport.queue.depth).toBe(0);
  });

  it("multi-event frame overflow stops after the first failure and counts one gap", async () => {
    const { transport, socket, stream } = await openStream({ maxEvents: 2, maxBytes: 4_096 });
    const next = stream[Symbol.asyncIterator]().next();
    const events = [liveBook("tok-up", 1), liveBook("tok-up", 2), liveBook("tok-up", 3)];
    socket.message(JSON.stringify(events));
    await expect(next).rejects.toThrow(/MARKET_INGRESS_OVERFLOW/);
    expect(transport.queue.snapshot().gapTotal).toBe(1);
    expect(stream.pendingWaiterCount()).toBe(0);
  });
});

describe("B+C — atomic full book and price_change", () => {
  function adapter() {
    const instance = new PolymarketMarketDataAdapter();
    instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
    instance.setTokenIds(["tok-up"]);
    return instance;
  }

  it("malformed full-book quantity does not SYNC or publish", () => {
    const instance = adapter();
    expect(() => instance.normalizeSourceEvent({
      type: "book",
      payload: {
        tokenId: "tok-up", market: "mkt", timestamp: 1_000,
        bids: [{ price: "0.47", size: "nope" }], asks: [{ price: "0.48", size: "5" }],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.isTokenSynchronized("tok-up")).toBe(false);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.lastAcceptedTimestamp("tok-up")).toBeUndefined();
  });

  it("malformed full-book price does not SYNC", () => {
    const instance = adapter();
    expect(() => instance.normalizeSourceEvent({
      type: "book",
      payload: {
        tokenId: "tok-up", market: "mkt", timestamp: 1_000,
        bids: [{ price: "xx", size: "10" }], asks: [{ price: "0.48", size: "5" }],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.isTokenSynchronized("tok-up")).toBe(false);
  });

  it("malformed full-book timestamp does not leave AWAITING stuck without a resync error", () => {
    const instance = adapter();
    expect(() => instance.normalizeSourceEvent({
      type: "book",
      payload: {
        tokenId: "tok-up", market: "mkt", timestamp: "NaN",
        bids: [{ price: "0.47", size: "10" }], asks: [{ price: "0.48", size: "5" }],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.tokenSyncState("tok-up")).toBe("AWAITING_BOOK");
  });

  it("malformed price_change side/price/size/timestamp leave zero mutation", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    const before = instance.bookState("tok-up")!;
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_001, [{ tokenId: "tok-up", side: "HOLD", price: "0.10", size: "9" }]))).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.tokenSyncState("tok-up")).toBe("GAPPED");
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    expect(instance.isTokenSynchronized("tok-up")).toBe(true);
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: { timestamp: 2_001, priceChanges: [{ tokenId: "tok-up", side: "BUY", price: "bad", size: "1" }] },
    })).toThrow(MarketIngressMalformedError);
    instance.normalizeSourceEvent(liveBook("tok-up", 3_000));
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: { timestamp: 3_001, priceChanges: [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "nope" }] },
    })).toThrow(MarketIngressMalformedError);
    instance.normalizeSourceEvent(liveBook("tok-up", 4_000));
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: { timestamp: "x", priceChanges: [{ tokenId: "tok-up", side: "BUY", price: "0.41", size: "1" }] },
    })).toThrow(MarketIngressMalformedError);
    expect(before.bids.get("0.47")).toBe("10");
  });

  it("malformed second entry in a multi-entry payload commits nothing", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: {
        timestamp: 1_001,
        priceChanges: [
          { tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" },
          { tokenId: "tok-up", side: "NOPE", price: "0.60", size: "1" },
        ],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
  });

  it("stale timestamp remains a safe-drop, not a forced reconnect", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(instance.normalizeSourceEvent(liveDelta("tok-up", 999))).toEqual([]);
    expect(instance.isTokenSynchronized("tok-up")).toBe(true);
    expect(instance.bookState("tok-up")?.bids.get("0.47")).toBe("10");
  });

  it("fresh canonical resync restores normal delta processing", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_001, [{ tokenId: "tok-up", side: "HOLD", price: "0.1", size: "1" }]))).toThrow(MarketIngressMalformedError);
    const resynced = instance.normalizeSourceEvent(liveBook("tok-up", 2_000, "12"));
    expect(resynced).toHaveLength(1);
    expect(instance.normalizeSourceEvent(liveDelta("tok-up", 2_001))).toHaveLength(1);
  });

  it("A: valid delta + malformed tickSize throws MarketIngressMalformedError with zero trusted mutation", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(instance.lastAcceptedTimestamp("tok-up")).toBe(1_000);
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_001, [], { tickSize: "nope" }))).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.tokenSyncState("tok-up")).toBe("GAPPED");
    expect(instance.lastAcceptedTimestamp("tok-up")).toBeUndefined();
    expect(instance.isTokenSynchronized("tok-up")).toBe(false);
  });

  it("B: valid delta + malformed minOrderSize throws MarketIngressMalformedError with zero trusted mutation", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_001, [], { minOrderSize: "nope" }))).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.tokenSyncState("tok-up")).toBe("GAPPED");
    expect(instance.lastAcceptedTimestamp("tok-up")).toBeUndefined();
  });

  it("C: multi-entry valid book deltas do not commit when later snapshot-level validation fails", () => {
    const instance = new PolymarketMarketDataAdapter();
    instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
    instance.registerManualToken("tok-down", "mkt", "DOWN", "BTC");
    instance.setTokenIds(["tok-up", "tok-down"]);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    instance.normalizeSourceEvent(liveBook("tok-down", 1_000));
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: {
        timestamp: 1_001,
        tickSize: "nope",
        priceChanges: [
          { tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" },
          { tokenId: "tok-down", side: "SELL", price: "0.52", size: "3" },
        ],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.bookState("tok-down")).toBeUndefined();
    expect(instance.tokenSyncState("tok-up")).toBe("GAPPED");
    expect(instance.tokenSyncState("tok-down")).toBe("GAPPED");
    expect(instance.lastAcceptedTimestamp("tok-up")).toBeUndefined();
    expect(instance.lastAcceptedTimestamp("tok-down")).toBeUndefined();
  });

  it("D: fresh full book after snapshot-metadata gap restores normal processing", () => {
    const instance = adapter();
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_001, [], { tickSize: "nope" }))).toThrow(MarketIngressMalformedError);
    expect(instance.tokenSyncState("tok-up")).toBe("GAPPED");
    const resynced = instance.normalizeSourceEvent(liveBook("tok-up", 2_000, "12"));
    expect(resynced).toHaveLength(1);
    expect(instance.isTokenSynchronized("tok-up")).toBe(true);
    const deltas = instance.normalizeSourceEvent(liveDelta("tok-up", 2_001));
    expect(deltas).toHaveLength(1);
    expect(instance.bookState("tok-up")?.bids.get("0.47")).toBe("9");
  });
});

describe("D+E+F — end-to-end trigger provenance", () => {
  it("classifies market-derived NEWS as MARKET and reference-derived NEWS as REFERENCE", () => {
    expect(classifyTriggerProvenance({ kind: "NEWS_INFORMATION", evidence: { because: "BOOK_IMBALANCE" } })).toBe("MARKET");
    expect(classifyTriggerProvenance({ kind: "NEWS_INFORMATION", evidence: { because: "BTC_RAPID_MOVE" } })).toBe("REFERENCE");
    expect(classifyTriggerProvenance({ kind: "NEARING_EXPIRY" })).toBe("NONE");
    expect(classifyTriggerProvenance({ kind: "FAILED_HEDGE" })).toBe("NONE");
  });

  it("3: BOOK_IMBALANCE and derived NEWS stay stale after gap/resync", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    await market.emitSnapshot(bookSnapshot("UP", "0.48"));
    await drainRuntime(engine);
    const before = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
    const book = before.filter((event) => event.payload.kind === "BOOK_IMBALANCE");
    const news = before.filter((event) => event.payload.kind === "NEWS_INFORMATION");
    expect(book.length).toBeGreaterThan(0);
    expect(news.length).toBeGreaterThan(0);
    for (const event of [...book, ...news]) {
      expect(engine.triggerProvenanceForTest(String(event.payload.id))?.kind).toBe("MARKET");
      expect(engine.isTriggerActionable(String(event.payload.id))).toBe(true);
    }
    market.emitGap(["btc-up", "btc-down"]);
    await drainRuntime(engine);
    for (const event of [...book, ...news]) {
      expect(engine.isTriggerActionable(String(event.payload.id))).toBe(false);
    }
    market.markTokenSync("btc-up", "SYNCED");
    await market.emitSnapshot(bookSnapshot("UP", "0.49", { eventId: createId("market") }));
    await drainRuntime(engine);
    for (const event of [...book, ...news]) {
      expect(engine.isTriggerActionable(String(event.payload.id))).toBe(false);
    }
    const actionable = mcpVisibleRecentTriggers(engine.state, (id) => engine.isTriggerActionable(id));
    expect(actionable.some((item) => book.some((event) => event.payload.id === item.id))).toBe(false);
    await engine.stop();
  });

  it("4: reference rapid-move and derived NEWS cannot activate after a new reference epoch", async () => {
    const { engine } = await startQualifiedEngine();
    const start = Date.parse("2026-08-31T12:00:00.000Z");
    await engine.ingestReferenceTickForTest(refTick("BTC", "100", start));
    await engine.ingestReferenceTickForTest(refTick("BTC", "102", start + 1_000));
    await drainRuntime(engine);
    const epoch = engine.referenceEpochForTest("BTC");
    const ids = [...engine.state.recentTriggers.filter((item) => item.kind === "BTC_RAPID_MOVE" || item.kind === "NEWS_INFORMATION")].map((item) => item.id);
    expect(ids.length).toBeGreaterThan(0);
    await engine.applyReferenceConnectionForTest("BTC", "STALE");
    expect(engine.referenceEpochForTest("BTC")).toBe(epoch + 1);
    await engine.applyReferenceConnectionForTest("BTC", "DISCONNECTED");
    expect(engine.referenceEpochForTest("BTC")).toBe(epoch + 1);
    for (const id of ids) expect(engine.isTriggerActionable(id)).toBe(false);
    await engine.applyReferenceConnectionForTest("BTC", "CONNECTED");
    await engine.ingestReferenceTickForTest(refTick("BTC", "103", start + 5_000));
    for (const id of ids) expect(engine.isTriggerActionable(id)).toBe(false);
    await engine.stop();
  });

  it("5: journal-wait gap falsifies actionability after TRIGGER_EVENT persist", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    await market.waitForHandlers();
    let release!: () => void;
    engine.setTriggerEventPersistHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
    await market.emitSnapshot(bookSnapshot("UP", "0.48"));
    await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight + engine.runtimeSupervisorSnapshot().totalQueued > 0);
    market.emitGap(["btc-up"]);
    release();
    engine.setTriggerEventPersistHoldForTest(null);
    await drainRuntime(engine);
    const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION"] });
    const triggers = events.filter((event) => {
      const kind = String(event.payload.kind ?? "");
      return event.type === "TRIGGER_EVENT" && (kind === "BOOK_IMBALANCE" || kind === "NEWS_INFORMATION");
    });
    expect(triggers.length).toBeGreaterThan(0);
    expect(events.some((event) => event.type === "GROK_ACTIVATION_DECISION" && event.payload.activate === true)).toBe(false);
    for (const event of triggers) {
      expect(engine.isTriggerActionable(String(event.payload.id))).toBe(false);
      const trigger = engine.state.recentTriggers.find((item) => item.id === event.payload.id);
      if (trigger) {
        expect(await engine.submitGrokSignal(submission(trigger), "fair_probability_analyst")).toMatchObject({ status: "stale" });
      }
    }
    await engine.stop();
  });

  it("6: submitGrokSignal is stale after source epoch invalidates an in-flight activation", async () => {
    const { ScriptedMarketAdapter, discoveryReport, qualified } = await import("./paper-engine-testkit.js");
    const activation = new HeldActivationBridge();
    let release!: () => void;
    activation.hold = new Promise<void>((resolve) => { release = resolve; });
    const scripted = new ScriptedMarketAdapter();
    scripted.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: scripted as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation,
    });
    await engine.start();
    expect((await engine.beginExperiment()).ok).toBe(true);
    await scripted.waitForHandlers();
    await scripted.emitSnapshot(bookSnapshot("UP", "0.48"));
    await waitFor(() => activation.calls >= 1);
    scripted.emitGap(["btc-up"]);
    release();
    await drainRuntime(engine);
    const trigger = engine.state.recentTriggers.find((item) => item.kind === "BOOK_IMBALANCE");
    expect(trigger).toBeTruthy();
    expect(engine.isTriggerActionable(trigger!.id)).toBe(false);
    expect(await engine.submitGrokSignal(submission(trigger!), "fair_probability_analyst")).toMatchObject({ status: "stale" });
    expect(engine.state.latestConsensus).toBeNull();
    await engine.stop();
  });
});

describe("G+H+I — reference ingress, connect, timestamps", () => {
  it("7: socket that never opens hits a finite connect timeout and retries", async () => {
    let created = 0;
    const adapter = new ReferencePriceFeedAdapter({
      asset: "SOL", symbol: "SOLUSDT", url: "wss://example/sol",
      factory: () => {
        created += 1;
        return new SilentRefSocket();
      },
      reconnect: { initialDelayMs: 20, maxDelayMs: 20, jitterRatio: 0, connectTimeoutMs: 30 },
    });
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => created >= 2, 3_000);
    expect(created).toBeGreaterThanOrEqual(2);
    abort.abort(REFERENCE_WS_CONNECT_TIMEOUT);
    await running;
  });

  it("8: slow handler keeps the pre-handler queue finite and overflows only that asset", async () => {
    const sockets: ControllableRefSocket[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "BTC", symbol: "BTCUSDT", url: "wss://example/btc",
      factory: () => {
        const socket = new ControllableRefSocket();
        sockets.push(socket);
        queueMicrotask(() => socket.open());
        return socket;
      },
      reconnect: { initialDelayMs: 20, maxDelayMs: 20, jitterRatio: 0 },
    });
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let started = 0;
    const connections: string[] = [];
    const errors: string[] = [];
    const abort = new AbortController();
    const running = adapter.run({
      onTick: async () => {
        started += 1;
        if (started === 1) await hold;
      },
      onConnection: (event) => { connections.push(event.state); },
      onError: (error) => { errors.push(error.message); },
    }, abort.signal);
    await waitFor(() => sockets.length === 1 && connections.includes("CONNECTED"));
    const socket = sockets[0]!;
    socket.message(binanceTrade("100"));
    await waitFor(() => started === 1);
    for (let i = 0; i < REFERENCE_INGRESS_BOUNDS.maxEvents + 8; i += 1) {
      socket.message(binanceTrade(String(100 + i), "1", Date.now() + i));
    }
    expect(adapter.lastIngress?.depth ?? 0).toBeLessThanOrEqual(REFERENCE_INGRESS_BOUNDS.maxEvents);
    await waitFor(() => errors.includes(REFERENCE_INGRESS_OVERFLOW) || connections.includes("STALE"));
    release();
    abort.abort("test");
    await running;
  });

  it("9: delayed frame from an old session cannot establish a new baseline", async () => {
    const sockets: ControllableRefSocket[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "ETH", symbol: "ETHUSDT", url: "wss://example/eth",
      factory: () => {
        const socket = new ControllableRefSocket();
        sockets.push(socket);
        queueMicrotask(() => socket.open());
        return socket;
      },
      reconnect: { initialDelayMs: 15, maxDelayMs: 15, jitterRatio: 0 },
    });
    const ticks: string[] = [];
    const abort = new AbortController();
    const running = adapter.run({
      onTick: (tick) => { ticks.push(tick.price); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => sockets.length === 1);
    const first = sockets[0]!;
    first.close();
    await waitFor(() => sockets.length === 2);
    first.message(binanceTrade("999"));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(ticks).not.toContain("999");
    sockets[1]!.message(binanceTrade("101"));
    await waitFor(() => ticks.includes("101"));
    abort.abort("test");
    await running;
  });

  it("10: stale/out-of-order reference tick does not append or emit a rapid-move", async () => {
    const { engine } = await startQualifiedEngine();
    const start = Date.parse("2026-08-31T12:00:00.000Z");
    await engine.ingestReferenceTickForTest(refTick("BTC", "100", start));
    await engine.ingestReferenceTickForTest(refTick("BTC", "100.1", start + 1_000));
    await drainRuntime(engine);
    const before = engine.state.recentTriggers.filter((item) => item.kind === "BTC_RAPID_MOVE").length;
    await engine.ingestReferenceTickForTest(refTick("BTC", "200", start + 500));
    await drainRuntime(engine);
    expect(engine.state.recentTriggers.filter((item) => item.kind === "BTC_RAPID_MOVE").length).toBe(before);
    expect(engine.referenceWindowTotalEntriesForTest()).toBeLessThanOrEqual(2);
    await engine.stop();
  });

  it("A: never-open close-never-acks connect timeout settles and retries", async () => {
    const sockets: CloseNeverAcksSocket[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "SOL", symbol: "SOLUSDT", url: "wss://example/sol",
      factory: () => {
        const socket = new CloseNeverAcksSocket();
        sockets.push(socket);
        return socket;
      },
      reconnect: { initialDelayMs: 20, maxDelayMs: 20, jitterRatio: 0, connectTimeoutMs: 30, closeWatchdogMs: 40 },
    });
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => sockets.length >= 2, 3_000);
    expect(sockets.length).toBeGreaterThanOrEqual(2);
    expect(adapter.lastIngress).toBeNull();
    abort.abort(REFERENCE_WS_CONNECT_TIMEOUT);
    await running;
  });

  it("B: abort while CONNECTING + close-never-acks settles run promptly", async () => {
    const sockets: CloseNeverAcksSocket[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "ETH", symbol: "ETHUSDT", url: "wss://example/eth",
      factory: () => {
        const socket = new CloseNeverAcksSocket();
        sockets.push(socket);
        return socket;
      },
      reconnect: { initialDelayMs: 5_000, maxDelayMs: 5_000, jitterRatio: 0, connectTimeoutMs: 5_000, closeWatchdogMs: 40 },
    });
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => sockets.length === 1);
    const started = Date.now();
    abort.abort("test");
    await running;
    expect(Date.now() - started).toBeLessThan(250);
    expect(adapter.lastIngress).toBeNull();
    expect(adapter.running).toBe(false);
  });

  it("C+D: late open/message/close after forced finalization are ignored and nothing is retained", async () => {
    const sockets: CloseNeverAcksSocket[] = [];
    const ticks: string[] = [];
    const connections: string[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "BTC", symbol: "BTCUSDT", url: "wss://example/btc",
      factory: () => {
        const socket = new CloseNeverAcksSocket();
        sockets.push(socket);
        return socket;
      },
      reconnect: { initialDelayMs: 20, maxDelayMs: 20, jitterRatio: 0, connectTimeoutMs: 30, closeWatchdogMs: 40 },
    });
    const generationBefore = adapter.currentSessionGeneration();
    const abort = new AbortController();
    const running = adapter.run({
      onTick: (tick) => { ticks.push(tick.price); },
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => sockets.length >= 1 && adapter.currentSessionGeneration() > generationBefore);
    await waitFor(() => sockets.length >= 2, 3_000);
    const first = sockets[0]!;
    first.open();
    first.message(binanceTrade("999"));
    first.dispatchEvent(new Event("close"));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(ticks).not.toContain("999");
    expect(connections).not.toContain("CONNECTED");
    expect(first.liveListeners).toBe(0);
    expect(adapter.lastIngress).toBeNull();
    abort.abort("test");
    await running;
  });

  it("STALE terminal initiation is one-shot while close never acks", async () => {
    const sockets: CloseNeverAcksSocket[] = [];
    const connections: string[] = [];
    const adapter = new ReferencePriceFeedAdapter({
      asset: "XRP", symbol: "XRPUSDT", url: "wss://example/xrp",
      staleTimeoutMs: 40,
      factory: () => {
        const socket = new CloseNeverAcksSocket();
        sockets.push(socket);
        queueMicrotask(() => socket.open());
        return socket;
      },
      reconnect: { initialDelayMs: 10_000, maxDelayMs: 10_000, jitterRatio: 0, closeWatchdogMs: 40 },
    });
    const abort = new AbortController();
    const running = adapter.run({
      onTick: () => undefined,
      onConnection: (event) => { connections.push(event.state); },
      onError: () => undefined,
    }, abort.signal);
    await waitFor(() => connections.includes("CONNECTED"));
    await waitFor(() => connections.includes("STALE"), 2_000);
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    expect(connections.filter((state) => state === "STALE")).toHaveLength(1);
    expect(adapter.lastIngress).toBeNull();
    abort.abort("test");
    await running;
  });
});

describe("J — market reconnect backoff is stability-gated", () => {
  it("11: connect-then-die before a full book increases backoff; historic reconnects do not relabel generation_change", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue, stableAfterMs: 50 });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    adapter.registerManualToken("tok-down", "tok-down", "DOWN", "BTC");
    const ac = new AbortController();
    const gaps: string[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
      onGap: (event) => { gaps.push(event.reason); },
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 1);
    await waitFor(() => transport.subscribeCalls >= 2 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 2);
    expect(adapter.reconnectDelays[1]!).toBeGreaterThan(adapter.reconnectDelays[0]! * 0.8);
    await waitFor(() => transport.subscribeCalls >= 3 && transport.stream && !transport.current().closed);
    expect(transport.push(liveBook("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 3);
    adapter.setTokenIds(["tok-up", "tok-down"]);
    await waitFor(() => gaps.includes("generation_change"), 20_000);
    expect(gaps.filter((reason) => reason === "generation_change").length).toBeGreaterThan(0);
    ac.abort();
    await running;
  }, 25_000);

  it("trusted book that dies immediately does not reset backoff; a stable trusted session does", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue, stableAfterMs: 40 });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1);
    expect(transport.push(liveBook("tok-up", 1_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 1);
    const afterUnstable = adapter.reconnectDelays[0]!;
    await waitFor(() => transport.subscribeCalls >= 2);
    expect(transport.push(liveBook("tok-up", 2_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    await new Promise((resolve) => setTimeout(resolve, 50));
    transport.current().endRemote();
    await waitFor(() => transport.subscribeCalls >= 3);
    await waitFor(() => transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 2);
    expect(adapter.reconnectDelays.at(-1)!).toBeLessThanOrEqual(afterUnstable * 1.5);
    ac.abort();
    await running;
  }, 15_000);

  it("A: slow connect plus immediate trusted-book death does not reset backoff", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue, stableAfterMs: 40 });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 1);
    await waitFor(() => transport.subscribeCalls >= 2 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 2);
    const grown = adapter.reconnectDelays[1]!;
    expect(grown).toBeGreaterThan(adapter.reconnectDelays[0]! * 0.8);
    const held = transport.holdNextSubscribe();
    await waitFor(() => transport.subscribeCalls >= 3);
    await new Promise((resolve) => setTimeout(resolve, 50));
    held.release();
    await waitFor(() => transport.stream && !transport.current().closed);
    expect(transport.push(liveBook("tok-up", 3_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 3);
    expect(adapter.reconnectDelays[2]!).toBeGreaterThan(grown * 0.8);
    ac.abort();
    await running;
  }, 15_000);

  it("B: fast connect + trusted book + survive stableAfterMs resets backoff", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue, stableAfterMs: 40 });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 1);
    await waitFor(() => transport.subscribeCalls >= 2 && transport.stream && !transport.current().closed);
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 2);
    const grown = adapter.reconnectDelays[1]!;
    await waitFor(() => transport.subscribeCalls >= 3 && transport.stream && !transport.current().closed);
    expect(transport.push(liveBook("tok-up", 3_000)).ok).toBe(true);
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    await new Promise((resolve) => setTimeout(resolve, 50));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 3);
    expect(adapter.reconnectDelays[2]!).toBeLessThan(grown);
    ac.abort();
    await running;
  }, 15_000);

  it("C: connect without a trusted book for longer than stableAfterMs never resets backoff", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue, stableAfterMs: 40 });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const ac = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, ac.signal);
    await waitFor(() => transport.subscribeCalls >= 1 && transport.stream && !transport.current().closed);
    await new Promise((resolve) => setTimeout(resolve, 60));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 1);
    const first = adapter.reconnectDelays[0]!;
    await waitFor(() => transport.subscribeCalls >= 2 && transport.stream && !transport.current().closed);
    await new Promise((resolve) => setTimeout(resolve, 60));
    transport.current().endRemote();
    await waitFor(() => adapter.reconnectDelays.length >= 2);
    expect(adapter.reconnectDelays[1]!).toBeGreaterThan(first * 0.8);
    ac.abort();
    await running;
  }, 15_000);
});

describe("N1 — real transport to Engine/MCP gap and resync", () => {
  it("malformed socket message settles the iterator, gaps, hides MCP book, then a fresh book resumes", async () => {
    ControllableWebSocket.instances = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: ControllableWebSocket as unknown as typeof WebSocket,
      pingIntervalMs: 1_000,
      staleMs: 5_000,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ POLYMARKET_TOKEN_IDS: "tok-up" }), journal, { error: () => undefined }, {
      market: adapter,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    await waitFor(() => ControllableWebSocket.instances.length >= 1);
    const socket = ControllableWebSocket.instances[0]!;
    socket.open();
    socket.message(JSON.stringify(liveBook("tok-up", 1_000)));
    await waitFor(() => adapter.isTokenSynchronized("tok-up") && engine.state.marketSnapshots.has("tok-up"));
    expect(mcpVisibleOrderbook(engine.state, "tok-up", (id) => engine.isTokenSynchronized(id))?.asks[0]?.price).toBe("0.48");
    socket.message("{broken");
    await waitFor(() => !adapter.isTokenSynchronized("tok-up"));
    expect(mcpVisibleOrderbook(engine.state, "tok-up", (id) => engine.isTokenSynchronized(id))).toBeNull();
    await waitFor(() => ControllableWebSocket.instances.length >= 2);
    const next = ControllableWebSocket.instances[1]!;
    next.open();
    next.message(JSON.stringify(liveBook("tok-up", 2_000, "11")));
    await waitFor(() => adapter.isTokenSynchronized("tok-up"));
    expect(mcpVisibleOrderbook(engine.state, "tok-up", (id) => engine.isTokenSynchronized(id))?.bids[0]?.quantity).toBe("11");
    next.message(JSON.stringify(liveDelta("tok-up", 2_001)));
    await waitFor(() => engine.state.marketSnapshots.get("tok-up")?.bids[0]?.quantity === "9");
    await engine.stop();
  }, 15_000);
});

describe("N2 — malformed multi-change has no maker fill or market trigger", () => {
  it("valid book then malformed multi-change gaps without a maker fill", async () => {
    const instance = new PolymarketMarketDataAdapter();
    instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
    instance.setTokenIds(["tok-up"]);
    const [first] = instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(first).toBeTruthy();
    expect(() => instance.normalizeSourceEvent({
      type: "price_change",
      payload: {
        timestamp: 1_001,
        priceChanges: [
          { tokenId: "tok-up", side: "BUY", price: "0.47", size: "1" },
          { tokenId: "tok-up", side: "ASKS", price: "0.48", size: "1" },
        ],
      },
    })).toThrow(MarketIngressMalformedError);
    expect(instance.bookState("tok-up")).toBeUndefined();
    expect(instance.normalizeSourceEvent(liveDelta("tok-up", 1_002))).toEqual([]);
  });
});
