import { describe, expect, it } from "vitest";
import { evaluateProtectiveDeRiskCandidate, positionSchema } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { MarketIngressMalformedError, PolymarketMarketDataAdapter, ScriptedBoundedMarketTransport, type MinOrderSizeFetcher } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
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

function liveDelta(tokenId: string, timestamp: number, fields: Record<string, unknown> = {}) {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId, side: "BUY", price: "0.47", size: "9" }],
      ...fields,
    },
  };
}

function instanceWithFetcher(fetcher?: MinOrderSizeFetcher, options: { hydrateMinOrderSizeFromRest?: boolean } = {}) {
  const instance = fetcher
    ? new PolymarketMarketDataAdapter(undefined, { minOrderSizeFetcher: fetcher })
    : new PolymarketMarketDataAdapter(undefined, options);
  instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
  instance.registerManualToken("tok-down", "mkt", "DOWN", "BTC");
  instance.setTokenIds(["tok-up", "tok-down"]);
  return instance;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

describe("REST min_order_size hydration for trusted WS books that omit the field", () => {
  it("1: WS full book WITH min_order_size never invokes REST and stays authoritative", async () => {
    let calls = 0;
    const instance = instanceWithFetcher(async () => { calls += 1; return "bad"; });
    const first = instance.normalizeSourceEvent(liveBook("tok-up", 1_000, "5"));
    expect(first[0]?.minOrderSize).toBe("5");
    // price_change carries the WS-supplied value without any REST involvement.
    const delta = instance.normalizeSourceEvent(liveDelta("tok-up", 1_001));
    expect(delta[0]?.minOrderSize).toBe("5");
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
    // A later full book that still omits the field must not erase the value.
    const second = instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    expect(second[0]?.minOrderSize).toBe("3");
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBe("3");
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
    // The fetch is detached (one microtask behind ingest); wait for the attempt.
    await waitFor(() => calls === 1);
    // Repeated full books within the 60s backoff do not generate unbounded REST.
    for (let seq = 2; seq <= 6; seq += 1) {
      const snapshot = instance.normalizeSourceEvent(liveBook("tok-up", seq * 1_000));
      expect(snapshot[0]?.minOrderSize).toBeNull();
    }
    expect(calls).toBe(1);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();

    // The protective path fails safely with the precise reason.
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
      snapshot: {
        ...first[0]!,
        bids: [{ price: "0.90", quantity: "1585.55" }],
      },
      maxOrderSizeUsd: 25,
    });
    expect(evaluated.candidate).toBeNull();
    expect(evaluated.reason).toBe("MIN_ORDER_SIZE_UNKNOWN");
  });

  it("5a: stale REST hydration cannot apply across a reconnect/resync epoch change", async () => {
    let calls = 0;
    let pendingResolve: ((value: string | null) => void) | null = null;
    const instance = instanceWithFetcher(() => {
      calls += 1;
      return new Promise<string | null>((resolve) => { pendingResolve = resolve; });
    });
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => calls === 1 && pendingResolve != null);
    // Stream epoch advances (reconnect/resubscribe surrogate) while the REST
    // request is in flight; the token book is replaced under the new epoch.
    instance.bumpTokenEpochForTest("tok-up");
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    expect(calls).toBe(1); // pending: no duplicate REST request
    pendingResolve!("9");
    await new Promise((resolve) => setTimeout(resolve, 20));
    // Stale apply must be dropped: the new-epoch book still lacks the value.
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();

    // A fresh epoch request hydrates normally.
    instance.normalizeSourceEvent(liveBook("tok-up", 3_000));
    await waitFor(() => calls === 2 && pendingResolve != null);
    pendingResolve!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
    expect(instance.hydratedMinOrderSizeForTest("tok-down")).toBeNull();
  });

  it("5b: token replacement cannot leak a hydrated value onto a different token", async () => {
    const instance = instanceWithFetcher(async (tokenId) => (tokenId === "tok-up" ? "7" : null));
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "7");
    instance.normalizeSourceEvent(liveBook("tok-down", 1_000));
    await waitFor(() => instance.minOrderSizeFetcherCallCountForTest() >= 2);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(instance.hydratedMinOrderSizeForTest("tok-down")).toBeNull();

    // Remove tok-up from the managed set: its hydration bookkeeping dies with
    // the book and no value survives for a new token.
    instance.setTokenIds(["tok-up-new"]);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    const next = instance.normalizeSourceEvent(liveBook("tok-up-new", 1_000));
    expect(next).toHaveLength(0); // tok-up-new is not a registered token
  });

  it("6: production REST wiring derives the fetcher from client.fetchOrderBook, and absent fetchOrderBook disables it", async () => {
    const withBook = { fetchOrderBook: async () => ({ minOrderSize: "4" }) };
    const instance = new PolymarketMarketDataAdapter(withBook as never, { hydrateMinOrderSizeFromRest: true });
    instance.registerManualToken("tok-up", "mkt", "UP", "BTC");
    instance.setTokenIds(["tok-up"]);
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "4");

    const withoutBook = {};
    const disabled = new PolymarketMarketDataAdapter(withoutBook as never, { hydrateMinOrderSizeFromRest: true });
    disabled.registerManualToken("tok-up", "mkt", "UP", "BTC");
    disabled.setTokenIds(["tok-up"]);
    const first = disabled.normalizeSourceEvent(liveBook("tok-up", 1_000));
    expect(first[0]?.minOrderSize).toBeNull();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(disabled.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    expect(disabled.minOrderSizeFetcherCallCountForTest()).toBe(0);
  });
it("6: real malformed-delta invalidation race: stale P1 cannot apply to the resync book nor clear P2's pending registration", async () => {
    let calls = 0;
    const resolvers: Array<(value: string | null) => void> = [];
    const instance = instanceWithFetcher(() => {
      calls += 1;
      return new Promise<string | null>((resolve) => { resolvers.push(resolve); });
    });
    // P1 starts from an omit-book.
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => resolvers.length === 1);
    // Real invalidation path (not the test epoch hook): a malformed price_change
    // triggers failIncrementalTrust -> GAPPED + book deleted + hydration
    // invalidation, WITHOUT advancing the TokenSyncIndex epoch.
    expect(() => instance.normalizeSourceEvent(liveDelta("tok-up", 1_005, { tickSize: "nope" }))).toThrow(MarketIngressMalformedError);
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    // Resync: a fresh omit-book is committed under the same sync epoch and
    // starts P2 (generation bumped by the invalidation).
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    await waitFor(() => resolvers.length === 2 && calls === 2);
    // Resolve the STALE P1 first: it must not hydrate the new book...
    resolvers[0]!("99");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    // ...and must not clear P2's pending registration: another omit-book while
    // P2 is pending must NOT start P3.
    instance.normalizeSourceEvent(liveBook("tok-up", 3_000));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(calls).toBe(2);
    // Resolve P2: the authoritative value applies normally.
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
    expect(calls).toBe(2);
  });

  it("7: same-token removal/re-add starts a fresh generation that stale requests cannot touch", async () => {
    let calls = 0;
    const resolvers: Array<(value: string | null) => void> = [];
    const instance = instanceWithFetcher(() => {
      calls += 1;
      return new Promise<string | null>((resolve) => { resolvers.push(resolve); });
    });
    // P1 starts.
    instance.normalizeSourceEvent(liveBook("tok-up", 1_000));
    await waitFor(() => resolvers.length === 1);
    // Real replacement lifecycle: remove tok-up, then re-add the same id. The
    // sync epoch can reset to 1 (same as P1's captured epoch), so only the
    // monotonic hydration generation can discriminate staleness here.
    instance.setTokenIds(["tok-down"]);
    instance.setTokenIds(["tok-up", "tok-down"]);
    // A new omit-book after re-add starts P2 under the bumped generation.
    instance.normalizeSourceEvent(liveBook("tok-up", 2_000));
    await waitFor(() => resolvers.length === 2 && calls === 2);
    // Stale P1 resolves now: the generation fence must block the apply even
    // though token, epoch, and live book all match structurally.
    resolvers[0]!("99");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(instance.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
    // P2's registration survives (no P3 on another omit-book).
    instance.normalizeSourceEvent(liveBook("tok-up", 3_000));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(calls).toBe(2);
    // The fresh request resolves normally.
    resolvers[1]!("9");
    await waitFor(() => instance.hydratedMinOrderSizeForTest("tok-up") === "9");
    expect(instance.hydratedMinOrderSizeForTest("tok-down")).toBeNull();
  });
it("8: real run consumer path — hydration reaches onSnapshot with no extra WS event, and a new-epoch first omit-book starts P2 after reconnect", async () => {
    let calls = 0;
    const resolvers: Array<(value: string | null) => void> = [];
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transport,
      minOrderSizeFetcher: () => {
        calls += 1;
        return new Promise<string | null>((resolve) => { resolvers.push(resolve); });
      },
    });
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const captured: Array<{ tokenId: string; minOrderSize: string | null; sequence: number; sourceTimestamp: string }> = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: (snapshot) => {
        captured.push({ tokenId: snapshot.tokenId, minOrderSize: snapshot.minOrderSize, sequence: snapshot.sequence, sourceTimestamp: snapshot.sourceTimestamp });
      },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    try {
      await waitFor(() => adapter.activeStreamCount() === 1);
      // Initial omit-book is emitted to the consumer with minOrderSize=null; P1 starts.
      transport.push({ type: "book", payload: { tokenId: "tok-up", market: "mkt", timestamp: 1_000, bids: [{ price: "0.40", size: "1" }], asks: [{ price: "0.60", size: "3" }] } });
      await waitFor(() => captured.length === 1 && resolvers.length === 1);
      expect(captured[0]!.minOrderSize).toBeNull();

      // Reconnect / generation change while P1 is unresolved: beginStreamEpoch
      // must invalidate the pending hydration (blocking the quiet-stream hole).
      adapter.beginStreamEpoch(["tok-up"], "reconnect");
      expect(adapter.isTokenSynchronized("tok-up")).toBe(false);

      // First omit-book of the new epoch must start P2 IMMEDIATELY.
      transport.push({ type: "book", payload: { tokenId: "tok-up", market: "mkt", timestamp: 2_000, bids: [{ price: "0.40", size: "1" }], asks: [{ price: "0.60", size: "3" }] } });
      await waitFor(() => resolvers.length === 2 && calls === 2);

      // Stale P1 resolves first: no apply, no disturbance of P2.
      resolvers[0]!("99");
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(adapter.hydratedMinOrderSizeForTest("tok-up")).toBeNull();
      expect(calls).toBe(2);
      // Another omit-book while P2 is pending must NOT start P3.
      transport.push({ type: "book", payload: { tokenId: "tok-up", market: "mkt", timestamp: 3_000, bids: [{ price: "0.40", size: "1" }], asks: [{ price: "0.60", size: "3" }] } });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(calls).toBe(2);

      // P2 resolves: consumer-visible WITHOUT another WS event, same book identity.
      const identity = captured[captured.length - 1]!;
      resolvers[1]!("3");
      await waitFor(() => captured.some((entry) => entry.minOrderSize === "3"));
      const hydrated = captured[captured.length - 1]!;
      expect(hydrated.minOrderSize).toBe("3");
      expect(hydrated.tokenId).toBe("tok-up");
      expect(hydrated.sequence).toBe(identity.sequence);
      expect(hydrated.sourceTimestamp).toBe(identity.sourceTimestamp);
      expect(calls).toBe(2);
    } finally {
      controller.abort();
      await running.catch(() => undefined);
    }
  });

  it("9: engine consumer state observes the hydrated minOrderSize with no additional WS event", async () => {
    const market = qualified("btc-live", "BTC", "btc-live-up", "btc-live-down");
    const transport = new ScriptedBoundedMarketTransport();
    const byToken = new Map<string, (value: string | null) => void>();
    const adapter = new PolymarketMarketDataAdapter(undefined, {
      transport,
      minOrderSizeFetcher: (tokenId) => new Promise<string | null>((resolve) => { byToken.set(tokenId, resolve); }),
    });
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
      expect(engine.state.marketSnapshots.get("btc-live-up")?.minOrderSize).toBeNull();
      expect(engine.state.marketSnapshots.get("btc-live-down")?.minOrderSize).toBeNull();

      // REST resolves the UP token only; no additional WS event is pushed.
      byToken.get("btc-live-up")?.("3");
      await waitFor(() => engine.state.marketSnapshots.get("btc-live-up")?.minOrderSize === "3");
      const pair = (engine as unknown as { snapshots: Map<string, { UP?: { minOrderSize: string | null } }> }).snapshots.get("btc-live");
      expect(pair?.UP?.minOrderSize).toBe("3");
      expect(engine.state.marketSnapshots.get("btc-live-down")?.minOrderSize).toBeNull();
    } finally {
      await engine.stop();
    }
  });
});