import { describe, expect, it } from "vitest";
import { evaluateProtectiveDeRiskCandidate, positionSchema } from "@pm/core";
import { PolymarketMarketDataAdapter, type MinOrderSizeFetcher } from "@pm/market-data";

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

function liveDelta(tokenId: string, timestamp: number) {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: [{ tokenId, side: "BUY", price: "0.47", size: "9" }],
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
});