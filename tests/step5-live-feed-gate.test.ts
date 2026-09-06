import { describe, expect, it } from "vitest";
import { type ReferenceTick } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { PolymarketMarketDataAdapter, ScriptedBoundedMarketTransport } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.js";
import { discoveryReport, qualified, ScriptedReferenceRegistry, qualifiedPaperEnv } from "./paper-engine-testkit.js";

const waitFor = async (predicate: () => boolean, timeoutMs = 2_000): Promise<void> => {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

function reference(asset: string, at = new Date().toISOString()): ReferenceTick {
  return { eventId: `reference-${asset}`, traceId: `trace-${asset}`, asset, symbol: `${asset}USDT`, venue: "synthetic", price: "100", quantity: "1", sourceTimestamp: at, receivedAt: at, tradeId: at };
}

async function startRealFeedEngine(holdConnect = false, now?: () => Date) {
  const market = qualified("btc-live", "BTC", "btc-live-up", "btc-live-down");
  const transport = new ScriptedBoundedMarketTransport();
  const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
  adapter.registerManualToken("btc-live-up", market.id, "UP", "BTC");
  adapter.registerManualToken("btc-live-down", market.id, "DOWN", "BTC");
  (adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<ReturnType<typeof discoveryReport>> }).discoverCryptoMarkets = async () => discoveryReport([market]);
  const connectHold = holdConnect ? transport.holdNextSubscribe() : null;
  const journal = new MemoryEventJournal();
  const engine = new EngineService(
    qualifiedPaperEnv(), journal, { error: () => undefined },
    { market: adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(), now },
  );
  await engine.start();
  return { engine, journal, adapter, transport, market, connectHold };
}

function bookEvent(tokenId: string, marketId: string, timestamp = Date.now()): Record<string, unknown> {
  return {
    type: "book", payload: {
      tokenId, market: marketId, timestamp,
      bids: [{ price: "0.47", size: "10" }], asks: [{ price: "0.48", size: "10" }], lastTradePrice: "0.48",
    },
  };
}

async function establishTrustedBaseline(
  setup: Awaited<ReturnType<typeof startRealFeedEngine>>,
  at = new Date().toISOString(),
): Promise<void> {
  await waitFor(() => setup.transport.streams.length >= 1 && setup.engine.state.connections.polymarket.state === "CONNECTED");
  setup.transport.push(bookEvent("btc-live-up", setup.market.id));
  setup.transport.push(bookEvent("btc-live-down", setup.market.id, Date.now() + 1));
  await waitFor(() => setup.engine.isMarketSynchronized(setup.market.id));
  await setup.engine.ingestReferenceTickForTest(reference("BTC", at));
}

async function holdDurableStart(setup: Awaited<ReturnType<typeof startRealFeedEngine>>) {
  let release!: () => void;
  let entered = false;
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const original = setup.journal.commitDurableTransition.bind(setup.journal);
  setup.journal.commitDurableTransition = async (input, expected) => {
    if (input.type === "EXPERIMENT_STARTED") {
      entered = true;
      await hold;
    }
    return original(input, expected);
  };
  return { release, entered: () => entered, pending: setup.engine.beginExperiment() };
}

describe("Step 5 live-feed experiment start gate", () => {
  it("rejects a real-adapter start while the market stream is disconnected", async () => {
    const setup = await startRealFeedEngine(true);
    try {
      await waitFor(() => setup.engine.state.connections.polymarket.state === "CONNECTING");
      const result = await setup.engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toContain("MARKET_FEED_NOT_READY");
      expect(await setup.journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).toHaveLength(0);
    } finally {
      setup.connectHold?.release();
      await setup.engine.stop();
    }
  });

  it("requires trusted books and a fresh required reference before committing", async () => {
    const setup = await startRealFeedEngine();
    try {
      await waitFor(() => setup.transport.streams.length === 1 && setup.engine.state.connections.polymarket.state === "CONNECTED");
      const withoutBook = await setup.engine.beginExperiment();
      expect(withoutBook.ok).toBe(false);
      expect(withoutBook.blockers).toContain("MARKET_BOOK_NOT_TRUSTED");
      const now = Date.now();
      setup.transport.push(bookEvent("btc-live-up", setup.market.id, now));
      setup.transport.push(bookEvent("btc-live-down", setup.market.id, now + 1));
      await waitFor(() => setup.engine.isMarketSynchronized(setup.market.id));
      const withoutReference = await setup.engine.beginExperiment();
      expect(withoutReference.ok).toBe(false);
      expect(withoutReference.blockers).toContain("REFERENCE_FEED_STALE");
      await setup.engine.ingestReferenceTickForTest(reference("BTC"));
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      expect(await setup.journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).toHaveLength(1);
    } finally {
      await setup.engine.stop();
    }
  });

  it("does not commit after a feed gap is observed by the authoritative guard", async () => {
    const setup = await startRealFeedEngine();
    try {
      await waitFor(() => setup.transport.streams.length === 1 && setup.engine.state.connections.polymarket.state === "CONNECTED");
      const now = Date.now();
      setup.transport.push(bookEvent("btc-live-up", setup.market.id, now));
      setup.transport.push(bookEvent("btc-live-down", setup.market.id, now + 1));
      await waitFor(() => setup.engine.isMarketSynchronized(setup.market.id));
      await setup.engine.ingestReferenceTickForTest(reference("BTC"));
      let release!: () => void;
      const preflightHold = new Promise<void>((resolve) => { release = resolve; });
      setup.journal.setAppendAcquireHoldForTest(preflightHold, ["EXPERIMENT_PREFLIGHT"]);
      const pending = setup.engine.beginExperiment();
      await waitFor(() => setup.journal.inFlightAppends > 0);
      setup.transport.current().endRemote();
      await waitFor(() => setup.adapter.unsynchronizedTokenCount() > 0, 3_000);
      release();
      const result = await pending;
      expect(result.ok).toBe(false);
      expect(await setup.journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).toHaveLength(0);
    } finally {
      setup.journal.setAppendAcquireHoldForTest(null);
      await setup.engine.stop();
    }
  });

  it("fail-closes a post-guard market invalidation and permits a fresh retry", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableStart(setup);
      await waitFor(() => held.entered());
      setup.transport.current().endRemote();
      await waitFor(() => setup.adapter.unsynchronizedTokenCount() > 0, 3_000);
      await waitFor(() => setup.engine.state.connections.polymarket.state === "DISCONNECTED", 4_000);
      held.release();
      const rejected = await held.pending;
      expect(rejected.ok).toBe(false);
      expect(rejected.blockers).toContain("EXPERIMENT_FEED_INVALIDATED_DURING_START");
      expect(setup.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 })).toHaveLength(1);
      await waitFor(() => setup.transport.streams.length >= 2 && setup.engine.state.connections.polymarket.state === "CONNECTED", 4_000);
      await establishTrustedBaseline(setup);
      const retried = await setup.engine.beginExperiment();
      expect(retried.ok).toBe(true);
      const starts = await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 });
      expect(starts).toHaveLength(2);
      expect(new Set(starts.map((row) => row.aggregateId)).size).toBe(2);
      expect(setup.engine.experimentManifest()?.status).toBe("RUNNING");
    } finally {
      await setup.engine.stop();
    }
  });

  it("closes a start when required reference freshness expires during durable commit", async () => {
    let nowMs = Date.now();
    const setup = await startRealFeedEngine(false, () => new Date(nowMs));
    try {
      const baseline = new Date(nowMs).toISOString();
      await establishTrustedBaseline(setup, baseline);
      const held = await holdDurableStart(setup);
      await waitFor(() => held.entered());
      nowMs += 10 * 60_000;
      held.release();
      const rejected = await held.pending;
      expect(rejected.ok).toBe(false);
      expect(rejected.blockers).toContain("REFERENCE_FEED_STALE");
      expect(rejected.blockers).toContain("EXPERIMENT_FEED_INVALIDATED_DURING_START");
      expect(setup.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
    } finally {
      await setup.engine.stop();
    }
  });

  it("closes a start when discovery changes the token/source generation during durable commit", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableStart(setup);
      await waitFor(() => held.entered());
      const replacement = qualified("btc-live-replacement", "BTC", "btc-replacement-up", "btc-replacement-down");
      setup.adapter.registerManualToken("btc-replacement-up", replacement.id, "UP", "BTC");
      setup.adapter.registerManualToken("btc-replacement-down", replacement.id, "DOWN", "BTC");
      (setup.adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<ReturnType<typeof discoveryReport>> }).discoverCryptoMarkets = async () => discoveryReport([replacement]);
      await setup.engine.reconcileDiscovery();
      held.release();
      const rejected = await held.pending;
      expect(rejected.ok).toBe(false);
      expect(rejected.blockers).toContain("EXPERIMENT_FEED_INVALIDATED_DURING_START");
      expect(setup.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 })).toHaveLength(1);
    } finally {
      await setup.engine.stop();
    }
  });
});
