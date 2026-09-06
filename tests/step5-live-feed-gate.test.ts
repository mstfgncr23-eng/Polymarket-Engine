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

function reference(asset: string): ReferenceTick {
  const at = new Date().toISOString();
  return { eventId: `reference-${asset}`, traceId: `trace-${asset}`, asset, symbol: `${asset}USDT`, venue: "synthetic", price: "100", quantity: "1", sourceTimestamp: at, receivedAt: at, tradeId: at };
}

async function startRealFeedEngine(holdConnect = false) {
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
    { market: adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
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
});
