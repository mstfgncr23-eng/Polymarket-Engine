import { describe, expect, it } from "vitest";
import { experimentAllowsFirstLeg, triggerEventSchema, type GrokActivationBridge, type GrokActivationRequest, type GrokActivationResult, type ReferenceTick } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { PolymarketMarketDataAdapter, ScriptedBoundedMarketTransport } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { CrashInjectedError } from "../apps/engine/src/crash-injection.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.js";
import { discoveryReport, qualified, ScriptedReferenceRegistry, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";

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

async function startRealFeedEngine(holdConnect = false, now?: () => Date, activation: GrokActivationBridge = new NoopGrokActivationBridge()) {
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
    { market: adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation, now },
  );
  await engine.start();
  return { engine, journal, adapter, transport, market, connectHold };
}

class CountingActivationBridge implements GrokActivationBridge {
  readonly provider = "counting";
  calls = 0;
  async activate(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    this.calls += 1;
    return request.targetRoles.map((targetRole) => ({
      provider: this.provider, ok: true, skipped: false, targetRole,
      requestedAt: request.createdAt, sentAt: request.createdAt, errorCode: null,
      httpStatus: null, grokWakeObserved: false as const, idempotencyKey: `${request.triggerId}:${targetRole}`,
    }));
  }
}

function researchTrigger(id: string): ReturnType<typeof triggerEventSchema.parse> {
  return triggerEventSchema.parse({
    id, traceId: `trace-${id}`, marketId: "btc-live", asset: "BTC", kind: "BOOK_IMBALANCE",
    severity: 1, score: 1, dedupeKey: id, evidence: { source: "pending-start-test" },
    sourceEventIds: [`source-${id}`], source: "system", createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
  });
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
  await waitFor(() => {
    const pair = (setup.engine as unknown as { snapshots: Map<string, { UP?: unknown; DOWN?: unknown }> }).snapshots.get(setup.market.id);
    return setup.engine.isMarketSynchronized(setup.market.id) && Boolean(pair?.UP && pair?.DOWN);
  });
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

async function holdDurableValidation(setup: Awaited<ReturnType<typeof startRealFeedEngine>>) {
  let release!: () => void;
  let entered = false;
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const original = setup.journal.commitDurableTransition.bind(setup.journal);
  setup.journal.commitDurableTransition = async (input, expected) => {
    if (input.type === "EXPERIMENT_START_VALIDATED") {
      entered = true;
      await hold;
    }
    return original(input, expected);
  };
  return { release, entered: () => entered, pending: setup.engine.beginExperiment() };
}

describe("Step 5 live-feed experiment start gate", () => {
  it("treats durable pending state as outside the canonical entry window", () => {
    const running = { status: "RUNNING" as const, startValidationPending: true } as never;
    expect(experimentAllowsFirstLeg(running)).toBe(false);
    expect(experimentAllowsFirstLeg({ status: "RUNNING" } as never)).toBe(true);
  });

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

  it("blocks synchronized markets whose UP/DOWN snapshots are stale", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const snapshots = (setup.engine as unknown as { snapshots: Map<string, { UP?: Record<string, unknown>; DOWN?: Record<string, unknown> }> }).snapshots;
      const pair = snapshots.get(setup.market.id);
      expect(pair?.UP).toBeTruthy();
      expect(pair?.DOWN).toBeTruthy();
      const staleAt = new Date(Date.now() - 60_000).toISOString();
      snapshots.set(setup.market.id, {
        UP: { ...pair!.UP, receivedAt: staleAt },
        DOWN: { ...pair!.DOWN, receivedAt: staleAt },
      });
      const result = await setup.engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toContain("MARKET_BOOK_NOT_TRUSTED");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(0);
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

  it("fails closed while discovery is reconciling at the start boundary", async () => {
    const setup = await startRealFeedEngine();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    try {
      const original = setup.adapter.discoverCryptoMarkets.bind(setup.adapter);
      setup.adapter.discoverCryptoMarkets = async (...args) => {
        await hold;
        return original(...args);
      };
      const reconciling = setup.engine.reconcileDiscovery();
      await waitFor(() => setup.engine.state.discoveryStatus === "RECONCILING");
      const result = await setup.engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toContain("DISCOVERY_RECONCILING");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(0);
      release();
      await reconciling;
    } finally {
      release?.();
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
      expect(setup.engine.experimentManifest()?.status).toBe("ABORTED_START");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(1);
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
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableStart(setup);
      await waitFor(() => held.entered());
      // Mark the required reference feed stale while the validation commit is
      // held; market books remain fresh, isolating the reference race.
      await setup.engine.applyReferenceConnectionForTest("BTC", "STALE");
      held.release();
      const rejected = await held.pending;
      expect(rejected.ok).toBe(false);
      expect(rejected.blockers).toContain("REFERENCE_FEED_STALE");
      expect(rejected.blockers).toContain("EXPERIMENT_FEED_INVALIDATED_DURING_START");
      expect(setup.engine.experimentManifest()?.status).toBe("ABORTED_START");
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
      expect(setup.engine.experimentManifest()?.status).toBe("ABORTED_START");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(1);
    } finally {
      await setup.engine.stop();
    }
  });

  it("fails closed when feed truth changes before the final START_VALIDATED beforeCommit", async () => {
    const setup = await startRealFeedEngine();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let validationLookupComplete = false;
    const originalRead = setup.journal.readDurableByOperationKey.bind(setup.journal);
    setup.journal.readDurableByOperationKey = async (operationKey) => {
      const existing = await originalRead(operationKey);
      if (operationKey.endsWith(":start_validated")) {
        // The durable payload/idempotency lookup has completed, but the
        // commit has not reached its final beforeCommit guard yet.
        validationLookupComplete = true;
        await hold;
      }
      return existing;
    };
    try {
      await establishTrustedBaseline(setup);
      const pending = setup.engine.beginExperiment();
      await waitFor(() => validationLookupComplete);
      expect(setup.engine.experimentManifest()?.status).toBe("RUNNING");
      expect(setup.engine.experimentManifest()?.startValidationPending).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(0);

      await setup.engine.applyReferenceConnectionForTest("BTC", "STALE");
      release();
      const result = await pending;

      expect(result.ok).toBe(false);
      expect(result.blockers).toContain("REFERENCE_FEED_STALE");
      expect(result.blockers).toContain("EXPERIMENT_FEED_INVALIDATED_DURING_START");
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(0);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(1);
      expect(setup.engine.experimentManifest()?.status).toBe("ABORTED_START");
      expect(setup.engine.experimentManifest()?.startValidationPending).toBe(false);
      expect((await setup.engine.beginExperiment()).ok).toBe(false);
    } finally {
      release();
      await setup.engine.stop();
    }
  });

  it("treats disconnect after the validation linearization point as runtime feed loss", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableValidation(setup);
      await waitFor(() => held.entered());
      setup.transport.current().endRemote();
      await waitFor(() => setup.engine.state.connections.polymarket.state === "DISCONNECTED", 4_000);
      held.release();
      const result = await held.pending;
      expect(result.ok).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(0);
      expect(setup.engine.experimentManifest()?.status).toBe("RUNNING");
      expect(setup.engine.experimentFeedBlockersForTest()).toContain("MARKET_FEED_NOT_READY");
    } finally {
      await setup.engine.stop();
    }
  });

  it("treats discovery generation changes after validation as runtime feed loss", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableValidation(setup);
      await waitFor(() => held.entered());
      const replacement = qualified("btc-validation-replacement", "BTC", "btc-validation-up", "btc-validation-down");
      setup.adapter.registerManualToken("btc-validation-up", replacement.id, "UP", "BTC");
      setup.adapter.registerManualToken("btc-validation-down", replacement.id, "DOWN", "BTC");
      (setup.adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<ReturnType<typeof discoveryReport>> }).discoverCryptoMarkets = async () => discoveryReport([replacement]);
      await setup.engine.reconcileDiscovery();
      held.release();
      const result = await held.pending;
      expect(result.ok).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(0);
      expect(setup.engine.experimentManifest()?.status).toBe("RUNNING");
      expect(setup.engine.experimentFeedBlockersForTest()).toContain("MARKET_BOOK_NOT_TRUSTED");
    } finally {
      await setup.engine.stop();
    }
  });

  it("treats reference invalidation after validation as runtime feed loss", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableValidation(setup);
      await waitFor(() => held.entered());
      await setup.engine.applyReferenceConnectionForTest("BTC", "STALE");
      held.release();
      const result = await held.pending;
      expect(result.ok).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(0);
      expect(setup.engine.experimentManifest()?.status).toBe("RUNNING");
      expect(setup.engine.experimentFeedBlockersForTest()).toContain("REFERENCE_FEED_STALE");
    } finally {
      await setup.engine.stop();
    }
  });

  it("recovers a validation-linearized start safely after a crash at the runtime-admission boundary", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const original = setup.journal.commitDurableTransition.bind(setup.journal);
      setup.journal.commitDurableTransition = async (input, expected) => {
        const result = await original(input, expected);
        if (input.type === "EXPERIMENT_START_VALIDATED") {
          // Invalidation occurs after the durable validation LP. The crash
          // hook fires at the boundary where the old compensating abort would
          // have run; recovery must use ordinary feed-loss admission.
          await setup.engine.applyReferenceConnectionForTest("BTC", "STALE");
        }
        return result;
      };
      setup.engine.setCrashInjectionForTest("AFTER_START_VALIDATED_BEFORE_RUNTIME_ADMISSION");
      await expect(setup.engine.beginExperiment()).rejects.toBeInstanceOf(CrashInjectedError);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(0);
      await setup.engine.stop().catch(() => undefined);
      setup.journal.loadLatestCheckpoint = async () => null;
      const recovered = new EngineService(
        qualifiedPaperEnv(), setup.journal, { error: () => undefined },
        { market: setup.adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
      );
      await recovered.start();
      try {
        expect(recovered.experimentManifest()?.status).toBe("RUNNING");
        expect(recovered.experimentManifest()?.startValidationPending).toBe(false);
        expect(recovered.experimentFeedBlockersForTest().length).toBeGreaterThan(0);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(0);
      } finally {
        await recovered.stop();
      }
    } finally {
      if (setup.engine.lastShutdownResult() === null) await setup.engine.stop();
    }
  });

  it("recovers a committed but unvalidated start as an aborted terminal identity", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      setup.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
      await expect(setup.engine.beginExperiment()).rejects.toBeInstanceOf(CrashInjectedError);
      await setup.engine.stop();
      const recovered = new EngineService(
        qualifiedPaperEnv(), setup.journal, { error: () => undefined },
        { market: setup.adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
      );
      await recovered.start();
      try {
        expect(recovered.experimentManifest()?.status).toBe("ABORTED_START");
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_ABORTED"], limit: 8 })).toHaveLength(1);
        await waitFor(() => setup.transport.streams.length >= 2 && recovered.state.connections.polymarket.state === "CONNECTED", 4_000);
        await establishTrustedBaseline({ ...setup, engine: recovered });
        const retry = await recovered.beginExperiment();
        expect(retry.ok).toBe(true);
        const starts = await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 });
        expect(starts).toHaveLength(2);
        const lifecycle = await import("../apps/engine/src/cleanup-lifecycle.js").then(({ resolveCleanupLifecycle }) => resolveCleanupLifecycle(setup.journal));
        expect(lifecycle.state).toBe("ACTIVE_RUNNING");
        expect(lifecycle.experimentId).toBe(recovered.experimentManifest()?.id);
      } finally {
        await recovered.stop();
      }
    } finally {
      if (setup.engine.lastShutdownResult() === null) await setup.engine.stop();
    }
  });

  it("recovers a validated start from durable transitions after total checkpoint loss", async () => {
    const setup = await startRealFeedEngine();
    await establishTrustedBaseline(setup);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      await setup.engine.stop();
      setup.journal.loadLatestCheckpoint = async () => null;
      const recovered = new EngineService(
        qualifiedPaperEnv(), setup.journal, { error: () => undefined },
        { market: setup.adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
      );
      await recovered.start();
      try {
        expect(recovered.experimentManifest()?.status).toBe("RUNNING");
        expect(recovered.experimentManifest()?.startValidationPending).toBe(false);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      } finally {
        await recovered.stop();
      }
    } finally {
      if (setup.engine.lastShutdownResult() === null) await setup.engine.stop();
    }
  });

  it("reports checkpoint degradation after validated start while preserving durable authority", async () => {
    const setup = await startRealFeedEngine();
    const originalSave = setup.journal.saveCheckpoint.bind(setup.journal);
    try {
      await establishTrustedBaseline(setup);
      setup.journal.saveCheckpoint = async () => { throw new Error("CHECKPOINT_WRITE_FAILED"); };
      const result = await setup.engine.beginExperiment();
      expect(result).toMatchObject({ ok: true, checkpoint: "degraded", warnings: ["CHECKPOINT_DEGRADED"] });
      expect(setup.engine.state.journalDegraded).toBe(true);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      expect(await setup.engine.beginExperiment()).toMatchObject({ ok: false, blockers: ["EXPERIMENT_ALREADY_ACTIVE"] });
      setup.journal.saveCheckpoint = originalSave;
      await setup.engine.stop();
      setup.journal.loadLatestCheckpoint = async () => null;
      const recovered = new EngineService(
        qualifiedPaperEnv(), setup.journal, { error: () => undefined },
        { market: setup.adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
      );
      await recovered.start();
      try {
        expect(recovered.experimentManifest()?.status).toBe("RUNNING");
        expect(recovered.experimentManifest()?.startValidationPending).toBe(false);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      } finally {
        await recovered.stop();
      }
    } finally {
      setup.journal.saveCheckpoint = originalSave;
      if (setup.engine.lastShutdownResult() === null) await setup.engine.stop();
    }
  });

  it("recovers validated state after a crash before its checkpoint", async () => {
    const setup = await startRealFeedEngine();
    await establishTrustedBaseline(setup);
    try {
      const original = setup.journal.commitDurableTransition.bind(setup.journal);
      let validatedKey = "";
      setup.journal.commitDurableTransition = async (input, expected) => {
        if (input.type === "EXPERIMENT_START_VALIDATED") {
          validatedKey = input.operationKey;
          setup.engine.setCrashInjectionForTest("AFTER_DERIVED_AUDIT_BEFORE_CHECKPOINT", { operationKey: input.operationKey });
        }
        return original(input, expected);
      };
      await expect(setup.engine.beginExperiment()).rejects.toBeInstanceOf(CrashInjectedError);
      expect(validatedKey).toContain(":start_validated");
      setup.journal.loadLatestCheckpoint = async () => null;
      await setup.engine.stop().catch(() => undefined);
      const recovered = new EngineService(
        qualifiedPaperEnv(), setup.journal, { error: () => undefined },
        { market: setup.adapter, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() },
      );
      await recovered.start();
      try {
        expect(recovered.experimentManifest()?.status).toBe("RUNNING");
        expect(recovered.experimentManifest()?.startValidationPending).toBe(false);
        expect(await setup.journal.queryDurableTransitions({ types: ["EXPERIMENT_START_VALIDATED"], limit: 8 })).toHaveLength(1);
      } finally {
        await recovered.stop();
      }
    } finally {
      if (setup.engine.lastShutdownResult() === null) await setup.engine.stop();
    }
  });

  it("does not let maintenance auto-complete an aborted start before retry", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      const held = await holdDurableStart(setup);
      await waitFor(() => held.entered());
      setup.transport.current().endRemote();
      await waitFor(() => setup.adapter.unsynchronizedTokenCount() > 0, 3_000);
      held.release();
      await held.pending;
      await setup.engine.tickMaintenance();
      expect(setup.engine.experimentManifest()?.status).toBe("ABORTED_START");
      expect((await setup.engine.beginExperiment()).ok).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("fences first-leg fills while the post-commit start validation is pending", async () => {
    const setup = await startRealFeedEngine();
    try {
      await establishTrustedBaseline(setup);
      let release!: () => void;
      const hold = new Promise<void>((resolve) => { release = resolve; });
      setup.engine.setStartPostCommitValidationHoldForTest(hold);
      const pending = setup.engine.beginExperiment();
      await waitFor(() => setup.engine.experimentManifest()?.status === "RUNNING");
      const fill = paperFill(setup.market.id, "UP", "0.40", { id: "pending-first-leg" });
      await setup.engine.applyObservedFill(fill);
      expect(setup.engine.state.snapshot().positions).toHaveLength(0);
      expect(await setup.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 8 })).toHaveLength(0);
      release();
      expect((await pending).ok).toBe(true);
      await setup.engine.applyObservedFill(fill);
      expect(setup.engine.state.snapshot().positions).toHaveLength(1);
    } finally {
      setup.engine.setStartPostCommitValidationHoldForTest(null);
      await setup.engine.stop();
    }
  });

  it("does not dispatch activation from a trigger while start validation is pending", async () => {
    const activation = new CountingActivationBridge();
    const setup = await startRealFeedEngine(false, undefined, activation);
    try {
      await establishTrustedBaseline(setup);
      await waitFor(() => setup.engine.runtimeSupervisorSnapshot().unresolved === 0, 5_000);
      activation.calls = 0;
      let release!: () => void;
      const hold = new Promise<void>((resolve) => { release = resolve; });
      setup.engine.setStartPostCommitValidationHoldForTest(hold);
      const pending = setup.engine.beginExperiment();
      await waitFor(() => setup.engine.experimentManifest()?.status === "RUNNING");
      expect((setup.engine as unknown as { isExperimentWindowOpen: () => boolean }).isExperimentWindowOpen()).toBe(false);
      expect((setup.engine as unknown as { isCanonicalWindowAdmission: (type: string) => boolean }).isCanonicalWindowAdmission("TRIGGER_EVENT")).toBe(false);
      const pendingCalls = activation.calls;
      await setup.engine.registerTriggerForResearch(researchTrigger("pending-activation"));
      expect(activation.calls).toBe(pendingCalls);
      release();
      expect((await pending).ok).toBe(true);
      expect((setup.engine as unknown as { isExperimentWindowOpen: () => boolean }).isExperimentWindowOpen()).toBe(true);
      expect((setup.engine as unknown as { isCanonicalWindowAdmission: (type: string) => boolean }).isCanonicalWindowAdmission("TRIGGER_EVENT")).toBe(true);
      await setup.engine.registerTriggerForResearch(researchTrigger("validated-activation"));
      await waitFor(() => activation.calls > 0);
      expect(activation.calls).toBeGreaterThan(0);
    } finally {
      setup.engine.setStartPostCommitValidationHoldForTest(null);
      await setup.engine.stop();
    }
  });
});
