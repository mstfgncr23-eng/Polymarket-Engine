import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
  createId,
  type MarketSnapshot,
  type ReferenceTick,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";
import { feeSchedule, snapshot as baseSnapshot } from "./fixtures.js";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function learningDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-engine-cf-"));
  dirs.push(dir);
  return dir;
}

type Clock = { nowMs: number };

async function waitFor(predicate: () => boolean, timeoutMs = 8_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function drainRuntime(engine: EngineService): Promise<void> {
  await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0, 15_000);
}

function iso(clock: Clock): string {
  return new Date(clock.nowMs).toISOString();
}

function zeroFee() {
  return { ...feeSchedule, rate: "0", rebateRate: "0" };
}

function book(clock: Clock, marketId: string, outcome: "UP" | "DOWN", sequence: number): MarketSnapshot {
  const at = iso(clock);
  // Cheap asks (pair cost << 1) + strong UP bid imbalance for actionable shadow edge / confidence.
  if (outcome === "UP") {
    return baseSnapshot({
      eventId: createId("book"),
      traceId: createId("trace"),
      marketId,
      tokenId: `${marketId}-up`,
      outcome: "UP",
      asset: "BTC",
      bids: [{ price: "0.38", quantity: "5000" }, { price: "0.37", quantity: "5000" }],
      asks: [{ price: "0.40", quantity: "500" }, { price: "0.41", quantity: "500" }],
      feeSchedule: zeroFee(),
      sourceTimestamp: at,
      receivedAt: at,
      sequence,
      hash: null,
    });
  }
  return baseSnapshot({
    eventId: createId("book"),
    traceId: createId("trace"),
    marketId,
    tokenId: `${marketId}-down`,
    outcome: "DOWN",
    asset: "BTC",
    bids: [{ price: "0.38", quantity: "10" }],
    asks: [{ price: "0.40", quantity: "500" }, { price: "0.41", quantity: "500" }],
    feeSchedule: zeroFee(),
    sourceTimestamp: at,
    receivedAt: at,
    sequence,
    hash: null,
  });
}

function referenceTick(clock: Clock, price: string): ReferenceTick {
  return {
    eventId: createId("reference"),
    traceId: createId("trace"),
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "deterministic-test",
    price,
    quantity: "1",
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    tradeId: createId("trade"),
  };
}

describe("EngineService PR2 counterfactual integration", () => {
  it("real V2 shadow path advances CF, observes state, settles after restart without active discovery", async () => {
    const learningDataDir = learningDir();
    const START_MS = Date.parse("2026-09-17T08:00:00.000Z");
    const END_MS = Date.parse("2026-09-17T08:15:00.000Z");
    const clock: Clock = { nowMs: START_MS };
    const resolutions = new Map<string, Record<string, unknown>>();
    const marketId = "btc-mkt";
    const marketDef = qualified(
      marketId,
      "BTC",
      `${marketId}-up`,
      `${marketId}-down`,
      new Date(END_MS).toISOString(),
    );
    // Persist inverted mapping so restart cannot silently fall back to UP/DOWN defaults.
    marketDef.qualification.binary.yesOutcome = "DOWN";
    marketDef.qualification.binary.noOutcome = "UP";

    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([marketDef]));
    const journal = new MemoryEventJournal();
    const env = qualifiedPaperEnv({
      LEARNING_DATA_DIR: learningDataDir,
      LEARNING_ENABLED: "true",
      PAPER_START_BALANCE: "1000",
      PAPER_EXECUTION_STYLE: "TAKER",
      PAPER_EXECUTION_LATENCY_MS: "0",
      EXPERIMENT_DURATION_HOURS: "1",
      ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
      SETTLEMENT_POLL_MS: "1",
      LEARNING_SETTLEMENT_POLL_MS: "1",
      LEARNING_CHECKPOINT_SECONDS: "900,600,300",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(clock.nowMs),
      resolutionFetcher: async (id) => resolutions.get(id) ?? null,
    });

    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);

      await engine.ingestReferenceTickForTest(referenceTick(clock, "100000"));
      clock.nowMs += 1_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100500"));

      await market.emitSnapshot(book(clock, marketId, "UP", 1));
      await market.emitSnapshot(book(clock, marketId, "DOWN", 1));
      await drainRuntime(engine);
      // Shadow execution is dispatched via setImmediate.
      await waitFor(() => engine.counterfactualSummary().fillCount >= 1, 10_000);
      await drainRuntime(engine);

      const summaryAfterFirst = engine.counterfactualSummary();
      expect(summaryAfterFirst.fillCount).toBeGreaterThanOrEqual(1);
      expect(summaryAfterFirst.recoveryReady).toBe(true);
      expect(Number(summaryAfterFirst.availableCashUsd)).toBeLessThan(1000);

      const cf = (engine as unknown as { counterfactualStore: {
        decisionState: () => { recoveryReady: boolean; stateBasis: string; positions: unknown[]; availableCashUsd: string };
        hasUnpairedPosition: (id: string) => boolean;
        settlementLookupFor: (id: string) => {
          marketId: string; slug: string | null; endDate: string; yesMapsTo: string; noMapsTo: string;
        } | null;
        openPositionMarketIds: () => string[];
        hasSettledMarket: (id: string) => boolean;
      } }).counterfactualStore;

      const decision = cf.decisionState();
      expect(decision.stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
      expect(decision.recoveryReady).toBe(true);
      expect(decision.positions.length).toBeGreaterThanOrEqual(1);
      expect(cf.hasUnpairedPosition(marketId)).toBe(true);
      const lookup = cf.settlementLookupFor(marketId);
      expect(lookup).not.toBeNull();
      expect(lookup?.yesMapsTo).toBe("DOWN");
      expect(lookup?.noMapsTo).toBe("UP");
      expect(lookup?.endDate).toBe(new Date(END_MS).toISOString());

      // Isolate V1 after CF durable advance: subsequent CF observation must not mutate V1.
      const v1AfterCf = engine.ledger.snapshot();
      const v1CashAfterCf = engine.ledger.availableCash().toFixed();
      const v1PosAfterCf = engine.ledger.positions.size;

      // Next evaluation observes CF state (unpaired / duplicate) without a second CF fill.
      clock.nowMs += 2_000;
      await market.emitSnapshot(book(clock, marketId, "UP", 2));
      await market.emitSnapshot(book(clock, marketId, "DOWN", 2));
      await drainRuntime(engine);
      await new Promise((r) => setImmediate(r));
      await new Promise((r) => setImmediate(r));
      expect(engine.counterfactualSummary().fillCount).toBe(summaryAfterFirst.fillCount);
      expect(cf.hasUnpairedPosition(marketId)).toBe(true);
      expect(engine.ledger.snapshot()).toEqual(v1AfterCf);
      expect(engine.ledger.availableCash().toFixed()).toBe(v1CashAfterCf);
      expect(engine.ledger.positions.size).toBe(v1PosAfterCf);
    } finally {
      await engine.stop();
    }

    // Restart with market ABSENT from active discovery — durable lookup must settle CF.
    clock.nowMs = END_MS + 60_000;
    resolutions.set(marketId, {
      closed: true,
      umaResolutionStatus: "resolved",
      outcomes: ["Yes", "No"],
      outcomePrices: ["1", "0"],
    });
    // yesMapsTo=DOWN, noMapsTo=UP, Yes wins → DOWN wins → upPayoff=0, downPayoff=1

    const restarted = new EngineService(
      qualifiedPaperEnv({
        LEARNING_DATA_DIR: learningDataDir,
        LEARNING_ENABLED: "true",
        PAPER_START_BALANCE: "1000",
        PAPER_EXECUTION_STYLE: "TAKER",
        SETTLEMENT_POLL_MS: "1",
        LEARNING_SETTLEMENT_POLL_MS: "1",
      }),
      new MemoryEventJournal(),
      { error: () => undefined },
      {
        market: (() => {
          const m = new ScriptedMarketAdapter();
          // Empty discovery — market not in active set.
          m.queue(discoveryReport([]));
          return m as never;
        })(),
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(clock.nowMs),
        resolutionFetcher: async (id) => resolutions.get(id) ?? null,
      },
    );

    try {
      await restarted.start();
      const cfSummary = restarted.counterfactualSummary();
      expect(cfSummary.recoveryReady).toBe(true);
      expect(cfSummary.fillCount).toBeGreaterThanOrEqual(1);
      expect(
        (restarted as unknown as { counterfactualStore: { openPositionMarketIds: () => string[] } })
          .counterfactualStore.openPositionMarketIds().length,
      ).toBeGreaterThanOrEqual(1);

      // Drive official public resolution polling (same path as maintenance).
      for (let i = 0; i < 5; i += 1) {
        await restarted.tickMaintenance();
        if (restarted.counterfactualSummary().settlementCount >= 1) break;
        clock.nowMs += 1_000;
      }
      expect(restarted.counterfactualSummary().settlementCount).toBeGreaterThanOrEqual(1);
      const cf2 = (restarted as unknown as { counterfactualStore: {
        hasSettledMarket: (id: string) => boolean;
        openPositionMarketIds: () => string[];
        summary: () => { availableCashUsd: string; realizedPnl: string; fees: string; fillCount: number; settlementCount: number };
      } }).counterfactualStore;
      expect(cf2.hasSettledMarket(marketId)).toBe(true);
      expect(cf2.openPositionMarketIds()).toEqual([]);
      expect(restarted.ledger.positions.size).toBe(0);
      expect(restarted.ledger.availableCash().toFixed()).toBe("1000");

      const settledSnapshot = {
        availableCashUsd: cf2.summary().availableCashUsd,
        realizedPnl: cf2.summary().realizedPnl,
        fees: cf2.summary().fees,
        fillCount: cf2.summary().fillCount,
        settlementCount: cf2.summary().settlementCount,
      };

      await restarted.stop();

      // Second restart: replay equivalent.
      const replay = new EngineService(
        qualifiedPaperEnv({
          LEARNING_DATA_DIR: learningDataDir,
          PAPER_START_BALANCE: "1000",
        }),
        new MemoryEventJournal(),
        { error: () => undefined },
        {
          market: (() => {
            const m = new ScriptedMarketAdapter();
            m.queue(discoveryReport([]));
            return m as never;
          })(),
          referenceRegistry: new ScriptedReferenceRegistry() as never,
          activation: new NoopGrokActivationBridge(),
          now: () => new Date(clock.nowMs),
          resolutionFetcher: async () => null,
        },
      );
      try {
        await replay.start();
        const summary = replay.counterfactualSummary();
        expect(summary.recoveryReady).toBe(true);
        expect(summary.fillCount).toBe(settledSnapshot.fillCount);
        expect(summary.settlementCount).toBe(settledSnapshot.settlementCount);
        expect(summary.availableCashUsd).toBe(settledSnapshot.availableCashUsd);
        expect(summary.realizedPnl).toBe(settledSnapshot.realizedPnl);
        expect(summary.fees).toBe(settledSnapshot.fees);
        expect(replay.ledger.availableCash().toFixed()).toBe("1000");
      } finally {
        await replay.stop();
      }
    } finally {
      try { await restarted.stop(); } catch { /* already stopped */ }
    }
  });
  it("discovery refresh before shadow eval keeps captured inverted settlementLookup", async () => {
    const learningDataDir = learningDir();
    const START_MS = Date.parse("2026-09-17T08:00:00.000Z");
    const END_MS = Date.parse("2026-09-17T08:15:00.000Z");
    const clock: Clock = { nowMs: START_MS };
    const resolutions = new Map<string, Record<string, unknown>>();
    const marketId = "btc-mkt-race";
    const marketDef = qualified(
      marketId,
      "BTC",
      `${marketId}-up`,
      `${marketId}-down`,
      new Date(END_MS).toISOString(),
    );
    marketDef.qualification.binary.yesOutcome = "DOWN";
    marketDef.qualification.binary.noOutcome = "UP";

    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([marketDef]));
    const journal = new MemoryEventJournal();
    const env = qualifiedPaperEnv({
      LEARNING_DATA_DIR: learningDataDir,
      LEARNING_ENABLED: "true",
      PAPER_START_BALANCE: "1000",
      PAPER_EXECUTION_STYLE: "TAKER",
      PAPER_EXECUTION_LATENCY_MS: "0",
      EXPERIMENT_DURATION_HOURS: "1",
      ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
      SETTLEMENT_POLL_MS: "1",
      LEARNING_SETTLEMENT_POLL_MS: "1",
      LEARNING_CHECKPOINT_SECONDS: "900,600,300",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(clock.nowMs),
      resolutionFetcher: async (id) => resolutions.get(id) ?? null,
    });

    let releaseShadow: (() => void) | null = null;
    const shadowHold = new Promise<void>((resolve) => { releaseShadow = resolve; });
    engine.setNativeV2ShadowEvalHoldForTest(shadowHold);

    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);

      await engine.ingestReferenceTickForTest(referenceTick(clock, "100000"));
      clock.nowMs += 1_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100500"));

      await market.emitSnapshot(book(clock, marketId, "UP", 1));
      await market.emitSnapshot(book(clock, marketId, "DOWN", 1));
      await drainRuntime(engine);
      // Allow setImmediate to schedule shadow eval, which is now held.
      await new Promise((r) => setImmediate(r));
      await new Promise((r) => setImmediate(r));
      expect(engine.counterfactualSummary().fillCount).toBe(0);

      // BEFORE shadow eval/CF commit: discovery refresh removes the market from live maps.
      market.queue(discoveryReport([]));
      await engine.reconcileDiscovery();
      // Ensure live maps no longer carry the market (covers stale SingleFlight generations).
      if (engine.monitoredMarketIds().includes(marketId)) {
        engine.dropLiveMarketForTest(marketId);
      }
      expect(engine.monitoredMarketIds()).not.toContain(marketId);
      expect(engine.entryQualifiedIds()).not.toContain(marketId);

      releaseShadow?.();
      engine.setNativeV2ShadowEvalHoldForTest(null);
      await waitFor(() => engine.counterfactualSummary().fillCount >= 1, 10_000);

      const cf = (engine as unknown as { counterfactualStore: {
        settlementLookupFor: (id: string) => {
          marketId: string; slug: string | null; endDate: string; yesMapsTo: string; noMapsTo: string;
        } | null;
        hasUnpairedPosition: (id: string) => boolean;
      } }).counterfactualStore;
      const lookup = cf.settlementLookupFor(marketId);
      expect(lookup).not.toBeNull();
      expect(lookup?.yesMapsTo).toBe("DOWN");
      expect(lookup?.noMapsTo).toBe("UP");
      expect(lookup?.endDate).toBe(new Date(END_MS).toISOString());
      expect(cf.hasUnpairedPosition(marketId)).toBe(true);
      expect(engine.counterfactualSummary().fillCount).toBeGreaterThanOrEqual(1);
      expect(engine.counterfactualSummary().recoveryReady).toBe(true);
    } finally {
      engine.setNativeV2ShadowEvalHoldForTest(null);
      releaseShadow?.();
      await engine.stop();
    }

    // Restart with empty discovery + official resolution using persisted ORIGINAL inverted mapping.
    clock.nowMs = END_MS + 60_000;
    resolutions.set(marketId, {
      closed: true,
      umaResolutionStatus: "resolved",
      outcomes: ["Yes", "No"],
      outcomePrices: ["1", "0"],
    });

    const restarted = new EngineService(
      qualifiedPaperEnv({
        LEARNING_DATA_DIR: learningDataDir,
        LEARNING_ENABLED: "true",
        PAPER_START_BALANCE: "1000",
        PAPER_EXECUTION_STYLE: "TAKER",
        SETTLEMENT_POLL_MS: "1",
        LEARNING_SETTLEMENT_POLL_MS: "1",
      }),
      new MemoryEventJournal(),
      { error: () => undefined },
      {
        market: (() => {
          const m = new ScriptedMarketAdapter();
          m.queue(discoveryReport([]));
          return m as never;
        })(),
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(clock.nowMs),
        resolutionFetcher: async (id) => resolutions.get(id) ?? null,
      },
    );

    try {
      await restarted.start();
      expect(restarted.counterfactualSummary().fillCount).toBeGreaterThanOrEqual(1);
      for (let i = 0; i < 5; i += 1) {
        await restarted.tickMaintenance();
        if (restarted.counterfactualSummary().settlementCount >= 1) break;
        clock.nowMs += 1_000;
      }
      expect(restarted.counterfactualSummary().settlementCount).toBeGreaterThanOrEqual(1);
      const cf2 = (restarted as unknown as { counterfactualStore: {
        hasSettledMarket: (id: string) => boolean;
        openPositionMarketIds: () => string[];
        summary: () => { availableCashUsd: string; realizedPnl: string; fees: string; fillCount: number; settlementCount: number };
      } }).counterfactualStore;
      expect(cf2.hasSettledMarket(marketId)).toBe(true);
      expect(cf2.openPositionMarketIds()).toEqual([]);
      expect(restarted.ledger.availableCash().toFixed()).toBe("1000");

      const settledSnapshot = {
        availableCashUsd: cf2.summary().availableCashUsd,
        realizedPnl: cf2.summary().realizedPnl,
        fees: cf2.summary().fees,
        fillCount: cf2.summary().fillCount,
        settlementCount: cf2.summary().settlementCount,
      };
      await restarted.stop();

      const replay = new EngineService(
        qualifiedPaperEnv({
          LEARNING_DATA_DIR: learningDataDir,
          PAPER_START_BALANCE: "1000",
        }),
        new MemoryEventJournal(),
        { error: () => undefined },
        {
          market: (() => {
            const m = new ScriptedMarketAdapter();
            m.queue(discoveryReport([]));
            return m as never;
          })(),
          referenceRegistry: new ScriptedReferenceRegistry() as never,
          activation: new NoopGrokActivationBridge(),
          now: () => new Date(clock.nowMs),
          resolutionFetcher: async () => null,
        },
      );
      try {
        await replay.start();
        const summary = replay.counterfactualSummary();
        expect(summary.fillCount).toBe(settledSnapshot.fillCount);
        expect(summary.settlementCount).toBe(settledSnapshot.settlementCount);
        expect(summary.availableCashUsd).toBe(settledSnapshot.availableCashUsd);
        expect(summary.realizedPnl).toBe(settledSnapshot.realizedPnl);
        expect(summary.fees).toBe(settledSnapshot.fees);
      } finally {
        await replay.stop();
      }
    } finally {
      try { await restarted.stop(); } catch { /* already stopped */ }
    }
  });

  it("durable CF lookup outranks later live discovery remapping", async () => {
    const learningDataDir = learningDir();
    const START_MS = Date.parse("2026-09-17T08:00:00.000Z");
    const END_MS = Date.parse("2026-09-17T08:15:00.000Z");
    const clock: Clock = { nowMs: START_MS };
    const resolutions = new Map<string, Record<string, unknown>>();
    const marketId = "btc-mkt-remap";
    const marketDef = qualified(
      marketId,
      "BTC",
      `${marketId}-up`,
      `${marketId}-down`,
      new Date(END_MS).toISOString(),
    );
    marketDef.qualification.binary.yesOutcome = "DOWN";
    marketDef.qualification.binary.noOutcome = "UP";

    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([marketDef]));
    const engine = new EngineService(
      qualifiedPaperEnv({
        LEARNING_DATA_DIR: learningDataDir,
        LEARNING_ENABLED: "true",
        PAPER_START_BALANCE: "1000",
        PAPER_EXECUTION_STYLE: "TAKER",
        PAPER_EXECUTION_LATENCY_MS: "0",
        EXPERIMENT_DURATION_HOURS: "1",
        ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
        SETTLEMENT_POLL_MS: "1",
        LEARNING_SETTLEMENT_POLL_MS: "1",
        LEARNING_CHECKPOINT_SECONDS: "900,600,300",
      }),
      new MemoryEventJournal(),
      { error: () => undefined },
      {
        market: market as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(clock.nowMs),
        resolutionFetcher: async (id) => resolutions.get(id) ?? null,
      },
    );

    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100000"));
      clock.nowMs += 1_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100500"));
      await market.emitSnapshot(book(clock, marketId, "UP", 1));
      await market.emitSnapshot(book(clock, marketId, "DOWN", 1));
      await drainRuntime(engine);
      await waitFor(() => engine.counterfactualSummary().fillCount >= 1, 10_000);

      const cf = (engine as unknown as { counterfactualStore: {
        settlementLookupFor: (id: string) => { yesMapsTo: string; noMapsTo: string } | null;
      } }).counterfactualStore;
      expect(cf.settlementLookupFor(marketId)?.yesMapsTo).toBe("DOWN");

      // Re-discover same market with inverted-back-to-default mapping — durable capture must win.
      const remapped = qualified(
        marketId,
        "BTC",
        `${marketId}-up`,
        `${marketId}-down`,
        new Date(END_MS).toISOString(),
      );
      remapped.qualification.binary.yesOutcome = "UP";
      remapped.qualification.binary.noOutcome = "DOWN";
      market.queue(discoveryReport([remapped]));
      await engine.reconcileDiscovery();

      clock.nowMs = END_MS + 60_000;
      resolutions.set(marketId, {
        closed: true,
        umaResolutionStatus: "resolved",
        outcomes: ["Yes", "No"],
        outcomePrices: ["1", "0"],
      });
      for (let i = 0; i < 5; i += 1) {
        await engine.tickMaintenance();
        if (engine.counterfactualSummary().settlementCount >= 1) break;
        clock.nowMs += 1_000;
      }
      expect(engine.counterfactualSummary().settlementCount).toBeGreaterThanOrEqual(1);
      // Original inverted capture: Yes→DOWN, Yes wins ⇒ DOWN wins ⇒ UP position loses (not default Yes→UP win).
      const settled = (engine as unknown as { counterfactualStore: {
        hasSettledMarket: (id: string) => boolean;
        summary: () => { realizedPnl: string; availableCashUsd: string };
      } }).counterfactualStore;
      expect(settled.hasSettledMarket(marketId)).toBe(true);
      expect(Number(settled.summary().realizedPnl)).toBeLessThan(0);
      expect(Number(settled.summary().availableCashUsd)).toBeLessThan(1000);
    } finally {
      await engine.stop();
    }
  });

});
