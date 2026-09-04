import { describe, expect, it } from "vitest";
import {
  createIdleExperiment,
  type ExperimentManifest,
  type LedgerState,
  type PaperFill,
  type PaperOrder,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION,
  IdempotencyConflictError,
  MemoryEventJournal,
  hashDurablePayload,
} from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { CrashInjectedError, type CrashInjectionPoint } from "../apps/engine/src/crash-injection.js";
import { RecoveryFailClosedError } from "../apps/engine/src/recovery.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { fill, order, snapshot } from "./fixtures.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  paperFill,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";

const logger = { error: () => undefined };

function idleExperiment(): ExperimentManifest {
  return createIdleExperiment({
    durationHours: 48, strategyVersion: "test", riskConfiguration: {}, modelConfiguration: {},
    startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test",
  });
}

function emptyLedger(at = "2026-09-01T00:00:00.000Z"): LedgerState {
  return {
    startingBalance: "1000", cash: "1000", reservedCash: "0", positionValue: "0", equity: "1000",
    realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0",
    aiCost: "0", infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0",
    highWaterMark: "1000", drawdown: "0", updatedAt: at,
  };
}

async function startPaper(journal: MemoryEventJournal, options: {
  now?: () => Date;
  resolutions?: Map<string, Record<string, unknown>>;
  market?: ScriptedMarketAdapter;
  env?: Record<string, string | undefined>;
  endDate?: string;
  recoveryPageSize?: number;
} = {}) {
  const market = options.market ?? new ScriptedMarketAdapter();
  const endDate = options.endDate ?? new Date(Date.now() + 15 * 60_000).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", endDate)]));
  const env = qualifiedPaperEnv({
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    EXPERIMENT_DURATION_HOURS: "48",
    ...options.env,
  });
  const resolutions = options.resolutions ?? new Map<string, Record<string, unknown>>();
  const engine = new EngineService(env, journal, logger, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: options.now,
    resolutionFetcher: async (id) => resolutions.get(id) ?? null,
    recoveryPageSize: options.recoveryPageSize,
  });
  await engine.start();
  return { engine, market, env, resolutions };
}

function criticalSnapshot(engine: EngineService) {
  return {
    ledger: engine.ledger.snapshot(),
    positions: [...engine.ledger.positions.values()].map((item) => structuredClone(item)).sort((a, b) => a.marketId.localeCompare(b.marketId)),
    resting: engine.restingOrderCount(),
    reserved: engine.reservedCash(),
    settled: [...engine.ledger.positions.values()].length,
    experiment: engine.experimentManifest() ? {
      id: engine.experimentManifest()!.id,
      status: engine.experimentManifest()!.status,
      closePolicy: engine.experimentManifest()!.closePolicy,
    } : null,
    cursor: engine.lastAppliedDurableCursorForTest(),
  };
}

async function auditCounts(journal: MemoryEventJournal) {
  const types = [
    "PAPER_FILL", "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
    "FIRST_LEG_FILLED", "HEDGE_COMPLETE", "POSITION_SETTLED",
    "EXPERIMENT_STARTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED",
  ];
  const events = await journal.queryEvents({ types });
  const counts: Record<string, number> = {};
  for (const type of types) counts[type] = events.filter((event) => event.type === type).length;
  return counts;
}

function makerFill(orderId: string, quantity: string, extra: Partial<PaperFill> = {}): PaperFill {
  return {
    ...fill("UP", "0.47", quantity),
    orderId, marketId: "btc-mkt", tokenId: "btc-up", liquidityRole: "MAKER",
    ...extra,
  };
}

describe("durable journal API", () => {
  it("commits monotonically and treats same-key same-payload as existing", async () => {
    const journal = new MemoryEventJournal();
    const first = await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    });
    expect(first).toMatchObject({ sequence: 1, inserted: true, existing: false });
    const again = await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    });
    expect(again).toMatchObject({ sequence: 1, inserted: false, existing: true });
    const second = await journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    });
    expect(second.sequence).toBe(2);
    const page = await journal.readDurableTransitionsAfter(0, 10);
    expect(page.map((row) => row.sequence)).toEqual([1, 2]);
    expect(await journal.latestDurableSequence()).toBe(2);
    expect(hashDurablePayload({ n: 1 })).toBe(hashDurablePayload({ n: 1 }));
  });

  it("hard-fails same-key different payload", async () => {
    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    });
    await expect(journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 2 },
    })).rejects.toBeInstanceOf(IdempotencyConflictError);
  });
});

describe("A-T critical durability matrix", () => {
  it("A first-leg fill: uncommitted does not survive, committed applies once", async () => {
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal);
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    const buy = paperFill("btc-mkt", "UP", "0.40");
    first.engine.setCrashInjectionForTest("BEFORE_DURABLE_COMMIT");
    await expect(first.engine.applyObservedFill(buy)).rejects.toBeInstanceOf(CrashInjectedError);
    expect(first.engine.ledger.positions.size).toBe(0);
    await first.engine.stop();

    const afterUncommitted = await startPaper(journal);
    expect(afterUncommitted.engine.ledger.positions.size).toBe(0);
    await afterUncommitted.engine.applyObservedFill(buy);
    expect(afterUncommitted.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    afterUncommitted.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    const second = paperFill("btc-mkt", "DOWN", "0.48");
    await expect(afterUncommitted.engine.applyObservedFill(second)).rejects.toBeInstanceOf(CrashInjectedError);
    expect(afterUncommitted.engine.ledger.positions.get("btc-mkt")?.downQuantity ?? "0").toBe("0");
    const cursor = await afterUncommitted.engine.latestDurableSequenceForTest();
    expect(afterUncommitted.engine.lastAppliedDurableCursorForTest()).toBeLessThan(cursor);
    await afterUncommitted.engine.stop();

    const recovered = await startPaper(journal);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("10");
    expect(recovered.engine.lastAppliedDurableCursorForTest()).toBe(cursor);
    await recovered.engine.applyObservedFill(second);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("10");
    await recovered.engine.stop();
  });

  it("B hedge / second leg is exactly-once after crash", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    expect((await engine.beginExperiment()).ok).toBe(true);
    await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
    const hedge = paperFill("btc-mkt", "DOWN", "0.48");
    engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT");
    await expect(engine.applyObservedFill(hedge)).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    const position = recovered.engine.ledger.positions.get("btc-mkt");
    expect(position?.pairedQuantity).toBe("10");
    expect(position?.unpairedQuantity).toBe("0");
    const hedges = await journal.queryEvents({ types: ["HEDGE_COMPLETE"] });
    expect(hedges.length).toBeGreaterThanOrEqual(1);
    await recovered.engine.stop();
  });

  it("C maker reserve is write-ahead and not doubled", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const maker = order("MAKER", { quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up" });
    engine.setCrashInjectionForTest("BEFORE_DURABLE_COMMIT");
    await expect(engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }))).rejects.toBeInstanceOf(CrashInjectedError);
    expect(Number(engine.reservedCash())).toBe(0);
    await engine.stop();
    const mid = await startPaper(journal);
    await mid.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    expect(Number(mid.engine.reservedCash())).toBeCloseTo(4.7, 8);
    await mid.engine.stop();
    const recovered = await startPaper(journal);
    expect(Number(recovered.engine.reservedCash())).toBeCloseTo(4.7, 8);
    expect(recovered.engine.restingOrderCount()).toBe(1);
    await recovered.engine.stop();
  });

  it("D maker partial fill keeps remaining reservation", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const maker = order("MAKER", { id: "order_maker_partial", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up" });
    await engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(engine.applyObservedFill(makerFill("order_maker_partial", "4"))).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("4");
    expect(Number(recovered.engine.reservedCash())).toBeGreaterThan(0);
    expect(Number(recovered.engine.reservedCash())).toBeLessThan(4.7);
    await recovered.engine.stop();
  });

  it("E maker release/cancel is exactly-once", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const maker = order("MAKER", { id: "order_maker_release", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up" });
    await engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(engine.cancelMakerForTest("order_maker_release")).rejects.toBeInstanceOf(CrashInjectedError);
    expect(Number(engine.reservedCash())).toBeCloseTo(4.7, 8);
    await engine.stop();
    const recovered = await startPaper(journal);
    expect(Number(recovered.engine.reservedCash())).toBe(0);
    expect(recovered.engine.restingOrderCount()).toBe(0);
    await recovered.engine.cancelMakerForTest("order_maker_release");
    expect(Number(recovered.engine.reservedCash())).toBe(0);
    await recovered.engine.stop();
  });

  it("F settlement is exactly-once and does not call the provider on replay", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const resolutions = new Map<string, Record<string, unknown>>();
    let fetches = 0;
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", new Date(nowMs + 60_000).toISOString())]));
    const env = qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
    const engine = new EngineService(env, journal, logger, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(nowMs),
      resolutionFetcher: async (id) => {
        fetches += 1;
        return resolutions.get(id) ?? null;
      },
    });
    await engine.start();
    expect((await engine.beginExperiment()).ok).toBe(true);
    await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
    nowMs += 120_000;
    resolutions.set("btc-mkt", {
      state: { closed: true },
      resolution: { umaResolutionStatus: "settled" },
      outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
    });
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    const fetchesAfterCrash = fetches;
    await engine.stop();
    const recovered = new EngineService(env, journal, logger, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(nowMs),
      resolutionFetcher: async (id) => {
        fetches += 1;
        return resolutions.get(id) ?? null;
      },
    });
    await recovered.start();
    expect(recovered.ledger.positions.has("btc-mkt")).toBe(false);
    expect(fetches).toBe(fetchesAfterCrash);
    await recovered.tickMaintenance();
    expect(recovered.ledger.positions.has("btc-mkt")).toBe(false);
    await recovered.stop();
  });

  it("G/H/I experiment start, ENTRY_CLOSED, COMPLETED survive the crash points", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal, { now: () => new Date(nowMs), env: { ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" } });
    first.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(first.engine.beginExperiment()).rejects.toBeInstanceOf(CrashInjectedError);
    expect(first.engine.experimentManifest()?.status).toBe("IDLE");
    await first.engine.stop();
    const running = await startPaper(journal, { now: () => new Date(nowMs) });
    expect(running.engine.experimentManifest()?.status).toBe("RUNNING");
    nowMs += 48 * 3_600_000 + 1;
    running.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(running.engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    await running.engine.stop();
    const closed = await startPaper(journal, { now: () => new Date(nowMs) });
    expect(closed.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
    await closed.engine.tickMaintenance();
    expect(closed.engine.experimentManifest()?.status).toBe("COMPLETED");
    await closed.engine.stop();
  });

  it("J mark-to-market is durable and not fabricated on replay", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect((await engine.beginExperiment()).ok).toBe(true);
    nowMs += 48 * 3_600_000 + 1;
    await engine.tickMaintenance();
    expect(engine.experimentManifest()?.status).toBe("COMPLETED");
    await engine.stop();
    const recovered = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect(recovered.engine.experimentManifest()?.status).toBe("COMPLETED");
    await recovered.engine.stop();
  });

  it("K idempotent duplicate fill does not double inventory", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const buy = paperFill("btc-mkt", "UP", "0.40");
    await engine.applyObservedFill(buy);
    await engine.applyObservedFill(buy);
    expect(engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    await engine.stop();
  });

  it("L idempotency conflict is sticky FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const buy = paperFill("btc-mkt", "UP", "0.40");
    await engine.applyObservedFill(buy);
    const conflict = { ...buy, quantity: "3" };
    await expect(engine.applyObservedFill(conflict)).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(engine.integrityFault()).toBe(true);
    expect(engine.overloadState()).toBe("FAIL_CLOSED");
    await engine.stop();
  });

  it("M legacy empty IDLE baseline upgrades", async () => {
    const journal = new MemoryEventJournal();
    await journal.saveCheckpoint({
      experimentId: "exp_legacy_idle",
      sequence: 1,
      payload: {
        ledger: emptyLedger(),
        positions: [],
        experiment: { ...idleExperiment(), id: "exp_legacy_idle" },
        restingOrders: [],
        makerReservations: {},
        settledMarkets: [],
      },
    });
    const { engine } = await startPaper(journal);
    expect(engine.experimentManifest()?.status).toBe("IDLE");
    expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
    const checkpoint = await journal.loadLatestCheckpoint();
    expect((checkpoint?.payload as { schemaVersion?: number }).schemaVersion).toBe(CHECKPOINT_SCHEMA_VERSION);
    expect(typeof (checkpoint?.payload as { durableCursor?: number }).durableCursor).toBe("number");
    await engine.stop();
  });

  it("N legacy unsafe FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    const running = { ...idleExperiment(), id: "exp_legacy_run", status: "RUNNING" as const, startedAt: "2026-09-01T00:00:00.000Z", entryClosesAt: "2026-09-03T00:00:00.000Z" };
    await journal.saveCheckpoint({
      experimentId: running.id,
      sequence: 1,
      payload: {
        ledger: emptyLedger(),
        positions: [],
        experiment: running,
        restingOrders: [],
        makerReservations: {},
      },
    });
    await expect(startPaper(journal)).rejects.toBeInstanceOf(RecoveryFailClosedError);
  });

  it("O corrupt/unknown authoritative type FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    await engine.commitDurableForTest({
      operationKey: "bad:1", type: "UNKNOWN_AUTHORITY", aggregateType: "x", aggregateId: "x", payload: {},
    });
    await engine.stop();
    await expect(startPaper(journal)).rejects.toBeInstanceOf(RecoveryFailClosedError);
  });

  it("P long-tail pagination replays every committed transition", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal, { recoveryPageSize: 2 });
    for (let i = 0; i < 5; i += 1) {
      await engine.applyObservedFill(paperFill(`mkt-${i}`, "UP", "0.40", { id: `fill_page_${i}` }));
    }
    expect(engine.ledger.positions.size).toBe(5);
    engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT");
    await expect(engine.applyObservedFill(paperFill("mkt-tail", "UP", "0.40", { id: "fill_page_tail" }))).rejects.toBeInstanceOf(CrashInjectedError);
    const recovered = await startPaper(journal, { recoveryPageSize: 2 });
    expect(recovered.engine.ledger.positions.size).toBe(6);
    await recovered.engine.stop();
  });

  it("Q checkpoint fence refuses cursor skip", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"))).rejects.toBeInstanceOf(CrashInjectedError);
    await expect(engine.checkpointForTest()).rejects.toThrow(/CHECKPOINT_FENCE/);
    await engine.stop();
  });

  it("R clean shutdown checkpoint restores without replay holes", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    expect((await engine.beginExperiment()).ok).toBe(true);
    await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
    await engine.stop();
    expect(engine.lastShutdownResult().checkpointed).toBe(true);
    const recovered = await startPaper(journal);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    expect(recovered.engine.experimentManifest()?.status).toBe("RUNNING");
    await recovered.engine.stop();
  });

  it("S unclean crash after commit before apply recovers the fill", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    expect((await engine.beginExperiment()).ok).toBe(true);
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"))).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    await recovered.engine.stop();
  });

  it("T crash after derived before checkpoint still recovers from tail", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    expect((await engine.beginExperiment()).ok).toBe(true);
    engine.setCrashInjectionForTest("AFTER_DERIVED_AUDIT_BEFORE_CHECKPOINT");
    await expect(engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"))).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    await recovered.engine.stop();
  });
});

describe("golden equivalence control vs crashed", () => {
  async function runGolden(crashAt: CrashInjectionPoint | null) {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const resolutions = new Map<string, Record<string, unknown>>();
    const market = new ScriptedMarketAdapter();
    const end = new Date(nowMs + 90 * 24 * 3_600_000).toISOString();
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", end)]));
    const env = qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", EXPERIMENT_DURATION_HOURS: "48" });
    const make = async () => {
      market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", end)]));
      const engine = new EngineService(env, journal, logger, {
        market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(), now: () => new Date(nowMs),
        resolutionFetcher: async (id) => resolutions.get(id) ?? null,
      });
      await engine.start();
      return engine;
    };
    let engine = await make();
    const maybeCrash = async (point: CrashInjectionPoint, work: () => Promise<void>) => {
      if (crashAt !== point) {
        await work();
        return;
      }
      engine.setCrashInjectionForTest(point);
      await expect(work()).rejects.toBeInstanceOf(CrashInjectedError);
      engine = await make();
      await work();
    };
    await maybeCrash("AFTER_DURABLE_COMMIT_BEFORE_APPLY", async () => {
      if (engine.experimentManifest()?.status === "RUNNING") return;
      const started = await engine.beginExperiment();
      if (!started.ok) throw new Error("begin failed");
    });
    const maker: PaperOrder = order("MAKER", {
      id: "order_golden_maker", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up",
    });
    await maybeCrash("AFTER_APPLY_BEFORE_DERIVED_AUDIT", async () => {
      await engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    });
    await engine.applyObservedFill(makerFill("order_golden_maker", "4", { id: "fill_golden_partial" }));
    await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40", { id: "fill_golden_first" }));
    await engine.applyObservedFill(paperFill("btc-mkt", "DOWN", "0.48", { id: "fill_golden_hedge" }));
    await engine.cancelMakerForTest("order_golden_maker", "RELEASE_REMAINING");
    nowMs += 90 * 24 * 3_600_000 + 1;
    resolutions.set("btc-mkt", {
      state: { closed: true },
      resolution: { umaResolutionStatus: "settled" },
      outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
    });
    await engine.tickMaintenance();
    nowMs += 48 * 3_600_000 + 1;
    await engine.tickMaintenance();
    const snap = criticalSnapshot(engine);
    const counts = await auditCounts(journal);
    await engine.stop();
    return { snap, counts, cursor: snap.cursor };
  }

  it("control and crashed engines match ledger/experiment/cursor/audit counts", async () => {
    const control = await runGolden(null);
    const crashed = await runGolden("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    expect(crashed.snap.ledger).toEqual(control.snap.ledger);
    expect(crashed.snap.positions).toEqual(control.snap.positions);
    expect(crashed.snap.reserved).toEqual(control.snap.reserved);
    expect(crashed.snap.resting).toEqual(control.snap.resting);
    expect(crashed.snap.experiment?.status).toEqual(control.snap.experiment?.status);
    expect(crashed.snap.experiment?.closePolicy).toEqual(control.snap.experiment?.closePolicy);
    expect(crashed.snap.cursor).toEqual(control.snap.cursor);
    expect(crashed.counts).toEqual(control.counts);
  });
});

describe("PR #12 bounds remain untouched", () => {
  it("keeps supervisor and journal admission constants", async () => {
    const { RUNTIME_BOUNDS } = await import("../apps/engine/src/runtime-supervisor.js");
    expect(RUNTIME_BOUNDS.trigger.p0).toEqual({ concurrency: 4, queueLimit: 256, highWater: 192 });
    expect(RUNTIME_BOUNDS.trigger.p3).toEqual({ concurrency: 4, queueLimit: 1024, highWater: 768 });
    expect(RUNTIME_BOUNDS.journal).toEqual({
      active: 4, noncriticalActive: 3, waiting: 128, highWater: 96, criticalWaiting: 32, noncriticalWaiting: 96,
    });
  });
});
