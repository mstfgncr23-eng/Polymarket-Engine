import { describe, expect, it } from "vitest";
import {
  createIdleExperiment,
  createRestingMakerOrder,
  type ExperimentManifest,
  type LedgerState,
  type PaperFill,
  type PaperOrder,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION,
  DurableCursorBehindError,
  IdempotencyConflictError,
  MemoryEventJournal,
  derivedAuditEventId,
  hashDurablePayload,
} from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { CrashInjectedError, type CrashInjectionPoint } from "../apps/engine/src/crash-injection.js";
import { CRITICAL_AUDIT_TYPES, RecoveryFailClosedError } from "../apps/engine/src/recovery.js";
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
  const experiment = engine.experimentManifest();
  return {
    ledger: engine.ledger.snapshot(),
    positions: [...engine.ledger.positions.values()].map((item) => structuredClone(item)).sort((a, b) => a.marketId.localeCompare(b.marketId)),
    restingOrders: [...engine.restingOrdersForTest().values()]
      .map((item) => structuredClone(item))
      .sort((a, b) => a.order.id.localeCompare(b.order.id)),
    makerReservations: Object.fromEntries([...engine.makerReservationsForTest().entries()].sort(([a], [b]) => a.localeCompare(b))),
    settledMarkets: [...engine.settledMarketsForTest()].sort(),
    reserved: engine.reservedCash(),
    experiment: experiment ? {
      id: experiment.id,
      status: experiment.status,
      closePolicy: experiment.closePolicy,
      startedAt: experiment.startedAt,
      entryClosesAt: experiment.entryClosesAt,
      completedAt: experiment.completedAt,
      markToMarketAt: experiment.markToMarketAt ?? null,
    } : null,
    cursor: engine.lastAppliedDurableCursorForTest(),
    derivedAuditCursor: engine.derivedAuditCursorForTest(),
  };
}

async function reportCriticalAudit(journal: MemoryEventJournal) {
  const events = await journal.queryEvents({ types: [...CRITICAL_AUDIT_TYPES] });
  const derived = events
    .filter((event) => event.id.startsWith("derived:"))
    .map((event) => ({ id: event.id, type: event.type, payload: event.payload }))
    .sort((a, b) => a.id.localeCompare(b.id));
  const counts: Record<string, number> = {};
  for (const type of CRITICAL_AUDIT_TYPES) counts[type] = derived.filter((event) => event.type === type).length;
  return { derived, counts };
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
      payload: { n: 1 }, occurredAt: new Date("2026-09-01T00:00:00.000Z"),
    }, 0);
    expect(first).toMatchObject({ sequence: 1, inserted: true, existing: false });
    expect(first.transition.occurredAt).toBe("2026-09-01T00:00:00.000Z");
    const again = await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    }, 0);
    expect(again).toMatchObject({ sequence: 1, inserted: false, existing: true });
    expect(again.transition.occurredAt).toBe("2026-09-01T00:00:00.000Z");
    const second = await journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    }, 1);
    expect(second.sequence).toBe(2);
    const page = await journal.readDurableTransitionsAfter(0, 10);
    expect(page.map((row) => row.sequence)).toEqual([1, 2]);
    expect(await journal.latestDurableSequence()).toBe(2);
    expect(hashDurablePayload({ n: 1, derivedAudit: [{ extra: true }] })).toBe(hashDurablePayload({ n: 1 }));
  });

  it("hard-fails same-key different payload", async () => {
    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    }, 0);
    await expect(journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 2 },
    }, 1)).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("refuses insert when expected previous sequence is behind latest", async () => {
    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    }, 0);
    await expect(journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    }, 0)).rejects.toBeInstanceOf(DurableCursorBehindError);
    expect(await journal.latestDurableSequence()).toBe(1);
    const inserted = await journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    }, 1);
    expect(inserted.sequence).toBe(2);
  });

  it("assigns the next sequence from latest committed after a failed insert", async () => {
    const journal = new MemoryEventJournal();
    const first = await journal.commitDurableTransition({
      operationKey: "fill:a", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "a",
      payload: { n: 1 },
    }, 0);
    expect(first.sequence).toBe(1);
    journal.failNextDurableInsert = true;
    await expect(journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    }, 1)).rejects.toThrow("DURABLE_INSERT_FAILED");
    expect(await journal.latestDurableSequence()).toBe(1);
    const second = await journal.commitDurableTransition({
      operationKey: "fill:b", type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: "b",
      payload: { n: 2 },
    }, 1);
    expect(second.sequence).toBe(2);
    expect((await journal.readDurableTransitionsAfter(0, 10)).map((row) => row.sequence)).toEqual([1, 2]);
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
    mid.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
    await expect(mid.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }))).rejects.toBeInstanceOf(CrashInjectedError);
    expect(Number(mid.engine.reservedCash())).toBe(0);
    const reservedRow = (await journal.readDurableTransitionsAfter(0, 20)).find((row) => row.type === "PAPER_MAKER_RESERVED");
    expect(reservedRow).toBeDefined();
    await mid.engine.stop();
    const recovered = await startPaper(journal);
    expect(Number(recovered.engine.reservedCash())).toBeCloseTo(4.7, 8);
    expect(recovered.engine.restingOrderCount()).toBe(1);
    expect(recovered.engine.ledger.snapshot().updatedAt).toBe(reservedRow!.occurredAt);
    await recovered.engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    expect(Number(recovered.engine.reservedCash())).toBeCloseTo(4.7, 8);
    await recovered.engine.stop();
  });

  it("D maker partial fill keeps remaining reservation and exact resting remaining", async () => {
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
    expect(recovered.engine.restingOrdersForTest().get("order_maker_partial")?.remaining).toBe("6");
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
    const cancelRow = (await journal.readDurableTransitionsAfter(0, 20)).find((row) => row.type === "PAPER_ORDER_CANCELLED");
    expect(recovered.engine.ledger.snapshot().updatedAt).toBe(cancelRow?.occurredAt);
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

  it("L2 same fill id with changed fee/slippage/orderId conflicts via payload hash", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const buy = paperFill("btc-mkt", "UP", "0.40", { id: "fill_hash_conflict", feeUsd: "0.01", slippageUsd: "0", orderId: "order_orig" });
    await engine.applyObservedFill(buy);
    await expect(engine.applyObservedFill({ ...buy, feeUsd: "0.02" })).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(engine.integrityFault()).toBe(true);
    await engine.stop();

    const second = await startPaper(journal);
    await expect(second.engine.applyObservedFill({ ...buy, slippageUsd: "0.05" })).rejects.toBeInstanceOf(IdempotencyConflictError);
    await second.engine.stop();

    const third = await startPaper(journal);
    await expect(third.engine.applyObservedFill({ ...buy, orderId: "order_changed" })).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(third.engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    await third.engine.stop();
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

  it("U settlement AFTER_APPLY_BEFORE_DERIVED_AUDIT repairs resolution/win/pair/pnl audit", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const resolutions = new Map<string, Record<string, unknown>>([
      ["btc-mkt", {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      }],
    ]);
    const first = await startPaper(journal, { now: () => new Date(nowMs), resolutions, endDate: new Date(nowMs + 60_000).toISOString() });
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    await first.engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
    await first.engine.applyObservedFill(paperFill("btc-mkt", "DOWN", "0.48"));
    nowMs += 120_000;
    first.engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT", {
      operationKey: `settlement:btc-mkt:${first.engine.experimentManifest()!.id}`,
    });
    await expect(first.engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    await first.engine.stop();
    const recovered = await startPaper(journal, { now: () => new Date(nowMs), resolutions, endDate: new Date(nowMs - 60_000).toISOString() });
    expect(recovered.engine.ledger.positions.has("btc-mkt")).toBe(false);
    expect(recovered.engine.settledMarketsForTest().has("btc-mkt")).toBe(true);
    const audit = await reportCriticalAudit(journal);
    expect(audit.counts.MARKET_RESOLUTION_OBSERVED).toBeGreaterThanOrEqual(1);
    expect(audit.counts.POSITION_SETTLED).toBeGreaterThanOrEqual(1);
    expect(audit.counts.PAIR_REALIZED).toBeGreaterThanOrEqual(1);
    expect(audit.derived.some((event) => event.type === "PNL_SNAPSHOT" && event.payload.source === "SETTLEMENT")).toBe(true);
    expect(["POSITION_WIN", "POSITION_LOSS", "POSITION_BREAKEVEN"].some((type) => (audit.counts[type] ?? 0) > 0)).toBe(true);
    await recovered.engine.stop();
  });

  it("V MTM crash after commit and after apply does not double-apply or false-conflict", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    await first.engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
    await first.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP" }));
    await first.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN" }));
    nowMs += 48 * 3_600_000 + 1;
    const mtmKey = `mtm:${first.engine.experimentManifest()!.id}`;
    first.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: mtmKey });
    await expect(first.engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    const equityAfterCommit = first.engine.ledger.snapshot().equity;
    await first.engine.stop();

    const mid = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect(mid.engine.ledger.snapshot().equity).not.toBe(equityAfterCommit);
    await mid.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP" }));
    await mid.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN" }));
    mid.engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT", { operationKey: mtmKey });
    await expect(mid.engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    const markedEquity = mid.engine.ledger.snapshot().equity;
    await mid.engine.stop();

    const recovered = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect(recovered.engine.ledger.snapshot().equity).toBe(markedEquity);
    await recovered.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP" }));
    await recovered.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN" }));
    await recovered.engine.tickMaintenance();
    expect(recovered.engine.integrityFault()).toBe(false);
    expect(recovered.engine.ledger.snapshot().equity).toBe(markedEquity);
    expect(recovered.engine.experimentManifest()?.status).toBe("COMPLETED");
    const audit = await reportCriticalAudit(journal);
    expect(audit.derived.some((event) => event.type === "PNL_SNAPSHOT" && event.payload.source === "MARK_TO_MARKET")).toBe(true);
    await recovered.engine.stop();
  });

  it("W experiment COMPLETED AFTER_DURABLE_COMMIT_BEFORE_APPLY retains committed timestamps", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal, { now: () => new Date(nowMs) });
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    const experimentId = first.engine.experimentManifest()!.id;
    nowMs += 48 * 3_600_000 + 1;
    first.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: `experiment:${experimentId}:completed` });
    await expect(first.engine.tickMaintenance()).rejects.toBeInstanceOf(CrashInjectedError);
    expect(first.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
    const completedRow = (await journal.readDurableTransitionsAfter(0, 20)).find((row) => row.type === "EXPERIMENT_COMPLETED");
    expect(completedRow).toBeDefined();
    const committed = (completedRow!.payload.experiment as { completedAt?: string }).completedAt;
    await first.engine.stop();
    nowMs += 60_000;
    const recovered = await startPaper(journal, { now: () => new Date(nowMs) });
    expect(recovered.engine.experimentManifest()?.status).toBe("COMPLETED");
    expect(recovered.engine.experimentManifest()?.completedAt).toBe(committed);
    await recovered.engine.tickMaintenance();
    expect(recovered.engine.experimentManifest()?.completedAt).toBe(committed);
    expect(recovered.engine.integrityFault()).toBe(false);
    await recovered.engine.stop();
  });

  it("X two engines sharing a journal cannot skip a foreign durable transition", async () => {
    const journal = new MemoryEventJournal();
    const a = await startPaper(journal);
    expect((await a.engine.beginExperiment()).ok).toBe(true);
    await a.engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40", { id: "fill_engine_a" }));
    const cursorBeforeB = a.engine.lastAppliedDurableCursorForTest();
    const b = await startPaper(journal);
    await b.engine.applyObservedFill(paperFill("btc-mkt", "DOWN", "0.48", { id: "fill_engine_b" }));
    expect(await journal.latestDurableSequence()).toBe(cursorBeforeB + 1);
    expect(a.engine.lastAppliedDurableCursorForTest()).toBe(cursorBeforeB);
    await a.engine.applyObservedFill(paperFill("alt-mkt", "UP", "0.40", { id: "fill_engine_a_next" }));
    expect(a.engine.lastAppliedDurableCursorForTest()).toBe(cursorBeforeB + 2);
    expect(a.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("10");
    expect(a.engine.ledger.positions.get("alt-mkt")?.upQuantity).toBe("10");
    const sequences = (await journal.readDurableTransitionsAfter(cursorBeforeB - 1, 10)).map((row) => row.operationKey);
    expect(sequences).toContain("fill:fill_engine_b");
    expect(sequences.indexOf("fill:fill_engine_b")).toBeLessThan(sequences.indexOf("fill:fill_engine_a_next"));
    await a.engine.stop();
    await b.engine.stop();
  });

  it("Y derived fill audit is not rebuilt from later hedge state", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const first = paperFill("btc-mkt", "UP", "0.40", { id: "fill_hist_first" });
    const hedge = paperFill("btc-mkt", "DOWN", "0.48", { id: "fill_hist_hedge" });
    await engine.applyObservedFill(first);
    await engine.applyObservedFill(hedge);
    engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT");
    await expect(engine.applyObservedFill(paperFill("alt-mkt", "UP", "0.40", { id: "fill_hist_tail" }))).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    const firstUpdated = (await journal.queryEvents({ types: ["POSITION_UPDATED"] }))
      .find((event) => event.id === derivedAuditEventId("POSITION_UPDATED", "fill_hist_first"));
    expect(firstUpdated?.payload.downQuantity).toBe("0");
    expect(firstUpdated?.payload.upQuantity).toBe("10");
    const firstPnl = (await journal.queryEvents({ types: ["PNL_SNAPSHOT"] }))
      .find((event) => event.id === derivedAuditEventId("PNL_SNAPSHOT", "fill_hist_first"));
    const hedgePnl = (await journal.queryEvents({ types: ["PNL_SNAPSHOT"] }))
      .find((event) => event.id === derivedAuditEventId("PNL_SNAPSHOT", "fill_hist_hedge"));
    expect(firstPnl?.payload.cash).not.toEqual(hedgePnl?.payload.cash);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("10");
    await recovered.engine.stop();
  });

  it("Z malformed checkpoint becomes RecoveryFailClosedError and is counted", async () => {
    const journal = new MemoryEventJournal();
    await journal.saveCheckpoint({
      experimentId: "exp_schema_bad",
      sequence: 1,
      payload: {
        schemaVersion: CHECKPOINT_SCHEMA_VERSION,
        durableCursor: 0,
        ledger: { cash: "not-a-decimal" },
        positions: [],
        experiment: { ...idleExperiment(), id: "exp_schema_bad" },
      },
    });
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const env = qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
    const engine = new EngineService(env, journal, logger, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await expect(engine.start()).rejects.toBeInstanceOf(RecoveryFailClosedError);
    const metric = await engine.metrics.recoveryFailures.get();
    expect(metric.values.some((item) => item.labels.reason === "malformed" && item.value >= 1)).toBe(true);
  });

  it("failed durable insert does not leave a sequence gap that recovery treats as corruption", async () => {
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal);
    const buy = paperFill("btc-mkt", "UP", "0.40", { id: "fill_gap_first" });
    await first.engine.applyObservedFill(buy);
    const afterFirst = first.engine.lastAppliedDurableCursorForTest();
    journal.failNextDurableInsert = true;
    const secondFill = paperFill("btc-mkt", "DOWN", "0.48", { id: "fill_gap_second" });
    await expect(first.engine.applyObservedFill(secondFill)).rejects.toThrow("DURABLE_INSERT_FAILED");
    expect(await journal.latestDurableSequence()).toBe(afterFirst);
    expect(first.engine.lastAppliedDurableCursorForTest()).toBe(afterFirst);
    await first.engine.stop();

    const recovered = await startPaper(journal);
    expect(recovered.engine.lastAppliedDurableCursorForTest()).toBe(afterFirst);
    await recovered.engine.applyObservedFill(secondFill);
    expect(recovered.engine.lastAppliedDurableCursorForTest()).toBe(afterFirst + 1);
    expect(recovered.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("10");
    expect(recovered.engine.overloadState()).not.toBe("FAIL_CLOSED");
    await recovered.engine.stop();
  });

  it("duplicate PAPER_FILL does not re-run live afterApply side effects", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const buy = paperFill("btc-mkt", "UP", "0.40", { id: "fill_idempotent_live" });
    await engine.applyObservedFill(buy);
    const paperFillMetric = async () => {
      const metric = await engine.metrics.paperFills.get();
      return metric.values.reduce((sum, item) => sum + item.value, 0);
    };
    const feedFills = () => engine.state.feed.filter((item) => item.type === "PAPER_FILL").length;
    const exposureTriggers = () => engine.state.recentTriggers.filter((item) => item.kind === "DIRECTIONAL_EXPOSURE").length;
    await new Promise((resolve) => setTimeout(resolve, 20));
    const metricAfterFirst = await paperFillMetric();
    const feedAfterFirst = feedFills();
    const triggersAfterFirst = exposureTriggers();
    expect(metricAfterFirst).toBeGreaterThanOrEqual(1);
    expect(feedAfterFirst).toBe(1);
    await engine.applyObservedFill(buy);
    expect(engine.ledger.positions.get("btc-mkt")?.upQuantity).toBe("10");
    expect(await paperFillMetric()).toBe(metricAfterFirst);
    expect(feedFills()).toBe(feedAfterFirst);
    expect(exposureTriggers()).toBe(triggersAfterFirst);
    await engine.stop();
  });

  it("failed MTM derived audit does not advance the audit cursor and is repaired", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    await first.engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40", { id: "fill_mtm_audit" }));
    await first.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP" }));
    await first.market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN" }));
    nowMs += 48 * 3_600_000 + 1;
    journal.failAppendTypes.add("LEDGER_MARK_TO_MARKET");
    await expect(first.engine.tickMaintenance()).rejects.toThrow("JOURNAL_APPEND_FAILED");
    expect(first.engine.derivedAuditCursorForTest()).toBeLessThan(first.engine.lastAppliedDurableCursorForTest());
    const mtm = (await journal.readDurableTransitionsAfter(0, 50)).find((row) => row.type === "LEDGER_MARK_TO_MARKET");
    expect(mtm).toBeDefined();
    expect(first.engine.derivedAuditCursorForTest()).toBeLessThan(mtm!.sequence);
    expect((await journal.queryEvents({ types: ["LEDGER_MARK_TO_MARKET"] })).length).toBe(0);
    journal.failAppendTypes.clear();
    await first.engine.stop();

    const recovered = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { EXPERIMENT_CLOSE_POLICY: "mark_to_market", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" },
    });
    expect((await journal.queryEvents({ types: ["LEDGER_MARK_TO_MARKET"] })).length).toBeGreaterThanOrEqual(1);
    expect(recovered.engine.derivedAuditCursorForTest()).toBeGreaterThanOrEqual(mtm!.sequence);
    await recovered.engine.stop();
  });

  it("failed infrastructure derived audit does not advance the audit cursor and is repaired", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const first = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { ESTIMATED_INFRA_COST_PER_HOUR_USD: "12" },
    });
    expect((await first.engine.beginExperiment()).ok).toBe(true);
    const beforeAccrual = first.engine.lastAppliedDurableCursorForTest();
    nowMs += 3_600_000;
    journal.failAppendTypes.add("INFRASTRUCTURE_COST_ACCRUED");
    await expect(first.engine.tickMaintenance()).rejects.toThrow("JOURNAL_APPEND_FAILED");
    const infra = (await journal.readDurableTransitionsAfter(beforeAccrual, 10)).find((row) => row.type === "INFRASTRUCTURE_COST_ACCRUED");
    expect(infra).toBeDefined();
    expect(first.engine.derivedAuditCursorForTest()).toBeLessThan(infra!.sequence);
    expect((await journal.queryEvents({ types: ["INFRASTRUCTURE_COST_ACCRUED"] })).length).toBe(0);
    journal.failAppendTypes.clear();
    await first.engine.stop();

    const recovered = await startPaper(journal, {
      now: () => new Date(nowMs),
      env: { ESTIMATED_INFRA_COST_PER_HOUR_USD: "12" },
    });
    expect((await journal.queryEvents({ types: ["INFRASTRUCTURE_COST_ACCRUED"] })).length).toBeGreaterThanOrEqual(1);
    expect(recovered.engine.derivedAuditCursorForTest()).toBeGreaterThanOrEqual(infra!.sequence);
    await recovered.engine.stop();
  });

  it("maker invariant A: resting without reservation FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    const resting = createRestingMakerOrder(
      order("MAKER", { id: "order_orphan_resting", marketId: "btc-mkt", tokenId: "btc-up" }),
      snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }),
    );
    await journal.saveCheckpoint({
      experimentId: "exp_inv_a",
      sequence: 1,
      payload: {
        schemaVersion: CHECKPOINT_SCHEMA_VERSION,
        durableCursor: 0,
        ledger: emptyLedger(),
        positions: [],
        experiment: { ...idleExperiment(), id: "exp_inv_a" },
        restingOrders: [resting],
        makerReservations: {},
        settledMarkets: [],
      },
    });
    await expect(startPaper(journal)).rejects.toMatchObject({ name: "RecoveryFailClosedError", reason: "invariant" });
  });

  it("maker invariant B: reservation without resting FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    await journal.saveCheckpoint({
      experimentId: "exp_inv_b",
      sequence: 1,
      payload: {
        schemaVersion: CHECKPOINT_SCHEMA_VERSION,
        durableCursor: 0,
        ledger: { ...emptyLedger(), reservedCash: "4.7" },
        positions: [],
        experiment: { ...idleExperiment(), id: "exp_inv_b" },
        restingOrders: [],
        makerReservations: { order_orphan_reserve: "4.7" },
        settledMarkets: [],
      },
    });
    await expect(startPaper(journal)).rejects.toMatchObject({ name: "RecoveryFailClosedError", reason: "invariant" });
  });

  it("maker invariant C: reservation total != reservedCash FAIL_CLOSED", async () => {
    const journal = new MemoryEventJournal();
    const resting = createRestingMakerOrder(
      order("MAKER", { id: "order_mismatch", marketId: "btc-mkt", tokenId: "btc-up" }),
      snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }),
    );
    await journal.saveCheckpoint({
      experimentId: "exp_inv_c",
      sequence: 1,
      payload: {
        schemaVersion: CHECKPOINT_SCHEMA_VERSION,
        durableCursor: 0,
        ledger: { ...emptyLedger(), reservedCash: "1" },
        positions: [],
        experiment: { ...idleExperiment(), id: "exp_inv_c" },
        restingOrders: [resting],
        makerReservations: { order_mismatch: "4.7" },
        settledMarkets: [],
      },
    });
    await expect(startPaper(journal)).rejects.toMatchObject({ name: "RecoveryFailClosedError", reason: "invariant" });
  });

  it("MARKET_REMOVED awaits durable cancel before dropping resting state", async () => {
    const journal = new MemoryEventJournal();
    const { engine, market } = await startPaper(journal);
    const maker = order("MAKER", { id: "order_market_removed", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up" });
    await engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    expect(engine.restingOrderCount()).toBe(1);
    market.queue(discoveryReport([]));
    await engine.reconcileDiscovery();
    expect(engine.restingOrderCount()).toBe(0);
    expect(engine.makerReservationsForTest().size).toBe(0);
    expect(Number(engine.reservedCash())).toBe(0);
    const cancelled = (await journal.queryEvents({ types: ["PAPER_ORDER_CANCELLED"] }))
      .find((event) => event.id === derivedAuditEventId("PAPER_ORDER_CANCELLED", "order_market_removed"));
    expect(cancelled?.payload.reason).toBe("MARKET_REMOVED");
    await engine.stop();
  });

  it("maker reserve preserves OPEN_FIRST_LEG and HEDGE_SECOND_LEG in PAPER_ORDER_CREATED", async () => {
    const journal = new MemoryEventJournal();
    const { engine } = await startPaper(journal);
    const first = order("MAKER", { id: "order_intent_first", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up" });
    const hedge = order("MAKER", { id: "order_intent_hedge", quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-down" });
    await engine.restMakerForTest(first, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }), "OPEN_FIRST_LEG");
    engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: "maker:order_intent_hedge:reserve" });
    await expect(engine.restMakerForTest(hedge, snapshot({ marketId: "btc-mkt", tokenId: "btc-down" }), "HEDGE_SECOND_LEG")).rejects.toBeInstanceOf(CrashInjectedError);
    await engine.stop();
    const recovered = await startPaper(journal);
    const created = await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
    expect(created.find((event) => event.id === derivedAuditEventId("PAPER_ORDER_CREATED", "order_intent_first"))?.payload.intent).toBe("OPEN_FIRST_LEG");
    expect(created.find((event) => event.id === derivedAuditEventId("PAPER_ORDER_CREATED", "order_intent_hedge"))?.payload.intent).toBe("HEDGE_SECOND_LEG");
    expect(recovered.engine.restingOrdersForTest().has("order_intent_hedge")).toBe(true);
    await recovered.engine.stop();
  });
});

describe("golden equivalence control vs crashed", () => {
  async function runGolden(multiCrash: boolean) {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const resolutions = new Map<string, Record<string, unknown>>();
    const market = new ScriptedMarketAdapter();
    const end = new Date(nowMs + 60 * 60_000).toISOString();
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
    const crashAndResume = async (point: CrashInjectionPoint, operationKey: string | undefined, work: () => Promise<void>) => {
      if (!multiCrash) {
        await work();
        return;
      }
      engine.setCrashInjectionForTest(point, operationKey ? { operationKey } : undefined);
      try {
        await work();
        return;
      } catch (error) {
        expect(error).toBeInstanceOf(CrashInjectedError);
      }
      await engine.stop();
      engine = await make();
      await work();
    };
    await crashAndResume("AFTER_DURABLE_COMMIT_BEFORE_APPLY", undefined, async () => {
      if (engine.experimentManifest()?.status === "RUNNING") return;
      const started = await engine.beginExperiment();
      if (!started.ok) throw new Error("begin failed");
    });
    const maker: PaperOrder = order("MAKER", {
      id: "order_golden_maker", traceId: "trace_golden_maker", triggerId: "trigger_golden_maker",
      quantity: "10", requestedPrice: "0.47", marketId: "btc-mkt", tokenId: "btc-up",
    });
    await crashAndResume("AFTER_DURABLE_COMMIT_BEFORE_APPLY", "maker:order_golden_maker:reserve", async () => {
      if (engine.restingOrdersForTest().has("order_golden_maker")) return;
      await engine.restMakerForTest(maker, snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }));
    });
    await crashAndResume("AFTER_APPLY_BEFORE_DERIVED_AUDIT", "fill:fill_golden_partial", async () => {
      if (engine.ledger.positions.get("btc-mkt")?.upQuantity === "4" || Number(engine.ledger.positions.get("btc-mkt")?.upQuantity ?? 0) >= 4) return;
      await engine.applyObservedFill(makerFill("order_golden_maker", "4", {
        id: "fill_golden_partial", traceId: "trace_golden_partial", sourceMarketEventId: "event_golden_partial",
      }));
    });
    await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40", {
      id: "fill_golden_first", orderId: "order_golden_first", traceId: "trace_golden_first", sourceMarketEventId: "event_golden_first",
    }));
    await engine.applyObservedFill(paperFill("btc-mkt", "DOWN", "0.48", {
      id: "fill_golden_hedge", orderId: "order_golden_hedge", traceId: "trace_golden_hedge", sourceMarketEventId: "event_golden_hedge",
    }));
    await crashAndResume("AFTER_DURABLE_COMMIT_BEFORE_APPLY", "maker:order_golden_maker:cancel", async () => {
      if (!engine.restingOrdersForTest().has("order_golden_maker") && !engine.makerReservationsForTest().has("order_golden_maker")) return;
      await engine.cancelMakerForTest("order_golden_maker", "RELEASE_REMAINING");
    });
    nowMs += 60 * 60_000 + 1;
    resolutions.set("btc-mkt", {
      state: { closed: true },
      resolution: { umaResolutionStatus: "settled" },
      outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
    });
    await crashAndResume("AFTER_APPLY_BEFORE_DERIVED_AUDIT", `settlement:btc-mkt:${engine.experimentManifest()!.id}`, async () => {
      await engine.tickMaintenance();
    });
    nowMs += 48 * 3_600_000 + 1;
    await crashAndResume("AFTER_DURABLE_COMMIT_BEFORE_APPLY", `experiment:${engine.experimentManifest()!.id}:completed`, async () => {
      await engine.tickMaintenance();
    });
    const snap = criticalSnapshot(engine);
    const audit = await reportCriticalAudit(journal);
    await engine.stop();
    return { snap, audit };
  }

  function normalizeGolden<T>(value: T, experimentId: string | undefined): T {
    if (!experimentId) return value;
    const rewrite = (item: unknown): unknown => {
      if (typeof item === "string") return item.includes(experimentId) ? item.replaceAll(experimentId, "EXPERIMENT") : item;
      if (Array.isArray(item)) return item.map(rewrite);
      if (item && typeof item === "object") {
        return Object.fromEntries(Object.entries(item).map(([key, nested]) => [key, rewrite(nested)]));
      }
      return item;
    };
    return rewrite(value) as T;
  }

  it("control and multi-restart golden engines match deep ledger/resting/reservations/audit payloads", async () => {
    const rawControl = await runGolden(false);
    const rawCrashed = await runGolden(true);
    const controlNorm = normalizeGolden(rawControl, rawControl.snap.experiment?.id);
    const crashedNorm = normalizeGolden(rawCrashed, rawCrashed.snap.experiment?.id);
    expect(crashedNorm.snap.ledger).toEqual(controlNorm.snap.ledger);
    expect(crashedNorm.snap.positions).toEqual(controlNorm.snap.positions);
    expect(crashedNorm.snap.restingOrders).toEqual(controlNorm.snap.restingOrders);
    expect(crashedNorm.snap.makerReservations).toEqual(controlNorm.snap.makerReservations);
    expect(crashedNorm.snap.settledMarkets).toEqual(controlNorm.snap.settledMarkets);
    expect(crashedNorm.snap.reserved).toEqual(controlNorm.snap.reserved);
    expect(crashedNorm.snap.experiment).toEqual(controlNorm.snap.experiment);
    expect(crashedNorm.snap.cursor).toEqual(controlNorm.snap.cursor);
    expect(crashedNorm.audit.counts).toEqual(controlNorm.audit.counts);
    expect(crashedNorm.audit.derived).toEqual(controlNorm.audit.derived);
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
