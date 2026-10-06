import { afterEach, describe, expect, it, vi } from "vitest";
import { d, settlementCashDelta, settlementRealizedDelta, type BinarySettlementLookup } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { validatePostReplayInvariants, type RecoverableEngineState } from "../apps/engine/src/recovery.js";
import { order, snapshot } from "./fixtures.js";
import {
  cleanupAllocatedLearningDataDirs, discoveryReport, paperFill, qualified,
  qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry,
} from "./paper-engine-testkit.js";

afterEach(cleanupAllocatedLearningDataDirs);
const MARKET = "orphan";
const terminal = { closed: true, umaResolutionStatus: "resolved", outcomes: ["Up", "Down"], outcomePrices: ["0", "1"] };
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}
function internals(engine: EngineService) {
  return engine as unknown as {
    cancelVanishedMakers(ids: readonly string[]): Promise<void>;
    releaseMaker(id: string, options: { cancel?: boolean; reason?: string }): Promise<void>;
    recoverableBag(): RecoverableEngineState;
    pollSettlements(): Promise<void>;
    resolveSettlementLookup(id: string, economic?: boolean): BinarySettlementLookup | null;
    economicSettlementLookups: Map<string, BinarySettlementLookup>;
    marketsById: Map<string, ReturnType<typeof qualified>>;
  };
}
async function setup() {
  const clock = { ms: Date.parse("2026-10-03T21:03:00Z") };
  const endDate = new Date(clock.ms + 120_000).toISOString();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([
    qualified(MARKET, "BTC", "orphan-up", "orphan-down", endDate),
    qualified("historical", "SOL", "historical-up", "historical-down", new Date(clock.ms - 1).toISOString()),
  ]));
  const journal = new MemoryEventJournal();
  const env = qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
  const fetcher = vi.fn<(id: string) => Promise<Record<string, unknown> | null>>().mockResolvedValue(terminal);
  const createEngine = () => new EngineService(env, journal, { error: () => undefined }, {
    market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: fetcher,
  });
  const engine = createEngine();
  await engine.start();
  return { engine, market, clock, journal, fetcher, endDate, createEngine };
}
async function open(engine: EngineService) {
  await engine.applyObservedFill(paperFill(MARKET, "DOWN", "0.89", { quantity: "5.61797753" }), "OPEN_FIRST_LEG");
  expect(engine.ledger.positions.has(MARKET)).toBe(true);
}

describe("economic settlement survives discovery reconciliation", () => {
  it("preserves a first-leg maker admitted after vanished-maker cancellation snapshots orders", async () => {
    const s = await setup();
    const reached = deferred();
    const resume = deferred();
    const first = order("MAKER", { marketId: MARKET, tokenId: "orphan-up", outcome: "UP" });
    const late = order("MAKER", { marketId: MARKET, tokenId: "orphan-up", outcome: "UP" });
    const book = snapshot({ marketId: MARKET, tokenId: "orphan-up", outcome: "UP" });
    const original = internals(s.engine).releaseMaker.bind(s.engine);
    const hold = vi.spyOn(internals(s.engine), "releaseMaker").mockImplementation(async (id, options) => {
      await original(id, options);
      if (id === first.id && options.reason === "MARKET_REMOVED") {
        // The real cancelVanishedMakers iterator already snapshotted the old
        // order. Pause its release await after the durable reservation release.
        reached.resolve();
        await resume.promise;
      }
    });
    let refresh: Promise<void> | undefined;
    const assertReservations = async () => {
      const latest = await s.engine.latestDurableSequenceForTest();
      expect(validatePostReplayInvariants(internals(s.engine).recoverableBag(),
        s.engine.lastAppliedDurableCursorForTest(), latest)).toEqual({ ok: true, reasons: [] });
    };
    try {
      await s.engine.restMakerForTest(first, book, "OPEN_FIRST_LEG");
      s.market.queue(discoveryReport([]));
      refresh = s.engine.reconcileDiscovery();
      await reached.promise;
      expect(s.engine.restingOrdersForTest().has(first.id)).toBe(false);
      expect(s.engine.makerReservationsForTest().has(first.id)).toBe(false);
      await s.engine.restMakerForTest(late, book, "OPEN_FIRST_LEG");
      expect(s.engine.ledger.positions.has(MARKET)).toBe(false);
      expect(s.engine.restingOrdersForTest().has(late.id)).toBe(true);
      expect(d(s.engine.makerReservationsForTest().get(late.id)!)).toEqual(d(s.engine.reservedCash()));
      await assertReservations();
      resume.resolve();
      await refresh;
      hold.mockRestore();
      expect(s.engine.restingOrdersForTest().get(late.id)?.intent).toBe("OPEN_FIRST_LEG");
      expect(d(s.engine.reservedCash()).gt(0)).toBe(true);
      expect(s.engine.monitoredMarketIds()).toContain(MARKET);
      expect(s.engine.subscribedTokenIds()).toContain("orphan-up");
      expect(s.engine.monitoredMarketIds()).not.toContain("historical");
      expect(s.engine.entryQualifiedIds()).not.toContain(MARKET);
      await assertReservations();
      // Once the last obligation is released, historical context is removable.
      await s.engine.cancelMakerForTest(late.id);
      s.market.queue(discoveryReport([]));
      await s.engine.reconcileDiscovery();
      expect(s.engine.monitoredMarketIds()).toEqual([]);
      expect(s.engine.restingOrderCount()).toBe(0);
      expect(s.engine.makerReservationsForTest().size).toBe(0);
      expect(d(s.engine.reservedCash()).isZero()).toBe(true);
      await assertReservations();
    } finally {
      resume.resolve();
      await refresh;
      hold.mockRestore();
      await s.engine.stop();
    }
  });

  it("re-reads obligations after cancellation yields to a first-leg fill, then evicts completed history", async () => {
    const s = await setup();
    const reached = deferred();
    const resume = deferred();
    const original = internals(s.engine).cancelVanishedMakers.bind(s.engine);
    const hold = vi.spyOn(internals(s.engine), "cancelVanishedMakers").mockImplementation(async (ids) => {
      reached.resolve();
      await resume.promise;
      await original(ids);
    });
    let refresh: Promise<void> | undefined;
    try {
      s.market.queue(discoveryReport([]));
      refresh = s.engine.reconcileDiscovery();
      await reached.promise; // The initial obligation/nextMarkets reads have completed.
      expect(s.engine.ledger.positions.has(MARKET)).toBe(false);
      await open(s.engine);
      resume.resolve();
      await refresh;
      hold.mockRestore();
      expect(s.engine.monitoredMarketIds()).toContain(MARKET);
      expect(s.engine.monitoredMarketIds()).not.toContain("historical");
      expect(internals(s.engine).resolveSettlementLookup(MARKET, true)).toMatchObject({ marketId: MARKET, endDate: s.endDate });
      expect(s.engine.entryQualifiedIds()).not.toContain(MARKET);
      s.clock.ms += 120_001;
      await internals(s.engine).pollSettlements();
      s.market.queue(discoveryReport([]));
      await s.engine.reconcileDiscovery();
      expect(s.engine.monitoredMarketIds()).toEqual([]);
      expect(internals(s.engine).economicSettlementLookups.size).toBe(0);
      // Rediscovery of history cannot create another economic settlement.
      s.market.queue(discoveryReport([qualified(MARKET, "BTC", "orphan-up", "orphan-down", s.endDate)]));
      await s.engine.reconcileDiscovery();
      await internals(s.engine).pollSettlements();
      expect((await s.journal.readDurableTransitionsAfter(0, 100)).filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
    } finally {
      resume.resolve();
      await refresh;
      hold.mockRestore();
      await s.engine.stop();
    }
  });

  it.each([false, true])("settles with live context missing=%s, emitting correct economics exactly once", async (dropLive) => {
    const s = await setup();
    try {
      await open(s.engine);
      const fillAuthority = (await s.journal.readDurableTransitionsAfter(0, 100)).find((row) => row.type === "PAPER_FILL");
      expect(internals(s.engine).resolveSettlementLookup(MARKET, true)).toMatchObject({ marketId: MARKET, yesMapsTo: "UP", noMapsTo: "DOWN", endDate: s.endDate });
      const position = s.engine.ledger.positions.get(MARKET)!;
      const before = s.engine.ledger.snapshot();
      if (dropLive) s.engine.dropLiveMarketForTest(MARKET);
      await internals(s.engine).pollSettlements();
      expect(s.fetcher).not.toHaveBeenCalled(); // End date is authoritative.
      s.clock.ms += 120_001;
      await internals(s.engine).pollSettlements();
      const after = s.engine.ledger.snapshot();
      expect(d(after.cash).minus(before.cash).toFixed()).toBe(settlementCashDelta(position, "0", "1"));
      expect(d(after.realizedPnl).minus(before.realizedPnl).toFixed()).toBe(settlementRealizedDelta(position, "0", "1"));
      expect(fillAuthority?.payload.settlementLookup).toMatchObject({ marketId: MARKET, yesMapsTo: "UP", noMapsTo: "DOWN", endDate: s.endDate });
      expect(s.engine.ledger.positions.has(MARKET)).toBe(false);
      expect(internals(s.engine).economicSettlementLookups.size).toBe(0);
      const events = await s.journal.queryEvents({ types: ["POSITION_SETTLED", "MARKET_RESOLUTION_OBSERVED"] });
      expect(events.filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
      expect(events.find((row) => row.type === "MARKET_RESOLUTION_OBSERVED")?.payload.winner).toBe("DOWN");
      const fetches = s.fetcher.mock.calls.length;
      await internals(s.engine).pollSettlements();
      expect(s.engine.ledger.snapshot()).toEqual(after);
      expect(s.fetcher.mock.calls.length).toBe(fetches);
      expect((await s.journal.readDurableTransitionsAfter(0, 100)).filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });

  it.each([
    ["null", null],
    ["non-terminal", { ...terminal, closed: false, umaResolutionStatus: "proposed" }],
    ["probabilistic", { ...terminal, outcomePrices: ["0.4", "0.6"] }],
    ["same payoff", { ...terminal, outcomePrices: ["1", "1"] }],
    ["malformed outcomes", { ...terminal, outcomes: '["Up",' }],
    ["malformed payoff", { ...terminal, outcomePrices: '["0",' }],
  ])("keeps orphan inventory unresolved for %s and preserves backoff", async (_name, raw) => {
    const s = await setup();
    try {
      await open(s.engine);
      s.engine.dropLiveMarketForTest(MARKET);
      s.fetcher.mockResolvedValue(raw as Record<string, unknown> | null);
      const before = s.engine.ledger.snapshot();
      s.clock.ms += 120_001;
      await internals(s.engine).pollSettlements();
      expect(s.engine.ledger.positions.has(MARKET)).toBe(true);
      expect(s.engine.ledger.snapshot()).toEqual(before);
      expect(await s.journal.queryEvents({ types: ["POSITION_SETTLED", "MARKET_RESOLUTION_OBSERVED"] })).toEqual([]);
      expect(s.engine.settlementBackoffCountForTest()).toBe(1);
      const calls = s.fetcher.mock.calls.length;
      await internals(s.engine).pollSettlements();
      expect(s.fetcher.mock.calls.length).toBe(calls);
      s.clock.ms += 2 * s.engine.env.SETTLEMENT_POLL_MS;
      await internals(s.engine).pollSettlements();
      expect(s.fetcher.mock.calls.length).toBeGreaterThan(calls);
    } finally { await s.engine.stop(); }
  });

  it("preserves fill-time inverted YES/NO mapping despite rediscovery and slug fallback", async () => {
    const s = await setup();
    try {
      const live = internals(s.engine).marketsById.get(MARKET)!;
      live.qualification.binary.yesOutcome = "DOWN";
      live.qualification.binary.noOutcome = "UP";
      await open(s.engine);
      live.qualification.binary.yesOutcome = "UP";
      live.qualification.binary.noOutcome = "DOWN";
      s.engine.dropLiveMarketForTest(MARKET);
      s.fetcher.mockImplementation(async (id) => id === MARKET ? null : { ...terminal, outcomes: ["Yes", "No"], outcomePrices: ["1", "0"] });
      s.clock.ms += 120_001;
      await internals(s.engine).pollSettlements();
      expect(s.fetcher.mock.calls.map(([id]) => id)).toEqual([MARKET, live.slug]);
      expect((await s.journal.queryEvents({ types: ["MARKET_RESOLUTION_OBSERVED"] }))[0].payload.winner).toBe("DOWN");
    } finally { await s.engine.stop(); }
  });

  it("does not settle inventory closed while public resolution is in flight", async () => {
    const s = await setup();
    const reached = deferred();
    const resume = deferred();
    let poll: Promise<void> | undefined;
    try {
      await open(s.engine);
      s.engine.dropLiveMarketForTest(MARKET);
      s.clock.ms += 120_001;
      s.fetcher.mockImplementation(async () => { reached.resolve(); await resume.promise; return terminal; });
      poll = internals(s.engine).pollSettlements();
      await reached.promise;
      await s.engine.applyObservedFill(paperFill(MARKET, "DOWN", "0.9", { side: "SELL", quantity: "5.61797753" }));
      expect(s.engine.ledger.positions.has(MARKET)).toBe(false);
      const afterSell = s.engine.ledger.snapshot();
      resume.resolve();
      await poll;
      expect(s.engine.ledger.snapshot()).toEqual(afterSell);
      expect(internals(s.engine).economicSettlementLookups.size).toBe(0);
      expect(await s.journal.queryEvents({ types: ["POSITION_SETTLED", "MARKET_RESOLUTION_OBSERVED"] })).toEqual([]);
    } finally { resume.resolve(); await poll; await s.engine.stop(); }
  });

  it("concurrent settlement polls create only one durable economic transition", async () => {
    const s = await setup();
    try {
      await open(s.engine);
      s.engine.dropLiveMarketForTest(MARKET);
      s.clock.ms += 120_001;
      const before = s.engine.ledger.snapshot();
      await Promise.all([internals(s.engine).pollSettlements(), internals(s.engine).pollSettlements()]);
      expect(d(s.engine.ledger.snapshot().cash).minus(before.cash).toFixed()).toBe("5.61797753");
      expect(s.engine.integrityFault()).toBe(false);
      expect((await s.journal.readDurableTransitionsAfter(0, 100)).filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });

  it.each(["mapping", "end date"])("fails closed when authoritative %s is absent", async (missing) => {
    const s = await setup();
    try {
      const live = internals(s.engine).marketsById.get(MARKET)!;
      if (missing === "mapping") live.qualification.binary.noOutcome = "UP";
      else live.endDate = "invalid";
      await open(s.engine);
      s.engine.dropLiveMarketForTest(MARKET);
      s.clock.ms += 120_001;
      await internals(s.engine).pollSettlements();
      expect(s.fetcher).not.toHaveBeenCalled();
      expect(s.engine.ledger.positions.has(MARKET)).toBe(true);
      expect(await s.journal.queryEvents({ types: ["POSITION_SETTLED"] })).toEqual([]);
    } finally { await s.engine.stop(); }
  });

  it.each([false, true])("recovers orphan identity from checkpoint=%s and never replays settlement cash twice", async (checkpoint) => {
    const s = await setup();
    let recovered: EngineService | undefined;
    let restarted: EngineService | undefined;
    try {
      if (checkpoint) expect((await s.engine.beginExperiment()).ok).toBe(true);
      await open(s.engine);
      s.engine.dropLiveMarketForTest(MARKET);
      if (checkpoint) await s.engine.checkpointForTest();
      // Prevent stop from checkpointing, forcing the selected recovery boundary.
      s.engine.abandonWithoutCheckpointForTest();
      await s.engine.stop();
      s.clock.ms += 120_001;
      s.market.queue(discoveryReport([]));
      recovered = s.createEngine();
      await recovered.start();
      expect(recovered.monitoredMarketIds()).not.toContain(MARKET);
      expect(internals(recovered).resolveSettlementLookup(MARKET, true)).toMatchObject({ endDate: s.endDate, yesMapsTo: "UP", noMapsTo: "DOWN" });
      await internals(recovered).pollSettlements();
      const settled = recovered.ledger.snapshot();
      recovered.abandonWithoutCheckpointForTest();
      await recovered.stop();
      const fetches = s.fetcher.mock.calls.length;
      s.market.queue(discoveryReport([]));
      restarted = s.createEngine();
      await restarted.start();
      await internals(restarted).pollSettlements();
      expect(restarted.ledger.snapshot().cash).toBe(settled.cash);
      expect(restarted.ledger.snapshot().realizedPnl).toBe(settled.realizedPnl);
      expect(restarted.ledger.positions.has(MARKET)).toBe(false);
      expect(s.fetcher.mock.calls.length).toBe(fetches);
      expect((await s.journal.readDurableTransitionsAfter(0, 100)).filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
    } finally {
      await s.engine.stop();
      await recovered?.stop();
      await restarted?.stop();
    }
  });
});
