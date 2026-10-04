import { describe, expect, it, vi } from "vitest";
import { calculateFee, createId, d, engineEnvSchema, observeNakedDirectionalPosition, createNakedDirectionalDiagnostic,
  SOAK_PATH_LIMIT, SOAK_ANOMALY_LIMIT, SOAK_PERSIST_PATH_LIMIT, noteNakedDirectionalAnomaly, type MarketSnapshot, type PaperOrder,
  type ReferenceTick, type NakedDirectionalDiagnostic, type PaperFill, type TriggerEvent, type ConsensusDecision, type NativeDecision } from "@pm/core";
import { MemoryEventJournal, isSemanticallyValidCheckpointPayload } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { type RiskReductionOwnership, unhedgedEpisode } from "../apps/engine/src/recovery.js";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import { streamExperimentSummary } from "../apps/engine/src/final-report.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot, order, feeSchedule } from "./fixtures.js";

const START = Date.parse("2026-01-01T00:00:00Z"), MARKET = "soak-mkt";
type Outcome = "UP" | "DOWN";
type Internals = {
  nakedDirectionalDiagnostics: Map<string, NakedDirectionalDiagnostic>;
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  maybeProtectiveDeRisk(marketId: string): Promise<void>;
  maybeEarlyCatastrophicPairLock(marketId: string): Promise<{ acted: boolean }>;
  maybeGuardianProtectiveSell(marketId: string): Promise<boolean>;
  evaluateGuardian(marketId: string): { reason: string; decision: { accepted: boolean } | null };
  admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean>;
  submitPaperOrder(order: PaperOrder, book: MarketSnapshot, key: string, intent: string): Promise<boolean>;
  flushSoakDiagnostics(): Promise<void>;
  observeSoakPosition(marketId: string): void;
  evaluateTrade(trigger: TriggerEvent, consensus: ConsensusDecision, native: NativeDecision): Promise<void>;
  settleMarket(marketId: string, lookup: { marketId: string; slug: string | null; endDate: string; yesMapsTo: Outcome; noMapsTo: Outcome }): Promise<void>;
  activeTriggers: Map<string, TriggerEvent>;
  snapshots: Map<string, Partial<Record<Outcome, MarketSnapshot>>>;
};
const internal = (engine: EngineService) => engine as unknown as Internals;

async function start(enabled = true, opts: { journal?: MemoryEventJournal; clock?: { ms: number }; closePolicy?: string; duration?: string; secondMarket?: boolean } = {}) {
  const clock = opts.clock ?? { ms: START }, journal = opts.journal ?? new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified(MARKET, "XRP", `${MARKET}-up`, `${MARKET}-down`, new Date(START + 3_600_000).toISOString()),
    ...(opts.secondMarket ? [qualified("soak-other", "XRP", "soak-other-up", "soak-other-down", new Date(START + 7_200_000).toISOString())] : [])]));
  const resolution = { winner: null as Outcome | null };
  const engine = new EngineService(qualifiedPaperEnv({ NAKED_DIRECTIONAL_SOAK_ENABLED: String(enabled),
    EXPERIMENT_CLOSE_POLICY: opts.closePolicy ?? "observe_until_settlement", EXPERIMENT_DURATION_HOURS: opts.duration ?? "48",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0" }),
  journal, { error: () => undefined }, { market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.ms), resolutionFetcher: async () => resolution.winner === null ? null : {
      closed: true, umaResolutionStatus: "resolved", outcomes: ["Up", "Down"],
      outcomePrices: resolution.winner === "UP" ? ["1", "0"] : ["0", "1"],
    } });
  await engine.start(); await market.waitForHandlers();
  return { engine, journal, market, clock, resolution };
}
type Scenario = Awaited<ReturnType<typeof start>>;
function book(s: Scenario, outcome: Outcome, price: string, overrides: Partial<MarketSnapshot> = {}) {
  return snapshot({ eventId: createId("book"), traceId: createId("trace"), marketId: MARKET, tokenId: `${MARKET}-${outcome.toLowerCase()}`,
    outcome, asset: "XRP", sourceTimestamp: new Date(s.clock.ms).toISOString(), receivedAt: new Date(s.clock.ms).toISOString(),
    bids: [{ price, quantity: "10000" }], asks: [{ price: d(price).plus("0.01").toFixed(), quantity: "10000" }], ...overrides });
}
function setBook(s: Scenario, outcome: Outcome, price: string, overrides: Partial<MarketSnapshot> = {}) {
  const b = book(s, outcome, price, overrides);
  const pair = internal(s.engine).snapshots.get(MARKET) ?? {};
  pair[outcome] = b; internal(s.engine).snapshots.set(MARKET, pair);
  s.engine.state.marketSnapshots.set(b.tokenId, b); s.market.markTokenSync(b.tokenId, "SYNCED");
  return b;
}
async function seed(s: Scenario, outcome: Outcome = "UP", fee = "0") {
  expect((await s.engine.beginExperiment()).ok).toBe(true);
  setBook(s, "UP", "0.79"); setBook(s, "DOWN", "0.39");
  const fill = paperFill(MARKET, outcome, "0.80", { quantity: "5", feeUsd: fee, timestamp: new Date(s.clock.ms).toISOString(), feeSchedule });
  await s.engine.applyObservedFill(fill, "OPEN_FIRST_LEG");
  s.clock.ms += 10_000;
  return fill;
}
const rows = (s: Scenario, type: string) => s.journal.queryEvents({ types: [type] });
async function drain(s: Scenario) {
  await expect.poll(() => s.engine.runtimeSupervisorSnapshot().unresolved, { timeout: 10_000 }).toBe(0);
}
async function drive(s: Scenario, oppositePrice: string, heldPrice = "0.60") {
  setBook(s, "UP", heldPrice); setBook(s, "DOWN", oppositePrice, { asks: [{ price: oppositePrice, quantity: "10000" }] });
  s.engine.state.noteReferenceTick("XRP", new Date(s.clock.ms).toISOString());
  await internal(s.engine).maybeProtectiveDeRisk(MARKET);
}

describe("naked directional PAPER authority", { timeout: 30_000 }, () => {
  it("defaults off, parses false explicitly, and retains PAPER-only validation", () => {
    expect(engineEnvSchema.parse({}).NAKED_DIRECTIONAL_SOAK_ENABLED).toBe(false);
    expect(engineEnvSchema.parse({ NAKED_DIRECTIONAL_SOAK_ENABLED: "false" }).NAKED_DIRECTIONAL_SOAK_ENABLED).toBe(false);
    expect(engineEnvSchema.parse({ NAKED_DIRECTIONAL_SOAK_ENABLED: "true" }).NAKED_DIRECTIONAL_SOAK_ENABLED).toBe(true);
    expect(() => engineEnvSchema.parse({ TRADING_MODE: "live", NAKED_DIRECTIONAL_SOAK_ENABLED: "true" })).toThrow();
  });

  it("executes production first legs identically with the flag off/on through native entry and actual PAPER orders", async () => {
    const economics: Record<string, unknown>[] = [];
    for (const enabled of [false, true]) {
      const s = await start(enabled);
      try {
        expect((await s.engine.beginExperiment()).ok).toBe(true);
        await s.market.emitSnapshot(book(s, "UP", "0.39")); await s.market.emitSnapshot(book(s, "DOWN", "0.39"));
        const tick = (price: string): ReferenceTick => ({ eventId: createId("reference"), traceId: createId("trace"), asset: "XRP",
          symbol: "XRPUSDT", venue: "test", price, quantity: "1", sourceTimestamp: new Date(s.clock.ms).toISOString(),
          receivedAt: new Date(s.clock.ms).toISOString(), tradeId: createId("trade") });
        await s.engine.ingestReferenceTickForTest(tick("100")); s.clock.ms += 1_000;
        await s.engine.ingestReferenceTickForTest(tick("102")); await drain(s);
        const fills = (await rows(s, "PAPER_FILL")).filter((row) => row.payload.intent === "OPEN_FIRST_LEG");
        expect(fills).toHaveLength(1);
        const f = fills[0]!.payload;
        economics.push({ price: f.fillPrice, quantity: f.quantity, fee: f.feeUsd, outcome: f.outcome, latency: f.latencyMs });
        expect((await rows(s, "PAPER_ORDER_CREATED")).filter((row) => row.payload.intent === "OPEN_FIRST_LEG")).toHaveLength(1);
        expect(internal(s.engine).nakedDirectionalDiagnostics.size).toBe(enabled ? 1 : 0);
      } finally { await s.engine.stop(); }
    }
    expect(economics[1]).toEqual(economics[0]);
  });

  it.each([
    ["normal .04+ hedge", "0.10", 10_000, "HEDGE_SECOND_LEG"],
    ["PR3 catastrophic lock", "0.30", 10_000, "RESCUE_SECOND_LEG"],
    ["Guardian adverse movement", "0.40", 10_000, "GUARDIAN_PROTECTIVE_SELL"],
    ["deadline protection", "0.40", 310_000, "PROTECTIVE_DE_RISK"],
  ] as const)("%s executes when OFF and leaves the real position untouched when ON", async (_, opposite, age, intent) => {
    for (const enabled of [false, true]) {
      const s = await start(enabled);
      try {
        await seed(s); s.clock.ms = START + age;
        await drive(s, opposite);
        if (!enabled) {
          expect((await rows(s, "PAPER_FILL")).filter((row) => row.payload.intent === intent)).toHaveLength(1);
        } else {
          await internal(s.engine).maybeEarlyCatastrophicPairLock(MARKET);
          await internal(s.engine).maybeGuardianProtectiveSell(MARKET);
          expect((await rows(s, "PAPER_ORDER_CREATED")).filter((row) => row.payload.intent !== "OPEN_FIRST_LEG")).toHaveLength(0);
          expect(await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 100 })).toHaveLength(0);
          expect(internal(s.engine).riskReductionOwners.size).toBe(0);
          expect(s.engine.ledger.positions.get(MARKET)).toMatchObject({ unpairedQuantity: "5", pairedQuantity: "0", totalFees: "0" });
        }
      } finally { await s.engine.stop(); }
    }
  });

  it.each(["HEDGE_SECOND_LEG", "RESCUE_SECOND_LEG", "GUARDIAN_PROTECTIVE_SELL", "PROTECTIVE_DE_RISK"] as const)(
    "blocks %s at order, owner, maker and fill boundaries", async (intent) => {
      const s = await start();
      try {
        await seed(s);
        const side = intent === "GUARDIAN_PROTECTIVE_SELL" || intent === "PROTECTIVE_DE_RISK" ? "SELL" : "BUY";
        const outcome = side === "SELL" ? "UP" : "DOWN";
        const b = setBook(s, outcome, "0.20");
        const o = order("TAKER", { marketId: MARKET, tokenId: b.tokenId, outcome, side, quantity: "5", requestedPrice: "0.20" });
        expect(await internal(s.engine).submitPaperOrder(o, b, "soak-direct", intent)).toBe(false);
        expect(await internal(s.engine).admitRiskReduction({ marketId: MARKET, unhedgedStartedAt: unhedgedEpisode(s.engine.ledger.positions.get(MARKET)!),
          intent, order: o, admittedResidualQuantity: "5", decisionReceivedAt: new Date(s.clock.ms).toISOString(), state: "ADMITTED" })).toBe(false);
        await s.engine.restMakerForTest({ ...o, orderType: "MAKER" }, b, "HEDGE_SECOND_LEG");
        await s.engine.applyObservedFill(paperFill(MARKET, outcome, "0.20", { side, quantity: "5", timestamp: new Date(s.clock.ms).toISOString() }), intent);
        expect(s.engine.restingOrderCount()).toBe(0);
        expect(s.engine.ledger.positions.get(MARKET)?.unpairedQuantity).toBe("5");
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
        expect(await rows(s, "PAPER_FILL")).toHaveLength(1);
      } finally { await s.engine.stop(); }
    });

  it("gates evaluateTrade's production normal hedge candidate before risk admission", async () => {
    for (const enabled of [false, true]) {
      const s = await start(enabled);
      try {
        await seed(s); setBook(s, "UP", "0.60"); setBook(s, "DOWN", "0.09", { asks: [{ price: "0.10", quantity: "10000" }] });
        s.engine.state.noteReferenceTick("XRP", new Date(s.clock.ms).toISOString());
        const timestamp = new Date(s.clock.ms).toISOString();
        const trigger = { id: "direct-hedge-trigger", traceId: "direct-hedge", marketId: MARKET, asset: "XRP", kind: "scheduled_review",
          severity: 2, dedupeKey: "direct-hedge", evidence: {}, sourceEventIds: [], evidenceIds: [], createdAt: timestamp,
          expiresAt: new Date(s.clock.ms + 30000).toISOString() } as TriggerEvent;
        internal(s.engine).activeTriggers.set(trigger.id, trigger);
        const consensus = { id: "direct-consensus", traceId: trigger.traceId, triggerId: trigger.id, consensusDirection: "UP", consensusConfidence: 0.9,
          agentAgreement: 1, signalFreshness: 1, conflicts: [], estimatedEdge: 0.1, fairProbability: 0.9, marketProbability: 0.8,
          evidenceIds: [], calculatedAt: timestamp, authority: "NATIVE_DETERMINISTIC_V1", featureVector: {} } as ConsensusDecision;
        await internal(s.engine).evaluateTrade(trigger, consensus, { direction: "UP", confidence: 0.9, fairProbability: 0.9,
          marketProbability: 0.8, featureVector: {} } as NativeDecision);
        expect((await rows(s, "PAPER_FILL")).filter((row) => row.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(enabled ? 0 : 1);
      } finally { await s.engine.stop(); }
    }
  });

  it("waits until every diagnostic position resolves", async () => {
    const s = await start(true, { secondMarket: true, duration: "0.001" });
    try {
      await seed(s);
      await s.engine.applyObservedFill(paperFill("soak-other", "DOWN", "0.30", {
        quantity: "3", timestamp: new Date(s.clock.ms).toISOString(), feeUsd: "0", feeSchedule,
      }), "OPEN_FIRST_LEG");
      await s.engine.tickMaintenance(); expect(s.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      s.resolution.winner = "UP"; s.clock.ms = START + 3_600_001;
      await s.engine.tickMaintenance(); await s.engine.tickMaintenance();
      expect(s.engine.ledger.positions.size).toBe(1); expect(s.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      s.clock.ms = START + 7_200_001; await s.engine.tickMaintenance(); await s.engine.tickMaintenance();
      expect(s.engine.ledger.positions.size).toBe(0); expect(s.engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(await rows(s, "POSITION_SETTLED")).toHaveLength(2); expect(await rows(s, "NAKED_DIRECTIONAL_SUMMARY")).toHaveLength(2);
    } finally { await s.engine.stop(); }
  });

  it("keeps database access out of the observer and caps incremental durable samples", async () => {
    const s = await start();
    try {
      await seed(s);
      const read = vi.spyOn(s.journal, "readDurableByOperationKey"), query = vi.spyOn(s.journal, "queryEvents"), health = vi.spyOn(s.journal, "healthCheck");
      for (let i = 0; i < 200; i++) {
        s.clock.ms += 1;
        setBook(s, "UP", "0.70"); setBook(s, "DOWN", "0.10", { asks: [{ price: d("0.1").plus(d(i % 20).div(1000)).toFixed(), quantity: "1000" }] });
        internal(s.engine).observeSoakPosition(MARKET);
      }
      expect(read).not.toHaveBeenCalled(); expect(query).not.toHaveBeenCalled(); expect(health).not.toHaveBeenCalled();
      await internal(s.engine).flushSoakDiagnostics();
      const batches = await s.journal.queryDurableTransitions({ types: ["NAKED_DIRECTIONAL_OBSERVATION"], limit: 100 });
      const saved = (batches[0]!.payload.diagnostics as NakedDirectionalDiagnostic[])[0]!;
      expect(saved.path.length).toBeLessThanOrEqual(SOAK_PERSIST_PATH_LIMIT);
      s.clock.ms += 6000; internal(s.engine).observeSoakPosition(MARKET); await internal(s.engine).flushSoakDiagnostics();
      const next = await s.journal.queryDurableTransitions({ types: ["NAKED_DIRECTIONAL_OBSERVATION"], limit: 100 });
      const delta = (next.at(-1)!.payload.diagnostics as NakedDirectionalDiagnostic[])[0]!;
      expect(delta.path.every((point) => point.sequence > saved.observationCount)).toBe(true);
      expect(internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.path.length).toBeGreaterThan(SOAK_PERSIST_PATH_LIMIT);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it.each(["UP", "DOWN"] as const)("entry close waits for %s resolution, settles exactly once and includes a canonical diagnostic report", async (winner) => {
    const s = await start(true, { closePolicy: "mark_to_market", duration: "0.001" });
    try {
      const fill = await seed(s, "UP", "0.05");
      expect(s.engine.experimentManifest()?.closePolicy).toBe("observe_until_settlement");
      await s.market.emitSnapshot(book(s, "UP", "0.31"));
      await s.market.emitSnapshot(book(s, "DOWN", "0.08", { asks: [{ price: "0.10", quantity: "10000" }] }));
      await s.engine.tickMaintenance();
      expect(s.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(s.engine.ledger.positions.get(MARKET)?.unpairedQuantity).toBe("5");
      const firstOrders = (await rows(s, "PAPER_ORDER_CREATED")).length;
      await s.engine.ingestReferenceTickForTest({ eventId: "late-ref", traceId: "late", asset: "XRP", symbol: "XRPUSDT", venue: "test", price: "200",
        quantity: "1", sourceTimestamp: new Date(s.clock.ms).toISOString(), receivedAt: new Date(s.clock.ms).toISOString(), tradeId: "late" });
      await drain(s);
      expect((await rows(s, "PAPER_ORDER_CREATED")).length).toBe(firstOrders);
      expect(await rows(s, "MARK_TO_MARKET_CLOSED")).toHaveLength(0);
      s.clock.ms = START + 3_600_001; s.resolution.winner = winner;
      await s.engine.tickMaintenance(); await s.engine.tickMaintenance();
      await s.engine.applyObservedFill(fill, "OPEN_FIRST_LEG");
      expect(s.engine.ledger.positions.size).toBe(0);
      expect(s.engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(await rows(s, "PAPER_FILL")).toHaveLength(1);
      expect(await rows(s, "POSITION_SETTLED")).toHaveLength(1);
      expect(await s.journal.queryDurableTransitions({ types: ["POSITION_SETTLED"], limit: 100 })).toHaveLength(1);
      const summaries = await rows(s, "NAKED_DIRECTIONAL_SUMMARY"); expect(summaries).toHaveLength(1);
      expect(summaries[0]!.payload).toMatchObject({ winner, actualPaperResult: winner === "UP" ? "0.95" : "-4.05", lifetimeMs: 3_600_001 });
      const manifest = s.engine.experimentManifest()!;
      const report = await streamExperimentSummary(s.journal, manifest, { reportKind: "final", complete: true });
      expect(report.summary.nakedDirectionalDiagnostics).toHaveLength(1);
      const acc = ExperimentSummaryAccumulator.fromSnapshot(report.accumulatorSnapshot!);
      expect(acc.finalize().nakedDirectionalDiagnostics).toEqual(report.summary.nakedDirectionalDiagnostics);
    } finally { await s.engine.stop(); }
  });

  it("persists exact between-sample extrema and recovers the mode with the runtime flag off", async () => {
    const s = await start(); let restarted: Scenario | undefined;
    try {
      await seed(s);
      s.clock.ms = START + 10_001; await s.market.emitSnapshot(book(s, "UP", "0.12345678"));
      await s.market.emitSnapshot(book(s, "DOWN", "0.12", { asks: [{ price: "0.12345678", quantity: "10000" }] }));
      s.clock.ms += 1; await s.market.emitSnapshot(book(s, "UP", "0.91"));
      s.clock.ms += 1; await s.market.emitSnapshot(book(s, "UP", "0.40"));
      await drain(s); await internal(s.engine).flushSoakDiagnostics();
      const diagnostic = structuredClone(internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!);
      expect(diagnostic.minDirectionalPnl).toMatchObject({ value: "-3.3827161", at: new Date(START + 10_001).toISOString() });
      expect(diagnostic.maxDirectionalPnl?.value).toBe("0.55");
      expect(diagnostic.maxExecutablePairEdge?.value).toBe("0.07654322");
      s.engine.abandonWithoutCheckpointForTest(); await s.engine.stop();
      restarted = await start(false, { journal: s.journal, clock: s.clock });
      expect(internal(restarted.engine).nakedDirectionalDiagnostics.get(MARKET)?.minDirectionalPnl).toEqual(diagnostic.minDirectionalPnl);
      expect(restarted.engine.experimentManifest()?.riskConfiguration.nakedDirectionalSoakEnabled).toBe(true);
      await drive(restarted, "0.10");
      expect(restarted.engine.ledger.positions.get(MARKET)?.unpairedQuantity).toBe("5");
      expect(await rows(restarted, "PAPER_FILL")).toHaveLength(1);
    } finally { if (restarted) await restarted.engine.stop(); else await s.engine.stop(); }
  });

  it("preserves checkpoint diagnostics, rejects malformed snapshots, and keeps hard integrity fences", async () => {
    const s = await start();
    try {
      await seed(s); await s.engine.checkpointForTest();
      const checkpoint = await s.journal.loadLatestCheckpoint();
      expect(checkpoint?.payload.nakedDirectionalDiagnostics).toHaveLength(1);
      expect(isSemanticallyValidCheckpointPayload(checkpoint!.payload)).toBe(true);
      expect(isSemanticallyValidCheckpointPayload({ ...checkpoint!.payload, nakedDirectionalDiagnostics: [{}] })).toBe(false);
      s.engine.abandonWithoutCheckpointForTest();
      expect(s.engine.state.overloadState === "FAIL_CLOSED" || s.engine.recoveryFailClosedForTest()).toBe(true);
    } finally { await s.engine.stop(); }
  });

  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)(
    "repairs the settlement summary exactly once after a crash at %s", async (point) => {
      const s = await start(true, { duration: "0.001" }); let restarted: Scenario | undefined;
      try {
        await seed(s, "UP", "0.05"); await s.engine.closeExperimentEntries();
        s.resolution.winner = "UP"; s.clock.ms = START + 3_600_001;
        const operationKey = `settlement:${MARKET}:${s.engine.experimentManifest()!.id}`;
        s.engine.setCrashInjectionForTest(point, { operationKey });
        await expect(internal(s.engine).settleMarket(MARKET, { marketId: MARKET, slug: null,
          endDate: new Date(START + 3_600_000).toISOString(), yesMapsTo: "UP", noMapsTo: "DOWN" })).rejects.toThrow();
        s.engine.abandonWithoutCheckpointForTest(); await s.engine.stop();
        restarted = await start(false, { journal: s.journal, clock: s.clock });
        await restarted.engine.tickMaintenance(); await restarted.engine.tickMaintenance();
        expect(restarted.engine.ledger.positions.size).toBe(0);
        expect(restarted.engine.ledger.snapshot().cash).toBe("1000.95");
        expect(await rows(restarted, "PAPER_FILL")).toHaveLength(1);
        expect(await rows(restarted, "POSITION_SETTLED")).toHaveLength(1);
        expect(await rows(restarted, "NAKED_DIRECTIONAL_SUMMARY")).toHaveLength(1);
        expect(await restarted.journal.queryDurableTransitions({ types: ["POSITION_SETTLED"], limit: 100 })).toHaveLength(1);
      } finally { if (restarted) await restarted.engine.stop(); else await s.engine.stop(); }
    });
});

describe("real-position diagnostic observations", () => {
  it("uses full real depth/VWAP and fees, rejects unavailable books and tracks arbitrary edge changes", async () => {
    const s = await start();
    try {
      await seed(s, "UP", "0.05");
      await s.market.emitSnapshot(book(s, "UP", "0.70"));
      await s.market.emitSnapshot(book(s, "DOWN", "0.10", { asks: [{ price: "0.10", quantity: "2" }, { price: "0.20", quantity: "3" }] }));
      let point = internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.current!;
      expect(point).toMatchObject({ oppositeVwap: "0.16", oppositeDepth: "5", grossPairEdge: "0.04", directionalPnl: "-0.55" });
      const fee = calculateFee("5", "0.16", "TAKER", feeSchedule);
      expect(point.oppositeFee).toBe(fee);
      expect(point.lockedPairPnl).toBe(d("0.2").minus("0.05").minus(fee).toFixed());
      s.clock.ms += 17; await s.market.emitSnapshot(book(s, "DOWN", "0.14", { asks: [{ price: "0.13789", quantity: "5" }] }));
      point = internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.current!;
      expect(point.grossPairEdge).toBe("0.06211"); expect(point.sincePreviousPairChangeMs).toBe(17);
      await s.market.emitSnapshot(book(s, "DOWN", "0.05", { asks: [{ price: "0.05", quantity: "1" }] }));
      expect(internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.current!.grossPairEdge).toBeNull();
      await s.market.emitSnapshot(book(s, "DOWN", "0.01", { receivedAt: new Date(s.clock.ms + 10).toISOString() }));
      expect(internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.current!.oppositeFreshness).toBe("FUTURE");
      expect(internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!.maxExecutablePairEdge?.value).toBe("0.06211");
      s.clock.ms += 20_000; await s.engine.tickMaintenance();
      s.market.emitGap([`${MARKET}-down`], "count_limit");
      await s.engine.applyReferenceConnectionForTest("XRP", "DISCONNECTED", 1);
      const diagnostic = internal(s.engine).nakedDirectionalDiagnostics.get(MARKET)!;
      expect(diagnostic.anomalyCounts.SLOW_CONSUMER).toBe(1);
      expect(diagnostic.anomalyCounts.RECONNECT).toBeGreaterThan(0);
      expect(diagnostic.anomalyCounts.BOOK_STALE).toBeGreaterThan(0);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });

  it("bounds lifetime paths/anomalies while preserving exact extrema beyond compaction", () => {
    const fill: PaperFill = paperFill(MARKET, "UP", "0.80", { quantity: "5", timestamp: new Date(START).toISOString() });
    // Use the actual ledger position schema from a real fill, without engine I/O.
    const position = { id: "real-position", marketId: MARKET, upQuantity: "5", downQuantity: "0", averageUpCost: "0.8", averageDownCost: "0",
      pairedQuantity: "0", unpairedOutcome: "UP" as const, unpairedQuantity: "5", combinedPairCost: "0.8", totalFees: "0", totalSlippage: "0",
      openedAt: fill.timestamp, updatedAt: fill.timestamp };
    const diagnostic = createNakedDirectionalDiagnostic(position, fill);
    for (let i = 0; i < 1000; i++) {
      const now = new Date(START + i), stamp = now.toISOString();
      const held = snapshot({ marketId: MARKET, outcome: "UP", bids: [{ price: i === 9 ? "0.00000001" : "0.9", quantity: "5" }], receivedAt: stamp, sourceTimestamp: stamp });
      const opposite = snapshot({ marketId: MARKET, outcome: "DOWN", asks: [{ price: i === 11 ? "0.01234567" : d("0.1").plus(d(i % 20).div(1000)).toFixed(), quantity: "5" }], receivedAt: stamp, sourceTimestamp: stamp });
      observeNakedDirectionalPosition(diagnostic, position, { held, opposite, heldSynchronized: true, oppositeSynchronized: true, now, staleTimeoutMs: 15000, feedState: "CONNECTED" });
      noteNakedDirectionalAnomaly(diagnostic, now, "TEST_ANOMALY", {});
    }
    expect(diagnostic.path.length).toBeLessThanOrEqual(SOAK_PATH_LIMIT);
    expect(diagnostic.path[0]!.ageMs).toBe(0); expect(diagnostic.path.at(-1)!.ageMs).toBe(999);
    expect(diagnostic.pathCompactions).toBeGreaterThan(0);
    expect(diagnostic.minDirectionalPnl).toMatchObject({ value: "-3.99999995", ageMs: 9 });
    expect(diagnostic.maxExecutablePairEdge).toMatchObject({ value: "0.18765433", ageMs: 11 });
    expect(diagnostic.anomalies).toHaveLength(SOAK_ANOMALY_LIMIT);
    expect(diagnostic.anomalyCounts.TEST_ANOMALY).toBe(1000);
  });
});
