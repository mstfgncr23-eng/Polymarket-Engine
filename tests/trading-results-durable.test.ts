import { describe, expect, it } from "vitest";
import { createIdleExperiment, PaperLedger, type PaperFill } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { applyDurableTransition, cloneRecoverableState, plannedDurableTransition, validatePostReplayInvariants, type RecoverableEngineState } from "../apps/engine/src/recovery.js";
import { emptyResultAccounting, tradingResults, economicResults } from "../apps/engine/src/trading-results.js";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { CrashInjectedError, type CrashInjectionPoint } from "../apps/engine/src/crash-injection.js";
import { paperFill, qualifiedPaperEnv, qualified, discoveryReport, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";
import { buildExperimentSummary } from "../apps/engine/src/report.js";

const at = new Date("2026-09-01T00:00:00.000Z");
function bag(): RecoverableEngineState {
  return { ledger: new PaperLedger("1000", at), experiment: null, restingOrders: new Map(), makerReservations: new Map(), settledMarkets: new Set(), lastInfrastructureAccrualAt: 0, resultAccounting: emptyResultAccounting() };
}
function applyFill(state: RecoverableEngineState, fill: PaperFill) {
  applyDurableTransition(state, plannedDurableTransition({ operationKey: `fill:${fill.id}`, type: "PAPER_FILL", aggregateType: "fill", aggregateId: fill.id, payload: { fill, positionId: `pos:${fill.marketId}`, reservationAfter: null } }, at, 1));
}
function settle(state: RecoverableEngineState, marketId = "btc-mkt", upPayoff = "1") {
  const row = plannedDurableTransition({ operationKey: `settlement:${marketId}`, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: marketId, payload: { marketId, upPayoff, downPayoff: upPayoff === "1" ? "0" : "1", settledAt: at.toISOString() } }, at, 2);
  applyDurableTransition(state, row);
  return row;
}

describe("durable episode result accounting", () => {
  it.each([["0.4", "0.1", "1", "wins"], ["0.4", "0.1", "0", "losses"], ["0.99", "0.1", "1", "breakeven"], ["0.99", "0.2", "1", "losses"]])("fee-inclusive buy %s fee %s payoff %s counts one %s", (price, fee, payoff, classification) => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP", price, { feeUsd: fee }));
    const row = settle(state, "btc-mkt", payoff);
    applyDurableTransition(state, row); // Reapplying settlement with no inventory cannot increment.
    const results = tradingResults(state.resultAccounting!)!;
    expect(results.completed).toBe(1);
    expect(results[classification as "wins" | "losses" | "breakeven"]).toBe(1);
    expect(results.winRate).toBe(classification === "wins" ? 1 : 0);
    expect(validatePostReplayInvariants(state, 2, 2)).toEqual({ ok: true, reasons: [] });
  });

  it("partial safety exits and remaining settlement count once with the whole episode economics", () => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP", "0.8", { feeUsd: "0.1" }));
    applyFill(state, paperFill("btc-mkt", "UP", "0.1", { side: "SELL", quantity: "5", feeUsd: "0.1" }));
    expect(tradingResults(state.resultAccounting!)?.completed).toBe(0);
    settle(state); // Remaining inventory wins $1, prior safety sell loses $3.5 => loss.
    expect(tradingResults(state.resultAccounting!)).toEqual({ completed: 1, wins: 0, losses: 1, breakeven: 0, winRate: 0 });
    expect(state.resultAccounting!.episodes).toEqual({});
    expect(economicResults(state.resultAccounting!)).toEqual({ trades: 1, meteors: 1, completedNetPnl: "-2.7", completedFees: "0.2",
      worstTradeNetPnl: "-2.7", buySpend: "8", sellProceeds: "0.5", settlementPayout: "5", fees: "0.2" });
    expect(state.ledger.snapshot().netTradingPnl).toBe("-2.7");
  });

  it("fully closed safety-exit episodes count once; a new market episode counts separately", () => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP", "0.4", { feeUsd: "0.1" }));
    applyFill(state, paperFill("btc-mkt", "UP", "0.42", { side: "SELL", feeUsd: "0.1" }));
    expect(tradingResults(state.resultAccounting!)?.breakeven).toBe(1);
    settle(state); // No economic inventory => no additional completion.
    applyFill(state, paperFill("btc-mkt", "UP", "0.4"));
    settle(state);
    expect(tradingResults(state.resultAccounting!)).toEqual({ completed: 2, wins: 1, losses: 0, breakeven: 1, winRate: 0.5 });
    expect(state.resultAccounting!.episodes).toEqual({});
  });

  it("new experiment resets results, clone/preflight does not mutate live totals", () => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP"));
    const clone = cloneRecoverableState(state);
    settle(clone);
    expect(tradingResults(state.resultAccounting!)?.completed).toBe(0);
    settle(state);
    const experiment = createIdleExperiment({ durationHours: 48, strategyVersion: "test", riskConfiguration: {}, modelConfiguration: {}, startingBalance: "1000", systemVersion: "test", gitSha: "test" });
    applyDurableTransition(state, plannedDurableTransition({ operationKey: "new", type: "EXPERIMENT_STARTED", aggregateType: "experiment", aggregateId: experiment.id, payload: { experiment: { ...experiment, status: "RUNNING", startedAt: at.toISOString() } } }, at, 3));
    expect(tradingResults(state.resultAccounting!)).toEqual({ completed: 0, wins: 0, losses: 0, breakeven: 0, winRate: null });
  });

  it("snapshot counters survive decision feed truncation and invalid totals fail invariants", () => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP"));
    settle(state);
    const store = new EngineStateStore(state.ledger.snapshot(), state.ledger.positions);
    store.resultAccounting = state.resultAccounting!;
    for (let i = 0; i < 700; i += 1) store.record({ id: String(i), at: at.toISOString(), type: "GUARDIAN", traceId: "test", marketId: null, detail: {} });
    expect(store.snapshot().decisionFeed).toHaveLength(500);
    expect(store.snapshot().tradingResults).toEqual({ completed: 1, wins: 1, losses: 0, breakeven: 0, winRate: 1 });
    state.resultAccounting!.completed += 1;
    expect(validatePostReplayInvariants(state, 1, 1).reasons).toContain("result_accounting_invalid");
  });
});

describe("P1-10 dollar cashflow authority", () => {
  it.each(["0", "1"])("does not realize a locked pair before settlement; payout=%s gives identical real dollars", payoff => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP", "0.65", { feeUsd: "0.05" }));
    applyFill(state, paperFill("btc-mkt", "DOWN", "0.30", { feeUsd: "0.04" }));
    expect(economicResults(state.resultAccounting!)!.completedNetPnl).toBe("0");
    expect(tradingResults(state.resultAccounting!)!.completed).toBe(0);
    settle(state, "btc-mkt", payoff);
    expect(economicResults(state.resultAccounting!)).toMatchObject({ trades: 1, completedNetPnl: "0.41", completedFees: "0.09", settlementPayout: "10", fees: "0.09", meteors: 0 });
    expect(state.ledger.snapshot().cash).toBe("1000.41");
  });
  it("includes partial entry fills, maker rebates, taker fees and safety SELL exactly once per closed trade", () => {
    const state = bag();
    applyFill(state, paperFill("btc-mkt", "UP", "0.65", { quantity: "4", feeUsd: "-0.01", liquidityRole: "MAKER" }));
    applyFill(state, paperFill("btc-mkt", "UP", "0.70", { quantity: "6", feeUsd: "0.03" }));
    applyFill(state, paperFill("btc-mkt", "UP", "0.30", { side: "SELL", quantity: "10", feeUsd: "0.02" }));
    expect(economicResults(state.resultAccounting!)).toMatchObject({ trades: 1, completedNetPnl: "-3.84", completedFees: "0.04", fees: "0.04", meteors: 1, worstTradeNetPnl: "-3.84" });
    expect(state.ledger.snapshot().cash).toBe("996.16");
    applyFill(state, paperFill("btc-mkt", "UP", "0.65", { feeUsd: "0" }));
    settle(state);
    expect(economicResults(state.resultAccounting!)).toMatchObject({ trades: 2, completedNetPnl: "-0.34", meteors: 1 });
  });
  it("keeps legacy totals unavailable rather than inventing historical profit", () => {
    const legacy = emptyResultAccounting(); delete legacy.economics;
    expect(economicResults(legacy)).toBe(null);
  });
  it("same fill retry, checkpoint and restart preserve net dollars, fees and trade counts", async () => {
    const journal = new MemoryEventJournal(); let engine = await startEngine(journal);
    try {
      await engine.beginExperiment();
      const buy = paperFill("btc-mkt", "UP", "0.65", { feeUsd: "0.1" });
      await engine.applyObservedFill(buy); await engine.applyObservedFill(buy);
      const sell = paperFill("btc-mkt", "UP", "0.40", { side: "SELL", feeUsd: "0.1" });
      await engine.applyObservedFill(sell); await engine.applyObservedFill(sell);
      const before = engine.state.snapshot();
      expect(before.economicResults).toMatchObject({ trades: 1, completedNetPnl: "-2.7", fees: "0.2", meteors: 1 });
      await engine.checkpointForTest(); await engine.stop(); engine = await startEngine(journal);
      expect(engine.state.snapshot().economicResults).toEqual(before.economicResults);
      expect(engine.ledger.snapshot()).toEqual(before.balance);
      expect((await journal.queryEvents({ types: ["PNL_SNAPSHOT"] })).at(-1)?.payload.economicResults).toEqual(before.economicResults);
    } finally { await engine.stop(); }
  });
  it("actual mark-to-market closure keeps completed economics beside unrealized residual and survives report replay", async () => {
    const journal = new MemoryEventJournal(); const engine = await startEngine(journal);
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.65", { feeUsd: "0.1" }));
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40", { side: "SELL", feeUsd: "0.1" }));
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.65", { feeUsd: "0.1" }));
      const before = engine.state.snapshot();
      const book = snapshot({ marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP",
        bids: [{ price: "0.60", quantity: "1000" }], asks: [{ price: "0.61", quantity: "1000" }] });
      const authority = engine as unknown as { snapshots: Map<string, object>; markToMarketClose(n: number): Promise<boolean> };
      authority.snapshots.set("btc-mkt", { UP: book });
      expect(await authority.markToMarketClose(1)).toBe(true);
      const events = await journal.queryEvents({ experimentId: engine.experimentManifest()!.id, limit: 1000 });
      const mtm = events.filter(e => e.type === "PNL_SNAPSHOT").at(-1)!;
      expect(mtm.payload.source).toBe("MARK_TO_MARKET");
      expect(mtm.payload.economicResults).toEqual(before.economicResults);
      expect(mtm.payload.tradingResults).toEqual(before.tradingResults);
      const summary = buildExperimentSummary(events, engine.experimentManifest()!.id);
      expect(summary.economicResults).toEqual(before.economicResults);
      expect(summary.winLossDistribution).toEqual({ wins: 0, losses: 1, breakeven: 0 });
      expect(summary.unrealizedPnlUsd).toBe(-0.5);
      expect(summary.realizedNetTradingPnlUsd).toBe(-2.8);
      expect(summary.netTradingPnlUsd).toBe(-3.3);
      await engine.checkpointForTest();
      expect(engine.state.snapshot().economicResults).toEqual(before.economicResults);
    } finally { await engine.stop(); }
  });
});

async function startEngine(journal: MemoryEventJournal) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
  const engine = new EngineService(qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
    market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
  });
  await engine.start();
  return engine;
}

describe("result counters across actual engine commit/checkpoint crash boundaries", () => {
  const points: CrashInjectionPoint[] = ["BEFORE_DURABLE_COMMIT", "AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT", "AFTER_DERIVED_AUDIT_BEFORE_CHECKPOINT", "AFTER_CHECKPOINT"];
  it.each(points)("%s preserves exactly-once accounting", async (point) => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal);
    let recovered: EngineService | undefined;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.4"));
      await engine.checkpointForTest();
      const input = {
        operationKey: `settlement:btc-mkt:${engine.experimentManifest()!.id}`, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: "pos:btc-mkt",
        buildPayload: () => ({ marketId: "btc-mkt", upPayoff: "1", downPayoff: "0", settledAt: at.toISOString(), derivedAudit: [] }), checkpoint: true,
      };
      // Exercise the same serialized durable gate used by real settlement.
      type SettlementInput = typeof input;
      const authority = engine as unknown as { commitAuthoritative(input: SettlementInput): Promise<unknown> };
      engine.setCrashInjectionForTest(point, { operationKey: input.operationKey });
      await expect(authority.commitAuthoritative(input)).rejects.toBeInstanceOf(CrashInjectedError);
      engine.abandonWithoutCheckpointForTest();
      await engine.stop();
      recovered = await startEngine(journal);
      expect(recovered.state.snapshot().tradingResults?.completed).toBe(point === "BEFORE_DURABLE_COMMIT" ? 0 : 1);
      const replayAuthority = recovered as unknown as { commitAuthoritative(input: SettlementInput): Promise<unknown> };
      await replayAuthority.commitAuthoritative(input);
      await replayAuthority.commitAuthoritative(input);
      expect(recovered.state.snapshot().tradingResults).toEqual({ completed: 1, wins: 1, losses: 0, breakeven: 0, winRate: 1 });
      await recovered.checkpointForTest();
      expect((await journal.loadLatestCheckpoint())?.payload.resultAccounting).toMatchObject({ completed: 1, wins: 1 });
      await recovered.stop();
      recovered = await startEngine(journal);
      expect(recovered.state.snapshot().tradingResults?.completed).toBe(1);
    } finally { await engine.stop(); await recovered?.stop(); }
  });

  it("legacy checkpoints recover totals from retained durable history", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal);
    try {
      await engine.beginExperiment();
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.4"));
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.6", { side: "SELL" }));
      await engine.checkpointForTest();
      const checkpoint = (await journal.loadLatestCheckpoint())!;
      const { resultAccounting: _ignored, ...oldPayload } = checkpoint.payload;
      await journal.saveCheckpoint({ experimentId: checkpoint.experimentId, sequence: checkpoint.sequence + 1, payload: oldPayload });
      engine.abandonWithoutCheckpointForTest();
      await engine.stop();
      const recovered = await startEngine(journal);
      try { expect(recovered.state.snapshot().tradingResults).toEqual({ completed: 1, wins: 1, losses: 0, breakeven: 0, winRate: 1 }); }
      finally { await recovered.stop(); }
    } finally { await engine.stop(); }
  });

  it("compacted legacy evidence stays unavailable rather than inventing historical zeros", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal);
    let recovered: EngineService | undefined;
    try {
      await engine.beginExperiment();
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.4"));
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.6", { side: "SELL" }));
      await engine.checkpointForTest();
      const checkpoint = (await journal.loadLatestCheckpoint())!;
      const { resultAccounting: _ignored, ...oldPayload } = checkpoint.payload;
      await journal.saveCheckpoint({ experimentId: checkpoint.experimentId, sequence: checkpoint.sequence + 1, payload: oldPayload });
      await journal.compactDurableTransitionsThrough(engine.lastAppliedDurableCursorForTest());
      engine.abandonWithoutCheckpointForTest();
      await engine.stop();
      recovered = await startEngine(journal);
      expect(recovered.state.snapshot().tradingResults).toBeNull();
      expect(recovered.ledger.snapshot().netTradingPnl).toBe(engine.ledger.snapshot().netTradingPnl);
    } finally { await engine.stop(); await recovered?.stop(); }
  });
});
