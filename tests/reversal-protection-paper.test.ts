import { describe, expect, it, vi } from "vitest";
import { NATIVE_DETERMINISTIC_AUTHORITY, type MarketSnapshot, type ReversalProtectionState,
  type JournalEvent, CANONICAL_REPORT_INPUT_EVENT_TYPES, AUDIT_CRITICAL_EVENT_TYPES } from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { type RiskReductionOwnership } from "../apps/engine/src/recovery.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START = Date.parse("2026-10-05T00:00:00Z"), MARKET = "reversal-mkt", UP = "reversal-up", DOWN = "reversal-down";
type Internals = {
  reversalProtectionStates: Map<string, ReversalProtectionState>;
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  snapshots: Map<string, Partial<Record<"UP" | "DOWN", MarketSnapshot>>>;
  experiment: { status: string; riskConfiguration: Record<string, unknown> };
  maybeReversalProtection(m: string): Promise<void>;
  maybeProtectiveDeRisk(m: string): Promise<void>;
  pollFailedHedges(): Promise<void>;
  commitAuthoritative(input: Record<string, unknown>): Promise<unknown>;
  admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean>;
  durableGate: { run<T>(callback: () => Promise<T>): Promise<T> };
  onMinOrderSizeHydrated(event: { tokenId: string; minOrderSize: string; tokenEpoch: number }): void;
  applyFill(...args: unknown[]): Promise<boolean>;
};
const internal = (s: Scenario) => s.engine as unknown as Internals;
async function start(journal = new MemoryEventJournal(), clock = { ms: START }, env: Record<string, string> = {}) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified(MARKET, "SOL", UP, DOWN, new Date(START + 3600000).toISOString())]));
  const errors: unknown[] = [];
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0",
    NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.12", PROFIT_TRAILING_PAIR_SOAK_ENABLED: "true",
    PROFIT_TRAILING_PAIR_ARM_EDGE: "0.05", PROFIT_TRAILING_PAIR_RETRACE_EDGE: "0.02",
    REVERSAL_PROTECTION_ENABLED: "true", ...env }), journal, { error: (e: unknown) => errors.push(e) }, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: async () => null,
    });
  await engine.start(); await market.waitForHandlers();
  return { engine, journal, clock, market, errors };
}
type Scenario = Awaited<ReturnType<typeof start>>;
const rows = (s: Scenario, type: string) => s.journal.queryEvents({ types: [type] });
const state = (s: Scenario) => internal(s).reversalProtectionStates.get(MARKET);
const residual = (s: Scenario) => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity ?? "0";
async function riskFills(s: Scenario) { return (await rows(s, "PAPER_FILL")).filter(r => ["PROTECTIVE_DE_RISK", "RESCUE_SECOND_LEG"].includes(String(r.payload.intent))); }
async function waitFor(s: Scenario, test: () => boolean | Promise<boolean>) {
  const until = Date.now() + 3000;
  while (!await test()) {
    if (Date.now() > until) throw Error(`reversal lifecycle timeout: ${JSON.stringify({ state: state(s), residual: residual(s), errors: s.errors })}`);
    await new Promise(r => setTimeout(r, 2));
  }
}
function book(s: Scenario, outcome: "UP" | "DOWN", bid: string | null, ask: string | null, quantity = "10000", sequence = 1) {
  return snapshot({ marketId: MARKET, tokenId: outcome === "UP" ? UP : DOWN, outcome, asset: "SOL",
    bids: bid ? [{ price: bid, quantity }] : [], asks: ask ? [{ price: ask, quantity }] : [],
    receivedAt: new Date(s.clock.ms).toISOString(), sourceTimestamp: new Date(s.clock.ms).toISOString(),
    sequence, hash: `${outcome}-${sequence}` });
}
async function publish(s: Scenario, bid: string | null = "0.60", ask: string | null = "0.30", quantity = "10000", seq = 1) {
  await s.market.emitSnapshot(book(s, "UP", bid, "0.81", quantity, seq));
  await s.market.emitSnapshot(book(s, "DOWN", "0.29", ask, quantity, seq));
}
async function seed(s: Scenario) {
  expect((await s.engine.beginExperiment()).ok).toBe(true);
  await s.engine.applyObservedFill(paperFill(MARKET, "UP", "0.80", { tokenId: UP, quantity: "5", timestamp: new Date(s.clock.ms).toISOString(), feeUsd: "0" }));
}
function event(s: Scenario, id: string, direction: "UP" | "DOWN" | "FLAT" = "DOWN"): JournalEvent {
  return createJournalEvent({ id, traceId: id, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: id,
    occurredAt: new Date(s.clock.ms), sourceEventId: "same-source",
    payload: { marketId: MARKET, authority: NATIVE_DETERMINISTIC_AUTHORITY, consensusDirection: direction,
      consensusConfidence: 0.01, estimatedEdge: -0.99, evidenceIds: ["same-evidence"], triggerId: "same-trigger" } });
}
const observe = (s: Scenario, id: string, direction: "UP" | "DOWN" | "FLAT" = "DOWN") => s.engine.persistReportInput(event(s, id, direction));
async function confirm(s: Scenario) {
  await observe(s, "opposite-1"); await observe(s, "opposite-2");
  s.clock.ms += 2000; await observe(s, "opposite-3");
}
async function restart(s: Scenario, env: Record<string, string> = {}) { await s.engine.stop(); return start(s.journal, s.clock, env); }
async function settle(s: Scenario) {
  await internal(s).commitAuthoritative({ operationKey: `settlement:${MARKET}:test`, type: "POSITION_SETTLED",
    aggregateType: "position", aggregateId: MARKET, buildPayload: () => ({ marketId: MARKET, upPayoff: "1", downPayoff: "0",
      settledAt: new Date(s.clock.ms).toISOString(), derivedAudit: [] }) });
}

describe("reversal protection real PAPER authority", () => {
  it("RP-ENG-01: two opposites leave residual and create no risk-reduction order/fill", async () => {
    const s = await start(); try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "2");
      expect(state(s)?.oppositeCount).toBe(2); expect(state(s)?.confirmedAt).toBeNull(); expect(residual(s)).toBe("5");
      expect(await riskFills(s)).toHaveLength(0); expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it.each([
    ["RP-ENG-02/05", "0.75", "0.30", "PROTECTIVE_DE_RISK"],
    ["RP-ENG-06", "0.60", "0.30", "RESCUE_SECOND_LEG"],
    ["RP-ENG-07", "0.70", "0.30", "PROTECTIVE_DE_RISK"],
    ["RP-ENG-08", null, "0.30", "RESCUE_SECOND_LEG"],
    ["RP-ENG-09", "0.60", null, "PROTECTIVE_DE_RISK"],
  ] as const)("%s: real canonical A/B action (%s/%s)", async (label, bid, ask, intent) => {
    const s = await start(); try { await seed(s); await publish(s, bid, ask); await confirm(s);
      expect(residual(s)).toBe("0"); expect(state(s)).toBeUndefined();
      const fills = await riskFills(s); expect(fills).toHaveLength(1);
      expect(fills[0]?.payload).toMatchObject({ intent, side: intent === "PROTECTIVE_DE_RISK" ? "SELL" : "BUY", quantity: "5",
        deRisk: { deRiskReason: "REVERSAL_CONFIRMED", selection: intent, tieBreakUsed: label === "RP-ENG-07" } });
      const orders = (await rows(s, "PAPER_ORDER_CREATED")).filter(r => r.payload.intent === intent);
      expect(orders).toHaveLength(1); expect(orders[0]?.payload.orderType).toBe("TAKER");
      expect(await rows(s, "REVERSAL_PROTECTION_CONFIRMED")).toHaveLength(1);
      expect(s.engine.ledger.snapshot().cash).not.toBe("996"); expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-03: third at 360001ms does not activate or force time exit", async () => {
    const s = await start(); try { await seed(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 360001; await publish(s); await observe(s, "3"); await internal(s).pollFailedHedges();
      expect(state(s)?.oppositeCount).toBe(2); expect(await riskFills(s)).toHaveLength(0); expect(residual(s)).toBe("5");
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-04: same/FLAT ignore, same trigger/source still count, exact row dedupes", async () => {
    const s = await start(); try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "1");
      await observe(s, "held", "UP"); await observe(s, "flat", "FLAT"); await observe(s, "2");
      expect(state(s)?.oppositeCount).toBe(2); await observe(s, "burst-3");
      expect(state(s)?.oppositeCount).toBe(2); expect(await riskFills(s)).toHaveLength(0);
      s.clock.ms += 2000; await observe(s, "3"); expect(await riskFills(s)).toHaveLength(1);
      const counted = await rows(s, "REVERSAL_PROTECTION_STATE_UPDATED"); expect(counted).toHaveLength(3);
      expect(counted[2]?.payload).toMatchObject({ oppositeCount: 3, requiredOppositeCount: 3, requiredMinOppositeSpanMs: 2000,
        triggerId: "same-trigger", sourceEventId: "same-source", consensusConfidence: 0.01 });
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-10: neither executable retains confirmation; paced book retry needs no new decisions", async () => {
    const s = await start(); try { await seed(s); await publish(s, null, null); await confirm(s);
      expect(state(s)?.confirmedAt).not.toBeNull(); expect(residual(s)).toBe("5"); expect(await riskFills(s)).toHaveLength(0);
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 2);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each(["stale", "unsynchronized"])("RP-ENG-11: %s books fail closed until fresh", async mode => {
    const s = await start(); try { await seed(s); await publish(s);
      if (mode === "stale") s.clock.ms += 16000;
      else { s.market.markTokenSync(UP, "GAPPED"); s.market.markTokenSync(DOWN, "GAPPED"); }
      await confirm(s); expect(await riskFills(s)).toHaveLength(0); expect(residual(s)).toBe("5"); expect(state(s)?.oppositeCount).toBe(3);
    } finally { await s.engine.stop(); }
  });
  it("full residual policy waits on partial executable depth", async () => {
    const s = await start(); try { await seed(s); await publish(s, "0.60", "0.30", "2"); await confirm(s);
      expect(residual(s)).toBe("5"); expect(await riskFills(s)).toHaveLength(0); expect(state(s)?.oppositeCount).toBe(3);
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 2); await waitFor(s, () => residual(s) === "0");
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-12/13: target at .12 before third cannot be consumed twice", async () => {
    const s = await start(); try { await seed(s); await observe(s, "1"); await observe(s, "2");
      await publish(s, "0.79", "0.08"); await waitFor(s, () => residual(s) === "0"); await observe(s, "3");
      expect(state(s)).toBeUndefined(); expect(await riskFills(s)).toHaveLength(0);
      expect((await rows(s, "PAPER_FILL")).filter(r => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-13: .05 arm/.02 retrace remains active before confirmation", async () => {
    const s = await start(); try { await seed(s); await observe(s, "1"); await publish(s, "0.79", "0.15");
      await waitFor(s, async () => (await rows(s, "PROFIT_TRAILING_PAIR_STATE_UPDATED")).length > 0);
      s.clock.ms += 1000; await publish(s, "0.79", "0.18", "10000", 2); await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(0); expect((await rows(s, "PAPER_ORDER_CREATED")).some(r => r.payload.strategyId === "profit-trailing-pair-trail")).toBe(true);
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-14: concurrent third decisions and callbacks have one owner/order/fill", async () => {
    const s = await start(); try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 2000;
      await Promise.all([observe(s, "3"), observe(s, "4"), internal(s).maybeReversalProtection(MARKET), publish(s, "0.60", "0.30", "10000", 2)]);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1); expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it("profit-trailing flight already owns the residual when third opposite confirms", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 2000;
      s.journal.setAppendInFlightHoldForTest(new Promise<void>(r => release = r), ["PAPER_ORDER_CREATED"]);
      await publish(s, "0.79", "0.08");
      await waitFor(s, () => internal(s).riskReductionOwners.size === 1 && s.journal.inFlightAppends > 0);
      // Order-derived audit holds the mutation gate. Queue the third event,
      // then release that audit so admission/count/fill can serialize normally.
      const pending = observe(s, "3"); expect(await riskFills(s)).toHaveLength(0);
      release(); s.journal.setAppendInFlightHoldForTest(null); await pending; await waitFor(s, () => residual(s) === "0");
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect((await rows(s, "PAPER_FILL")).filter(r => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
      expect(state(s)).toBeUndefined();
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it("noncounting consensus needs no new database state lookup", async () => {
    const s = await start(); try { await seed(s); await observe(s, "1");
      const spy = vi.spyOn(s.journal, "readDurableByOperationKey");
      await observe(s, "1"); await observe(s, "held", "UP"); await observe(s, "flat", "FLAT");
      s.clock.ms += 360001; await observe(s, "outside");
      expect(spy).not.toHaveBeenCalled(); spy.mockRestore(); expect(state(s)?.oppositeCount).toBe(1);
    } finally { await s.engine.stop(); }
  });
  it("confirmed min-order hydration retries without a new market price or consensus", async () => {
    const s = await start(); try { await seed(s);
      await s.market.emitSnapshot({ ...book(s, "UP", null, "0.81"), minOrderSize: null });
      await s.market.emitSnapshot({ ...book(s, "DOWN", "0.29", "0.30"), minOrderSize: null });
      await confirm(s); expect(residual(s)).toBe("5");
      s.clock.ms += 5001; internal(s).onMinOrderSizeHydrated({ tokenId: DOWN, minOrderSize: "1", tokenEpoch: s.market.tokenEpoch(DOWN) });
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("old consensus awaiting the mutation gate cannot change a new unhedged episode", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await observe(s, "1"); await observe(s, "2");
      const position = s.engine.ledger.positions.get(MARKET)!;
      const old = event(s, "old-third");
      let entered!: () => void; const ready = new Promise<void>(r => entered = r);
      const holding = internal(s).durableGate.run(async () => {
        entered(); await new Promise<void>(r => release = r);
        const hedge = paperFill(MARKET, "DOWN", "0.30", { tokenId: DOWN, quantity: "5", timestamp: new Date(s.clock.ms).toISOString() });
        await internal(s).commitAuthoritative({ operationKey: `fill:${hedge.id}`, type: "PAPER_FILL", aggregateType: "position", aggregateId: position.id,
          buildPayload: () => ({ fill: hedge, positionId: position.id, reservationAfter: null, intent: "HEDGE_SECOND_LEG", derivedAudit: [] }) });
        s.clock.ms += 1000;
        // Model a canonical recovered additional first leg after the previous
        // residual was paired. Its episode timestamp changes on the ledger.
        const next = paperFill(MARKET, "UP", "0.80", { tokenId: UP, quantity: "5", timestamp: new Date(s.clock.ms).toISOString() });
        await internal(s).commitAuthoritative({ operationKey: `fill:${next.id}`, type: "PAPER_FILL", aggregateType: "position", aggregateId: position.id,
          buildPayload: () => ({ fill: next, positionId: position.id, reservationAfter: null, intent: "OPEN_FIRST_LEG", derivedAudit: [] }) });
      });
      await ready; const pending = s.engine.persistReportInput(old); release(); await holding; await pending;
      expect(state(s)).toBeUndefined(); expect(residual(s)).toBe("5");
      await observe(s, "new-first"); expect(state(s)?.oppositeCount).toBe(1); expect(await riskFills(s)).toHaveLength(0);
    } finally { release?.(); await s.engine.stop(); }
  });
  it("RP-ENG-15: settlement wins across delayed reversal evaluation", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 2000;
      s.journal.setAppendInFlightHoldForTest(new Promise<void>(r => release = r), ["RISK_EVALUATION"]);
      const pending = observe(s, "3"); await waitFor(s, () => s.journal.inFlightAppends > 0 && !!state(s)?.confirmedAt);
      await settle(s); const cash = s.engine.ledger.snapshot().cash;
      release(); s.journal.setAppendInFlightHoldForTest(null); await pending;
      expect(await riskFills(s)).toHaveLength(0); expect(state(s)).toBeUndefined(); expect(s.engine.ledger.snapshot().cash).toBe(cash);
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it("RP-ENG-16: ENTRY_CLOSED protects existing episode", async () => {
    const s = await start(); try { await seed(s); await s.engine.closeExperimentEntries(); await publish(s); await confirm(s);
      expect(residual(s)).toBe("0"); expect(await riskFills(s)).toHaveLength(1);
      expect(internal(s).experiment.status).toBe("ENTRY_CLOSED");
    } finally { await s.engine.stop(); }
  });
  it("RP-ENG-17: depth disappears after admission; local failure, no fill, later retry", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 2000;
      const errors = s.engine.runtimeBookkeepingForTest().executionErrors;
      s.journal.setAppendInFlightHoldForTest(new Promise<void>(r => release = r), ["PAPER_ORDER_CREATED"]);
      const pending = observe(s, "3"); await waitFor(s, () => internal(s).riskReductionOwners.size === 1 && s.journal.inFlightAppends > 0);
      await publish(s, null, null, "10000", 2); release(); s.journal.setAppendInFlightHoldForTest(null); await pending;
      expect(await riskFills(s)).toHaveLength(0); expect(residual(s)).toBe("5"); expect(state(s)?.oppositeCount).toBe(3);
      expect((await rows(s, "PAPER_EXECUTION_ERROR"))[0]?.payload.message).toBe("INSUFFICIENT_DEPTH");
      expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(errors);
      expect((await rows(s, "RISK_REDUCTION_FAILED"))[0]?.payload.reason).toBe("INSUFFICIENT_DEPTH");
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 3); await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(1); expect(s.engine.integrityFault()).toBe(false);
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it("default OFF preserves behavior; unrelated experiment cannot activate", async () => {
    let s = await start(undefined, undefined, { REVERSAL_PROTECTION_ENABLED: "false" });
    try { await seed(s); await publish(s); await confirm(s); expect(state(s)).toBeUndefined(); expect(await riskFills(s)).toHaveLength(0); }
    finally { await s.engine.stop(); }
    s = await start(undefined, undefined, { PROFIT_TRAILING_PAIR_SOAK_ENABLED: "false" });
    try { await expect(s.engine.beginExperiment()).rejects.toThrow("INVALID_REVERSAL_PROTECTION_V1_CONFIG"); }
    finally { await s.engine.stop(); }
  });
  it("pre-entry consensus cannot count, and legacy Guardian/deadline stay suppressed", async () => {
    const s = await start(); try { expect((await s.engine.beginExperiment()).ok).toBe(true); await observe(s, "before-entry");
      await s.engine.applyObservedFill(paperFill(MARKET, "UP", "0.80", { tokenId: UP, quantity: "5", timestamp: new Date(s.clock.ms).toISOString() }));
      s.clock.ms += 301000; await publish(s, "0.01", "0.99"); await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(state(s)).toBeUndefined(); expect(await riskFills(s)).toHaveLength(0); expect(residual(s)).toBe("5");
    } finally { await s.engine.stop(); }
  });
});

describe("reversal protection recovery and crash boundaries", () => {
  it.each([1, 2, 3])("RP-REC-01/02/03/04: count %s survives checkpoint compaction, exact replay and changed env", async count => {
    let s = await start(); try { await seed(s); for (let i=1; i<=count; i++) {
      if (i === 3) s.clock.ms += 2000; await observe(s, `c-${i}`);
    }
      await s.engine.checkpointForTest(); await s.journal.compactDurableTransitionsThrough(s.engine.lastAppliedDurableCursorForTest());
      s = await restart(s, { REVERSAL_PROTECTION_ENABLED: "false", REVERSAL_PROTECTION_WINDOW_MS: "1", REVERSAL_PROTECTION_OPPOSITE_COUNT: "1",
        REVERSAL_PROTECTION_MIN_OPPOSITE_SPAN_MS: "1" });
      expect(state(s)?.oppositeCount).toBe(count); expect(internal(s).experiment.riskConfiguration).toMatchObject({ reversalProtectionEnabled: true,
        reversalProtectionWindowMs: 360000, reversalProtectionOppositeCount: 3, reversalProtectionMinOppositeSpanMs: 2000 });
      await observe(s, `c-${count}`); expect(state(s)?.oppositeCount).toBe(count);
      for (let i=count+1; i<=3; i++) {
        if (i === 3) s.clock.ms += 2000; await observe(s, `c-${i}`);
      }
      s.clock.ms += 5001; await publish(s); await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each([1, 2, 3])("count %s and source row recover after commit-before-apply crash", async count => {
    let s = await start(); try { await seed(s); for(let i=1; i<count; i++) await observe(s, `crash-${i}`);
      if (count === 3) s.clock.ms += 2000;
      const position = s.engine.ledger.positions.get(MARKET)!;
      const key = `reversal:${position.id}:${position.unhedgedStartedAt ?? position.openedAt}:crash-${count}`;
      s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: key });
      await expect(observe(s, `crash-${count}`)).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s); expect(state(s)?.oppositeCount).toBe(count);
      expect((await rows(s, "CONSENSUS_DECISION")).filter(r=>r.id === `crash-${count}`)).toHaveLength(1);
      await observe(s, `crash-${count}`); expect(state(s)?.oppositeCount).toBe(count); expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it.each(["admission", "fill"])("RP-REC-05/06: crash after %s resumes original order exactly once", async boundary => {
    let s = await start();
    try { await seed(s); await publish(s); await observe(s, "1"); await observe(s, "2");
      s.clock.ms += 2000;
      // Arm the real production commit at its canonical operation identity.
      const engine = s.engine as unknown as { admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean> };
      const admit = engine.admitRiskReduction.bind(engine);
      engine.admitRiskReduction = async owner => {
        s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: boundary === "admission"
          ? `risk-reduction:${owner.order.id}:admitted` : `fill:risk-reduction-fill:${owner.order.id}` });
        return admit(owner);
      };
      await expect(observe(s, "3")).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s); const admissions = await rows(s, "RISK_REDUCTION_ADMITTED"); expect(admissions).toHaveLength(1);
      const id = admissions[0]!.payload.orderId;
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 2); await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(1); expect((await riskFills(s))[0]?.payload.orderId).toBe(id);
      s = await restart(s); await publish(s, "0.60", "0.30", "10000", 3); expect(await riskFills(s)).toHaveLength(1); expect(state(s)).toBeUndefined();
    } finally { await s.engine.stop(); }
  });
  it("retention/report/archive classify counted state and confirmation as canonical protected evidence", () => {
    for (const type of ["REVERSAL_PROTECTION_STATE_UPDATED", "REVERSAL_PROTECTION_CONFIRMED"])
      { expect(CANONICAL_REPORT_INPUT_EVENT_TYPES).toContain(type); expect(AUDIT_CRITICAL_EVENT_TYPES).toContain(type); }
  });
});
