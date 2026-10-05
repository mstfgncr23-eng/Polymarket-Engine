import { describe, expect, it } from "vitest";
import { d, evaluateRisk, executePaperTaker, riskLimitsSchema, type MarketSnapshot, type PaperOrder } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { type RiskReductionOwnership, unhedgedEpisode } from "../apps/engine/src/recovery.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { order, safeRisk, snapshot } from "./fixtures.js";

const START = Date.parse("2026-01-01T00:00:00Z");
const MARKET = "guardian-mkt";
type Outcome = "UP" | "DOWN";
type Internals = {
  snapshots: Map<string, Partial<Record<Outcome, MarketSnapshot>>>;
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  durableGate: { run<T>(work: () => Promise<T>): Promise<T>; outside<T>(work: () => Promise<T>): Promise<T> };
  evaluateGuardian(market: string): { reason: string; decision: { accepted: boolean; order: PaperOrder | null } | null };
  maybeGuardianProtectiveSell(market: string): Promise<boolean>;
  maybeProtectiveDeRisk(market: string): Promise<void>;
  maybeEarlyCatastrophicPairLock(market: string): Promise<{ acted: boolean }>;
  admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean>;
  reserveMaker(order: PaperOrder, book: MarketSnapshot, intent: "HEDGE_SECOND_LEG"): Promise<void>;
  failRiskReduction(owner: RiskReductionOwnership, reason: string): Promise<void>;
  submitPaperOrder(order: PaperOrder, book: MarketSnapshot, key: string, intent: RiskReductionOwnership["intent"]): Promise<boolean>;
  applyFill(fill: ReturnType<typeof executePaperTaker>, extras: { intent: RiskReductionOwnership["intent"]; riskReductionOwnership: RiskReductionOwnership }): Promise<boolean>;
  commitAuthoritative(input: Record<string, unknown>): Promise<unknown>;
};
const internal = (engine: EngineService) => engine as unknown as Internals;
async function start(journal = new MemoryEventJournal(), clock = { ms: START }, latency = "0") {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified(MARKET, "XRP", "guardian-up", "guardian-down", new Date(START + 3_600_000).toISOString())]));
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: latency }),
  journal, { error: () => undefined }, { market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.ms), resolutionFetcher: async () => null });
  await engine.start(); await market.waitForHandlers();
  return { engine, journal, market, clock, held: "UP" as Outcome };
}
type Scenario = Awaited<ReturnType<typeof start>>;
function book(s: Scenario, outcome: Outcome, overrides: Partial<MarketSnapshot> = {}) {
  const held = outcome === s.held;
  return snapshot({ marketId: MARKET, tokenId: `guardian-${outcome.toLowerCase()}`, outcome, asset: "XRP",
    receivedAt: new Date(s.clock.ms).toISOString(), sourceTimestamp: new Date(s.clock.ms).toISOString(),
    bids: [{ price: held ? "0.60" : "0.39", quantity: "10000" }],
    asks: [{ price: held ? "0.81" : "0.40", quantity: "10000" }], ...overrides });
}
function setBook(s: Scenario, outcome: Outcome, overrides: Partial<MarketSnapshot> = {}) {
  const b = book(s, outcome, overrides);
  const pair = internal(s.engine).snapshots.get(MARKET) ?? {};
  pair[outcome] = b; internal(s.engine).snapshots.set(MARKET, pair);
  s.engine.state.marketSnapshots.set(b.tokenId, b); s.market.markTokenSync(b.tokenId, "SYNCED");
  return b;
}
const opposite = (s: Scenario): Outcome => s.held === "UP" ? "DOWN" : "UP";
async function seed(s: Scenario, held: Outcome = "UP") {
  s.held = held;
  await s.engine.beginExperiment();
  await s.market.emitSnapshot(book(s, "UP")); await s.market.emitSnapshot(book(s, "DOWN"));
  await s.engine.applyObservedFill(paperFill(MARKET, held, "0.80", {
    tokenId: `guardian-${held.toLowerCase()}`, quantity: "5", timestamp: new Date(START).toISOString(),
  }));
  s.clock.ms = START + 10_000;
  setBook(s, "UP"); setBook(s, "DOWN");
  s.engine.state.noteReferenceTick("XRP", new Date(s.clock.ms).toISOString());
}
const rows = (s: Scenario, type: string) => s.journal.queryEvents({ types: [type] });
const guardianOrders = async (s: Scenario) => (await rows(s, "PAPER_ORDER_CREATED")).filter((r) => r.payload.intent === "GUARDIAN_PROTECTIVE_SELL");
const guardianFills = async (s: Scenario) => (await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "GUARDIAN_PROTECTIVE_SELL");
const residual = (s: Scenario) => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity ?? "0";
async function waitFor(test: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await test())) {
    if (Date.now() > deadline) throw new Error("Guardian lifecycle did not complete");
    await new Promise((r) => setTimeout(r, 2));
  }
}
function guardianOwner(s: Scenario): RiskReductionOwnership {
  const o = internal(s.engine).evaluateGuardian(MARKET).decision?.order;
  expect(o).toBeTruthy();
  return { marketId: MARKET, unhedgedStartedAt: unhedgedEpisode(s.engine.ledger.positions.get(MARKET)!),
    intent: "GUARDIAN_PROTECTIVE_SELL", order: o!, admittedResidualQuantity: residual(s),
    decisionReceivedAt: new Date(s.clock.ms).toISOString(), state: "ADMITTED" };
}
function rescueOwner(s: Scenario, id = "rescue-first"): RiskReductionOwnership {
  const outcome = opposite(s);
  return { marketId: MARKET, unhedgedStartedAt: unhedgedEpisode(s.engine.ledger.positions.get(MARKET)!),
    intent: "RESCUE_SECOND_LEG", order: order("TAKER", { id, marketId: MARKET, tokenId: `guardian-${outcome.toLowerCase()}`,
      outcome, side: "BUY", quantity: "5", requestedPrice: "0.30", createdAt: new Date(s.clock.ms).toISOString() }),
    admittedResidualQuantity: "5", decisionReceivedAt: new Date(s.clock.ms).toISOString(), state: "ADMITTED" };
}
async function actualRescueFailure(s: Scenario) {
  const outcome = opposite(s);
  const decisionBook = setBook(s, outcome, { asks: [{ price: "0.30", quantity: "10000" }] });
  const owner = rescueOwner(s);
  // Decision book had full depth; the real PAPER simulator sees only two
  // shares after admission and produces an actual execution failure.
  setBook(s, outcome, { asks: [{ price: "0.30", quantity: "2" }] });
  await internal(s.engine).submitPaperOrder(owner.order, decisionBook, "failed-rescue", owner.intent);
  expect((await rows(s, "PAPER_EXECUTION_ERROR"))[0]?.payload.orderId).toBe(owner.order.id);
  expect(internal(s.engine).riskReductionOwners.size).toBe(0);
  setBook(s, outcome, { asks: [{ price: "0.30", quantity: "10000" }] });
  return owner;
}
async function restart(s: Scenario) {
  await s.engine.stop();
  const recovered = await start(s.journal, s.clock);
  recovered.held = s.held;
  return recovered;
}

describe("Engine Guardian real PAPER authority", () => {
  it.each(["cap", "depth"])("unrelated hourly execution errors cannot veto a real full Guardian SELL after PR3 %s failure", async (failure) => {
    const s = await start(); try {
      await seed(s);
      if (failure === "depth") setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "2" }] });
      const maxErrors = riskLimitsSchema.parse({}).maxExecutionErrorsPerHour;
      s.engine.seedRuntimeBookkeepingForTest({ executionErrors: Array(maxErrors + 1).fill(s.clock.ms - 1000) });
      expect(await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_FAILED"], limit: 100 })).toHaveLength(0);
      expect(internal(s.engine).evaluateGuardian(MARKET).decision?.accepted).toBe(true);
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      const orders = await guardianOrders(s);
      const fills = await guardianFills(s);
      expect(orders).toHaveLength(1); expect(fills).toHaveLength(1);
      expect(fills[0]?.payload).toMatchObject({ orderId: orders[0]?.aggregateId, side: "SELL", outcome: "UP", quantity: "5" });
      expect(residual(s)).toBe("0"); expect(internal(s.engine).riskReductionOwners.size).toBe(0);
    } finally { await s.engine.stop(); }
  });
  it.each(["database", "journal", "integrity"])("hourly-error exemption retains the %s hard Guardian authority fence", async (fault) => {
    const s = await start(); try {
      await seed(s);
      s.engine.seedRuntimeBookkeepingForTest({ executionErrors: Array(100).fill(s.clock.ms - 1000) });
      expect(internal(s.engine).evaluateGuardian(MARKET).decision?.accepted).toBe(true);
      if (fault === "database") s.engine.state.database = "DOWN";
      if (fault === "journal") s.engine.state.journalDegraded = true;
      if (fault === "integrity") s.engine.abandonWithoutCheckpointForTest();
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("HARD_AUTHORITY_UNAVAILABLE");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0); expect(await guardianFills(s)).toHaveLength(0);
      expect(await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 100 })).toHaveLength(0);
      expect(residual(s)).toBe("5");
    } finally { await s.engine.stop(); }
  });
  it("an actual Guardian execution failure remains durable and observable despite the hourly-error exemption", async () => {
    const s = await start(); let release!: () => void;
    try {
      await seed(s);
      s.engine.seedRuntimeBookkeepingForTest({ executionErrors: Array(100).fill(s.clock.ms - 1000) });
      const executionErrorsBefore = s.engine.runtimeBookkeepingForTest().executionErrors;
      s.journal.setAppendInFlightHoldForTest(new Promise<void>((r) => { release = r; }), ["PAPER_ORDER_CREATED"]);
      const pending = internal(s.engine).maybeGuardianProtectiveSell(MARKET);
      await waitFor(() => internal(s.engine).riskReductionOwners.has(MARKET) && s.journal.inFlightAppends > 0);
      const owner = internal(s.engine).riskReductionOwners.get(MARKET)!;
      // Fresh valid held book still belongs to this exact residual, but no
      // longer supplies the full order. The actual simulator must fail.
      setBook(s, "UP", { bids: [{ price: "0.60", quantity: "2" }] });
      release(); s.journal.setAppendInFlightHoldForTest(null);
      await pending;
      const failed = await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_FAILED"], limit: 100 });
      expect(failed).toHaveLength(1);
      expect(failed[0]?.payload).toMatchObject({ executionFailure: true, reason: "INSUFFICIENT_DEPTH",
        ownership: { intent: "GUARDIAN_PROTECTIVE_SELL", order: { id: owner.order.id } } });
      expect((await rows(s, "PAPER_EXECUTION_ERROR"))[0]?.payload).toMatchObject({
        intent: "GUARDIAN_PROTECTIVE_SELL", orderId: owner.order.id, message: "INSUFFICIENT_DEPTH" });
      expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(executionErrorsBefore);
      expect(await guardianFills(s)).toHaveLength(0); expect(residual(s)).toBe("5");
      expect(internal(s.engine).riskReductionOwners.size).toBe(0);
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it.each(["OPEN_FIRST_LEG", "HEDGE_SECOND_LEG", "RESCUE_SECOND_LEG", "PROTECTIVE_DE_RISK"] as const)("retains the global execution-error veto for %s", (intent) => {
    const limits = riskLimitsSchema.parse({});
    expect(evaluateRisk(safeRisk({ intent, executionErrorsLastHour: limits.maxExecutionErrorsPerHour + 1 }), limits).vetoes)
      .toContain("ABNORMAL_EXECUTION_ERRORS");
  });
  it("yields to canonical normal profitable hedge", async () => {
    const s = await start(); try {
      await seed(s); setBook(s, "DOWN", { bids: [{ price: "0.09", quantity: "10000" }], asks: [{ price: "0.10", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("NORMAL_HEDGE_PRIORITY");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it("yields to full affordable PR3 pair lock", async () => {
    const s = await start(); try {
      await seed(s); setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("PR3_PRIORITY");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianOrders(s)).toHaveLength(0);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "RESCUE_SECOND_LEG")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("valid hedge maker owns the residual before Guardian", async () => {
    const s = await start(); try {
      await seed(s);
      const maker = order("MAKER", { id: "healthy-hedge-maker", marketId: MARKET, tokenId: "guardian-down",
        outcome: "DOWN", quantity: "5", requestedPrice: "0.39", createdAt: new Date(s.clock.ms).toISOString() });
      await internal(s.engine).reserveMaker(maker, book(s, "DOWN"), "HEDGE_SECOND_LEG");
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("HEDGE_MAKER_PRIORITY");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it.each(["UP", "DOWN"] as const)("meteor: cap failure really sells full held %s and records actual terminal PnL", async (held) => {
    const s = await start(); try {
      await seed(s, held);
      const before = s.engine.ledger.snapshot();
      // Trigger through the actual market adapter callback, not a synthetic
      // accounting close or a policy-only decision.
      await s.market.emitSnapshot(book(s, held));
      await waitFor(async () => (await guardianFills(s)).length === 1);
      const created = (await guardianOrders(s))[0]!;
      const fill = (await guardianFills(s))[0]!;
      expect(created.payload).toMatchObject({ side: "SELL", outcome: held, tokenId: `guardian-${held.toLowerCase()}`, quantity: "5" });
      expect(fill.payload).toMatchObject({ orderId: created.aggregateId, side: "SELL", outcome: held, quantity: "5", fillPrice: "0.6", feeUsd: "0.08400" });
      expect(d(residual(s)).eq(0)).toBe(true);
      expect(s.engine.ledger.snapshot().cash).toBe(d(before.cash).plus("2.916").toFixed());
      expect(s.engine.ledger.snapshot().netTradingPnl).toBe("-1.084");
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
      expect((await rows(s, "POSITION_UPDATED")).some((r) => r.sourceEventId === fill.aggregateId)).toBe(true);
      expect((await rows(s, "POSITION_UPDATED")).find((r) => r.sourceEventId === fill.aggregateId)?.payload).toMatchObject({
        unpairedQuantity: "0", totalFees: "0.084", closed: true,
        deRisk: { finalState: "RISK_NEUTRALIZED", residualQuantity: "0" },
      });
      expect((await rows(s, "GUARDIAN_EVALUATION"))[0]?.payload.reason).toBe("PR3_LOCKED_LOSS_EXCEEDS_CAP");
      s.clock.ms = START + 600_000;
      setBook(s, "UP"); setBook(s, "DOWN");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      await internal(s.engine).maybeEarlyCatastrophicPairLock(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each(["2", "0"])("full opposite depth failure (%s shares) permits real SELL", async (qty) => {
    const s = await start(); try {
      await seed(s); setBook(s, "DOWN", { asks: qty === "0" ? [] : [{ price: "0.30", quantity: qty }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("PR3_FULL_DEPTH_UNAVAILABLE");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianFills(s)).toHaveLength(1); expect(residual(s)).toBe("0");
    } finally { await s.engine.stop(); }
  });
  it("actual same-episode rescue failure overrides affordable PR3 and survives checkpoint compaction", async () => {
    let s = await start(); try {
      await seed(s); const failed = await actualRescueFailure(s);
      await s.engine.checkpointForTest(); await s.journal.compactDurableTransitionsThrough(s.engine.lastAppliedDurableCursorForTest());
      s = await restart(s); setBook(s, "UP"); setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("SAME_EPISODE_RESCUE_EXECUTION_FAILED");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianFills(s)).toHaveLength(1);
      expect((await guardianOrders(s))[0]?.payload.deRisk).toMatchObject({ rescueFailureOrderId: failed.order.id });
    } finally { await s.engine.stop(); }
  });
  it("old-episode actual failure and generic execution errors never authorize Guardian", async () => {
    const s = await start(); try {
      await seed(s); await actualRescueFailure(s);
      s.engine.ledger.positions.get(MARKET)!.unhedgedStartedAt = new Date(START + 1).toISOString();
      s.clock.ms += 1; setBook(s, "UP"); setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("PR3_PRIORITY");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it("same-episode rescue failure still adopts a newly admissible normal hedge", async () => {
    const s = await start(); try {
      await seed(s); await actualRescueFailure(s);
      setBook(s, "DOWN", { bids: [{ price: "0.09", quantity: "10000" }], asks: [{ price: "0.10", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("NORMAL_HEDGE_PRIORITY");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianOrders(s)).toHaveLength(0);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
      expect(residual(s)).toBe("0");
    } finally { await s.engine.stop(); }
  });
  it("a lifecycle release is not an actual rescue execution failure", async () => {
    const s = await start(); try {
      await seed(s); const owner = rescueOwner(s);
      await internal(s.engine).admitRiskReduction(owner);
      await internal(s.engine).failRiskReduction(owner, "ADMITTED_RESIDUAL_CHANGED");
      setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }] });
      expect(internal(s.engine).evaluateGuardian(MARKET).reason).toBe("PR3_PRIORITY");
    } finally { await s.engine.stop(); }
  });
  it.each(["partial", "stale", "future", "unsynchronized", "unknown-fee", "unknown-minimum"])("does not fabricate full SELL from %s held book", async (mode) => {
    const s = await start(); try {
      await seed(s);
      setBook(s, "UP", mode === "partial" ? { bids: [{ price: "0.60", quantity: "2" }] }
        : mode === "stale" ? { receivedAt: new Date(START - 60_000).toISOString() }
        : mode === "future" ? { receivedAt: new Date(s.clock.ms + 1).toISOString() }
        : mode === "unknown-fee" ? { feeSchedule: null }
        : mode === "unknown-minimum" ? { minOrderSize: null } : {});
      if (mode === "unsynchronized") s.market.markTokenSync("guardian-up", "GAPPED");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0); expect(residual(s)).toBe("5");
    } finally { await s.engine.stop(); }
  });
  it.each(["age", "loss"])("keeps locked %s threshold", async (gate) => {
    const s = await start(); try {
      await seed(s);
      if (gate === "age") s.clock.ms -= 1;
      setBook(s, "UP", gate === "loss" ? { bids: [{ price: "0.79", quantity: "10000" }] } : {}); setBook(s, "DOWN");
      expect(await internal(s.engine).maybeGuardianProtectiveSell(MARKET)).toBe(false);
      expect(await guardianOrders(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it.each(["normal", "PR3-economics", "PR3-owner", "residual", "held-depth", "token"])("serialized admission rejects the %s race", async (race) => {
    const s = await start(); try {
      await seed(s); const owner = guardianOwner(s);
      let pending!: Promise<boolean>;
      await internal(s.engine).durableGate.run(async () => {
        pending = internal(s.engine).durableGate.outside(() => internal(s.engine).admitRiskReduction(owner));
        await new Promise((r) => setTimeout(r, 5));
        if (race === "normal") setBook(s, "DOWN", { bids: [{ price: "0.09", quantity: "10000" }], asks: [{ price: "0.10", quantity: "10000" }] });
        if (race === "PR3-economics") setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }] });
        if (race === "PR3-owner") expect(await internal(s.engine).admitRiskReduction(rescueOwner(s))).toBe(true);
        if (race === "residual") s.engine.ledger.positions.get(MARKET)!.unpairedQuantity = "4";
        if (race === "held-depth") setBook(s, "UP", { bids: [{ price: "0.60", quantity: "2" }] });
        if (race === "token") owner.order.tokenId = "wrong-held-token";
      });
      expect(await pending).toBe(false); expect(await guardianOrders(s)).toHaveLength(0);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it("concurrent Guardian callbacks produce one admitted owner, order and fill", async () => {
    const s = await start(); try {
      await seed(s);
      await Promise.all(Array.from({ length: 8 }, () => internal(s.engine).maybeGuardianProtectiveSell(MARKET)));
      expect(await guardianOrders(s)).toHaveLength(1); expect(await guardianFills(s)).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("a new episode during evaluation persistence cannot inherit the old Guardian decision", async () => {
    const s = await start(); let release!: () => void;
    try {
      await seed(s);
      s.journal.setAppendInFlightHoldForTest(new Promise<void>((r) => { release = r; }), ["GUARDIAN_EVALUATION"]);
      const pending = internal(s.engine).maybeGuardianProtectiveSell(MARKET);
      await waitFor(() => s.journal.inFlightAppends > 0);
      s.engine.ledger.positions.get(MARKET)!.unhedgedStartedAt = new Date(START + 1).toISOString();
      release(); s.journal.setAppendInFlightHoldForTest(null);
      expect(await pending).toBe(false); expect(await guardianOrders(s)).toHaveLength(0);
      expect(residual(s)).toBe("5");
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)("restart after Guardian admission at %s retains original identity", async (point) => {
    let s = await start(); try {
      await seed(s); const owner = guardianOwner(s);
      s.engine.setCrashInjectionForTest(point, { operationKey: `risk-reduction:${owner.order.id}:admitted` });
      await expect(internal(s.engine).admitRiskReduction(owner)).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s);
      expect(internal(s.engine).riskReductionOwners.get(MARKET)?.order.id).toBe(owner.order.id);
      await s.market.emitSnapshot(book(s, "UP")); await s.market.emitSnapshot(book(s, "DOWN"));
      await waitFor(async () => (await guardianFills(s)).length === 1);
      expect((await guardianOrders(s))[0]?.aggregateId).toBe(owner.order.id); expect(await guardianOrders(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)("restart after Guardian fill at %s never sells again", async (point) => {
    let s = await start(); try {
      await seed(s); const owner = guardianOwner(s);
      s.engine.setCrashInjectionForTest(point, { operationKey: `fill:risk-reduction-fill:${owner.order.id}` });
      await expect(internal(s.engine).submitPaperOrder(owner.order, book(s, "UP"), "crash-fill", owner.intent)).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s); const cash = s.engine.ledger.snapshot().cash;
      expect(residual(s)).toBe("0"); expect(internal(s.engine).riskReductionOwners.size).toBe(0);
      expect(await guardianFills(s)).toHaveLength(1); expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
      await s.market.emitSnapshot(book(s, "UP")); await s.market.emitSnapshot(book(s, "DOWN"));
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianFills(s)).toHaveLength(1); expect(s.engine.ledger.snapshot().cash).toBe(cash);
    } finally { await s.engine.stop(); }
  });
  it.each(["residual", "settlement", "depth"])("latency %s change cannot consume an obsolete residual", async (change) => {
    const s = await start(undefined, undefined, "50"); try {
      await seed(s); const owner = guardianOwner(s);
      const pending = internal(s.engine).submitPaperOrder(owner.order, book(s, "UP"), "latency", owner.intent);
      await waitFor(async () => (await guardianOrders(s)).length === 1);
      if (change === "residual") s.engine.ledger.positions.get(MARKET)!.unpairedQuantity = "4";
      if (change === "depth") setBook(s, "UP", { bids: [{ price: "0.60", quantity: "2" }] });
      if (change === "settlement") await internal(s.engine).commitAuthoritative({ operationKey: `settlement:${MARKET}:${s.engine.state.experimentId ?? "paper"}`,
        type: "POSITION_SETTLED", aggregateType: "position", aggregateId: MARKET,
        buildPayload: () => ({ marketId: MARKET, upPayoff: "1", downPayoff: "0", settledAt: new Date(s.clock.ms).toISOString(), derivedAudit: [] }) });
      await pending;
      expect(await guardianFills(s)).toHaveLength(0); expect(internal(s.engine).riskReductionOwners.size).toBe(0);
      const fill = executePaperTaker({ order: owner.order, snapshotAfterLatency: book(s, "UP"), measuredLatencyMs: 0 });
      if (change !== "settlement") await expect(internal(s.engine).applyFill(fill, { intent: owner.intent, riskReductionOwnership: owner })).rejects.toThrow("TAKER_AUTHORITY_RELEASED");
      else expect(await internal(s.engine).applyFill(fill, { intent: owner.intent, riskReductionOwnership: owner })).toBe(false);
      expect(await guardianFills(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it("pending Guardian owns the deadline phase so old protective cannot double consume", async () => {
    const s = await start(); try {
      await seed(s); const owner = guardianOwner(s);
      await internal(s.engine).admitRiskReduction(owner);
      s.clock.ms = START + 600_000;
      setBook(s, "UP"); setBook(s, "DOWN");
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await guardianFills(s)).toHaveLength(1);
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1); expect(residual(s)).toBe("0");
    } finally { await s.engine.stop(); }
  });
  it("released pre-deadline PR3 ownership hands the residual to the existing deadline tie-preferred SELL", async () => {
    const s = await start(); try {
      await seed(s);
      const zeroFee = { ...book(s, "UP").feeSchedule!, rate: "0", rebateRate: "0" };
      setBook(s, "UP", { bids: [{ price: "0.70", quantity: "10000" }], feeSchedule: zeroFee });
      setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }], feeSchedule: zeroFee });
      const owner = rescueOwner(s, "pr3-deadline-handoff");
      owner.deRisk = { deRiskReason: "EARLY_CATASTROPHIC_PAIR_LOCK", unhedgedStartedAt: owner.unhedgedStartedAt };
      expect(await internal(s.engine).admitRiskReduction(owner)).toBe(true);
      s.clock.ms = START + 600_000;
      setBook(s, "UP", { bids: [{ price: "0.70", quantity: "10000" }], feeSchedule: zeroFee });
      setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "10000" }], feeSchedule: zeroFee });
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      const failed = (await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_FAILED"], limit: 10 }))[0]!;
      expect(failed.payload).toMatchObject({ executionFailure: false, ownership: { order: { id: owner.order.id } } });
      expect(String(failed.payload.reason)).toContain("DEADLINE");
      const protective = (await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "PROTECTIVE_DE_RISK");
      expect(protective).toHaveLength(1);
      expect(protective[0]?.payload).toMatchObject({ side: "SELL", outcome: "UP", quantity: "5", fillPrice: "0.7" });
      expect(residual(s)).toBe("0"); expect(internal(s.engine).riskReductionOwners.size).toBe(0);
      expect(await guardianOrders(s)).toHaveLength(0);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "RESCUE_SECOND_LEG")).toHaveLength(0);
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(2);
    } finally { await s.engine.stop(); }
  });
});
