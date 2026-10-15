import { describe, expect, it, vi } from "vitest";
import { NATIVE_DETERMINISTIC_AUTHORITY, type MarketSnapshot, type GuardianV2State,
  type JournalEvent, type ExperimentManifest, CANONICAL_REPORT_INPUT_EVENT_TYPES, AUDIT_CRITICAL_EVENT_TYPES } from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { type RiskReductionOwnership } from "../apps/engine/src/recovery.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot, order } from "./fixtures.js";

const START = Date.parse("2026-10-05T00:00:00Z"), MARKET = "reversal-mkt", UP = "reversal-up", DOWN = "reversal-down";
type Internals = {
  reversalProtectionStates: Map<string, GuardianV2State>;
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  snapshots: Map<string, Partial<Record<"UP" | "DOWN", MarketSnapshot>>>;
  experiment: ExperimentManifest;
  maybeReversalProtection(m: string): Promise<void>;
  scheduleGuardianDeadline(): void;
  maybeProtectiveDeRisk(m: string): Promise<void>;
  pollFailedHedges(): Promise<void>;
  commitAuthoritative(input: Record<string, unknown>): Promise<unknown>;
  admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean>;
  durableGate: { run<T>(callback: () => Promise<T>): Promise<T> };
  awaitGuardianPersistence(marketId: string): Promise<void>;
  onMinOrderSizeHydrated(event: { tokenId: string; minOrderSize: string; tokenEpoch: number }): void;
  applyFill(...args: unknown[]): Promise<boolean>;
  cancelRestingHedgeMakers(marketId: string, reason: string): Promise<void>;
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
async function riskFills(s: Scenario) { return (await rows(s, "PAPER_FILL")).filter(r => ["PROTECTIVE_DE_RISK", "RESCUE_SECOND_LEG", "GUARDIAN_PROTECTIVE_SELL"].includes(String(r.payload.intent))); }
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
      calculatedAt: new Date(s.clock.ms).toISOString(), expiresAt: new Date(s.clock.ms + 15000).toISOString(),
      consensusConfidence: 0.65, estimatedEdge: 0.015, evidenceIds: ["same-evidence"], triggerId: "same-trigger" } });
}
const observe = (s: Scenario, id: string, direction: "UP" | "DOWN" | "FLAT" = "DOWN") => s.engine.persistReportInput(event(s, id, direction));
async function restart(s: Scenario, env: Record<string, string> = {}) { await s.engine.stop(); return start(s.journal, s.clock, env); }
async function legacy360Manifest(status: "COMPLETED" | "RUNNING"): Promise<ExperimentManifest> {
  const source = await start();
  try {
    const manifest = structuredClone(internal(source).experiment);
    return {
      ...manifest,
      status,
      riskConfiguration: { ...manifest.riskConfiguration, reversalProtectionEnabled: true,
        reversalProtectionWindowMs: 360000, reversalProtectionOppositeCount: 3 },
      startedAt: new Date(START - 60_000).toISOString(),
      entryClosesAt: new Date(START + 60_000).toISOString(),
      completedAt: status === "COMPLETED" ? new Date(START - 1_000).toISOString() : null,
      unresolvedPositionCount: 0,
      startValidationPending: false,
    };
  } finally { await source.engine.stop(); }
}
async function settle(s: Scenario) {
  await internal(s).commitAuthoritative({ operationKey: `settlement:${MARKET}:test`, type: "POSITION_SETTLED",
    aggregateType: "position", aggregateId: MARKET, buildPayload: () => ({ marketId: MARKET, upPayoff: "1", downPayoff: "0",
      settledAt: new Date(s.clock.ms).toISOString(), derivedAudit: [] }) });
}

async function publishSensor(s: Scenario, ratio: string, pressure = true, quantity = "10000") {
  const mid = Number(ratio) * .80;
  const up = book(s, "UP", (mid - .005).toFixed(6), (mid + .005).toFixed(6), quantity, s.clock.ms - START);
  const down = book(s, "DOWN", "0.70", "0.72", quantity, s.clock.ms - START);
  // (UP imbalance - DOWN imbalance)/2 = -.5 -> normalized +.5 against held UP.
  up.bids[0]!.quantity = pressure ? quantity : String(Number(quantity) * 3);
  up.asks[0]!.quantity = pressure ? String(Number(quantity) * 3) : quantity;
  down.bids[0]!.quantity = pressure ? String(Number(quantity) * 3) : quantity;
  down.asks[0]!.quantity = pressure ? quantity : String(Number(quantity) * 3);
  await s.market.emitSnapshot(up); await s.market.emitSnapshot(down);
  // Asynchronous snapshot ingestion schedules Guardian persistence; wait for the
  // durable projection, not just the in-memory observation, in PAPER tests.
  await internal(s).awaitGuardianPersistence(MARKET);
}
async function armEarly(s: Scenario) {
  await publishSensor(s, ".75");
  s.clock.ms = START + 5000; await publishSensor(s, ".70");
  s.clock.ms = START + 10000; await publishSensor(s, ".70");
  expect(state(s)?.phase).toBe("EARLY_RECOVERY_WATCH");
}
async function timeout(s: Scenario) {
  s.clock.ms = START + 70000;
  await internal(s).maybeReversalProtection(MARKET);
  expect(state(s)?.phase).toBe("PROTECTION_PENDING");
}
async function armLate(s: Scenario) {
  s.clock.ms = START + 200000; await publishSensor(s, ".60"); await observe(s, "late-arm");
  expect(state(s)?.phase).toBe("LATE_CONFIRMATION");
}

describe("Guardian V2 real PAPER lifecycle with independent emergency execution", () => {
  it.each(["cancellation", "admission"] as const)("confirmed Guardian survives normal strategy cash/risk vetoes during %s", async boundary => {
    for (const change of ["cash", "errors", "drawdown", "dailyLoss"] as const) {
      const s = await start();
      try {
        await seed(s); await armEarly(s); await timeout(s);
        const up = book(s, "UP", "0.60", "0.81"), down = book(s, "DOWN", "0.29", "0.30");
        internal(s).snapshots.set(MARKET, { UP: up, DOWN: down });
        s.engine.state.marketSnapshots.set(UP, up); s.engine.state.marketSnapshots.set(DOWN, down);
        let changed = false, plans = 0;
        const mutate = () => {
          if (changed) return; changed = true;
          if (change === "cash") s.engine.ledger.reserve(s.engine.ledger.availableCash().minus("0.01"), new Date(s.clock.ms));
          else if (change === "errors") (s.engine as unknown as { executionErrors: number[] }).executionErrors.push(...Array(5).fill(s.clock.ms));
          else {
            const ledger = s.engine.ledger.snapshot();
            if (change === "drawdown") ledger.drawdown = "100"; else ledger.netTradingPnl = "-50";
            s.engine.ledger.restore(ledger, [...s.engine.ledger.positions.values()]);
          }
        };
        const commit = s.journal.commitDurableTransition.bind(s.journal);
        s.journal.commitDurableTransition = (input, cursor, options) => {
          if (input.type === "RISK_REDUCTION_ADMITTED") { plans++; if (boundary === "admission") mutate(); }
          return commit(input, cursor, options);
        };
        if (boundary === "cancellation") {
          const cancel = internal(s).cancelRestingHedgeMakers.bind(internal(s));
          internal(s).cancelRestingHedgeMakers = async (marketId, reason) => { mutate(); await cancel(marketId, reason); };
        }
        await internal(s).maybeReversalProtection(MARKET);
        expect(changed).toBe(true); expect(plans).toBe(1);
        const fills = await riskFills(s);
        expect(fills).toHaveLength(1);
        expect(fills[0]!.payload).toMatchObject({ intent: "GUARDIAN_PROTECTIVE_SELL", quantity: "5", side: "SELL", fillPrice: "0.6" });
        expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1); expect(residual(s)).toBe("0");
        expect(s.engine.integrityFault()).toBe(false);
        console.log(JSON.stringify({ evidence: "R2_REAL_RISK_REVALIDATION", boundary, change, plans, fills: fills.length }));
      } finally { await s.engine.stop(); }
    }
  });

  it.each(["expired", "future", "missing"])("inline LATE cannot arm from %s consensus, matching independent worker", async mode => {
    const s = await start();
    try {
      await seed(s); s.clock.ms = START + 200000; await publishSensor(s, ".60");
      const bad = event(s, "bad-" + mode);
      if (mode === "expired") bad.payload.expiresAt = new Date(s.clock.ms - 1).toISOString();
      if (mode === "future") bad.payload.calculatedAt = new Date(s.clock.ms + 1).toISOString();
      if (mode === "missing") delete bad.payload.expiresAt;
      await s.engine.persistReportInput(bad);
      expect(state(s)?.phase).not.toBe("LATE_CONFIRMATION");
      expect(await rows(s, "GUARDIAN_LATE_SEVERITY_ARMED")).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });

  it("continuous book churn no longer waits on normal A/B audit or exhausts its REQUOTE budget", async () => {
    const s = await start();
    try {
      await seed(s); await armEarly(s); await timeout(s);
      const install = () => {
        const up = book(s, "UP", s.clock.ms % 2 ? "0.76" : "0.75", "0.81"), down = book(s, "DOWN", "0.29", "0.40");
        internal(s).snapshots.set(MARKET, { UP: up, DOWN: down });
        s.engine.state.marketSnapshots.set(UP, up); s.engine.state.marketSnapshots.set(DOWN, down);
      };
      install(); let plans = 0; const original = s.journal.commitDurableTransition.bind(s.journal);
      s.journal.commitDurableTransition = async (input, cursor, options) => {
        if (input.type === "RISK_REDUCTION_ADMITTED") { plans++; s.clock.ms++; install(); }
        return original(input, cursor, options);
      };
      const started = performance.now();
      await internal(s).maybeReversalProtection(MARKET);
      expect(plans).toBe(1); expect(await riskFills(s)).toHaveLength(1); expect(residual(s)).toBe("0");
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      console.log(JSON.stringify({ evidence: "GUARDIAN_CHURN_TO_REAL_FILL", attempts: plans, elapsedMs: performance.now() - started,
        chain: (await rows(s, "GUARDIAN_EVALUATION")).map(r => r.payload), fills: (await riskFills(s)).map(r => r.payload) }));
    } finally { await s.engine.stop(); }
  });

  it("same economics with a new book sequence/timestamp do not consume requote budget", async () => {
    const s = await start();
    try {
      await seed(s); await armEarly(s); await timeout(s);
      const install = () => {
        const up = book(s, "UP", "0.75", "0.81", "10000", s.clock.ms), down = book(s, "DOWN", "0.29", "0.40", "10000", s.clock.ms);
        internal(s).snapshots.set(MARKET, { UP: up, DOWN: down });
        s.engine.state.marketSnapshots.set(UP, up); s.engine.state.marketSnapshots.set(DOWN, down);
      };
      install(); let plans = 0; const original = s.journal.commitDurableTransition.bind(s.journal);
      s.journal.commitDurableTransition = async (input, cursor, options) => {
        if (input.type === "RISK_REDUCTION_ADMITTED") { plans++; s.clock.ms++; install(); }
        return original(input, cursor, options);
      };
      await internal(s).maybeReversalProtection(MARKET);
      expect(plans).toBe(1); expect(await riskFills(s)).toHaveLength(1); expect(residual(s)).toBe("0");
    } finally { await s.engine.stop(); }
  });
  it.each(["cancellation", "admission"])("revalidates Guardian SELL after a changed book during %s wait, with one real fill", async boundary => {
    const s = await start();
    try {
      await seed(s); await armEarly(s); await timeout(s);
      const install = (bid: string, ask: string) => {
        const up = book(s, "UP", bid, "0.81"), down = book(s, "DOWN", "0.29", ask);
        internal(s).snapshots.set(MARKET, { UP: up, DOWN: down });
        s.engine.state.marketSnapshots.set(UP, up); s.engine.state.marketSnapshots.set(DOWN, down);
      };
      install("0.75", "0.40"); // A initially dominates .60 B salvage
      let changed = false;
      if (boundary === "cancellation") {
        const original = internal(s).cancelRestingHedgeMakers.bind(internal(s));
        internal(s).cancelRestingHedgeMakers = async (marketId, reason) => {
          if (!changed) { changed = true; s.clock.ms++; install("0.30", "0.40"); }
          await original(marketId, reason);
        };
      } else {
        const original = s.journal.commitDurableTransition.bind(s.journal);
        s.journal.commitDurableTransition = async (input, cursor, options) => {
          if (!changed && input.type === "RISK_REDUCTION_ADMITTED") {
            changed = true; s.clock.ms++; install("0.30", "0.40");
          }
          return original(input, cursor, options);
        };
      }
      await internal(s).maybeReversalProtection(MARKET);
      expect(changed).toBe(true); const fills = await riskFills(s);
      expect(fills).toHaveLength(1);
      expect(fills[0]!.payload).toMatchObject({ intent: "GUARDIAN_PROTECTIVE_SELL", side: "SELL", quantity: "5", fillPrice: "0.3" });
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect(residual(s)).toBe("0"); expect(s.engine.integrityFault()).toBe(false);
      console.log(JSON.stringify({ evidence: "QUOTE_WAIT_REARBITRATION", boundary, selection: fills[0]!.payload.intent,
        admittedOwners: 1, fullResidual: "5", actualFills: 1 }));
    } finally { await s.engine.stop(); }
  });

  it("general V2 protection cannot fill a book which ages past freshness AFTER durable admission", async () => {
    const s = await start();
    try {
      await seed(s); await armEarly(s); await timeout(s);
      const up = book(s, "UP", "0.75", "0.81"), down = book(s, "DOWN", "0.29", "0.40");
      internal(s).snapshots.set(MARKET, { UP: up, DOWN: down });
      s.engine.state.marketSnapshots.set(UP, up); s.engine.state.marketSnapshots.set(DOWN, down);
      const original = internal(s).admitRiskReduction.bind(internal(s));
      internal(s).admitRiskReduction = async owner => { const admitted = await original(owner); s.clock.ms += 16000; return admitted; };
      await internal(s).maybeReversalProtection(MARKET);
      expect(await riskFills(s)).toHaveLength(0); expect(residual(s)).toBe("5");
      expect(await rows(s, "RISK_REDUCTION_FAILED")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it("EARLY arms from price/book snapshots with zero opposite consensus", async () => {
    const s = await start(); try { await seed(s); await armEarly(s);
      expect(await riskFills(s)).toHaveLength(0); expect(await rows(s, "GUARDIAN_EARLY_DETERIORATION_ARMED")).toHaveLength(1);
      expect(await rows(s, "CONSENSUS_DECISION")).toHaveLength(0); expect(residual(s)).toBe("5");
    } finally { await s.engine.stop(); }
  });
  it.each([
    ["0.75", "0.30", "PROTECTIVE_DE_RISK", false],
    ["0.60", "0.30", "RESCUE_SECOND_LEG", false],
    ["0.70", "0.30", "PROTECTIVE_DE_RISK", true],
  ] as const)("confirmed Guardian owns execution at held bid %s opposite ask %s regardless of normal %s", async (bid, ask, _intent, _tie) => {
    const s = await start(); try { await seed(s); await armEarly(s); await timeout(s); await publish(s, bid, ask);
      await waitFor(s, () => residual(s) === "0");
      const fills = await riskFills(s); expect(fills).toHaveLength(1);
      expect(fills[0]?.payload).toMatchObject({ intent: "GUARDIAN_PROTECTIVE_SELL", side: "SELL", quantity: "5",
        deRisk: { deRiskReason: "REVERSAL_CONFIRMED", guardianSensor: "EARLY", guardianReason: "EARLY_TIMEOUT", selection: "PROTECTIVE_DE_RISK", tieBreakUsed: false } });
      expect(state(s)).toBeUndefined(); expect(await rows(s, "GUARDIAN_EXECUTION_COMPLETED")).toHaveLength(1);
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]?.payload.orderType).toBe("TAKER"); expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it.each([[69999, ".95", false], [70000, ".95", true], [70001, "1", true]] as const)(
    "strict deadline: at %sms ratio %s protection=%s", async (ms, ratio, protection) => {
      const s = await start(); try { await seed(s); await armEarly(s); s.clock.ms = START + ms; await publishSensor(s, ratio);
        await internal(s).maybeReversalProtection(MARKET);
        expect((await rows(s, "GUARDIAN_EARLY_TRUE_RECOVERY")).length).toBe(protection ? 0 : 1);
        expect((await rows(s, "GUARDIAN_EARLY_TIMEOUT_PROTECT")).length).toBe(protection ? 1 : 0);
      } finally { await s.engine.stop(); }
    });
  it("fake rebound .75 cannot release, .15 immediately confirms armed EARLY", async () => {
    const s = await start(); try { await seed(s); await armEarly(s); s.clock.ms += 1000; await publishSensor(s, ".75");
      expect(state(s)?.phase).toBe("EARLY_RECOVERY_WATCH"); s.clock.ms += 1000; await publishSensor(s, ".15");
      expect(await rows(s, "GUARDIAN_EARLY_EMERGENCY_PROTECT")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each([true, false])("LATE confirmation at 30s uses decision-boundary book pressure: %s", async pressure => {
    const s = await start(); try { await seed(s); await armLate(s); expect(await riskFills(s)).toHaveLength(0);
      s.clock.ms += 29999; await publishSensor(s, ".574", pressure); expect(state(s)?.phase).toBe("LATE_CONFIRMATION");
      s.clock.ms += 1; await publishSensor(s, ".574", pressure); await internal(s).maybeReversalProtection(MARKET);
      expect(await rows(s, pressure ? "GUARDIAN_LATE_CONFIRMATION_ACCEPTED" : "GUARDIAN_LATE_CONFIRMATION_RELEASED")).toHaveLength(1);
      if (!pressure) expect(await riskFills(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it("LATE floor protects immediately and is attributed to LATE", async () => {
    const s = await start(); try { await seed(s); await armLate(s); s.clock.ms++; await publishSensor(s, ".15");
      expect(await rows(s, "GUARDIAN_LATE_EMERGENCY_PROTECT")).toHaveLength(1);
      await waitFor(s, async () => (await riskFills(s)).length === 1);
      expect((await riskFills(s))[0]?.payload.deRisk).toMatchObject({ guardianSensor: "LATE", guardianReason: "LATE_EMERGENCY" });
    } finally { await s.engine.stop(); }
  });
  it.each(["EARLY", "LATE"])("normal target hedge during %s cancels Guardian", async sensor => {
    const s = await start(); try { await seed(s); if (sensor === "EARLY") await armEarly(s); else await armLate(s);
      s.clock.ms++; await publish(s, "0.79", "0.08"); await waitFor(s, () => residual(s) === "0");
      expect(state(s)).toBeUndefined(); s.clock.ms += 60000; await internal(s).maybeReversalProtection(MARKET);
      expect(await riskFills(s)).toHaveLength(0);
      expect((await rows(s, "PAPER_FILL")).filter(r => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it.each(["EARLY", "LATE"])("settlement during %s cancels Guardian", async sensor => {
    const s = await start(); try { await seed(s); if (sensor === "EARLY") await armEarly(s); else await armLate(s);
      await settle(s); const cash = s.engine.ledger.snapshot().cash; s.clock.ms += 60000;
      await internal(s).maybeReversalProtection(MARKET); expect(state(s)).toBeUndefined();
      expect(await riskFills(s)).toHaveLength(0); expect(s.engine.ledger.snapshot().cash).toBe(cash);
    } finally { await s.engine.stop(); }
  });
  it("full residual only: partial/non-executable depth retains durable pending and retries", async () => {
    let s = await start(); try { await seed(s); await armEarly(s); await timeout(s); await publish(s, "0.60", "0.30", "2");
      await internal(s).maybeReversalProtection(MARKET); expect(residual(s)).toBe("5"); expect(await riskFills(s)).toHaveLength(0);
      expect(state(s)?.phase).toBe("PROTECTION_PENDING");
      await s.engine.checkpointForTest(); await s.journal.compactDurableTransitionsThrough(s.engine.lastAppliedDurableCursorForTest());
      s = await restart(s); expect(state(s)?.phase).toBe("PROTECTION_PENDING");
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 2);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
      s = await restart(s); await publish(s); await internal(s).maybeReversalProtection(MARKET);
      expect(await riskFills(s)).toHaveLength(1); expect(state(s)).toBeUndefined();
    } finally { await s.engine.stop(); }
  });
  it.each(["stale", "unsynchronized"])("%s book cannot arm sensor or execute pending protection", async mode => {
    const s = await start(); try { await seed(s); await armEarly(s); await timeout(s); await publish(s, null, null);
      s.clock.ms += 5001;
      if (mode === "stale") { await publish(s); s.clock.ms += 16000; }
      else { s.market.markTokenSync(UP, "GAPPED"); s.market.markTokenSync(DOWN, "GAPPED"); }
      await internal(s).maybeReversalProtection(MARKET);
      expect(state(s)?.phase).toBe("PROTECTION_PENDING"); expect(await riskFills(s)).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });
  it("concurrent deadline callbacks produce one ownership/order/fill", async () => {
    const s = await start(); try { await seed(s); await armEarly(s); await timeout(s);
      await Promise.all([publish(s), internal(s).maybeReversalProtection(MARKET), internal(s).maybeReversalProtection(MARKET)]);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1); expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("retry cannot steal ownership from normal profitable hedge", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await armEarly(s);
      s.journal.setAppendInFlightHoldForTest(new Promise<void>(r => release = r), ["PAPER_ORDER_CREATED"]);
      s.clock.ms++; await publish(s, "0.79", "0.08");
      await waitFor(s, () => internal(s).riskReductionOwners.size === 1 && s.journal.inFlightAppends > 0);
      s.clock.ms = START + 70000;
      // The admitted hedge must resume on a genuinely fresh book after this
      // 60s clock jump; the original admission-era book is no longer executable.
      await publish(s, "0.79", "0.08", "10000", 2);
      const pending = internal(s).maybeReversalProtection(MARKET); release(); s.journal.setAppendInFlightHoldForTest(null);
      await pending; await waitFor(s, () => residual(s) === "0");
      expect((await rows(s, "PAPER_FILL")).filter(r => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1); expect(state(s)).toBeUndefined();
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it("normal .05 arm/.02 retrace stays active during EARLY watch", async () => {
    const s = await start(); try { await seed(s); await armEarly(s); s.clock.ms++;
      await publish(s, "0.79", "0.15"); await waitFor(s, async () => (await rows(s, "PROFIT_TRAILING_PAIR_STATE_UPDATED")).length > 0);
      s.clock.ms++; await publish(s, "0.79", "0.18", "10000", 2); await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(0); expect((await rows(s, "PAPER_ORDER_CREATED")).some(r => r.payload.strategyId === "profit-trailing-pair-trail")).toBe(true);
    } finally { await s.engine.stop(); }
  });
  it("ENTRY_CLOSED still protects the current episode", async () => {
    const s = await start(); try { await seed(s); await armEarly(s); await s.engine.closeExperimentEntries(); await timeout(s); await publish(s);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("confirmed Guardian cancels the normal hedge maker and fences a delayed partial fill and duplicate cancel", async () => {
    const s = await start();
    try {
      await seed(s); await armEarly(s);
      const maker = order("MAKER", { id: "normal-maker-before-confirmation", marketId: MARKET, tokenId: DOWN,
        outcome: "DOWN", quantity: "5", requestedPrice: "0.29", createdAt: new Date(s.clock.ms).toISOString() });
      await s.engine.restMakerForTest(maker, book(s, "DOWN", "0.29", "0.30"), "HEDGE_SECOND_LEG");
      await timeout(s);
      await publish(s, "0.60", "0.30");
      await waitFor(s, () => residual(s) === "0");
      const cash = s.engine.ledger.snapshot().cash;
      const delayed = paperFill(MARKET, "DOWN", "0.29", { orderId: maker.id, tokenId: DOWN, quantity: "2", liquidityRole: "MAKER" });
      expect(await internal(s).applyFill(delayed, { intent: "HEDGE_SECOND_LEG" })).toBe(false);
      await s.engine.cancelMakerForTest(maker.id, "GUARDIAN_PRIORITY");
      expect(s.engine.ledger.snapshot().cash).toBe(cash);
      expect(await riskFills(s)).toHaveLength(1);
      expect(s.engine.restingOrderCount()).toBe(0); expect(s.engine.reservedCash()).toBe("0");
      const transitions = await s.journal.queryDurableTransitions({ limit: 1000 });
      const cancel = transitions.find(t => t.operationKey === `maker:${maker.id}:cancel`)!;
      const admitted = transitions.find(t => t.type === "RISK_REDUCTION_ADMITTED")!;
      expect(cancel.sequence).toBeLessThan(admitted.sequence); expect(s.engine.integrityFault()).toBe(false);
      console.log(JSON.stringify({ evidence: "GUARDIAN_MAKER_HANDOFF", cancelSequence: cancel.sequence, admissionSequence: admitted.sequence, fills: 1, delayedPartialFillApplied: false }));
    } finally { await s.engine.stop(); }
  });
  it.each([false, true])("confirmed Guardian releases a previously admitted normal taker even with oppositeBookMissing=%s and prevents duplicate execution", async missingOpposite => {
    const s = await start();
    try {
      await seed(s); await armEarly(s);
      const position = s.engine.ledger.positions.get(MARKET)!;
      const hedge = order("TAKER", { id: "normal-pending-before-confirmation", marketId: MARKET, tokenId: DOWN,
        outcome: "DOWN", quantity: "5", requestedPrice: "0.30", createdAt: new Date(s.clock.ms).toISOString() });
      const owner: RiskReductionOwnership = { marketId: MARKET, unhedgedStartedAt: position.unhedgedStartedAt ?? position.openedAt,
        intent: "HEDGE_SECOND_LEG", order: hedge, admittedResidualQuantity: "5", decisionReceivedAt: new Date(s.clock.ms).toISOString(), state: "ADMITTED" };
      expect(await internal(s).admitRiskReduction(owner)).toBe(true);
      await timeout(s);
      if (missingOpposite) {
        const held = book(s, "UP", "0.60", "0.81");
        internal(s).snapshots.set(MARKET, { UP: held });
        s.engine.state.marketSnapshots.set(UP, held); s.engine.state.marketSnapshots.delete(DOWN);
        await internal(s).maybeReversalProtection(MARKET);
      } else await publish(s, "0.60", "0.30");
      await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(1);
      expect((await rows(s, "PAPER_FILL")).filter(r => r.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(0);
      expect((await rows(s, "RISK_REDUCTION_FAILED"))[0]?.payload.reason).toBe("GUARDIAN_PRIORITY");
      const delayed = paperFill(MARKET, "DOWN", "0.30", { orderId: hedge.id, tokenId: DOWN, quantity: "5" });
      await expect(internal(s).applyFill(delayed, { intent: "HEDGE_SECOND_LEG", riskReductionOwnership: owner })).rejects.toThrow("TAKER_AUTHORITY_RELEASED");
      expect(await riskFills(s)).toHaveLength(1); expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
  it("OFF preserves existing behavior; incompatible candidate settings reject start", async () => {
    let s = await start(undefined, undefined, { REVERSAL_PROTECTION_ENABLED: "false" });
    try { await seed(s); await publishSensor(s, ".15"); expect(state(s)).toBeUndefined(); expect(await riskFills(s)).toHaveLength(0); }
    finally { await s.engine.stop(); }
    const incompatible: Record<string, string>[] = [{ PROFIT_TRAILING_PAIR_SOAK_ENABLED: "false" },
      { NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.11" }, { PROFIT_TRAILING_PAIR_ARM_EDGE: "0.06" }];
    for (const env of incompatible) {
      s = await start(undefined, undefined, env);
      try { await expect(s.engine.beginExperiment()).rejects.toThrow("INVALID_REVERSAL_PROTECTION_V2_CONFIG"); }
      finally { await s.engine.stop(); }
    }
    await expect(start(undefined, undefined, { PAPER_EXECUTION_STYLE: "MAKER" })).rejects.toThrow("INVALID_PROFIT_TRAILING_PAIR_CONFIG");
  });
});

describe("Guardian V2 durable recovery and crash boundaries", () => {
  it("deadline wakeup protects without another snapshot or consensus", async () => {
    const s = await start();
    try { await seed(s); await armEarly(s);
      vi.useFakeTimers(); internal(s).scheduleGuardianDeadline();
      s.clock.ms = START + 70000; await vi.advanceTimersByTimeAsync(60000);
      expect(state(s)?.phase).toBe("PROTECTION_PENDING");
      expect(await rows(s, "GUARDIAN_EARLY_TIMEOUT_PROTECT")).toHaveLength(1);
    } finally { vi.useRealTimers(); await s.engine.stop(); }
  });
  it("depth disappearance after durable admission retries without global breaker or duplicate fill", async () => {
    const s = await start(); let release!: () => void;
    try { await seed(s); await armEarly(s); await timeout(s); s.clock.ms += 5001;
      const errors = s.engine.runtimeBookkeepingForTest().executionErrors;
      const held = book(s, "UP", "0.60", "0.81"), oppositeBook = book(s, "DOWN", "0.29", "0.30");
      internal(s).snapshots.set(MARKET, { UP: held, DOWN: oppositeBook });
      s.engine.state.marketSnapshots.set(UP, held); s.engine.state.marketSnapshots.set(DOWN, oppositeBook);
      s.journal.setAppendInFlightHoldForTest(new Promise<void>(r => release = r), ["PAPER_ORDER_CREATED"]);
      const pending = internal(s).maybeReversalProtection(MARKET);
      await waitFor(s, () => internal(s).riskReductionOwners.size === 1 && s.journal.inFlightAppends > 0);
      const changing = Promise.all([s.market.emitSnapshot(book(s, "UP", null, "0.81", "10000", 2)),
        s.market.emitSnapshot(book(s, "DOWN", "0.29", null, "10000", 2))]);
      release(); s.journal.setAppendInFlightHoldForTest(null); await pending; await changing;
      expect(await riskFills(s)).toHaveLength(0); expect(state(s)?.phase).toBe("PROTECTION_PENDING");
      expect((await rows(s, "PAPER_EXECUTION_ERROR"))[0]?.payload.message).toBe("INSUFFICIENT_DEPTH");
      expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(errors);
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 3);
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { release?.(); s.journal.setAppendInFlightHoldForTest(null); await s.engine.stop(); }
  });
  it.each(["EARLY", "LATE"])("%s arm/deadline survives checkpoint compaction and changed env", async sensor => {
    let s = await start(); try { await seed(s); if (sensor === "EARLY") await armEarly(s); else await armLate(s);
      const before = structuredClone(state(s)); await s.engine.checkpointForTest();
      await s.journal.compactDurableTransitionsThrough(s.engine.lastAppliedDurableCursorForTest());
      s = await restart(s, { REVERSAL_PROTECTION_ENABLED: "false" }); expect(state(s)).toEqual(before);
      expect(internal(s).experiment.riskConfiguration.guardianVersion).toBe(2);
      if (sensor === "EARLY") { await timeout(s); await publish(s); }
      else { s.clock.ms += 30000; await publishSensor(s, ".574"); }
      await waitFor(s, () => residual(s) === "0"); expect(await riskFills(s)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("EARLY lookback and persistence survive restart before arm", async () => {
    let s = await start(); try { await seed(s); await publishSensor(s, ".75"); s.clock.ms += 5000; await publishSensor(s, ".70");
      const before = structuredClone(state(s)); s = await restart(s); expect(state(s)).toEqual(before);
      s.clock.ms += 5000; await publishSensor(s, ".70"); expect(state(s)?.phase).toBe("EARLY_RECOVERY_WATCH");
    } finally { await s.engine.stop(); }
  });
  it("duplicate late event id and old observation cannot rearm after release/restart", async () => {
    let s = await start(); try { await seed(s); await armLate(s);
      // Re-deliver the exact persisted event; same ID with a different recordedAt
      // is an identity conflict, not an idempotent duplicate.
      const original = (await rows(s, "CONSENSUS_DECISION")).find(row => row.id === "late-arm")!;
      await s.engine.persistReportInput(original); const revision = state(s)?.revision;
      await s.engine.persistReportInput(original); expect(state(s)?.revision).toBe(revision);
      s.clock.ms += 30000; await publishSensor(s, ".60", false); expect(state(s)?.phase).toBe("OBSERVING");
      s = await restart(s); await s.engine.persistReportInput(original); expect(state(s)?.phase).toBe("OBSERVING");
      expect(await rows(s, "GUARDIAN_LATE_SEVERITY_ARMED")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
  it("late arm recovers source row after commit-before-apply crash", async () => {
    let s = await start(); try { await seed(s); s.clock.ms = START + 200000; await publishSensor(s, ".60");
      const position = s.engine.ledger.positions.get(MARKET)!;
      // The running V2 experiment uses formatGeneration=3, whose durable key
      // includes the g3 suffix. A legacy key would never trigger this fault.
      const key = `reversal:${position.id}:${position.unhedgedStartedAt ?? position.openedAt}:v2g3:1`;
      s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: key });
      const original = event(s, "crash-late");
      await expect(s.engine.persistReportInput(original)).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s); expect(state(s)?.phase).toBe("LATE_CONFIRMATION");
      expect((await rows(s, "CONSENSUS_DECISION")).filter(r => r.id === "crash-late")).toHaveLength(1);
      await s.engine.persistReportInput(original); expect(state(s)?.revision).toBe(1);
    } finally { await s.engine.stop(); }
  });
  it.each(["admission", "fill"])("crash after %s resumes original order exactly once", async boundary => {
    let s = await start(); try { await seed(s); await armEarly(s); await timeout(s);
      const admit = internal(s).admitRiskReduction.bind(internal(s));
      internal(s).admitRiskReduction = async owner => {
        s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: boundary === "admission"
          ? `risk-reduction:${owner.order.id}:admitted` : `fill:risk-reduction-fill:${owner.order.id}` }); return admit(owner);
      };
      // Direct awaited executor exposes the crash; book callbacks stay locally observed.
      internal(s).snapshots.set(MARKET, { UP: book(s, "UP", "0.60", "0.81"), DOWN: book(s, "DOWN", "0.29", "0.30") });
      for (const snap of Object.values(internal(s).snapshots.get(MARKET)!)) s.engine.state.marketSnapshots.set(snap!.tokenId, snap!);
      s.clock.ms += 5001;
      await expect(internal(s).maybeReversalProtection(MARKET)).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s); expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      const orderId = (await rows(s, "RISK_REDUCTION_ADMITTED"))[0]!.payload.orderId;
      s.clock.ms += 5001; await publish(s, "0.60", "0.30", "10000", 2); await waitFor(s, () => residual(s) === "0");
      expect(await riskFills(s)).toHaveLength(1); expect((await riskFills(s))[0]?.payload.orderId).toBe(orderId);
      s = await restart(s); await publish(s); expect(await riskFills(s)).toHaveLength(1); expect(state(s)).toBeUndefined();
    } finally { await s.engine.stop(); }
  });
  it.each([180000, 360000])("completed legacy %sms manifest replays safely", async window => {
    const journal = new MemoryEventJournal(); const manifest = await legacy360Manifest("COMPLETED");
    manifest.riskConfiguration.reversalProtectionWindowMs = window;
    delete manifest.riskConfiguration.guardianVersion; delete manifest.riskConfiguration.guardianV2Config;
    await journal.commitDurableTransition({ operationKey: `legacy-completed-${window}`, type: "EXPERIMENT_COMPLETED",
      aggregateType: "experiment", aggregateId: manifest.id, payload: { ...manifest, derivedAudit: [] }, occurredAt: new Date(manifest.completedAt!) }, 0);
    const s = await start(journal); try { expect(internal(s).experiment.status).toBe("COMPLETED"); expect(s.engine.integrityFault()).toBe(false); }
    finally { await s.engine.stop(); }
  });
  it("incompatible active legacy V1 manifest fails closed", async () => {
    const journal = new MemoryEventJournal(); const manifest = await legacy360Manifest("RUNNING");
    delete manifest.riskConfiguration.guardianVersion; delete manifest.riskConfiguration.guardianV2Config;
    await journal.commitDurableTransition({ operationKey: "legacy-running", type: "EXPERIMENT_STARTED",
      aggregateType: "experiment", aggregateId: manifest.id, payload: { ...manifest, derivedAudit: [] }, occurredAt: new Date(manifest.startedAt!) }, 0);
    await expect(start(journal)).rejects.toThrow("reversal_protection_config_invalid");
  });
  it("all V2 state/action events are protected canonical report evidence", () => {
    for (const type of ["GUARDIAN_EARLY_DETERIORATION_ARMED", "GUARDIAN_EARLY_TRUE_RECOVERY", "GUARDIAN_EARLY_EMERGENCY_PROTECT",
      "GUARDIAN_EARLY_TIMEOUT_PROTECT", "GUARDIAN_LATE_SEVERITY_ARMED", "GUARDIAN_LATE_CONFIRMATION_RELEASED",
      "GUARDIAN_LATE_CONFIRMATION_ACCEPTED", "GUARDIAN_LATE_EMERGENCY_PROTECT", "GUARDIAN_EXECUTION_COMPLETED"]) {
      expect(CANONICAL_REPORT_INPUT_EVENT_TYPES).toContain(type); expect(AUDIT_CRITICAL_EVENT_TYPES).toContain(type);
    }
  });
});
