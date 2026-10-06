import { describe, expect, it, vi } from "vitest";
import { feeScheduleSchema, isReportInputEventType, type JournalEvent, type MarketSnapshot, type PaperFill } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import type { RiskReductionOwnership } from "../apps/engine/src/recovery.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START = Date.parse("2026-01-01T00:00:00Z");
const MARKET = "retry-gap";
const fee = feeScheduleSchema.parse({ rate: "0", exponent: 1, takerOnly: true, rebateRate: "0", source: "configured_test_fixture", effectiveAt: new Date(START).toISOString() });
type Outcome = "UP" | "DOWN";
type Attempt = { riskId: string; admitted: boolean; reason: string };
type Internals = {
  snapshots: Map<string, Partial<Record<Outcome, MarketSnapshot>>>;
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  riskReductionInFlight: Set<string>;
  earlyCatastrophicNextAttemptAt: Map<string, number>;
  earlyCatastrophicVetoFingerprint: Map<string, string | null>;
  protectiveDeRiskNextAttemptAt: Map<string, number>;
  maybeProtectiveDeRisk(m: string): Promise<void>;
  maybeEarlyCatastrophicPairLock(m: string): Promise<{ acted: boolean }>;
  evaluateGuardian(m: string): { reason: string };
  applyFill(fill: PaperFill, extras: Record<string, unknown>): Promise<boolean>;
  recordProtectiveDeRiskSuppressed(m: string, gate: string, reason: string): void;
  recordEarlyAttemptTerminated(m: string, order: import("@pm/core").PaperOrder, attempt: Attempt): Promise<void>;
  admitRiskReduction(owner: RiskReductionOwnership, attempt?: Attempt): Promise<boolean>;
};
const internal = (s: Scenario) => s.engine as unknown as Internals;

// Hooks run at exact journal awaits; no sleeps, polling, or production hooks.
class BoundaryJournal extends MemoryEventJournal {
  onAppend?: (event: JournalEvent) => void | Promise<void>;
  onRead?: (key: string) => void | Promise<void>;
  beforeAppend?: (event: JournalEvent) => void | Promise<void>;
  override async append(...args: Parameters<MemoryEventJournal["append"]>) {
    await this.beforeAppend?.(args[0]);
    const result = await super.append(...args);
    await this.onAppend?.(args[0]);
    return result;
  }
  override async readDurableByOperationKey(key: string) {
    await this.onRead?.(key);
    return super.readDurableByOperationKey(key);
  }
}
async function start() {
  const clock = { ms: START };
  const journal = new BoundaryJournal();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified(MARKET, "XRP", `${MARKET}-up`, `${MARKET}-down`, new Date(START + 3_600_000).toISOString())]));
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0" }), journal, { error: () => undefined }, {
    market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: async () => null,
  });
  await engine.start(); await market.waitForHandlers();
  const s = { engine, journal, market, clock };
  await engine.beginExperiment();
  await market.emitSnapshot(book(s, "UP")); await market.emitSnapshot(book(s, "DOWN"));
  await engine.applyObservedFill(paperFill(MARKET, "UP", "0.80", { tokenId: `${MARKET}-up`, quantity: "5", feeUsd: "0", feeSchedule: fee, timestamp: new Date(START).toISOString() }));
  clock.ms = START + 20_000;
  setBook(s, "UP"); setBook(s, "DOWN");
  engine.state.noteReferenceTick("XRP", new Date(clock.ms).toISOString());
  return s;
}
type Scenario = Awaited<ReturnType<typeof start>>;
function book(s: Pick<Scenario, "clock">, outcome: Outcome, overrides: Partial<MarketSnapshot> = {}) {
  return snapshot({ marketId: MARKET, tokenId: `${MARKET}-${outcome.toLowerCase()}`, outcome, asset: "XRP", feeSchedule: fee,
    receivedAt: new Date(s.clock.ms).toISOString(), sourceTimestamp: new Date(s.clock.ms).toISOString(),
    bids: [{ price: outcome === "UP" ? "0.70" : "0.29", quantity: "10000" }],
    asks: [{ price: outcome === "UP" ? "0.81" : "0.30", quantity: "10000" }], ...overrides });
}
function setBook(s: Scenario, outcome: Outcome, overrides: Partial<MarketSnapshot> = {}) {
  const b = book(s, outcome, overrides);
  const pair = internal(s).snapshots.get(MARKET) ?? {};
  pair[outcome] = b; internal(s).snapshots.set(MARKET, pair);
  s.engine.state.marketSnapshots.set(b.tokenId, b); s.market.markTokenSync(b.tokenId, "SYNCED");
}
const rows = (s: Scenario, type: string) => s.journal.queryEvents({ types: [type] });
const evaluations = async (s: Scenario) => (await rows(s, "RISK_EVALUATION")).filter((r) => r.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK");
const terminal = async (s: Scenario) => (await rows(s, "PROTECTIVE_DE_RISK_SUPPRESSED")).filter((r) => r.payload.gate === "EARLY_ACCEPTED_ATTEMPT_TERMINATED");
const residual = (s: Scenario) => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity ?? "0";
function afterFirstAccepted(s: Scenario, work: () => void | Promise<void>) {
  let called = false;
  s.journal.onAppend = async (event) => {
    if (!called && event.type === "RISK_EVALUATION" && event.payload.accepted === true) {
      called = true; await work();
    }
  };
}
async function assertIdle(s: Scenario) {
  expect(internal(s).riskReductionOwners.size).toBe(0);
  expect(internal(s).riskReductionInFlight.size).toBe(0);
  expect(s.engine.integrityFault()).toBe(false);
}

describe("PR3 accepted-without-action retry lifecycle", () => {
  it("serialized admission rejects the first rescue; current PR3 retries in the same pass with fresh facts", async () => {
    const s = await start(); try {
      let firstOrderId = "";
      s.journal.onRead = (key) => {
        if (!firstOrderId && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) {
          firstOrderId = key.slice("risk-reduction:".length, -":admitted".length);
          s.engine.ledger.positions.get(MARKET)!.unpairedQuantity = "6";
        }
      };
      const original = internal(s).evaluateGuardian.bind(internal(s));
      let restored = false;
      vi.spyOn(internal(s), "evaluateGuardian").mockImplementation((m) => {
        if (!restored && firstOrderId) {
          restored = true;
          expect(internal(s).riskReductionInFlight.size).toBe(0);
          expect(internal(s).riskReductionOwners.size).toBe(0);
          s.engine.ledger.positions.get(MARKET)!.unpairedQuantity = "5";
          setBook(s, "DOWN", { asks: [{ price: "0.29", quantity: "10000" }] });
        }
        return original(m);
      });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(s.clock.ms).toBe(START + 20_000);
      expect(await evaluations(s)).toHaveLength(2);
      const admitted = await rows(s, "RISK_REDUCTION_ADMITTED");
      expect(admitted).toHaveLength(1); expect(admitted[0]!.payload.orderId).not.toBe(firstOrderId);
      const orders = await rows(s, "PAPER_ORDER_CREATED");
      expect(orders).toHaveLength(1); expect(orders[0]!.aggregateId).not.toBe(firstOrderId);
      expect((await rows(s, "PAPER_FILL"))[1]!.payload).toMatchObject({ intent: "RESCUE_SECOND_LEG", fillPrice: "0.29", quantity: "5" });
      expect(await terminal(s)).toHaveLength(1);
      expect((await terminal(s))[0]!.payload.reason).toBe("ADMISSION_RESIDUAL_CHANGED");
      expect(residual(s)).toBe("0"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      await assertIdle(s);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it.each(["pre-submit", "serialized"] as const)("%s loss-cap rejection yields immediately to Guardian exactly once under concurrent callbacks", async (boundary) => {
    const s = await start(); try {
      const change = async () => {
        setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] });
        await Promise.all(Array.from({ length: 6 }, () => internal(s).maybeProtectiveDeRisk(MARKET)));
        expect(internal(s).riskReductionInFlight.has(MARKET)).toBe(true);
        expect(internal(s).riskReductionOwners.size).toBe(0);
      };
      if (boundary === "pre-submit") afterFirstAccepted(s, change);
      else {
        let changed = false;
        s.journal.onRead = (key) => {
          if (!changed && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) {
            changed = true; setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] });
          }
        };
      }
      await internal(s).maybeProtectiveDeRisk(MARKET);
      await Promise.all(Array.from({ length: 6 }, () => internal(s).maybeProtectiveDeRisk(MARKET)));
      expect(s.clock.ms).toBe(START + 20_000);
      expect((await rows(s, "GUARDIAN_EVALUATION"))[0]!.payload.reason).toBe("PR3_LOCKED_LOSS_EXCEEDS_CAP");
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]!.payload.intent).toBe("GUARDIAN_PROTECTIVE_SELL");
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "GUARDIAN_PROTECTIVE_SELL")).toHaveLength(1);
      expect(await terminal(s)).toHaveLength(1); expect(residual(s)).toBe("0");
      await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it.each(["pre-submit", "serialized"] as const)("%s mark improves after acceptance: waits without rescue or SELL; a later normal hedge remains possible", async (boundary) => {
    const s = await start(); try {
      const improve = () => setBook(s, "UP", { bids: [{ price: "0.79", quantity: "10000" }] });
      if (boundary === "pre-submit") afterFirstAccepted(s, improve);
      else {
        let changed = false;
        s.journal.onRead = (key) => {
          if (!changed && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) { changed = true; improve(); }
        };
      }
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
      expect((await terminal(s))[0]!.payload.reason).toBe(boundary === "pre-submit" ? "FINAL_FENCE_MARK_LOSS_NOT_REACHED" : "MARK_LOSS_NOT_REACHED");
      expect(residual(s)).toBe("5"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      expect(internal(s).evaluateGuardian(MARKET).reason).toBe("MARK_LOSS_NOT_REACHED");
      // Current PR3 gate wakes normal hedge authority on the next loss event.
      setBook(s, "UP"); setBook(s, "DOWN", { asks: [{ price: "0.10", quantity: "10000" }] });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]!.payload.intent).toBe("HEDGE_SECOND_LEG");
      expect(residual(s)).toBe("0"); await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it.each(["UP", "DOWN"] as const)("%s book gaps after acceptance: fails closed until resync", async (outcome) => {
    const s = await start(); try {
      afterFirstAccepted(s, () => { s.market.markTokenSync(`${MARKET}-${outcome.toLowerCase()}`, "GAPPED"); });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
      expect((await terminal(s))[0]!.payload.reason).toBe("FINAL_FENCE_BOOK_UNAVAILABLE_OR_FUTURE_STALE");
      expect(residual(s)).toBe("5"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      setBook(s, "UP"); setBook(s, "DOWN");
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(residual(s)).toBe("0"); await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it("successful durable rescue installs action pacing, blocks duplicates, then clears pacing on completion", async () => {
    const s = await start(); try {
      const original = internal(s).applyFill.bind(internal(s));
      let checked = false;
      vi.spyOn(internal(s), "applyFill").mockImplementation(async (fill, extras) => {
        if (extras.intent === "RESCUE_SECOND_LEG") {
          checked = true;
          expect(internal(s).earlyCatastrophicNextAttemptAt.get(MARKET)).toBe(s.clock.ms + 5_000);
          expect(internal(s).earlyCatastrophicVetoFingerprint.has(MARKET)).toBe(false);
          expect(internal(s).riskReductionOwners.size).toBe(1);
          await Promise.all(Array.from({ length: 6 }, () => internal(s).maybeProtectiveDeRisk(MARKET)));
        }
        return original(fill, extras);
      });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(checked).toBe(true); expect(await evaluations(s)).toHaveLength(1);
      expect((await evaluations(s))[0]!.payload.deRisk).toMatchObject({ finalState: "PRECHECK_ACCEPTED", residualQuantity: "5", residualExposure: "4" });
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]!.payload.deRisk.finalState).toBe("ORDER_ADMITTED");
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(await terminal(s)).toHaveLength(0);
      expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1); await assertIdle(s);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it("stable risk veto retains 30s fingerprint pacing and retires it when current state becomes admissible", async () => {
    const s = await start(); try {
      s.engine.state.draining = true;
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(internal(s).earlyCatastrophicNextAttemptAt.get(MARKET)).toBe(s.clock.ms + 30_000);
      expect(internal(s).earlyCatastrophicVetoFingerprint.get(MARKET)).toBeTruthy();
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await evaluations(s)).toHaveLength(1); expect(await terminal(s)).toHaveLength(0);
      s.engine.state.draining = false;
      setBook(s, "DOWN", { asks: [{ price: "0.29", quantity: "10000" }] });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(residual(s)).toBe("0"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      expect(internal(s).earlyCatastrophicVetoFingerprint.has(MARKET)).toBe(false); await assertIdle(s);
    } finally { s.engine.state.draining = false; await s.engine.stop(); }
  });

  it("admitted execution failure retains the 5s action slot; immediate Guardian completion clears it", async () => {
    const s = await start(); try {
      s.journal.onAppend = (event) => {
        if (event.type === "PAPER_ORDER_CREATED" && event.payload.intent === "RESCUE_SECOND_LEG") {
          setBook(s, "DOWN", { asks: [{ price: "0.30", quantity: "2" }] });
        }
      };
      await internal(s).maybeEarlyCatastrophicPairLock(MARKET);
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1);
      expect(await rows(s, "RISK_REDUCTION_FAILED")).toHaveLength(1);
      expect(internal(s).earlyCatastrophicNextAttemptAt.get(MARKET)).toBe(s.clock.ms + 5_000);
      expect(await terminal(s)).toHaveLength(0); expect(residual(s)).toBe("5"); await assertIdle(s);
      setBook(s, "DOWN");
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect((await rows(s, "PAPER_ORDER_CREATED")).map((r) => r.payload.intent)).toEqual(["RESCUE_SECOND_LEG", "GUARDIAN_PROTECTIVE_SELL"]);
      expect((await rows(s, "GUARDIAN_EVALUATION"))[0]!.payload.reason).toBe("SAME_EPISODE_RESCUE_EXECUTION_FAILED");
      expect(residual(s)).toBe("0"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.intent === "GUARDIAN_PROTECTIVE_SELL")).toHaveLength(1);
      await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it("deadline crossing after acceptance rejects PR3 and hands authority to PR82 in the same pass", async () => {
    const s = await start(); try {
      afterFirstAccepted(s, () => {
        s.clock.ms = START + s.engine.riskLimits.maxUnhedgedDurationMs;
        setBook(s, "UP"); setBook(s, "DOWN");
      });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect((await terminal(s))[0]!.payload.reason).toBe("FINAL_FENCE_DEADLINE_OWNERSHIP");
      const orders = await rows(s, "PAPER_ORDER_CREATED"); expect(orders).toHaveLength(1);
      expect(orders[0]!.payload.deRisk.deRiskReason).not.toBe("EARLY_CATASTROPHIC_PAIR_LOCK");
      expect(orders[0]!.payload.intent).not.toBe("GUARDIAN_PROTECTIVE_SELL");
      expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      expect(residual(s)).toBe("0"); await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it("normal hedge priority terminates the accepted rescue without claiming rescue admission", async () => {
    const s = await start(); try {
      afterFirstAccepted(s, () => setBook(s, "DOWN", { asks: [{ price: "0.10", quantity: "10000" }] }));
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect((await evaluations(s))[0]!.payload.deRisk.finalState).toBe("PRECHECK_ACCEPTED");
      expect(await terminal(s)).toHaveLength(1);
      expect((await terminal(s))[0]!.payload.reason).toBe("NORMAL_HEDGE_PRIORITY");
      const admitted = await rows(s, "RISK_REDUCTION_ADMITTED"); expect(admitted).toHaveLength(1);
      expect(admitted[0]!.payload.intent).toBe("HEDGE_SECOND_LEG");
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect(residual(s)).toBe("0"); expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      await assertIdle(s);
    } finally { await s.engine.stop(); }
  });

  it("terminal reason is queryable once per acceptance even when generic suppression was recently emitted", async () => {
    const s = await start(); try {
      internal(s).recordProtectiveDeRiskSuppressed(MARKET, "EARLY_CATASTROPHIC_PAIR_LOCK", "UNHEDGED_AGE_NOT_REACHED");
      afterFirstAccepted(s, () => setBook(s, "UP", { bids: [{ price: "0.79", quantity: "10000" }] }));
      await internal(s).maybeProtectiveDeRisk(MARKET);
      const accepted = (await evaluations(s))[0]!;
      expect(accepted.payload.deRisk.finalState).toBe("PRECHECK_ACCEPTED");
      const t = await terminal(s); expect(t).toHaveLength(1);
      expect(t[0]!.payload).toMatchObject({ riskEvaluationId: accepted.aggregateId, reason: "FINAL_FENCE_MARK_LOSS_NOT_REACHED", precheckAccepted: true, durableAdmission: false, residualQuantity: "5" });
      expect(isReportInputEventType(t[0]!.type)).toBe(false);
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await terminal(s)).toHaveLength(1); expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });

  it("same-pass retry is bounded: two rejected acceptances do not spin or retain a retry slot", async () => {
    const s = await start(); try {
      s.journal.onAppend = (event) => {
        if (event.type === "RISK_EVALUATION" && event.payload.accepted) s.market.markTokenSync(`${MARKET}-down`, "GAPPED");
      };
      const guardian = internal(s).evaluateGuardian.bind(internal(s));
      vi.spyOn(internal(s), "evaluateGuardian").mockImplementation((m) => {
        // Resync when economic authority resumes, independent of diagnostics.
        setBook(s, "UP"); setBook(s, "DOWN"); return guardian(m);
      });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await evaluations(s)).toHaveLength(2); expect(await terminal(s)).toHaveLength(2);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
      expect(internal(s).earlyCatastrophicNextAttemptAt.has(MARKET)).toBe(false);
      expect(residual(s)).toBe("5"); await assertIdle(s);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it("a second same-pass PR3 rejection still hands the changed residual to current Guardian authority", async () => {
    const s = await start(); try {
      let accepts = 0;
      s.journal.onAppend = (event) => {
        if (event.type === "RISK_EVALUATION" && event.payload.accepted) {
          accepts++;
          if (accepts === 1) s.market.markTokenSync(`${MARKET}-down`, "GAPPED");
          else setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] });
        }
      };
      const guardian = internal(s).evaluateGuardian.bind(internal(s));
      vi.spyOn(internal(s), "evaluateGuardian").mockImplementation((m) => {
        if (accepts === 1) { setBook(s, "UP"); setBook(s, "DOWN"); }
        return guardian(m);
      });
      await internal(s).maybeProtectiveDeRisk(MARKET);
      expect(await evaluations(s)).toHaveLength(2); expect(await terminal(s)).toHaveLength(2);
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]!.payload.intent).toBe("GUARDIAN_PROTECTIVE_SELL");
      expect(await rows(s, "RISK_REDUCTION_ADMITTED")).toHaveLength(1); expect(residual(s)).toBe("0"); await assertIdle(s);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });
});

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}
function captureTerminalTask(s: Scenario) {
  const original = internal(s).recordEarlyAttemptTerminated.bind(internal(s));
  const tasks: Promise<void>[] = [];
  vi.spyOn(internal(s), "recordEarlyAttemptTerminated").mockImplementation((...args) => {
    const task = original(...args); tasks.push(task); return task;
  });
  return tasks;
}

describe("PR3 terminal observability never owns economic handoff", () => {
  it.each(["read", "append"] as const)("Guardian completes before a held terminal diagnostic %s is released", async (phase) => {
    const s = await start();
    const entered = deferred(), release = deferred();
    const tasks = captureTerminalTask(s);
    let handoff: Promise<void> | undefined;
    try {
      vi.useFakeTimers({ toFake: ["setTimeout"] });
      afterFirstAccepted(s, () => setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] }));
      if (phase === "read") {
        let held = false;
        s.journal.onRead = async (key) => {
          if (!held && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) {
            held = true; entered.resolve(); await release.promise;
          }
        };
      } else s.journal.beforeAppend = async (event) => {
        if (event.payload.gate === "EARLY_ACCEPTED_ATTEMPT_TERMINATED") { entered.resolve(); await release.promise; }
      };
      handoff = internal(s).maybeProtectiveDeRisk(MARKET);
      await entered.promise;
      await vi.runAllTimersAsync();
      // These assertions run while diagnostic persistence is still held. The
      // previous implementation cannot admit or fill Guardian at this point.
      expect(residual(s)).toBe("0");
      expect((await rows(s, "PAPER_ORDER_CREATED"))[0]!.payload.intent).toBe("GUARDIAN_PROTECTIVE_SELL");
      expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
      expect(await terminal(s)).toHaveLength(0);
      await handoff; await assertIdle(s);
      await Promise.all(Array.from({ length: 6 }, () => internal(s).maybeProtectiveDeRisk(MARKET)));
      release.resolve(); await Promise.all(tasks);
      expect(await terminal(s)).toHaveLength(1);
      expect((await terminal(s))[0]!.payload).toMatchObject({ residualQuantity: "5", reason: "FINAL_FENCE_LOCKED_LOSS_EXCEEDS_CAP", durableAdmission: false });
      expect(tasks).toHaveLength(1);
    } finally {
      release.resolve(); await vi.runAllTimersAsync(); await handoff; await Promise.all(tasks);
      vi.useRealTimers(); vi.restoreAllMocks(); await s.engine.stop();
    }
  });

  it.each(["read", "append"] as const)("stop does not wait for a held diagnostic %s or append after journal closure", async (phase) => {
    const s = await start();
    const entered = deferred(), release = deferred();
    const tasks = captureTerminalTask(s);
    let handoff: Promise<void> | undefined;
    try {
      vi.useFakeTimers({ toFake: ["setTimeout"] });
      afterFirstAccepted(s, () => setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] }));
      if (phase === "read") {
        let held = false;
        s.journal.onRead = async (key) => {
          if (!held && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) {
            held = true; entered.resolve(); await release.promise;
          }
        };
      } else s.journal.beforeAppend = async (event) => {
        if (event.payload.gate === "EARLY_ACCEPTED_ATTEMPT_TERMINATED") { entered.resolve(); await release.promise; }
      };
      const closed = vi.spyOn(s.journal, "close");
      handoff = internal(s).maybeProtectiveDeRisk(MARKET);
      await entered.promise; await vi.runAllTimersAsync();
      expect(residual(s)).toBe("0"); await handoff;
      await s.engine.stop();
      expect(closed).toHaveBeenCalledOnce();
      release.resolve(); await Promise.all(tasks);
      expect(await terminal(s)).toHaveLength(0);
      expect(s.engine.integrityFault()).toBe(false);
    } finally {
      release.resolve(); await vi.runAllTimersAsync(); await handoff; await Promise.all(tasks);
      vi.useRealTimers(); vi.restoreAllMocks(); await s.engine.stop();
    }
  });

  it.each(["read", "append", "unexpected"] as const)("detached diagnostic %s rejection is handled without changing economic health", async (phase) => {
    const s = await start();
    const unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);
    try {
      const tasks = captureTerminalTask(s);
      const logger = vi.spyOn((s.engine as unknown as { logger: { error(...args: unknown[]): void } }).logger, "error");
      afterFirstAccepted(s, () => setBook(s, "DOWN", { asks: [{ price: "0.40", quantity: "10000" }] }));
      if (phase === "read") {
        let failed = false;
        s.journal.onRead = (key) => {
          if (!failed && key.startsWith("risk-reduction:") && key.endsWith(":admitted")) { failed = true; throw new Error("DIAGNOSTIC_READ_REJECTED"); }
        };
      } else if (phase === "append") s.journal.beforeAppend = (event) => {
        if (event.payload.gate === "EARLY_ACCEPTED_ATTEMPT_TERMINATED") throw new Error("DIAGNOSTIC_APPEND_REJECTED");
      };
      else vi.spyOn(internal(s), "recordEarlyAttemptTerminated").mockRejectedValue(new Error("UNEXPECTED_DIAGNOSTIC_REJECTION"));
      await internal(s).maybeProtectiveDeRisk(MARKET); await Promise.all(tasks);
      expect(residual(s)).toBe("0"); expect(await terminal(s)).toHaveLength(0);
      expect(logger).toHaveBeenCalled(); expect(unhandled).not.toHaveBeenCalled();
      expect(s.engine.state.database).toBe("UP"); expect(s.engine.state.journalDegraded).toBe(false); await assertIdle(s);
    } finally { process.off("unhandledRejection", unhandled); vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)("%s never becomes a false non-admission terminal fact", async (point) => {
    const s = await start();
    try {
      const tasks = captureTerminalTask(s);
      const original = internal(s).admitRiskReduction.bind(internal(s));
      vi.spyOn(internal(s), "admitRiskReduction").mockImplementation((owner, attempt) => {
        s.engine.setCrashInjectionForTest(point, { operationKey: `risk-reduction:${owner.order.id}:admitted` });
        return original(owner, attempt);
      });
      await expect(internal(s).maybeEarlyCatastrophicPairLock(MARKET)).rejects.toThrow("CRASH_INJECTED");
      await Promise.all(tasks);
      expect(tasks).toHaveLength(1);
      expect(await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 10 })).toHaveLength(1);
      expect(await terminal(s)).toHaveLength(0);
      expect((await evaluations(s))[0]!.payload.deRisk.finalState).toBe("PRECHECK_ACCEPTED");
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });
});
