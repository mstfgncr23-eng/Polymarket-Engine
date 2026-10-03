import { describe, expect, it } from "vitest";
import { type MarketSnapshot, type PaperOrder, executePaperTaker } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { type RiskReductionOwnership, unhedgedEpisode } from "../apps/engine/src/recovery.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { order, snapshot } from "./fixtures.js";

const START = Date.parse("2026-01-01T00:00:00Z");
type Intent = RiskReductionOwnership["intent"];
type Internals = {
  durableGate: { run<T>(work: () => Promise<T>): Promise<T> };
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  snapshots: Map<string, Partial<Record<"UP" | "DOWN", MarketSnapshot>>>;
  admitRiskReduction(owner: RiskReductionOwnership): Promise<boolean>;
  failRiskReduction(owner: RiskReductionOwnership, reason: string): Promise<void>;
  resumeRiskReduction(owner: RiskReductionOwnership): Promise<void>;
  submitPaperOrder(order: PaperOrder, book: MarketSnapshot, key: string, intent: Intent): Promise<boolean>;
  applyFill(fill: ReturnType<typeof executePaperTaker>, extras: { intent: Intent; riskReductionOwnership: RiskReductionOwnership }): Promise<boolean>;
  maybeEarlyCatastrophicPairLock(market: string): Promise<{ acted: boolean }>;
  maybeProtectiveDeRisk(market: string): Promise<void>;
  commitAuthoritative(input: Record<string, unknown>): Promise<unknown>;
};
const internal = (engine: EngineService) => engine as unknown as Internals;

async function start(journal = new MemoryEventJournal(), clock = { ms: START }) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("owner-mkt", "XRP", "owner-up", "owner-down", new Date(START + 3_600_000).toISOString())]));
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0" }),
  journal, { error: () => undefined }, { market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.ms), resolutionFetcher: async () => null });
  await engine.start();
  await market.waitForHandlers();
  return { engine, journal, market, clock };
}
type Scenario = Awaited<ReturnType<typeof start>>;
function book(s: Scenario, outcome: "UP" | "DOWN") {
  return snapshot({ marketId: "owner-mkt", tokenId: outcome === "UP" ? "owner-up" : "owner-down", outcome,
    asset: "XRP", receivedAt: new Date(s.clock.ms).toISOString(), sourceTimestamp: new Date(s.clock.ms).toISOString(),
    bids: [{ price: outcome === "UP" ? "0.60" : "0.29", quantity: "100" }],
    asks: [{ price: outcome === "UP" ? "0.81" : "0.30", quantity: "100" }] });
}
async function seed(s: Scenario) {
  await s.engine.beginExperiment();
  await s.market.emitSnapshot(book(s, "UP"));
  await s.market.emitSnapshot(book(s, "DOWN"));
  await s.engine.applyObservedFill(paperFill("owner-mkt", "UP", "0.80", {
    tokenId: "owner-up", quantity: "5", timestamp: new Date(START).toISOString(),
  }));
}
function ownership(s: Scenario, id: string, intent: Intent = "RESCUE_SECOND_LEG"): RiskReductionOwnership {
  const sell = intent === "PROTECTIVE_DE_RISK";
  const position = s.engine.ledger.positions.get("owner-mkt")!;
  return { marketId: position.marketId, unhedgedStartedAt: unhedgedEpisode(position), intent,
    order: order("TAKER", { id, marketId: position.marketId, tokenId: sell ? "owner-up" : "owner-down",
      outcome: sell ? "UP" : "DOWN", side: sell ? "SELL" : "BUY", quantity: "5",
      requestedPrice: sell ? "0.60" : "0.30", createdAt: new Date(s.clock.ms).toISOString() }),
    admittedResidualQuantity: position.unpairedQuantity, decisionReceivedAt: new Date(s.clock.ms).toISOString(), state: "ADMITTED" };
}
const owners = (s: Scenario) => internal(s.engine).riskReductionOwners;
const rows = (s: Scenario, type: string) => s.journal.queryEvents({ types: [type] });
async function waitFor(predicate: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("lifecycle did not complete");
    await new Promise((r) => setTimeout(r, 2));
  }
}
async function restart(s: Scenario) {
  await s.engine.stop();
  return start(s.journal, s.clock);
}
async function resume(s: Scenario) {
  await s.market.emitSnapshot(book(s, "UP"));
  await s.market.emitSnapshot(book(s, "DOWN"));
  await waitFor(() => owners(s).size === 0);
}

describe("shared durable risk-reduction taker ownership", () => {
  it("durably admits one episode before order audit and blocks duplicate admission", async () => {
    const s = await start();
    try {
      await seed(s);
      const a = ownership(s, "admission");
      expect(await internal(s.engine).admitRiskReduction(a)).toBe(true);
      expect((await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 10 }))[0]?.payload.ownership).toEqual(a);
      expect(await internal(s.engine).admitRiskReduction(ownership(s, "duplicate"))).toBe(false);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect(owners(s).get(a.marketId)).toEqual(a);
    } finally { await s.engine.stop(); }
  });

  it("serializes concurrent canonical admissions even without the in-memory flight set", async () => {
    const s = await start();
    try {
      await seed(s);
      const result = await Promise.all([internal(s.engine).admitRiskReduction(ownership(s, "parallel-a")),
        internal(s.engine).admitRiskReduction(ownership(s, "parallel-b", "PROTECTIVE_DE_RISK"))]);
      expect(result.filter(Boolean)).toHaveLength(1);
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("queues a detached taker spawned inside a mutation callback instead of inheriting its lock", async () => {
    const s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "detached-callback");
      let pending!: Promise<boolean>;
      await internal(s.engine).durableGate.run(async () => {
        pending = internal(s.engine).submitPaperOrder(owner.order, book(s, "DOWN"), "detached", owner.intent);
        await new Promise((r) => setTimeout(r, 5));
        expect(owners(s).size).toBe(0);
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
      });
      expect(await pending).toBe(true);
      expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("restores pending ownership from a checkpoint after ordinary durable-log compaction", async () => {
    let s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "compacted-pending");
      await internal(s.engine).admitRiskReduction(owner);
      await s.engine.checkpointForTest();
      const cursor = s.engine.lastAppliedDurableCursorForTest();
      await s.journal.compactDurableTransitionsThrough(cursor);
      expect(await s.journal.readDurableTransitionsAfter(0, 100)).toHaveLength(0);
      s = await restart(s);
      expect(owners(s).get(owner.marketId)).toEqual(owner);
      expect(await internal(s.engine).admitRiskReduction(ownership(s, "compacted-duplicate"))).toBe(false);
      await resume(s);
      expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.orderId === owner.order.id)).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });

  it("durably supersedes old episode ownership without inheriting it or its failure", async () => {
    let s = await start();
    try {
      await seed(s);
      const old = ownership(s, "old-episode");
      await internal(s.engine).admitRiskReduction(old);
      // Model a recovered position belonging to a later episode. The lifecycle
      // must release the old identity before admitting the new one.
      s.engine.ledger.positions.get(old.marketId)!.unhedgedStartedAt = new Date(START + 1).toISOString();
      const next = ownership(s, "new-episode");
      expect(await internal(s.engine).admitRiskReduction(next)).toBe(true);
      await internal(s.engine).failRiskReduction(old, "LATE_OLD_FAILURE");
      expect(owners(s).get(old.marketId)?.order.id).toBe(next.order.id);
      expect((await rows(s, "RISK_REDUCTION_FAILED"))[0]?.payload).toMatchObject({ orderId: old.order.id, reason: "EPISODE_SUPERSEDED" });
    } finally { await s.engine.stop(); }
  });

  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)(
    "recovers admission before PAPER_ORDER_CREATED at %s", async (point) => {
      let s = await start();
      try {
        await seed(s);
        const owner = ownership(s, `before-create-${point}`);
        s.engine.setCrashInjectionForTest(point, { operationKey: `risk-reduction:${owner.order.id}:admitted` });
        await expect(internal(s.engine).admitRiskReduction(owner)).rejects.toThrow("CRASH_INJECTED");
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(0);
        s = await restart(s);
        expect(owners(s).get(owner.marketId)).toEqual(owner);
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
        await resume(s);
        expect(await rows(s, "PAPER_FILL")).toHaveLength(2); // first leg + recovered order
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      } finally { await s.engine.stop(); }
    });

  it("recovers a created PR3 order, blocks PR3/protective/normal duplication, and resumes its original id", async () => {
    let s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "created-pr3");
      s.engine.setCrashInjectionForTest("BEFORE_DURABLE_COMMIT", { operationKey: `fill:risk-reduction-fill:${owner.order.id}` });
      await expect(internal(s.engine).submitPaperOrder(owner.order, book(s, "DOWN"), "pr3", owner.intent)).rejects.toThrow("CRASH_INJECTED");
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      s = await restart(s);
      expect(owners(s).get(owner.marketId)?.order.id).toBe(owner.order.id);
      s.clock.ms += 20_000;
      expect((await internal(s.engine).maybeEarlyCatastrophicPairLock(owner.marketId)).acted).toBe(false);
      for (const intent of ["PROTECTIVE_DE_RISK", "RESCUE_SECOND_LEG", "HEDGE_SECOND_LEG"] as const) {
        const duplicate = ownership(s, `duplicate-${intent}`, intent);
        expect(await internal(s.engine).submitPaperOrder(duplicate.order, book(s, duplicate.order.outcome), intent, intent)).toBe(false);
      }
      expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
      await resume(s);
      const executed = (await rows(s, "PAPER_FILL")).filter((r) => r.payload.orderId === owner.order.id);
      expect(executed).toHaveLength(1);
      expect(executed[0]?.payload.quantity).toBe("5");
      expect(Number(s.engine.ledger.positions.get(owner.marketId)?.unpairedQuantity)).toBe(0);
    } finally { await s.engine.stop(); }
  });

  it.each(["AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)(
    "atomically recovers fill/completion at %s", async (point) => {
      let s = await start();
      try {
        await seed(s);
        const owner = ownership(s, `fill-${point}`);
        s.engine.setCrashInjectionForTest(point, { operationKey: `fill:risk-reduction-fill:${owner.order.id}` });
        await expect(internal(s.engine).submitPaperOrder(owner.order, book(s, "DOWN"), "fill", owner.intent)).rejects.toThrow("CRASH_INJECTED");
        s = await restart(s);
        expect(owners(s).size).toBe(0);
        expect(Number(s.engine.ledger.positions.get(owner.marketId)?.unpairedQuantity)).toBe(0);
        expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
        expect((await rows(s, "POSITION_UPDATED")).filter((r) => r.sourceEventId === `risk-reduction-fill:${owner.order.id}`)).toHaveLength(1);
        expect(await internal(s.engine).admitRiskReduction(owner)).toBe(false);
      } finally { await s.engine.stop(); }
    });

  it("recovers failure before memory release and permits a new retry without reviving the failed order", async () => {
    let s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "failed");
      await internal(s.engine).admitRiskReduction(owner);
      s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: `risk-reduction:${owner.order.id}:failed` });
      await expect(internal(s.engine).failRiskReduction(owner, "INSUFFICIENT_DEPTH")).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s);
      expect(owners(s).size).toBe(0);
      expect((await rows(s, "RISK_REDUCTION_FAILED"))[0]?.payload.reason).toBe("INSUFFICIENT_DEPTH");
      expect(await internal(s.engine).admitRiskReduction(owner)).toBe(false);
      expect(await internal(s.engine).admitRiskReduction(ownership(s, "retry"))).toBe(true);
    } finally { await s.engine.stop(); }
  });

  it.each(["HEDGE_SECOND_LEG", "RESCUE_SECOND_LEG", "PROTECTIVE_DE_RISK"] as const)(
    "%s uses the real PAPER lifecycle and remains completed after checkpoint/restart", async (intent) => {
      let s = await start();
      try {
        await seed(s);
        const owner = ownership(s, `real-${intent}`, intent);
        expect(await internal(s.engine).submitPaperOrder(owner.order, book(s, owner.order.outcome), intent, intent)).toBe(true);
        const balance = s.engine.ledger.snapshot();
        expect(await rows(s, "PAPER_ORDER_CREATED")).toHaveLength(1);
        expect((await rows(s, "PAPER_FILL")).filter((r) => r.payload.orderId === owner.order.id)).toHaveLength(1);
        expect(await rows(s, "RISK_REDUCTION_COMPLETED")).toHaveLength(1);
        expect(owners(s).size).toBe(0);
        s = await restart(s);
        expect(owners(s).size).toBe(0);
        expect(s.engine.ledger.snapshot().cash).toBe(balance.cash);
        expect(await internal(s.engine).admitRiskReduction(owner)).toBe(false);
      } finally { await s.engine.stop(); }
    });

  it("does not execute a stale admitted quantity and rejects delayed fills after durable failure", async () => {
    let s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "stale-quantity");
      await internal(s.engine).admitRiskReduction(owner);
      // Model a checkpoint whose recovered ledger includes a correction to
      // the residual. Recovery may not execute the original five-share order.
      s.engine.ledger.applyFill(paperFill(owner.marketId, "UP", "0.60", {
        side: "SELL", quantity: "1", tokenId: "owner-up", timestamp: new Date(START).toISOString(),
      }));
      s = await restart(s);
      expect(owners(s).size).toBe(0); // startup aborts without waiting for a book
      await internal(s.engine).resumeRiskReduction(owner);
      expect(owners(s).size).toBe(0);
      expect((await rows(s, "RISK_REDUCTION_FAILED"))[0]?.payload.reason).toBe("RECOVERED_RESIDUAL_CHANGED");
      const staleFill = executePaperTaker({ order: owner.order, snapshotAfterLatency: book(s, "DOWN"), measuredLatencyMs: 0 });
      await expect(internal(s.engine).applyFill(staleFill, { intent: owner.intent, riskReductionOwnership: owner })).rejects.toThrow("TAKER_AUTHORITY_RELEASED");
      expect(await rows(s, "PAPER_FILL")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("settlement supersedes pending ownership atomically and survives restart", async () => {
    let s = await start();
    try {
      await seed(s);
      const owner = ownership(s, "settled-pending");
      await internal(s.engine).admitRiskReduction(owner);
      // Commit through the same production mutation/replay primitive as
      // settleMarket, then crash before its in-memory/derived aftermath.
      s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: "settlement:owner-mkt:test" });
      await expect(internal(s.engine).commitAuthoritative({ operationKey: "settlement:owner-mkt:test",
        type: "POSITION_SETTLED", aggregateType: "position", aggregateId: "owner-mkt",
        buildPayload: () => ({ marketId: owner.marketId, upPayoff: "1", downPayoff: "0",
          settledAt: new Date(s.clock.ms).toISOString(), derivedAudit: [] }) })).rejects.toThrow("CRASH_INJECTED");
      s = await restart(s);
      expect(owners(s).size).toBe(0);
      expect((await s.journal.readDurableByOperationKey("settlement:owner-mkt:test"))?.type).toBe("POSITION_SETTLED");
      const balance = s.engine.ledger.snapshot().cash;
      await internal(s.engine).resumeRiskReduction(owner);
      expect(s.engine.ledger.snapshot().cash).toBe(balance);
      expect(await rows(s, "PAPER_FILL")).toHaveLength(1);
      expect(await internal(s.engine).admitRiskReduction(owner)).toBe(false);
    } finally { await s.engine.stop(); }
  });
});
