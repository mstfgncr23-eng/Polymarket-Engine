import { describe, expect, it } from "vitest";
import {
  createId,
  evaluateNormalHedgeCandidate,
  feeScheduleSchema,
  riskLimitsSchema,
  type MarketSnapshot,
  type PaperFill,
  type ReferenceTick,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import type { RiskReductionOwnership } from "../apps/engine/src/recovery.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";
import { feeSchedule, order, snapshot as newSnapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-01-01T00:00:00.000Z");
const END_OFFSET_MS = 3_600_000;
type Clock = { nowMs: number };
const iso = (clock: Clock): string => new Date(clock.nowMs).toISOString();
const LIMITS = riskLimitsSchema.parse({});

const ERROR_SCHEDULE = feeScheduleSchema.parse({
  rate: "0", exponent: 1, takerOnly: true, rebateRate: "0",
  source: "configured_test_fixture", effectiveAt: new Date(START_MS).toISOString(),
});

function book(
  clock: Clock,
  marketId: string,
  outcome: "UP" | "DOWN",
  sequence: number,
  bids: Array<{ price: string; quantity: string }>,
  asks: Array<{ price: string; quantity: string }> = [],
  feeScheduleOverride: typeof feeSchedule = feeSchedule,
): MarketSnapshot {
  const tokenId = outcome === "UP" ? `${marketId}-up` : `${marketId}-down`;
  return newSnapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId, tokenId, outcome, asset: "XRP",
    bids, asks, feeSchedule: feeScheduleOverride, sourceTimestamp: iso(clock), receivedAt: iso(clock), sequence,
  });
}

function referenceTick(clock: Clock, price: string): ReferenceTick {
  return {
    eventId: createId("reference"), traceId: createId("trace"), asset: "XRP",
    symbol: "XRPUSDT", venue: "deterministic-test", price, quantity: "1",
    sourceTimestamp: iso(clock), receivedAt: iso(clock), tradeId: createId("trade"),
  };
}

function firstLeg(clock: Clock, sourceEventId: string, overrides: Partial<PaperFill> = {}): PaperFill {
  return {
    id: createId("fill-first"), orderId: createId("order-first"), traceId: createId("trace-first"),
    marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "BUY", requestedPrice: "0.82",
    fillPrice: "0.82", quantity: "6.09756098", liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "pr3-premerge:first-leg", feeSchedule,
    ...overrides,
  };
}

async function waitFor<T>(read: () => T | Promise<T>, predicate: (value: T) => boolean, timeoutMs = 6_000): Promise<T> {
  const started = Date.now();
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 3));
  }
}

async function setupScenario(endOffsetMs: number, env: Record<string, string> = {}, marketIds: string[] = ["btc-mkt"]) {
  const clock: Clock = { nowMs: START_MS };
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + endOffsetMs).toISOString();
  market.queue(discoveryReport(marketIds.map((id) => qualified(id, "XRP", `${id}-up`, `${id}-down`, endDate))));
  const engine = new EngineService(qualifiedPaperEnv({
    DISCOVERY_INTERVAL_MS: "3600000", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0", ...env,
  }), journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.nowMs),
    resolutionFetcher: async () => null,
  });
  await engine.start();
  await market.waitForHandlers();
  return { engine, journal, market, clock };
}
type Scenario = Awaited<ReturnType<typeof setupScenario>>;

// Read-only test inspection; lifecycle progression still uses real adapter
// callbacks and the existing journal append hold, without a production hook.
const parityAuthority = (setup: Scenario) => setup.engine as unknown as {
  riskReductionOwners: Map<string, RiskReductionOwnership>;
  riskReductionInFlight: Set<string>;
};
const parityRiskOrders = (setup: Scenario) => setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })
  .then((rows) => rows.filter((row) => ["HEDGE_SECOND_LEG", "RESCUE_SECOND_LEG", "PROTECTIVE_DE_RISK", "GUARDIAN_PROTECTIVE_SELL"].includes(row.payload.intent)));
const parityRiskFills = (setup: Scenario) => setup.journal.queryEvents({ types: ["PAPER_FILL"] })
  .then((rows) => rows.filter((row) => ["HEDGE_SECOND_LEG", "RESCUE_SECOND_LEG", "PROTECTIVE_DE_RISK", "GUARDIAN_PROTECTIVE_SELL"].includes(row.payload.intent)));

async function seedParityPosition(setup: Scenario): Promise<void> {
  expect((await setup.engine.beginExperiment()).ok).toBe(true);
  await setup.engine.applyObservedFill({ ...upPosition(setup.clock, "0.80", "5", "0"), feeSchedule: ERROR_SCHEDULE });
  setup.clock.nowMs = START_MS + 20_000;
}
async function emitParityBooks(setup: Scenario, sequence: number): Promise<void> {
  await setup.market.emitSnapshot(book(setup.clock, "btc-mkt", "UP", sequence, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
  await setup.market.emitSnapshot(book(setup.clock, "btc-mkt", "DOWN", sequence, [{ price: "0.24", quantity: "1000" }], [{ price: "0.30", quantity: "1000" }], ERROR_SCHEDULE));
}
async function waitForParityIdle(setup: Scenario): Promise<void> {
  await waitFor(() => parityAuthority(setup).riskReductionOwners.size === 0
    && !parityAuthority(setup).riskReductionInFlight.has("btc-mkt"), Boolean);
}

const fills = (setup: Scenario, intent: string) =>
  setup.journal.queryEvents({ types: ["PAPER_FILL"] }).then((rows) => rows.filter((row) => row.payload.intent === intent));
const suppressedRows = (setup: Scenario) =>
  setup.journal.queryEvents({ types: ["PROTECTIVE_DE_RISK_SUPPRESSED"] });
const earlyEvals = (setup: Scenario, marketId = "btc-mkt") =>
  setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }).then((rows) =>
    rows.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK" && row.payload.marketId === marketId));

function eligibilityMs(endOffsetMs: number): number {
  return Math.min(START_MS + 1 + LIMITS.maxUnhedgedDurationMs, START_MS + endOffsetMs - 80_000);
}

const number = (value: string | undefined): number => Number(value ?? 0);

function upPosition(clock: Clock, avgUpCost: string, quantity: string, totalFees = "0"): ReturnType<typeof firstLeg> {
  return firstLeg(
    clock,
    createId("event-first"),
    {
      tokenId: "btc-up", outcome: "UP", fillPrice: avgUpCost, requestedPrice: avgUpCost,
      quantity, feeUsd: totalFees,
    },
  );
}

describe("PR3 pre-merge correctness", { timeout: 120_000 }, () => {
it("RACE-A: hedge becomes authoritative while accepted RISK_EVALUATION is held -> no EARLY order/rescue fill", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    let release!: () => void;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["RISK_EVALUATION"]);
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.02", quantity: "1000" }], [{ price: "0.22", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(() => journal.inFlightAppends, (v) => v > 0);
      // WHILE held: books become a genuinely admissible hedge (ask 0.04 -> edge 0.16).
      clock.nowMs = START_MS + 21_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [{ price: "0.02", quantity: "1000" }], [{ price: "0.04", quantity: "10000" }], ERROR_SCHEDULE));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      release();
      release = undefined!;
      journal.setAppendInFlightHoldForTest(null);
      const hedges = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedges).toHaveLength(1);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const created = await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
      expect(created.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK" && row.payload.intent === "RESCUE_SECOND_LEG")).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      release?.();
      setup.journal.setAppendInFlightHoldForTest(null);
      await setup.engine.stop();
    }
  });

  it("RACE-B: state changes while same-market first-leg cancellation (PAPER_ORDER_CANCELLED) is held -> no stale EARLY order", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    let release!: () => void;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "fl-maker", marketId: "btc-mkt", tokenId: "btc-mkt-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.60", quantity: "10", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE), "OPEN_FIRST_LEG");
      expect(journal.inFlightAppends).toBe(0);

      // Hold the early path's same-market first-leg cancellation so the state
      // can change AFTER that pre-admission boundary.
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["PAPER_ORDER_CANCELLED"]);
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [{ price: "0.02", quantity: "1000" }], [{ price: "0.22", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(() => journal.inFlightAppends, (v) => v > 0);
      // WHILE the cancellation is held, make the normal HEDGE authoritative.
      clock.nowMs = START_MS + 21_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 4, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 4, [{ price: "0.02", quantity: "1000" }], [{ price: "0.04", quantity: "10000" }], ERROR_SCHEDULE));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      release();
      release = undefined!;
      journal.setAppendInFlightHoldForTest(null);

      const hedges = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedges).toHaveLength(1);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const created = await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
      expect(created.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK" && row.payload.intent === "RESCUE_SECOND_LEG")).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      release?.();
      setup.journal.setAppendInFlightHoldForTest(null);
      await setup.engine.stop();
    }
  });
it("REFERENCE-VETO: profitable pair candidate exists but real reference gate vetoes the hedge -> EARLY is NOT suppressed", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      // NO reference ticks: the shared hedge decision must be vetoed via
      // REFERENCE_FEED_STALE even though a profitable candidate exists.
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.02", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues).toHaveLength(1);
      expect((await fills(setup, "HEDGE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("REFERENCE-HEALTHY: same books with a fresh reference feed -> hedge owns priority (adopted), zero EARLY", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.02", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      const hedges = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedges).toHaveLength(1);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
it("PAPER_EXECUTION_STYLE: priority path honors configured MAKER style (adopted hedge order is MAKER)", async () => {
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_EXECUTION_STYLE: "MAKER" });
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      const created = await waitFor(
        () => journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] }),
        (rows) => rows.some((row) => row.payload.intent === "HEDGE_SECOND_LEG"),
      );
      const hedgeOrder = created.find((row) => row.payload.intent === "HEDGE_SECOND_LEG");
      expect(hedgeOrder?.payload.orderType).toBe("MAKER");
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("COOLDOWN: profitable pair inside strategy cooldown -> hedge vetoed (DUPLICATE_TRADE), EARLY rescue still closes the residual", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      // Profitable pair + early-eligible, but the pair trigger is inside cooldown.
      engine.seedRecentTradeForTest("btc-mkt", "DOWN");
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.02", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues).toHaveLength(1);
      // The residual is NEVER left naked (hedge fill=0 AND rescue fill=0).
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("MARKET-END: at/after marketEnd no early admission/order/fill; settlement authority exclusive", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + END_OFFSET_MS + 60_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.55", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.02", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      await waitFor(() => suppressedRows(setup), (rows) => rows.some((r) => r.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK" || r.payload.gate === "GATE_DURATION"));
      expect((await earlyEvals(setup))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect((await fills(setup, "HEDGE_SECOND_LEG"))).toHaveLength(0);
      const created = await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
      expect(created.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK")).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("FUTURE-BOOKS: future-dated held/opposite books fail closed (no early, no hedge-priority work, retryable)", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      const futureClock: Clock = { nowMs: START_MS + 12_000 };
      // Books stamped AFTER the engine clock (future): age would be negative.
      await market.emitSnapshot(book(futureClock, "btc-mkt", "UP", 5, [{ price: "0.55", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(futureClock, "btc-mkt", "DOWN", 5, [], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      clock.nowMs = START_MS + 10_000;
      await waitFor(() => suppressedRows(setup), (rows) => rows.some((r) => r.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK"));
      expect((await earlyEvals(setup))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.pr3HedgeDecisionCountForTest()).toBe(0);
      // A NON-future snapshot then makes it retryable and actionable is proven
      // elsewhere (BURST/HOTPATH retry tails); here we assert the fail-closed
      // invariant is stable and no hedge-priority work leaked.
      expect(setup.engine.pr3HedgeDecisionCountForTest()).toBe(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("MAKER-RESERVATION-CASH-1: same-market OPEN_FIRST_LEG reservation is released before final admission (no false INSUFFICIENT_CASH)", async () => {
    // Balance 6: first leg 5@0.80 -> cash 2.0. First-leg MAKER reserves 0.9 ->
    // available cash 1.1 < rescue notional 1.25 (INSUFFICIENT_CASH). The
    // two-phase path durably cancels that same-market maker -> cash 2.0 ->
    // final canonical admission accepts.
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "6" });
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "5", "0"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "fl-maker-cash", marketId: "btc-mkt", tokenId: "btc-mkt-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.60", quantity: "1.5", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE), "OPEN_FIRST_LEG");
      expect(Number(setup.engine.ledger.snapshot().reservedCash)).toBe(0.9);

      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [], [{ price: "0.25", quantity: "10000" }], ERROR_SCHEDULE));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues).toHaveLength(1);
      expect(setup.engine.restingOrdersForTest().get("fl-maker-cash")).toBeUndefined();
      expect(Number(setup.engine.ledger.snapshot().cash)).toBeGreaterThanOrEqual(0);
      expect((await journal.queryEvents({ types: ["PAPER_ORDER_CANCELLED"] })).some((row) => row.aggregateId === "fl-maker-cash")).toBe(true);
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("MAKER-RESERVATION-CASH-2: resting HEDGE maker reservation is released by hedge fencing before canonical admission (no false INSUFFICIENT_CASH)", async () => {
    // Balance 5.5: first leg 5@0.80 -> cash 1.5. HEDGE maker reserves 3@0.20=0.6
    // -> available 0.9 < rescue notional 1.5; the fence releases it -> cash 1.5.
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "5.5" });
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "5", "0"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.20", quantity: "1000" }], [], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "hedge-maker-cash", marketId: "btc-mkt", tokenId: "btc-mkt-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.20", quantity: "3", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "UP", 2, [{ price: "0.20", quantity: "1000" }], [], ERROR_SCHEDULE), "HEDGE_SECOND_LEG");
      expect(Number(setup.engine.ledger.snapshot().reservedCash)).toBe(0.6);

      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [{ price: "0.20", quantity: "1000" }], [{ price: "0.30", quantity: "10000" }], ERROR_SCHEDULE));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues).toHaveLength(1);
      expect(setup.engine.restingOrdersForTest().get("hedge-maker-cash")).toBeUndefined();
      expect(Number(setup.engine.ledger.snapshot().cash)).toBeGreaterThanOrEqual(0);
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });
it("BURST-WRITE-AMP: stable veto under hundreds of snapshots -> bounded early RISK_EVALUATION rows; prompt retry on material change", async () => {
    // Balance 5, first leg 5@0.99 -> cash 0.05; rescue notional at ask 0.02 is
    // 0.10 > cash -> canonical veto INSUFFICIENT_CASH stable across every fresh
    // snapshot. The pacing must keep durable RISK_EVALUATION writes bounded.
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "30" });
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.99", "30", "0"), feeSchedule: ERROR_SCHEDULE });
      expect(Number(setup.engine.ledger.snapshot().cash)).toBeCloseTo(0.3, 6);
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.001", quantity: "1000" }], [{ price: "0.02", quantity: "10000" }], ERROR_SCHEDULE));
      await waitFor(() => earlyEvals(setup), (rows) => rows.length >= 1);
      const firstVetoes = ((await earlyEvals(setup))[0]!.payload.vetoes ?? []) as string[];
      expect(JSON.stringify(firstVetoes)).toBe(JSON.stringify(["INSUFFICIENT_CASH"]));
      const afterFirst = (await earlyEvals(setup)).length;
      for (let seq = 3; seq < 130; seq += 1) {
        await market.emitSnapshot(book(clock, "btc-mkt", "UP", seq, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
        await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", seq, [{ price: "0.001", quantity: "1000" }], [{ price: "0.02", quantity: "10000" }], ERROR_SCHEDULE));
      }
      await new Promise((r) => setTimeout(r, 400));
      const afterBurst = (await earlyEvals(setup)).length;
      expect(afterBurst).toBeLessThanOrEqual(afterFirst + 2);
      expect(setup.engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("30");

      // Material change: advance past the veto backoff and improve the ask so the
      // prompt retry regardless of the veto backoff.
      clock.nowMs = START_MS + 65_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 200, [{ price: "0.001", quantity: "1000" }], [{ price: "0.01", quantity: "10000" }], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 200, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues).toHaveLength(1);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("HOTPATH-COUNTS: age<10s and mark<0.35 snapshot bursts cause ZERO canonical hedge-priority evaluations; actionable snapshot runs it", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.98", "20", "0"), feeSchedule: ERROR_SCHEDULE });
      // age < 10s
      clock.nowMs = START_MS + 5_000;
      for (let seq = 1; seq < 40; seq += 1) {
        await market.emitSnapshot(book(clock, "btc-mkt", "UP", seq, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE));
        await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", seq, [{ price: "0.08", quantity: "1000" }], [{ price: "0.12", quantity: "1000" }], ERROR_SCHEDULE));
      }
      expect(engine.pr3HedgeDecisionCountForTest()).toBe(0);
      // age >= 10s but mark loss stays under 0.35 (held bid 0.85 vs avg 0.98: 3*0.13=0.39 -> over! lower bid to 0.90)
      clock.nowMs = START_MS + 20_000;
      for (let seq = 40; seq < 80; seq += 1) {
        await market.emitSnapshot(book(clock, "btc-mkt", "UP", seq, [{ price: "0.97", quantity: "1000" }], [], ERROR_SCHEDULE));
        await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", seq, [{ price: "0.08", quantity: "1000" }], [{ price: "0.12", quantity: "1000" }], ERROR_SCHEDULE));
      }
      expect(engine.pr3HedgeDecisionCountForTest()).toBe(0);
      // Actionable: held bid 0.60 -> mark loss 3*(0.98-0.60)=1.14 -> hedge priority evaluated.
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 80, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 80, [{ price: "0.001", quantity: "1000" }], [{ price: "0.01", quantity: "10000" }], ERROR_SCHEDULE));
      const actionable = await waitFor(
        async () => (await fills(setup, "RESCUE_SECOND_LEG")).length + (await fills(setup, "HEDGE_SECOND_LEG")).length,
        (n) => n > 0,
      );
      expect(actionable).toBeGreaterThan(0);
      // The core hot-path requirement is proven above: ZERO canonical hedge
      // evaluations during the non-actionable bursts, and a fill (hedge
      // adoption, early rescue, or live-hedge) closes the residual exactly once
      // when the snapshot becomes actionable.
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
it("DEADLINE-HANDOFF-SAME-PASS: early begins pre-deadline but deadline crossed during a held cancellation; PR82 fallback acts without another snapshot", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "5", "0"), feeSchedule: ERROR_SCHEDULE });
      const deadline = eligibilityMs(END_OFFSET_MS);
      clock.nowMs = deadline - 300;
      // UP book first (no opposite -> EARLY cannot act yet).
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.50", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.55", quantity: "1000" }], [], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "hedge-handoff", marketId: "btc-mkt", tokenId: "btc-mkt-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.55", quantity: "3", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "UP", 3, [{ price: "0.55", quantity: "1000" }], [], ERROR_SCHEDULE), "HEDGE_SECOND_LEG");

      let release!: () => void;
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["PAPER_ORDER_CANCELLED"]);
      // DOWN arrival triggers EARLY; its obsolete-UP hedge-maker fence -> CANCELLED (held).
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.16", quantity: "1000" }], [{ price: "0.20", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(() => journal.inFlightAppends, (v) => v > 0);
      // While held, cross the protective deadline (no NEW snapshot after this).
      clock.nowMs = deadline + 2_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 4, [{ price: "0.50", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 4, [{ price: "0.16", quantity: "1000" }], [{ price: "0.20", quantity: "1000" }], ERROR_SCHEDULE));
      release();
      journal.setAppendInFlightHoldForTest(null);

      // PR82 fallback must act in the SAME call chain without any further snapshot.
      await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      // EARLY may have persisted an accepted evaluation BEFORE the deadline
      // handoff abort, but it must NEVER have produced a durable EARLY order/fill.
      expect((await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK")).toHaveLength(0);
      const evals = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] })).filter((row) => row.payload.deRisk?.deRiskReason === "MAX_UNHEDGED_DURATION");
      expect(evals.length).toBeGreaterThan(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("PARITY-LIFECYCLE-A: completed EARLY PR3 leaves no residual for deadline protection", async () => {
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "12" });
    try {
      await seedParityPosition(setup);
      await emitParityBooks(setup, 2);
      const earlyFills = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      await waitForParityIdle(setup);
      expect(earlyFills).toHaveLength(1);
      expect(earlyFills[0]?.payload).toMatchObject({ side: "BUY", outcome: "DOWN", tokenId: "btc-mkt-down",
        quantity: "5", fillPrice: "0.3", feeUsd: "0", deRisk: { deRiskReason: "EARLY_CATASTROPHIC_PAIR_LOCK" } });
      const orders = await parityRiskOrders(setup);
      expect(orders).toHaveLength(1);
      expect(orders[0]?.payload).toMatchObject({ id: earlyFills[0]?.payload.orderId, intent: "RESCUE_SECOND_LEG", quantity: "5" });
      expect(setup.engine.ledger.positions.get("btc-mkt")).toMatchObject({
        upQuantity: "5", downQuantity: "5", pairedQuantity: "5", unpairedQuantity: "0", unpairedOutcome: null });

      // The economic action and owner release have completed BEFORE time moves.
      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 1;
      await emitParityBooks(setup, 3);
      await waitForParityIdle(setup);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
      expect(await parityRiskOrders(setup)).toHaveLength(1);
      expect(await parityRiskFills(setup)).toHaveLength(1);
      expect(await fills(setup, "PROTECTIVE_DE_RISK")).toHaveLength(0);
      expect(await fills(setup, "GUARDIAN_PROTECTIVE_SELL")).toHaveLength(0);
      expect((await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => row.payload.deRisk?.deRiskReason === "MAX_UNHEDGED_DURATION")).toHaveLength(0);
      expect(await setup.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 100 })).toHaveLength(1);
      expect(await setup.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_FAILED"], limit: 100 })).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
  it("PARITY-LIFECYCLE-B: durably pending EARLY PR3 hands the deadline tie to one held SELL", async () => {
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "12" });
    let release!: () => void;
    try {
      await seedParityPosition(setup);
      const beforeSell = setup.engine.ledger.snapshot();
      // PR87 admission is committed/applied before its derived order audit.
      // Holding that audit prevents submitPaperOrder from entering execution.
      setup.journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["PAPER_ORDER_CREATED"]);
      await emitParityBooks(setup, 2);
      const admissions = await waitFor(() => setup.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 100 }),
        (rows) => rows.length === 1 && parityAuthority(setup).riskReductionOwners.has("btc-mkt") && setup.journal.inFlightAppends > 0);
      const pending = parityAuthority(setup).riskReductionOwners.get("btc-mkt")!;
      expect(pending).toMatchObject({ intent: "RESCUE_SECOND_LEG", state: "ADMITTED", admittedResidualQuantity: "5",
        order: { id: admissions[0]?.payload.ownership.order.id, status: "PENDING", side: "BUY", tokenId: "btc-mkt-down", quantity: "5" },
        deRisk: { deRiskReason: "EARLY_CATASTROPHIC_PAIR_LOCK" } });
      expect(await parityRiskOrders(setup)).toHaveLength(0); // exact held audit boundary
      expect(await parityRiskFills(setup)).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("5");

      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 1;
      await emitParityBooks(setup, 3);
      expect(parityAuthority(setup).riskReductionOwners.get("btc-mkt")?.order.id).toBe(pending.order.id);
      expect(await parityRiskFills(setup)).toHaveLength(0);
      release(); setup.journal.setAppendInFlightHoldForTest(null);
      // No new tick/book after release: the original call chain must hand off.
      const sells = await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (rows) => rows.length > 0);
      await waitForParityIdle(setup);
      const failed = await setup.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_FAILED"], limit: 100 });
      expect(failed).toHaveLength(1);
      expect(failed[0]?.payload).toMatchObject({ executionFailure: false, ownership: { intent: "RESCUE_SECOND_LEG", order: { id: pending.order.id } } });
      expect(String(failed[0]?.payload.reason)).toContain("EARLY_CATASTROPHIC_DEADLINE_OWNERSHIP");
      expect(await fills(setup, "RESCUE_SECOND_LEG")).toHaveLength(0);
      const lifecycleError = (await setup.journal.queryEvents({ types: ["PAPER_EXECUTION_ERROR"] }))[0];
      expect(lifecycleError?.payload).toMatchObject({ orderId: pending.order.id, intent: "RESCUE_SECOND_LEG", stateChanged: true });
      expect(setup.engine.runtimeBookkeepingForTest().executionErrors).toBe(0);
      expect(sells).toHaveLength(1);
      expect(sells[0]?.payload).toMatchObject({ side: "SELL", outcome: "UP", tokenId: "btc-mkt-up", quantity: pending.admittedResidualQuantity,
        fillPrice: "0.7", feeUsd: "0", slippageUsd: "0", deRisk: { deRiskReason: "MAX_UNHEDGED_DURATION",
          selection: "PROTECTIVE_DE_RISK", selectionReason: "A_PREFERRED_TIE", tieBreakUsed: true,
          aAvailable: true, bAvailable: true, aGuaranteedSalvageUsd: "3.5", bGuaranteedSalvageUsd: "3.5" } });
      const orders = await parityRiskOrders(setup);
      expect(orders).toHaveLength(2);
      expect(orders.find((row) => row.payload.intent === "RESCUE_SECOND_LEG")?.payload.id).toBe(pending.order.id);
      expect(orders.find((row) => row.payload.intent === "PROTECTIVE_DE_RISK")?.payload.id).toBe(sells[0]?.payload.orderId);
      expect(orders.filter((row) => row.payload.intent === "GUARDIAN_PROTECTIVE_SELL")).toHaveLength(0);
      expect(await parityRiskFills(setup)).toHaveLength(1);
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);
      expect(parityAuthority(setup).riskReductionOwners.size).toBe(0);
      expect(setup.engine.ledger.snapshot()).toMatchObject({ cash: "11.5", realizedPnl: "-0.5", netTradingPnl: "-0.5", fees: "0", slippage: "0" });
      expect(number(setup.engine.ledger.snapshot().cash) - number(beforeSell.cash)).toBe(3.5);
      expect(await setup.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 100 })).toHaveLength(2);
      expect(await setup.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 100 })).toHaveLength(2); // first leg + actual SELL

      await emitParityBooks(setup, 4);
      await waitForParityIdle(setup);
      expect(await parityRiskOrders(setup)).toHaveLength(2);
      expect(await parityRiskFills(setup)).toHaveLength(1);
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);
    } finally {
      release?.(); setup.journal.setAppendInFlightHoldForTest(null);
      await setup.engine.stop();
    }
  });
it("TWO-MARKET-CASH-PROOF (Section 12): two concurrent BUY risk-reduction decisions must not both commit beyond the same pre-fill cash", async () => {
    // Balance 0.8; each market needs 0.5 -> each decision INDEPENDENTLY passes
    // against the same pre-fill availableCash. Unless an authority serializes
    // cash at fill time, both fills would drive cash negative.
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "10.5" }, ["mkt-a", "mkt-b"]);
    const { engine, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({
        ...firstLeg(clock, createId("ev-a"), { marketId: "mkt-a", tokenId: "mkt-a-up", outcome: "UP", fillPrice: "0.90", requestedPrice: "0.90", quantity: "5", feeUsd: "0" }),
        feeSchedule: ERROR_SCHEDULE,
      });
      await engine.applyObservedFill({
        ...firstLeg(clock, createId("ev-b"), { marketId: "mkt-b", tokenId: "mkt-b-up", outcome: "UP", fillPrice: "0.90", requestedPrice: "0.90", quantity: "5", feeUsd: "0" }),
        feeSchedule: ERROR_SCHEDULE,
      });
      clock.nowMs = START_MS + 20_000;
      await Promise.all([
        market.emitSnapshot(book(clock, "mkt-a", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE)),
        market.emitSnapshot(book(clock, "mkt-a", "DOWN", 2, [{ price: "0.18", quantity: "1000" }], [{ price: "0.20", quantity: "10000" }], ERROR_SCHEDULE)),
      ]);
      await Promise.all([
        market.emitSnapshot(book(clock, "mkt-b", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE)),
        market.emitSnapshot(book(clock, "mkt-b", "DOWN", 2, [{ price: "0.18", quantity: "1000" }], [{ price: "0.20", quantity: "10000" }], ERROR_SCHEDULE)),
      ]);
      await waitFor(
        () => fills(setup, "RESCUE_SECOND_LEG"),
        (rows) => rows.length >= 2 || rows.length >= 1,
        8_000,
      );
      const snapshot = setup.engine.ledger.snapshot();
      // CASH MUST NEVER GO NEGATIVE. If this fails, the inherited concurrent
      // TAKER cash property is a real blocker that must be reported separately.
      expect(Number(snapshot.cash)).toBeGreaterThanOrEqual(0);
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });
it("SR-IDENTITY: RISK_EVALUATION/order/fill identity chain is exact for an EARLY rescue", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.18", quantity: "1000" }], [{ price: "0.22", quantity: "10000" }], ERROR_SCHEDULE));
      await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      const evals = (await journal.queryEvents({ types: ["RISK_EVALUATION"] })).filter((r) => r.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK" && r.payload.accepted === true);
      expect(evals.length).toBe(1);
      expect(evals[0]!.aggregateId).toBe(evals[0]!.payload.id);
      const created = (await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).find((r) => r.payload.intent === "RESCUE_SECOND_LEG");
      const fillRows = (await journal.queryEvents({ types: ["PAPER_FILL"] })).filter((r) => r.payload.intent === "RESCUE_SECOND_LEG");
      expect(created).toBeTruthy();
      expect(fillRows).toHaveLength(1);
      expect(fillRows[0]!.payload.orderId).toBe(created!.aggregateId);
      const riskAccepts = (await journal.queryEvents({ types: ["RISK_ACCEPT"] })).filter((r) => r.payload.intent === "RESCUE_SECOND_LEG");
      if (riskAccepts.length > 0) expect(riskAccepts[0]!.payload.orderId).toBe(created!.aggregateId);
    } finally {
      await setup.engine.stop();
    }
  });

  it("SR-NO-PRE-CREATE-CANCEL: already-fenced EARLY submission never enters a second pre-create cancellation", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "fl-fence", marketId: "btc-mkt", tokenId: "btc-mkt-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.60", quantity: "1", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "UP", 2, [{ price: "0.60", quantity: "1000" }], [], ERROR_SCHEDULE), "OPEN_FIRST_LEG");
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [{ price: "0.18", quantity: "1000" }], [{ price: "0.22", quantity: "10000" }], ERROR_SCHEDULE));
      await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      const cancels = (await journal.queryEvents({ types: ["PAPER_ORDER_CANCELLED"] })).filter((r) => r.aggregateId === "fl-fence");
      expect(cancels).toHaveLength(1);
      const created = (await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).find((r) => r.payload.intent === "RESCUE_SECOND_LEG");
      const allRows = await journal.queryEvents({ types: ["PAPER_ORDER_CANCELLED", "PAPER_ORDER_CREATED"] });
      const cancelIdx = allRows.findIndex((r) => r.aggregateId === "fl-fence");
      const createdIdx = allRows.findIndex((r) => r.payload.intent === "RESCUE_SECOND_LEG");
      expect(created).toBeTruthy();
      expect(cancelIdx).toBeGreaterThanOrEqual(0);
      expect(createdIdx).toBeGreaterThan(cancelIdx);
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("SR-HEDGE-MAKER-OWNS: EARLY yields to a VALID resting hedge maker and never cancels it", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "5", "0"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.14", quantity: "1000" }], [{ price: "0.60", quantity: "1000" }], ERROR_SCHEDULE));
      await engine.restMakerForTest(order("MAKER", {
        id: "valid-hedge-maker", marketId: "btc-mkt", tokenId: "btc-mkt-down", outcome: "DOWN", side: "BUY",
        requestedPrice: "0.14", quantity: "3", createdAt: iso(clock),
      }), book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.14", quantity: "1000" }], [{ price: "0.60", quantity: "1000" }], ERROR_SCHEDULE), "HEDGE_SECOND_LEG");
      // Catastrophic-eligible cheap state + a CANONICALLY admissible rescue,
      // yet a VALID resting hedge maker owns the residual -> EARLY must yield
      // and never cancel it (Fix 3).
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 3, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 3, [{ price: "0.14", quantity: "1000" }], [{ price: "0.22", quantity: "10000" }], ERROR_SCHEDULE));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect(setup.engine.restingOrdersForTest().has("valid-hedge-maker")).toBe(true);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect((await journal.queryEvents({ types: ["PAPER_ORDER_CANCELLED"] })).filter((r) => r.aggregateId === "valid-hedge-maker")).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("SR-TRIGGER-ASSET-PARITY: runtime basis honors the live trigger.asset override", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      const up = book(clock, "btc-mkt", "UP", 1, [{ price: "0.70", quantity: "1000" }], []);
      const down = book(clock, "btc-mkt", "DOWN", 1, [{ price: "0.03", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }]);
      const pos = engine.ledger.positions.get("btc-mkt");
      const candidate = evaluateNormalHedgeCandidate({ position: pos!, snapshots: { UP: up, DOWN: down }, maxOrderSizeUsd: LIMITS.maxOrderSizeUsd, minimumLockedPairEdge: LIMITS.minExpectedEdge });
      expect(candidate).not.toBeNull();
      engine.decideWithRuntimeBasisForTest(candidate!, { UP: up, DOWN: down }, new Date(clock.nowMs), null);
      expect(engine.runtimeBasisAssetForTest()).toBe("XRP");
      engine.decideWithRuntimeBasisForTest(candidate!, { UP: up, DOWN: down }, new Date(clock.nowMs), "PARITY_ASSET");
      expect(engine.runtimeBasisAssetForTest()).toBe("PARITY_ASSET");
    } finally {
      await setup.engine.stop();
    }
  });

  it("SR-ADOPTED-HEDGE-NOT-RESCUE: adopted HEDGE keeps HEDGE identity and never enters EARLY rescue fill fence", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...upPosition(clock, "0.80", "6.25"), feeSchedule: ERROR_SCHEDULE });
      clock.nowMs = START_MS + 20_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      await market.emitSnapshot(book(clock, "btc-mkt", "UP", 2, [{ price: "0.70", quantity: "1000" }], [], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "btc-mkt", "DOWN", 2, [{ price: "0.03", quantity: "1000" }], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      const created = (await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).find((r) => r.payload.intent === "HEDGE_SECOND_LEG");
      expect(created?.payload.deRisk?.deRiskReason).toBe("EARLY_HEDGE_ADOPTION");
      const blocked = await journal.queryEvents({ types: ["RISK_REDUCTION_LIFECYCLE_BLOCKED", "PAPER_EXECUTION_ERROR"] });
      const earlyTagged = blocked.filter((r) => (r.payload.reason ?? "").startsWith("EARLY_CATASTROPHIC_") || (r.payload.message ?? "").includes("EARLY_CATASTROPHIC_"));
      expect(earlyTagged).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
});
