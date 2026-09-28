import { describe, expect, it } from "vitest";
import {
  computeGuaranteedLockedPnl,
  createId,
  evaluateEarlyCatastrophicPairLock,
  feeScheduleSchema,
  riskLimitsSchema,
  type MarketSnapshot,
  type PaperFill,
  type ReferenceTick,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
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
  outcome: "UP" | "DOWN",
  sequence: number,
  bids: Array<{ price: string; quantity: string }>,
  asks: Array<{ price: string; quantity: string }> = [],
  feeScheduleOverride: typeof feeSchedule = feeSchedule,
): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return newSnapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-mkt", tokenId, outcome, asset: "XRP",
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
    fillModel: "pr3-test:first-leg", feeSchedule,
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

async function setupScenario(endOffsetMs: number, env: Record<string, string> = {}) {
  const clock: Clock = { nowMs: START_MS };
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + endOffsetMs).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "XRP", "btc-up", "btc-down", endDate)]));
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

const fills = (setup: Awaited<ReturnType<typeof setupScenario>>, intent: string) =>
  setup.journal.queryEvents({ types: ["PAPER_FILL"] }).then((rows) => rows.filter((row) => row.payload.intent === intent));
const suppressed = (setup: Awaited<ReturnType<typeof setupScenario>>) =>
  setup.journal.queryEvents({ types: ["PROTECTIVE_DE_RISK_SUPPRESSED"] });

function eligibilityMs(endOffsetMs: number): number {
  return Math.min(START_MS + 1 + LIMITS.maxUnhedgedDurationMs, START_MS + endOffsetMs - 80_000);
}

const number = (value: string | undefined): number => Number(value ?? 0);

type Scenario = Awaited<ReturnType<typeof setupScenario>>;

/** A TRULY unhedged UP position with the given economics. */
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
describe("PR3 early catastrophic pair-lock", { timeout: 40_000 }, () => {
  const earlyEvals = (setup: Scenario) =>
    setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }).then((rows) =>
      rows.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK"));

  it("Test A (4981707): UP @0.84 -> early full pair-lock ~-$0.587, no protective SELL", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.84", fillPrice: "0.84",
          quantity: "5.74712644", feeUsd: "0.05407",
        }),
        feeSchedule,
      });
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("5.74712644");

      setup.clock.nowMs = START_MS + 20_000; // age > 10s
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.77", quantity: "1000" }], [{ price: "0.85", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.24", quantity: "2000" }]));

      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues[0]?.payload).toMatchObject({ side: "BUY", outcome: "DOWN", quantity: "5.74712644", fillPrice: "0.24" });
      expect(number(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      const evals = await earlyEvals(setup);
      expect(evals.length).toBeGreaterThan(0);
      const pnl = Number(evals[0]?.payload?.earlyCatastrophic?.guaranteedLockedPnl ?? NaN);
      expect(pnl).toBeCloseTo(-0.5872, 2);
      // AUDIT IDENTITY: RISK_EVALUATION aggregate == payload risk id; the order
      // id from the canonical decision flows into PAPER_ORDER_CREATED and the
      // rescue fill; intent/deRisk reason remains EARLY_CATASTROPHIC_PAIR_LOCK.
      const earlyEval = evals[0]!;
      expect(earlyEval.aggregateId).toBe(earlyEval.payload.id);
      expect(earlyEval.payload.intent).toBe("RESCUE_SECOND_LEG");
      expect(earlyEval.payload.deRisk?.deRiskReason).toBe("EARLY_CATASTROPHIC_PAIR_LOCK");
      const createdRows = await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
      const earlyCreated = createdRows.find((row) =>
        row.payload.intent === "RESCUE_SECOND_LEG" && row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK");
      expect(earlyCreated).toBeTruthy();
      const rescueFillRow = (await setup.journal.queryEvents({ types: ["PAPER_FILL"] }))
        .find((row) => row.payload.intent === "RESCUE_SECOND_LEG");
      expect(rescueFillRow?.payload.orderId).toBe(earlyCreated!.aggregateId);
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("Test B (4983148): DOWN @0.85 -> early full pair-lock ~-$0.535", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          requestedPrice: "0.85", fillPrice: "0.85", quantity: "5.88235294", feeUsd: "0.05250",
        }),
        feeSchedule,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.785", quantity: "1000" }], [{ price: "0.85", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.22", quantity: "10000" }]));

      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues[0]?.payload).toMatchObject({ side: "BUY", outcome: "UP", quantity: "5.88235294", fillPrice: "0.22" });
      expect(number(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
      const evals = await earlyEvals(setup);
      expect(Number(evals[0]?.payload?.earlyCatastrophic?.guaranteedLockedPnl ?? NaN)).toBeCloseTo(-0.5349, 2);
    } finally {
      await setup.engine.stop();
    }
  });
it("Test C (4962594 false-rescue control): locked > cap -> NO rescue, then normal hedge path", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.88", fillPrice: "0.88",
          quantity: "5.68181818", feeUsd: "0.04200",
        }),
        feeSchedule,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.79", quantity: "1000" }], [{ price: "0.89", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.23", quantity: "100" }]));
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("5.68181818");
      // The early path evaluated and suppressed (per-market audible at least
      // once per 60s); the cap exceeded, so the position stays untouched.
      await waitFor(
        () => suppressed(setup),
        (rows) => rows.some((row) => row.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK"),
      );

      // Improve the opposite ask to a NORMAL PROFITABLE hedge state: the hedge
      // must now fire, not the early rescue. Ask depth notional >= minLiquidity
      // so the normal hedge RISK check accepts it.
      setup.clock.nowMs = START_MS + 21_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [{ price: "0.79", quantity: "1000" }], [{ price: "0.89", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [{ price: "0.04", quantity: "100" }], [{ price: "0.05", quantity: "10000" }]));
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "100"));
      setup.clock.nowMs = START_MS + 22_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "102"));

      const hedges = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedges[0]?.payload).toMatchObject({ side: "BUY", outcome: "DOWN", quantity: "5.68181818", fillPrice: "0.05" });
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("Test D (normal-hedge priority race): catastrophic + hedge simultaneously -> exactly HEDGE, zero rescue", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0.07",
        }),
        feeSchedule,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.11", quantity: "10000" }], [{ price: "0.12", quantity: "10000" }]));
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "100"));
      setup.clock.nowMs = START_MS + 21_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "102"));

      const hedgeRows = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedgeRows).toHaveLength(1);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
it("Test E: partial opposite depth -> no early rescue; next fresh snapshot retries full", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0.07",
        }),
        feeSchedule,
      });
      setup.clock.nowMs = START_MS + 20_000;
      // Opposite depth covers only 2 of 6.25 -> partial -> NO early rescue.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.24", quantity: "2" }]));
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.25");
      await waitFor(
        () => suppressed(setup),
        (rows) => rows.some((row) => row.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK"),
      );

      // A LATER FRESH snapshot with full opposite depth retries successfully.
      setup.clock.nowMs = START_MS + 21_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [], [{ price: "0.24", quantity: "2000" }]));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues[0]?.payload).toMatchObject({ quantity: "6.25" });
      expect(number(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("Test F: Dec boundary rows (age / mark / cap / exact -0.60 allowed)", async () => {
    // F1: age < 10s
    const ageSetup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await ageSetup.engine.beginExperiment()).ok).toBe(true);
      await ageSetup.engine.applyObservedFill({
        ...firstLeg(ageSetup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0.07",
        }),
        feeSchedule,
      });
      ageSetup.clock.nowMs = START_MS + 5_000; // below the 10s minimum
      await ageSetup.market.emitSnapshot(book(ageSetup.clock, "UP", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await ageSetup.market.emitSnapshot(book(ageSetup.clock, "DOWN", 2, [], [{ price: "0.24", quantity: "100" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(ageSetup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await ageSetup.engine.stop();
    }

    // F2: mark loss < 0.35 (held-only book: the mark check is first)
    const markSetup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await markSetup.engine.beginExperiment()).ok).toBe(true);
      await markSetup.engine.applyObservedFill({
        ...firstLeg(markSetup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0.07",
        }),
        feeSchedule,
      });
      markSetup.clock.nowMs = START_MS + 20_000;
      await markSetup.market.emitSnapshot(book(markSetup.clock, "UP", 2, [{ price: "0.77", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(markSetup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      await waitFor(
        () => suppressed(markSetup),
        (rows) => rows.some((row) => row.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK" && row.payload.reason === "MARK_LOSS_NOT_REACHED"),
      );
    } finally {
      await markSetup.engine.stop();
    }

    // F3: locked PnL < -0.60 -> no rescue
    const capSetup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await capSetup.engine.beginExperiment()).ok).toBe(true);
      await capSetup.engine.applyObservedFill({
        ...firstLeg(capSetup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0.07",
        }),
        feeSchedule,
      });
      capSetup.clock.nowMs = START_MS + 20_000;
      await capSetup.market.emitSnapshot(book(capSetup.clock, "UP", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }]));
      await capSetup.market.emitSnapshot(book(capSetup.clock, "DOWN", 2, [], [{ price: "0.55", quantity: "100" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(capSetup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      await waitFor(
        () => suppressed(capSetup),
        (rows) => rows.some((row) => row.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK"),
      );
    } finally {
      await capSetup.engine.stop();
    }
// F4 unit: exact -0.60 locked PnL is allowed by the Decimal comparison.
    const exactPosition = {
      id: "pos-exact", marketId: "btc-mkt", upQuantity: "5", downQuantity: "0",
      averageUpCost: "0.70", averageDownCost: "0", pairedQuantity: "0",
      unpairedOutcome: "UP" as const, unpairedQuantity: "5", unhedgedStartedAt: iso({ nowMs: START_MS }),
      combinedPairCost: "0.70", totalFees: "0", totalSlippage: "0",
      openedAt: iso({ nowMs: START_MS }), updatedAt: iso({ nowMs: START_MS }),
    };
    const exactPnl = computeGuaranteedLockedPnl({ position: exactPosition, rescueQuantity: "5", rescueVwap: "0.42", rescueTakerFeeUsd: "0" });
    expect(Number(exactPnl)).toBeCloseTo(-0.6, 6);
    const exactEval = evaluateEarlyCatastrophicPairLock({
      position: exactPosition,
      heldSnapshot: book({ nowMs: START_MS + 50_000 }, "UP", 9, [{ price: "0.60", quantity: "5" }], [{ price: "0.72", quantity: "5" }], ERROR_SCHEDULE),
      oppositeSnapshot: book({ nowMs: START_MS + 50_000 }, "DOWN", 9, [], [{ price: "0.42", quantity: "5" }], ERROR_SCHEDULE),
      nowMs: START_MS + 50_000,
      maxOrderSizeUsd: LIMITS.maxOrderSizeUsd,
      minUnhedgedMs: 10_000,
      triggerLossUsd: 0.35,
      maxLockedLossUsd: 0.60,
      hedgeAdmissible: false,
    });
    expect(exactEval.eligible).toBe(true);
    expect(Number(exactEval.guaranteedLockedPnl)).toBeCloseTo(-0.6, 6);

    // F5 engine: exact -0.60 locked PnL is allowed end-to-end (zero-fee books).
    const boundarySetup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await boundarySetup.engine.beginExperiment()).ok).toBe(true);
      await boundarySetup.engine.applyObservedFill({
        ...firstLeg(boundarySetup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.70", fillPrice: "0.70",
          quantity: "5", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      boundarySetup.clock.nowMs = START_MS + 20_000;
      await boundarySetup.market.emitSnapshot(book(boundarySetup.clock, "UP", 2, [{ price: "0.60", quantity: "1000" }], [{ price: "0.72", quantity: "1000" }], ERROR_SCHEDULE));
      await boundarySetup.market.emitSnapshot(book(boundarySetup.clock, "DOWN", 2, [], [{ price: "0.42", quantity: "1000" }], ERROR_SCHEDULE));
      const rescues = await waitFor(() => fills(boundarySetup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(rescues[0]?.payload).toMatchObject({ quantity: "5", fillPrice: "0.42" });
      expect(number(boundarySetup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
    } finally {
      await boundarySetup.engine.stop();
    }
  });
it("Test G: latency deterioration -> final-fence rejects the actual fill", async () => {
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_EXECUTION_LATENCY_MS: "2000" });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.90", fillPrice: "0.90",
          quantity: "6.25", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.80", quantity: "1000" }], [{ price: "0.92", quantity: "1000" }], ERROR_SCHEDULE));
      // Decision ladder: full residual executable at 0.10(4)+0.30(2.25) -> VWAP
      // 0.172, locked pnl = 6.25 - 5.625 - 1.075 = -0.45 (>= -0.6): admission.
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.10", quantity: "4" }, { price: "0.30", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(() => earlyEvals(setup), (rows) => rows.length > 0);

      // During paper latency the opposite book reshapes toward the worst level:
      // full fill at VWAP 0.30 -> locked pnl = 6.25 - 5.625 - 1.875 = -2.50 <
      // -0.60 -> the FINAL-FENCE loss-cap revalidation MUST reject the mutation.
      setup.clock.nowMs = START_MS + 20_400;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [], [{ price: "0.30", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(
        () => suppressed(setup),
        (rows) => rows.some((row) => row.payload.gate === "FINAL_FENCE" && row.payload.reason === "EARLY_CATASTROPHIC_LOSS_CAP_EXCEEDED"),
      );
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const durableFills = await setup.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 16 });
      expect(durableFills.filter((row) => row.operationKey.startsWith("fill:")).map((row) => row.operationKey)).toHaveLength(1);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.25");
      // State remains retryable: a fresh GOOD snapshot can still fire later.
      setup.clock.nowMs = START_MS + 21_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 4, [{ price: "0.80", quantity: "1000" }], [{ price: "0.92", quantity: "1000" }], ERROR_SCHEDULE));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 4, [], [{ price: "0.10", quantity: "4" }, { price: "0.30", quantity: "1000" }], ERROR_SCHEDULE));
      const retries = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(retries[0]?.payload).toMatchObject({ quantity: "6.25" });
      expect(number(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("Test H: old deadline A/B fallback still acts when early does not qualify", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, createId("event-first")), feeSchedule });
      // Pre-deadline: mark is healthy -> the early protection correctly stays off.
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.80", quantity: "1000" }], [{ price: "0.99", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.30", quantity: "1000" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);

      // At the old deadline the A/B fallback fires (A wins) with its own reason.
      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 1;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [], [{ price: "0.30", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [{ price: "0.75", quantity: "1000" }], []));
      const protective = await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (rows) => rows.length > 0);
      expect(protective[0]?.payload).toMatchObject({ side: "SELL", outcome: "DOWN", fillPrice: "0.75" });
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(number(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity)).toBe(0);
      const evals = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => row.payload.deRisk?.deRiskReason === "MAX_UNHEDGED_DURATION");
      expect(evals.length).toBeGreaterThan(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("BLOCKER1-cash: canonical decideRescueSecondLegOrder rejects INSUFFICIENT_CASH (zero early fill)", async () => {
    const setup = await setupScenario(END_OFFSET_MS, { PAPER_START_BALANCE: "20" });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.90", fillPrice: "0.90",
          quantity: "20", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.80", quantity: "1000" }], [{ price: "0.92", quantity: "1000" }], ERROR_SCHEDULE));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.11", quantity: "10000" }], ERROR_SCHEDULE));
      // Early conditions all pass (mark loss >= 0.35, full depth, cap >= -0.60),
      // but canonical risk admission -> INSUFFICIENT_CASH (cash 2 < notional 2.2).
      await waitFor(
        () => suppressed(setup),
        (rows) => rows.some((row) => row.payload.gate === "EARLY_CATASTROPHIC_PAIR_LOCK"),
      );
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const evals = await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] });
      const earlyEval = evals.filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK");
      expect(earlyEval.length).toBeGreaterThan(0);
      expect(earlyEval[0]?.payload.accepted).toBe(false);
      expect(earlyEval[0]?.payload.vetoes).toContain("INSUFFICIENT_CASH");
      expect(earlyEval[0]?.payload.deRisk?.finalState).toBe("RISK_VETO");
      const durable = await setup.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 16 });
      expect(durable.filter((row) => row.operationKey.startsWith("fill:")).map((row) => row.operationKey)).toHaveLength(1);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("20");
      expect(setup.engine.ledger.snapshot().cash).toBe("2"); // unchanged, retryable
    } finally {
      await setup.engine.stop();
    }
  });

  it("BLOCKER1-exposure: canonical decideRescueSecondLegOrder rejects MAX_MARKET_EXPOSURE", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.90", fillPrice: "0.90",
          quantity: "200", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.85", quantity: "1000" }], [{ price: "0.92", quantity: "1000" }], ERROR_SCHEDULE));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.10", quantity: "10000" }], ERROR_SCHEDULE));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const evals = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => row.payload.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK");
      expect(evals.length).toBeGreaterThan(0);
      expect(evals[0]?.payload.accepted).toBe(false);
      expect(evals[0]?.payload.vetoes).toContain("MAX_MARKET_EXPOSURE");
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("200");
    } finally {
      await setup.engine.stop();
    }
  });
it("BLOCKER2-deadline: at/after deadline the PR82 A/B fallback owns (early never fires even if eligible)", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          requestedPrice: "0.60", fillPrice: "0.60", quantity: "10", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.60", quantity: "1000" }], [{ price: "0.61", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.30", quantity: "10000" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0); // healthy mark -> no early pre-deadline

      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 1;
      // Mark loss >= 0.35 and early cap pass, but the deadline owns the position.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [], [{ price: "0.30", quantity: "10000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [{ price: "0.75", quantity: "1000" }], []));
      const protective = await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (rows) => rows.length > 0);
      expect(protective[0]?.payload).toMatchObject({ side: "SELL", outcome: "DOWN", fillPrice: "0.75", quantity: "10" });
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect((await earlyEvals(setup))).toHaveLength(0);
      const evals = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => row.payload.deRisk?.deRiskReason === "MAX_UNHEDGED_DURATION");
      expect(evals.length).toBeGreaterThan(0);
      expect(evals[0]?.payload.deRisk?.selectionReason).toBe("A_STRICTLY_BETTER");
    } finally {
      await setup.engine.stop();
    }
  });

  it("BLOCKER2-partial: old fallback partial SELL continues managing residual; early never takes over", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.engine.applyObservedFill({
        ...firstLeg(setup.clock, createId("event-first"), {
          requestedPrice: "0.60", fillPrice: "0.60", quantity: "10", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      setup.clock.nowMs = START_MS + 20_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.60", quantity: "1000" }], [{ price: "0.61", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.25", quantity: "10000" }]));

      // At the deadline the fallback SELLs only 4 (bid depth 4): residual 6.
      // Phase 1 uses an EMPTY opposite book so B is unavailable -> A_ONLY
      // partial SELL.
      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 1;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [], []));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [{ price: "0.75", quantity: "4" }], []));
      await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (rows) => rows.length === 1);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.downQuantity).toBe("6");

      // A later snapshot would satisfy the EARLY predicate, but the deadline
      // fallback owns the residual: fallback B pairs the remaining 6. (Clock
      // advances well past the fallback retry slot.)
      setup.clock.nowMs = eligibilityMs(END_OFFSET_MS) + 6_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 4, [], [{ price: "0.15", quantity: "10000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 4, [{ price: "0.50", quantity: "1000" }], []));
      const rescues = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (rows) => rows.length === 1);
      expect(rescues[0]?.payload).toMatchObject({ quantity: "6", fillPrice: "0.15" });
      expect((await earlyEvals(setup))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
it("BLOCKER3-race: hedge becomes admissible AFTER early flight acquisition -> early yields, exactly one HEDGE", async () => {
    const setup = await setupScenario(END_OFFSET_MS);
    const { engine, journal, market, clock } = setup;
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({
        ...firstLeg(clock, createId("event-first"), {
          tokenId: "btc-up", outcome: "UP", requestedPrice: "0.80", fillPrice: "0.80",
          quantity: "6.25", feeUsd: "0",
        }),
        feeSchedule: ERROR_SCHEDULE,
      });
      clock.nowMs = START_MS + 20_000;
      // Seed a resting hedge maker + a DOWN book where the hedge is NOT
      // admissible (ask 0.19 -> pair edge 0.01 < minExpectedEdge).
      const downBook = book(clock, "DOWN", 2, [{ price: "0.14", quantity: "10" }], [{ price: "0.19", quantity: "10000" }], ERROR_SCHEDULE);
      await market.emitSnapshot(downBook);
      await engine.restMakerForTest(order("MAKER", {
        id: "seed-hedge", marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "BUY",
        requestedPrice: "0.14", quantity: "1", createdAt: iso(clock),
      }), downBook, "HEDGE_SECOND_LEG");

      let releaseHold!: () => void;
      const hold = new Promise<void>((resolve) => { releaseHold = resolve; });
      journal.setAppendInFlightHoldForTest(hold, ["PAPER_ORDER_CANCELLED"]);
      // THIS snapshot makes the early path eligible (mark loss >= 0.35, full
      // depth at 0.19, cap passes) -> it acquires the flight -> its maker
      // cancellation awaits the held PAPER_ORDER_CANCELLED append.
      await market.emitSnapshot(book(clock, "UP", 2, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }], ERROR_SCHEDULE));
      await waitFor(
        () => journal.queryDurableTransitions({ types: ["PAPER_ORDER_CANCELLED"], limit: 8 }),
        (rows) => rows.length > 0,
      );
      await waitFor(() => journal.inFlightAppends, (value) => value > 0);

      // WHILE the early path is suspended in its pre-admission cancellation
      // boundary, a fresh snapshot makes the normal hedge GENUINELY admissible
      // (ask 0.04 -> pair edge 0.16, liquidity >= 100).
      clock.nowMs = START_MS + 21_000;
      await market.emitSnapshot(book(clock, "UP", 3, [{ price: "0.70", quantity: "1000" }], [{ price: "0.82", quantity: "1000" }], ERROR_SCHEDULE));
      await market.emitSnapshot(book(clock, "DOWN", 3, [{ price: "0.03", quantity: "10000" }], [{ price: "0.04", quantity: "10000" }], ERROR_SCHEDULE));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      clock.nowMs = START_MS + 22_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      releaseHold();
      journal.setAppendInFlightHoldForTest(null);

      const hedges = await waitFor(() => fills(setup, "HEDGE_SECOND_LEG"), (rows) => rows.length > 0);
      expect(hedges).toHaveLength(1);
      expect(hedges[0]?.payload).toMatchObject({ side: "BUY", outcome: "DOWN", quantity: "6.25", fillPrice: "0.04" });
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
});