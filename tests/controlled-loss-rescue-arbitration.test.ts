import { describe, expect, it } from "vitest";
import {
  arbitrateRiskReduction,
  createId,
  decideRescueSecondLegOrder,
  evaluateRescueSecondLegCandidate,
  riskLimitsSchema,
  verifiedRescuePairLock,
  walkBook,
  type MarketSnapshot,
  type PaperFill,
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
import { feeSchedule, snapshot as newSnapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-01-01T00:00:00.000Z");
type Clock = { nowMs: number };
const iso = (clock: Clock): string => new Date(clock.nowMs).toISOString();
const LIMITS = riskLimitsSchema.parse({});

function book(clock: Clock, outcome: "UP" | "DOWN", sequence: number,
  bids: Array<{ price: string; quantity: string }>, asks: Array<{ price: string; quantity: string }> = []): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return newSnapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-mkt", tokenId, outcome, asset: "XRP",
    bids, asks, sourceTimestamp: iso(clock), receivedAt: iso(clock), sequence,
  });
}

function firstLeg(clock: Clock, sourceEventId: string, price = "0.82", qty = "6.09756098"): PaperFill {
  return {
    id: createId("fill-first"), orderId: createId("order-first"), traceId: createId("trace-first"),
    marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "BUY", requestedPrice: price,
    fillPrice: price, quantity: qty, liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "rescue-test:first-leg", feeSchedule,
  };
}

function hedgeFill(clock: Clock, sourceEventId: string, price = "0.12", qty = "6.09756098"): PaperFill {
  return {
    id: createId("fill-hedge"), orderId: createId("order-hedge"), traceId: createId("trace-hedge"),
    marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP", side: "BUY", requestedPrice: price,
    fillPrice: price, quantity: qty, liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "rescue-test:hedge", feeSchedule,
  };
}

function staleProtectiveFill(clock: Clock, qty: string, sourceEventId = createId("event-stale-pr")): PaperFill {
  return {
    id: createId("fill-stale-pr"), orderId: createId("order-stale-pr"), traceId: createId("trace-stale-pr"),
    marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "SELL", requestedPrice: "0.75",
    fillPrice: "0.75", quantity: qty, liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "rescue-test:stale-protective", feeSchedule,
  };
}

function staleRescueFill(clock: Clock, qty: string, sourceEventId = createId("event-stale-r")): PaperFill {
  return {
    id: createId("fill-stale-r"), orderId: createId("order-stale-r"), traceId: createId("trace-stale-r"),
    marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP", side: "BUY", requestedPrice: "0.20",
    fillPrice: "0.20", quantity: qty, liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "rescue-test:stale-rescue", feeSchedule,
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

/** De-risk deadline for a position opened at START_MS+1 in a market ending at START_MS+endOffset. */
function eligibilityMs(endOffsetMs: number): number {
  return Math.min(START_MS + 1 + LIMITS.maxUnhedgedDurationMs, START_MS + endOffsetMs - 80_000);
}

describe("controlled-loss rescue arbitration (PR2 — engine)", { timeout: 30_000 }, () => {
  it("case 4962603: SELL @0.75 vs UP ask @0.30 -> PROTECTIVE_DE_RISK wins (A)", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.09756098");

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      // Observe both books fresh (UP first so the DOWN-triggered rescue run
      // prices B against the 0.30 ladder, not the initial 0.59 one).
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.30", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.75", quantity: "1000" }], []));

      const rows = await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (list) => list.length > 0);
      expect(rows[0]?.payload).toMatchObject({ fillPrice: "0.75" });
      expect(setup.engine.ledger.positions.get("btc-mkt")?.downQuantity ?? "0").toBe("0");
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      const evaluation = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .find((row) => row.payload.deRisk?.selection !== undefined);
      expect(evaluation?.payload.deRisk).toMatchObject({
        selection: "PROTECTIVE_DE_RISK", selectionReason: "A_STRICTLY_BETTER",
      });
      // The 120s market's expiry deadline (marketEnd - 80s) is the binding
      // bound at +40s; the 5-minute duration bound is not yet reached.
      expect(evaluation?.payload.deRisk?.eligibilityReason).toBe("PRE_EXPIRY_SAFETY");
      expect(Number(evaluation?.payload.deRisk?.aGuaranteedSalvageUsd)).toBeGreaterThan(
        Number(evaluation?.payload.deRisk?.bGuaranteedSalvageUsd ?? "-99"),
      );
    } finally {
      await setup.engine.stop();
    }
  });
it("B-wins: opposite UP ask @0.20 -> RESCUE_SECOND_LEG pairs the full residual", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      // Emit the OPPOSITE book first so the arbiter observes the freshest UP
      // ask ladder (0.20) when the DOWN snapshot triggers the rescue run.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.20", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.75", quantity: "1000" }], []));

      const rows = await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (list) => list.length > 0);
      expect(rows[0]?.payload).toMatchObject({ side: "BUY", outcome: "UP" });
      const position = setup.engine.ledger.positions.get("btc-mkt");
      expect(position?.pairedQuantity).toBe("6.09756098");
      expect(position?.unpairedQuantity).toBe("0");
      expect(position?.upQuantity).toBe("6.09756098");
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      // Post-pair: further snapshots must no-op for both protective and rescue.
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 3, [{ price: "0.75", quantity: "1000" }], []));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [], [{ price: "0.20", quantity: "1000" }]));
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(1);
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("A-no-depth / B-available -> selects RESCUE_SECOND_LEG", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      // Held DOWN: NO bids. Opposite UP: executable asks (fresh first).
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.25", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], []));

      await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (list) => list.length > 0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.pairedQuantity).toBe("6.09756098");
    } finally {
      await setup.engine.stop();
    }
  });

  it("B-no-depth / A-available -> selects PROTECTIVE_DE_RISK", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.75", quantity: "1000" }], []));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], []));

      await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (list) => list.length > 0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.downQuantity ?? "0").toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });
it("neither executable -> no economic mutation, bounded suppression, residual kept", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      // Both books empty; emit UP first so the DOWN-triggered run sees an empty
      // opposite ladder too (the held DOWN book is already empty).
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], []));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], []));

      const rows = await waitFor(
        () => suppressed(setup),
        (list) => list.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL"),
      );
      expect(rows.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL")).toBe(true);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.09756098");
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("insufficient cash -> B unavailable (INSUFFICIENT_CASH), A selected", async () => {
    const setup = await setupScenario(120_000, { PAPER_START_BALANCE: "1" });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId, "0.40", "2"), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.39", quantity: "1000" }], []));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.25", quantity: "1000" }]));

      const rows = await waitFor(() => fills(setup, "PROTECTIVE_DE_RISK"), (list) => list.length > 0);
      expect(rows.length).toBeGreaterThan(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("pre-eligibility normal hedge wins as V1; rescue never interferes before eligibility", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = START_MS + 10_000;
      await setup.engine.applyObservedFill(hedgeFill(setup.clock, createId("event-hedge")), "HEDGE_SECOND_LEG");
      const position = setup.engine.ledger.positions.get("btc-mkt");
      expect(position?.pairedQuantity).toBe("6.09756098");
      expect(position?.unpairedQuantity).toBe("0");
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("RESCUE fence: stale protective/rescue fills cannot double-consume residual", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.20", quantity: "1000" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], []));
      await waitFor(() => fills(setup, "RESCUE_SECOND_LEG"), (list) => list.length > 0);

      await expect(setup.engine.applyObservedFill(
        staleProtectiveFill(setup.clock, "4"),
        "PROTECTIVE_DE_RISK",
      )).rejects.toThrow("NO_UNPAIRED_RESIDUAL");
      await expect(setup.engine.applyObservedFill(
        staleRescueFill(setup.clock, "2"),
        "RESCUE_SECOND_LEG",
      )).rejects.toThrow("NO_UNPAIRED_RESIDUAL");
      expect(setup.engine.integrityFault()).toBe(false);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await setup.engine.stop();
    }
  });

  it("market expiry during rescue -> final fence blocks the mutation", async () => {
    const setup = await setupScenario(120_000, { PAPER_EXECUTION_LATENCY_MS: "800" });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      const timeoutBook = book(setup.clock, "DOWN", 2, [{ price: "0.75", quantity: "1000" }]);
      let releaseHold: (() => void) | null = null;
      const hold = new Promise<void>((resolve) => { releaseHold = resolve; });
      setup.journal.setAppendInFlightHoldForTest(hold, ["PAPER_ORDER_CREATED"]);
      try {
        await setup.market.emitSnapshot(timeoutBook);
        await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [], [{ price: "0.30", quantity: "1000" }]));
        await waitFor(() => setup.journal.inFlightAppends, (value) => value > 0);
        setup.clock.nowMs = START_MS + 121_000; // market end passed
      } finally {
        releaseHold?.();
        setup.journal.setAppendInFlightHoldForTest(null);
      }
      await waitFor(
        () => setup.journal.queryEvents({ types: ["PAPER_EXECUTION_ERROR"] }),
        (rows) => rows.some((row) => row.payload.stateChanged === true
          && (row.payload.intent === "PROTECTIVE_DE_RISK" || row.payload.intent === "RESCUE_SECOND_LEG")),
      );
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.09756098");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("stale snapshots fail closed for both A and B (no mutation)", async () => {
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(setup.clock, "DOWN", 1, []);
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      setup.clock.nowMs = eligibilityMs(120_000) + 1;
      const staleReceivedAt = new Date(setup.clock.nowMs - 60_000).toISOString();
      await setup.market.emitSnapshot(newSnapshot({
        ...book(setup.clock, "DOWN", 2, [{ price: "0.75", quantity: "1000" }], []),
        receivedAt: staleReceivedAt, sourceTimestamp: staleReceivedAt,
      }));
      await setup.market.emitSnapshot(newSnapshot({
        ...book(setup.clock, "UP", 2, [], [{ price: "0.25", quantity: "1000" }]),
        receivedAt: staleReceivedAt, sourceTimestamp: staleReceivedAt,
      }));

      const rows = await waitFor(
        () => suppressed(setup),
        (list) => list.some((row) => row.payload.gate === "GATE_SNAPSHOT_STALE"),
      );
      expect(rows.some((row) => row.payload.gate === "GATE_SNAPSHOT_STALE")).toBe(true);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("6.09756098");
      expect((await fills(setup, "PROTECTIVE_DE_RISK"))).toHaveLength(0);
      expect((await fills(setup, "RESCUE_SECOND_LEG"))).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("restart after rescue B preserves paired/unpaired/cash economics (exactly-once fill)", async () => {
    const journal = new MemoryEventJournal();
    const clock: Clock = { nowMs: START_MS };
    const endDate = new Date(START_MS + 120_000).toISOString();
    const envFactory = () => qualifiedPaperEnv({
      DISCOVERY_INTERVAL_MS: "3600000", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
      PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0",
    });
    const make = (env: ReturnType<typeof envFactory>) => {
      const market = new ScriptedMarketAdapter();
      market.queue(discoveryReport([qualified("btc-mkt", "XRP", "btc-up", "btc-down", endDate)]));
      return {
        engine: new EngineService(env, journal, { error: () => undefined }, {
          market: market as never,
          referenceRegistry: new ScriptedReferenceRegistry() as never,
          activation: new NoopGrokActivationBridge(),
          now: () => new Date(clock.nowMs),
          resolutionFetcher: async () => null,
        }),
        market,
      };
    };

    const envA = envFactory();
    const first = make(envA);
    try {
      await first.engine.start();
      await first.market.waitForHandlers();
      expect((await first.engine.beginExperiment()).ok).toBe(true);
      await first.market.emitSnapshot(book(clock, "UP", 1, [], [{ price: "0.59", quantity: "1000" }]));
      const downBook = book(clock, "DOWN", 1, []);
      await first.engine.applyObservedFill({ ...firstLeg(clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      clock.nowMs = eligibilityMs(120_000) + 1;
      await first.market.emitSnapshot(book(clock, "UP", 2, [], [{ price: "0.20", quantity: "1000" }]));
      await first.market.emitSnapshot(book(clock, "DOWN", 2, [], []));
      await waitFor(() => fills({ engine: first.engine, journal, market: first.market, clock }, "RESCUE_SECOND_LEG"), (list) => list.length > 0);
      expect(first.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
      const cashBefore = first.engine.ledger.snapshot().cash;
      expect(Number(cashBefore)).toBeLessThan(Number(qualifiedPaperEnv({}).PAPER_START_BALANCE ?? 1000) + 1);
    } finally {
      await first.engine.stop();
    }

    // Restart on the same journal: ledger state must replay deterministically.
    const envB = envFactory();
    const recovered = make(envB);
    try {
      await recovered.engine.start();
      await recovered.market.waitForHandlers();
      const position = recovered.engine.ledger.positions.get("btc-mkt");
      expect(position?.pairedQuantity).toBe("6.09756098");
      expect(position?.unpairedQuantity).toBe("0");
      expect(position?.upQuantity).toBe("6.09756098");
      expect(position?.downQuantity).toBe("6.09756098");
      const rows = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 20 });
      // first-leg + rescue = exactly two durable PAPER_FILL transitions.
      expect(rows.filter((row) => row.operationKey.startsWith("fill:")).length).toBe(2);
      expect(recovered.engine.integrityFault()).toBe(false);
    } finally {
      await recovered.engine.stop();
    }
  });
});
describe("controlled-loss rescue core (unit)", () => {
  const clock = { nowMs: START_MS };
  const POSE = new Date("2026-01-01T00:00:05.000Z");
  const held = (qty = "6.09756098") => ({
    id: "pos-btc", marketId: "btc-mkt", upQuantity: "0", downQuantity: qty,
    averageUpCost: "0", averageDownCost: "0.82", pairedQuantity: "0",
    unpairedOutcome: "DOWN" as const, unpairedQuantity: qty,
    unhedgedStartedAt: iso(clock), combinedPairCost: "0.82", totalFees: "0", totalSlippage: "0",
    openedAt: iso(clock), updatedAt: iso(clock),
  });

  it("candidate B: opposite BUY capped by residual and depth; rejects wrong outcome", () => {
    const position = held();
    const up = book(clock, "UP", 1, [], [{ price: "0.20", quantity: "3" }, { price: "0.22", quantity: "5" }]);
    const evaluation = evaluateRescueSecondLegCandidate({ position, snapshot: up, maxOrderSizeUsd: LIMITS.maxOrderSizeUsd });
    expect(evaluation.reason).toBeNull();
    expect(evaluation.candidate).not.toBeNull();
    expect(evaluation.candidate!.intent).toBe("RESCUE_SECOND_LEG");
    expect(evaluation.candidate!.side).toBe("BUY");
    expect(Number(evaluation.candidate!.quantity)).toBeLessThanOrEqual(Number(position.unpairedQuantity));
    const down = book(clock, "DOWN", 1, [{ price: "0.75", quantity: "100" }], []);
    expect(evaluateRescueSecondLegCandidate({ position, snapshot: down, maxOrderSizeUsd: LIMITS.maxOrderSizeUsd }).reason)
      .toBe("OPPOSITE_OUTCOME_UNRESOLVABLE");
  });
it("decideRescueSecondLegOrder: salvage = q - notional - fee (TAKER ask walk)", () => {
    const position = held();
    const up = book(clock, "UP", 1, [], [{ price: "0.20", quantity: "1000" }]);
    const candidate = evaluateRescueSecondLegCandidate({ position, snapshot: up, maxOrderSizeUsd: LIMITS.maxOrderSizeUsd }).candidate!;
    const rescuePosition = {
      marketId: position.marketId, unpairedOutcome: position.unpairedOutcome,
      unpairedQuantity: position.unpairedQuantity, expectedOppositeTokenId: "btc-up",
    };
    const decision = decideRescueSecondLegOrder({
      candidate, snapshotAfterDecisionLatency: up, limits: LIMITS,
      traceId: "trace-r", triggerId: "trigger-r", rescuePosition,
      baseRisk: baseRiskFor(up, POSE),
    });
    expect(decision.accepted).toBe(true);
    expect(decision.order?.orderType).toBe("TAKER");
    expect(decision.guaranteedSalvageUsd).not.toBeNull();
    const walk = walkBook(up.asks, candidate.quantity, candidate.limitPrice, "BUY");
    expect(Number(decision.guaranteedSalvageUsd)).toBeCloseTo(
      Number(candidate.quantity) - Number(walk.notional) - Number(decision.feeUsdTotal ?? 0), 8,
    );
  });

  it("verifiedRescuePairLock: fails closed on wrong token/outcome/over-residual/non-decrease", () => {
    const good = {
      rescueIntent: true,
      candidate: { marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP" as const, side: "BUY" as const, quantity: "6" },
      position: {
        marketId: "btc-mkt", unpairedOutcome: "DOWN" as const, unpairedQuantity: "6.09756098",
        expectedOppositeTokenId: "btc-up",
      },
      projectedDirectionalExposureUsd: "5", directionalExposureUsd: "5",
      projectedUnhedgedExposureUsd: "0.09", unhedgedExposureUsd: "5",
    };
    expect(verifiedRescuePairLock(good)).toBe(true);
    expect(verifiedRescuePairLock({ ...good, rescueIntent: false })).toBe(false);
    expect(verifiedRescuePairLock({ ...good, candidate: { ...good.candidate, tokenId: "btc-up-old" } })).toBe(false);
    expect(verifiedRescuePairLock({ ...good, candidate: { ...good.candidate, outcome: "DOWN" } })).toBe(false);
    expect(verifiedRescuePairLock({ ...good, candidate: { ...good.candidate, quantity: "7" } })).toBe(false);
    expect(verifiedRescuePairLock({ ...good, projectedUnhedgedExposureUsd: "5" })).toBe(false); // strict decrease
  });

  it("arbitrateRiskReduction: deterministic A/tie/B and NEITHER", () => {
    const a = (salvage: string | null) => ({
      kind: "PROTECTIVE_DE_RISK" as const, accepted: salvage !== null, rejectionReason: salvage === null ? "depth" : null,
      quantity: "6", vwap: "0.75", feeUsd: "0", guaranteedSalvageUsd: salvage,
    });
    const b = (salvage: string | null) => ({
      kind: "RESCUE_SECOND_LEG" as const, accepted: salvage !== null, rejectionReason: salvage === null ? "depth" : null,
      quantity: "6", vwap: "0.20", feeUsd: "0", guaranteedSalvageUsd: salvage,
    });
    expect(arbitrateRiskReduction(a("4.5"), b("4.8"))).toMatchObject({ selection: "RESCUE_SECOND_LEG", tieBreakUsed: false });
    expect(arbitrateRiskReduction(a("4.5"), b("4.5"))).toMatchObject({ selection: "PROTECTIVE_DE_RISK", tieBreakUsed: true, selectionReason: "A_PREFERRED_TIE" });
    expect(arbitrateRiskReduction(a("4.5"), b("4.0"))).toMatchObject({ selection: "PROTECTIVE_DE_RISK" });
    expect(arbitrateRiskReduction(a("4.5"), b(null))).toMatchObject({ selection: "PROTECTIVE_DE_RISK", selectionReason: "A_ONLY" });
    expect(arbitrateRiskReduction(a(null), b("4.8"))).toMatchObject({ selection: "RESCUE_SECOND_LEG", selectionReason: "B_ONLY" });
    expect(arbitrateRiskReduction(a(null), b(null))).toMatchObject({ selection: "NONE", selectionReason: "NEITHER_AVAILABLE" });
  });
});

function baseRiskFor(snapshot: MarketSnapshot, now: Date) {
  return {
    now, marketFeedAt: new Date(snapshot.receivedAt), btcFeedAt: now, asset: "XRP",
    assetExposureUsd: "0", databaseAvailable: true, timestampDisorder: false, riskStateAvailable: true,
    dailyPnlUsd: "0", drawdownUsd: "0", marketExposureUsd: "0", totalExposureUsd: "0",
    directionalExposureUsd: "5", unhedgedExposureUsd: "5", unhedgedDurationMs: 0,
    projectedMarketExposureUsd: "0", projectedTotalExposureUsd: "0", projectedDirectionalExposureUsd: "5",
    projectedUnhedgedExposureUsd: "0", projectedAssetExposureUsd: "0", availableCashUsd: "1000",
    referenceProviderBlocked: false, marketEntryQualified: true, externalSignalsLastMinute: 0,
    lastStrategyTradeAt: null, duplicateTrade: false, experimentEntryOpen: true,
    executionErrorsLastHour: 0, externalRiskRecommendedVeto: false, shuttingDown: false,
  };
}