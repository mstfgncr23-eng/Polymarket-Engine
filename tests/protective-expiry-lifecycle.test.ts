import { describe, expect, it } from "vitest";
import { createId, type MarketSnapshot, type PaperFill } from "@pm/core";
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
import { snapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-01-01T00:00:00.000Z");

type Clock = { nowMs: number };

function iso(clock: Clock): string { return new Date(clock.nowMs).toISOString(); }

function book(
  clock: Clock,
  outcome: "UP" | "DOWN",
  sequence: number,
  bids: Array<{ price: string; quantity: string }>,
  asks = [{ price: "0.40", quantity: "300" }],
): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return snapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-mkt", tokenId, outcome, asset: "XRP",
    bids, asks, sourceTimestamp: iso(clock), receivedAt: iso(clock), sequence,
  });
}

async function waitFor<T>(read: () => T | Promise<T>, predicate: (value: T) => boolean, timeoutMs = 4_000): Promise<T> {
  const started = Date.now();
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function setupScenario(endOffsetMs: number, env: Record<string, string> = {}) {
  const clock: Clock = { nowMs: START_MS };
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + endOffsetMs).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "XRP", "btc-up", "btc-down", endDate)]));
  const engine = new EngineService(qualifiedPaperEnv({
    DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "0",
    ...env,
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

function firstLeg(clock: Clock, sourceEventId: string): PaperFill {
  return {
    id: createId("fill-first"), orderId: createId("order-first"), traceId: createId("trace-first"),
    marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "BUY", requestedPrice: "0.50",
    fillPrice: "0.50", quantity: "10", liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "expiry-test:first-leg", feeSchedule: null,
  };
}

const suppressed = (setup: Awaited<ReturnType<typeof setupScenario>>) =>
  setup.journal.queryEvents({ types: ["PROTECTIVE_DE_RISK_SUPPRESSED"] });

const protectiveFills = (setup: Awaited<ReturnType<typeof setupScenario>>) =>
  setup.journal.queryEvents({ types: ["PAPER_FILL"] }).then((rows) => rows.filter((row) => row.payload.intent === "PROTECTIVE_DE_RISK"));

describe("expiry-aware protective de-risk deadline", () => {
it("A: max unhedged duration remains authoritative when expiry is farther away", async () => {
    const setup = await setupScenario(3_600_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      // One second before the 5-minute max-unhedged deadline the attempt is
      // deferred by the duration bound even though the market is far from end.
      setup.clock.nowMs = START_MS + 299_000;
      await setup.engine.tickMaintenance();
      const durationRows = await waitFor(() => suppressed(setup), (list) =>
        list.some((row) => row.payload.gate === "GATE_DURATION" && row.payload.reason === "UNHEDGED_DURATION_NOT_EXCEEDED"));
      const durationRow = durationRows.find((row) => row.payload.reason === "UNHEDGED_DURATION_NOT_EXCEEDED");
      expect(durationRow?.payload.effectiveProtectiveDeadlineMs).toBe(START_MS + 300_000);
      expect(durationRow?.payload.marketEndMs).toBe(START_MS + 3_600_000);
      expect((await protectiveFills(setup))).toHaveLength(0);

      // Past the deadline with a fresh synchronized book: protective SELL
      // executes while the market is comfortably pre-expiry.
      setup.clock.nowMs = START_MS + 301_000;
      // Opposite asks @0.60 make the pair-lock salvage strictly worse than the
      // protective SELL @0.45; emit UP first so the DOWN-triggered rescue run
      // observes the intended (fresh) opposite ladder.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], [{ price: "0.60", quantity: "300" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "10" }]));
      const rows = await waitFor(() => protectiveFills(setup), (list) => list.length === 1);
      expect(rows[0]?.payload).toMatchObject({ side: "SELL", quantity: "10", intent: "PROTECTIVE_DE_RISK" });
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);
      expect((await suppressed(setup)).some((row) => row.payload.reason === "MARKET_EXPIRED")).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });
it("B: expiry bound forces eligibility BEFORE market end when less than maxUnhedgedDuration remains", async () => {
    // Market ends 120s after the position opens; the safety window (80s) makes
    // the effective deadline marketEnd - 80s = +40s instead of +300s.
    const setup = await setupScenario(120_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      // +39s: before the +40s expiry-derived deadline, still deferred.
      setup.clock.nowMs = START_MS + 39_000;
      await setup.engine.tickMaintenance();
      await waitFor(() => suppressed(setup), (list) =>
        list.some((row) => row.payload.gate === "GATE_DURATION" && row.payload.reason === "UNHEDGED_DURATION_NOT_EXCEEDED"));
      expect((await protectiveFills(setup))).toHaveLength(0);

      // +41s: eligible under the expiry bound, far before the 5-minute duration.
      setup.clock.nowMs = START_MS + 41_000;
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], [{ price: "0.60", quantity: "300" }]));
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "10" }]));
      const rows = await waitFor(() => protectiveFills(setup), (list) => list.length === 1);
      expect(rows[0]?.payload).toMatchObject({ side: "SELL", quantity: "10", intent: "PROTECTIVE_DE_RISK" });
      // Executed before the market's usable lifetime ended.
      expect(Date.parse(rows[0]!.occurredAt)).toBeLessThan(START_MS + 120_000);
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });
it("C: entry extremely close to expiry never executes after market end; skips deterministically", async () => {
    // Market ends 60s after the position opens; the expiry-derived deadline is
    // negative, so the position is immediately eligible — but after market end
    // the attempt must deterministically fail as MARKET_EXPIRED with no SELL.
    const setup = await setupScenario(60_000);
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      // Post-expiry fresh book: eligibility is barred and the skip is durable.
      setup.clock.nowMs = START_MS + 61_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "10" }]));
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }]));
      const rows = await waitFor(() => suppressed(setup), (list) =>
        list.some((row) => row.payload.gate === "GATE_DURATION" && row.payload.reason === "MARKET_EXPIRED"));
      const row = rows.find((item) => item.payload.reason === "MARKET_EXPIRED");
      expect(row?.payload.effectiveProtectiveDeadlineMs).toBe(START_MS - 20_000);
      expect(row?.payload.marketEndMs).toBe(START_MS + 60_000);
      expect((await protectiveFills(setup))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("D: market expiry while the protective candidate is prepared is stopped by the final fence", async () => {
    const setup = await setupScenario(120_000, { PAPER_EXECUTION_LATENCY_MS: "800" });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      // Eligible (+80s, deadline +40s) candidate is admitted; hold its
      // PAPER_ORDER_CREATED append while the clock crosses market end (+120s).
      setup.clock.nowMs = START_MS + 80_000;
      const timeoutBook = book(setup.clock, "DOWN", 2, [{ price: "0.50", quantity: "10" }]);
      let releaseHold: (() => void) | null = null;
      const hold = new Promise<void>((resolve) => { releaseHold = resolve; });
      setup.journal.setAppendInFlightHoldForTest(hold, ["PAPER_ORDER_CREATED"]);
      try {
        await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], [{ price: "0.60", quantity: "300" }]));
        await setup.market.emitSnapshot(timeoutBook);
        await waitFor(() => setup.journal.inFlightAppends, (value) => value > 0);
        // The market expires while the order is still pending.
        setup.clock.nowMs = START_MS + 121_000;
      } finally {
        releaseHold?.();
        setup.journal.setAppendInFlightHoldForTest(null);
      }

      await waitFor(
        () => setup.journal.queryEvents({ types: ["PAPER_EXECUTION_ERROR"] }),
        (rows) => rows.some((row) => row.payload.intent === "PROTECTIVE_DE_RISK" && row.payload.stateChanged === true),
      );
      const errorRows = await setup.journal.queryEvents({ types: ["PAPER_EXECUTION_ERROR"] });
      const error = errorRows.find((row) => row.payload.intent === "PROTECTIVE_DE_RISK");
      expect(error?.payload.message).toContain("MARKET_EXPIRED");
      expect(error?.payload.deRisk).toMatchObject({ finalState: "STATE_CHANGED" });

      // No protective economic mutation may exist after the expiry fence.
      expect((await protectiveFills(setup))).toHaveLength(0);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      await waitFor(() => suppressed(setup), rows => rows.some(row => row.payload.gate === "FINAL_FENCE" && row.payload.reason === "MARKET_EXPIRED"));
      const fence = (await suppressed(setup)).find((row) => row.payload.gate === "FINAL_FENCE");
      expect(fence?.payload.reason).toBe("MARKET_EXPIRED");
      expect(setup.engine.integrityFault()).toBe(false);
      expect(setup.engine.overloadState()).not.toBe("FAIL_CLOSED");
    } finally {
      await setup.engine.stop();
    }
  });
});
