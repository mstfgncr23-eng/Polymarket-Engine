import { describe, expect, it } from "vitest";
import { classifyJournalRetention, classifyReportInput, createId, isProtectedJournalType, type MarketSnapshot, type PaperFill } from "@pm/core";
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

function book(clock: Clock, outcome: "UP" | "DOWN", sequence: number, bids: Array<{ price: string; quantity: string }>, asks = [{ price: "0.40", quantity: "300" }]): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return snapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-mkt", tokenId, outcome, asset: "XRP",
    bids, asks, sourceTimestamp: iso(clock), receivedAt: iso(clock), sequence,
  });
}

async function waitFor<T>(read: () => T | Promise<T>, predicate: (value: T) => boolean, timeoutMs = 3_000): Promise<T> {
  const started = Date.now();
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function setupScenario() {
  const clock: Clock = { nowMs: START_MS };
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  // Market end far enough past START that the diagnostic gates (observed up to
  // +480s) fire pre-expiry; the expiry-aware protective deadline must not turn
  // these observations into MARKET_EXPIRED rows.
  const endDate = new Date(START_MS + 2_000_000).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "XRP", "btc-up", "btc-down", endDate)]));
  const env = qualifiedPaperEnv({
    DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "0",
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
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
    fillModel: "diagnostics-test:first-leg", feeSchedule: null,
  };
}

const suppressed = (setup: Awaited<ReturnType<typeof setupScenario>>) =>
  setup.journal.queryEvents({ types: ["PROTECTIVE_DE_RISK_SUPPRESSED"] });

describe("protective de-risk diagnostic suppression observability", () => {
  it("1 control: healthy protective SELL emits NO suppression event", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 299_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "10" }]));
      // Opposite asks @0.60 make the pair-lock salvage strictly worse than the
      // protective SELL @0.45, so the old control path still selects A.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], [{ price: "0.60", quantity: "300" }]));
      // Load both books BEFORE eligibility so the post-deadline evaluation is a
      // single deterministic attempt on the intended (fresh) state.
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      await waitFor(
        () => setup.journal.queryEvents({ types: ["PAPER_FILL"] }),
        (rows) => rows.some((row) => row.payload.intent === "PROTECTIVE_DE_RISK"),
      );
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);
      const rows = await suppressed(setup);
      // The healthy path may emit pre-deadline duration or concurrent-flight
      // observability rows, but never any post-timeout snapshot/fence gate.
      // The PR3 early catastrophic gate intentionally evaluates every fresh
      // state and audibly declines until the loss cap is satisfied (declining
      // here is correct — the pair-lock would lock beyond the cap).
      expect(rows.filter((r) => r.payload.gate !== "GATE_DURATION"
        && r.payload.gate !== "GATE_IN_FLIGHT"
        && r.payload.gate !== "EARLY_CATASTROPHIC_PAIR_LOCK")).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });
it("2 records UNHEDGED_DURATION_NOT_EXCEEDED before the policy deadline", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 299_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_DURATION"));
      const row = rows.find((item) => item.payload.gate === "GATE_DURATION");
      expect(row?.payload.reason).toBe("UNHEDGED_DURATION_NOT_EXCEEDED");
      expect(Number(row?.payload.unhedgedDurationMs)).toBeGreaterThanOrEqual(299_000);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
      expect((await suppressed(setup)).length).toBeGreaterThanOrEqual(1);
    } finally {
      await setup.engine.stop();
    }
  });

  it("3 records SNAPSHOT_MISSING when no unpaired-token snapshot exists at timeout", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_SNAPSHOT_MISSING"));
      const row = rows.find((item) => item.payload.gate === "GATE_SNAPSHOT_MISSING");
      expect(row?.payload.reason).toBe("SNAPSHOT_MISSING");
      expect(row?.payload.snapshotPresent).toBe(false);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
      expect((await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).filter((r) => r.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(0);
    } finally {
      await setup.engine.stop();
    }
  });

  it("4 records TOKEN_UNSYNCHRONIZED while the opposite token keeps updating", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.40", quantity: "10" }]));
      setup.clock.nowMs = START_MS + 301_000;
      setup.market.markTokenSync("btc-down", "AWAITING_BOOK", 2);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 3, [{ price: "0.55", quantity: "300" }]));
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_TOKEN_UNSYNCHRONIZED"));
      const row = rows.find((item) => item.payload.gate === "GATE_TOKEN_UNSYNCHRONIZED");
      expect(row?.payload.reason).toBe("TOKEN_UNSYNCHRONIZED");
      expect(row?.payload.snapshotPresent).toBe(true);
      expect(row?.payload.tokenSynchronized).toBe(false);
      expect(row?.payload.tokenId).toBe("btc-down");
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("5 records SNAPSHOT_STALE when the unpaired book ages past the freshness window", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      // Store the DOWN book at +299s (blocked feed from then on) without a SELL
      // (zero bid depth -> candidate-null row, proving the book made it).
      setup.clock.nowMs = START_MS + 299_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.40", quantity: "10" }]));
      // No executable opposite asks: the A-side diagnostic must not be replaced
      // by a rescue pair-lock (PR2 selects B only when B is actually executable).
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], []));
      // Both books are fresh; step past the deadline and trigger the single
      // deterministic evaluation.
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL"));
      // A different gate (SNAPSHOT_STALE) transitions within the same 60s
      // window and must emit immediately (rate limit is per market+gate).
      setup.clock.nowMs = START_MS + 317_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_SNAPSHOT_STALE"));
      const row = rows.find((item) => item.payload.gate === "GATE_SNAPSHOT_STALE");
      expect(row?.payload.reason).toBe("SNAPSHOT_STALE");
      expect(row?.payload.snapshotPresent).toBe(true);
      expect(row?.payload.tokenSynchronized).toBe(true);
      expect(Number(row?.payload.snapshotAgeMs)).toBeGreaterThan(15_000);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("6 records NO_EXECUTABLE_BID_DEPTH when the unpaired book has no executable bids", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 299_000;
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [], [{ price: "0.40", quantity: "10" }]));
      // No executable opposite asks so the A-side suppression remains observable.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], []));
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL"));
      const row = rows.find((item) => item.payload.gate === "GATE_CANDIDATE_NULL");
      expect(row?.payload.reason).toBe("NO_EXECUTABLE_BID_DEPTH");
      expect(row?.payload.snapshotPresent).toBe(true);
      expect(row?.payload.bidsPresent).toBe(false);
      expect(row?.payload.bidDepth).toBe("0");
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("6b soak shape: fresh synchronized book with deep bids but missing minimum order size fails safely as MIN_ORDER_SIZE_UNKNOWN", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      // Reproduce experiment_d2fe2633-fd06-4d6f-a2fa-587bd4457023 market 4806624:
      // fresh snapshot, synchronized token, bestBid 0.45, ~89k bid depth, fee
      // schedule known, but minOrderSize missing on the snapshots the engine sees.
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 299_000;
      await setup.market.emitSnapshot(snapshot({
        ...book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "88961.66" }]),
        minOrderSize: null,
      }));
      // No executable opposite asks so the A-side suppression remains observable.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], []));
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL"));
      const row = rows.find((item) => item.payload.gate === "GATE_CANDIDATE_NULL");
      expect(row?.payload.reason).toBe("MIN_ORDER_SIZE_UNKNOWN");
      expect(row?.payload.snapshotPresent).toBe(true);
      expect(row?.payload.tokenSynchronized).toBe(true);
      expect(row?.payload.bidsPresent).toBe(true);
      expect(row?.payload.bestBid).toBe("0.45");
      expect(row?.payload.bidDepth).toBe("88961.66");
      expect(row?.payload.feeScheduleKnown).toBe(true);
      expect(row?.payload.minOrderSize).toBe(null);
      expect(row?.payload.candidateQuantity).toBe(null);
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("6c distinguishes executable quantity below the known minimum order size from missing depth", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      setup.clock.nowMs = START_MS + 299_000;
      // 0.5 executable shares against the fixture's known minimum order size
      // of 1: quantity exists but is below the minimum.
      await setup.market.emitSnapshot(book(setup.clock, "DOWN", 2, [{ price: "0.45", quantity: "0.5" }]));
      // No executable opposite asks so the A-side suppression remains observable.
      await setup.market.emitSnapshot(book(setup.clock, "UP", 2, [{ price: "0.55", quantity: "300" }], []));
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_CANDIDATE_NULL"));
      const row = rows.find((item) => item.payload.gate === "GATE_CANDIDATE_NULL");
      expect(row?.payload.reason).toBe("QUANTITY_BELOW_MIN_ORDER_SIZE");
      expect(row?.payload.bidsPresent).toBe(true);
      expect(row?.payload.bidDepth).toBe("0.5");
      expect(row?.payload.minOrderSize).toBe("1");
      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("7 transition: a different gate is durably recorded within the same 60s window, repeated identical gate stays bounded", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(setup.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await setup.market.emitSnapshot(book(setup.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await setup.engine.applyObservedFill({ ...firstLeg(setup.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });

      // Pre-timeout diagnostic consumes the GATE_DURATION slot at +299s.
      setup.clock.nowMs = START_MS + 299_000;
      await setup.engine.tickMaintenance();
      await waitFor(() => suppressed(setup), (list) => list.some((row) => row.payload.gate === "GATE_DURATION"));

      // Post-timeout gate (SNAPSHOT_MISSING) arrives 2s later: a different gate
      // for the same market must emit immediately, despite the 60s per-gate limit.
      setup.clock.nowMs = START_MS + 301_000;
      await setup.engine.tickMaintenance();
      const rows = await waitFor(
        () => suppressed(setup),
        (list) => list.some((row) => row.payload.gate === "GATE_DURATION") && list.some((row) => row.payload.gate === "GATE_SNAPSHOT_MISSING"),
      );
      const durationRow = rows.find((item) => item.payload.gate === "GATE_DURATION");
      const missingRow = rows.find((item) => item.payload.gate === "GATE_SNAPSHOT_MISSING");
      expect(durationRow).toBeTruthy();
      expect(missingRow).toBeTruthy();
      expect(missingRow?.payload.reason).toBe("SNAPSHOT_MISSING");
      expect(Number(missingRow?.payload.unhedgedDurationMs)).toBeGreaterThanOrEqual(301_000);
      expect(Date.parse(missingRow?.payload.at as string)).toBeGreaterThan(Date.parse(durationRow?.payload.at as string));

      // Repeated identical gate (+303s) stays bounded: no extra SNAPSHOT_MISSING row.
      setup.clock.nowMs = START_MS + 303_000;
      await setup.engine.tickMaintenance();
      await new Promise((resolve) => setTimeout(resolve, 30));
      const after = await suppressed(setup);
      expect(after.filter((item) => item.payload.gate === "GATE_SNAPSHOT_MISSING")).toHaveLength(1);

      expect(setup.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");
      expect(setup.engine.integrityFault()).toBe(false);
    } finally {
      await setup.engine.stop();
    }
  });

  it("8 retention/report classification of the suppression event", () => {
    // Report class: operational (non-canonical, excluded from the report hash).
    expect(classifyReportInput("PROTECTIVE_DE_RISK_SUPPRESSED")).toBe("operational");
    // Journal retention: purge-safe monitor_control, never unknown_retain.
    expect(classifyJournalRetention("PROTECTIVE_DE_RISK_SUPPRESSED")).toBe("monitor_control");
    expect(isProtectedJournalType("PROTECTIVE_DE_RISK_SUPPRESSED")).toBe(false);
  });
});