import { describe, expect, it } from "vitest";
import {
  createId,
  triggerEventSchema,
  type MarketSnapshot,
  type ReferenceTick,
  type TriggerEvent,
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
import { snapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-09-18T01:00:00.000Z");

type Clock = { nowMs: number };

function marketBook(
  clock: Clock,
  outcome: "UP" | "DOWN",
  sequence: number,
  bid: string,
  ask: string,
): MarketSnapshot {
  return snapshot({
    eventId: `book-${outcome.toLowerCase()}-${sequence}`,
    traceId: `trace-book-${outcome.toLowerCase()}-${sequence}`,
    marketId: "btc-revalidate",
    tokenId: outcome === "UP" ? "btc-revalidate-up" : "btc-revalidate-down",
    outcome,
    asset: "BTC",
    bids: [{ price: bid, quantity: "100" }],
    asks: [{ price: ask, quantity: "100" }],
    sourceTimestamp: new Date(clock.nowMs).toISOString(),
    receivedAt: new Date(clock.nowMs).toISOString(),
    sequence,
  });
}

function referenceTick(clock: Clock, price: string): ReferenceTick {
  return {
    eventId: createId("ref"),
    traceId: createId("trace"),
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "deterministic-test",
    price,
    quantity: "1",
    sourceTimestamp: new Date(clock.nowMs).toISOString(),
    receivedAt: new Date(clock.nowMs).toISOString(),
    tradeId: createId("trade"),
  };
}

function manualTrigger(clock: Clock, suffix: string): TriggerEvent {
  return triggerEventSchema.parse({
    id: `trigger-${suffix}`,
    traceId: `trace-${suffix}`,
    marketId: "btc-revalidate",
    asset: "BTC",
    source: "system",
    kind: "BOOK_IMBALANCE",
    severity: 2,
    score: 1,
    dedupeKey: `dedupe-${suffix}`,
    evidence: { test: "native-entry-revalidation" },
    sourceEventIds: [`source-${suffix}`],
    evidenceIds: [`evidence-${suffix}`],
    createdAt: new Date(clock.nowMs).toISOString(),
    expiresAt: new Date(clock.nowMs + 30_000).toISOString(),
  });
}

async function waitFor<T>(
  read: () => Promise<T>,
  predicate: (value: T) => boolean,
  timeoutMs = 2_000,
): Promise<T> {
  const started = Date.now();
  for (;;) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function setup() {
  const clock = { nowMs: START_MS };
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([
    qualified(
      "btc-revalidate",
      "BTC",
      "btc-revalidate-up",
      "btc-revalidate-down",
      new Date(START_MS + 5 * 60_000).toISOString(),
    ),
  ]));
  const engine = new EngineService(
    qualifiedPaperEnv({
      EXPERIMENT_DURATION_HOURS: "1",
      PAPER_EXECUTION_STYLE: "TAKER",
      PAPER_EXECUTION_LATENCY_MS: "0",
      ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    }),
    journal,
    { error: () => undefined },
    {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(clock.nowMs),
    },
  );
  await engine.start();
  await market.waitForHandlers();

  // Warm synchronized state before the experiment so automatically generated
  // precursor triggers cannot mutate the paper ledger.
  await market.emitSnapshot(marketBook(clock, "UP", 1, "0.58", "0.60"));
  await market.emitSnapshot(marketBook(clock, "DOWN", 1, "0.40", "0.42"));
  await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
  clock.nowMs += 1_000;
  await engine.ingestReferenceTickForTest(referenceTick(clock, "100.30"));
  expect((await engine.beginExperiment()).ok).toBe(true);

  return { clock, journal, market, engine };
}

describe("native first-leg execution-time revalidation", () => {
  it("blocks the original trigger when current truth no longer confirms its direction", async () => {
    const test = await setup();
    let release: (() => void) | null = null;
    try {
      const trigger = manualTrigger(test.clock, "direction-flip");
      const hold = new Promise<void>((resolve) => { release = resolve; });
      test.engine.setNativeEntryRevalidationHoldForTest(hold);

      const processing = test.engine.registerTriggerForResearch(trigger);
      const consensusRows = await waitFor(
        () => test.journal.queryEvents({ types: ["CONSENSUS_DECISION"] }),
        (rows) => rows.some((row) => row.traceId === trigger.traceId),
      );
      const observation = consensusRows.find((row) => row.traceId === trigger.traceId);
      expect(observation?.payload.consensusDirection).toBe("UP");

      // Move the binary pair far enough that the still-positive reference
      // evidence now points toward the lower-probability side. PR-1 makes the
      // fresh native direction FLAT, so PR-3 must reject the stale UP decision.
      await test.market.emitSnapshot(marketBook(test.clock, "UP", 2, "0.17", "0.19"));
      await test.market.emitSnapshot(marketBook(test.clock, "DOWN", 2, "0.81", "0.83"));

      release();
      release = null;
      test.engine.setNativeEntryRevalidationHoldForTest(null);
      await processing;

      const revalidation = (await test.journal.queryEvents({ types: ["NATIVE_ENTRY_REVALIDATION"] }))
        .find((row) => row.traceId === trigger.traceId);
      expect(revalidation?.payload).toMatchObject({
        version: "native-entry-revalidation-v1",
        status: "REJECTED",
        reason: "DIRECTION_NOT_CONFIRMED",
        observationDirection: "UP",
        currentDirection: "FLAT",
        currentUpEventId: "book-up-2",
        currentDownEventId: "book-down-2",
      });
      expect((await test.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => row.traceId === trigger.traceId)).toHaveLength(0);
      expect((await test.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] }))
        .filter((row) => row.traceId === trigger.traceId)).toHaveLength(0);
    } finally {
      release?.();
      test.engine.setNativeEntryRevalidationHoldForTest(null);
      await test.engine.stop();
    }
  });

  it("feeds risk the revalidated fair/prior when the direction remains confirmed", async () => {
    const test = await setup();
    let release: (() => void) | null = null;
    try {
      const trigger = manualTrigger(test.clock, "same-direction");
      const hold = new Promise<void>((resolve) => { release = resolve; });
      test.engine.setNativeEntryRevalidationHoldForTest(hold);

      const processing = test.engine.registerTriggerForResearch(trigger);
      const consensusRows = await waitFor(
        () => test.journal.queryEvents({ types: ["CONSENSUS_DECISION"] }),
        (rows) => rows.some((row) => row.traceId === trigger.traceId),
      );
      const observation = consensusRows.find((row) => row.traceId === trigger.traceId);
      const observedMarketProbability = Number(observation?.payload.marketProbability);
      expect(observation?.payload.consensusDirection).toBe("UP");
      expect(observedMarketProbability).toBeCloseTo(0.59, 8);

      // Same UP thesis, materially newer market prior.
      await test.market.emitSnapshot(marketBook(test.clock, "UP", 2, "0.68", "0.70"));
      await test.market.emitSnapshot(marketBook(test.clock, "DOWN", 2, "0.30", "0.32"));

      release();
      release = null;
      test.engine.setNativeEntryRevalidationHoldForTest(null);
      await processing;

      const riskRows = await waitFor(
        () => test.journal.queryEvents({ types: ["RISK_EVALUATION"] }),
        (rows) => rows.some((row) => row.traceId === trigger.traceId),
      );
      const risk = riskRows.find((row) => row.traceId === trigger.traceId);
      const alpha = risk?.payload.alpha as Record<string, unknown> | undefined;
      const revalidation = alpha?.entryRevalidation as Record<string, unknown> | undefined;

      expect(Number(alpha?.marketProbability)).toBeCloseTo(0.69, 8);
      expect(Number(alpha?.fairProbability)).toBeGreaterThan(Number(alpha?.marketProbability));
      expect(revalidation).toMatchObject({
        version: "native-entry-revalidation-v1",
        status: "CONFIRMED",
        observationDirection: "UP",
        direction: "UP",
        upEventId: "book-up-2",
        downEventId: "book-down-2",
      });
      expect(Number(revalidation?.observationMarketProbability)).toBeCloseTo(0.59, 8);
      expect(Number(revalidation?.marketProbability)).toBeCloseTo(0.69, 8);
    } finally {
      release?.();
      test.engine.setNativeEntryRevalidationHoldForTest(null);
      await test.engine.stop();
    }
  });
});
