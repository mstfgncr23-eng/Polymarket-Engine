import { describe, expect, it } from "vitest";
import { createId, type MarketSnapshot, type ReferenceTick } from "@pm/core";
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

function marketBook(clock: Clock, outcome: "UP" | "DOWN", sequence: number): MarketSnapshot {
  return snapshot({
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-race",
    tokenId: outcome === "UP" ? "btc-race-up" : "btc-race-down",
    outcome,
    asset: "BTC",
    bids: [{ price: "0.39", quantity: "2000" }],
    asks: [{ price: "0.40", quantity: "300" }],
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    sequence,
  });
}

function referenceTick(clock: Clock, price: string): ReferenceTick {
  return {
    eventId: createId("reference"),
    traceId: createId("trace"),
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "deterministic-test",
    price,
    quantity: "1",
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    tradeId: createId("trade"),
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function drainRuntime(engine: EngineService): Promise<void> {
  await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0, 10_000);
}

describe("native close-vs-entry linearization", () => {
  it("does not create a first-leg order when close-entries wins before a blocked trigger reaches the economic boundary", async () => {
    const clock: Clock = { nowMs: START_MS };
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([
      qualified("btc-race", "BTC", "btc-race-up", "btc-race-down", new Date(START_MS + 15 * 60_000).toISOString()),
    ]));

    const engine = new EngineService(
      qualifiedPaperEnv({
        EXPERIMENT_DURATION_HOURS: "1",
        ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
        PAPER_EXECUTION_STYLE: "TAKER",
        PAPER_EXECUTION_LATENCY_MS: "0",
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

    let releaseTrigger!: () => void;
    const triggerHold = new Promise<void>((resolve) => { releaseTrigger = resolve; });

    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);

      engine.setProcessTriggerHoldForTest(triggerHold);
      await market.emitSnapshot(marketBook(clock, "UP", 1));
      await market.emitSnapshot(marketBook(clock, "DOWN", 1));
      await engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      clock.nowMs = START_MS + 1_000;
      await engine.ingestReferenceTickForTest(referenceTick(clock, "102"));

      // At least one real trigger task must already be blocked inside the
      // runtime before the operator closes entry. This prevents a vacuous test.
      await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved > 0);

      const closed = await engine.closeExperimentEntries();
      expect(closed.ok).toBe(true);
      expect(engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");

      releaseTrigger();
      engine.setProcessTriggerHoldForTest(null);
      await drainRuntime(engine);

      const events = await journal.readAll();
      expect(events.some((event) => event.type === "TRIGGER_EVENT")).toBe(true);
      expect(events.some((event) => event.type === "GROK_ACTIVATION_DECISION")).toBe(true);
      expect(events.filter((event) => event.type === "PAPER_ORDER_CREATED")).toHaveLength(0);
      expect(events.filter((event) => event.type === "PAPER_FILL")).toHaveLength(0);
      expect(engine.ledger.positions.size).toBe(0);
      // With no economic residue, maintenance may legitimately advance the
      // already-closed experiment from ENTRY_CLOSED to COMPLETED while the
      // released trigger drains. It must never return to an entry-capable state.
      expect(engine.experimentManifest()?.status).not.toBe("RUNNING");
    } finally {
      releaseTrigger();
      engine.setProcessTriggerHoldForTest(null);
      await engine.stop();
    }
  });
});
