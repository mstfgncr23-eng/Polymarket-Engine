import {
  createId,
  grokSignalSubmissionSchema,
  triggerEventSchema,
  type MarketSnapshot,
  type ReferenceTick,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { describe, expect, it } from "vitest";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualifiedPaperEnv,
  qualified,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

async function drainRuntime(engine: EngineService): Promise<void> {
  const started = Date.now();
  while (engine.runtimeSupervisorSnapshot().unresolved !== 0) {
    if (Date.now() - started > 5_000) throw new Error("runtime drain timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function marketSnapshot(sequence: number, tokenId = "btc-up"): MarketSnapshot {
  const at = new Date(Date.parse("2026-08-31T12:00:00.000Z") + sequence).toISOString();
  return snapshot({
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId,
    outcome: tokenId === "btc-down" ? "DOWN" : "UP",
    asset: "BTC",
    sourceTimestamp: at,
    receivedAt: at,
    sequence,
  });
}

function referenceTick(sequence: number, price = "100"): ReferenceTick {
  const at = new Date(Date.parse("2026-08-31T12:00:00.000Z") + sequence * 1_000).toISOString();
  return {
    eventId: createId("reference"),
    traceId: createId("trace"),
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "binance",
    price,
    quantity: "1",
    sourceTimestamp: at,
    receivedAt: at,
    tradeId: createId("trade"),
  };
}

function monitoringTrigger(id: string, now = Date.now()): TriggerEvent {
  return triggerEventSchema.parse({
    id,
    traceId: createId("trace"),
    marketId: "btc-mkt",
    asset: "BTC",
    kind: "NEARING_EXPIRY",
    severity: 1,
    score: 1,
    dedupeKey: id,
    evidence: { source: "persistence-admission-test" },
    sourceEventIds: [createId("source")],
    source: "system",
    createdAt: new Date(now).toISOString(),
    expiresAt: new Date(now + 60_000).toISOString(),
  });
}

function fairSubmission(trigger: TriggerEvent, submissionId: string) {
  return grokSignalSubmissionSchema.parse({
    submissionId,
    traceId: trigger.traceId,
    triggerId: trigger.id,
    correlationId: createId("correlation"),
    marketId: trigger.marketId,
    asset: trigger.asset,
    agentId: "fair_probability_analyst",
    role: "fair_probability_analyst",
    timestamp: new Date().toISOString(),
    output: {
      confidence: 0.8,
      evidenceIds: trigger.sourceEventIds,
      expiresAt: trigger.expiresAt,
      reasonCodes: ["PERSISTENCE_ADMISSION_TEST"],
      direction: "UP",
      fairProbability: 0.6,
      marketProbability: 0.5,
      estimatedEdge: 0.04,
    },
  });
}

describe("persistence admission keeps live inputs in memory", () => {
  it("processes market snapshots and reference ticks without raw journal rows", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    try {
      await market.waitForHandlers();
      for (let sequence = 1; sequence <= 16; sequence += 1) await market.emitSnapshot(marketSnapshot(sequence));
      expect(await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).toEqual([]);
      expect((await engine.beginExperiment()).ok).toBe(true);
      for (let sequence = 17; sequence <= 32; sequence += 1) await market.emitSnapshot(marketSnapshot(sequence));
      for (let sequence = 1; sequence <= 32; sequence += 1) {
        await engine.ingestReferenceTickForTest(referenceTick(sequence, sequence === 2 ? "102" : "100"));
      }
      await drainRuntime(engine);

      expect(engine.state.marketSnapshots.get("btc-up")?.sequence).toBe(32);
      expect(engine.state.referenceLastMessageAt("BTC")).toBe(referenceTick(32).receivedAt);
      expect(engine.referenceWindowTotalEntriesForTest()).toBeGreaterThan(0);
      expect(engine.state.recentTriggers.some((trigger) => trigger.kind === "BTC_RAPID_MOVE")).toBe(true);
      expect(await journal.queryEvents({ types: ["MARKET_SNAPSHOT", "REFERENCE_TICK"] })).toEqual([]);
    } finally {
      await engine.stop();
    }
  });

  it("does not grow the journal during a 10k raw observation stream", async () => {
    const { engine, market, journal } = await startQualifiedEngine();
    try {
      await market.waitForHandlers();
      const before = (await journal.readAll()).length;
      for (let sequence = 1; sequence <= 5_000; sequence += 1) await market.emitSnapshot(marketSnapshot(sequence));
      for (let sequence = 1; sequence <= 5_000; sequence += 1) await engine.ingestReferenceTickForTest(referenceTick(sequence));
      await drainRuntime(engine);

      expect((await journal.readAll()).length).toBe(before);
      expect(await journal.queryEvents({ types: ["MARKET_SNAPSHOT", "REFERENCE_TICK"] })).toEqual([]);
    } finally {
      await engine.stop();
    }
  }, 30_000);

  it("admits ordinary canonical inputs only inside an experiment window", async () => {
    const { engine, journal } = await startQualifiedEngine();
    const now = Date.now();
    const idleTrigger = monitoringTrigger("idle-admission-trigger", now);
    try {
      await engine.registerTriggerForResearch(idleTrigger);
      const idleSubmission = fairSubmission(idleTrigger, "idle-admission-submission");
      const beforeRetry = (await journal.readAll()).length;
      expect(await engine.submitGrokSignal(idleSubmission, "fair_probability_analyst")).toMatchObject({ status: "accepted" });
      expect(await engine.submitGrokSignal(idleSubmission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      for (let attempt = 0; attempt < 100; attempt += 1) {
        expect(await engine.submitGrokSignal(idleSubmission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      }
      await drainRuntime(engine);
      expect(await journal.queryEvents({ types: [
        "TRIGGER_EVENT", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
      ] })).toEqual([]);
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:idle-admission-submission")).toBeNull();
      expect((await journal.readAll()).length).toBe(beforeRetry);

      expect((await engine.beginExperiment()).ok).toBe(true);
      const runningTrigger = monitoringTrigger("running-admission-trigger", now + 1_000);
      await engine.registerTriggerForResearch(runningTrigger);
      await engine.submitGrokSignal(fairSubmission(runningTrigger, "running-admission-submission"), "fair_probability_analyst");
      await drainRuntime(engine);
      const events = await journal.queryEvents({ types: [
        "TRIGGER_EVENT", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
      ] });
      expect(events.some((event) => event.type === "TRIGGER_EVENT" && event.payload.experimentId === engine.experimentManifest()?.id)).toBe(true);
      expect(events.some((event) => event.type === "GROK_ACTIVATION_DECISION")).toBe(true);
      expect(events.some((event) => event.type === "GROK_SIGNAL_ACCEPTED")).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("requires the durable accepted fact for running-experiment duplicates", async () => {
    const { engine, journal } = await startQualifiedEngine();
    const trigger = monitoringTrigger("running-duplicate-trigger");
    const runningSubmission = fairSubmission(trigger, "running-duplicate-submission");
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.registerTriggerForResearch(trigger);
      expect(await engine.submitGrokSignal(runningSubmission, "fair_probability_analyst")).toMatchObject({ status: "accepted" });
      expect(await engine.submitGrokSignal(runningSubmission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:running-duplicate-submission")).toBeTruthy();
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("boots cleanly from an empty journal without recovery artifacts", async () => {
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      expect(engine.state.recoveryComplete).toBe(true);
      expect(engine.state.experimentStatus).toBe("IDLE");
      expect(await journal.latestDurableSequence()).toBe(0);
      expect(await journal.listCheckpoints()).toEqual([]);
      expect(await journal.queryEvents({ types: ["MARKET_SNAPSHOT", "REFERENCE_TICK"] })).toEqual([]);
    } finally {
      await engine.stop();
    }
  });
});
