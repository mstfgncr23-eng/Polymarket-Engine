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

async function waitForJournalAppend(journal: MemoryEventJournal): Promise<void> {
  const started = Date.now();
  while (journal.inFlightAppends === 0) {
    if (Date.now() - started > 1_000) throw new Error("journal append did not start");
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

  it("suppresses concurrent idle retries without creating a durable claim", async () => {
    const { engine, journal } = await startQualifiedEngine();
    let release!: () => void;
    try {
      const trigger = monitoringTrigger("idle-concurrent-trigger");
      const idleSubmission = fairSubmission(trigger, "idle-concurrent-submission");
      await engine.registerTriggerForResearch(trigger);
      engine.setGrokClaimHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
      const first = engine.submitGrokSignal(idleSubmission, "fair_probability_analyst");
      const second = engine.submitGrokSignal(idleSubmission, "fair_probability_analyst");
      release();
      const results = await Promise.all([first, second]);
      expect(results.map((result) => result.status).sort()).toEqual(["accepted", "duplicate"]);
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:idle-concurrent-submission")).toBeNull();
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toEqual([]);
    } finally {
      release?.();
      engine.setGrokClaimHoldForTest(null);
      await engine.stop();
    }
  });

  it("promotes an in-flight idle submission before active canonical persistence", async () => {
    const { engine, journal } = await startQualifiedEngine();
    let release!: () => void;
    try {
      const trigger = monitoringTrigger("idle-boundary-trigger");
      const idleSubmission = fairSubmission(trigger, "idle-boundary-submission");
      await engine.registerTriggerForResearch(trigger);
      engine.setIdleGrokAcceptanceHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
      const first = engine.submitGrokSignal(idleSubmission, "fair_probability_analyst");

      expect((await engine.beginExperiment()).ok).toBe(true);
      release();

      expect(await first).toMatchObject({ status: "accepted" });
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:idle-boundary-submission")).toBeTruthy();
      const accepted = await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] });
      expect(accepted).toHaveLength(1);
      expect(accepted[0]?.payload.experimentId).toBe(engine.experimentManifest()?.id);
      expect(await engine.submitGrokSignal(idleSubmission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
    } finally {
      release?.();
      engine.setIdleGrokAcceptanceHoldForTest(null);
      await engine.stop();
    }
  });

  it("keeps concurrent retries around the experiment boundary single-fact", async () => {
    const { engine, journal } = await startQualifiedEngine();
    let release!: () => void;
    try {
      const trigger = monitoringTrigger("idle-boundary-concurrent-trigger");
      const idleSubmission = fairSubmission(trigger, "idle-boundary-concurrent-submission");
      await engine.registerTriggerForResearch(trigger);
      engine.setIdleGrokAcceptanceHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
      const first = engine.submitGrokSignal(idleSubmission, "fair_probability_analyst");
      const retry = engine.submitGrokSignal(idleSubmission, "fair_probability_analyst");
      expect(await retry).toEqual({ status: "duplicate" });
      expect((await engine.beginExperiment()).ok).toBe(true);
      release();
      expect(await first).toMatchObject({ status: "accepted" });
      expect(await engine.submitGrokSignal(idleSubmission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:idle-boundary-concurrent-submission")).toBeTruthy();
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
    } finally {
      release?.();
      engine.setIdleGrokAcceptanceHoldForTest(null);
      await engine.stop();
    }
  });

  it("keeps an accepted idle tombstone across start without an active duplicate", async () => {
    const { engine, journal } = await startQualifiedEngine();
    try {
      const trigger = monitoringTrigger("idle-tombstone-trigger");
      const submission = fairSubmission(trigger, "idle-tombstone-submission");
      await engine.registerTriggerForResearch(trigger);
      expect(await engine.submitGrokSignal(submission, "fair_probability_analyst")).toMatchObject({ status: "accepted" });
      expect((await engine.beginExperiment()).ok).toBe(true);

      expect(await engine.submitGrokSignal(submission, "fair_probability_analyst")).toEqual({ status: "duplicate" });
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:idle-tombstone-submission")).toBeNull();
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toEqual([]);
    } finally {
      await engine.stop();
    }
  });

  it("serializes concurrent experiment starts to one authoritative lifecycle", async () => {
    const { engine, journal } = await startQualifiedEngine();
    let release!: () => void;
    try {
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["EXPERIMENT_PREFLIGHT"]);
      const first = engine.beginExperiment();
      await waitForJournalAppend(journal);
      const second = engine.beginExperiment();
      release();
      const results = await Promise.all([first, second]);
      expect(results.filter((result) => result.ok)).toHaveLength(1);
      expect(results.filter((result) => !result.ok)).toHaveLength(1);
      expect(results.find((result) => !result.ok)).toMatchObject({ blockers: ["EXPERIMENT_ALREADY_ACTIVE"] });

      const started = await journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 });
      expect(started).toHaveLength(1);
      const experimentId = started[0]!.aggregateId;
      expect(engine.experimentManifest()?.id).toBe(experimentId);
      expect(await journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).toHaveLength(1);

      await engine.stop();
      const restartMarket = new ScriptedMarketAdapter();
      restartMarket.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
      const restarted = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
        market: restartMarket as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
      });
      await restarted.start();
      try {
        expect(restarted.experimentManifest()?.id).toBe(experimentId);
        expect(restarted.experimentManifest()?.status).toBe("RUNNING");
        expect(await journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
      } finally {
        await restarted.stop();
      }
    } finally {
      release?.();
      journal.setAppendInFlightHoldForTest(null);
      await engine.stop();
    }
  });

  it("releases the lifecycle gate after a failed start so a later start can proceed", async () => {
    const { engine, journal } = await startQualifiedEngine();
    try {
      engine.setCrashInjectionForTest("BEFORE_DURABLE_COMMIT");
      await expect(engine.beginExperiment()).rejects.toThrow("CRASH_INJECTED");
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      expect((await journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 }))).toEqual([]);
      expect((await engine.beginExperiment()).ok).toBe(true);
      expect(await journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 8 })).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("releases the lifecycle gate after a thrown preflight append", async () => {
    const { engine, journal } = await startQualifiedEngine();
    try {
      journal.failAppendTypes.add("EXPERIMENT_PREFLIGHT");
      await expect(engine.beginExperiment()).rejects.toThrow("JOURNAL_APPEND_FAILED");
      journal.failAppendTypes.delete("EXPERIMENT_PREFLIGHT");
      const retry = await engine.beginExperiment();
      expect(retry.ok).toBe(false);
      expect(retry.blockers).toContain("OVERLOAD_FAIL_CLOSED");
    } finally {
      journal.failAppendTypes.delete("EXPERIMENT_PREFLIGHT");
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
