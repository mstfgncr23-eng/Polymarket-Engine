import {
  AstraDailyBudget,
  AstraDispatchGovernor,
  AstraMemoryStore,
  astraModelOutputSchema,
  buildAstraFeaturePacket,
  experimentAllowsFirstLeg,
  renderExperimentSummaryMarkdown,
  type AstraClient,
  type AstraDecisionRequest,
  type AstraModelResult,
  type AstraProviderDispatchContext,
} from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { describe, expect, it } from "vitest";
import { AstraClientError, OpenAIAstraClient } from "../apps/engine/src/astra/client.js";
import { AstraDecisionService, AstraDeepFailureError, AstraStageAccountingError } from "../apps/engine/src/astra/service.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import { now, snapshot } from "./fixtures.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";

function pair() {
  const up = snapshot({ marketId: "astra-final-market", tokenId: "astra-final-up", outcome: "UP", asks: [{ price: "0.48", quantity: "100" }], bids: [{ price: "0.47", quantity: "100" }] });
  const down = snapshot({ marketId: "astra-final-market", tokenId: "astra-final-down", outcome: "DOWN", asks: [{ price: "0.51", quantity: "100" }], bids: [{ price: "0.50", quantity: "100" }] });
  return { up, down };
}

function makeTrigger(marketId: string, tokenId: string, suffix: string, at = now) {
  return {
    id: `${suffix}-trigger`, traceId: `${suffix}-trace`, marketId, asset: "BTC", source: "polymarket" as const,
    kind: "PAIR_COST_OPPORTUNITY" as const, severity: 2 as const, score: 1, dedupeKey: `${suffix}-dedupe`, evidence: {},
    sourceEventIds: [tokenId], evidenceIds: [tokenId], createdAt: at.toISOString(), expiresAt: new Date(at.getTime() + 30_000).toISOString(),
  };
}

function decisionOutput(action: "BUY_UP" | "BUY_DOWN" | "NO_TRADE", uncertainty: number, reason = "TEST") {
  return astraModelOutputSchema.parse({ action, fairProbabilityUp: action === "BUY_DOWN" ? 0.35 : action === "BUY_UP" ? 0.65 : 0.5, confidence: 0.8, uncertainty, reasonCodes: [reason] });
}

function modelResult(output: ReturnType<typeof decisionOutput>, costUsd = 0.01, id = "fake") : AstraModelResult {
  return { output, usage: { inputTokens: 10, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 2, reasoningTokens: 0, costUsd }, providerRequestId: id };
}

function providerAuditEvent(
  type: "ASTRA_PROVIDER_DISPATCH_AUTHORIZED" | "ASTRA_PROVIDER_DISPATCH_STARTED" | "ASTRA_MODEL_INVOCATION" | "ASTRA_FAILURE",
  dispatchId: string,
  stage = "FAST",
  payload: Record<string, unknown> = {},
) {
  return createJournalEvent({
    id: `provider-audit:${type}:${dispatchId}`,
    traceId: dispatchId.split(":")[0] ?? dispatchId,
    type,
    aggregateType: "astra_provider_dispatch",
    aggregateId: dispatchId,
    payload: { authority: "ASTRA_BRAIN_V1", stage, dispatchId, ...payload },
  });
}

function serviceOptions(overrides: Record<string, unknown> = {}) {
  return {
    deepUncertaintyThreshold: 0.35,
    deepDrawdownUtilizationThreshold: 0.7,
    deepUnhedgedUtilizationThreshold: 0.7,
    memory: new AstraMemoryStore({ maxEntries: 8, maxLessons: 4, maxBytes: 8_000 }),
    maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 },
    inputUsdPerM: 10,
    cachedInputUsdPerM: 1,
    cacheWriteUsdPerM: 12.5,
    outputUsdPerM: 50,
    ...overrides,
  };
}

describe("Astra Brain V1 final correctness regressions", () => {
  it("keeps the rolling governor bounded and restores dispatch evidence across restart", () => {
    const governor = new AstraDispatchGovernor({ maxDispatchesPerHour: 2, minDispatchIntervalMs: 10, marketCooldownMs: 50, maxMarkets: 2 });
    governor.consume("market-a", 0);
    expect(governor.tryAdmit("market-b", 5)).toMatchObject({ allowed: false, reason: "GLOBAL_MIN_INTERVAL" });
    governor.consume("market-b", 10);
    expect(governor.tryAdmit("market-c", 20)).toMatchObject({ allowed: false, reason: "ROLLING_HOUR_CAP" });
    expect(governor.snapshot(20).dispatches).toHaveLength(2);
    const recovered = new AstraDispatchGovernor({ maxDispatchesPerHour: 2, minDispatchIntervalMs: 10, marketCooldownMs: 50, maxMarkets: 2 });
    recovered.restoreFromEvidence([
      { marketId: "market-a", occurredAt: new Date(0).toISOString() },
      { marketId: "market-b", occurredAt: new Date(10).toISOString() },
    ], 20);
    expect(recovered.tryAdmit("market-c", 20)).toMatchObject({ allowed: false, reason: "ROLLING_HOUR_CAP" });
    expect(recovered.snapshot(20).marketCooldowns.length).toBeLessThanOrEqual(2);
  });

  it("cannot raise the FAST rolling cap above twelve even with an unsafe constructor option", () => {
    const governor = new AstraDispatchGovernor({ maxDispatchesPerHour: 99, minDispatchIntervalMs: 0, marketCooldownMs: 0, maxMarkets: 32 });
    for (let index = 0; index < 12; index += 1) governor.consume(`market-${index}`, index);
    expect(governor.tryAdmit("market-over-cap", 12)).toMatchObject({ allowed: false, reason: "ROLLING_HOUR_CAP" });
    expect(governor.snapshot(12).dispatches).toHaveLength(12);
  });

  it("does not erase a checkpointed governor when recent journal evidence is absent", async () => {
    const clock = { value: now.getTime() };
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true" }), journal, { error: () => undefined }, {
      market: new ScriptedMarketAdapter() as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    const internal = engine as unknown as {
      astraGovernor: AstraDispatchGovernor;
      restoreAstraGovernorFromJournal: () => Promise<void>;
    };
    internal.astraGovernor.restore({ dispatches: [{ atMs: clock.value - 1_000 }], lastDispatchAtMs: clock.value - 1_000, marketCooldowns: [] }, clock.value);
    await internal.restoreAstraGovernorFromJournal();
    expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(1);
  });

  it("does not erase a checkpointed FAST governor when recent evidence is DEEP-only", async () => {
    const clock = { value: now.getTime() };
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true" }), journal, { error: () => undefined }, {
      market: new ScriptedMarketAdapter() as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    const internal = engine as unknown as {
      astraGovernor: AstraDispatchGovernor;
      restoreAstraGovernorFromJournal: () => Promise<void>;
    };
    internal.astraGovernor.restore({ dispatches: [{ atMs: clock.value - 1_000 }], lastDispatchAtMs: clock.value - 1_000, marketCooldowns: [] }, clock.value);
    await journal.append(createJournalEvent({
      id: "deep-only-recovery-evidence", traceId: "deep-only-recovery", type: "ASTRA_PROVIDER_DISPATCH_AUTHORIZED",
      aggregateType: "astra_provider_dispatch", aggregateId: "deep-only-recovery:DEEP",
      occurredAt: new Date(clock.value), payload: { authority: "ASTRA_BRAIN_V1", stage: "DEEP", dispatchId: "deep-only-recovery:DEEP" },
    }));
    await internal.restoreAstraGovernorFromJournal();
    expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(1);
  });

  it("does not weaken a checkpointed FAST governor when recovery evidence is partial", async () => {
    const clock = { value: now.getTime() };
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true" }), journal, { error: () => undefined }, {
      market: new ScriptedMarketAdapter() as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    const internal = engine as unknown as {
      astraGovernor: AstraDispatchGovernor;
      restoreAstraGovernorFromJournal: () => Promise<void>;
    };
    internal.astraGovernor.restore({
      dispatches: Array.from({ length: 12 }, (_, index) => ({ atMs: clock.value - 12_000 + index * 1_000 })),
      lastDispatchAtMs: clock.value - 1_000, marketCooldowns: [],
    }, clock.value);
    await journal.append(createJournalEvent({
      id: "partial-fast-recovery", traceId: "partial-fast-recovery", type: "ASTRA_PROVIDER_DISPATCH_AUTHORIZED",
      aggregateType: "astra_provider_dispatch", aggregateId: "partial-fast-recovery:FAST",
      occurredAt: new Date(clock.value - 500),
      payload: { authority: "ASTRA_BRAIN_V1", stage: "FAST", dispatchId: "partial-fast-recovery:FAST" },
    }));
    await internal.restoreAstraGovernorFromJournal();
    expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(12);
    clock.value += 10_000;
    expect(internal.astraGovernor.tryAdmit("partial-fresh-market", clock.value)).toMatchObject({
      allowed: false, reason: "ROLLING_HOUR_CAP",
    });
  });

  it("serializes concurrent markets so the FAST governor cannot be oversubscribed", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("concurrent-governor-market", "BTC", "concurrent-up", "concurrent-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS: "0" }), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    const authorize = (engine as unknown as {
      authorizeAndBeginAstraProviderDispatch: (context: AstraProviderDispatchContext, beginDispatch: () => boolean) => Promise<void>;
    }).authorizeAndBeginAstraProviderDispatch.bind(engine);
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const experimentId = engine.experimentManifest()!.id;
      const runGeneration = (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration;
      const outcomes = await Promise.allSettled(Array.from({ length: 13 }, (_, index) => authorize({
        experimentId, runGeneration, traceId: `concurrent-${index}`, triggerId: `concurrent-${index}`,
        stage: "FAST", model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY", marketId: `market-${index}`,
      }, () => true)));
      expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(12);
      expect(outcomes.filter((outcome) => outcome.status === "rejected").map((outcome) => (outcome.reason as { admissionReason?: string }).admissionReason))
        .toEqual(["ROLLING_HOUR_CAP"]);
      expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(12);
    } finally {
      await engine.stop();
    }
  });

  it("rejects a repeated dispatch identity before a second SDK attempt", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "duplicate-dispatch-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "duplicate-up", "duplicate-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({
      ASTRA_ENABLED: "true", ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS: "0", ASTRA_FAST_MARKET_COOLDOWN_MS: "0",
    }), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    const authorize = (engine as unknown as {
      authorizeAndBeginAstraProviderDispatch: (context: AstraProviderDispatchContext, beginDispatch: () => boolean) => Promise<void>;
    }).authorizeAndBeginAstraProviderDispatch.bind(engine);
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const context = {
        experimentId: engine.experimentManifest()!.id,
        runGeneration: (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration,
        traceId: "duplicate-dispatch", triggerId: "duplicate-dispatch", stage: "FAST" as const,
        model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY" as const, marketId,
      };
      let attempts = 0;
      await authorize(context, () => { attempts += 1; return true; });
      await expect(authorize(context, () => { attempts += 1; return true; })).rejects.toMatchObject({
        dispatchState: "NOT_DISPATCHED", admissionReason: "DISPATCH_ALREADY_AUTHORIZED",
      });
      expect(attempts).toBe(1);
      expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("recovers all interleaved FAST evidence across experiment identities after restart", async () => {
    const clock = { value: now.getTime() };
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    const engine = new EngineService(qualifiedPaperEnv({
      ASTRA_ENABLED: "true", ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS: "0",
    }), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(clock.value),
    });
    const events = [];
    for (let index = 0; index < 12; index += 1) {
      const dispatchId = "recovery-fast-" + index;
      const occurredAt = new Date(clock.value - 60_000 + index * 1_000);
      for (const eventType of ["ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "ASTRA_PROVIDER_DISPATCH_STARTED"] as const) {
        events.push(createJournalEvent({
          id: "recovery-" + eventType + "-" + index,
          traceId: dispatchId,
          type: eventType,
          aggregateType: "astra_provider_dispatch",
          aggregateId: dispatchId,
          occurredAt,
          payload: {
            authority: "ASTRA_BRAIN_V1", stage: "FAST", dispatchId,
            experimentId: index % 2 === 0 ? "old-experiment" : "newer-experiment",
            marketId: "recovery-market-" + index,
          },
        }));
      }
    }
    for (let index = 0; index < 20; index += 1) {
      events.push(createJournalEvent({
        id: "recovery-deep-" + index,
        traceId: "recovery-deep-" + index,
        type: "ASTRA_PROVIDER_DISPATCH_AUTHORIZED",
        aggregateType: "astra_provider_dispatch",
        aggregateId: "recovery-deep-" + index,
        occurredAt: new Date(clock.value - 20_000 + index * 500),
        payload: { authority: "ASTRA_BRAIN_V1", stage: "DEEP", experimentId: "newer-experiment" },
      }));
    }
    await journal.appendMany(events);
    engine.setRecoveryPageSizeForTest(3);
    await (engine as unknown as { restoreAstraGovernorFromJournal: () => Promise<void> }).restoreAstraGovernorFromJournal();
    expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(12);
    expect((engine as unknown as { astraGovernor: AstraDispatchGovernor }).astraGovernor.tryAdmit("fresh-market", clock.value))
      .toMatchObject({ allowed: false, reason: "ROLLING_HOUR_CAP" });
  });

  it("preserves the global FAST governor across prepare-next and opens only after the rolling hour", async () => {
    const clock = { value: now.getTime() };
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("governor-lifecycle-market", "BTC", "governor-lifecycle-up", "governor-lifecycle-down", new Date(clock.value + 900_000).toISOString())]));
    const engine = new EngineService(qualifiedPaperEnv({
      ASTRA_ENABLED: "true", ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS: "0",
    }), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(clock.value),
    });
    const authorize = (engine as unknown as {
      authorizeAndBeginAstraProviderDispatch: (context: AstraProviderDispatchContext, beginDispatch: () => boolean) => Promise<void>;
    }).authorizeAndBeginAstraProviderDispatch.bind(engine);
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      for (let index = 0; index < 12; index += 1) {
        await authorize({
          experimentId: engine.experimentManifest()!.id,
          runGeneration: (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration,
          traceId: "lifecycle-fast-" + index, triggerId: "lifecycle-fast-" + index,
          stage: "FAST", model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY",
          marketId: "lifecycle-market-" + index,
        }, () => true);
        clock.value += 6_000;
      }
      expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(12);
      await engine.completeExperimentForTest();
      expect((await engine.ensureSourceSealForTest()).sealed).toBe(true);
      await engine.writeFinalReportForTest();
      expect((await engine.prepareNextExperiment()).ok).toBe(true);
      expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(12);
      expect((await engine.beginExperiment()).ok).toBe(true);
      await expect(authorize({
        experimentId: engine.experimentManifest()!.id,
        runGeneration: (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration,
        traceId: "lifecycle-blocked", triggerId: "lifecycle-blocked", stage: "FAST",
        model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY", marketId: "lifecycle-new-market",
      }, () => true)).rejects.toMatchObject({ admissionReason: "ROLLING_HOUR_CAP", dispatchState: "NOT_DISPATCHED" });
      clock.value += 3_600_001;
      await authorize({
        experimentId: engine.experimentManifest()!.id,
        runGeneration: (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration,
        traceId: "lifecycle-after-hour", triggerId: "lifecycle-after-hour", stage: "FAST",
        model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY", marketId: "lifecycle-new-market",
      }, () => true);
      expect(engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("measures the rolling interval from actual SDK start after slow authorization", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("governor-timestamp-market", "BTC", "governor-timestamp-up", "governor-timestamp-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => modelResult(decisionOutput("NO_TRADE", 0.1), 0, "governor-timestamp") },
    });
    const authorize = (engine as unknown as {
      authorizeAndBeginAstraProviderDispatch: (context: AstraProviderDispatchContext, beginDispatch: () => boolean) => Promise<void>;
    }).authorizeAndBeginAstraProviderDispatch.bind(engine);
    const context = (marketId: string): AstraProviderDispatchContext => ({
      experimentId: engine.experimentManifest()!.id,
      runGeneration: (engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration,
      traceId: `governor-${marketId}`,
      triggerId: `governor-${marketId}`, stage: "FAST", model: "gpt-6-astra", triggerKind: "PAIR_COST_OPPORTUNITY", marketId,
    });
    let releaseAuthorization!: () => void;
    journal.appendInFlightHold = new Promise<void>((resolve) => { releaseAuthorization = resolve; });
    journal.appendInFlightHoldTypes = ["ASTRA_PROVIDER_DISPATCH_AUTHORIZED"];
    const starts: number[] = [];
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const first = authorize(context("governor-market-a"), () => { starts.push(clock.value); return true; });
      await new Promise((resolve) => setTimeout(resolve, 0));
      clock.value += 10_000;
      releaseAuthorization();
      await first;
      journal.appendInFlightHold = null;
      journal.appendInFlightHoldTypes = null;
      expect(starts).toEqual([now.getTime() + 10_000]);
      clock.value += 1_000;
      await expect(authorize(context("governor-market-b"), () => { starts.push(clock.value); return true; }))
        .rejects.toMatchObject({ admissionReason: "GLOBAL_MIN_INTERVAL", dispatchState: "NOT_DISPATCHED" });
      expect(starts).toHaveLength(1);
    } finally {
      journal.appendInFlightHold = null;
      journal.appendInFlightHoldTypes = null;
      releaseAuthorization?.();
      await engine.stop();
    }
  });

  it("uses a strict, time-aware first-leg deadline", () => {
    const startedAt = new Date("2026-09-13T00:00:00.000Z");
    const deadline = new Date("2026-09-13T01:00:00.000Z");
    const manifest = {
      status: "RUNNING" as const, startValidationPending: false, entryClosesAt: deadline.toISOString(),
    } as never;
    expect(experimentAllowsFirstLeg(manifest, new Date(deadline.getTime() - 1))).toBe(true);
    expect(experimentAllowsFirstLeg(manifest, deadline)).toBe(false);
    expect(experimentAllowsFirstLeg(manifest, new Date(deadline.getTime() + 1))).toBe(false);
    expect(startedAt.getTime()).toBeLessThan(deadline.getTime());
  });

  it("expires decisions before retrieval while retaining non-expiring lessons", () => {
    const clock = { value: Date.parse("2026-09-13T12:00:00.000Z") };
    const memory = new AstraMemoryStore({ maxEntries: 8, maxLessons: 4, maxBytes: 8_000 }, () => new Date(clock.value));
    memory.add({ id: "expired", kind: "DECISION", createdAt: new Date(clock.value - 2_000).toISOString(), expiresAt: new Date(clock.value - 1).toISOString(), summary: "expired", features: {} });
    memory.add({ id: "active", kind: "DECISION", createdAt: new Date(clock.value).toISOString(), expiresAt: new Date(clock.value + 10_000).toISOString(), summary: "active", features: {} });
    memory.add({ id: "lesson", kind: "LESSON", createdAt: new Date(clock.value).toISOString(), expiresAt: null, summary: "keep", features: {} });
    expect(memory.relevant(8).map((entry) => entry.id)).toEqual(["active", "lesson"]);
    const restored = new AstraMemoryStore({ maxEntries: 8, maxLessons: 4, maxBytes: 8_000 }, () => new Date(clock.value + 20_000));
    restored.restore(memory.snapshot());
    expect(restored.relevant(8).map((entry) => entry.id)).toEqual(["lesson"]);
  });

  it("puts BATCH behind the provider fence and propagates its run identity", async () => {
    const events: string[] = [];
    let observedRunGeneration: number | undefined;
    const service = new AstraDecisionService(serviceOptions({
      client: {
        decide: async () => modelResult(decisionOutput("NO_TRADE", 0.1)),
        learn: async () => {
          events.push("provider");
          return {
            output: { lessons: [] },
            usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0.02 },
            providerRequestId: "batch-fake",
          };
        },
      },
      onStageSuccess: async ({ stage, runGeneration }: { stage: string; runGeneration?: number }) => {
        events.push("account:" + stage);
        observedRunGeneration = runGeneration;
        return true;
      },
    }) as never);
    await expect(service.learn({
      traceId: "batch-fence-trace", experimentId: "batch-experiment", runGeneration: 7,
      observedAt: now.toISOString(),
      aggregate: { sampleIds: ["sample"], sampleCount: 1, calibration: { samples: 1, brierSum: 0.1, reliability: 0.9 }, previousLessons: [] },
      providerDispatchAuthorization: async (context, beginDispatch) => {
        events.push("authorized:" + context.stage);
        expect(context.experimentId).toBe("batch-experiment");
        expect(context.runGeneration).toBe(7);
        expect(beginDispatch()).toBe(true);
        events.push("started");
      },
    })).resolves.not.toBeNull();
    expect(events).toEqual(["authorized:BATCH", "started", "provider", "account:BATCH"]);
    expect(observedRunGeneration).toBe(7);
  });

  it("does not call the BATCH client for a stale fenced run", async () => {
    let providerCalls = 0;
    const service = new AstraDecisionService(serviceOptions({
      client: {
        decide: async () => modelResult(decisionOutput("NO_TRADE", 0.1)),
        learn: async () => {
          providerCalls += 1;
          return {
            output: { lessons: [] },
            usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0.02 },
            providerRequestId: "stale-batch",
          };
        },
      },
      onStageFailure: async ({ error }: { error: AstraClientError }) => {
        expect(error.admissionReason).toBe("STALE_RUN");
        return true;
      },
    }) as never);
    await expect(service.learn({
      traceId: "stale-batch-trace", experimentId: "old-experiment", runGeneration: 3,
      observedAt: now.toISOString(),
      aggregate: { sampleIds: ["sample"], sampleCount: 1, calibration: { samples: 1, brierSum: 0.1, reliability: 0.9 }, previousLessons: [] },
      providerDispatchAuthorization: async () => {
        throw new AstraClientError("ASTRA_PROVIDER_DISPATCH_BLOCKED", "BATCH", "NOT_DISPATCHED", { admissionReason: "STALE_RUN" });
      },
    })).rejects.toMatchObject({ dispatchState: "NOT_DISPATCHED", admissionReason: "STALE_RUN" });
    expect(providerCalls).toBe(0);
  });

  it("does not let legacy calendar-day budget state gate provider dispatch", async () => {
    const { up, down } = pair();
    const observedBeforeMidnight = new Date("2026-09-13T23:59:59.000Z");
    const currentAfterMidnight = new Date("2026-09-14T00:00:01.000Z");
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "billing", observedBeforeMidnight), up, down, reference: null, now: observedBeforeMidnight })!;
    const legacyBudget = new AstraDailyBudget(0.000001);
    legacyBudget.restore({ day: "2026-09-14", spentUsd: 999, calls: 999 });
    let calls = 0;
    let accountedUsd = 0;
    const service = new AstraDecisionService(serviceOptions({
      budget: legacyBudget,
      nowFn: () => currentAfterMidnight,
      client: { decide: async () => { calls += 1; return modelResult(decisionOutput("NO_TRADE", 0.1), 0.123); } },
      onStageSuccess: async ({ result }: { result: AstraModelResult }) => { accountedUsd += result.usage.costUsd; return true; },
    }) as never);
    await service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } });
    expect(calls).toBe(1);
    expect(accountedUsd).toBeCloseTo(0.123, 10);
    expect(legacyBudget.snapshot(currentAfterMidnight)).toEqual({ day: "", spentUsd: 0, calls: 0 });

    const batchService = new AstraDecisionService(serviceOptions({
      budget: legacyBudget,
      nowFn: () => currentAfterMidnight,
      client: { decide: async () => modelResult(decisionOutput("NO_TRADE", 0.1)), learn: async () => ({ output: { lessons: [] }, usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0.01 }, providerRequestId: "batch" }) },
    }) as never);
    await expect(batchService.learn({ traceId: "batch-billing", observedAt: observedBeforeMidnight.toISOString(), aggregate: { sampleIds: ["s"], sampleCount: 1, calibration: { samples: 1, brierSum: 0.1, reliability: 0.9 }, previousLessons: [] } })).resolves.not.toBeNull();
  });

  it("does not use pricing reservation math as a monetary dispatch gate", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "pricing"), up, down, reference: null, now })!;
    const request = { stage: "FAST" as const, traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } };
    const legacyBudget = new AstraDailyBudget(0.000001);
    let dispatched = 0;
    const service = new AstraDecisionService(serviceOptions({ budget: legacyBudget, cacheWriteUsdPerM: 100_000, inputUsdPerM: 100_000, outputUsdPerM: 100_000, client: { decide: async () => { dispatched += 1; return modelResult(decisionOutput("NO_TRADE", 0.1)); } } }) as never);
    await expect(service.decide(request)).resolves.toMatchObject({ decision: { action: "NO_TRADE" } });
    expect(dispatched).toBe(1);
  });

  it("rejects an Astra result that returns after the entry deadline", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-deadline-race-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "deadline-up", "deadline-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    let entered!: () => void;
    let release!: () => void;
    const enteredPromise = new Promise<void>((resolve) => { entered = resolve; });
    const released = new Promise<AstraModelResult>((resolve) => { release = () => resolve(modelResult(astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.59, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["DEADLINE_RACE"] }), 0, "deadline")); });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => { entered(); return released; } },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({ marketId, tokenId: "deadline-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "deadline-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      const pending = engine.registerTriggerForResearch(makeTrigger(marketId, up.eventId, "deadline-race", new Date(clock.value)));
      await enteredPromise;
      clock.value = Date.parse(engine.experimentManifest()!.entryClosesAt!);
      release();
      await pending;
      expect(engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).toHaveLength(0);
      expect(await journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 10 })).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("waits for every concurrent Astra provider operation before durable completion", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketA = "astra-finalization-concurrent-a";
    const marketB = "astra-finalization-concurrent-b";
    market.queue(discoveryReport([
      qualified(marketA, "BTC", "concurrent-a-up", "concurrent-a-down", new Date(clock.value + 900_000).toISOString()),
      qualified(marketB, "BTC", "concurrent-b-up", "concurrent-b-down", new Date(clock.value + 900_000).toISOString()),
    ]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const entered = new Map<string, () => void>();
    const enteredPromises = new Map<string, Promise<void>>();
    const releases = new Map<string, () => void>();
    const releasePromises = new Map<string, Promise<void>>();
    for (const marketId of [marketA, marketB]) {
      enteredPromises.set(marketId, new Promise<void>((resolve) => { entered.set(marketId, resolve); }));
      releasePromises.set(marketId, new Promise<void>((resolve) => { releases.set(marketId, resolve); }));
    }
    const engine = new EngineService(qualifiedPaperEnv({
      ASTRA_ENABLED: "true", ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS: "0",
    }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: {
        decide: async (request) => {
          const marketId = request.featurePacket.marketId;
          entered.get(marketId)?.();
          await releasePromises.get(marketId);
          return modelResult(decisionOutput("NO_TRADE", 0.1), 0, "concurrent-" + marketId);
        },
      },
    });
    let pendingA: Promise<void> | undefined;
    let pendingB: Promise<void> | undefined;
    let pendingCompletion: Promise<void> | undefined;
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      await market.emitSnapshot(snapshot({ marketId: marketA, tokenId: "concurrent-a-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] }));
      await market.emitSnapshot(snapshot({ marketId: marketA, tokenId: "concurrent-a-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] }));
      await market.emitSnapshot(snapshot({ marketId: marketB, tokenId: "concurrent-b-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] }));
      await market.emitSnapshot(snapshot({ marketId: marketB, tokenId: "concurrent-b-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] }));
      pendingA = engine.registerTriggerForResearch(makeTrigger(marketA, "concurrent-a-up", "concurrent-a"));
      pendingB = engine.registerTriggerForResearch(makeTrigger(marketB, "concurrent-b-up", "concurrent-b"));
      await Promise.all([enteredPromises.get(marketA)!, enteredPromises.get(marketB)!]);
      pendingCompletion = engine.completeExperimentForTest();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(engine.experimentManifest()?.status).toBe("RUNNING");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(0);
      releases.get(marketA)!();
      await pendingA;
      expect(engine.experimentManifest()?.status).toBe("RUNNING");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(0);
      releases.get(marketB)!();
      await pendingB;
      await pendingCompletion;
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(1);
    } finally {
      releases.get(marketA)?.();
      releases.get(marketB)?.();
      await pendingA?.catch(() => undefined);
      await pendingB?.catch(() => undefined);
      await pendingCompletion?.catch(() => undefined);
      await engine.stop();
    }
  });

  it("does not durably complete while an Astra provider operation is still in flight", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-finalization-race-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "finalize-up", "finalize-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    let entered!: () => void;
    let release!: () => void;
    const enteredPromise = new Promise<void>((resolve) => { entered = resolve; });
    const released = new Promise<AstraModelResult>((resolve) => {
      release = () => resolve(modelResult(decisionOutput("NO_TRADE", 0.1), 0, "finalization-race"));
    });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => { entered(); return released; } },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      await market.emitSnapshot(snapshot({ marketId, tokenId: "finalize-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] }));
      await market.emitSnapshot(snapshot({ marketId, tokenId: "finalize-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] }));
      const pendingDecision = engine.registerTriggerForResearch(makeTrigger(marketId, "finalize-up", "finalization-race"));
      await enteredPromise;
      const pendingCompletion = engine.completeExperimentForTest();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(engine.experimentManifest()?.status).toBe("RUNNING");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(0);
      release();
      await pendingDecision;
      await pendingCompletion;
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("does not deadlock when the in-flight Astra result observes entry close", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-self-finalization-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "self-finalize-up", "self-finalize-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    let entered!: () => void;
    let release!: () => void;
    const enteredPromise = new Promise<void>((resolve) => { entered = resolve; });
    const released = new Promise<AstraModelResult>((resolve) => {
      release = () => resolve(modelResult(decisionOutput("NO_TRADE", 0.1), 0, "self-finalization"));
    });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => { entered(); return released; } },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      await market.emitSnapshot(snapshot({ marketId, tokenId: "self-finalize-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] }));
      await market.emitSnapshot(snapshot({ marketId, tokenId: "self-finalize-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] }));
      const pendingDecision = engine.registerTriggerForResearch(makeTrigger(marketId, "self-finalize-up", "self-finalization"));
      await enteredPromise;
      expect((await engine.closeExperimentEntries()).ok).toBe(true);
      release();
      await pendingDecision;
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(await journal.queryEvents({ types: ["EXPERIMENT_COMPLETED"] })).toHaveLength(1);
    } finally {
      await engine.stop();
    }
  });

  it("durably accounts Fast before Deep and retains Fast when Deep fails", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "stage-barrier"), up, down, reference: null, now })!;
    const events: string[] = [];
    let providerCalls = 0;
    const service = new AstraDecisionService(serviceOptions({
      budget: new AstraDailyBudget(1),
      onBudgetChange: async (stage: string, change: string) => events.push(`budget:${stage}:${change}`),
      onStageSuccess: async ({ stage }: { stage: string }) => { events.push(`account:${stage}`); return true; },
      client: { decide: async (request: AstraDecisionRequest) => {
        events.push(`provider:${request.stage}`); providerCalls += 1;
        if (request.stage === "DEEP") throw new AstraClientError("ASTRA_REQUEST_TIMEOUT", "DEEP", "DISPATCHED_UNKNOWN");
        return modelResult(decisionOutput("BUY_UP", 0.9), 0.04, "fast");
      } },
    }) as never);
    await expect(service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } })).rejects.toBeInstanceOf(AstraDeepFailureError);
    expect(events.indexOf("account:FAST")).toBeGreaterThan(events.indexOf("provider:FAST"));
    expect(events.indexOf("provider:DEEP")).toBeGreaterThan(events.indexOf("account:FAST"));
    expect(providerCalls).toBe(2);
  });

  it("classifies malformed Deep structured output as an explicit invalid-response failure", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "deep-invalid"), up, down, reference: null, now })!;
    let calls = 0;
    const client = new OpenAIAstraClient({
      apiKey: "test-only-injected", model: "gpt-6-astra", reasoning: { FAST: "low", DEEP: "medium", BATCH: "high" },
      timeoutMs: { FAST: 1_000, DEEP: 1_000, BATCH: 1_000 }, maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 },
      maxConcurrency: { FAST: 1, DEEP: 1 }, pricing: { inputUsdPerM: 10, cachedInputUsdPerM: 1, outputUsdPerM: 50 },
      client: { responses: { parse: async () => calls++ === 0
        ? { id: "fast", output_parsed: decisionOutput("BUY_UP", 0.9), usage: { input_tokens: 1, output_tokens: 1 } }
        : { id: "deep", output_parsed: { action: "BUY_UP" }, usage: { input_tokens: 1, output_tokens: 1 } } } } as never,
    });
    const service = new AstraDecisionService(serviceOptions({ client, budget: new AstraDailyBudget(1) }) as never);
    await expect(service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } })).rejects.toMatchObject({ reason: "DEEP_INVALID_RESPONSE" });
  });

  it("fails closed when the stage accounting barrier rejects the Fast result", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "accounting-failure"), up, down, reference: null, now })!;
    let deepCalls = 0;
    let recoveredFailure: AstraClientError | null = null;
    const service = new AstraDecisionService(serviceOptions({
      budget: new AstraDailyBudget(1),
      client: { decide: async (request: AstraDecisionRequest) => {
        if (request.stage === "DEEP") deepCalls += 1;
        return modelResult(decisionOutput("BUY_UP", 0.9));
      } },
      onStageSuccess: async () => false,
      onStageFailure: async ({ error }: { error: AstraClientError }) => { recoveredFailure = error; },
    }) as never);
    await expect(service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } })).rejects.toBeInstanceOf(AstraStageAccountingError);
    expect(deepCalls).toBe(0);
    expect(recoveredFailure).toMatchObject({ dispatchState: "RESPONSE_RECEIVED", knownUsage: true, providerRequestId: "fake" });
  });

  it("preserves known usage when failure recovery itself throws", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: makeTrigger(up.marketId, up.eventId, "accounting-recovery-failure"), up, down, reference: null, now })!;
    let failureCalls = 0;
    let recoveredFailure: AstraClientError | null = null;
    const service = new AstraDecisionService(serviceOptions({
      client: { decide: async () => modelResult(decisionOutput("NO_TRADE", 0.1), 0.23, "known-after-recovery") },
      onStageSuccess: async () => false,
      onStageFailure: async ({ error }: { error: AstraClientError }) => {
        recoveredFailure = error;
        failureCalls += 1;
        if (failureCalls === 1) throw new Error("RECOVERY_ACCOUNTING_UNAVAILABLE");
      },
    }) as never);
    await expect(service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } })).rejects.toBeInstanceOf(AstraStageAccountingError);
    expect(recoveredFailure).toMatchObject({ dispatchState: "RESPONSE_RECEIVED", knownUsage: true, providerRequestId: "known-after-recovery" });
  });

  it("produces an explicit durable fail-closed NO_TRADE after a Deep timeout", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-deep-failure-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "deep-failure-up", "deep-failure-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new MemoryEventJournal();
    const references = new ScriptedReferenceRegistry();
    references.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: references as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async (request) => request.stage === "FAST"
        ? modelResult(decisionOutput("BUY_UP", 0.9), 0.05, "fast-failure")
        : Promise.reject(new AstraClientError("ASTRA_REQUEST_TIMEOUT", "DEEP", "DISPATCHED_UNKNOWN")) },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({ marketId, tokenId: "deep-failure-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "deep-failure-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      await engine.registerTriggerForResearch(makeTrigger(marketId, up.eventId, "deep-failure", new Date(clock.value)));
      const failures = await journal.queryEvents({ types: ["ASTRA_FAILURE"] });
      const decisions = await journal.queryEvents({ types: ["ASTRA_DECISION"] });
      expect(failures.some((event) => event.payload.reason === "TIMEOUT")).toBe(true);
      expect(decisions.some((event) => event.payload.decisionSource === "FAIL_CLOSED" && (event.payload.decision as { action?: string }).action === "NO_TRADE")).toBe(true);
      expect(await journal.queryEvents({ types: ["ASTRA_MODEL_INVOCATION"] })).toHaveLength(1);
      expect(await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).toHaveLength(0);
      expect(Number(engine.state.snapshot().balance.aiCost)).toBeGreaterThan(0);
    } finally {
      await engine.stop();
    }
  });

  it("renders actual Astra model cost while preserving Native zero-cost semantics", () => {
    const accumulator = new ExperimentSummaryAccumulator("astra-cost-report");
    accumulator.fold(createJournalEvent({ traceId: "cost", type: "ASTRA_MODEL_INVOCATION", aggregateType: "astra_invocation", aggregateId: "cost", payload: { authority: "ASTRA_BRAIN_V1", stage: "FAST", usage: { costUsd: 0.37 } } }));
    const summary = accumulator.finalize();
    const markdown = renderExperimentSummaryMarkdown(summary);
    expect(summary.modelCostByAgentUsd.ASTRA_BRAIN_V1).toBeCloseTo(0.37, 10);
    expect(markdown).toContain("Astra/OpenAI model cost: $0.37");
    expect(markdown).not.toContain("OpenAI token cost: $0");
  });

  it("reports NONE for an AUTHORIZED-only crash orphan and preserves it across restore", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-orphan");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "orphan:FAST"));
    const restored = ExperimentSummaryAccumulator.fromSnapshot(accumulator.serialize());
    const summary = restored.finalize();
    expect(summary.providerDispatchAuthorizedCount).toBe(1);
    expect(summary.providerDispatchStartedCount).toBe(0);
    expect(summary.providerDispatchUnresolvedAuthorizationCount).toBe(1);
    expect(summary.providerCostCompleteness).toBe("NONE");
  });

  it("reads a pre-lifecycle rollup conservatively instead of claiming complete cost", () => {
    const accumulator = new ExperimentSummaryAccumulator("legacy-provider-rollup");
    const legacy = accumulator.serialize() as unknown as Record<string, unknown>;
    delete legacy.providerDispatchAuthorizedCount;
    delete legacy.providerDispatchLifecycle;
    delete legacy.providerDispatchTrackingOverflow;
    delete legacy.providerDispatchTrackingIncomplete;
    delete legacy.providerKnownTerminalCount;
    const restored = ExperimentSummaryAccumulator.fromSnapshot(legacy as never);
    const summary = restored.finalize();
    expect(summary.providerDispatchTrackingIncomplete).toBe(true);
    expect(summary.providerCostCompleteness).toBe("NONE");
  });

  it("reports PARTIAL when an AUTHORIZED-only orphan coexists with known usage", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-partial");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "orphan:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "known:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "known:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_MODEL_INVOCATION", "known:FAST", "FAST", { usage: { costUsd: 0.12 } }));
    const summary = accumulator.finalize();
    expect(summary.providerCostCompleteness).toBe("PARTIAL");
    expect(summary.providerDispatchUnresolvedAuthorizationCount).toBe(1);
    expect(summary.modelCostByAgentUsd.ASTRA_BRAIN_V1).toBeCloseTo(0.12, 10);
  });

  it("counts a complete AUTHORIZED-to-success lifecycle once", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-complete");
    const events = [
      providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "complete:FAST"),
      providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "complete:FAST"),
      providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "complete:FAST"),
      providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "complete:FAST"),
      providerAuditEvent("ASTRA_MODEL_INVOCATION", "complete:FAST", "FAST", { usage: { costUsd: 0.09 } }),
      providerAuditEvent("ASTRA_MODEL_INVOCATION", "complete:FAST", "FAST", { usage: { costUsd: 0.09 } }),
    ];
    for (const event of events) accumulator.fold(event);
    const summary = accumulator.finalize();
    expect(summary.providerDispatchAuthorizedCount).toBe(1);
    expect(summary.providerDispatchStartedCount).toBe(1);
    expect(summary.successfulModelInvocationCount).toBe(1);
    expect(summary.modelCostByAgentUsd.ASTRA_BRAIN_V1).toBeCloseTo(0.09, 10);
    expect(summary.providerCostCompleteness).toBe("COMPLETE");
  });

  it("keeps dispatched-unknown failure conservative even after start", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-unknown");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "unknown:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "unknown:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_FAILURE", "unknown:FAST", "FAST", { dispatchState: "DISPATCHED_UNKNOWN", reason: "TIMEOUT" }));
    const summary = accumulator.finalize();
    expect(summary.dispatchedUnknownFailureCount).toBe(1);
    expect(summary.providerCostCompleteness).toBe("NONE");
  });

  it("retains known provider usage from a structured-output failure without an invocation row", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-known-failure");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "structured:DEEP", "DEEP"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "structured:DEEP", "DEEP"));
    accumulator.fold(providerAuditEvent("ASTRA_FAILURE", "structured:DEEP", "DEEP", {
      dispatchState: "RESPONSE_RECEIVED", knownUsage: true, usage: { costUsd: 0.17 }, reason: "STRUCTURED_OUTPUT_INVALID",
    }));
    const summary = accumulator.finalize();
    expect(summary.successfulModelInvocationCount).toBe(0);
    expect(summary.knownUsageFailureCount).toBe(1);
    expect(summary.modelCostByAgentUsd.ASTRA_BRAIN_V1).toBeCloseTo(0.17, 10);
    expect(summary.providerCostCompleteness).toBe("COMPLETE");
  });

  it("resolves an AUTHORIZED lifecycle with explicit NOT_DISPATCHED evidence", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-not-dispatched");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "blocked:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_FAILURE", "blocked:FAST", "FAST", { dispatchState: "NOT_DISPATCHED", reason: "DISPATCH_NOT_STARTED" }));
    const summary = accumulator.finalize();
    expect(summary.providerDispatchUnresolvedAuthorizationCount).toBe(0);
    expect(summary.notDispatchedFailureCount).toBe(1);
    expect(summary.providerCostCompleteness).toBe("COMPLETE");
  });

  it("fails closed on contradictory provider terminal evidence", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-contradiction");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "contradiction:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_FAILURE", "contradiction:FAST", "FAST", { dispatchState: "NOT_DISPATCHED", reason: "DISPATCH_NOT_STARTED" }));
    accumulator.fold(providerAuditEvent("ASTRA_MODEL_INVOCATION", "contradiction:FAST", "FAST", { usage: { costUsd: 0.11 } }));
    const summary = accumulator.finalize();
    expect(summary.providerDispatchTrackingIncomplete).toBe(true);
    expect(summary.providerCostCompleteness).toBe("PARTIAL");
  });

  it("does not let invocation plus failure evidence hide an unrelated orphan", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-mixed");
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "orphan:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "mixed:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_STARTED", "mixed:FAST"));
    accumulator.fold(providerAuditEvent("ASTRA_MODEL_INVOCATION", "mixed:FAST", "FAST", { usage: { costUsd: 0.2 } }));
    accumulator.fold(providerAuditEvent("ASTRA_FAILURE", "mixed:FAST", "FAST", { dispatchState: "RESPONSE_RECEIVED", knownUsage: true, usage: { costUsd: 0.2 }, reason: "STRUCTURED_OUTPUT_INVALID" }));
    const summary = accumulator.finalize();
    expect(summary.successfulModelInvocationCount).toBe(1);
    expect(summary.knownUsageFailureCount).toBe(1);
    expect(summary.modelCostByAgentUsd.ASTRA_BRAIN_V1).toBeCloseTo(0.2, 10);
    expect(summary.providerDispatchUnresolvedAuthorizationCount).toBe(1);
    expect(summary.providerCostCompleteness).toBe("PARTIAL");
  });

  it("fails closed when bounded provider lifecycle tracking overflows", () => {
    const accumulator = new ExperimentSummaryAccumulator("provider-overflow");
    for (let index = 0; index < 10_001; index += 1) {
      accumulator.fold(providerAuditEvent("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", `overflow-${index}:FAST`));
    }
    const summary = accumulator.finalize();
    expect(summary.providerDispatchAuthorizedCount).toBe(10_001);
    expect(summary.providerDispatchTrackingOverflow).toBe(true);
    expect(summary.providerCostCompleteness).toBe("NONE");
  });
});
