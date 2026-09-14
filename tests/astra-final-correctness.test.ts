import {
  AstraDailyBudget,
  AstraMemoryStore,
  astraModelOutputSchema,
  buildAstraFeaturePacket,
  experimentAllowsFirstLeg,
  renderExperimentSummaryMarkdown,
  type AstraClient,
  type AstraDecisionRequest,
  type AstraModelResult,
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
    kind: "SCHEDULED_REVIEW" as const, severity: 2 as const, score: 1, dedupeKey: `${suffix}-dedupe`, evidence: {},
    sourceEventIds: [tokenId], evidenceIds: [tokenId], createdAt: at.toISOString(), expiresAt: new Date(at.getTime() + 30_000).toISOString(),
  };
}

function decisionOutput(action: "BUY_UP" | "BUY_DOWN" | "NO_TRADE", uncertainty: number, reason = "TEST") {
  return astraModelOutputSchema.parse({ action, fairProbabilityUp: action === "BUY_DOWN" ? 0.35 : action === "BUY_UP" ? 0.65 : 0.5, confidence: 0.8, uncertainty, reasonCodes: [reason] });
}

function modelResult(output: ReturnType<typeof decisionOutput>, costUsd = 0.01, id = "fake") : AstraModelResult {
  return { output, usage: { inputTokens: 10, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 2, reasoningTokens: 0, costUsd }, providerRequestId: id };
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
    const service = new AstraDecisionService(serviceOptions({
      budget: new AstraDailyBudget(1),
      client: { decide: async (request: AstraDecisionRequest) => {
        if (request.stage === "DEEP") deepCalls += 1;
        return modelResult(decisionOutput("BUY_UP", 0.9));
      } },
      onStageSuccess: async () => false,
    }) as never);
    await expect(service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } })).rejects.toBeInstanceOf(AstraStageAccountingError);
    expect(deepCalls).toBe(0);
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
      expect(failures.some((event) => event.payload.reason === "DEEP_TIMEOUT")).toBe(true);
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
});
