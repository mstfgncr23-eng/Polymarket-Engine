import {
  AstraDailyBudget,
  AstraBatchLearner,
  AstraMemoryStore,
  TemporalPairHedgeStrategy,
  astraFeaturePacketSchema,
  astraBatchOutputSchema,
  astraModelOutputSchema,
  buildAstraFeaturePacket,
  evaluateAstraOpportunityGate,
  evaluateRisk,
  riskLimitsSchema,
  type AstraClient,
  type AstraDecisionRequest,
  type AstraModelResult,
} from "@pm/core";
import { describe, expect, it } from "vitest";
import { OpenAIAstraClient } from "../apps/engine/src/astra/client.js";
import { AstraDecisionService } from "../apps/engine/src/astra/service.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { feeSchedule, fill, now, safeRisk, snapshot } from "./fixtures.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";

function pair() {
  const up = snapshot({ marketId: "astra-market", tokenId: "astra-up", outcome: "UP", asks: [{ price: "0.48", quantity: "100" }], bids: [{ price: "0.47", quantity: "100" }] });
  const down = snapshot({ marketId: "astra-market", tokenId: "astra-down", outcome: "DOWN", asks: [{ price: "0.51", quantity: "100" }], bids: [{ price: "0.50", quantity: "100" }] });
  return { up, down };
}

function trigger(up: ReturnType<typeof pair>["up"], suffix = "astra") {
  return {
    id: `${suffix}-trigger`, traceId: `${suffix}-trace`, marketId: up.marketId, asset: "BTC", source: "polymarket" as const,
    kind: "SCHEDULED_REVIEW" as const, severity: 2 as const, score: 1, dedupeKey: `${suffix}-dedupe`, evidence: {}, sourceEventIds: [up.eventId], evidenceIds: [up.eventId],
    createdAt: now.toISOString(), expiresAt: new Date(now.getTime() + 30_000).toISOString(),
  };
}

describe("Astra Brain V1 deterministic boundary", () => {
  it("extracts bounded features and classifies regime without selecting a trade", () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({
      trigger: trigger(up), up, down,
      reference: { asset: "BTC", venue: "binance", freshness: "fresh", freshnessMs: 20, recentMoveBps: 25, recentVolatilityBps: 10, referenceWindowMs: 60_000, referenceSourceTimestamp: now.toISOString() },
      now,
    });
    expect(packet).toMatchObject({ marketId: "astra-market", pairAskCost: 0.99, regime: "TRENDING" });
    expect(packet?.upDepthUsd).toBe(48);
    expect(packet && astraFeaturePacketSchema.safeParse(packet).success).toBe(true);
  });

  it("gate is deterministic and never treats a rejected opportunity as a model decision", () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now: new Date(now.getTime()) })!;
    const accepted = evaluateAstraOpportunityGate({ featurePacket: packet, now, marketSynchronized: true, triggerProvenanceValid: true, minDepthUsd: 25, maxSpreadBps: 200, maxPairAskCost: 1 });
    expect(accepted.accepted).toBe(true);
    expect(evaluateAstraOpportunityGate({ ...{ featurePacket: packet, now, marketSynchronized: false, triggerProvenanceValid: true } }).reason).toBe("UNSYNCHRONIZED");
  });

  it("keeps the first-leg cap at $12.50 while hedges retain the existing $25 ceiling", () => {
    const { up, down } = pair();
    const consensus = {
      id: "consensus", traceId: "trace", triggerId: "trigger", consensusDirection: "UP" as const,
      consensusConfidence: 0.9, agentAgreement: 1, signalFreshness: 1, conflicts: [], estimatedEdge: 0.1,
      evidenceIds: [], calculatedAt: now.toISOString(), authority: "ASTRA_BRAIN_V1" as const,
    };
    const strategy = new TemporalPairHedgeStrategy();
    const first = strategy.evaluate({ consensus, snapshots: { UP: up, DOWN: down }, position: null, maxOrderSizeUsd: 25, maxFirstLegOrderSizeUsd: 12.5, minimumLockedPairEdge: 0.01 });
    expect(Number(first?.quantity) * Number(first?.limitPrice)).toBeCloseTo(12.5, 8);
    const position = { id: "p", marketId: up.marketId, upQuantity: "30", downQuantity: "0", averageUpCost: "0.40", averageDownCost: "0", pairedQuantity: "0", unpairedOutcome: "UP" as const, unpairedQuantity: "30", combinedPairCost: "0", totalFees: "0", totalSlippage: "0", openedAt: now.toISOString(), updatedAt: now.toISOString() };
    const hedge = strategy.evaluate({ consensus, snapshots: { UP: up, DOWN: down }, position, maxOrderSizeUsd: 25, maxFirstLegOrderSizeUsd: 12.5, minimumLockedPairEdge: 0.01 });
    expect(Number(hedge?.quantity) * Number(hedge?.limitPrice)).toBeCloseTo(15.3, 8);
    expect(evaluateRisk(safeRisk({ intent: "OPEN_FIRST_LEG", orderSizeUsd: "12.51" }), riskLimitsSchema.parse({})).vetoes).toContain("MAX_FIRST_LEG_ORDER_SIZE");
    expect(evaluateRisk(safeRisk({ intent: "HEDGE_SECOND_LEG", orderSizeUsd: "25" }), riskLimitsSchema.parse({})).vetoes).not.toContain("MAX_FIRST_LEG_ORDER_SIZE");
  });

  it("uses injected fake results, selects Deep only when requested, and enforces the $5 daily budget", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const calls: string[] = [];
    const fake: AstraClient = { decide: async (request: AstraDecisionRequest): Promise<AstraModelResult> => {
      calls.push(request.stage);
      return { output: astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.6, confidence: 0.8, uncertainty: request.stage === "FAST" ? 0.8 : 0.1, reasonCodes: ["FAKE_TEST"] }), usage: { inputTokens: 100, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 20, reasoningTokens: 0, costUsd: 0.01 }, providerRequestId: "fake-request" };
    } };
    const service = new AstraDecisionService({ client: fake, deepUncertaintyThreshold: 0.35, deepDrawdownUtilizationThreshold: 0.7, deepUnhedgedUtilizationThreshold: 0.7, memory: new AstraMemoryStore({ maxEntries: 4, maxLessons: 2, maxBytes: 2_000 }), budget: new AstraDailyBudget(5), maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 }, inputUsdPerM: 10, outputUsdPerM: 50 });
    const result = await service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } });
    expect(calls).toEqual(["FAST", "DEEP"]);
    expect(result.decision.authority).toBe("ASTRA_BRAIN_V1");
    expect(result.decision.usage.costUsd).toBeCloseTo(0.02);
  });

  it("does not call Deep after a Fast NO_TRADE", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const calls: string[] = [];
    const service = new AstraDecisionService({
      client: { decide: async (request) => {
        calls.push(request.stage);
        return { output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.9, uncertainty: 1, reasonCodes: ["INSUFFICIENT_EVIDENCE"] }), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "fake-no-trade" };
      } },
      deepUncertaintyThreshold: 0.35, deepDrawdownUtilizationThreshold: 0.7, deepUnhedgedUtilizationThreshold: 0.7, memory: new AstraMemoryStore({ maxEntries: 4, maxLessons: 2, maxBytes: 2_000 }),
      budget: new AstraDailyBudget(5), maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 }, inputUsdPerM: 10, outputUsdPerM: 50,
    });
    const result = await service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } });
    expect(result.decision.action).toBe("NO_TRADE");
    expect(calls).toEqual(["FAST"]);
  });

  it("keeps memory and learner state bounded and checkpoint-restorable", () => {
    const memory = new AstraMemoryStore({ maxEntries: 2, maxLessons: 1, maxBytes: 1_000 });
    for (let i = 0; i < 5; i += 1) memory.add({ id: `m:${i}`, kind: "DECISION", createdAt: now.toISOString(), expiresAt: null, summary: `bounded-${i}`, features: { i }, outcome: "OPEN" });
    expect(memory.counts().entries).toBe(2);
    const restored = new AstraMemoryStore({ maxEntries: 2, maxLessons: 1, maxBytes: 1_000 });
    restored.restore(memory.snapshot());
    expect(restored.relevant(10).map((item) => item.id)).toEqual(["m:3", "m:4"]);
    const learner = new AstraBatchLearner(8);
    learner.observe("s:1", { predictedProbability: 0.8, outcome: 1 });
    learner.observe("s:2", { predictedProbability: 0.2, outcome: 0 });
    expect(learner.flush(2)?.calibration.samples).toBe(2);
    learner.applyLessons([{ scope: "global", lesson: "Prefer bounded evidence.", confidence: 0.7 }]);
    const learnerRestored = new AstraBatchLearner(8);
    learnerRestored.restore(learner.snapshot());
    expect(learnerRestored.snapshot().processedSettlementIds).toEqual(["s:1", "s:2"]);
    expect(learnerRestored.snapshot().lessons).toHaveLength(1);
  });

  it("validates Responses structured output through the official client seam without network", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    let request: Record<string, unknown> | null = null;
    const fakeSdk = { responses: { parse: async (input: Record<string, unknown>) => {
      request = input;
      return { id: "resp_fake", output_parsed: { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.2, reasonCodes: ["FAKE_SDK"] }, usage: { input_tokens: 10, output_tokens: 5, input_tokens_details: { cached_tokens: 2 } } };
    } } } as never;
    const client = new OpenAIAstraClient({ apiKey: "test-only-injected", model: "gpt-6-astra", reasoning: { FAST: "low", DEEP: "medium", BATCH: "high" }, timeoutMs: { FAST: 1000, DEEP: 1000, BATCH: 1000 }, maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2048 }, maxConcurrency: { FAST: 1, DEEP: 1 }, pricing: { inputUsdPerM: 10, cachedInputUsdPerM: 1, outputUsdPerM: 50 }, client: fakeSdk });
    const result = await client.decide({ stage: "FAST", traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } });
    expect(result.output.action).toBe("NO_TRADE");
    expect(request).toMatchObject({ model: "gpt-6-astra", store: false, max_output_tokens: 256, text: { format: { type: "json_schema", strict: true } } });
  });

  it("uses the bounded BATCH structured-output seam without network", async () => {
    const calls: Record<string, unknown>[] = [];
    const fakeSdk = { responses: { parse: async (input: Record<string, unknown>) => {
      calls.push(input);
      const format = (input.text as { format?: { name?: string } }).format?.name;
      return format === "astra_batch_lessons"
        ? { id: "resp_batch_fake", output_parsed: astraBatchOutputSchema.parse({ lessons: [{ scope: "global", lesson: "Keep evidence bounded.", confidence: 0.8 }] }), usage: { input_tokens: 12, output_tokens: 8, input_tokens_details: { cached_tokens: 3 } } }
        : { id: "resp_fast_fake", output_parsed: { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.2, reasonCodes: ["FAKE_SDK"] }, usage: { input_tokens: 10, output_tokens: 5, input_tokens_details: { cached_tokens: 2 } } };
    } } } as never;
    const client = new OpenAIAstraClient({ apiKey: "test-only-injected", model: "gpt-6-astra", reasoning: { FAST: "low", DEEP: "medium", BATCH: "high" }, timeoutMs: { FAST: 1000, DEEP: 1000, BATCH: 1000 }, maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2048 }, maxConcurrency: { FAST: 1, DEEP: 1 }, pricing: { inputUsdPerM: 10, cachedInputUsdPerM: 1, outputUsdPerM: 50 }, client: fakeSdk });
    const result = await client.learn({ stage: "BATCH", traceId: "batch-trace", observedAt: now.toISOString(), aggregate: { sampleIds: ["s:1"], sampleCount: 1, calibration: { samples: 1, brierSum: 0.1, reliability: 0.9 }, previousLessons: [] } });
    expect(result.output.lessons[0]?.scope).toBe("global");
    expect(calls[0]).toMatchObject({ model: "gpt-6-astra", store: false, max_output_tokens: 2048, text: { format: { type: "json_schema", strict: true } } });
  });

  it("routes an enabled engine through Astra without Native fallback", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("astra-market", "BTC", "astra-up", "astra-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new (await import("@pm/db")).MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const env = qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
    const calls: string[] = [];
    const astraClient: AstraClient = { decide: async (request) => {
      calls.push(request.stage);
      return { output: { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["FAKE_ENGINE"] }, usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "fake" };
    } };
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never,
      activation: new NoopGrokActivationBridge(), astraClient, now: () => new Date(clock.value),
    });
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({ marketId: "astra-market", tokenId: "astra-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId: "astra-market", tokenId: "astra-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      await engine.registerTriggerForResearch(trigger(up));
      expect(calls.length).toBeGreaterThan(0);
      expect(calls.every((stage) => stage === "FAST")).toBe(true);
      expect(await journal.queryEvents({ types: ["ASTRA_MODEL_INVOCATION"] })).toHaveLength(calls.length);
      expect(await journal.queryEvents({ types: ["NATIVE_DECISION_COMPLETE"] })).toHaveLength(0);
      expect(engine.experimentManifest()?.modelConfiguration).toMatchObject({ authority: "ASTRA_BRAIN_V1", nativeFallback: false });

      await engine.applyObservedFill({ ...fill("UP", "0.40", "10"), id: "astra-hedge-first", orderId: "astra-hedge-first-order", marketId: "astra-market", tokenId: "astra-up", timestamp: new Date(clock.value).toISOString() });
      await engine.closeExperimentEntries();
      const beforeHedgeCalls = calls.length;
      const hedgeUp = snapshot({ marketId: "astra-market", tokenId: "astra-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const hedgeDown = snapshot({ marketId: "astra-market", tokenId: "astra-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(hedgeUp); await market.emitSnapshot(hedgeDown);
      await engine.registerTriggerForResearch(trigger(hedgeUp, "hedge"));
      expect(calls.length).toBe(beforeHedgeCalls);
      expect((await journal.queryEvents({ types: ["ASTRA_DECISION"] })).some((event) => event.payload.bypass === "DETERMINISTIC_HEDGE")).toBe(true);
    } finally {
      await engine.stop();
    }
  });
});
