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
import { describe, expect, it, vi } from "vitest";
import { AstraClientError, estimateAstraDecisionInputTokens, OpenAIAstraClient } from "../apps/engine/src/astra/client.js";
import { AstraBudgetError, AstraDecisionService } from "../apps/engine/src/astra/service.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { evaluateExperimentPreflight } from "../apps/engine/src/preflight.js";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import { createJournalEvent, MemoryEventJournal } from "@pm/db";
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
    for (const pairAskCost of [1.01, 1.03]) {
      const accepted = evaluateAstraOpportunityGate({ featurePacket: { ...packet, pairAskCost }, now, marketSynchronized: true, triggerProvenanceValid: true, minDepthUsd: 25, maxSpreadBps: 200 });
      expect(accepted.accepted).toBe(true);
    }
    expect(evaluateAstraOpportunityGate({ ...{ featurePacket: packet, now, marketSynchronized: false, triggerProvenanceValid: true } }).reason).toBe("UNSYNCHRONIZED");
  });

  it("does not call Astra outside the open first-leg window", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-window-market";
    const discovered = qualified(marketId, "BTC", "window-up", "window-down", new Date(clock.value + 900_000).toISOString());
    market.queue(discoveryReport([discovered]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const calls: string[] = [];
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async (request) => {
        calls.push(request.stage);
        return { output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["WINDOW_TEST"] }), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: `window-${calls.length}` };
      } },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      const up = snapshot({ marketId, tokenId: "window-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "window-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);

      await engine.registerTriggerForResearch(trigger(up, "idle-window"));
      expect(calls).toHaveLength(0);
      expect(engine.astraBudgetSnapshotForTest().spentUsd).toBe(0);
      expect(await journal.queryEvents({ types: ["ASTRA_MODEL_INVOCATION"] })).toHaveLength(0);

      expect((await engine.beginExperiment()).ok).toBe(true);
      expect((await journal.queryEvents({ types: ["EXPERIMENT_PREFLIGHT"] })).at(-1)?.payload).toMatchObject({
        checks: { activationProvider: "openai", decisionAuthority: "ASTRA_BRAIN_V1", localExecution: false, externalInference: true, grokDisabled: true, mcpRoles: [], missingRoles: [] },
      });
      await engine.registerTriggerForResearch(trigger(up, "running-window"));
      const runningCalls = calls.length;
      expect(runningCalls).toBeGreaterThan(0);

      expect((await engine.closeExperimentEntries()).ok).toBe(true);
      await engine.registerTriggerForResearch(trigger(up, "closed-window"));
      expect(calls).toHaveLength(runningCalls);

      await engine.completeExperimentForTest();
      await engine.registerTriggerForResearch(trigger(up, "completed-window"));
      expect(calls).toHaveLength(runningCalls);
      expect(await journal.queryEvents({ types: ["ASTRA_MODEL_INVOCATION"] })).toHaveLength(runningCalls);
    } finally {
      await engine.stop();
    }
  });

  it("keeps persisted Astra authority across a restart when the environment flag is disabled", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-authority-restart";
    const discovered = qualified(marketId, "BTC", "authority-up", "authority-down", new Date(clock.value + 900_000).toISOString());
    market.queue(discoveryReport([discovered]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const calls: string[] = [];
    const client: AstraClient = { decide: async (request) => {
      calls.push(request.stage);
      return { output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["AUTHORITY_RESTART"] }), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: `authority-${calls.length}` };
    } };
    const first = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), astraClient: client, now: () => new Date(clock.value),
    });
    await first.start(); await market.waitForHandlers();
    expect((await first.beginExperiment()).ok).toBe(true);
    expect(first.experimentManifest()?.modelConfiguration.authority).toBe("ASTRA_BRAIN_V1");
    await first.stop();

    market.queue(discoveryReport([discovered]));
    const second = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "false", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), astraClient: client, now: () => new Date(clock.value),
    });
    try {
      await second.start(); await market.waitForHandlers();
      expect(second.experimentManifest()?.modelConfiguration.authority).toBe("ASTRA_BRAIN_V1");
      const up = snapshot({ marketId, tokenId: "authority-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "authority-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      await second.registerTriggerForResearch(trigger(up, "authority-after-restart"));
      expect(calls.length).toBeGreaterThan(0);
      expect(await journal.queryEvents({ types: ["NATIVE_DECISION_COMPLETE"] })).toHaveLength(0);
    } finally {
      await second.stop();
    }
  });

  it("fails closed instead of falling back to Native for an active Astra experiment without a client", async () => {
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-authority-blocked";
    const discovered = qualified(marketId, "BTC", "blocked-up", "blocked-down", new Date(now.getTime() + 900_000).toISOString());
    market.queue(discoveryReport([discovered]));
    const first = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
      astraClient: { decide: async () => ({ output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.1, reasonCodes: ["SEED"] }), usage: { inputTokens: 1, cachedInputTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0, cacheWriteTokens: 0 }, providerRequestId: "seed" }) },
    });
    await first.start(); await market.waitForHandlers();
    expect((await first.beginExperiment()).ok).toBe(true);
    await first.closeExperimentEntries();
    await first.stop();

    market.queue(discoveryReport([discovered]));
    const recovered = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "false", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(),
    });
    await expect(recovered.start()).rejects.toThrow("astra_authority_unavailable");
    expect(recovered.recoveryFailClosedForTest()).toBe(true);
    await recovered.stop();
  });

  it("keeps persisted Native authority when Astra is enabled after restart", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "native-authority-restart";
    const discovered = qualified(marketId, "BTC", "native-up", "native-down", new Date(clock.value + 900_000).toISOString());
    market.queue(discoveryReport([discovered]));
    const journal = new MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const first = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "false", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
    });
    await first.start(); await market.waitForHandlers();
    expect((await first.beginExperiment()).ok).toBe(true);
    expect(first.experimentManifest()?.modelConfiguration.authority).toBe("NATIVE_DETERMINISTIC_V1");
    await first.stop();

    const calls: string[] = [];
    const client: AstraClient = { decide: async (request) => {
      calls.push(request.stage);
      return { output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["MUST_NOT_ROUTE"] }), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "must-not-route" };
    } };
    market.queue(discoveryReport([discovered]));
    const second = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), astraClient: client, now: () => new Date(clock.value),
    });
    try {
      await second.start(); await market.waitForHandlers();
      const up = snapshot({ marketId, tokenId: "native-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "native-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      await second.registerTriggerForResearch(trigger(up, "native-after-restart"));
      expect(second.experimentManifest()?.modelConfiguration.authority).toBe("NATIVE_DETERMINISTIC_V1");
      expect(calls).toHaveLength(0);
      expect(second.state.latestConsensus?.authority).toBe("NATIVE_DETERMINISTIC_V1");
    } finally {
      await second.stop();
    }
  });

  it("derives Astra regime reporting from canonical decisions without an unknown invocation regime", () => {
    const accumulator = new ExperimentSummaryAccumulator("astra-report-regime");
    const invocation = (stage: "FAST" | "DEEP" | "BATCH") => createJournalEvent({
      traceId: `invocation-${stage}`, type: "ASTRA_MODEL_INVOCATION", aggregateType: "astra_invocation", aggregateId: `invocation-${stage}`,
      payload: { authority: "ASTRA_BRAIN_V1", stage, usage: { costUsd: 0 }, providerRequestId: stage },
    });
    const decision = (regime: "TRENDING" | "VOLATILE") => createJournalEvent({
      traceId: `decision-${regime}`, type: "ASTRA_DECISION", aggregateType: "astra", aggregateId: `decision-${regime}`,
      payload: { authority: "ASTRA_BRAIN_V1", featurePacket: { regime }, stages: ["FAST", "DEEP"], decision: { action: "NO_TRADE" } },
    });
    accumulator.fold(invocation("FAST")); accumulator.fold(invocation("DEEP")); accumulator.fold(invocation("BATCH"));
    accumulator.fold(decision("TRENDING")); accumulator.fold(decision("VOLATILE"));
    const summary = accumulator.finalize();
    expect(summary.performanceByMarketRegime).toMatchObject({ TRENDING: { source: "ASTRA_DECISION", stage: "DEEP" }, VOLATILE: { source: "ASTRA_DECISION", stage: "DEEP" } });
    expect(summary.performanceByMarketRegime.unknown).toBeUndefined();
  });

  it("reports truthful Astra preflight authority metadata without changing Native or legacy Grok modes", () => {
    const market = qualified("preflight-astra", "BTC", "preflight-up", "preflight-down");
    const entryMarkets = [{ marketId: market.id, asset: market.asset, referenceRequired: false, blockedStatus: null, yesTokenId: "preflight-up", noTokenId: "preflight-down", endDate: market.endDate, feeScheduleKnown: true, binarySafe: true, monitored: true }];
    const base = { databaseUp: true, discoveryStatus: "MONITORING", qualifiedOrManaged: 1, tokenConfiguredPath: true, entryMarkets, reference: [], mcpRoleTokens: [], bootstrapFailed: false } as const;
    const astra = evaluateExperimentPreflight({ ...base, env: qualifiedPaperEnv({ ASTRA_ENABLED: "true", GROK_ACTIVATION_PROVIDER: "noop" }), mode: "48H_ASTRA_PAPER_EXPERIMENT" });
    expect(astra.checks).toMatchObject({ activationProvider: "openai", decisionAuthority: "ASTRA_BRAIN_V1", localExecution: false, externalInference: true, grokDisabled: true, grokResearchRequired: false, mcpRoles: [], missingRoles: [] });
    expect(astra.warnings).not.toContain("GROK_WAKE_NOT_VALIDATED");
    const native = evaluateExperimentPreflight({ ...base, env: qualifiedPaperEnv({ ASTRA_ENABLED: "false" }), mode: "48H_NATIVE_PAPER_EXPERIMENT" });
    expect(native.checks).toMatchObject({ activationProvider: "native_deterministic", decisionAuthority: "NATIVE_DETERMINISTIC_V1", localExecution: true, externalInference: false, grokDisabled: true });
    const grok = evaluateExperimentPreflight({ ...base, env: qualifiedPaperEnv({ ASTRA_ENABLED: "false" }), mode: "48H_GROK_PAPER_EXPERIMENT" });
    expect(grok.checks).toMatchObject({ decisionAuthority: "LEGACY_EXTERNAL", grokDisabled: false, grokResearchRequired: true });
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
    const uneconomicHedge = strategy.evaluate({
      consensus, snapshots: { UP: up, DOWN: down },
      position: { ...position, averageUpCost: "0.995" }, maxOrderSizeUsd: 25, maxFirstLegOrderSizeUsd: 12.5, minimumLockedPairEdge: 0.01,
    });
    expect(uneconomicHedge).toBeNull();
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

  it("passes the exact Fast proposal to Deep, which may confirm, veto, or flip it", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const fastProposal = astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.63, confidence: 0.71, uncertainty: 0.9, reasonCodes: ["FAST_PROPOSAL"] });
    const deepOutputs = [
      { action: "BUY_UP", fairProbabilityUp: 0.64, confidence: 0.72, uncertainty: 0.2, reasonCodes: ["DEEP_CONFIRMS_FAST"] },
      { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.8, uncertainty: 0.7, reasonCodes: ["DEEP_VETOES_FAST"] },
      { action: "BUY_DOWN", fairProbabilityUp: 0.36, confidence: 0.74, uncertainty: 0.2, reasonCodes: ["DEEP_FLIPS_FAST"] },
    ] as const;
    for (const expected of deepOutputs) {
      let deepProposal: unknown;
      const service = new AstraDecisionService({
        client: { decide: async (request) => {
          if (request.stage === "FAST") return { output: fastProposal, usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "fast" };
          deepProposal = request.fastProposal;
          return { output: astraModelOutputSchema.parse(expected), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "deep" };
        } },
        deepUncertaintyThreshold: 0.35, deepDrawdownUtilizationThreshold: 0.7, deepUnhedgedUtilizationThreshold: 0.7,
        memory: new AstraMemoryStore({ maxEntries: 4, maxLessons: 2, maxBytes: 2_000 }), budget: new AstraDailyBudget(5),
        maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 }, inputUsdPerM: 10, outputUsdPerM: 50,
      });
      const result = await service.decide({ traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } });
      expect(deepProposal).toEqual(fastProposal);
      expect(result.decision.action).toBe(expected.action);
    }
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
    const fastProposal = astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.61, confidence: 0.7, uncertainty: 0.8, reasonCodes: ["FAST_PROPOSAL"] });
    await client.decide({ stage: "DEEP", traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 }, fastProposal });
    const deepInput = (request?.input as Array<{ role: string; content: string }>)[1];
    expect(JSON.parse(deepInput.content).fastProposal).toEqual(fastProposal);
    expect((request?.input as Array<{ role: string; content: string }>)[0]?.content).toContain("challenge");
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

  it("fails closed with bounded Fast and Deep admission, including deadline-bound waiters", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const pending: Array<() => void> = [];
    const fakeSdk = { responses: { parse: vi.fn(() => new Promise((resolve) => {
      pending.push(() => resolve({ id: `resp_${pending.length}`, output_parsed: { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.1, reasonCodes: ["FAKE_SDK"] }, usage: { input_tokens: 1, output_tokens: 1 } }));
    })) } } as never;
    const client = new OpenAIAstraClient({
      apiKey: "test-only-injected", model: "gpt-6-astra", reasoning: { FAST: "low", DEEP: "medium", BATCH: "high" },
      timeoutMs: { FAST: 1_000, DEEP: 1_000, BATCH: 1_000 }, maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 },
      maxConcurrency: { FAST: 1, DEEP: 1 }, maxWaiters: { FAST: 0, DEEP: 0 }, pricing: { inputUsdPerM: 10, cachedInputUsdPerM: 1, outputUsdPerM: 50 }, client: fakeSdk,
    });
    const fastRequest = { stage: "FAST" as const, traceId: "fast-trace", triggerId: "fast-trigger", featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } };
    const deepRequest = { ...fastRequest, stage: "DEEP" as const, traceId: "deep-trace", fastProposal: astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.6, confidence: 0.7, uncertainty: 0.8, reasonCodes: ["FAST"] }) };
    const fastFirst = client.decide(fastRequest);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(client.decide(fastRequest)).rejects.toMatchObject({ message: "ASTRA_CONCURRENCY_SATURATED", dispatchState: "NOT_DISPATCHED" });
    expect(client.concurrencySnapshot("FAST")).toMatchObject({ active: 1, waiting: 0, maxWaiters: 0 });
    pending.shift()?.();
    await fastFirst;

    const deepFirst = client.decide(deepRequest);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(client.decide(deepRequest)).rejects.toMatchObject({ message: "ASTRA_CONCURRENCY_SATURATED", dispatchState: "NOT_DISPATCHED" });
    expect(client.concurrencySnapshot("DEEP")).toMatchObject({ active: 1, waiting: 0, maxWaiters: 0 });
    pending.shift()?.();
    await deepFirst;
  });

  it("bounds queued admission and aborts stale waiters before dispatch", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const pending: Array<() => void> = [];
    const fakeSdk = { responses: { parse: vi.fn(() => new Promise((resolve) => {
      pending.push(() => resolve({ id: "resp_waiter", output_parsed: { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.1, reasonCodes: ["FAKE_SDK"] }, usage: { input_tokens: 1, output_tokens: 1 } }));
    })) } } as never;
    const client = new OpenAIAstraClient({
      apiKey: "test-only-injected", model: "gpt-6-astra", reasoning: { FAST: "low", DEEP: "medium", BATCH: "high" },
      timeoutMs: { FAST: 40, DEEP: 40, BATCH: 40 }, maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 },
      maxConcurrency: { FAST: 1, DEEP: 1 }, maxWaiters: { FAST: 1, DEEP: 1 }, pricing: { inputUsdPerM: 10, cachedInputUsdPerM: 1, outputUsdPerM: 50 }, client: fakeSdk,
    });
    const request = { stage: "FAST" as const, traceId: "wait-trace", triggerId: "wait-trigger", featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } };
    const first = client.decide(request);
    await new Promise((resolve) => setTimeout(resolve, 0));
    const timedOut = client.decide(request);
    await expect(timedOut).rejects.toMatchObject({ message: "ASTRA_REQUEST_TIMEOUT", dispatchState: "NOT_DISPATCHED" });
    expect(client.concurrencySnapshot("FAST")).toMatchObject({ active: 1, waiting: 0, maxWaiters: 1 });
    const controller = new AbortController();
    const aborted = client.decide({ ...request, signal: controller.signal });
    controller.abort();
    await expect(aborted).rejects.toMatchObject({ message: "ASTRA_REQUEST_ABORTED", dispatchState: "NOT_DISPATCHED" });
    expect(client.concurrencySnapshot("FAST").waiting).toBe(0);
    pending.shift()?.();
    await first;
  });

  it("refunds only pre-dispatch failures and conservatively retains unknown spend for decisions and batch", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const options = {
      deepUncertaintyThreshold: 0.35, deepDrawdownUtilizationThreshold: 0.7, deepUnhedgedUtilizationThreshold: 0.7,
      memory: new AstraMemoryStore({ maxEntries: 4, maxLessons: 2, maxBytes: 2_000 }),
      maxOutputTokens: { FAST: 256, DEEP: 768, BATCH: 2_048 }, inputUsdPerM: 10, outputUsdPerM: 50,
    } as const;
    const input = { traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } };
    const preDispatchBudget = new AstraDailyBudget(1);
    const preDispatchService = new AstraDecisionService({ ...options, budget: preDispatchBudget, client: { decide: async () => { throw new AstraClientError("ASTRA_REQUEST_ABORTED", "FAST", "NOT_DISPATCHED"); } } });
    await expect(preDispatchService.decide(input)).rejects.toThrow("ASTRA_REQUEST_ABORTED");
    expect(preDispatchBudget.snapshot(now).spentUsd).toBe(0);

    const unknownBudget = new AstraDailyBudget(1);
    let unknownCalls = 0;
    const unknownService = new AstraDecisionService({ ...options, budget: unknownBudget, client: { decide: async () => { unknownCalls += 1; throw new AstraClientError("ASTRA_REQUEST_TIMEOUT", "FAST", "DISPATCHED_UNKNOWN"); } } });
    for (let i = 0; i < 100; i += 1) await expect(unknownService.decide(input)).rejects.toThrow();
    expect(unknownCalls).toBeLessThan(100);
    expect(unknownBudget.snapshot(now).spentUsd).toBeLessThanOrEqual(1 + 1e-9);
    expect(unknownBudget.snapshot(now).spentUsd).toBeGreaterThan(0);

    const knownBudget = new AstraDailyBudget(1);
    const knownService = new AstraDecisionService({ ...options, budget: knownBudget, client: { decide: async () => ({ output: astraModelOutputSchema.parse({ action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.5, uncertainty: 0.1, reasonCodes: ["KNOWN_USAGE"] }), usage: { inputTokens: 10, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 2, reasoningTokens: 0, costUsd: 0.123 }, providerRequestId: "known" }) } });
    await knownService.decide(input);
    expect(knownBudget.snapshot(now).spentUsd).toBeCloseTo(0.123, 10);

    const batchBudget = new AstraDailyBudget(1);
    const batchService = new AstraDecisionService({ ...options, budget: batchBudget, client: { decide: async () => { throw new Error("unused"); }, learn: async () => { throw new Error("provider outcome unknown"); } } });
    await expect(batchService.learn({ traceId: "batch-trace", observedAt: now.toISOString(), aggregate: { sampleIds: ["s:1"], sampleCount: 1, calibration: { samples: 1, brierSum: 0.1, reliability: 0.9 }, previousLessons: [] } })).rejects.toThrow("provider outcome unknown");
    expect(batchBudget.snapshot(now).spentUsd).toBeGreaterThan(0);
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

  it("rejects a delayed Astra decision after the original market epoch changes and later resynchronizes", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-stale-market";
    market.queue(discoveryReport([qualified(marketId, "BTC", "stale-up", "stale-down", new Date(clock.value + 900_000).toISOString())]));
    const journal = new (await import("@pm/db")).MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    let release!: () => void;
    let entered!: () => void;
    const decisionEntered = new Promise<void>((resolve) => { entered = resolve; });
    const decisionReleased = new Promise<AstraModelResult>((resolve) => { release = () => resolve({ output: astraModelOutputSchema.parse({ action: "BUY_UP", fairProbabilityUp: 0.7, confidence: 0.8, uncertainty: 0.1, reasonCodes: ["STALE_TEST"] }), usage: { inputTokens: 1, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0 }, providerRequestId: "stale-decision" }); });
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => { entered(); return decisionReleased; } },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({ marketId, tokenId: "stale-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "stale-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      const stale = { ...trigger(up, "stale"), kind: "BOOK_IMBALANCE" as const, evidence: { because: "BOOK_IMBALANCE" } };
      const pending = engine.registerTriggerForResearch(stale);
      await decisionEntered;
      market.emitGap(["stale-up", "stale-down"]);
      market.markTokenSync("stale-up", "SYNCED"); market.markTokenSync("stale-down", "SYNCED");
      await market.emitSnapshot({ ...up, eventId: "stale-resync-up", traceId: "stale-resync-up-trace", sequence: 2, receivedAt: new Date(clock.value + 1).toISOString(), sourceTimestamp: new Date(clock.value + 1).toISOString() });
      await market.emitSnapshot({ ...down, eventId: "stale-resync-down", traceId: "stale-resync-down-trace", sequence: 2, receivedAt: new Date(clock.value + 1).toISOString(), sourceTimestamp: new Date(clock.value + 1).toISOString() });
      release();
      await pending;
      expect(await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).toHaveLength(0);
      expect((await journal.queryEvents({ types: ["BOOK_REVALIDATION_REJECTED"] })).some((event) => ["PROVENANCE_CHANGED", "EPOCH_CHANGED"].includes(String(event.payload.reason)))).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("uses only executed Astra first-leg correlation for settlement learning and skips missing correlation", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-learning-market";
    const missingMarket = "astra-missing-correlation";
    market.queue(discoveryReport([
      qualified(marketId, "BTC", "learn-up", "learn-down", new Date(clock.value + 900_000).toISOString()),
      qualified(missingMarket, "ETH", "missing-up", "missing-down", new Date(clock.value + 900_000).toISOString()),
    ]));
    const journal = new (await import("@pm/db")).MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    let call = 0;
    const engine = new EngineService(qualifiedPaperEnv({ ASTRA_ENABLED: "true", ASTRA_BATCH_SETTLEMENT_THRESHOLD: "1", PAPER_EXECUTION_LATENCY_MS: "0", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.value),
      astraClient: { decide: async () => ({ output: astraModelOutputSchema.parse(call++ < 2 ? { action: "BUY_UP", fairProbabilityUp: 0.63, confidence: 0.9, uncertainty: 0.1, reasonCodes: ["EXECUTED"] } : { action: "NO_TRADE", fairProbabilityUp: 0.5, confidence: 0.9, uncertainty: 0.1, reasonCodes: ["LATER_NO_TRADE"] }), usage: { inputTokens: 1, cachedInputTokens: 0, outputTokens: 1, reasoningTokens: 0, costUsd: 0, cacheWriteTokens: 0 }, providerRequestId: `decision-${call}` }) },
    });
    try {
      await engine.start(); await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({ marketId, tokenId: "learn-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
      const down = snapshot({ marketId, tokenId: "learn-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
      await market.emitSnapshot(up); await market.emitSnapshot(down);
      await engine.registerTriggerForResearch(trigger(up, "executed"));
      const correlations = engine.astraExecutionCorrelationsForTest();
      expect(correlations).toHaveLength(1);
      expect(correlations[0]?.fairProbabilityUp).toBe(0.63);
      await engine.checkpointForTest();
      const checkpoint = await journal.loadLatestCheckpoint();
      expect((checkpoint?.payload as { astraExecutionCorrelations?: unknown[] }).astraExecutionCorrelations).toHaveLength(1);
      await engine.applyObservedFill({ ...fill("DOWN", "0.49", correlations[0]!.quantity), marketId, tokenId: "learn-down", id: "learning-hedge", orderId: "learning-hedge-order" });
      await engine.registerTriggerForResearch(trigger(up, "later"));
      await engine.settleMarketForTest(marketId, { closed: true, resolution: { umaResolutionStatus: "resolved" }, outcomes: ["UP", "DOWN"], outcomePrices: ["1", "0"] });
      expect(engine.astraLearningStateForTest().calibration.samples).toBe(1);
      expect(engine.astraLearningStateForTest().calibration.brierSum).toBeCloseTo((1 - 0.63) ** 2, 10);

      // A direct observed fill has no Astra execution correlation and must not
      // manufacture a neutral 0.5 calibration sample.
      await engine.applyObservedFill({ ...fill("UP", "0.40", "10"), marketId: missingMarket, tokenId: "missing-up", id: "missing-fill", orderId: "missing-order" });
      await engine.settleMarketForTest(missingMarket, { closed: true, resolution: { umaResolutionStatus: "resolved" }, outcomes: ["UP", "DOWN"], outcomePrices: ["1", "0"] });
      expect(engine.astraLearningStateForTest().calibration.samples).toBe(1);
    } finally {
      await engine.stop();
    }
  });

  it("uses a conservative pre-dispatch upper bound, caps settlement, and preserves unknown spend across checkpoint recovery", async () => {
    const { up, down } = pair();
    const packet = buildAstraFeaturePacket({ trigger: trigger(up), up, down, reference: null, now })!;
    const request = { stage: "FAST" as const, traceId: packet.traceId, triggerId: packet.triggerId, featurePacket: packet, memory: [], portfolio: { drawdownUtilization: 0, unhedgedUtilization: 0, openPositionCount: 0 } };
    const inputUpperBound = estimateAstraDecisionInputTokens(request);
    expect(inputUpperBound).toBeGreaterThan(Math.ceil(JSON.stringify({ featurePacket: packet, memory: [], portfolio: request.portfolio }).length / 4));
    const maxOutputTokens = 256;
    const inputUsdPerM = 10;
    const outputUsdPerM = 50;
    const safeReserve = (inputUpperBound / 1_000_000) * inputUsdPerM + (maxOutputTokens / 1_000_000) * outputUsdPerM;
    const blockedBudget = new AstraDailyBudget(safeReserve * 0.99);
    let dispatched = 0;
    const blockedService = new AstraDecisionService({ client: { decide: async () => { dispatched += 1; throw new Error("must not dispatch"); } }, deepUncertaintyThreshold: 1, deepDrawdownUtilizationThreshold: 1, deepUnhedgedUtilizationThreshold: 1, memory: new AstraMemoryStore({ maxEntries: 4, maxLessons: 2, maxBytes: 2_000 }), budget: blockedBudget, maxOutputTokens: { FAST: maxOutputTokens, DEEP: 768, BATCH: 2_048 }, inputUsdPerM, outputUsdPerM });
    await expect(blockedService.decide(request)).rejects.toBeInstanceOf(AstraBudgetError);
    expect(dispatched).toBe(0);

    const capped = new AstraDailyBudget(1);
    expect(capped.reserve(0.9, now)).toBe(true);
    capped.settle(0.9, 2, now);
    expect(capped.snapshot(now).spentUsd).toBe(1);
  });

  it("checkpoints unknown dispatched Astra spend before recovery can forget it", async () => {
    const clock = { value: now.getTime() };
    const market = new ScriptedMarketAdapter();
    const marketId = "astra-budget-recovery";
    const discovered = qualified(marketId, "BTC", "budget-up", "budget-down", new Date(clock.value + 900_000).toISOString());
    market.queue(discoveryReport([discovered]));
    const journal = new (await import("@pm/db")).MemoryEventJournal();
    const referenceRegistry = new ScriptedReferenceRegistry();
    referenceRegistry.resolve = async (asset) => ({ asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED", status: "UNSUPPORTED_DEFINITIVE" });
    const env = qualifiedPaperEnv({ ASTRA_ENABLED: "true", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
    const client = { decide: async (): Promise<never> => { throw new AstraClientError("ASTRA_REQUEST_TIMEOUT", "FAST", "DISPATCHED_UNKNOWN"); } };
    const first = new EngineService(env, journal, { error: () => undefined }, { market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), astraClient: client, now: () => new Date(clock.value) });
    await first.start(); await market.waitForHandlers();
    expect((await first.beginExperiment()).ok).toBe(true);
    const up = snapshot({ marketId, tokenId: "budget-up", outcome: "UP", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }] });
    const down = snapshot({ marketId, tokenId: "budget-down", outcome: "DOWN", asset: "BTC", receivedAt: new Date(clock.value).toISOString(), sourceTimestamp: new Date(clock.value).toISOString(), asks: [{ price: "0.49", quantity: "300" }], bids: [{ price: "0.48", quantity: "300" }] });
    await market.emitSnapshot(up); await market.emitSnapshot(down);
    await first.registerTriggerForResearch(trigger(up, "budget"));
    const spentBeforeRestart = first.astraBudgetSnapshotForTest().spentUsd;
    expect(spentBeforeRestart).toBeGreaterThan(0);
    await first.stop();

    market.queue(discoveryReport([discovered]));
    const second = new EngineService(env, journal, { error: () => undefined }, { market: market as never, referenceRegistry: referenceRegistry as never, activation: new NoopGrokActivationBridge(), astraClient: client, now: () => new Date(clock.value) });
    await second.start();
    expect(second.astraBudgetSnapshotForTest().spentUsd).toBeCloseTo(spentBeforeRestart, 12);
    await second.stop();
  });
});
