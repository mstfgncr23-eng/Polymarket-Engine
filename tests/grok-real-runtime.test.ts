import { describe, expect, it, vi } from "vitest";
import {
  activationRequestFromTrigger,
  containsSecretLeak,
  createId,
  externalAgentRoleDefinitions,
  engineEnvSchema,
  grokActivationRequestSchema,
  grokSignalSubmissionSchema,
  rolesForTriggerKind,
  type GrokActivationRequest,
  type TriggerEvent,
  type ReferenceTick,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";
import { GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.js";
import { XaiGrokActivationBridge } from "../apps/engine/src/activation/xai.js";
import { XaiInferenceClient, XaiInferenceError } from "../apps/engine/src/activation/xai-client.js";
import type { GrokRoleContext } from "../apps/engine/src/activation/role-runner.js";
import { boundedContextForPrompt } from "../apps/engine/src/activation/role-runner.js";
import { HotResearchCache } from "../apps/engine/src/activation/hot-research-cache.js";
import { createGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { feeSchedule } from "./fixtures.js";

const now = new Date("2026-01-01T00:00:00.000Z");
const expiry = new Date(now.getTime() + 60_000).toISOString();

function trigger(): TriggerEvent {
  return {
    id: "trigger_runtime", traceId: "trace_runtime", marketId: "market_runtime", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 3, score: 1, dedupeKey: "runtime", evidence: { book: "observed" },
    sourceEventIds: ["market_event"], evidenceIds: ["market_event"], createdAt: now.toISOString(), expiresAt: expiry,
  };
}

function context(request: GrokActivationRequest): GrokRoleContext {
  return {
    trigger: { id: request.triggerId, traceId: request.traceId, marketId: request.marketId, asset: request.asset, kind: request.triggerKind, severity: request.severity, evidenceIds: ["market_event"], sourceEventIds: ["market_event"], createdAt: request.createdAt, expiresAt: request.expiresAt },
    market: { marketId: "market_runtime", asset: "BTC", synchronized: true, snapshots: [{ marketId: "market_runtime", outcome: "UP", bids: [{ price: "0.48", quantity: "10" }], asks: [{ price: "0.49", quantity: "10" }] }] },
    reference: { asset: "BTC", latestPrice: "65000", freshness: "fresh" },
    portfolio: { cash: "1000", equity: "1000", reservedCash: "0", positions: [] },
    experiment: { id: "experiment_runtime", status: "RUNNING" }, recentStrategyRisk: [], researchToolsAvailable: false,
  };
}

function outputs(role: string): Record<string, unknown> {
  const base = { confidence: 0.8, evidenceIds: ["market_event"], expiresAt: expiry, reasonCodes: ["OBSERVED"] };
  if (role === "market_microstructure_scanner") return { ...base, direction: "UP", spread: "0.01", imbalance: 0.4, depthChange: 0.1, pairCost: "0.93", unusualConditions: [] };
  if (role === "crypto_market_scanner") return { ...base, direction: "UP", momentumBps: 3, volatilityBps: 4, volumeAnomalyScore: 0.2, accelerationBps: 1, regime: "CALM" };
  if (role === "news_x_intelligence_scanner") return { ...base, direction: "FLAT", classification: "NEUTRAL", summary: "No supplied news evidence.", sources: [] };
  if (role === "fair_probability_analyst") return { ...base, direction: "UP", fairProbability: 0.61, marketProbability: 0.48, estimatedEdge: 0.04 };
  if (role === "risk_hedge_analyst") return { ...base, recommendVeto: false, directionalExposureUsd: "0", unhedgedDurationMs: 0, pairedCost: null, adverseScenarios: [] };
  return { ...base, diagnosis: "Stable supplied state.", regime: "CALM", proposedChange: null };
}

function responseFor(role: string): Response {
  return new Response(JSON.stringify({ choices: [{ message: { content: JSON.stringify(outputs(role)) } }], usage: { prompt_tokens: 15, completion_tokens: 20 } }), { status: 200, headers: { "content-type": "application/json" } });
}

function runtimeTriggerAt(atMs: number, id: string, overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const createdAt = new Date(atMs).toISOString();
  return {
    id, traceId: `${id}_trace`, marketId: "market_runtime", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 3, score: 1, dedupeKey: `${id}_dedupe`, evidence: { book: "observed" },
    sourceEventIds: ["runtime_market_event"], evidenceIds: ["runtime_market_event"], createdAt,
    expiresAt: new Date(atMs + 60_000).toISOString(), ...overrides,
  };
}

async function waitForRuntime(condition: () => boolean | Promise<boolean>, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error("RUNTIME_TEST_TIMEOUT");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function createRuntimeProduct(overrides: Record<string, string | undefined> = {}) {
  let clockMs = now.getTime();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("market_runtime", "BTC", "up-runtime", "down-runtime", new Date(clockMs + 4 * 60 * 60_000).toISOString())]));
  const journal = new MemoryEventJournal();
  const env = qualifiedPaperEnv({
    GROK_ACTIVATION_PROVIDER: "xai", XAI_API_KEY: "xai-runtime-test-secret", XAI_MODEL: "grok-test",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", PAPER_EXECUTION_LATENCY_MS: "0", EXPERIMENT_DURATION_HOURS: "1",
    ...overrides,
  });
  const calls: Array<{ role: string; at: number; context: Record<string, any>; systemPrompt: string }> = [];
  const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
    const body = JSON.parse(String(init?.body)) as { response_format: { json_schema: { name: string } }; messages: Array<{ content: string }> };
    const role = body.response_format.json_schema.name.replace(/^grok_/, "");
    const contextEnvelope = JSON.parse(body.messages[1]!.content) as { context: Record<string, any> };
    calls.push({ role, at: Date.now(), context: contextEnvelope.context, systemPrompt: body.messages[0]!.content });
    const output = outputs(role);
    output.expiresAt = contextEnvelope.context.trigger.expiresAt;
    output.evidenceIds = contextEnvelope.context.trigger.evidenceIds ?? contextEnvelope.context.trigger.sourceEventIds ?? [];
    return new Response(JSON.stringify({ choices: [{ message: { content: JSON.stringify(output) } }], usage: { prompt_tokens: 30, completion_tokens: 20 } }), { status: 200, headers: { "content-type": "application/json" } });
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activationFetch: fetchImpl, now: () => new Date(clockMs),
  });
  await engine.start();
  await market.waitForHandlers();
  expect((await engine.beginExperiment()).ok).toBe(true);
  engine.state.noteReferenceTick("BTC", new Date(clockMs).toISOString(), { symbol: "BTCUSDT", venue: "binance", price: "65000" });
  return {
    engine, journal, market, calls,
    clock: { get: () => clockMs, set: (value: number) => { clockMs = value; }, advance: (ms: number) => { clockMs += ms; } },
  };
}

function runtimeSnapshot(outcome: "UP" | "DOWN", tokenId: string, sequence: number, atMs: number, ask = "0.40") {
  return {
    eventId: `runtime_${tokenId}_${sequence}`, traceId: `runtime_trace_${tokenId}_${sequence}`, marketId: "market_runtime", tokenId, outcome, asset: "BTC",
    bids: [{ price: "0.39", quantity: "300" }], asks: [{ price: ask, quantity: "300" }], lastTradePrice: ask,
    tickSize: "0.01", minOrderSize: "1", feeSchedule, sourceTimestamp: new Date(atMs).toISOString(), receivedAt: new Date(atMs).toISOString(), sequence, hash: `runtime_${sequence}`,
  };
}

function runtimeReferenceTick(atMs: number, id = "runtime_reference"): ReferenceTick {
  const iso = new Date(atMs).toISOString();
  return {
    eventId: id, traceId: `${id}_trace`, asset: "BTC", symbol: "BTCUSDT", venue: "binance",
    price: "65000", quantity: "1", sourceTimestamp: iso, receivedAt: iso, tradeId: id,
  };
}

describe("real Grok role runtime", () => {
  it("keeps xAI disabled by default even when a key is present", () => {
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", XAI_API_KEY: "xai-secret" });
    expect(createGrokActivationBridge(env).provider).toBe("noop");
  });

  it("executes all six registered roles through structured xAI output and the existing submission callback", async () => {
    const calls: Array<{ role: string; body: Record<string, unknown> }> = [];
    const submissions: string[] = [];
    const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
      const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      const messages = body.messages as Array<{ content: string }>;
      const match = messages[0]!.content.match(/agentId=([^.)]+)/);
      const role = match?.[1] ?? "strategy_supervisor";
      calls.push({ role, body });
      return responseFor(role);
    });
    const sameTrigger = trigger();
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(sameTrigger));
    const bridge = new XaiGrokActivationBridge({
      apiKey: "xai-test-secret", baseUrl: "https://xai.test/v1", model: "grok-test", timeoutMs: 1_000,
      fetchImpl, now: () => new Date(now), contextProvider: (item) => context(item),
      submitSignal: async (submission, clientId) => { expect(submission.agentId).toBe(clientId); submissions.push(submission.agentId); return { status: "accepted", signalId: createId("signal") }; },
    });
    const six = { ...request, targetRoles: externalAgentRoleDefinitions.map((item) => item.id) };
    const results = await bridge.activate(six);
    expect(results).toHaveLength(6);
    expect(results.every((item) => item.ok)).toBe(true);
    expect(new Set(submissions).size).toBe(6);
    expect(calls.map((item) => item.role)).toEqual(externalAgentRoleDefinitions.map((item) => item.id));
    for (const call of calls) {
      const format = call.body.response_format as { json_schema: { name: string; strict: boolean; schema: Record<string, unknown> } };
      expect(format.json_schema.name).toBe(`grok_${call.role}`);
      expect(format.json_schema.strict).toBe(true);
      expect(format.json_schema.schema).toMatchObject({ type: "object" });
      expect(JSON.stringify(call.body)).not.toContain("xai-test-secret");
    }
    const alphaPrompt = calls.find((call) => call.role === "fair_probability_analyst")?.body.messages[0]?.content ?? "";
    expect(alphaPrompt).toContain("not independent votes");
    expect(alphaPrompt).toContain("correlated evidence");
    expect(alphaPrompt).toContain("current synchronized raw book");
    expect(alphaPrompt).toContain("execution-cost math");
    expect(bridge.metricsSnapshot().every((item) => item.successes === 1 && item.requests === 1)).toBe(true);
  });

  it("maps auth and malformed output failures without exposing the key", async () => {
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger()));
    const unauthorized = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => new Response("{}", { status: 401 }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const authResult = await unauthorized.activate({ ...request, targetRoles: ["fair_probability_analyst"] });
    expect(authResult[0]).toMatchObject({ ok: false, errorCode: "XAI_AUTH", httpStatus: 401 });
    expect(containsSecretLeak(authResult, ["xai-secret"])).toBe(false);

    const malformed = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => responseFor("fair_probability_analyst"),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const invalid = await malformed.activate({ ...request, targetRoles: ["fair_probability_analyst"] });
    expect(invalid[0]?.ok).toBe(true);
    const badSchema = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => new Response(JSON.stringify({ choices: [{ message: { content: "{}" } }] }), { status: 200 }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const rejected = await badSchema.activate({ ...request, targetRoles: ["fair_probability_analyst"] });
    expect(rejected[0]).toMatchObject({ ok: false, errorCode: "XAI_SCHEMA_INVALID" });

    const unsupportedEvidence = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => {
        const output = outputs("fair_probability_analyst"); output.evidenceIds = ["invented-evidence"];
        return new Response(JSON.stringify({ choices: [{ message: { content: JSON.stringify(output) } }] }), { status: 200 });
      },
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    expect((await unsupportedEvidence.activate({ ...request, targetRoles: ["fair_probability_analyst"] }))[0]).toMatchObject({ ok: false, errorCode: "XAI_EVIDENCE_UNSUPPORTED" });
  });

  it("retries 429 with a bounded delay and never retries a trigger expiry", async () => {
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger()));
    let attempts = 0;
    const retry = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", maxAttempts: 2, delayImpl: async () => undefined,
      fetchImpl: vi.fn(async () => { attempts += 1; return attempts === 1 ? new Response("{}", { status: 429, headers: { "retry-after": "0" } }) : responseFor("fair_probability_analyst"); }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    expect((await retry.activate({ ...request, targetRoles: ["fair_probability_analyst"] }))[0]?.ok).toBe(true);
    expect(attempts).toBe(2);
    const expired = { ...request, expiresAt: new Date(now.getTime() + 1).toISOString() };
    const expiredBridge = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => { await new Promise((resolve) => setTimeout(resolve, 10)); return responseFor("fair_probability_analyst"); },
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    expect((await expiredBridge.activate({ ...request, expiresAt: new Date(now.getTime() - 1).toISOString(), targetRoles: ["fair_probability_analyst"] }))[0]).toMatchObject({ ok: false, errorCode: "TRIGGER_EXPIRED" });
    void expired;
  });

  it("counts actual role attempts across multi-market fanout and retries", async () => {
    let calls = 0;
    const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
      calls += 1;
      const body = JSON.parse(String(init?.body)) as { response_format: { json_schema: { name: string } } };
      return responseFor(body.response_format.json_schema.name.replace(/^grok_/, ""));
    });
    const bridge = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", maxRoleCallsPerMinute: 3,
      fetchImpl, contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger()));
    const first = await bridge.activate({ ...request, targetRoles: ["market_microstructure_scanner", "crypto_market_scanner"] });
    expect(first.every((item) => item.ok)).toBe(true);
    expect(bridge.budgetSnapshot()).toMatchObject({ limit: 3, used: 2, remaining: 1 });
    const second = await bridge.activate({ ...request, triggerId: "trigger_market_2", traceId: "trace_market_2", marketId: "market_2", targetRoles: ["news_x_intelligence_scanner", "fair_probability_analyst"] });
    expect(second[0]?.ok).toBe(true);
    expect(second[1]).toMatchObject({ ok: false, skipped: true, errorCode: "XAI_ROLE_CALL_BUDGET_EXHAUSTED" });
    expect(calls).toBe(3);
    expect(bridge.metricsSnapshot().reduce((total, item) => total + item.requests, 0)).toBe(3);

    let retryCalls = 0;
    const retry = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", maxAttempts: 2, maxRoleCallsPerMinute: 2,
      delayImpl: async () => undefined,
      fetchImpl: vi.fn(async (_input, init) => {
        retryCalls += 1;
        if (retryCalls === 1) return new Response("{}", { status: 503 });
        const body = JSON.parse(String(init?.body)) as { response_format: { json_schema: { name: string } } };
        return responseFor(body.response_format.json_schema.name.replace(/^grok_/, ""));
      }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const retried = await retry.activate({ ...request, targetRoles: ["fair_probability_analyst"] });
    expect(retried[0]?.ok).toBe(true);
    expect(retryCalls).toBe(2);
    expect(retry.metricsSnapshot().find((item) => item.role === "fair_probability_analyst")?.requests).toBe(2);
    expect(retry.budgetSnapshot()).toMatchObject({ used: 2, remaining: 0 });
  });

  it("maps server, malformed JSON, and expiry-during-call failures", async () => {
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger()));
    const server = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => new Response("{}", { status: 503 }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    expect((await server.activate({ ...request, targetRoles: ["fair_probability_analyst"] }))[0]).toMatchObject({ ok: false, errorCode: "XAI_SERVER_ERROR", httpStatus: 503 });
    const malformed = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => new Response("not-json", { status: 200 }),
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    expect((await malformed.activate({ ...request, targetRoles: ["fair_probability_analyst"] }))[0]).toMatchObject({ ok: false, errorCode: "XAI_INVALID_JSON" });
    let currentMs = now.getTime();
    const expiring = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        currentMs = now.getTime() + 5_000;
        return responseFor("fair_probability_analyst");
      },
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(currentMs),
    });
    expect((await expiring.activate({ ...request, expiresAt: new Date(now.getTime() + 1).toISOString(), targetRoles: ["fair_probability_analyst"] }))[0]).toMatchObject({ ok: false, errorCode: "TRIGGER_EXPIRED" });
  });

  it("keeps duplicate suppression at the existing coordinator boundary", async () => {
    let calls = 0;
    const sameTrigger = trigger();
    const request = grokActivationRequestSchema.parse(activationRequestFromTrigger(sameTrigger));
    const bridge = new XaiGrokActivationBridge({
      apiKey: "xai-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async () => { calls += 1; return responseFor("fair_probability_analyst"); },
      contextProvider: (item) => context(item), submitSignal: async () => ({ status: "accepted" }), now: () => new Date(now),
    });
    const coordinator = new GrokActivationCoordinator(bridge, { cooldownMs: 0, maxPerMinute: 10, suppressAtOrBelow: 0 }, () => new Date(now));
    const decision = coordinator.decide(sameTrigger);
    expect(decision.activate).toBe(true);
    coordinator.markRequested(sameTrigger, request);
    const duplicate = coordinator.decide(sameTrigger);
    expect(duplicate.reason).toBe("DUPLICATE");
    expect(calls).toBe(0);
  });
});

describe("xAI client contract", () => {
  it("supports external abort without leaking provider details", async () => {
    const abort = new AbortController();
    const client = new XaiInferenceClient({ apiKey: "super-secret", baseUrl: "https://xai.test/v1", model: "grok-test", fetchImpl: async (_url, init) => {
      await new Promise<void>((resolve) => { init?.signal?.addEventListener("abort", () => resolve(), { once: true }); });
      throw new DOMException("aborted", "AbortError");
    } });
    const promise = client.infer({ role: "fair_probability_analyst", systemPrompt: "system", userPrompt: "user", responseSchema: { type: "object" }, signal: abort.signal });
    abort.abort();
    await expect(promise).rejects.toMatchObject({ code: "XAI_ABORTED" });
    expect(XaiInferenceError).toBeDefined();
  });

  it("enforces the bounded request timeout", async () => {
    const client = new XaiInferenceClient({ apiKey: "super-secret", baseUrl: "https://xai.test/v1", model: "grok-test", timeoutMs: 250, fetchImpl: async (_url, init) => {
      await new Promise<void>((resolve) => { init?.signal?.addEventListener("abort", () => resolve(), { once: true }); });
      throw new DOMException("timeout", "AbortError");
    } });
    await expect(client.infer({ role: "fair_probability_analyst", systemPrompt: "system", userPrompt: "user", responseSchema: { type: "object" } })).rejects.toMatchObject({ code: "XAI_TIMEOUT" });
  }, 2_000);
});

describe("hot research cache and role isolation", () => {
  it("marks each role refresh-due from its own TTL headroom", () => {
    let clock = now.getTime();
    const cache = new HotResearchCache({ now: () => new Date(clock), maxEntries: 4, roleTtlMs: { market_microstructure_scanner: 3_000, crypto_market_scanner: 9_000 } });
    const base = {
      marketId: "m", asset: "BTC", triggerId: "t", traceId: "tr", generatedAt: new Date(clock).toISOString(),
      expiresAt: new Date(clock + 60_000).toISOString(), evidenceIds: ["e1"], sourceEventIds: ["s1"],
      featureIds: ["f1"], derivedFrom: ["s1"], marketSequence: 1, referenceTimestamp: null, output: { direction: "UP" },
    };
    cache.put({ ...base, role: "market_microstructure_scanner" });
    cache.put({ ...base, role: "crypto_market_scanner" });
    expect(cache.refreshDueFor("m", "BTC", ["market_microstructure_scanner", "crypto_market_scanner"])).toEqual([]);
    clock += 2_000;
    expect(cache.refreshDueFor("m", "BTC", ["market_microstructure_scanner", "crypto_market_scanner"])).toEqual(["market_microstructure_scanner"]);
  });

  it("expires role-specific research and remains bounded", () => {
    let clock = now.getTime();
    const cache = new HotResearchCache({ now: () => new Date(clock), maxEntries: 2, roleTtlMs: { market_microstructure_scanner: 1_000 } });
    const base = {
      marketId: "m", asset: "BTC", triggerId: "t", traceId: "tr", generatedAt: new Date(clock).toISOString(),
      expiresAt: new Date(clock + 60_000).toISOString(), evidenceIds: ["e1", "e1"], sourceEventIds: ["s1"],
      featureIds: ["f1", "f1"], derivedFrom: ["s1"], marketSequence: 4, referenceTimestamp: null,
      output: { direction: "UP" },
    };
    cache.put({ ...base, role: "market_microstructure_scanner" });
    cache.put({ ...base, role: "crypto_market_scanner" });
    cache.put({ ...base, role: "news_x_intelligence_scanner" });
    expect(cache.footprint().entries).toBe(2);
    expect(cache.freshFor("m", "BTC", ["news_x_intelligence_scanner"])).toHaveLength(1);
    clock += 2_000;
    expect(cache.get("m", "BTC", "market_microstructure_scanner")).toBeNull();
  });

  it("keeps portfolio and prior outcomes isolated while exposing provenance only to Alpha", () => {
    const base = context(grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger())));
    base.recentStrategyRisk = [{ type: "RISK_ACCEPT" }];
    base.scoutResearch = [{ role: "crypto_market_scanner", featureIds: ["f1"], sourceEventIds: ["s1"], evidenceIds: ["e1"] }];
    const micro = boundedContextForPrompt(base, "market_microstructure_scanner");
    expect(micro.portfolio.cash).toBe("[omitted]");
    expect(micro.recentStrategyRisk).toEqual([]);
    expect(micro.scoutResearch).toEqual([]);
    const alpha = boundedContextForPrompt(base, "fair_probability_analyst");
    expect(alpha.portfolio.cash).toBe("[omitted]");
    expect(alpha.scoutResearch).toHaveLength(1);
    alpha.provenance = { sourceEventIds: ["s1", "s1"], correlatedFeatureIds: ["f1", "f1"], scoutKeys: ["k1", "k1"] };
    const deduplicatedAlpha = boundedContextForPrompt(alpha, "fair_probability_analyst");
    expect(deduplicatedAlpha.provenance).toMatchObject({ sourceEventIds: ["s1"], correlatedFeatureIds: ["f1"], scoutKeys: ["k1"] });
    const risk = boundedContextForPrompt(base, "risk_hedge_analyst");
    expect(risk.portfolio.cash).toBe("1000");
  });

  it("never forwards an unsynchronized book and exposes overlapping evidence", () => {
    const base = context(grokActivationRequestSchema.parse(activationRequestFromTrigger(trigger())));
    base.market = { ...base.market!, synchronized: false };
    base.scoutResearch = [
      { role: "crypto_market_scanner", evidenceIds: ["same-event"], sourceEventIds: ["same-event"], featureIds: ["btc-window"] },
      { role: "market_microstructure_scanner", evidenceIds: ["same-event"], sourceEventIds: ["same-event"], featureIds: ["btc-window"] },
    ];
    const alpha = boundedContextForPrompt(base, "fair_probability_analyst");
    expect(alpha.market?.synchronized).toBe(false);
    expect(alpha.market?.snapshots).toEqual([]);
    expect(alpha.provenance?.correlatedFeatureIds).toEqual(undefined);
    expect(alpha.scoutResearch?.map((item) => item.sourceEventIds)).toEqual([["same-event"], ["same-event"]]);
  });
});

describe("EngineService xAI product path", () => {
  it("routes source-owned scout signals into the real runtime and reaches existing consensus/paper ingress", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("market_runtime", "BTC", "up-runtime", "down-runtime", expiry)]));
    const journal = new MemoryEventJournal();
    const env = qualifiedPaperEnv({
      GROK_ACTIVATION_PROVIDER: "xai",
      XAI_API_KEY: "xai-runtime-test-secret",
      GROK_MCP_BEARER_TOKENS: "",
      XAI_ALLOW_PORTFOLIO_CONTEXT: "true",
      XAI_MODEL: "grok-test",
      ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
      PAPER_EXECUTION_LATENCY_MS: "0",
      EXPERIMENT_DURATION_HOURS: "1",
    });
    const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
      const body = JSON.parse(String(init?.body)) as { response_format: { json_schema: { name: string } }; messages: Array<{ content: string }> };
      const role = body.response_format.json_schema.name.replace(/^grok_/, "");
      const envelope = JSON.parse(body.messages[1]!.content) as { context: { trigger: { expiresAt: string; evidenceIds?: string[]; sourceEventIds?: string[] } } };
      const output = outputs(role);
      if (role === "fair_probability_analyst") output.estimatedEdge = 0.35;
      output.expiresAt = envelope.context.trigger.expiresAt;
      output.evidenceIds = envelope.context.trigger.evidenceIds ?? envelope.context.trigger.sourceEventIds ?? [];
      return new Response(JSON.stringify({ choices: [{ message: { content: JSON.stringify(output) } }], usage: { prompt_tokens: 30, completion_tokens: 20 } }), { status: 200, headers: { "content-type": "application/json" } });
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activationFetch: fetchImpl,
      now: () => new Date(now),
    });
    try {
      await engine.start();
      await market.waitForHandlers();
      expect((await engine.beginExperiment()).ok).toBe(true);
      engine.state.noteReferenceTick("BTC", now.toISOString(), { symbol: "BTCUSDT", venue: "binance", price: "65000" });
      const marketSnapshot = (outcome: "UP" | "DOWN", tokenId: string) => ({
        eventId: createId("market"), traceId: createId("trace"), marketId: "market_runtime", tokenId, outcome, asset: "BTC",
        bids: [{ price: "0.39", quantity: "2000" }], asks: [{ price: "0.40", quantity: "300" }], lastTradePrice: "0.40",
        tickSize: "0.01", minOrderSize: "1", feeSchedule, sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "runtime",
      });
      await market.emitSnapshot(marketSnapshot("UP", "up-runtime") as never);
      await market.emitSnapshot(marketSnapshot("DOWN", "down-runtime") as never);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await engine.referenceTickForTest(runtimeReferenceTick(now.getTime()));
      const deadline = Date.now() + 3_000;
      while (engine.hotResearchCacheForTest().filter((entry) => entry.marketId === "market_runtime").length < 2 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 5));
      await engine.registerTriggerForResearch(runtimeTriggerAt(now.getTime(), "runtime_product_trigger", { severity: 2 }));
      while ((engine.state.grokActivity.acceptedSignals < 3 || (await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length === 0) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(engine.state.grokActivity.acceptedSignals).toBeGreaterThanOrEqual(2);
       expect(engine.state.latestConsensus).toMatchObject({ consensusDirection: "UP" });
       expect(engine.hotResearchCacheForTest().map((entry) => entry.role)).toEqual(expect.arrayContaining(["market_microstructure_scanner", "crypto_market_scanner"]));
       expect((await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length).toBeGreaterThanOrEqual(1);
       const fillDeadline = Date.now() + 3_000;
       while ((await journal.queryEvents({ types: ["PAPER_FILL"] })).length === 0 && Date.now() < fillDeadline) await new Promise((resolve) => setTimeout(resolve, 5));
       expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBeGreaterThanOrEqual(1);
       expect(engine.ledger.positions.get("market_runtime")).toBeDefined();
       const alphaDecision = await journal.queryEvents({ types: ["CONSENSUS_DECISION"] });
       expect(alphaDecision.some((event) => (event.payload as { authority?: string }).authority === "ALPHA")).toBe(true);
      expect((fetchImpl as ReturnType<typeof vi.fn>).mock.calls.length).toBeGreaterThanOrEqual(3);
      const submissions = (await journal.readAll()).filter((event) => event.type === "GROK_SIGNAL_ACCEPTED");
      expect(submissions.length).toBe(2);
      expect((fetchImpl as ReturnType<typeof vi.fn>).mock.calls.map((call) => {
        const body = JSON.parse(String(call[1]?.body)) as { response_format: { json_schema: { name: string } } };
        return body.response_format.json_schema.name.replace(/^grok_/, "");
      })).not.toContain("news_x_intelligence_scanner");
      expect(JSON.stringify(submissions)).not.toContain("xai-runtime-test-secret");
    } finally {
      await engine.stop().catch(() => undefined);
    }
  });

  it("warms scouts before a hot trigger and executes the critical path with Alpha only", async () => {
    const setup = await createRuntimeProduct();
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().filter((entry) => entry.marketId === "market_runtime").length === 1);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get()));
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().filter((entry) => entry.marketId === "market_runtime").length === 2);
      const warmRoles = setup.calls.map((call) => call.role);
      expect(warmRoles).toEqual(expect.arrayContaining(["market_microstructure_scanner", "crypto_market_scanner"]));
      expect(warmRoles).not.toContain("news_x_intelligence_scanner");
      const tradeTriggerAt = Date.now();
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "hot_runtime", { severity: 2 }));
      await waitForRuntime(async () => (await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length === 1);
      const afterTrigger = setup.calls.filter((call) => call.at >= tradeTriggerAt);
      expect(afterTrigger.map((call) => call.role)).toEqual(["fair_probability_analyst"]);
      expect((await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length).toBe(1);
      expect((await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
      expect((await setup.journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).length).toBe(1);
      const alphaRisk = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .map((event) => event.payload as { alpha?: Record<string, unknown> })
        .find((payload) => payload.alpha)?.alpha;
      expect(alphaRisk).toMatchObject({
        alphaSubmittedFairProbability: 0.61,
        fairProbabilityForTradedOutcome: 0.61,
        tradedOutcome: "UP",
        usedComplement: false,
        executionStyle: "TAKER",
        takerExecutableVwap: "0.4",
        currentExecutablePrice: "0.4",
        quantity: expect.any(String),
        executableNotionalUsd: expect.any(String),
        costUsdPerShare: expect.any(String),
        netEvUsdPerShare: expect.any(Number),
        grossEvUsdTotal: expect.any(String),
        netEvUsdTotal: expect.any(String),
        expectedROI: expect.any(Number),
        decisionTimestamp: expect.any(String),
        scoutAgesMs: expect.any(Array),
        provenance: expect.any(Array),
      });
      expect(Number(alphaRisk?.rawEdgeUsdPerShare)).toBeCloseTo(0.21, 8);
      expect((afterTrigger.find((call) => call.role === "fair_probability_analyst")?.systemPrompt ?? "")).toContain("not independent votes");
      expect((afterTrigger.find((call) => call.role === "fair_probability_analyst")?.systemPrompt ?? "")).toContain("correlated evidence");
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("filters a severity-3 economic trigger to Alpha only in the internal xAI path", async () => {
    const setup = await createRuntimeProduct();
    try {
      const preFilterRoles = rolesForTriggerKind("BOOK_IMBALANCE", 3);
      expect(preFilterRoles).toEqual(expect.arrayContaining([
        "market_microstructure_scanner", "crypto_market_scanner", "fair_probability_analyst", "strategy_supervisor",
      ]));

      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().some((entry) => entry.role === "market_microstructure_scanner"));
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get(), "severity3_reference"));
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().some((entry) => entry.role === "crypto_market_scanner"));
      await new Promise((resolve) => setTimeout(resolve, 0));

      const tradeAt = Date.now();
      const callIndex = setup.calls.length;
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "severity3_hot_trade", { severity: 3 }));
      await waitForRuntime(async () => (await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length === 1);

      const afterTrigger = setup.calls.slice(callIndex).filter((call) => call.at >= tradeAt);
      expect(afterTrigger.map((call) => call.role)).toEqual(["fair_probability_analyst"]);
      expect(afterTrigger.some((call) => call.role === "strategy_supervisor")).toBe(false);
      expect(afterTrigger.some((call) => call.role === "risk_hedge_analyst")).toBe(false);
      expect(afterTrigger.every((call) => !["market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner"].includes(call.role))).toBe(true);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).toHaveLength(1);
      expect(await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).toHaveLength(1);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("fails closed on a cold cache without invoking a stale Alpha trade", async () => {
    const setup = await createRuntimeProduct();
    try {
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "cold_runtime", { severity: 2 }));
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(setup.calls).toHaveLength(0);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toHaveLength(0);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("fails closed when the pre-trigger scout cache has expired", async () => {
    const setup = await createRuntimeProduct({ XAI_SCOUT_MICRO_TTL_MS: "1000", XAI_SCOUT_CRYPTO_TTL_MS: "1000", XAI_SCOUT_NEWS_TTL_MS: "1000" });
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().length === 1);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get()));
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().length === 2);
      const beforeTrigger = setup.calls.length;
      setup.clock.advance(2_000);
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "expired_runtime", { severity: 2 }));
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(setup.engine.hotResearchCacheForTest()).toHaveLength(0);
      expect(setup.calls.slice(beforeTrigger).filter((call) => call.role === "fair_probability_analyst")).toHaveLength(0);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toHaveLength(0);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("never forwards an unsynchronized book and creates no trade", async () => {
    const setup = await createRuntimeProduct();
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().length === 1);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get()));
      await waitForRuntime(() => setup.engine.hotResearchCacheForTest().length === 2);
      const before = setup.calls.length;
      setup.market.markTokenSync("up-runtime", "GAPPED");
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "unsync_runtime", { severity: 2 }));
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toHaveLength(0);
      const after = setup.calls.slice(before);
      expect(after.every((call) => call.context.market?.synchronized !== true || call.context.market.snapshots.length === 0)).toBe(true);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("routes refreshes by precursor owner and refreshes micro before its TTL expires", async () => {
    const setup = await createRuntimeProduct({
      XAI_SCOUT_MICRO_TTL_MS: "3000", XAI_SCOUT_CRYPTO_TTL_MS: "9000", XAI_SCOUT_NEWS_TTL_MS: "30000",
    });
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.calls.length === 1);
      expect(setup.calls.map((call) => call.role)).toEqual(["market_microstructure_scanner"]);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get(), "source_reference"));
      await waitForRuntime(() => setup.calls.length === 2);
      expect(setup.calls.map((call) => call.role)).toEqual(["market_microstructure_scanner", "crypto_market_scanner"]);
      expect(setup.calls.map((call) => call.role)).not.toContain("news_x_intelligence_scanner");
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Micro TTL is 3s and its 1s headroom makes it due at t=2s, while the
      // cache remains usable until t=3s. The refresh gap is not trigger cooldown.
      setup.clock.advance(2_000);
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 2, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 2, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.calls.length === 3);
      expect(setup.calls.map((call) => call.role)).toEqual([
        "market_microstructure_scanner", "crypto_market_scanner", "market_microstructure_scanner",
      ]);
      await new Promise((resolve) => setTimeout(resolve, 0));

      const tradeAt = Date.now();
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "duty_cycle_trade", { severity: 2 }));
      await waitForRuntime(async () => (await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length === 1);
      expect(setup.calls.filter((call) => call.at >= tradeAt).map((call) => call.role)).toEqual(["fair_probability_analyst"]);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("stops background scout refresh at ENTRY_CLOSED", async () => {
    const setup = await createRuntimeProduct({ EXPERIMENT_DURATION_HOURS: "0.001" });
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.calls.length === 1);
      setup.clock.advance(10_000);
      await setup.engine.tickMaintenance();
      expect(["ENTRY_CLOSED", "COMPLETED"]).toContain(setup.engine.experimentManifest()?.status);
      const before = setup.calls.length;
      setup.clock.advance(10_000);
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 2, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 2, setup.clock.get(), "0.70") as never);
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get(), "closed_reference"));
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(setup.calls.length).toBe(before);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toHaveLength(0);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("fails closed with an explicit budget reason when required scouts are stale", async () => {
    const setup = await createRuntimeProduct({ XAI_MAX_ROLE_CALLS_PER_MINUTE: "1" });
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.calls.length === 1);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get(), "budget_reference"));
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "budget_exhausted", { severity: 2 }));
      await waitForRuntime(async () => (await setup.journal.queryEvents({ types: ["GROK_ACTIVATION_DECISION"] })).some((event) => String((event.payload as { reason?: string }).reason).includes("BUDGET_EXHAUSTED")));
      expect(setup.calls.map((call) => call.role)).toEqual(["market_microstructure_scanner"]);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toHaveLength(0);
      expect(setup.engine.activation.runtimeBudgetSnapshot()).toMatchObject({ limit: 1, used: 1, remaining: 0 });
      expect(await setup.engine.metrics.registry.metrics()).toMatch(/grok_scout_refresh_skipped_total\{reason="BUDGET_EXHAUSTED"\} 1/);
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });

  it("labels maker calibration with the maker limit and separate taker reference", async () => {
    const setup = await createRuntimeProduct({ PAPER_EXECUTION_STYLE: "MAKER" });
    try {
      await setup.market.emitSnapshot(runtimeSnapshot("UP", "up-runtime", 1, setup.clock.get(), "0.40") as never);
      await setup.market.emitSnapshot(runtimeSnapshot("DOWN", "down-runtime", 1, setup.clock.get(), "0.70") as never);
      await waitForRuntime(() => setup.calls.length === 1);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await setup.engine.referenceTickForTest(runtimeReferenceTick(setup.clock.get(), "maker_reference"));
      await waitForRuntime(() => setup.calls.length === 2);
      await setup.engine.registerTriggerForResearch(runtimeTriggerAt(setup.clock.get(), "maker_calibration", { severity: 2 }));
      await waitForRuntime(async () => (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] })).some((event) => Boolean((event.payload as { alpha?: unknown }).alpha)));
      const alpha = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .map((event) => (event.payload as { alpha?: Record<string, unknown> }).alpha)
        .find((item) => item) as Record<string, unknown>;
      expect(alpha).toMatchObject({ executionStyle: "MAKER", makerLimitPrice: "0.39", takerReferenceVwap: "0.4" });
      expect(alpha.currentExecutablePrice).toBeUndefined();
      expect(alpha.takerExecutableVwap).toBeUndefined();
    } finally {
      await setup.engine.stop().catch(() => undefined);
    }
  });
});
