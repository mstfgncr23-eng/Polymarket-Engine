import { describe, expect, it, vi } from "vitest";
import {
  activationRequestFromTrigger,
  containsSecretLeak,
  createId,
  externalAgentRoleDefinitions,
  engineEnvSchema,
  grokActivationRequestSchema,
  grokSignalSubmissionSchema,
  type GrokActivationRequest,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";
import { GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.js";
import { XaiGrokActivationBridge } from "../apps/engine/src/activation/xai.js";
import { XaiInferenceClient, XaiInferenceError } from "../apps/engine/src/activation/xai-client.js";
import type { GrokRoleContext } from "../apps/engine/src/activation/role-runner.js";
import { createGrokActivationBridge } from "../apps/engine/src/activation/index.js";

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
    market: { marketId: "market_runtime", asset: "BTC", snapshots: [{ marketId: "market_runtime", outcome: "UP", bids: [{ price: "0.48", quantity: "10" }], asks: [{ price: "0.49", quantity: "10" }] }] },
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

describe("EngineService xAI product path", () => {
  it("originates three signals in the real runtime and reaches existing consensus/paper ingress", async () => {
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("market_runtime", "BTC", "up-runtime", "down-runtime", expiry)]));
    const journal = new MemoryEventJournal();
    const env = qualifiedPaperEnv({
      GROK_ACTIVATION_PROVIDER: "xai",
      XAI_API_KEY: "xai-runtime-test-secret",
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
      const marketSnapshot = (outcome: "UP" | "DOWN", tokenId: string) => ({
        eventId: createId("market"), traceId: createId("trace"), marketId: "market_runtime", tokenId, outcome, asset: "BTC",
        bids: [{ price: "0.39", quantity: "2000" }], asks: [{ price: "0.40", quantity: "300" }], lastTradePrice: "0.40",
        tickSize: "0.01", minOrderSize: "1", feeSchedule: null, sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "runtime",
      });
      await market.emitSnapshot(marketSnapshot("UP", "up-runtime") as never);
      await market.emitSnapshot(marketSnapshot("DOWN", "down-runtime") as never);
      const deadline = Date.now() + 3_000;
      while (engine.state.grokActivity.acceptedSignals < 3 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 5));
      expect(engine.state.grokActivity.acceptedSignals).toBeGreaterThanOrEqual(3);
      expect(engine.state.latestConsensus).not.toBeNull();
      expect((fetchImpl as ReturnType<typeof vi.fn>).mock.calls.length).toBeGreaterThanOrEqual(3);
      const submissions = (await journal.readAll()).filter((event) => event.type === "GROK_SIGNAL_ACCEPTED");
      expect(submissions.length).toBeGreaterThanOrEqual(3);
      expect(JSON.stringify(submissions)).not.toContain("xai-runtime-test-secret");
    } finally {
      await engine.stop().catch(() => undefined);
    }
  });
});
