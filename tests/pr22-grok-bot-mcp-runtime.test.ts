import { describe, expect, it } from "vitest";
import { createId, grokSignalSubmissionSchema, triggerEventSchema, type ExternalAgentRoleId, type GrokActivationRequest, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { SlackGrokActivationBridge } from "../apps/engine/src/activation/slack.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry, startQualifiedEngine, book } from "./paper-engine-testkit.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";

const now = () => new Date().toISOString();

function trigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return triggerEventSchema.parse({
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: createId("dedupe"), evidence: { source: "test" },
    sourceEventIds: ["book-event"], evidenceIds: ["book-event"], createdAt: now(),
    expiresAt: new Date(Date.now() + 60_000).toISOString(), ...overrides,
  });
}

function scoutOutput(role: "micro" | "crypto", evidenceIds = ["book-event"]): Record<string, unknown> {
  const base = { confidence: 0.8, evidenceIds, expiresAt: new Date(Date.now() + 30_000).toISOString(), reasonCodes: ["PR22"] };
  return role === "micro"
    ? { ...base, direction: "UP", spread: "0.01", imbalance: 0.4, depthChange: 0.1, pairCost: "0.93", unusualConditions: [] }
    : { ...base, direction: "UP", momentumBps: 4, volatilityBps: 2, volumeAnomalyScore: 0, accelerationBps: 1, regime: "TRENDING" };
}

function fairOutput(evidenceIds = ["book-event"]): Record<string, unknown> {
  return {
    confidence: 0.9, evidenceIds, expiresAt: new Date(Date.now() + 30_000).toISOString(), reasonCodes: ["PR22"],
    direction: "UP", fairProbability: 0.62, marketProbability: 0.48, estimatedEdge: 0.04,
  };
}

function newsSubmission(triggerEvent: TriggerEvent) {
  return grokSignalSubmissionSchema.parse({
    submissionId: createId("submission"), triggerId: triggerEvent.id, traceId: triggerEvent.traceId,
    correlationId: createId("correlation"), marketId: triggerEvent.marketId, asset: triggerEvent.asset,
    timestamp: now(), agentId: "news_x_intelligence_scanner", role: "news_x_intelligence_scanner",
    output: {
      confidence: 0.7, evidenceIds: triggerEvent.sourceEventIds, expiresAt: new Date(Date.now() + 30_000).toISOString(),
      reasonCodes: ["PR22"], direction: "FLAT", classification: "NEUTRAL", summary: "No supplied news evidence.", sources: [],
    },
  });
}

function submission(triggerEvent: TriggerEvent, agentId: "market_microstructure_scanner" | "crypto_market_scanner" | "fair_probability_analyst", output: Record<string, unknown>) {
  return grokSignalSubmissionSchema.parse({
    submissionId: createId("submission"), triggerId: triggerEvent.id, traceId: triggerEvent.traceId,
    correlationId: createId("correlation"), marketId: triggerEvent.marketId, asset: triggerEvent.asset,
    timestamp: now(), agentId, role: agentId, output,
  });
}

async function setup() {
  const result = await startQualifiedEngine();
  const at = now();
  await result.market.emitSnapshot(book({ marketId: result.marketId, tokenId: "btc-up", outcome: "UP", receivedAt: at, sourceTimestamp: at, traceId: createId("book") }));
  await result.market.emitSnapshot(book({ marketId: result.marketId, tokenId: "btc-down", outcome: "DOWN", receivedAt: at, sourceTimestamp: at, traceId: createId("book") }));
  return result;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started >= timeoutMs) throw new Error("test wait timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

type CapturedActivation = { triggerId: string; targetRoles: ExternalAgentRoleId[] };

async function setupSlackActivation() {
  const market = new ScriptedMarketAdapter();
  const marketDefinition = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
  market.queue(discoveryReport([marketDefinition]));
  const journal = new MemoryEventJournal();
  const env = qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "slack", SLACK_BOT_TOKEN: "xoxb-test-token", SLACK_CHANNEL_ID: "C-TEST" });
  const activations: CapturedActivation[] = [];
  const fetchImpl: typeof fetch = async (_input, init) => {
    const body = typeof init?.body === "string" ? JSON.parse(init.body) as { blocks?: Array<{ text?: { text?: string } }> } : {};
    const blockText = body.blocks?.[0]?.text?.text;
    const encoded = typeof blockText === "string" ? blockText.match(/```([\s\S]*)```/)?.[1] : undefined;
    if (encoded) {
      const payload = JSON.parse(encoded) as Pick<GrokActivationRequest, "triggerId" | "targetRoles">;
      activations.push({ triggerId: payload.triggerId, targetRoles: [...payload.targetRoles] });
    }
    return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const referenceRegistry = new ScriptedReferenceRegistry();
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: referenceRegistry as never,
    activation: new SlackGrokActivationBridge({
      token: env.SLACK_BOT_TOKEN!, channelId: env.SLACK_CHANNEL_ID!, apiUrl: "https://slack.test/api/chat.postMessage",
      fetchImpl, delayImpl: async () => undefined,
    }),
  });
  await engine.start();
  const at = now();
  await market.emitSnapshot(book({ marketId: marketDefinition.id, tokenId: "btc-up", outcome: "UP", receivedAt: at, sourceTimestamp: at, traceId: createId("book") }));
  await market.emitSnapshot(book({ marketId: marketDefinition.id, tokenId: "btc-down", outcome: "DOWN", receivedAt: at, sourceTimestamp: at, traceId: createId("book") }));
  return { engine, activations, market, journal, marketId: marketDefinition.id };
}

describe("PR22 external Grok Bot MCP runtime alignment", () => {
  it("routes authenticated MCP scout writes to cache mode and keeps Alpha context/portfolio role-scoped", async () => {
    const setupResult = await setup();
    const event = trigger();
    await setupResult.engine.registerTriggerForResearch(event);
    const server = await createApiServer({
      state: setupResult.engine.state, journal: setupResult.journal, metrics: new EngineMetrics(), sse: new SseHub(2, new EngineMetrics()),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "market_microstructure_scanner:scanner-token,fair_probability_analyst:fair-token", allowedOrigins: "", rateLimitPerMinute: 30 },
      submitGrokSignal: (signal, clientId, mode) => setupResult.engine.submitGrokSignal(signal, clientId, mode),
      persistReportInput: (eventInput) => setupResult.engine.persistReportInput(eventInput),
      getAlphaResearchContext: (triggerId) => setupResult.engine.getAlphaResearchContextForMcp(triggerId),
    });
    await server.listen({ host: "127.0.0.1", port: 0 });
    const address = server.server.address();
    const url = `http://127.0.0.1:${typeof address === "object" && address ? address.port : 0}/mcp`;
    const rpc = async (token: string, body: unknown) => {
      const response = await fetch(url, {
        method: "POST", headers: { authorization: `Bearer ${token}`, accept: "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-11-25" },
        body: JSON.stringify(body),
      });
      const text = await response.text();
      const line = text.split("\n").filter((item) => item.startsWith("data:")).at(-1)?.slice(5).trim();
      return { status: response.status, json: line ? JSON.parse(line) : JSON.parse(text) };
    };
    try {
      const submitted = await rpc("scanner-token", { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "submit_signal", arguments: submission(event, "market_microstructure_scanner", scoutOutput("micro")) } });
      expect(submitted.status).toBe(200);
      expect(JSON.stringify(submitted.json)).toContain("accepted");
      expect(setupResult.engine.state.latestConsensus).toBeNull();

      const forbidden = await rpc("scanner-token", { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "get_portfolio", arguments: {} } });
      expect(JSON.stringify(forbidden.json)).toContain("ROLE_TOOL_FORBIDDEN");
      const context = await rpc("fair-token", { jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "get_alpha_research_context", arguments: { triggerId: event.id } } });
      expect(context.status).toBe(200);
      expect(JSON.stringify(context.json)).toContain("missingScoutRoles");
      expect(JSON.stringify(context.json)).not.toContain("reservedCash");
    } finally {
      await server.close();
    }
  });

  it("admits required scouts into the bounded cache and wakes Alpha without peer consensus", async () => {
    const { engine } = await setup();
    const event = trigger();
    await engine.registerTriggerForResearch(event);

    const micro = await engine.submitGrokSignal(submission(event, "market_microstructure_scanner", scoutOutput("micro")), "market_microstructure_scanner", "scout");
    const crypto = await engine.submitGrokSignal(submission(event, "crypto_market_scanner", scoutOutput("crypto")), "crypto_market_scanner", "scout");
    expect(micro.status).toBe("accepted");
    expect(crypto.status).toBe("accepted");
    expect(engine.hotResearchCacheForTest().map((entry) => entry.role)).toEqual(expect.arrayContaining([
      "market_microstructure_scanner", "crypto_market_scanner",
    ]));
    expect(engine.state.latestConsensus).toBeNull();

    const context = engine.getAlphaResearchContextForMcp(event.id);
    expect(context).toMatchObject({ readyForAlpha: true, missingScoutRoles: [] });
    expect(context).not.toHaveProperty("portfolio");

    const alpha = await engine.submitGrokSignal(submission(event, "fair_probability_analyst", fairOutput()), "fair_probability_analyst", "alpha");
    expect(alpha.status).toBe("accepted");
    expect(engine.state.latestConsensus).toMatchObject({ triggerId: event.id, consensusDirection: "UP" });

    const newsEvent = trigger({ id: createId("trigger"), traceId: createId("trace"), kind: "NEWS_INFORMATION", sourceEventIds: ["news-event"], evidenceIds: ["news-event"] });
    await engine.registerTriggerForResearch(newsEvent);
    const news = await engine.submitGrokSignal(newsSubmission(newsEvent), "news_x_intelligence_scanner", "scout");
    expect(news.status).toBe("accepted");
    expect(engine.hotResearchCacheForTest().some((entry) => entry.role === "news_x_intelligence_scanner" && entry.triggerId === newsEvent.id)).toBe(true);
  });

  it("wakes only Alpha through the external Slack bridge after the final required scout is accepted", async () => {
    const setupResult = await setupSlackActivation();
    expect((await setupResult.engine.beginExperiment()).ok).toBe(true);
    const event = trigger();
    await setupResult.engine.registerTriggerForResearch(event);

    const rolesBeforeScout = setupResult.activations.flatMap((item) => item.targetRoles);
    expect(rolesBeforeScout).toEqual(expect.arrayContaining([
      "market_microstructure_scanner", "crypto_market_scanner",
    ]));
    expect(rolesBeforeScout).not.toContain("fair_probability_analyst");
    expect(rolesBeforeScout).not.toContain("risk_hedge_analyst");
    expect(rolesBeforeScout).not.toContain("strategy_supervisor");

    const micro = await setupResult.engine.submitGrokSignal(submission(event, "market_microstructure_scanner", scoutOutput("micro")), "market_microstructure_scanner", "scout");
    expect(micro.status).toBe("accepted");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(setupResult.activations.flatMap((item) => item.targetRoles)).not.toContain("fair_probability_analyst");

    const crypto = await setupResult.engine.submitGrokSignal(submission(event, "crypto_market_scanner", scoutOutput("crypto")), "crypto_market_scanner", "scout");
    expect(crypto.status).toBe("accepted");
    await waitFor(() => setupResult.activations.filter((item) => item.targetRoles.length === 1 && item.targetRoles[0] === "fair_probability_analyst").length === 1);
    expect(setupResult.activations.filter((item) => item.targetRoles.includes("fair_probability_analyst"))).toEqual([
      { triggerId: event.id, targetRoles: ["fair_probability_analyst"] },
    ]);
  });

  it("coalesces simultaneous final scout completions into one external Alpha wake", async () => {
    const setupResult = await setupSlackActivation();
    expect((await setupResult.engine.beginExperiment()).ok).toBe(true);
    const event = trigger();
    await setupResult.engine.registerTriggerForResearch(event);

    const [micro, crypto] = await Promise.all([
      setupResult.engine.submitGrokSignal(submission(event, "market_microstructure_scanner", scoutOutput("micro")), "market_microstructure_scanner", "scout"),
      setupResult.engine.submitGrokSignal(submission(event, "crypto_market_scanner", scoutOutput("crypto")), "crypto_market_scanner", "scout"),
    ]);
    expect(micro.status).toBe("accepted");
    expect(crypto.status).toBe("accepted");
    await waitFor(() => setupResult.activations.some((item) => item.targetRoles.length === 1 && item.targetRoles[0] === "fair_probability_analyst"));
    const alphaWakes = setupResult.activations.filter((item) => item.targetRoles.includes("fair_probability_analyst"));
    expect(alphaWakes).toHaveLength(1);
    expect(alphaWakes[0]).toEqual({ triggerId: event.id, targetRoles: ["fair_probability_analyst"] });
  });

  it("rejects a wrong-role, unsynchronized, expired, or unsupported-evidence scout without usable cache", async () => {
    const { engine, market } = await setup();
    const event = trigger();
    await engine.registerTriggerForResearch(event);
    const unsupported = await engine.submitGrokSignal(
      submission(event, "market_microstructure_scanner", scoutOutput("micro", ["not-trusted"])),
      "market_microstructure_scanner", "scout",
    );
    expect(unsupported.status).toBe("unsupported_evidence");
    expect(engine.hotResearchCacheForTest()).toHaveLength(0);

    const wrongRoleEvent = trigger({ id: createId("trigger"), traceId: createId("trace"), kind: "NEWS_INFORMATION", sourceEventIds: ["news-event"], evidenceIds: ["news-event"] });
    await engine.registerTriggerForResearch(wrongRoleEvent);
    const wrongRole = await engine.submitGrokSignal(
      submission(wrongRoleEvent, "market_microstructure_scanner", scoutOutput("micro", ["news-event"])),
      "market_microstructure_scanner", "scout",
    );
    expect(wrongRole.status).toBe("stale");

    const unsyncEvent = trigger({ id: createId("trigger"), traceId: createId("trace"), sourceEventIds: ["book-event-2"], evidenceIds: ["book-event-2"] });
    await engine.registerTriggerForResearch(unsyncEvent);
    market.markTokenSync("btc-up", "GAPPED");
    const unsynchronized = await engine.submitGrokSignal(
      submission(unsyncEvent, "market_microstructure_scanner", scoutOutput("micro", ["book-event-2"])),
      "market_microstructure_scanner", "scout",
    );
    expect(unsynchronized.status).toBe("stale");

    market.markTokenSync("btc-up", "SYNCED");
    const expiredEvent = trigger({ id: createId("trigger"), traceId: createId("trace"), sourceEventIds: ["book-event-3"], evidenceIds: ["book-event-3"] });
    await engine.registerTriggerForResearch(expiredEvent);
    const expired = await engine.submitGrokSignal(
      submission(expiredEvent, "market_microstructure_scanner", { ...scoutOutput("micro", ["book-event-3"]), expiresAt: new Date(Date.now() - 1).toISOString() }),
      "market_microstructure_scanner", "scout",
    );
    expect(expired.status).toBe("stale");
  });

  it("rehydrates an active scout duplicate from the canonical accepted payload", async () => {
    const { engine } = await setup();
    expect((await engine.beginExperiment()).ok).toBe(true);
    const event = trigger();
    await engine.registerTriggerForResearch(event);
    const first = submission(event, "market_microstructure_scanner", scoutOutput("micro"));
    expect((await engine.submitGrokSignal(first, "market_microstructure_scanner", "scout")).status).toBe("accepted");

    const altered = { ...first, output: { ...first.output, direction: "DOWN" as const } };
    expect((await engine.submitGrokSignal(altered, "market_microstructure_scanner", "scout")).status).toBe("duplicate");
    const cached = engine.hotResearchCacheForTest().find((entry) => entry.triggerId === event.id && entry.role === "market_microstructure_scanner");
    expect(cached?.output.direction).toBe("UP");
  });

  it("treats feed loss after final admission as runtime invalidation, not a stale acceptance", async () => {
    const { engine, journal, market } = await setup();
    expect((await engine.beginExperiment()).ok).toBe(true);
    const event = trigger();
    await engine.registerTriggerForResearch(event);
    const first = submission(event, "market_microstructure_scanner", scoutOutput("micro"));
    let release!: () => void;
    journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["GROK_SIGNAL_ACCEPTED"]);
    try {
      const pending = engine.submitGrokSignal(first, "market_microstructure_scanner", "scout");
      await waitFor(() => journal.inFlightAppends > 0);
      market.markTokenSync("btc-up", "GAPPED");
      release();
      expect((await pending).status).toBe("accepted");
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
      expect(engine.hotResearchCacheForTest()).toHaveLength(0);
    } finally {
      release();
      journal.setAppendInFlightHoldForTest(null);
    }
  });
});
