import { describe, expect, it } from "vitest";
import { createId, engineEnvSchema, fairProbabilityOutputSchema, grokSignalSubmissionSchema, parseGrokWritePayload, GrokSignalValidationError, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { ALLOWED_MCP_TOOLS, FORBIDDEN_MCP_TRADE_TOOLS, McpSecurityGate } from "../apps/engine/src/mcp.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { snapshot } from "./fixtures.js";

const nowIso = () => new Date().toISOString();
const futureIso = (ms = 30_000) => new Date(Date.now() + ms).toISOString();

function researchTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc", kind: "BOOK_IMBALANCE", severity: 2, score: 1,
    dedupeKey: "x", evidence: {}, sourceEventIds: ["event"], createdAt: nowIso(),
    expiresAt: futureIso(60_000), ...overrides,
  };
}

function microstructureOutput(overrides: Record<string, unknown> = {}) {
  return {
    confidence: 0.8, evidenceIds: ["e1"], expiresAt: futureIso(), reasonCodes: ["TEST"],
    direction: "UP", spread: "0.01", imbalance: 0.4, depthChange: 0.1, pairCost: "0.93", unusualConditions: [],
    ...overrides,
  };
}

function fairOutput(overrides: Record<string, unknown> = {}) {
  return {
    confidence: 0.82, evidenceIds: ["e1"], expiresAt: futureIso(), reasonCodes: ["TEST"],
    direction: "UP", fairProbability: 0.61, marketProbability: 0.48, estimatedEdge: 0.04,
    ...overrides,
  };
}

function riskOutput(overrides: Record<string, unknown> = {}) {
  return {
    confidence: 0.7, evidenceIds: ["e1"], expiresAt: futureIso(), reasonCodes: ["TEST"],
    recommendVeto: false, directionalExposureUsd: "10", unhedgedDurationMs: 0, pairedCost: "0.93", adverseScenarios: [],
    ...overrides,
  };
}

function submission(trigger: TriggerEvent, agentId: "market_microstructure_scanner" | "fair_probability_analyst" | "risk_hedge_analyst", output: Record<string, unknown>, extra: Record<string, unknown> = {}) {
  return grokSignalSubmissionSchema.parse({
    submissionId: createId("sub"), triggerId: trigger.id, traceId: trigger.traceId, correlationId: createId("corr"),
    marketId: trigger.marketId ?? "btc", timestamp: nowIso(), agentId, role: agentId, output, ...extra,
  });
}

async function testEngine() {
  const journal = new MemoryEventJournal();
  const env = engineEnvSchema.parse({
    NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
    GROK_MCP_BEARER_TOKENS: "scanner:super-secret-token", GROK_MCP_RATE_LIMIT_PER_MINUTE: "30",
  });
  const engine = new EngineService(env, journal, { error: () => undefined });
  return { journal, engine };
}

describe("Grok MCP auth and tool policy", () => {
  it("authenticates high-entropy bearer tokens by fingerprint and never stores the raw secret", () => {
    const gate = new McpSecurityGate({ bearerTokens: "scanner:super-secret-token", allowedOrigins: "https://grok.com", rateLimitPerMinute: 2 });
    const ok = gate.authenticate("Bearer super-secret-token");
    expect(ok).toMatchObject({ ok: true, clientId: "scanner" });
    if (ok.ok) expect(ok.tokenFingerprint).toHaveLength(12);
    expect(gate.authenticate("Bearer wrong")).toMatchObject({ ok: false, status: 401 });
    expect(new McpSecurityGate({ bearerTokens: "", allowedOrigins: "", rateLimitPerMinute: 1 }).authenticate("Bearer x")).toMatchObject({ ok: false, status: 503 });
  });

  it("does not expose any trade, wallet, or order tool name", () => {
    expect(ALLOWED_MCP_TOOLS.has("place_order")).toBe(false);
    expect(FORBIDDEN_MCP_TRADE_TOOLS.has("place_order")).toBe(true);
    expect([...ALLOWED_MCP_TOOLS]).toEqual(expect.arrayContaining([
      "get_markets", "get_market_snapshot", "get_orderbook", "get_recent_triggers", "get_positions",
      "get_portfolio", "get_strategy_metrics", "get_recent_decisions", "get_system_health", "get_reference_context",
      "submit_signal", "submit_analysis", "recommend_hedge", "recommend_veto",
    ]));
  });
});

describe("Grok signal inbox", () => {
  it("rejects malformed, stale, duplicate, and unknown-trigger submissions without creating positions", async () => {
    const { engine } = await testEngine();
    const trigger = researchTrigger();
    await engine.registerTriggerForResearch(trigger);
    expect(await engine.submitGrokSignal({ ...(submission(trigger, "fair_probability_analyst", fairOutput())), output: { ...fairOutput(), placeLiveOrder: true } } as never, "scanner")).toMatchObject({ status: "malformed" });
    expect(fairProbabilityOutputSchema.safeParse({ ...fairOutput(), fairProbability: 3, placeLiveOrder: true }).success).toBe(false);

    const stale = submission(trigger, "fair_probability_analyst", fairOutput({ expiresAt: new Date(Date.now() - 1_000).toISOString() }));
    expect(await engine.submitGrokSignal(stale, "fair_probability_analyst")).toMatchObject({ status: "stale" });

    const replay = submission(trigger, "market_microstructure_scanner", microstructureOutput(), { timestamp: new Date(Date.now() - 10 * 60_000).toISOString() });
    expect(await engine.submitGrokSignal(replay, "market_microstructure_scanner")).toMatchObject({ status: "replay_rejected" });

    expect(await engine.submitGrokSignal(submission({ ...trigger, id: "missing" }, "fair_probability_analyst", fairOutput()), "fair_probability_analyst")).toMatchObject({ status: "unknown_trigger" });

    const first = submission(trigger, "market_microstructure_scanner", microstructureOutput());
    expect(await engine.submitGrokSignal(first, "market_microstructure_scanner")).toMatchObject({ status: "accepted" });
    expect(await engine.submitGrokSignal(first, "market_microstructure_scanner")).toMatchObject({ status: "duplicate" });
    expect(engine.ledger.positions.size).toBe(0);
    expect(await engine.submitGrokSignal(submission(trigger, "fair_probability_analyst", fairOutput()), "market_microstructure_scanner")).toMatchObject({ status: "identity_mismatch" });
  });

  it("assembles consensus only after minimum fresh role evidence and does not invent orders without a live book", async () => {
    const { engine } = await testEngine();
    engine.state.marketSnapshots.set("up", snapshot({ marketId: "btc", tokenId: "up" }));
    const trigger = researchTrigger();
    await engine.registerTriggerForResearch(trigger);
    await engine.submitGrokSignal(submission(trigger, "market_microstructure_scanner", microstructureOutput()), "market_microstructure_scanner");
    expect(engine.state.latestConsensus).toBeNull();
    await engine.submitGrokSignal(submission(trigger, "fair_probability_analyst", fairOutput()), "fair_probability_analyst");
    expect(engine.state.latestConsensus?.consensusDirection).toBe("UP");
    expect(engine.ledger.positions.size).toBe(0);
  });

  it("maps write tools onto the inbox and rejects role mismatches", () => {
    const trigger = researchTrigger();
    const analysis = submission(trigger, "fair_probability_analyst", fairOutput());
    expect(parseGrokWritePayload("submit_analysis", analysis, "fair_probability_analyst").agentId).toBe("fair_probability_analyst");
    expect(() => parseGrokWritePayload("submit_analysis", submission(trigger, "market_microstructure_scanner", microstructureOutput()), "market_microstructure_scanner")).toThrow(GrokSignalValidationError);
    expect(() => parseGrokWritePayload("recommend_veto", submission(trigger, "risk_hedge_analyst", riskOutput({ recommendVeto: false })), "risk_hedge_analyst")).toThrow(/VETO_RECOMMENDATION_REQUIRED/);
    expect(parseGrokWritePayload("recommend_hedge", submission(trigger, "risk_hedge_analyst", riskOutput({ recommendVeto: false })), "risk_hedge_analyst").agentId).toBe("risk_hedge_analyst");
  });
});

const ROLE_TOKENS = [
  "market_microstructure_scanner:scanner-secret",
  "fair_probability_analyst:fair-secret",
  "risk_hedge_analyst:risk-secret",
  "strategy_supervisor:supervisor-secret",
].join(",");

describe("authenticated MCP HTTP", () => {
  async function listen(tokens = ROLE_TOKENS, rateLimitPerMinute = 30) {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      GROK_MCP_BEARER_TOKENS: tokens, GROK_MCP_RATE_LIMIT_PER_MINUTE: String(rateLimitPerMinute),
    });
    const engine = new EngineService(env, journal, { error: () => undefined });
    const trigger = researchTrigger();
    await engine.registerTriggerForResearch(trigger);
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: tokens, allowedOrigins: "https://grok.com,https://x.ai", rateLimitPerMinute },
      submitGrokSignal: (signal, clientId) => engine.submitGrokSignal(signal, clientId),
    });
    await server.listen({ host: "127.0.0.1", port: 0 });
    const address = server.server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    return { server, engine, journal, trigger, url: `http://127.0.0.1:${port}/mcp` };
  }

  async function rpc(url: string, body: unknown, headers: Record<string, string> = {}) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        authorization: "Bearer scanner-secret",
        accept: "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": "2025-11-25",
        ...headers,
      },
      body: JSON.stringify(body),
    });
    const text = await response.text();
    let json: unknown = null;
    try {
      if (text.includes("\ndata:")) {
        const data = text.split("\n").filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim()).at(-1);
        json = data ? JSON.parse(data) : text;
      } else json = JSON.parse(text);
    } catch { json = text; }
    return { status: response.status, json, text };
  }

  it("get_reference_context is authenticated, schema-validated, and read-only", async () => {
    const ctx = await listen();
    try {
      expect((await rpc(ctx.url, {
        jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "get_reference_context", arguments: { asset: "BTC" } },
      }, { authorization: "Bearer nope" })).status).toBe(401);
      ctx.engine.state.noteReferenceTick("BTC", new Date().toISOString(), { symbol: "BTCUSDT", venue: "binance", price: "65000.1" });
      const ok = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "get_reference_context", arguments: { asset: "BTC" } },
      });
      expect(ok.status).toBe(200);
      const text = JSON.stringify(ok.json);
      expect(text).toContain("BTCUSDT");
      expect(text).toContain("65000.1");
      expect(text).not.toContain("place_order");
      expect(text).not.toMatch(/"ticks":\s*\[/);
      const missing = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 3, method: "tools/call",
        params: { name: "get_reference_context", arguments: {} },
      });
      expect(JSON.stringify(missing.json)).toMatch(/invalid_type|required|error/i);
    } finally { await ctx.server.close(); }
  });

  it("rejects unauthorized, origin-invalid, GET, unconfigured, rate-limited, and forbidden trade tools", async () => {
    const configured = await listen();
    try {
      expect((await rpc(configured.url, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }, { authorization: "Bearer nope" })).status).toBe(401);
      expect((await rpc(configured.url, { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }, { origin: "https://evil.example" })).status).toBe(403);
      const get = await fetch(configured.url, { method: "GET", headers: { authorization: "Bearer scanner-secret" } });
      expect(get.status).toBe(405);
      const forbidden = await rpc(configured.url, { jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "place_order", arguments: { marketId: "btc" } } });
      expect(forbidden.status).toBe(200);
      expect(forbidden.json).toMatchObject({ error: { message: "FORBIDDEN_TRADE_TOOL" } });
      const unknown = await rpc(configured.url, { jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "not_a_real_tool", arguments: {} } });
      expect(unknown.json).toMatchObject({ error: { message: "UNKNOWN_TOOL" } });
      const events = await configured.journal.readAll();
      expect(events.some((event) => event.type === "MCP_INTERACTION" && JSON.stringify(event.payload).includes("super-secret-token"))).toBe(false);
      expect(events.some((event) => event.type === "MCP_INTERACTION" && (event.payload as { tokenFingerprint?: unknown }).tokenFingerprint)).toBe(true);
    } finally { await configured.server.close(); }

    const unconfigured = await listen("");
    try {
      expect((await rpc(unconfigured.url, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} })).status).toBe(503);
    } finally { await unconfigured.server.close(); }

    const limited = await listen(ROLE_TOKENS, 1);
    try {
      const first = await rpc(limited.url, { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "place_order", arguments: {} } });
      expect(first.status).toBe(200);
      expect((await rpc(limited.url, { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "place_order", arguments: {} } })).status).toBe(429);
    } finally { await limited.server.close(); }
  });

  it("lists only research tools and accepts a validated submit_signal", async () => {
    const ctx = await listen();
    try {
      const listed = await rpc(ctx.url, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} });
      const names = ((listed.json as { result?: { tools?: Array<{ name: string }> } }).result?.tools ?? []).map((tool) => tool.name).sort();
      expect(names).toEqual([...ALLOWED_MCP_TOOLS].sort());
      expect(names).not.toContain("place_order");

      const payload = submission(ctx.trigger, "market_microstructure_scanner", microstructureOutput());
      const submitted = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "submit_signal", arguments: payload },
      });
      expect(submitted.status).toBe(200);
      expect(JSON.stringify(submitted.json)).toContain("accepted");
      expect(ctx.engine.state.grokActivity.acceptedSignals).toBe(1);

      const malformed = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 3, method: "tools/call",
        params: { name: "submit_signal", arguments: { ...payload, submissionId: createId("sub2"), output: { ...microstructureOutput(), confidence: 9, placeLiveOrder: true } } },
      });
      expect(JSON.stringify(malformed.json)).toMatch(/MALFORMED_SIGNAL|error/i);
    } finally { await ctx.server.close(); }
  });

  it("scanner token cannot impersonate fair_probability_analyst", async () => {
    const ctx = await listen();
    try {
      const impersonating = submission(ctx.trigger, "fair_probability_analyst", fairOutput());
      const result = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "submit_signal", arguments: impersonating },
      });
      expect(JSON.stringify(result.json)).toContain("IDENTITY_CLIENT_MISMATCH");
      expect(ctx.engine.state.grokActivity.acceptedSignals).toBe(0);
      expect(ctx.engine.state.latestAgents.size).toBe(0);
    } finally { await ctx.server.close(); }
  });

  it("scanner token cannot recommend_veto", async () => {
    const ctx = await listen();
    try {
      const veto = submission(ctx.trigger, "risk_hedge_analyst", riskOutput({ recommendVeto: true }));
      const result = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "recommend_veto", arguments: veto },
      });
      expect(result.json).toMatchObject({ error: { message: "WRITE_TOOL_FORBIDDEN" } });
      expect(ctx.engine.state.grokActivity.acceptedSignals).toBe(0);
    } finally { await ctx.server.close(); }
  });

  it("fair_probability token cannot submit as risk_hedge_analyst", async () => {
    const ctx = await listen();
    try {
      const impersonating = submission(ctx.trigger, "risk_hedge_analyst", riskOutput({ recommendVeto: true }));
      const result = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "submit_analysis", arguments: impersonating },
      }, { authorization: "Bearer fair-secret" });
      expect(JSON.stringify(result.json)).toContain("IDENTITY_CLIENT_MISMATCH");
      expect(ctx.engine.state.grokActivity.acceptedSignals).toBe(0);
    } finally { await ctx.server.close(); }
  });

  it("correct token + correct role + correct tool succeeds", async () => {
    const ctx = await listen();
    try {
      const payload = submission(ctx.trigger, "market_microstructure_scanner", microstructureOutput());
      const result = await rpc(ctx.url, {
        jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "submit_signal", arguments: payload },
      }, { authorization: "Bearer scanner-secret" });
      expect(result.status).toBe(200);
      expect(JSON.stringify(result.json)).toContain("accepted");
      expect(ctx.engine.state.grokActivity.acceptedSignals).toBe(1);
      expect(ctx.engine.state.latestAgents.get("market_microstructure_scanner")?.agentId).toBe("market_microstructure_scanner");
    } finally { await ctx.server.close(); }
  });
});
