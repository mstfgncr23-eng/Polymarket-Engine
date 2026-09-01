import { createHash, timingSafeEqual } from "node:crypto";
import type { FastifyReply, FastifyRequest } from "fastify";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import {
  createId,
  grokSignalSubmissionSchema,
  parseGrokWritePayload,
  clientMayCallWriteTool,
  GrokSignalValidationError,
  type GrokSignalSubmission,
} from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";
import { buildExperimentSummary } from "./report.js";
import type { EngineStateStore } from "./state.js";

export interface McpConfig {
  bearerTokens: string;
  allowedOrigins: string;
  rateLimitPerMinute: number;
}

export interface McpDependencies {
  state: EngineStateStore;
  journal: EventJournal;
  metrics: EngineMetrics;
  submitSignal(signal: GrokSignalSubmission, clientId: string): Promise<Record<string, unknown>>;
}

type AuthResult = { ok: true; clientId: string; tokenFingerprint: string } | { ok: false; status: 401 | 503; reason: string; tokenFingerprint: string | null };

export const ALLOWED_MCP_READ_TOOLS = [
  "get_markets",
  "get_market_snapshot",
  "get_orderbook",
  "get_recent_triggers",
  "get_positions",
  "get_portfolio",
  "get_strategy_metrics",
  "get_recent_decisions",
  "get_system_health",
] as const;

export const ALLOWED_MCP_WRITE_TOOLS = [
  "submit_signal",
  "submit_analysis",
  "recommend_hedge",
  "recommend_veto",
] as const;

export const ALLOWED_MCP_TOOLS = new Set<string>([...ALLOWED_MCP_READ_TOOLS, ...ALLOWED_MCP_WRITE_TOOLS]);

export const FORBIDDEN_MCP_TRADE_TOOLS = new Set([
  "place_order", "submit_order", "create_order", "cancel_order", "replace_order",
  "sign_order", "sign_transaction", "get_private_key", "get_wallet", "wallet",
  "place_trade", "execute_trade", "live_order", "clob_order", "submit_live_order",
  "authenticate_clob", "get_signer",
]);

const grokWriteInputShape = {
  submissionId: z.string().min(1).max(160),
  triggerId: z.string().min(1).max(160),
  traceId: z.string().min(1).max(160),
  correlationId: z.string().min(1).max(160),
  marketId: z.string().min(1).max(160),
  timestamp: z.string().min(1),
  agentId: z.enum([
    "market_microstructure_scanner",
    "crypto_market_scanner",
    "news_x_intelligence_scanner",
    "fair_probability_analyst",
    "risk_hedge_analyst",
    "strategy_supervisor",
  ]),
  role: z.enum([
    "market_microstructure_scanner",
    "crypto_market_scanner",
    "news_x_intelligence_scanner",
    "fair_probability_analyst",
    "risk_hedge_analyst",
    "strategy_supervisor",
  ]),
  output: z.record(z.string(), z.unknown()),
};

export class McpSecurityGate {
  private readonly tokens: Array<{ clientId: string; digest: Buffer; fingerprint: string }>;
  private readonly origins: Set<string>;
  private readonly requestTimes = new Map<string, number[]>();

  constructor(private readonly config: McpConfig) {
    this.tokens = config.bearerTokens.split(",").map((entry) => entry.trim()).filter(Boolean).flatMap((entry) => {
      const delimiter = entry.indexOf(":");
      if (delimiter <= 0 || delimiter === entry.length - 1) return [];
      const clientId = entry.slice(0, delimiter).trim();
      const token = entry.slice(delimiter + 1).trim();
      const digest = sha256(token);
      return clientId && token ? [{ clientId, digest, fingerprint: digest.toString("hex").slice(0, 12) }] : [];
    });
    this.origins = new Set(config.allowedOrigins.split(",").map((origin) => origin.trim()).filter(Boolean));
  }

  authenticate(header: string | undefined): AuthResult {
    if (this.tokens.length === 0) return { ok: false, status: 503, reason: "MCP_AUTH_NOT_CONFIGURED", tokenFingerprint: null };
    if (!header?.startsWith("Bearer ")) return { ok: false, status: 401, reason: "INVALID_BEARER_TOKEN", tokenFingerprint: null };
    const candidate = sha256(header.slice(7));
    const match = this.tokens.find((token) => timingSafeEqual(candidate, token.digest));
    if (!match) return { ok: false, status: 401, reason: "INVALID_BEARER_TOKEN", tokenFingerprint: candidate.toString("hex").slice(0, 12) };
    return { ok: true, clientId: match.clientId, tokenFingerprint: match.fingerprint };
  }

  originAllowed(origin: string | undefined): boolean {
    return origin === undefined || this.origins.has(origin);
  }

  consume(clientId: string, now = Date.now()): boolean {
    const recent = (this.requestTimes.get(clientId) ?? []).filter((at) => now - at < 60_000);
    if (recent.length >= this.config.rateLimitPerMinute) { this.requestTimes.set(clientId, recent); return false; }
    recent.push(now); this.requestTimes.set(clientId, recent); return true;
  }
}

export function createMcpHttpHandler(config: McpConfig, dependencies: McpDependencies) {
  const security = new McpSecurityGate(config);

  return async (request: FastifyRequest, reply: FastifyReply): Promise<unknown> => {
    const started = performance.now();
    const method = extractRpcMethod(request.body) ?? request.method;
    const toolName = extractToolName(request.body);
    const requestId = extractRpcId(request.body);
    const auth = security.authenticate(request.headers.authorization);
    if (!auth.ok) {
      dependencies.state.recordGrokRejection("unauthorized");
      dependencies.metrics.mcpInteractions.inc({ status: auth.status === 503 ? "unconfigured" : "unauthorized", method });
      await audit(dependencies.journal, {
        clientId: "unauthorized", tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: auth.reason, rateLimitResult: "not_evaluated", durationMs: performance.now() - started,
      });
      reply.header("WWW-Authenticate", "Bearer");
      return reply.code(auth.status).send({ error: auth.reason });
    }
    if (!security.originAllowed(request.headers.origin)) {
      dependencies.metrics.mcpInteractions.inc({ status: "origin_rejected", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "ORIGIN_REJECTED", rateLimitResult: "not_evaluated", durationMs: performance.now() - started,
      });
      return reply.code(403).send({ error: "ORIGIN_REJECTED" });
    }
    if (!security.consume(auth.clientId)) {
      dependencies.state.recordGrokRejection("rate_limited");
      dependencies.metrics.mcpInteractions.inc({ status: "rate_limited", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "RATE_LIMITED", rateLimitResult: "limited", durationMs: performance.now() - started,
      });
      return reply.code(429).send({ error: "RATE_LIMITED" });
    }
    if (request.method !== "POST") {
      dependencies.metrics.mcpInteractions.inc({ status: "method_not_allowed", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "METHOD_NOT_ALLOWED", rateLimitResult: "ok", durationMs: performance.now() - started,
      });
      return reply.header("Allow", "POST").code(405).send({ error: "METHOD_NOT_ALLOWED" });
    }

    if (toolName && ALLOWED_MCP_WRITE_TOOLS.includes(toolName as (typeof ALLOWED_MCP_WRITE_TOOLS)[number]) && !clientMayCallWriteTool(auth.clientId, toolName)) {
      dependencies.state.recordGrokRejection("rejected");
      dependencies.metrics.mcpInteractions.inc({ status: "write_tool_forbidden", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "WRITE_TOOL_FORBIDDEN", rateLimitResult: "ok", durationMs: performance.now() - started,
        payloadMeta: { argumentKeys: argumentKeys(request.body), agentId: auth.clientId },
      });
      return reply.code(200).send({
        jsonrpc: "2.0",
        id: requestId,
        error: { code: -32601, message: "WRITE_TOOL_FORBIDDEN" },
      });
    }

    if (toolName && !ALLOWED_MCP_TOOLS.has(toolName)) {
      const forbidden = FORBIDDEN_MCP_TRADE_TOOLS.has(toolName) || looksLikeTradeTool(toolName);
      const status = forbidden ? "FORBIDDEN_TRADE_TOOL" : "UNKNOWN_TOOL";
      dependencies.state.recordGrokRejection("rejected");
      dependencies.metrics.mcpInteractions.inc({ status: forbidden ? "forbidden_trade" : "unknown_tool", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status, rateLimitResult: "ok", durationMs: performance.now() - started,
        payloadMeta: { argumentKeys: argumentKeys(request.body) },
      });
      return reply.code(200).send({
        jsonrpc: "2.0",
        id: requestId,
        error: { code: -32601, message: status },
      });
    }

    const server = createGrokMcpServer(auth.clientId, dependencies);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    try {
      await server.connect(transport);
      reply.hijack();
      await transport.handleRequest(request.raw, reply.raw, request.body);
      dependencies.metrics.mcpInteractions.inc({ status: "handled", method });
      dependencies.metrics.mcpLatency.observe({ method }, (performance.now() - started) / 1000);
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "HANDLED", rateLimitResult: "ok", durationMs: performance.now() - started,
        ...submissionAuditMeta(request.body),
      });
      return reply;
    } catch (error) {
      dependencies.metrics.mcpInteractions.inc({ status: "error", method });
      await audit(dependencies.journal, {
        clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, tool: toolName, requestId,
        status: "ERROR", rateLimitResult: "ok", durationMs: performance.now() - started, errorCode: safeErrorCode(error),
      });
      if (!reply.raw.headersSent) reply.raw.writeHead(500, { "Content-Type": "application/json" });
      if (!reply.raw.writableEnded) reply.raw.end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32603, message: "Internal server error" }, id: requestId }));
      return reply;
    } finally {
      await transport.close().catch(() => undefined);
      await server.close().catch(() => undefined);
    }
  };
}

export function createGrokMcpServer(clientId: string, dependencies: McpDependencies): McpServer {
  const server = new McpServer({ name: "polymarket-grok-research", version: "1.0.0" });
  const readOnly = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } as const;
  const write = { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false } as const;

  server.registerTool("get_markets", {
    description: "List currently observed Polymarket markets from persisted snapshots. Read-only; no trading capability.",
    inputSchema: { limit: z.number().int().min(1).max(100).default(25) }, annotations: readOnly,
  }, async ({ limit }) => {
    const markets = uniqueMarkets(dependencies.state).slice(0, limit);
    return toolResult(markets);
  });

  server.registerTool("get_market_snapshot", {
    description: "Read the latest normalized snapshots for one market. Read-only; no trading capability.",
    inputSchema: { marketId: z.string().min(1) }, annotations: readOnly,
  }, async ({ marketId }) => toolResult([...dependencies.state.marketSnapshots.values()].filter((snapshot) => snapshot.marketId === marketId)));

  server.registerTool("get_orderbook", {
    description: "Read one current normalized order book by token ID. Read-only; no trading capability.",
    inputSchema: { tokenId: z.string().min(1) }, annotations: readOnly,
  }, async ({ tokenId }) => toolResult(dependencies.state.marketSnapshots.get(tokenId) ?? null));

  server.registerTool("get_recent_triggers", {
    description: "Read recent deterministic triggers available for external research. Monitoring never waits for Grok.",
    inputSchema: { limit: z.number().int().min(1).max(100).default(25) }, annotations: readOnly,
  }, async ({ limit }) => toolResult(dependencies.state.recentTriggers.slice(0, limit)));

  server.registerTool("get_positions", {
    description: "Read paper positions. This tool cannot modify positions or place orders.",
    inputSchema: {}, annotations: readOnly,
  }, async () => toolResult(dependencies.state.snapshot().positions));

  server.registerTool("get_portfolio", {
    description: "Read paper ledger, cash, PnL, and positions. This tool cannot modify the portfolio.",
    inputSchema: {}, annotations: readOnly,
  }, async () => {
    const snapshot = dependencies.state.snapshot();
    return toolResult({ balance: snapshot.balance, positions: snapshot.positions, experiment: snapshot.engine });
  });

  server.registerTool("get_strategy_metrics", {
    description: "Read journal-derived paper strategy and experiment performance.",
    inputSchema: {}, annotations: readOnly,
  }, async () => toolResult(buildExperimentSummary(await dependencies.journal.readAll(), dependencies.state.experimentId ?? "unknown")));

  server.registerTool("get_recent_decisions", {
    description: "Read the recent persisted decision feed (triggers, consensus, risk, paper fills).",
    inputSchema: { limit: z.number().int().min(1).max(100).default(25) }, annotations: readOnly,
  }, async ({ limit }) => toolResult(dependencies.state.snapshot().decisionFeed.slice(0, limit)));

  server.registerTool("get_system_health", {
    description: "Read engine liveness/readiness, feed, database, and Grok Bot activity counters. No secrets.",
    inputSchema: {}, annotations: readOnly,
  }, async () => {
    const snapshot = dependencies.state.snapshot();
    return toolResult({
      engine: snapshot.engine, health: snapshot.health, grokActivity: snapshot.grokActivity, paperMode: snapshot.paperMode,
    });
  });

  const writeHandler = (tool: (typeof ALLOWED_MCP_WRITE_TOOLS)[number]) => async (raw: Record<string, unknown>) => {
    try {
      const submission = parseGrokWritePayload(tool, raw, clientId);
      const result = await dependencies.submitSignal(submission, clientId);
      return toolResult(result);
    } catch (error) {
      dependencies.state.recordGrokRejection("rejected");
      if (error instanceof z.ZodError) {
        return errorResult("MALFORMED_SIGNAL", error.issues.map((issue) => issue.message).slice(0, 8));
      }
      if (error instanceof GrokSignalValidationError) return errorResult(error.code, error.message);
      throw error;
    }
  };

  server.registerTool("submit_signal", {
    description: "Submit one schema-validated Grok Bot research signal. Never creates an order and exposes no wallet capability.",
    inputSchema: grokWriteInputShape, annotations: write,
  }, writeHandler("submit_signal"));

  server.registerTool("submit_analysis", {
    description: "Submit a fair-probability analysis. Requires agentId/role fair_probability_analyst. Never places an order.",
    inputSchema: grokWriteInputShape, annotations: write,
  }, writeHandler("submit_analysis"));

  server.registerTool("recommend_hedge", {
    description: "Recommend a hedge from the risk & hedge analyst. Cannot approve risk or place an order.",
    inputSchema: grokWriteInputShape, annotations: write,
  }, writeHandler("recommend_hedge"));

  server.registerTool("recommend_veto", {
    description: "Recommend an absolute research veto. The engine still applies deterministic risk; this cannot place an order.",
    inputSchema: grokWriteInputShape, annotations: write,
  }, writeHandler("recommend_veto"));

  return server;
}

function uniqueMarkets(state: EngineStateStore) {
  const markets = new Map<string, { marketId: string; outcomes: Array<{ tokenId: string; outcome: string }> }>();
  for (const snapshot of state.marketSnapshots.values()) {
    const existing = markets.get(snapshot.marketId) ?? { marketId: snapshot.marketId, outcomes: [] };
    if (!existing.outcomes.some((item) => item.tokenId === snapshot.tokenId)) {
      existing.outcomes.push({ tokenId: snapshot.tokenId, outcome: snapshot.outcome });
    }
    markets.set(snapshot.marketId, existing);
  }
  return [...markets.values()];
}

function toolResult(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value) }], structuredContent: { result: value } };
}

function errorResult(code: string, detail: unknown) {
  return { isError: true as const, content: [{ type: "text" as const, text: JSON.stringify({ error: code, detail }) }] };
}

function sha256(value: string): Buffer { return createHash("sha256").update(value, "utf8").digest(); }

function extractRpcMethod(body: unknown): string | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const method = (body as { method?: unknown }).method;
  if (method !== "tools/call") return typeof method === "string" ? method : null;
  const name = (body as { params?: { name?: unknown } }).params?.name;
  return typeof name === "string" ? `tools/call:${name}` : method;
}

function extractToolName(body: unknown): string | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  if ((body as { method?: unknown }).method !== "tools/call") return null;
  const name = (body as { params?: { name?: unknown } }).params?.name;
  return typeof name === "string" ? name : null;
}

function extractRpcId(body: unknown): string | number | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const id = (body as { id?: unknown }).id;
  return typeof id === "string" || typeof id === "number" ? id : null;
}

function argumentKeys(body: unknown): string[] {
  if (!body || typeof body !== "object" || Array.isArray(body)) return [];
  const args = (body as { params?: { arguments?: unknown } }).params?.arguments;
  if (!args || typeof args !== "object" || Array.isArray(args)) return [];
  return Object.keys(args).slice(0, 24);
}

function submissionAuditMeta(body: unknown): { idempotencyId?: string; traceId?: string; triggerId?: string; payloadMeta: Record<string, unknown> } {
  const args = (!body || typeof body !== "object" || Array.isArray(body))
    ? null
    : (body as { params?: { arguments?: Record<string, unknown> } }).params?.arguments ?? null;
  const meta = grokSignalSubmissionSchema.safeParse(args);
  return {
    idempotencyId: typeof args?.submissionId === "string" ? args.submissionId : undefined,
    traceId: typeof args?.traceId === "string" ? args.traceId : undefined,
    triggerId: typeof args?.triggerId === "string" ? args.triggerId : undefined,
    payloadMeta: {
      argumentKeys: args ? Object.keys(args).slice(0, 24) : [],
      agentId: typeof args?.agentId === "string" ? args.agentId : null,
      role: typeof args?.role === "string" ? args.role : null,
      marketId: typeof args?.marketId === "string" ? args.marketId : null,
      schemaValid: meta.success,
    },
  };
}

function looksLikeTradeTool(name: string): boolean {
  return /(place|submit|create|cancel|sign).*(order|trade|wallet)|wallet|private_key|signer|clob_auth/i.test(name);
}

function safeErrorCode(error: unknown): string { return error instanceof Error ? error.name.slice(0, 80) : "UNKNOWN_ERROR"; }

async function audit(journal: EventJournal, input: {
  clientId: string; tokenFingerprint: string | null; method: string; tool: string | null; requestId: string | number | null;
  status: string; rateLimitResult: string; durationMs: number; errorCode?: string; idempotencyId?: string; traceId?: string;
  triggerId?: string; payloadMeta?: Record<string, unknown>;
}): Promise<void> {
  await journal.append(createJournalEvent({
    traceId: input.traceId ?? createId("mcp_trace"),
    correlationId: input.idempotencyId ?? (input.requestId !== null ? String(input.requestId) : createId("mcp_req")),
    type: "MCP_INTERACTION", aggregateType: "mcp_client", aggregateId: input.clientId,
    payload: {
      clientId: input.clientId,
      tokenFingerprint: input.tokenFingerprint,
      method: input.method,
      tool: input.tool,
      requestId: input.requestId,
      idempotencyId: input.idempotencyId ?? null,
      status: input.status,
      rateLimitResult: input.rateLimitResult,
      traceId: input.traceId ?? null,
      triggerId: input.triggerId ?? null,
      payloadMeta: input.payloadMeta ?? {},
      durationMs: Math.max(0, Math.round(input.durationMs * 1000) / 1000),
      errorCode: input.errorCode ?? null,
    },
  })).catch(() => undefined);
}
