import { createHash, timingSafeEqual } from "node:crypto";
import type { FastifyReply, FastifyRequest } from "fastify";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { createId, grokSignalSubmissionSchema, type GrokSignalSubmission } from "@pm/core";
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
    const auth = security.authenticate(request.headers.authorization);
    if (!auth.ok) {
      dependencies.state.recordGrokRejection("unauthorized");
      dependencies.metrics.mcpInteractions.inc({ status: auth.status === 503 ? "unconfigured" : "unauthorized", method });
      await audit(dependencies.journal, { clientId: "unauthorized", tokenFingerprint: auth.tokenFingerprint, method, status: auth.reason, durationMs: performance.now() - started });
      reply.header("WWW-Authenticate", "Bearer");
      return reply.code(auth.status).send({ error: auth.reason });
    }
    if (!security.originAllowed(request.headers.origin)) {
      dependencies.metrics.mcpInteractions.inc({ status: "origin_rejected", method });
      await audit(dependencies.journal, { clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, status: "ORIGIN_REJECTED", durationMs: performance.now() - started });
      return reply.code(403).send({ error: "ORIGIN_REJECTED" });
    }
    if (!security.consume(auth.clientId)) {
      dependencies.state.recordGrokRejection("rate_limited");
      dependencies.metrics.mcpInteractions.inc({ status: "rate_limited", method });
      await audit(dependencies.journal, { clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, status: "RATE_LIMITED", durationMs: performance.now() - started });
      return reply.code(429).send({ error: "RATE_LIMITED" });
    }
    if (request.method !== "POST") {
      dependencies.metrics.mcpInteractions.inc({ status: "method_not_allowed", method });
      await audit(dependencies.journal, { clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, status: "METHOD_NOT_ALLOWED", durationMs: performance.now() - started });
      return reply.header("Allow", "POST").code(405).send({ error: "METHOD_NOT_ALLOWED" });
    }

    const server = createGrokMcpServer(auth.clientId, dependencies);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    try {
      await server.connect(transport);
      reply.hijack();
      await transport.handleRequest(request.raw, reply.raw, request.body);
      dependencies.metrics.mcpInteractions.inc({ status: "handled", method });
      dependencies.metrics.mcpLatency.observe({ method }, (performance.now() - started) / 1000);
      await audit(dependencies.journal, { clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, status: "HANDLED", durationMs: performance.now() - started });
      return reply;
    } catch (error) {
      dependencies.metrics.mcpInteractions.inc({ status: "error", method });
      await audit(dependencies.journal, { clientId: auth.clientId, tokenFingerprint: auth.tokenFingerprint, method, status: "ERROR", durationMs: performance.now() - started, errorCode: safeErrorCode(error) });
      if (!reply.raw.headersSent) reply.raw.writeHead(500, { "Content-Type": "application/json" });
      if (!reply.raw.writableEnded) reply.raw.end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32603, message: "Internal server error" }, id: null }));
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

  server.registerTool("get_market_snapshots", {
    description: "Read current normalized Polymarket snapshots. No trading capability.",
    inputSchema: { marketId: z.string().optional(), tokenId: z.string().optional() }, annotations: readOnly,
  }, async ({ marketId, tokenId }) => toolResult([...dependencies.state.marketSnapshots.values()].filter((snapshot) => (!marketId || snapshot.marketId === marketId) && (!tokenId || snapshot.tokenId === tokenId))));

  server.registerTool("get_order_book", {
    description: "Read one current normalized order book by token ID.",
    inputSchema: { tokenId: z.string().min(1) }, annotations: readOnly,
  }, async ({ tokenId }) => toolResult(dependencies.state.marketSnapshots.get(tokenId) ?? null));

  server.registerTool("get_triggers", {
    description: "Read recent deterministic triggers available for external research.",
    inputSchema: { limit: z.number().int().min(1).max(100).default(25) }, annotations: readOnly,
  }, async ({ limit }) => toolResult(dependencies.state.recentTriggers.slice(0, limit)));

  server.registerTool("get_positions", {
    description: "Read paper positions and ledger state. This tool cannot modify positions.",
    inputSchema: {}, annotations: readOnly,
  }, async () => { const snapshot = dependencies.state.snapshot(); return toolResult({ positions: snapshot.positions, balance: snapshot.balance }); });

  server.registerTool("get_strategy_performance", {
    description: "Read journal-derived paper strategy and experiment performance.",
    inputSchema: {}, annotations: readOnly,
  }, async () => toolResult(buildExperimentSummary(await dependencies.journal.readAll(), dependencies.state.experimentId ?? "unknown")));

  server.registerTool("submit_structured_signal", {
    description: "Submit one schema-validated Grok Bot research signal. This never creates an order and exposes no wallet capability.",
    inputSchema: grokSignalSubmissionSchema,
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  }, async (raw) => toolResult(await dependencies.submitSignal(grokSignalSubmissionSchema.parse(raw), clientId)));

  return server;
}

function toolResult(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value) }], structuredContent: { result: value } };
}

function sha256(value: string): Buffer { return createHash("sha256").update(value, "utf8").digest(); }
function extractRpcMethod(body: unknown): string | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const method = (body as { method?: unknown }).method;
  if (method !== "tools/call") return typeof method === "string" ? method : null;
  const name = (body as { params?: { name?: unknown } }).params?.name;
  return typeof name === "string" ? `tools/call:${name}` : method;
}
function safeErrorCode(error: unknown): string { return error instanceof Error ? error.name.slice(0, 80) : "UNKNOWN_ERROR"; }
async function audit(journal: EventJournal, input: { clientId: string; tokenFingerprint: string | null; method: string; status: string; durationMs: number; errorCode?: string }): Promise<void> {
  await journal.append(createJournalEvent({
    traceId: createId("mcp_trace"), type: "MCP_INTERACTION", aggregateType: "mcp_client", aggregateId: input.clientId,
    payload: { ...input, durationMs: Math.max(0, Math.round(input.durationMs * 1000) / 1000) },
  })).catch(() => undefined);
}
