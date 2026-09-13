import cors from "@fastify/cors";
import sensible from "@fastify/sensible";
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { timingSafeEqual } from "node:crypto";
import { DECISION_EVENT_TYPES, HISTORY_DECISIONS_LIMIT, TRACE_PAGE_MAX, type GrokSignalSubmission } from "@pm/core";
import type { EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";
import type { DashboardSnapshot, EngineStateStore } from "./state.js";
import type { GrokSubmissionMode } from "./activation/role-runner.js";
import { createMcpHttpHandler, type McpConfig } from "./mcp.js";
import { MCP_OAUTH_ROLES, McpOAuthService, type OAuthRequestContext } from "./mcp-oauth.js";

import { FiniteSemaphore, RUNTIME_BOUNDS } from "./runtime-supervisor.js";

type Subscriber = { write(chunk: string): boolean; end(): void; writableLength?: number };
type SlowClientReason = "byte_threshold" | "persistent_backpressure";

interface TrackedClient {
  subscriber: Subscriber;
  bytes: number;
  consecutiveFalse: number;
}

export class SseHub {
  private readonly subscribers = new Set<Subscriber>();
  private readonly tracked = new Map<Subscriber, TrackedClient>();
  private sequence = 0;
  private snapshotTimer: NodeJS.Timeout | null = null;
  private pendingSnapshot: unknown = null;
  private snapshotBroadcasts = 0;
  constructor(private readonly snapshotHz = 4, private readonly metrics?: EngineMetrics) {}

  add(subscriber: Subscriber): () => void {
    this.subscribers.add(subscriber);
    this.tracked.set(subscriber, { subscriber, bytes: 0, consecutiveFalse: 0 });
    this.metrics?.sseSubscribers.set(this.subscribers.size);
    return () => this.remove(subscriber);
  }

  /** Read-only runtime counters used by the capacity harness and diagnostics. */
  statsForTest(): { subscribers: number; tracked: number; bufferedBytes: number; pendingSnapshot: boolean; snapshotTimerActive: boolean } {
    let bufferedBytes = 0;
    for (const state of this.tracked.values()) {
      const reported = typeof state.subscriber.writableLength === "number" ? state.subscriber.writableLength : state.bytes;
      bufferedBytes += Math.max(0, reported);
    }
    return {
      subscribers: this.subscribers.size,
      tracked: this.tracked.size,
      bufferedBytes,
      pendingSnapshot: this.pendingSnapshot !== null,
      snapshotTimerActive: this.snapshotTimer !== null,
    };
  }

  private remove(subscriber: Subscriber, reason?: SlowClientReason): void {
    if (!this.subscribers.delete(subscriber)) return;
    this.tracked.delete(subscriber);
    this.metrics?.sseSubscribers.set(this.subscribers.size);
    if (reason) this.metrics?.sseSlowClientDisconnects.inc({ reason });
    if (this.subscribers.size === 0) this.dropPendingSnapshot();
  }

  publish(type: string, data: unknown): void {
    if (type === "snapshot") {
      if (this.subscribers.size === 0) {
        this.metrics?.sseSnapshotsSkippedZeroSubscribers.inc();
        this.dropPendingSnapshot();
        return;
      }
      this.pendingSnapshot = data;
      if (this.snapshotTimer) return;
      const delay = Math.max(50, Math.floor(1000 / Math.max(0.5, this.snapshotHz)));
      this.snapshotTimer = setTimeout(() => {
        this.snapshotTimer = null;
        const payload = this.pendingSnapshot;
        this.pendingSnapshot = null;
        if (this.subscribers.size === 0 || payload === null || payload === undefined) return;
        const resolved = this.resolveSnapshot(payload);
        if (this.subscribers.size === 0) return;
        this.frame("snapshot", resolved);
        this.snapshotBroadcasts += 1;
      }, delay);
      this.snapshotTimer.unref?.();
      return;
    }
    this.frame(type, data);
  }

  snapshotBroadcastCount(): number { return this.snapshotBroadcasts; }

  close(): void {
    this.dropPendingSnapshot();
    for (const subscriber of this.subscribers) subscriber.end();
    this.subscribers.clear();
    this.tracked.clear();
    this.metrics?.sseSubscribers.set(0);
  }

  private dropPendingSnapshot(): void {
    if (this.snapshotTimer) {
      clearTimeout(this.snapshotTimer);
      this.snapshotTimer = null;
    }
    this.pendingSnapshot = null;
  }

  private resolveSnapshot(payload: unknown): unknown {
    if (typeof payload !== "function") return payload;
    const started = performance.now();
    const resolved = (payload as () => unknown)();
    this.metrics?.sseSnapshotBuildDuration.observe((performance.now() - started) / 1000);
    return resolved;
  }

  private frame(type: string, data: unknown): void {
    const serializeStarted = performance.now();
    const json = JSON.stringify(data);
    if (type === "snapshot") {
      this.metrics?.sseSnapshotSerializeDuration.observe((performance.now() - serializeStarted) / 1000);
      this.metrics?.sseSnapshotPayloadBytes.observe(Buffer.byteLength(json));
      this.metrics?.sseSnapshotBroadcasts.inc();
    }
    const encoded = `id: ${++this.sequence}\nevent: ${type}\ndata: ${json}\n\n`;
    for (const subscriber of [...this.subscribers]) this.writeToSubscriber(subscriber, encoded);
  }

  private writeToSubscriber(subscriber: Subscriber, encoded: string): void {
    const state = this.tracked.get(subscriber) ?? { subscriber, bytes: 0, consecutiveFalse: 0 };
    this.tracked.set(subscriber, state);
    let ok = true;
    try {
      ok = subscriber.write(encoded);
    } catch {
      try { subscriber.end(); } catch { /* already closed */ }
      this.remove(subscriber, "persistent_backpressure");
      return;
    }
    const reported = typeof subscriber.writableLength === "number" ? subscriber.writableLength : null;
    if (ok) {
      state.consecutiveFalse = 0;
      state.bytes = reported ?? 0;
    } else {
      state.consecutiveFalse += 1;
      this.metrics?.sseSubscriberBackpressure.inc();
      state.bytes = reported ?? state.bytes + Buffer.byteLength(encoded);
    }
    if (state.bytes >= RUNTIME_BOUNDS.sse.byteThreshold) {
      try { subscriber.end(); } catch { /* already closed */ }
      this.remove(subscriber, "byte_threshold");
      return;
    }
    if (state.consecutiveFalse >= RUNTIME_BOUNDS.sse.persistentBackpressure) {
      try { subscriber.end(); } catch { /* already closed */ }
      this.remove(subscriber, "persistent_backpressure");
    }
  }
}

export async function createApiServer(input: {
  state: EngineStateStore;
  journal: EventJournal;
  metrics: EngineMetrics;
  sse: SseHub;
  dashboardOrigin: string;
  mcp: McpConfig;
  submitGrokSignal(signal: GrokSignalSubmission, clientId: string, mode?: GrokSubmissionMode): Promise<Record<string, unknown>>;
  persistReportInput?(event: import("@pm/core").JournalEvent): Promise<void>;
  noteMcpRequest?(tool: string | null, triggerId?: string): void;
  isTokenSynchronized?(tokenId: string): boolean;
  isTriggerActionable?(triggerId: string): boolean;
  getAlphaResearchContext?(triggerId: string): Record<string, unknown> | null;
  startExperiment?: () => Promise<Record<string, unknown>>;
  closeExperimentEntries?: () => Promise<Record<string, unknown>>;
  prepareNextExperiment?: () => Promise<Record<string, unknown>>;
  engineReadToken?: string;
  operatorToken?: string;
  metricsToken?: string;
  nodeEnv?: string;
  oauthObserve?(event: import("./mcp-oauth.js").OAuthSafeEvent): void;
  logger: boolean | Record<string, unknown>;
}): Promise<FastifyInstance> {
  const app = Fastify({ logger: input.logger });
  await app.register(sensible);
  app.addContentTypeParser("application/x-www-form-urlencoded", { parseAs: "string" }, (_request, body, done) => done(null, body));
  await app.register(cors, { origin: input.dashboardOrigin, methods: ["GET", "POST"] });
  const heavyRead = new FiniteSemaphore(RUNTIME_BOUNDS.heavyRead.concurrency, RUNTIME_BOUNDS.heavyRead.waiting, "HEAVY_READ_BUSY");

  const protect = (request: FastifyRequest, token: string | undefined, productionRequired: boolean) => {
    if (!token) {
      if (productionRequired && input.nodeEnv === "production") return { ok: false as const, status: 401 as const };
      return { ok: true as const };
    }
    const header = request.headers.authorization ?? "";
    if (!header.startsWith("Bearer ")) return { ok: false as const, status: 401 as const };
    const provided = Buffer.from(header.slice(7));
    const expected = Buffer.from(token);
    if (provided.length !== expected.length || !timingSafeEqual(provided, expected)) return { ok: false as const, status: 401 as const };
    return { ok: true as const };
  };

  // In-memory only. Do not add I/O, discovery, journal, or feed work here — Fly health checks are 3s.
  app.get("/health", async () => ({ status: "ok", mode: "paper", draining: input.state.draining, engineAlive: input.state.engineAlive() }));
  app.get("/ready", async (_request, reply) => {
    const body = input.state.readiness();
    if (body.status !== "ready") return reply.code(503).send(body);
    return body;
  });
  app.get("/metrics", async (request, reply) => {
    const auth = protect(request, input.metricsToken, false);
    if (!auth.ok && input.metricsToken) return reply.code(401).send({ error: "METRICS_UNAUTHORIZED" });
    return reply.type(input.metrics.registry.contentType).send(await input.metrics.registry.metrics());
  });
  const persistReportInput = input.persistReportInput ?? (async (event) => {
    throw new Error(`PERSIST_REPORT_INPUT_REQUIRED:${event.type}`);
  });
  const mcpHandler = createMcpHttpHandler(input.mcp, {
    state: input.state, journal: input.journal, metrics: input.metrics, submitSignal: input.submitGrokSignal,
    persistReportInput,
    noteMcpRequest: input.noteMcpRequest,
    isTokenSynchronized: input.isTokenSynchronized,
    isTriggerActionable: input.isTriggerActionable,
    getAlphaResearchContext: input.getAlphaResearchContext,
    heavyRead: (work) => heavyRead.run(work),
  });
  app.route({ method: ["GET", "POST", "DELETE"], url: "/mcp", handler: mcpHandler });

  const oauth = input.mcp.oauth?.enabled
    ? new McpOAuthService(input.mcp.oauth, () => Date.now(), input.oauthObserve)
    : null;
  const requestOrigin = (request: FastifyRequest): string => {
    const forwarded = request.headers["x-forwarded-proto"];
    const protocol = (typeof forwarded === "string" ? forwarded.split(",")[0] : undefined) ?? "http";
    const host = request.headers.host ?? "localhost";
    return `${protocol}://${host}`;
  };
  const oauthContext = (request: FastifyRequest): OAuthRequestContext => ({ origin: requestOrigin(request), endpoint: request.url.split("?", 1)[0], remoteAddress: request.ip });
  const jsonBody = (request: FastifyRequest): Record<string, string | undefined> => {
    const body = request.body;
    if (!body || typeof body !== "object" || Array.isArray(body)) return {};
    return Object.fromEntries(Object.entries(body as Record<string, unknown>).map(([key, value]) => [key, typeof value === "string" ? value : undefined]));
  };
  const formBody = (request: FastifyRequest): Record<string, string | undefined> => {
    const body = request.body;
    if (typeof body === "string") return Object.fromEntries(new URLSearchParams(body).entries());
    return jsonBody(request);
  };
  const oauthJson = (reply: { code: (status: number) => { header: (name: string, value: string) => { send: (body: unknown) => unknown }; send: (body: unknown) => unknown } }, result: { status: number; body: unknown }) => {
    const target = reply.code(result.status);
    target.header("Cache-Control", "no-store");
    return target.send(result.body);
  };

  const protectedMetadata = async (request: FastifyRequest, reply: FastifyReply) => {
    const role = oauth?.roleFromPath((request.params as { role?: string }).role ?? "");
    if (!oauth || !role) return reply.code(404).send({ error: "NOT_FOUND" });
    input.oauthObserve?.({ event: "resource_metadata_requested", role, endpoint: request.url.split("?", 1)[0], status: "ok", timestamp: new Date().toISOString() });
    return reply.header("Cache-Control", "no-store").send(oauth.protectedResourceMetadata(role, requestOrigin(request)));
  };
  app.get<{ Params: { role: string } }>("/.well-known/oauth-protected-resource/mcp/:role", protectedMetadata);
  app.get<{ Params: { role: string } }>("/mcp/:role/.well-known/oauth-protected-resource", protectedMetadata);
  app.get("/.well-known/oauth-authorization-server", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "NOT_FOUND" });
    input.oauthObserve?.({ event: "authorization_metadata_requested", endpoint: request.url.split("?", 1)[0], status: "ok", timestamp: new Date().toISOString() });
    return reply.header("Cache-Control", "no-store").send(oauth.authorizationServerMetadata(requestOrigin(request)));
  });
  app.get("/oauth/.well-known/oauth-authorization-server", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "NOT_FOUND" });
    input.oauthObserve?.({ event: "authorization_metadata_requested", endpoint: request.url.split("?", 1)[0], status: "ok", timestamp: new Date().toISOString() });
    return reply.header("Cache-Control", "no-store").send(oauth.authorizationServerMetadata(requestOrigin(request)));
  });
  app.post("/oauth/register", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "NOT_FOUND" });
    return oauthJson(reply, oauth.register(request.body, oauthContext(request)));
  });
  app.get<{ Querystring: Record<string, string | undefined> }>("/oauth/authorize", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "NOT_FOUND" });
    const query = request.query as Record<string, string | undefined>;
    const role = typeof query.resource === "string" ? oauth.roleFromPath(query.resource.split("/mcp/")[1] ?? "") : null;
    if (!role || query.resource !== oauth.resourceForRole(role, requestOrigin(request))) return reply.code(400).send({ error: "invalid_target" });
    const result = oauth.authorizationPage(query, oauthContext(request), role);
    return reply.code(result.status).headers(result.headers ?? {}).send(result.body);
  });
  app.post<{ Body: Record<string, string> }>("/oauth/authorize", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "NOT_FOUND" });
    const body = formBody(request);
    const role = typeof body.resource === "string" ? oauth.roleFromPath(body.resource.split("/mcp/")[1] ?? "") : null;
    if (!role || body.resource !== oauth.resourceForRole(role, requestOrigin(request))) return reply.code(400).send({ error: "invalid_target" });
    const result = oauth.approve(body, oauthContext(request), role);
    const target = reply.code(result.status).headers(result.headers ?? {});
    return result.headers?.location ? target.send() : target.send(result.body);
  });
  app.post("/oauth/token", async (request, reply) => {
    if (!oauth) return reply.code(404).send({ error: "not_found" });
    return oauthJson(reply, oauth.token(formBody(request), oauthContext(request)));
  });
  if (oauth) {
    for (const role of MCP_OAUTH_ROLES) {
      const roleHandler = createMcpHttpHandler(input.mcp, {
        state: input.state, journal: input.journal, metrics: input.metrics, submitSignal: input.submitGrokSignal,
        persistReportInput, noteMcpRequest: input.noteMcpRequest, isTokenSynchronized: input.isTokenSynchronized,
        isTriggerActionable: input.isTriggerActionable, getAlphaResearchContext: input.getAlphaResearchContext,
        heavyRead: (work) => heavyRead.run(work), oauthObserve: input.oauthObserve,
      }, { oauth, requiredRole: role, resourceOrigin: requestOrigin });
      app.route({ method: ["GET", "POST", "DELETE"], url: `/mcp/${role}`, handler: roleHandler });
    }
  }

  const requireRead = async (request: FastifyRequest, reply: { code: (n: number) => { send: (body: unknown) => unknown } }) => {
    const auth = protect(request, input.engineReadToken, true);
    if (!auth.ok) return reply.code(401).send({ error: "ENGINE_READ_UNAUTHORIZED" });
    return null;
  };

  app.get("/api/dashboard", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    return input.state.snapshot();
  });
  app.get("/api/history/equity", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    return input.state.snapshot().equityHistory;
  });
  app.get<{ Params: { traceId: string }; Querystring: { cursor?: string; limit?: string } }>("/api/traces/:traceId", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    try {
      const requested = Number(request.query.limit ?? TRACE_PAGE_MAX);
      const limit = Math.max(1, Math.min(Number.isFinite(requested) ? requested : TRACE_PAGE_MAX, TRACE_PAGE_MAX));
      const after = parseTraceCursor(request.query.cursor);
      const page = await heavyRead.run(() => input.journal.readTracePage(request.params.traceId, { after, limit }));
      return {
        traceId: request.params.traceId,
        events: page.events,
        nextCursor: page.nextCursor ? `${page.nextCursor.occurredAt}|${page.nextCursor.id}` : null,
        limit,
        complete: page.nextCursor == null,
      };
    } catch (error) {
      if (error instanceof Error && error.message === "HEAVY_READ_BUSY") return reply.code(503).send({ error: "HEAVY_READ_BUSY" });
      throw error;
    }
  });
  app.get<{ Querystring: { limit?: string } }>("/api/history/decisions", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    try {
      const requested = Number(request.query.limit ?? HISTORY_DECISIONS_LIMIT);
      const limit = Math.max(1, Math.min(Number.isFinite(requested) ? requested : HISTORY_DECISIONS_LIMIT, HISTORY_DECISIONS_LIMIT));
      return { events: await heavyRead.run(() => input.journal.queryEvents({ types: DECISION_EVENT_TYPES, limit })) };
    } catch (error) {
      if (error instanceof Error && error.message === "HEAVY_READ_BUSY") return reply.code(503).send({ error: "HEAVY_READ_BUSY" });
      throw error;
    }
  });
  app.post("/api/operator/experiment/start", async (request, reply) => {
    if (!input.operatorToken) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    const auth = protect(request, input.operatorToken, true);
    if (!auth.ok) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    if (!input.startExperiment) return reply.code(503).send({ error: "EXPERIMENT_START_UNAVAILABLE" });
    return input.startExperiment();
  });
  app.post("/api/operator/experiment/close-entries", async (request, reply) => {
    if (!input.operatorToken) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    const auth = protect(request, input.operatorToken, true);
    if (!auth.ok) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    if (!input.closeExperimentEntries) return reply.code(503).send({ error: "EXPERIMENT_CLOSE_ENTRIES_UNAVAILABLE" });
    const result = await input.closeExperimentEntries();
    if (result.ok === false) return reply.code(409).send(result);
    return result;
  });
  app.post("/api/operator/experiment/prepare-next", async (request, reply) => {
    if (!input.operatorToken) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    const auth = protect(request, input.operatorToken, true);
    if (!auth.ok) return reply.code(401).send({ error: "ENGINE_OPERATOR_UNAUTHORIZED" });
    if (!input.prepareNextExperiment) return reply.code(503).send({ error: "EXPERIMENT_PREPARE_NEXT_UNAVAILABLE" });
    const result = await input.prepareNextExperiment();
    if (result.ok === false) return reply.code(409).send(result);
    return result;
  });
  app.get("/events", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) {
      reply.code(401);
      return denied;
    }
    reply.hijack();
    reply.raw.writeHead(200, {
      "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive", "X-Accel-Buffering": "no", "Access-Control-Allow-Origin": input.dashboardOrigin,
    });
    const buildStarted = performance.now();
    const snapshot = input.state.snapshot();
    input.metrics.sseSnapshotBuildDuration.observe((performance.now() - buildStarted) / 1000);
    const serializeStarted = performance.now();
    const json = JSON.stringify(snapshot);
    input.metrics.sseSnapshotSerializeDuration.observe((performance.now() - serializeStarted) / 1000);
    input.metrics.sseSnapshotPayloadBytes.observe(Buffer.byteLength(json));
    input.metrics.sseSnapshotBroadcasts.inc();
    if (reply.raw.write(`event: snapshot\ndata: ${json}\n\n`) === false) input.metrics.sseSubscriberBackpressure.inc();
    const remove = input.sse.add(reply.raw);
    const keepalive = setInterval(() => reply.raw.write(": keepalive\n\n"), 15_000);
    request.raw.on("close", () => { clearInterval(keepalive); remove(); });
  });
  return app;
}

function parseTraceCursor(raw: string | undefined): { occurredAt: string; id: string } | undefined {
  if (!raw) return undefined;
  const sep = raw.indexOf("|");
  if (sep <= 0 || sep === raw.length - 1) return undefined;
  return { occurredAt: raw.slice(0, sep), id: raw.slice(sep + 1) };
}
