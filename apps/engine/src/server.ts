import cors from "@fastify/cors";
import sensible from "@fastify/sensible";
import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import { timingSafeEqual } from "node:crypto";
import { DECISION_EVENT_TYPES, HISTORY_DECISIONS_LIMIT, TRACE_PAGE_MAX, type GrokSignalSubmission } from "@pm/core";
import type { EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";
import type { DashboardSnapshot, EngineStateStore } from "./state.js";
import { createMcpHttpHandler, type McpConfig } from "./mcp.js";

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
  submitGrokSignal(signal: GrokSignalSubmission, clientId: string): Promise<Record<string, unknown>>;
  persistReportInput?(event: import("@pm/core").JournalEvent): Promise<void>;
  noteMcpRequest?(tool: string | null, triggerId?: string): void;
  isTokenSynchronized?(tokenId: string): boolean;
  isTriggerActionable?(triggerId: string): boolean;
  startExperiment?: () => Promise<Record<string, unknown>>;
  engineReadToken?: string;
  operatorToken?: string;
  metricsToken?: string;
  nodeEnv?: string;
  logger: boolean | Record<string, unknown>;
}): Promise<FastifyInstance> {
  const app = Fastify({ logger: input.logger });
  await app.register(sensible);
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
    heavyRead: (work) => heavyRead.run(work),
  });
  app.route({ method: ["GET", "POST", "DELETE"], url: "/mcp", handler: mcpHandler });

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
