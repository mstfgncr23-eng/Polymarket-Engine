import cors from "@fastify/cors";
import sensible from "@fastify/sensible";
import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import { timingSafeEqual } from "node:crypto";
import { DECISION_EVENT_TYPES, type GrokSignalSubmission } from "@pm/core";
import type { EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";
import type { DashboardSnapshot, EngineStateStore } from "./state.js";
import { createMcpHttpHandler, type McpConfig } from "./mcp.js";

type Subscriber = { write(chunk: string): boolean; end(): void };

export class SseHub {
  private readonly subscribers = new Set<Subscriber>();
  private sequence = 0;
  private snapshotTimer: NodeJS.Timeout | null = null;
  private pendingSnapshot: unknown = null;
  private snapshotBroadcasts = 0;
  constructor(private readonly snapshotHz = 4) {}

  add(subscriber: Subscriber): () => void {
    this.subscribers.add(subscriber);
    return () => this.subscribers.delete(subscriber);
  }

  publish(type: string, data: unknown): void {
    if (type === "snapshot") {
      this.pendingSnapshot = data;
      if (this.snapshotTimer) return;
      const delay = Math.max(50, Math.floor(1000 / Math.max(0.5, this.snapshotHz)));
      this.snapshotTimer = setTimeout(() => {
        this.snapshotTimer = null;
        const payload = this.pendingSnapshot;
        this.pendingSnapshot = null;
        if (payload !== null && payload !== undefined) {
          const resolved = typeof payload === "function" ? (payload as () => unknown)() : payload;
          this.frame("snapshot", resolved);
          this.snapshotBroadcasts += 1;
        }
      }, delay);
      this.snapshotTimer.unref?.();
      return;
    }
    this.frame(type, data);
  }

  snapshotBroadcastCount(): number { return this.snapshotBroadcasts; }

  close(): void {
    if (this.snapshotTimer) clearTimeout(this.snapshotTimer);
    for (const subscriber of this.subscribers) subscriber.end();
    this.subscribers.clear();
  }

  private frame(type: string, data: unknown): void {
    const encoded = `id: ${++this.sequence}\nevent: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const subscriber of this.subscribers) subscriber.write(encoded);
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
  noteMcpRequest?(tool: string | null, triggerId?: string): void;
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
  const mcpHandler = createMcpHttpHandler(input.mcp, {
    state: input.state, journal: input.journal, metrics: input.metrics, submitSignal: input.submitGrokSignal,
    noteMcpRequest: input.noteMcpRequest,
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
  app.get<{ Params: { traceId: string } }>("/api/traces/:traceId", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    return { traceId: request.params.traceId, events: await input.journal.readTrace(request.params.traceId) };
  });
  app.get("/api/history/decisions", async (request, reply) => {
    const denied = await requireRead(request, reply);
    if (denied) return denied;
    return { events: await input.journal.queryEvents({ types: DECISION_EVENT_TYPES, limit: 500 }) };
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
    reply.raw.write(`event: snapshot\ndata: ${JSON.stringify(input.state.snapshot())}\n\n`);
    const remove = input.sse.add(reply.raw);
    const keepalive = setInterval(() => reply.raw.write(": keepalive\n\n"), 15_000);
    request.raw.on("close", () => { clearInterval(keepalive); remove(); });
  });
  return app;
}
