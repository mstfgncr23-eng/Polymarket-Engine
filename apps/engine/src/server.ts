import cors from "@fastify/cors";
import sensible from "@fastify/sensible";
import Fastify, { type FastifyInstance } from "fastify";
import type { EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";
import type { DashboardSnapshot, EngineStateStore } from "./state.js";
import { createMcpHttpHandler, type McpConfig } from "./mcp.js";
import type { GrokSignalSubmission } from "@pm/core";

type Subscriber = { write(chunk: string): boolean; end(): void };

export class SseHub {
  private readonly subscribers = new Set<Subscriber>();
  private sequence = 0;

  add(subscriber: Subscriber): () => void {
    this.subscribers.add(subscriber);
    return () => this.subscribers.delete(subscriber);
  }

  publish(type: string, data: unknown): void {
    const frame = `id: ${++this.sequence}\nevent: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const subscriber of this.subscribers) subscriber.write(frame);
  }

  close(): void {
    for (const subscriber of this.subscribers) subscriber.end();
    this.subscribers.clear();
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
  logger: boolean | Record<string, unknown>;
}): Promise<FastifyInstance> {
  const app = Fastify({ logger: input.logger });
  await app.register(sensible);
  await app.register(cors, { origin: input.dashboardOrigin, methods: ["GET"] });

  app.get("/health", async () => ({ status: "ok", mode: "paper", draining: input.state.draining, engineAlive: input.state.engineAlive() }));
  app.get("/ready", async (_request, reply) => {
    const body = input.state.readiness();
    if (body.status !== "ready") return reply.code(503).send(body);
    return body;
  });
  app.get("/metrics", async (_request, reply) => reply.type(input.metrics.registry.contentType).send(await input.metrics.registry.metrics()));
  const mcpHandler = createMcpHttpHandler(input.mcp, {
    state: input.state, journal: input.journal, metrics: input.metrics, submitSignal: input.submitGrokSignal,
    noteMcpRequest: input.noteMcpRequest,
  });
  app.route({ method: ["GET", "POST", "DELETE"], url: "/mcp", handler: mcpHandler });
  app.get("/api/dashboard", async (): Promise<DashboardSnapshot> => input.state.snapshot());
  app.get("/api/history/equity", async () => input.state.snapshot().equityHistory);
  app.get<{ Params: { traceId: string } }>("/api/traces/:traceId", async (request) => ({ traceId: request.params.traceId, events: await input.journal.readTrace(request.params.traceId) }));
  app.get("/events", async (request, reply) => {
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
