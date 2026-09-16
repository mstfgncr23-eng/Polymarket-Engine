import { afterEach, describe, expect, it } from "vitest";
import { PaperLedger } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { createApiServer, SseHub } from "../apps/engine/src/server.js";
import { EngineStateStore } from "../apps/engine/src/state.js";

const TOKEN = "native-v2-read-token";
const servers: Awaited<ReturnType<typeof createApiServer>>[] = [];

afterEach(async () => {
  while (servers.length > 0) await servers.pop()!.close();
});

async function server() {
  const ledger = new PaperLedger(1000).snapshot();
  const state = new EngineStateStore(ledger, new Map());
  state.experimentId = "experiment_abc";
  state.experimentStatus = "RUNNING";
  const app = await createApiServer({
    state,
    journal: new MemoryEventJournal(),
    metrics: new EngineMetrics(),
    sse: new SseHub(),
    dashboardOrigin: "http://localhost:3000",
    mcp: { bearerTokens: "", allowedOrigins: "http://localhost:3000", rateLimitPerMinute: 60 },
    submitGrokSignal: async () => ({ ok: true }),
    getNativeV2ShadowSnapshot: () => ({ schemaVersion: 1, summary: { retained: 1 }, latest: { marketId: "m1" } }),
    exportNativeV2Shadow: () => ({
      schemaVersion: 1,
      generatedAt: "2026-09-16T18:30:00.000Z",
      experimentId: "experiment_abc",
      summary: { retained: 1 },
      evaluations: [{ marketId: "m1", outcome: "UP" }],
    }),
    engineReadToken: TOKEN,
    nodeEnv: "production",
    logger: false,
  });
  servers.push(app);
  return app;
}

describe("native v2 shadow RAM export API", () => {
  it("protects both RAM endpoints with the existing engine read token", async () => {
    const app = await server();
    const snapshot = await app.inject({ method: "GET", url: "/api/native-v2/shadow" });
    const exported = await app.inject({ method: "GET", url: "/api/native-v2/shadow/export" });
    expect(snapshot.statusCode).toBe(401);
    expect(exported.statusCode).toBe(401);
  });

  it("returns an authenticated bounded snapshot without journal access", async () => {
    const app = await server();
    const response = await app.inject({
      method: "GET",
      url: "/api/native-v2/shadow",
      headers: { authorization: `Bearer ${TOKEN}` },
    });
    expect(response.statusCode).toBe(200);
    expect(response.headers["cache-control"]).toBe("no-store");
    expect(response.json()).toEqual({ schemaVersion: 1, summary: { retained: 1 }, latest: { marketId: "m1" } });
  });

  it("downloads the in-memory observations as a JSON attachment", async () => {
    const app = await server();
    const response = await app.inject({
      method: "GET",
      url: "/api/native-v2/shadow/export",
      headers: { authorization: `Bearer ${TOKEN}` },
    });
    expect(response.statusCode).toBe(200);
    expect(response.headers["cache-control"]).toBe("no-store");
    expect(response.headers["content-type"]).toMatch(/^application\/json/);
    expect(response.headers["content-disposition"]).toBe('attachment; filename="native-v2-shadow-experiment_abc.json"');
    expect(response.json()).toMatchObject({
      schemaVersion: 1,
      experimentId: "experiment_abc",
      summary: { retained: 1 },
      evaluations: [{ marketId: "m1", outcome: "UP" }],
    });
  });
});
