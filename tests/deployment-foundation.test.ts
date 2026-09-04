import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PaperLedger, engineEnvSchema, parseEngineEnv } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { GET as eventsProxy } from "../apps/dashboard/app/api/engine/events/route.js";
import { GET as snapshotProxy } from "../apps/dashboard/app/api/engine/snapshot/route.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";
import { createGrokActivationBridge, NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function walkFiles(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === ".git" || entry.name === "dist" || entry.name === ".next" || entry.name === "coverage") continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) walkFiles(path, acc);
    else acc.push(path);
  }
  return acc;
}

const productionBase = {
  NODE_ENV: "production",
  TRADING_MODE: "paper",
  JOURNAL_MODE: "postgres",
  DATABASE_URL: "postgres://postgres:REPLACE_ME@db.hkvtmaqvhpbcfbmboibh.supabase.co:5432/postgres",
  ENGINE_READ_TOKEN: "prod-read-token",
  ENGINE_OPERATOR_TOKEN: "prod-operator-token",
} as const;

describe("deployment foundation", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("1 production env schema refuses non-paper trading mode", () => {
    expect(() => engineEnvSchema.parse({ ...productionBase, TRADING_MODE: "live" })).toThrow();
    expect(() => engineEnvSchema.parse({ ...productionBase, TRADING_MODE: "paper-live" })).toThrow();
    expect(() => parseEngineEnv({ ...productionBase, TRADING_MODE: "live" })).toThrow();
    const parsed = parseEngineEnv({ ...productionBase });
    expect(parsed.TRADING_MODE).toBe("paper");
    expect(parsed.NODE_ENV).toBe("production");
    expect(() => parseEngineEnv({ ...productionBase, JOURNAL_MODE: "memory" })).toThrow(/IN_MEMORY_JOURNAL_FORBIDDEN_IN_PRODUCTION/);
    expect(() => parseEngineEnv({ NODE_ENV: "production", TRADING_MODE: "paper", JOURNAL_MODE: "postgres", ENGINE_READ_TOKEN: "prod-read-token" })).toThrow(/DATABASE_URL_REQUIRED_FOR_POSTGRES_JOURNAL/);
  });

  it("2 API read routes require ENGINE_READ_TOKEN in production", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    const missing = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
      nodeEnv: "production",
    });
    const required = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
      engineReadToken: "prod-read-token",
      nodeEnv: "production",
    });
    try {
      expect((await missing.inject({ method: "GET", url: "/health" })).statusCode).toBe(200);
      expect((await missing.inject({ method: "GET", url: "/api/dashboard" })).statusCode).toBe(401);
      expect((await missing.inject({ method: "GET", url: "/events" })).statusCode).toBe(401);
      expect((await required.inject({ method: "GET", url: "/api/dashboard" })).statusCode).toBe(401);
      expect((await required.inject({ method: "GET", url: "/api/dashboard", headers: { authorization: "Bearer prod-read-token" } })).statusCode).toBe(200);
      expect(() => parseEngineEnv({ NODE_ENV: "production", TRADING_MODE: "paper", JOURNAL_MODE: "postgres", DATABASE_URL: productionBase.DATABASE_URL })).toThrow(/ENGINE_READ_TOKEN_REQUIRED_IN_PRODUCTION/);
      expect(() => parseEngineEnv({
        NODE_ENV: "production", TRADING_MODE: "paper", JOURNAL_MODE: "postgres",
        DATABASE_URL: productionBase.DATABASE_URL, ENGINE_READ_TOKEN: "prod-read-token",
      })).toThrow(/ENGINE_OPERATOR_TOKEN_REQUIRED_IN_PRODUCTION/);
    } finally {
      await missing.close();
      await required.close();
    }
  });

  it("3 dashboard code/env pattern does not expose ENGINE_READ_TOKEN via NEXT_PUBLIC_", () => {
    const example = readFileSync(join(repoRoot, ".env.example"), "utf8");
    expect(example).not.toMatch(/^NEXT_PUBLIC_ENGINE_READ_TOKEN=/m);
    expect(example).not.toMatch(/^NEXT_PUBLIC_ENGINE_INTERNAL_URL=/m);
    expect(example).toMatch(/never NEXT_PUBLIC_/);
    const dashboardDir = join(repoRoot, "apps/dashboard");
    for (const file of walkFiles(dashboardDir)) {
      if (!/\.(ts|tsx|js|mjs|cjs|json|env|example)$/.test(file) && !file.endsWith("vercel.json")) continue;
      const source = readFileSync(file, "utf8");
      expect(source, file).not.toMatch(/NEXT_PUBLIC_ENGINE_READ_TOKEN/);
      expect(source, file).not.toMatch(/NEXT_PUBLIC_ENGINE_OPERATOR_TOKEN/);
      expect(source, file).not.toMatch(/process\.env\.NEXT_PUBLIC_/);
    }
  });

  it("4 MONITOR_ONLY / engine start does not auto-begin experiment", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
      DISCOVERY_INTERVAL_MS: "3600000",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: new ScriptedMarketAdapter() as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      expect((await journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).length).toBe(0);
      const monitor = engine.preflight("MONITOR_ONLY");
      expect(monitor.ok).toBe(true);
      expect(monitor.checks.experimentMode).toBe("MONITOR_ONLY");
    } finally {
      await engine.stop();
    }
  });

  it("5 missing Grok config does not crash monitor-only mode", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper",
      GROK_ACTIVATION_PROVIDER: "noop", GROK_MCP_BEARER_TOKENS: "",
      DISCOVERY_INTERVAL_MS: "3600000",
    });
    expect(() => createGrokActivationBridge(env)).not.toThrow();
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: new ScriptedMarketAdapter() as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      const monitor = engine.preflight("MONITOR_ONLY");
      expect(monitor.ok).toBe(true);
      expect(monitor.blockers).not.toEqual(expect.arrayContaining(["GROK_MCP_ROLES_INCOMPLETE", "GROK_ACTIVATION_NOOP"]));
      expect(monitor.warnings).toEqual(expect.arrayContaining(["GROK_WAKE_NOT_VALIDATED"]));
    } finally {
      await engine.stop();
    }
  });

  it("6 database unavailable → unhealthy / fail-closed readiness", async () => {
    const ledger = new PaperLedger(1000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "DOWN";
    expect(state.engineAlive()).toBe(false);
    expect(state.readiness()).toMatchObject({ status: "not_ready", database: "DOWN", engineAlive: false, mode: "paper" });
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    engine.state.database = "DOWN";
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
    });
    try {
      const ready = await server.inject({ method: "GET", url: "/ready" });
      const health = await server.inject({ method: "GET", url: "/health" });
      expect(ready.statusCode).toBe(503);
      expect(ready.json()).toMatchObject({ status: "not_ready", database: "DOWN", engineAlive: false });
      expect(health.statusCode).toBe(200);
    } finally {
      await server.close();
    }
  });

  it("7 dashboard proxy ENGINE_UNREACHABLE (mock fetch failure)", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", "proxy-read-token");
    vi.stubGlobal("fetch", vi.fn(async () => {
      throw new Error("ECONNREFUSED");
    }));
    const snapshot = await snapshotProxy();
    expect(snapshot.status).toBe(503);
    expect(await snapshot.json()).toEqual({ error: "ENGINE_UNREACHABLE" });
    const events = await eventsProxy(new Request("http://dashboard.local/api/engine/events"));
    expect(events.status).toBe(503);
    expect(events.headers.get("content-type")).toMatch(/application\/json/);
    expect(await events.json()).toEqual({ error: "ENGINE_UNREACHABLE" });

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("event: snapshot\ndata: {}\n\n"));
        controller.close();
      },
    });
    vi.stubGlobal("fetch", vi.fn(async () => new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } })));
    const live = await eventsProxy(new Request("http://dashboard.local/api/engine/events"));
    expect(live.status).toBe(200);
    expect(live.headers.get("content-type")).toBe("text/event-stream");
    expect(live.body).toBeTruthy();
    await live.body?.cancel();
  });

  it("8 readiness does not require qualified markets", async () => {
    const ledger = new PaperLedger(1000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "UP";
    state.setDiscoveryStatus("ZERO_QUALIFIED", 0);
    state.setMarketMonitoring(false);
    const ready = state.readiness();
    expect(ready.status).toBe("ready");
    expect(ready.qualifiedMarketCount).toBe(0);
    expect(ready.marketMonitoring).toBe(false);
    expect(ready.discoveryStatus).toBe("ZERO_QUALIFIED");
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    engine.state.database = "UP";
    engine.state.setDiscoveryStatus("ZERO_QUALIFIED", 0);
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
    });
    try {
      const response = await server.inject({ method: "GET", url: "/ready" });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({ status: "ready", qualifiedMarketCount: 0, discoveryStatus: "ZERO_QUALIFIED" });
    } finally {
      await server.close();
    }
  });

  it("9 Docker/context includes migrations path", () => {
    const dockerfile = readFileSync(join(repoRoot, "Dockerfile.engine"), "utf8");
    expect(dockerfile).toMatch(/COPY --from=build[^\n]*\/workspace\/packages\/db\/migrations \.\/packages\/db\/migrations/);
    expect(dockerfile).toMatch(/DB_MIGRATIONS_DIR=\/app\/packages\/db\/migrations/);
    expect(dockerfile).toMatch(/REPORT_DIRECTORY=\/app\/reports/);
    expect(dockerfile).toMatch(/mkdir -p \/app\/reports && chown engine:engine \/app\/reports/);
    expect(existsSync(join(repoRoot, "packages/db/migrations/0000_initial.sql"))).toBe(true);
    expect(existsSync(join(repoRoot, "packages/db/migrations/0001_grok_mcp.sql"))).toBe(true);
    expect(existsSync(join(repoRoot, "packages/db/migrations/0002_journal_query.sql"))).toBe(true);
    expect(existsSync(join(repoRoot, "packages/db/migrations/0003_rls_lockdown.sql"))).toBe(true);
    expect(existsSync(join(repoRoot, "packages/db/migrations/meta/_journal.json"))).toBe(true);
    const rls = readFileSync(join(repoRoot, "packages/db/migrations/0003_rls_lockdown.sql"), "utf8");
    expect(rls).not.toMatch(/^#/m);
    expect(rls).toMatch(/^-- /m);
    expect(rls).toMatch(/ENABLE ROW LEVEL SECURITY/);
    expect(rls).not.toMatch(/ALTER TABLE \S+ FORCE ROW LEVEL SECURITY/);
    expect(rls).not.toMatch(/CREATE POLICY/i);
    expect(rls).toMatch(/pg_roles/);
    expect(rls).toMatch(/REVOKE ALL ON TABLE %I FROM %I/);
  });

  it("Next standalone output is for Fly/Docker, not Vercel", () => {
    const source = readFileSync(join(repoRoot, "apps/dashboard/next.config.ts"), "utf8");
    expect(source).toMatch(/process\.env\.VERCEL/);
    expect(source).toMatch(/standalone/);
    const vercelJson = JSON.parse(readFileSync(join(repoRoot, "apps/dashboard/vercel.json"), "utf8")) as {
      framework?: string; installCommand?: string; buildCommand?: string;
    };
    expect(vercelJson.installCommand).toContain("pnpm install --frozen-lockfile");
    expect(vercelJson.buildCommand).toContain("pnpm --filter @pm/dashboard build");
    expect(vercelJson.framework).toBe("nextjs");
  });

  it("operator token is exclusive; read token cannot start the experiment", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    let started = 0;
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
      startExperiment: async () => {
        started += 1;
        return { ok: true, stubbed: true };
      },
      engineReadToken: "read-token",
      operatorToken: "operator-token",
      nodeEnv: "production",
    });
    try {
      expect((await server.inject({
        method: "GET", url: "/api/dashboard", headers: { authorization: "Bearer read-token" },
      })).statusCode).toBe(200);
      expect((await server.inject({
        method: "POST", url: "/api/operator/experiment/start", headers: { authorization: "Bearer read-token" },
      })).statusCode).toBe(401);
      expect(started).toBe(0);
      const operator = await server.inject({
        method: "POST", url: "/api/operator/experiment/start", headers: { authorization: "Bearer operator-token" },
      });
      expect(operator.statusCode).toBe(200);
      expect(operator.json()).toMatchObject({ ok: true, stubbed: true });
      expect(started).toBe(1);
      const fallback = await createApiServer({
        state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
        dashboardOrigin: "http://localhost:3000", logger: false,
        mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
        submitGrokSignal: async () => ({ status: "ignored" }),
        startExperiment: async () => ({ ok: true }),
        engineReadToken: "read-token",
        nodeEnv: "production",
      });
      try {
        expect((await fallback.inject({
          method: "POST", url: "/api/operator/experiment/start", headers: { authorization: "Bearer read-token" },
        })).statusCode).toBe(401);
      } finally {
        await fallback.close();
      }
    } finally {
      await server.close();
    }
  });

  it("Fly workflow_dispatch requires operator-supplied fly_app", () => {
    const workflow = readFileSync(join(repoRoot, ".github/workflows/fly-deploy.yml"), "utf8");
    expect(workflow).toMatch(/workflow_dispatch:/);
    expect(workflow).toMatch(/fly_app:/);
    expect(workflow).toMatch(/required: true/);
    expect(workflow).toMatch(/flyctl deploy --config fly\.engine\.toml --app "\$\{\{ inputs\.fly_app \}\}" --ha=false/);
    const ci = readFileSync(join(repoRoot, ".github/workflows/ci.yml"), "utf8");
    expect(ci).not.toMatch(/FLY_API_TOKEN/);
    expect(ci).toMatch(/docker build -f Dockerfile\.engine/);
    expect(ci).toMatch(/test -w \/app\/reports/);
  });

  it("Drizzle migrations are additive and safe against a pre-provisioned schema", () => {
    const dir = join(repoRoot, "packages/db/migrations");
    const files = ["0000_initial.sql", "0001_grok_mcp.sql", "0002_journal_query.sql", "0003_rls_lockdown.sql", "0004_critical_replay.sql"];
    const sql = files.map((name) => readFileSync(join(dir, name), "utf8")).join("\n");
    expect(sql).not.toMatch(/\bDROP\s+TABLE\b/i);
    expect(sql).not.toMatch(/\bDROP\s+SCHEMA\b/i);
    expect(sql).not.toMatch(/\bDROP\s+DATABASE\b/i);
    expect(sql).not.toMatch(/\bTRUNCATE\b/i);
    expect(sql).not.toMatch(/\bDELETE\s+FROM\b/i);
    expect(readFileSync(join(dir, "0000_initial.sql"), "utf8")).toMatch(/CREATE TABLE IF NOT EXISTS/);
    expect(readFileSync(join(dir, "0001_grok_mcp.sql"), "utf8")).toMatch(/CREATE TABLE IF NOT EXISTS mcp_idempotency_keys/);
    expect(readFileSync(join(dir, "0002_journal_query.sql"), "utf8")).toMatch(/CREATE INDEX IF NOT EXISTS/);
    const rls = readFileSync(join(dir, "0003_rls_lockdown.sql"), "utf8");
    expect(rls).toMatch(/ENABLE ROW LEVEL SECURITY/);
    expect(rls).toMatch(/IF EXISTS \(SELECT 1 FROM pg_roles/);
    expect(rls).toMatch(/Duplicate supabase_migrations rows named 0003_rls_lockdown are harmless/);
    const journal = JSON.parse(readFileSync(join(dir, "meta/_journal.json"), "utf8")) as { entries: Array<{ tag: string }> };
    expect(readFileSync(join(dir, "0004_critical_replay.sql"), "utf8")).toMatch(/CREATE TABLE IF NOT EXISTS durable_transitions/);
    expect(journal.entries.map((entry) => entry.tag)).toEqual([
      "0000_initial", "0001_grok_mcp", "0002_journal_query", "0003_rls_lockdown", "0004_critical_replay",
    ]);
  });
});
