import { resolve } from "node:path";
import pino from "pino";
import { MemoryEventJournal, PostgresEventJournal, type EventJournal } from "@pm/db";
import { EngineService, parseEnv } from "./engine.js";
import { createApiServer } from "./server.js";

export interface Runtime {
  engine: EngineService;
  server: Awaited<ReturnType<typeof createApiServer>>;
  stop(): Promise<void>;
}

export async function startRuntime(envInput: NodeJS.ProcessEnv = process.env): Promise<Runtime> {
  const env = parseEnv(envInput);
  const logger = pino({ level: env.LOG_LEVEL, base: { service: "polymarket-engine", tradingMode: "paper" } });
  const journal = createJournal(env, logger);
  const engine = new EngineService(env, journal, logger);
  const server = await createApiServer({
    state: engine.state, journal, metrics: engine.metrics, sse: engine.sse,
    dashboardOrigin: env.DASHBOARD_ORIGIN, logger: { level: env.LOG_LEVEL },
    mcp: { bearerTokens: env.GROK_MCP_BEARER_TOKENS, allowedOrigins: env.GROK_MCP_ALLOWED_ORIGINS, rateLimitPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE },
    submitGrokSignal: (signal, clientId) => engine.submitGrokSignal(signal, clientId),
    persistReportInput: (event) => engine.persistReportInput(event),
    noteMcpRequest: (tool, triggerId) => engine.noteMcpRequest(tool, triggerId),
    isTokenSynchronized: (tokenId) => engine.isTokenSynchronized(tokenId),
    isTriggerActionable: (triggerId) => engine.isTriggerActionable(triggerId),
    startExperiment: () => engine.beginExperiment(),
    engineReadToken: env.ENGINE_READ_TOKEN,
    operatorToken: env.ENGINE_OPERATOR_TOKEN,
    metricsToken: env.METRICS_TOKEN,
    nodeEnv: env.NODE_ENV,
  });
  await server.listen({ host: "0.0.0.0", port: env.PORT });
  try { await engine.start(); }
  catch (error) { logger.error({ err: error }, "engine bootstrap failed; liveness remains available and readiness stays false"); }
  let stopped = false;
  return {
    engine, server,
    async stop() {
      if (stopped) return;
      stopped = true;
      await engine.stop().catch((error) => logger.error({ err: error }, "engine shutdown failed"));
      await server.close().catch((error) => logger.error({ err: error }, "http shutdown failed"));
    },
  };
}

function createJournal(env: ReturnType<typeof parseEnv>, logger: { warn(message: string): void }): EventJournal {
  if (env.JOURNAL_MODE === "memory") {
    if (env.NODE_ENV === "production") throw new Error("IN_MEMORY_JOURNAL_FORBIDDEN_IN_PRODUCTION");
    logger.warn("using explicit in-memory journal; restart recovery and durability are disabled");
    return new MemoryEventJournal();
  }
  if (!env.DATABASE_URL) throw new Error("DATABASE_URL_REQUIRED_FOR_POSTGRES_JOURNAL");
  return new PostgresEventJournal(env.DATABASE_URL, process.env.DB_MIGRATIONS_DIR ?? resolve(process.cwd(), "packages/db/migrations"));
}
