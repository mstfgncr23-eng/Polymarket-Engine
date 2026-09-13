import { resolve } from "node:path";
import pino from "pino";
import { MemoryEventJournal, PostgresEventJournal, type EventJournal } from "@pm/db";
import { EngineService, parseEnv } from "./engine.js";
import { MCP_OAUTH_ROLES } from "./mcp-oauth.js";
import { createApiServer } from "./server.js";
import { OpenAIAstraClient } from "./astra/client.js";

export interface Runtime {
  engine: EngineService;
  server: Awaited<ReturnType<typeof createApiServer>>;
  stop(): Promise<void>;
}

/**
 * OAuth operator approval uses one secret. Role authority is still derived from
 * the exact protected-resource URL and downscoped access token, never from the
 * approval secret itself. The old comma-separated role mapping is deliberately
 * not consumed here; this keeps production approval wiring unambiguous.
 */
export function resolveOAuthApprovalBootstrap(envInput: NodeJS.ProcessEnv, oauthEnabled: boolean): string {
  if (!oauthEnabled) return "";

  const secret = (envInput.GROK_MCP_OAUTH_APPROVAL_SECRET ?? "").trim();
  if (!secret) throw new Error("GROK_MCP_OAUTH_APPROVAL_SECRET_REQUIRED");
  if (secret.length < 32) throw new Error("GROK_MCP_OAUTH_APPROVAL_SECRET_TOO_SHORT");
  if (secret.includes(",")) throw new Error("GROK_MCP_OAUTH_APPROVAL_SECRET_INVALID");

  return MCP_OAUTH_ROLES.map((role) => `${role}:${secret}`).join(",");
}

export async function startRuntime(envInput: NodeJS.ProcessEnv = process.env): Promise<Runtime> {
  const env = parseEnv(envInput);
  const oauthBootstrapTokens = resolveOAuthApprovalBootstrap(envInput, env.GROK_MCP_OAUTH_ENABLED);
  const logger = pino({ level: env.LOG_LEVEL, base: { service: "polymarket-engine", tradingMode: "paper" } });
  const journal = createJournal(env, logger);
  // An active recovered Astra experiment owns its authority even when the
  // deployment flag changes. The client is still opt-in by key presence; the
  // engine will fail closed during recovery if that active authority cannot be
  // satisfied.
  const astraClient = env.ASTRA_API_KEY
    ? new OpenAIAstraClient({
      apiKey: env.ASTRA_API_KEY, model: env.ASTRA_MODEL,
      reasoning: { FAST: env.ASTRA_FAST_REASONING, DEEP: env.ASTRA_DEEP_REASONING, BATCH: env.ASTRA_BATCH_REASONING },
      timeoutMs: { FAST: env.ASTRA_FAST_TIMEOUT_MS, DEEP: env.ASTRA_DEEP_TIMEOUT_MS, BATCH: env.ASTRA_BATCH_TIMEOUT_MS },
      maxOutputTokens: { FAST: env.ASTRA_FAST_MAX_OUTPUT_TOKENS, DEEP: env.ASTRA_DEEP_MAX_OUTPUT_TOKENS, BATCH: env.ASTRA_BATCH_MAX_OUTPUT_TOKENS },
      maxConcurrency: { FAST: env.ASTRA_FAST_MAX_CONCURRENCY, DEEP: env.ASTRA_DEEP_MAX_CONCURRENCY },
      pricing: { inputUsdPerM: env.ASTRA_INPUT_USD_PER_M, cachedInputUsdPerM: env.ASTRA_CACHED_INPUT_USD_PER_M, cacheWriteUsdPerM: env.ASTRA_CACHE_WRITE_USD_PER_M, outputUsdPerM: env.ASTRA_OUTPUT_USD_PER_M },
    })
    : undefined;
  const engine = new EngineService(env, journal, logger, { astraClient });
  const server = await createApiServer({
    state: engine.state, journal, metrics: engine.metrics, sse: engine.sse,
    dashboardOrigin: env.DASHBOARD_ORIGIN, logger: { level: env.LOG_LEVEL },
    mcp: {
      bearerTokens: env.GROK_MCP_BEARER_TOKENS,
      allowedOrigins: env.GROK_MCP_ALLOWED_ORIGINS,
      rateLimitPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE,
      oauth: {
        enabled: env.GROK_MCP_OAUTH_ENABLED,
        issuer: env.GROK_MCP_OAUTH_ISSUER,
        bootstrapTokens: oauthBootstrapTokens,
        accessTokenTtlSeconds: env.GROK_MCP_OAUTH_ACCESS_TOKEN_TTL_SECONDS,
        authorizationCodeTtlSeconds: env.GROK_MCP_OAUTH_AUTH_CODE_TTL_SECONDS,
      },
    },
    submitGrokSignal: (signal, clientId, mode) => engine.submitGrokSignal(signal, clientId, mode),
    persistReportInput: (event) => engine.persistReportInput(event),
    noteMcpRequest: (tool, triggerId) => engine.noteMcpRequest(tool, triggerId),
    isTokenSynchronized: (tokenId) => engine.isTokenSynchronized(tokenId),
    isTriggerActionable: (triggerId) => engine.isTriggerActionable(triggerId),
    getAlphaResearchContext: (triggerId) => engine.getAlphaResearchContextForMcp(triggerId),
    oauthObserve: (event) => logger.info({
      oauthEvent: event.event, role: event.role ?? null, endpoint: event.endpoint ?? null,
      status: event.status ?? null, reason: event.reason ?? null, clientFingerprint: event.clientFingerprint ?? null,
      metadataFields: event.metadataFields ?? null, requestedGrantTypes: event.requestedGrantTypes ?? null,
      tokenEndpointAuthMethod: event.tokenEndpointAuthMethod ?? null, redirectUris: event.redirectUris ?? null,
      requestedScopes: event.requestedScopes ?? null,
      requestedScopeNames: event.requestedScopeNames ?? null, grantedScopeNames: event.grantedScopeNames ?? null,
      resource: event.resource ?? null,
    }, "mcp oauth event"),
    startExperiment: () => engine.beginExperiment(),
    closeExperimentEntries: () => engine.closeExperimentEntries(),
    prepareNextExperiment: () => engine.prepareNextExperiment(),
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
