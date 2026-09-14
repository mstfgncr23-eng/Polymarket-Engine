import { z } from "zod";

export const riskLimitsSchema = z.object({
  maxOrderSizeUsd: z.number().positive().default(25),
  /** Separate cap for new first legs; hedge legs retain maxOrderSizeUsd. */
  maxFirstLegOrderSizeUsd: z.number().positive().max(25).default(12.5),
  maxMarketExposureUsd: z.number().positive().default(100),
  maxAssetExposureUsd: z.number().positive().default(150),
  maxTotalExposureUsd: z.number().positive().default(300),
  maxDirectionalExposureUsd: z.number().positive().default(75),
  maxUnhedgedExposureUsd: z.number().positive().default(50),
  maxUnhedgedDurationMs: z.number().int().positive().default(300_000),
  maxDailyPaperLossUsd: z.number().positive().default(50),
  maxDrawdownUsd: z.number().positive().default(100),
  minLiquidityUsd: z.number().nonnegative().default(100),
  /** Minimum net expected value in USD/share for a $1 binary payout. */
  minExpectedEdge: z.number().min(0).max(1).default(0.015),
  maxSpread: z.number().min(0).max(1).default(0.08),
  minSignalConfidence: z.number().min(0).max(1).default(0.6),
  staleFeedTimeoutMs: z.number().int().positive().default(15_000),
  maxExternalSignalsPerMinute: z.number().int().positive().default(30),
  strategyCooldownMs: z.number().int().nonnegative().default(60_000),
  maxExecutionErrorsPerHour: z.number().int().positive().default(5),
});
export type RiskLimits = z.infer<typeof riskLimitsSchema>;

export const triggerConfigSchema = z.object({
  debounceMs: z.number().int().nonnegative().default(750),
  cooldownMs: z.number().int().nonnegative().default(10_000),
  triggerTtlMs: z.number().int().positive().default(30_000),
  btcRapidMoveBps: z.number().positive().default(15),
  referenceRapidMoveBps: z.number().positive().default(15),
  volatilitySpikeBps: z.number().positive().default(25),
  volumeSpikeMultiplier: z.number().positive().default(3),
  imbalanceThreshold: z.number().min(0).max(1).default(0.65),
  spreadChangeThreshold: z.number().min(0).max(1).default(0.02),
  depthChangeThreshold: z.number().min(0).default(0.4),
  pairCostThreshold: z.number().min(0).max(2).default(0.985),
  expiryWarningMs: z.number().int().positive().default(3_600_000),
  maxActivationsPerMinute: z.number().int().positive().default(12),
  activationSeveritySuppression: z.number().int().min(0).max(3).default(1),
  scheduledReviewIntervalMs: z.number().int().positive().default(900_000),
  newsSeverityThreshold: z.number().int().min(1).max(3).default(2),
  failedHedgeDurationMs: z.number().int().positive().default(300_000),
  disagreementAgreementMax: z.number().min(0).max(1).default(0.45),
});
export type TriggerConfig = z.infer<typeof triggerConfigSchema>;

export const qualificationConfigSchema = z.object({
  minRemainingDurationMs: z.number().int().nonnegative().default(120_000),
  maxRemainingDurationMs: z.number().int().positive().default(48 * 3_600_000),
  minLiquidityUsd: z.number().nonnegative().default(50),
  maxSpread: z.number().min(0).max(1).default(0.12),
  maxQualifiedMarkets: z.number().int().positive().default(40),
  allowPolymarketOnlyIfReferenceUnsupported: z.boolean().default(true),
  requireUnderlyingAsset: z.boolean().default(true),
});
export type QualificationConfig = z.infer<typeof qualificationConfigSchema>;

export const engineEnvSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  TRADING_MODE: z.literal("paper").default("paper"),
  PORT: z.coerce.number().int().positive().default(8080),
  LOG_LEVEL: z.string().default("info"),
  DATABASE_URL: z.string().url().optional(),
  JOURNAL_MODE: z.enum(["postgres", "memory"]).default("postgres"),
  PAPER_START_BALANCE: z.coerce.number().positive().default(1000),
  PAPER_EXECUTION_LATENCY_MS: z.coerce.number().int().nonnegative().default(150),
  PAPER_EXECUTION_STYLE: z.enum(["TAKER", "MAKER"]).default("TAKER"),
  EXPERIMENT_DURATION_HOURS: z.coerce.number().positive().default(48),
  EXPERIMENT_CLOSE_POLICY: z.enum(["observe_until_settlement", "mark_to_market"]).default("observe_until_settlement"),
  ENGINE_REGION: z.string().default("fra"),
  POLYMARKET_MARKET_SLUGS: z.string().default(""),
  POLYMARKET_TOKEN_IDS: z.string().default(""),
  REFERENCE_WS_BASE_URL: z.string().url().default("wss://data-stream.binance.vision/ws"),
  REFERENCE_REST_BASE_URL: z.string().url().default("https://api.binance.com"),
  /** @deprecated BTC-only leftover; ignored by multi-asset registry. Accepted so old env files still parse. */
  BTC_WS_URL: z.string().url().optional(),
  /** @deprecated BTC-only leftover; ignored by multi-asset registry. */
  BTC_SYMBOL: z.string().optional(),
  GROK_MCP_BEARER_TOKENS: z.string().default(""),
  GROK_MCP_ALLOWED_ORIGINS: z.string().default("https://grok.com,https://x.ai"),
  GROK_MCP_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().positive().default(60),
  /** OAuth 2.1 protected-resource compatibility is explicit opt-in and disabled by default. */
  GROK_MCP_OAUTH_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  GROK_MCP_OAUTH_ISSUER: z.string().refine((value) => {
    if (value === "") return true;
    try { const url = new URL(value); return (url.protocol === "https:" || url.protocol === "http:") && !url.hash; } catch { return false; }
  }, "OAuth issuer must be an absolute HTTP(S) URL or empty").default(""),
  GROK_MCP_OAUTH_BOOTSTRAP_TOKENS: z.string().default(""),
  GROK_MCP_OAUTH_ACCESS_TOKEN_TTL_SECONDS: z.coerce.number().int().positive().max(86_400).default(3_600),
  GROK_MCP_OAUTH_AUTH_CODE_TTL_SECONDS: z.coerce.number().int().positive().max(600).default(300),
  /** Safe by default: xAI inference is explicit opt-in and never enabled by a key alone. */
  GROK_ACTIVATION_PROVIDER: z.enum(["noop", "slack", "xai"]).default("noop"),
  SLACK_BOT_TOKEN: z.string().optional(),
  SLACK_CHANNEL_ID: z.string().optional(),
  SLACK_API_URL: z.string().url().default("https://slack.com/api/chat.postMessage"),
  XAI_API_KEY: z.string().optional(),
  XAI_API_BASE_URL: z.string().url().default("https://api.x.ai/v1"),
  /** Required explicitly when xAI is selected; safe noop mode needs no model. */
  XAI_MODEL: z.string().min(1).optional(),
  XAI_REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().max(120_000).default(20_000),
  XAI_MAX_OUTPUT_TOKENS: z.coerce.number().int().positive().max(16_384).default(1_024),
  /** Separate explicit gate for sending paper portfolio context to xAI. */
  XAI_ALLOW_PORTFOLIO_CONTEXT: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  XAI_SCOUT_CACHE_MAX_ENTRIES: z.coerce.number().int().positive().max(2_048).default(128),
  /** Shared actual xAI role-inference attempts per rolling minute, retries included. */
  XAI_MAX_ROLE_CALLS_PER_MINUTE: z.coerce.number().int().positive().max(240).default(24),
  XAI_SCOUT_MICRO_TTL_MS: z.coerce.number().int().positive().max(120_000).default(5_000),
  XAI_SCOUT_CRYPTO_TTL_MS: z.coerce.number().int().positive().max(300_000).default(15_000),
  XAI_SCOUT_NEWS_TTL_MS: z.coerce.number().int().positive().max(3_600_000).default(60_000),
  /** Astra is fully opt-in and never enabled by the presence of a key alone. */
  ASTRA_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  ASTRA_API_KEY: z.string().min(1).optional(),
  ASTRA_MODEL: z.string().min(1).default("gpt-6-astra"),
  ASTRA_FAST_REASONING: z.enum(["low", "medium", "high", "xhigh", "max"]).default("low"),
  ASTRA_DEEP_REASONING: z.enum(["low", "medium", "high", "xhigh", "max"]).default("medium"),
  ASTRA_BATCH_REASONING: z.enum(["low", "medium", "high", "xhigh", "max"]).default("high"),
  ASTRA_FAST_TIMEOUT_MS: z.coerce.number().int().positive().max(30_000).default(1_500),
  ASTRA_DEEP_TIMEOUT_MS: z.coerce.number().int().positive().max(60_000).default(3_000),
  ASTRA_BATCH_TIMEOUT_MS: z.coerce.number().int().positive().max(120_000).default(20_000),
  ASTRA_FAST_MAX_OUTPUT_TOKENS: z.coerce.number().int().positive().max(4_096).default(256),
  ASTRA_DEEP_MAX_OUTPUT_TOKENS: z.coerce.number().int().positive().max(8_192).default(768),
  ASTRA_BATCH_MAX_OUTPUT_TOKENS: z.coerce.number().int().positive().max(16_384).default(2_048),
  ASTRA_FAST_MAX_CONCURRENCY: z.coerce.number().int().positive().max(16).default(4),
  ASTRA_DEEP_MAX_CONCURRENCY: z.coerce.number().int().positive().max(8).default(2),
  ASTRA_INPUT_USD_PER_M: z.coerce.number().nonnegative().default(10),
  ASTRA_CACHED_INPUT_USD_PER_M: z.coerce.number().nonnegative().default(1),
  ASTRA_CACHE_WRITE_USD_PER_M: z.coerce.number().nonnegative().default(12.5),
  ASTRA_OUTPUT_USD_PER_M: z.coerce.number().nonnegative().default(50),
  ASTRA_GATE_COOLDOWN_MS: z.coerce.number().int().nonnegative().max(3_600_000).default(2_000),
  ASTRA_GATE_MAX_FINGERPRINTS: z.coerce.number().int().positive().max(4_096).default(256),
  ASTRA_DEEP_UNCERTAINTY_THRESHOLD: z.coerce.number().min(0).max(1).default(0.35),
  ASTRA_DEEP_DRAWDOWN_UTILIZATION_THRESHOLD: z.coerce.number().min(0).max(1).default(0.7),
  ASTRA_DEEP_UNHEDGED_UTILIZATION_THRESHOLD: z.coerce.number().min(0).max(1).default(0.7),
  ASTRA_BATCH_SETTLEMENT_THRESHOLD: z.coerce.number().int().positive().max(500).default(25),
  ASTRA_MEMORY_MAX_ENTRIES: z.coerce.number().int().positive().max(512).default(64),
  ASTRA_MEMORY_MAX_LESSONS: z.coerce.number().int().positive().max(32).default(8),
  ASTRA_MEMORY_MAX_BYTES: z.coerce.number().int().positive().max(1_000_000).default(16_384),
  DISCOVERY_INTERVAL_MS: z.coerce.number().int().positive().default(300_000),
  MAX_QUALIFIED_MARKETS: z.coerce.number().int().positive().default(40),
  ESTIMATED_INFRA_COST_PER_HOUR_USD: z.coerce.number().nonnegative().default(0.08),
  DASHBOARD_ORIGIN: z.string().url().default("http://localhost:3000"),
  ENGINE_READ_TOKEN: z.string().min(8).optional(),
  ENGINE_OPERATOR_TOKEN: z.string().min(8).optional(),
  METRICS_TOKEN: z.string().min(8).optional(),
  EXPERIMENT_REQUIRES_GROK: z.preprocess(
    (value) => value === true || value === "true" || value === "1",
    z.boolean(),
  ).default(false),
  SETTLEMENT_POLL_MS: z.coerce.number().int().positive().default(15_000),
  SCHEDULED_REVIEW_INTERVAL_MS: z.coerce.number().int().positive().default(900_000),
  TELEMETRY_QUEUE_LIMIT: z.coerce.number().int().positive().default(2_000),
  TELEMETRY_BATCH_SIZE: z.coerce.number().int().positive().default(50),
  DASHBOARD_SNAPSHOT_HZ: z.coerce.number().positive().default(4),
  ACTIVATION_FANOUT: z.coerce.number().int().positive().default(3),
  ACTIVATION_MEMORY_LIMIT: z.coerce.number().int().positive().default(500),
  RETENTION_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  RETENTION_TELEMETRY_TTL_MS: z.coerce.number().int().positive().default(6 * 60 * 60_000),
  RETENTION_MONITOR_TTL_MS: z.coerce.number().int().positive().default(24 * 60 * 60_000),
  RETENTION_COMPLETED_GRACE_MS: z.coerce.number().int().positive().default(7 * 24 * 60 * 60_000),
  RETENTION_MCP_IDEMPOTENCY_TTL_MS: z.coerce.number().int().positive().default(24 * 60 * 60_000),
  RETENTION_CHECKPOINT_KEEP: z.coerce.number().int().min(1).max(64).default(8),
  RETENTION_BATCH_SIZE: z.coerce.number().int().positive().default(200),
  RETENTION_CYCLE_BUDGET_MS: z.coerce.number().int().positive().default(250),
  RETENTION_MAX_BATCHES_PER_CYCLE: z.coerce.number().int().positive().default(20),
  RETENTION_MAX_ROWS_PER_CYCLE: z.coerce.number().int().positive().default(4_000),
  RETENTION_COMPACTION_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  RETENTION_ANALYTICS_ROLLUP_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
});
export type EngineEnv = z.infer<typeof engineEnvSchema>;

/** Production boot contract. Paper-only; durable Postgres journal; read token required. */
export function assertProductionEngineEnv(env: EngineEnv): void {
  if (env.TRADING_MODE !== "paper") throw new Error("LIVE_TRADING_DISABLED");
  if (env.JOURNAL_MODE !== "postgres") throw new Error("IN_MEMORY_JOURNAL_FORBIDDEN_IN_PRODUCTION");
  if (!env.DATABASE_URL) throw new Error("DATABASE_URL_REQUIRED_FOR_POSTGRES_JOURNAL");
  if (!env.ENGINE_READ_TOKEN) throw new Error("ENGINE_READ_TOKEN_REQUIRED_IN_PRODUCTION");
  if (!env.ENGINE_OPERATOR_TOKEN) throw new Error("ENGINE_OPERATOR_TOKEN_REQUIRED_IN_PRODUCTION");
  if (env.ASTRA_ENABLED && !env.ASTRA_API_KEY) throw new Error("ASTRA_API_KEY_REQUIRED_WHEN_ENABLED");
}

export function parseEngineEnv(env: NodeJS.ProcessEnv): EngineEnv {
  const parsed = engineEnvSchema.parse(env);
  if (parsed.NODE_ENV === "production") assertProductionEngineEnv(parsed);
  return parsed;
}
