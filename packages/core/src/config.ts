import { z } from "zod";

export const riskLimitsSchema = z.object({
  maxOrderSizeUsd: z.number().positive().default(25),
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
  /** Dedicated gross locked pair edge floor for NORMAL HEDGE_SECOND_LEG only. */
  normalHedgeMinLockedPairEdge: z.number().min(0).max(1).default(0.015),
  /**
   * Minimum fee/slippage-adjusted expected return on executable first-leg
   * notional. Unlike the absolute edge floor, this stays economically
   * meaningful near $1 where a few cents of edge can still be tiny ROI.
   */
  minExpectedROI: z.number().nonnegative().default(0.02),
  /**
   * Fresh OPEN_FIRST_LEG price-regime boundary (exclusive). Post-latency
   * executable first-leg price strictly BELOW this value selects the NORMAL
   * first-leg economic floors (normalFirstLegMinExpectedEdge / ROI); at/above
   * it the STRICT floors above remain unchanged. 0.90 exactly is STRICT.
   */
  normalFirstLegMaxPriceExclusive: z.number().min(0).max(1).default(0.9),
  /** Relaxed net edge floor (USD/share) for fresh first-legs below the price boundary. */
  normalFirstLegMinExpectedEdge: z.number().min(0).max(1).default(0.01),
  /** Relaxed expected ROI floor for fresh first-legs below the price boundary. */
  normalFirstLegMinExpectedROI: z.number().nonnegative().default(0.01),
  maxSpread: z.number().min(0).max(1).default(0.08),
  minSignalConfidence: z.number().min(0).max(1).default(0.6),
  staleFeedTimeoutMs: z.number().int().positive().default(15_000),
  maxExternalSignalsPerMinute: z.number().int().positive().default(30),
  strategyCooldownMs: z.number().int().nonnegative().default(60_000),
  maxExecutionErrorsPerHour: z.number().int().positive().default(5),
  /**
   * PR3 early catastrophic pair-lock protection. Applied to an unpaired
   * position BEFORE the old protective deadline: after the minimum unhedged
   * age, when the realizable mark loss on the held leg reaches the trigger
   * loss, a FULL fee-inclusive opposite pair-lock may buy the entire residual
   * BUY the opposite outcome as long as the guaranteed terminal PnL is no worse
   * than the configured max locked loss. Normal profitable hedges always take
   * priority; the old deadline A/B rescue remains the final fallback.
   */
  catastrophicPairLockMinUnhedgedMs: z.number().int().nonnegative().default(10_000),
  catastrophicPairLockTriggerLossUsd: z.number().nonnegative().default(0.35),
  catastrophicPairLockMaxLockedLossUsd: z.number().nonnegative().default(0.60),
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
  /** Explicit PAPER diagnostic authority: actual first legs held to resolution. */
  NAKED_DIRECTIONAL_SOAK_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** PAPER experiment: .12 target with profit high-water trailing protection. */
  PROFIT_TRAILING_PAIR_SOAK_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  PROFIT_TRAILING_PAIR_ARM_EDGE: z.coerce.number().min(0).max(1).default(0.05),
  PROFIT_TRAILING_PAIR_RETRACE_EDGE: z.coerce.number().positive().max(1).default(0.02),
  /** Locked Guardian V2 PAPER profit-trailing authority for new manifests, default OFF. */
  REVERSAL_PROTECTION_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** Run the same policy in an independent Guardian through a local socket. */
  GUARDIAN_SEPARATE_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** Legacy manifest compatibility fields. V2 thresholds are sealed constants in guardian-v2.ts. */
  REVERSAL_PROTECTION_WINDOW_MS: z.coerce.number().int().positive().default(180000),
  REVERSAL_PROTECTION_OPPOSITE_COUNT: z.coerce.number().int().positive().default(3),
  PORT: z.coerce.number().int().positive().default(8080),
  LOG_LEVEL: z.string().default("info"),
  DATABASE_URL: z.string().url().optional(),
  JOURNAL_MODE: z.enum(["postgres", "memory"]).default("postgres"),
  PAPER_START_BALANCE: z.coerce.number().positive().default(1000),
  PAPER_EXECUTION_LATENCY_MS: z.coerce.number().int().nonnegative().default(150),
  PAPER_EXECUTION_STYLE: z.enum(["TAKER", "MAKER"]).default("TAKER"),
  /** Runtime override for NORMAL HEDGE_SECOND_LEG gross locked pair edge floor. */
  NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: z.coerce.number().min(0).max(1).default(0.015),
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
  /**
   * Human/forensic journal archive on durable local storage. Journal rows that
   * are not runtime/recovery authority must never be purged before an archive
   * segment is atomically written and checksum-verified.
   */
  LOCAL_ARCHIVE_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  LOCAL_ARCHIVE_DIR: z.string().min(1).default("/data/archive"),
  ARCHIVE_REMOTE_HOST: z.string().default(""),
  LOCAL_ARCHIVE_BUCKET_MS: z.coerce.number().int().min(60_000).default(60 * 60_000),
  /**
   * Durable Native V2 learning memory. Default false so production OVH/Fly is
   * unchanged until an operator opts in. Fail-soft when enabled.
   */
  LEARNING_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** Host-mounted durable directory inside the engine container. */
  LEARNING_DATA_DIR: z.string().min(1).default("/data/learning"),
  /**
   * Operational DEFAULT max durable learning bytes (not a measured capacity
   * requirement). Prefer append segments; writer stops accepting new samples
   * once exceeded and marks learning degraded.
   */
  LEARNING_MAX_BYTES: z.coerce.number().int().positive().default(256_000_000),
  /** Comma-separated checkpoint seconds before expiry. Default: 240,180,120,60,30,15,5 */
  LEARNING_CHECKPOINT_SECONDS: z.string().default("240,180,120,60,30,15,5"),
  /** Optional dedicated poll interval for non-economic learning settlement labels. */
  LEARNING_SETTLEMENT_POLL_MS: z.coerce.number().int().positive().optional(),
  /**
   * Learned V2 shadow runtime (PR5). Default false. When enabled, loads a
   * verified candidate for advisory prediction only — never V1 authority.
   */
  LEARNING_SHADOW_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** Absolute path to candidate.json for shadow load (alternative to model id). */
  LEARNING_SHADOW_MODEL_PATH: z.string().min(1).optional(),
  /** Registry model id under LEARNING_DATA_DIR/models (alternative to path). */
  LEARNING_SHADOW_MODEL_ID: z.string().min(1).optional(),
  /**
   * Future controlled PAPER authority for PAPER_APPROVED models.
   * Default false. PR5 has NO engine execution hook — enabling this alone
   * cannot submit orders.
   */
  LEARNING_PAPER_AUTHORITY_ENABLED: z.preprocess(
    (value) => value === undefined ? undefined : value === true || value === "true" || value === "1",
    z.boolean().optional(),
  ).default(false),
  /** Explicit operator-approved model id for a future controlled PAPER path. */
  LEARNING_PAPER_AUTHORITY_MODEL_ID: z.string().min(1).optional(),
});
export type EngineEnv = z.infer<typeof engineEnvSchema>;

/** Production boot contract. Paper-only; durable Postgres journal; read token required. */
export function assertProductionEngineEnv(env: EngineEnv): void {
  if (env.TRADING_MODE !== "paper") throw new Error("LIVE_TRADING_DISABLED");
  if (env.JOURNAL_MODE !== "postgres") throw new Error("IN_MEMORY_JOURNAL_FORBIDDEN_IN_PRODUCTION");
  if (!env.DATABASE_URL) throw new Error("DATABASE_URL_REQUIRED_FOR_POSTGRES_JOURNAL");
  if (!env.ENGINE_READ_TOKEN) throw new Error("ENGINE_READ_TOKEN_REQUIRED_IN_PRODUCTION");
  if (!env.ENGINE_OPERATOR_TOKEN) throw new Error("ENGINE_OPERATOR_TOKEN_REQUIRED_IN_PRODUCTION");
}

export function parseEngineEnv(env: NodeJS.ProcessEnv): EngineEnv {
  const parsed = engineEnvSchema.parse(env);
  if (parsed.NODE_ENV === "production") assertProductionEngineEnv(parsed);
  return parsed;
}
