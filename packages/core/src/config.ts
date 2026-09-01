import { z } from "zod";

export const riskLimitsSchema = z.object({
  maxOrderSizeUsd: z.number().positive().default(25),
  maxMarketExposureUsd: z.number().positive().default(100),
  maxTotalExposureUsd: z.number().positive().default(300),
  maxDirectionalExposureUsd: z.number().positive().default(75),
  maxUnhedgedExposureUsd: z.number().positive().default(50),
  maxUnhedgedDurationMs: z.number().int().positive().default(300_000),
  maxDailyPaperLossUsd: z.number().positive().default(50),
  maxDrawdownUsd: z.number().positive().default(100),
  minLiquidityUsd: z.number().nonnegative().default(100),
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
  volatilitySpikeBps: z.number().positive().default(25),
  volumeSpikeMultiplier: z.number().positive().default(3),
  imbalanceThreshold: z.number().min(0).max(1).default(0.65),
  spreadChangeThreshold: z.number().min(0).max(1).default(0.02),
  depthChangeThreshold: z.number().min(0).default(0.4),
  pairCostThreshold: z.number().min(0).max(2).default(0.985),
  expiryWarningMs: z.number().int().positive().default(3_600_000),
});
export type TriggerConfig = z.infer<typeof triggerConfigSchema>;

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
  BTC_WS_URL: z.string().url().default("wss://stream.binance.com:9443/ws/btcusdt@aggTrade"),
  BTC_SYMBOL: z.string().default("BTCUSDT"),
  GROK_MCP_BEARER_TOKENS: z.string().default(""),
  GROK_MCP_ALLOWED_ORIGINS: z.string().default("https://grok.com,https://x.ai"),
  GROK_MCP_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().positive().default(60),
  ESTIMATED_INFRA_COST_PER_HOUR_USD: z.coerce.number().nonnegative().default(0.08),
  DASHBOARD_ORIGIN: z.string().url().default("http://localhost:3000"),
});
export type EngineEnv = z.infer<typeof engineEnvSchema>;

export function parseEngineEnv(env: NodeJS.ProcessEnv): EngineEnv {
  return engineEnvSchema.parse(env);
}
