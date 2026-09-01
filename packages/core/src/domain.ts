import { randomUUID } from "node:crypto";
import { z } from "zod";

export const decimalStringSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/, "decimal string required");
export const probabilitySchema = z.number().min(0).max(1);
export const confidenceSchema = z.number().min(0).max(1);
export const timestampSchema = z.string().datetime({ offset: true });
export const idSchema = z.string().min(1).max(160);

export type DecimalString = z.infer<typeof decimalStringSchema>;
export type IsoTimestamp = z.infer<typeof timestampSchema>;

export function createId(prefix: string): string {
  return `${prefix}_${randomUUID()}`;
}

export const sideSchema = z.enum(["BUY", "SELL"]);
export const outcomeSchema = z.enum(["UP", "DOWN"]);
export const directionSchema = z.enum(["UP", "DOWN", "FLAT"]);
export type Side = z.infer<typeof sideSchema>;
export type Outcome = z.infer<typeof outcomeSchema>;
export type Direction = z.infer<typeof directionSchema>;

export const bookLevelSchema = z.object({
  price: decimalStringSchema,
  quantity: decimalStringSchema.refine((value) => Number(value) >= 0, "non-negative quantity required"),
});

export const feeScheduleSchema = z.object({
  rate: decimalStringSchema,
  exponent: z.number().int().min(0).max(10),
  takerOnly: z.boolean(),
  rebateRate: decimalStringSchema,
  source: z.enum(["market_fee_schedule", "clob_fee_endpoint", "configured_test_fixture"]),
  effectiveAt: timestampSchema,
});

export const marketSnapshotSchema = z.object({
  eventId: idSchema,
  traceId: idSchema,
  marketId: idSchema,
  tokenId: idSchema,
  outcome: outcomeSchema,
  bids: z.array(bookLevelSchema),
  asks: z.array(bookLevelSchema),
  lastTradePrice: decimalStringSchema.nullable(),
  tickSize: decimalStringSchema.nullable(),
  minOrderSize: decimalStringSchema.nullable(),
  feeSchedule: feeScheduleSchema.nullable(),
  sourceTimestamp: timestampSchema,
  receivedAt: timestampSchema,
  sequence: z.number().int().nonnegative(),
  hash: z.string().nullable(),
});
export type MarketSnapshot = z.infer<typeof marketSnapshotSchema>;

export const btcTickSchema = z.object({
  eventId: idSchema,
  traceId: idSchema,
  symbol: z.string().min(1),
  price: decimalStringSchema,
  quantity: decimalStringSchema,
  sourceTimestamp: timestampSchema,
  receivedAt: timestampSchema,
  tradeId: z.string(),
});
export type BtcTick = z.infer<typeof btcTickSchema>;

export const severitySchema = z.union([z.literal(0), z.literal(1), z.literal(2), z.literal(3)]);
export const triggerKindSchema = z.enum([
  "BTC_RAPID_MOVE",
  "BTC_VOLATILITY_SPIKE",
  "BTC_VOLUME_SPIKE",
  "BOOK_IMBALANCE",
  "SPREAD_CHANGE",
  "DEPTH_CHANGE",
  "REPRICING_LAG",
  "PAIR_COST_OPPORTUNITY",
  "EDGE_CHANGE",
  "NEARING_EXPIRY",
  "FAILED_HEDGE",
  "DIRECTIONAL_EXPOSURE",
  "LOSS_CLUSTER",
  "SCHEDULED_REVIEW",
]);

export const triggerEventSchema = z.object({
  id: idSchema,
  traceId: idSchema,
  marketId: idSchema.nullable(),
  kind: triggerKindSchema,
  severity: severitySchema,
  score: z.number().nonnegative(),
  dedupeKey: z.string(),
  evidence: z.record(z.string(), z.unknown()),
  sourceEventIds: z.array(idSchema),
  createdAt: timestampSchema,
  expiresAt: timestampSchema,
});
export type TriggerEvent = z.infer<typeof triggerEventSchema>;

export const externalAgentRoleIdSchema = z.enum([
  "market_microstructure_scanner",
  "crypto_market_scanner",
  "news_x_intelligence_scanner",
  "fair_probability_analyst",
  "risk_hedge_analyst",
  "strategy_supervisor",
]);
export type ExternalAgentRoleId = z.infer<typeof externalAgentRoleIdSchema>;

export const reasonCodeSchema = z.string().regex(/^[A-Z][A-Z0-9_]*$/).max(100);

const signalBaseShape = {
  confidence: confidenceSchema,
  evidenceIds: z.array(idSchema),
  expiresAt: timestampSchema,
  reasonCodes: z.array(reasonCodeSchema),
};

export const marketMicrostructureOutputSchema = z.object({
  ...signalBaseShape,
  direction: directionSchema,
  spread: decimalStringSchema,
  imbalance: z.number().min(-1).max(1),
  depthChange: z.number(),
  pairCost: decimalStringSchema.nullable(),
  unusualConditions: z.array(z.string()),
}).strict();

export const cryptoMarketOutputSchema = z.object({
  ...signalBaseShape,
  direction: directionSchema,
  momentumBps: z.number(),
  volatilityBps: z.number().nonnegative(),
  volumeAnomalyScore: z.number().nonnegative(),
  accelerationBps: z.number(),
  regime: z.enum(["CALM", "TRENDING", "VOLATILE", "DISLOCATED"]),
}).strict();

export const newsXIntelligenceOutputSchema = z.object({
  ...signalBaseShape,
  direction: directionSchema,
  classification: z.enum(["BULLISH", "BEARISH", "NEUTRAL", "IRRELEVANT"]),
  summary: z.string().max(1500),
  sources: z.array(z.object({
    url: z.string().url(),
    title: z.string().max(300),
    publishedAt: timestampSchema.nullable(),
  })).max(12),
}).strict();

export const fairProbabilityOutputSchema = z.object({
  ...signalBaseShape,
  direction: directionSchema,
  fairProbability: probabilitySchema,
  marketProbability: probabilitySchema,
  estimatedEdge: z.number().min(-1).max(1),
}).strict();

export const riskHedgeOutputSchema = z.object({
  ...signalBaseShape,
  recommendVeto: z.boolean(),
  directionalExposureUsd: decimalStringSchema,
  unhedgedDurationMs: z.number().int().nonnegative(),
  pairedCost: decimalStringSchema.nullable(),
  adverseScenarios: z.array(z.string()).max(12),
}).strict();

export const strategySupervisorOutputSchema = z.object({
  ...signalBaseShape,
  diagnosis: z.string().max(4000),
  regime: z.string().max(100),
  proposedChange: z.object({
    status: z.literal("PROPOSED"),
    summary: z.string().max(2000),
    parameterPatch: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])),
  }).nullable(),
}).strict();

export type MarketMicrostructureOutput = z.infer<typeof marketMicrostructureOutputSchema>;
export type CryptoMarketOutput = z.infer<typeof cryptoMarketOutputSchema>;
export type NewsXIntelligenceOutput = z.infer<typeof newsXIntelligenceOutputSchema>;
export type FairProbabilityOutput = z.infer<typeof fairProbabilityOutputSchema>;
export type RiskHedgeOutput = z.infer<typeof riskHedgeOutputSchema>;
export type StrategySupervisorOutput = z.infer<typeof strategySupervisorOutputSchema>;

export const grokSignalSubmissionSchema = z.discriminatedUnion("agentId", [
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("market_microstructure_scanner"), output: marketMicrostructureOutputSchema }).strict(),
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("crypto_market_scanner"), output: cryptoMarketOutputSchema }).strict(),
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("news_x_intelligence_scanner"), output: newsXIntelligenceOutputSchema }).strict(),
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("fair_probability_analyst"), output: fairProbabilityOutputSchema }).strict(),
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("risk_hedge_analyst"), output: riskHedgeOutputSchema }).strict(),
  z.object({ submissionId: idSchema, triggerId: idSchema, traceId: idSchema, agentId: z.literal("strategy_supervisor"), output: strategySupervisorOutputSchema }).strict(),
]);
export type GrokSignalSubmission = z.infer<typeof grokSignalSubmissionSchema>;

export const grokSignalEnvelopeSchema = z.object({
  signalId: idSchema,
  submissionId: idSchema,
  traceId: idSchema,
  triggerId: idSchema,
  agentId: externalAgentRoleIdSchema,
  provider: z.literal("grok_bot_mcp"),
  clientId: idSchema,
  receivedAt: timestampSchema,
  output: z.unknown(),
  resultingDecision: z.string().nullable(),
}).strict();
export type GrokSignalEnvelope = z.infer<typeof grokSignalEnvelopeSchema>;

export const consensusDecisionSchema = z.object({
  id: idSchema,
  traceId: idSchema,
  triggerId: idSchema,
  consensusDirection: directionSchema,
  consensusConfidence: confidenceSchema,
  agentAgreement: confidenceSchema,
  signalFreshness: confidenceSchema,
  conflicts: z.array(z.string()),
  estimatedEdge: z.number().min(-1).max(1),
  evidenceIds: z.array(idSchema),
  calculatedAt: timestampSchema,
});
export type ConsensusDecision = z.infer<typeof consensusDecisionSchema>;

export const riskReasonCodeSchema = z.enum([
  "MARKET_FEED_STALE",
  "BTC_FEED_STALE",
  "DATABASE_UNAVAILABLE",
  "EVENT_TIMESTAMP_DISORDER",
  "RISK_STATE_UNAVAILABLE",
  "DAILY_LOSS_LIMIT",
  "MAX_DRAWDOWN",
  "MAX_ORDER_SIZE",
  "MAX_MARKET_EXPOSURE",
  "MAX_TOTAL_EXPOSURE",
  "MAX_DIRECTIONAL_EXPOSURE",
  "MAX_UNHEDGED_EXPOSURE",
  "MAX_UNHEDGED_DURATION",
  "MINIMUM_LIQUIDITY",
  "MINIMUM_EXPECTED_EDGE",
  "MAXIMUM_SPREAD",
  "MINIMUM_SIGNAL_CONFIDENCE",
  "SIGNAL_EXPIRED",
  "MAXIMUM_EXTERNAL_SIGNAL_RATE",
  "STRATEGY_COOLDOWN",
  "DUPLICATE_TRADE",
  "UNKNOWN_FEE_SCHEDULE",
  "INSUFFICIENT_DEPTH",
  "EXPERIMENT_ENTRY_CLOSED",
  "ABNORMAL_EXECUTION_ERRORS",
  "EXTERNAL_RISK_RECOMMENDED_VETO",
  "SHUTTING_DOWN",
]);
export type RiskReasonCode = z.infer<typeof riskReasonCodeSchema>;

export const riskEvaluationSchema = z.object({
  id: idSchema,
  traceId: idSchema,
  accepted: z.boolean(),
  vetoes: z.array(riskReasonCodeSchema),
  utilization: z.record(z.string(), z.number().nonnegative()),
  evaluatedAt: timestampSchema,
});
export type RiskEvaluation = z.infer<typeof riskEvaluationSchema>;

export const paperOrderSchema = z.object({
  id: idSchema,
  traceId: idSchema,
  triggerId: idSchema,
  strategyId: idSchema,
  marketId: idSchema,
  tokenId: idSchema,
  outcome: outcomeSchema,
  side: sideSchema,
  orderType: z.enum(["MAKER", "TAKER"]),
  requestedPrice: decimalStringSchema,
  quantity: decimalStringSchema,
  status: z.enum(["PENDING", "RESTING", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"]),
  createdAt: timestampSchema,
});
export type PaperOrder = z.infer<typeof paperOrderSchema>;

export const paperFillSchema = z.object({
  id: idSchema,
  orderId: idSchema,
  traceId: idSchema,
  marketId: idSchema,
  tokenId: idSchema,
  outcome: outcomeSchema,
  side: sideSchema,
  requestedPrice: decimalStringSchema,
  fillPrice: decimalStringSchema,
  quantity: decimalStringSchema,
  liquidityRole: z.enum(["MAKER", "TAKER"]),
  latencyMs: z.number().int().nonnegative(),
  slippageUsd: decimalStringSchema,
  feeUsd: decimalStringSchema,
  timestamp: timestampSchema,
  sourceMarketEventId: idSchema,
  fillConfidence: confidenceSchema,
  fillModel: z.string(),
  feeSchedule: feeScheduleSchema,
});
export type PaperFill = z.infer<typeof paperFillSchema>;

export const positionSchema = z.object({
  id: idSchema,
  marketId: idSchema,
  upQuantity: decimalStringSchema,
  downQuantity: decimalStringSchema,
  averageUpCost: decimalStringSchema,
  averageDownCost: decimalStringSchema,
  pairedQuantity: decimalStringSchema,
  unpairedOutcome: outcomeSchema.nullable(),
  unpairedQuantity: decimalStringSchema,
  combinedPairCost: decimalStringSchema,
  totalFees: decimalStringSchema,
  totalSlippage: decimalStringSchema,
  openedAt: timestampSchema,
  updatedAt: timestampSchema,
});
export type Position = z.infer<typeof positionSchema>;

export const ledgerStateSchema = z.object({
  startingBalance: decimalStringSchema,
  cash: decimalStringSchema,
  reservedCash: decimalStringSchema,
  positionValue: decimalStringSchema,
  equity: decimalStringSchema,
  realizedPnl: decimalStringSchema,
  unrealizedPnl: decimalStringSchema,
  grossPnl: decimalStringSchema,
  fees: decimalStringSchema,
  slippage: decimalStringSchema,
  aiCost: decimalStringSchema,
  infrastructureCost: decimalStringSchema,
  netTradingPnl: decimalStringSchema,
  netEconomicPnl: decimalStringSchema,
  highWaterMark: decimalStringSchema,
  drawdown: decimalStringSchema,
  updatedAt: timestampSchema,
});
export type LedgerState = z.infer<typeof ledgerStateSchema>;

export const experimentManifestSchema = z.object({
  id: idSchema,
  status: z.enum(["CREATED", "RUNNING", "ENTRY_CLOSED", "COMPLETED", "FAILED"]),
  durationHours: z.number().positive(),
  strategyVersion: z.string(),
  riskConfiguration: z.record(z.string(), z.unknown()),
  modelConfiguration: z.record(z.string(), z.unknown()),
  startingBalance: decimalStringSchema,
  systemVersion: z.string(),
  gitSha: z.string(),
  startedAt: timestampSchema,
  entryClosesAt: timestampSchema,
  completedAt: timestampSchema.nullable(),
});
export type ExperimentManifest = z.infer<typeof experimentManifestSchema>;

export const journalEventSchema = z.object({
  id: idSchema,
  traceId: idSchema,
  correlationId: idSchema,
  type: z.string().min(1),
  aggregateType: z.string().min(1),
  aggregateId: idSchema,
  payload: z.record(z.string(), z.unknown()),
  occurredAt: timestampSchema,
  recordedAt: timestampSchema,
  sourceEventId: idSchema.nullable(),
});
export type JournalEvent = z.infer<typeof journalEventSchema>;

export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };
