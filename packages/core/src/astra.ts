import { z } from "zod";
import { confidenceSchema, idSchema, probabilitySchema, timestampSchema, type Direction } from "./domain.js";

export const ASTRA_BRAIN_AUTHORITY = "ASTRA_BRAIN_V1" as const;
export const astraStageSchema = z.enum(["FAST", "DEEP", "BATCH"]);
export type AstraStage = z.infer<typeof astraStageSchema>;
export const astraActionSchema = z.enum(["BUY_UP", "BUY_DOWN", "NO_TRADE"]);
export type AstraAction = z.infer<typeof astraActionSchema>;
export const astraReasonCodeSchema = z.string().regex(/^[A-Z][A-Z0-9_]{0,31}$/);

export const astraModelOutputSchema = z.object({
  action: astraActionSchema,
  fairProbabilityUp: probabilitySchema,
  confidence: confidenceSchema,
  uncertainty: z.number().min(0).max(1),
  reasonCodes: z.array(astraReasonCodeSchema).max(6),
}).strict();
export type AstraModelOutput = z.infer<typeof astraModelOutputSchema>;

export const astraLessonScopeSchema = z.enum(["global", "asset", "regime", "direction"]);
export const astraLessonSchema = z.object({
  scope: astraLessonScopeSchema,
  lesson: z.string().min(1).max(240),
  confidence: confidenceSchema,
}).strict();
export type AstraLesson = z.infer<typeof astraLessonSchema>;

export const astraBatchOutputSchema = z.object({
  lessons: z.array(astraLessonSchema).max(8),
}).strict();
export type AstraBatchOutput = z.infer<typeof astraBatchOutputSchema>;

export const astraUsageSchema = z.object({
  inputTokens: z.number().int().nonnegative().default(0),
  cachedInputTokens: z.number().int().nonnegative().default(0),
  cacheWriteTokens: z.number().int().nonnegative().default(0),
  outputTokens: z.number().int().nonnegative().default(0),
  reasoningTokens: z.number().int().nonnegative().default(0),
  costUsd: z.number().nonnegative().default(0),
}).strict();
export type AstraUsage = z.infer<typeof astraUsageSchema>;

export const astraFeaturePacketSchema = z.object({
  schemaVersion: z.literal(1),
  marketId: idSchema,
  asset: z.string().min(1).nullable(),
  triggerId: idSchema,
  traceId: idSchema,
  phase: z.enum(["RUNNING", "ENTRY_CLOSED", "COMPLETED", "IDLE"]).default("RUNNING"),
  observedAt: timestampSchema,
  expiresAt: timestampSchema,
  upBid: z.number().min(0).max(1).default(0),
  upAsk: z.number().min(0).max(1).default(0),
  downBid: z.number().min(0).max(1).default(0),
  downAsk: z.number().min(0).max(1).default(0),
  upMid: z.number().min(0).max(1),
  downMid: z.number().min(0).max(1),
  upImbalance: z.number().min(-1).max(1),
  downImbalance: z.number().min(-1).max(1),
  bookPressure: z.number().min(-1).max(1).default(0),
  upSpreadBps: z.number().nonnegative(),
  downSpreadBps: z.number().nonnegative(),
  upDepthUsd: z.number().nonnegative(),
  downDepthUsd: z.number().nonnegative(),
  pairAskCost: z.number().min(0).max(2),
  recentMoveBps: z.number().nullable(),
  recentVolatilityBps: z.number().nonnegative().nullable(),
  referenceVenue: z.string().max(64).nullable().default(null),
  referenceFreshness: z.enum(["fresh", "stale", "unknown"]).default("unknown"),
  referenceRequired: z.boolean().default(false),
  referenceFreshnessMs: z.number().nonnegative().nullable(),
  timeToExpiryMs: z.number().int().nonnegative(),
  regime: z.enum(["CALM", "TRENDING", "VOLATILE", "DISLOCATED", "UNKNOWN"]),
  triggerKind: z.string().min(1).max(64),
  triggerSeverity: z.number().int().min(0).max(3),
  triggerScore: z.number().finite().default(0),
  currentMarketExposureUsd: z.number().finite().default(0),
  currentTotalExposureUsd: z.number().finite().default(0),
  currentDirectionalExposureUsd: z.number().finite().default(0),
  currentUnhedgedExposureUsd: z.number().finite().default(0),
  drawdownUtilization: z.number().min(0).max(1).default(0),
  availableCashUsd: z.number().finite().default(0),
  currentPositionSummary: z.string().max(512).nullable().default(null),
}).strict();
export type AstraFeaturePacket = z.infer<typeof astraFeaturePacketSchema>;

export interface AstraDecisionRequest {
  stage: AstraStage;
  traceId: string;
  triggerId: string;
  featurePacket: AstraFeaturePacket;
  memory: readonly Record<string, unknown>[];
  portfolio: {
    drawdownUtilization: number;
    unhedgedUtilization: number;
    openPositionCount: number;
  };
  signal?: AbortSignal;
}

export interface AstraModelResult {
  output: AstraModelOutput;
  usage: AstraUsage;
  providerRequestId: string | null;
  latencyMs?: number;
}

export interface AstraBatchRequest {
  stage: "BATCH";
  traceId: string;
  observedAt: string;
  aggregate: {
    sampleIds: readonly string[];
    sampleCount: number;
    calibration: { samples: number; brierSum: number; reliability: number };
    previousLessons: readonly AstraLesson[];
  };
  signal?: AbortSignal;
}

export interface AstraBatchResult {
  output: AstraBatchOutput;
  usage: AstraUsage;
  providerRequestId: string | null;
  latencyMs?: number;
}

export interface AstraClient {
  decide(request: AstraDecisionRequest): Promise<AstraModelResult>;
  learn?(request: AstraBatchRequest): Promise<AstraBatchResult>;
}

export interface AstraDecision {
  authority: typeof ASTRA_BRAIN_AUTHORITY;
  action: AstraAction;
  direction: Direction;
  fairProbabilityUp: number;
  confidence: number;
  uncertainty: number;
  expiresAt: string;
  reasonCodes: string[];
  rationaleCode: string;
  usage: AstraUsage;
  providerRequestId: string | null;
}
