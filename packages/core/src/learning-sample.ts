import type { NativeDecision } from "./native-decision.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import {
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  type NativeV2ShadowFeatureVector,
} from "./native-v2-shadow-features.js";
import type { Outcome } from "./domain.js";

/** Dataset schema version for durable Native V2 learning samples. */
export const LEARNING_SAMPLE_SCHEMA_VERSION = "learning-sample-v1" as const;

/** Documented default short-horizon product checkpoints (seconds before expiry). */
export const DEFAULT_LEARNING_CHECKPOINT_SECONDS = [240, 180, 120, 60, 30, 15, 5] as const;

export type LearningSettlementStatus = "unresolved" | "resolved";

export interface LearningSettlementLabel {
  status: LearningSettlementStatus;
  /** Official binary winner when resolved; never inferred. */
  outcome: Outcome | null;
  labeledAt: string | null;
  source: string | null;
  reason: string | null;
}

export interface LearningSampleV1 {
  schemaVersion: typeof LEARNING_SAMPLE_SCHEMA_VERSION;
  /** Deterministic identity: `${marketId}::${checkpointId}`. */
  sampleId: string;
  marketId: string;
  checkpointId: string;
  checkpointSeconds: number;
  asset: string | null;
  observedAt: string;
  /** From feature.timing.timeToExpiryMs — not recomputed. */
  timeToExpiryMs: number;
  featureSchemaVersion: typeof NATIVE_V2_SHADOW_FEATURE_VERSION;
  featureVector: NativeV2ShadowFeatureVector;
  v2FairProbability: NativeV2ShadowFairProbability;
  /** V1 market/fair probabilities from NativeDecision when available. */
  v1: {
    fairProbability: number | null;
    marketProbability: number | null;
    direction: NativeDecision["direction"] | null;
    confidence: number | null;
  };
  experimentId: string | null;
  gitSha: string | null;
  settlement: LearningSettlementLabel;
}

export function learningSampleId(marketId: string, checkpointId: string): string {
  return `${marketId}::${checkpointId}`;
}

export function checkpointIdFromSeconds(seconds: number): string {
  return `T-${seconds}`;
}

/**
 * Parse LEARNING_CHECKPOINT_SECONDS (comma-separated seconds).
 * Invalid entries are skipped; duplicates collapsed; order preserved (desc preferred).
 */
export function parseLearningCheckpointSeconds(raw: string | undefined | null): number[] {
  const source = raw == null || raw.trim() === ""
    ? DEFAULT_LEARNING_CHECKPOINT_SECONDS.join(",")
    : raw;
  const seen = new Set<number>();
  const out: number[] = [];
  for (const part of source.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const value = Number(trimmed);
    if (!Number.isFinite(value) || !Number.isInteger(value) || value <= 0) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

/**
 * Crossing semantics: emit a checkpoint only when it is truly crossed between
 * consecutive observations:
 *   previousTimeToExpiryMs > checkpointMs && timeToExpiryMs <= checkpointMs
 *
 * A late first observation (no previous TTE) must NOT backfill earlier
 * checkpoints — e.g. first obs at T-50s emits nothing for T-240/T-180/T-120.
 * Already-recorded checkpoints are skipped. Null timeToExpiryMs yields none.
 *
 * Invalid schedule entries relative to a known positive horizonMs (optional)
 * are skipped rather than invented — checkpoints whose ms exceed horizonMs
 * can never be crossed before the market ends from t=0, so they are omitted
 * when horizonMs is supplied.
 */
export function selectCrossedCheckpoints(input: {
  timeToExpiryMs: number | null;
  checkpointSeconds: readonly number[];
  alreadySampledCheckpointIds: ReadonlySet<string>;
  /** Optional market horizon from birth→end; invalid checkpoints above it are skipped. */
  horizonMs?: number | null;
  /**
   * Previous observation time-to-expiry for this market.
   * null/undefined = first observation → no historical backfill.
   */
  previousTimeToExpiryMs?: number | null;
}): Array<{ checkpointId: string; checkpointSeconds: number }> {
  const tte = input.timeToExpiryMs;
  if (tte === null || !Number.isFinite(tte)) return [];

  const previous = input.previousTimeToExpiryMs;
  // First observation: emit nothing; caller should seed previous TTE.
  if (previous === null || previous === undefined || !Number.isFinite(previous)) {
    return [];
  }

  const selected: Array<{ checkpointId: string; checkpointSeconds: number }> = [];
  for (const seconds of input.checkpointSeconds) {
    if (!Number.isFinite(seconds) || !Number.isInteger(seconds) || seconds <= 0) continue;
    const checkpointMs = seconds * 1_000;
    if (
      input.horizonMs !== null
      && input.horizonMs !== undefined
      && Number.isFinite(input.horizonMs)
      && input.horizonMs > 0
      && checkpointMs > input.horizonMs
    ) {
      continue;
    }
    // True first crossing only: was strictly above, now at/below.
    if (!(previous > checkpointMs && tte <= checkpointMs)) continue;
    const checkpointId = checkpointIdFromSeconds(seconds);
    if (input.alreadySampledCheckpointIds.has(checkpointId)) continue;
    selected.push({ checkpointId, checkpointSeconds: seconds });
  }
  return selected;
}

export function buildLearningSampleV1(input: {
  marketId: string;
  checkpointId: string;
  checkpointSeconds: number;
  asset: string | null;
  observedAt: Date | string;
  timeToExpiryMs: number;
  featureVector: NativeV2ShadowFeatureVector;
  v2FairProbability: NativeV2ShadowFairProbability;
  native?: Pick<NativeDecision, "fairProbability" | "marketProbability" | "direction" | "confidence"> | null;
  experimentId?: string | null;
  gitSha?: string | null;
}): LearningSampleV1 {
  const observedAt = typeof input.observedAt === "string"
    ? input.observedAt
    : input.observedAt.toISOString();
  return {
    schemaVersion: LEARNING_SAMPLE_SCHEMA_VERSION,
    sampleId: learningSampleId(input.marketId, input.checkpointId),
    marketId: input.marketId,
    checkpointId: input.checkpointId,
    checkpointSeconds: input.checkpointSeconds,
    asset: input.asset,
    observedAt,
    timeToExpiryMs: input.timeToExpiryMs,
    featureSchemaVersion: NATIVE_V2_SHADOW_FEATURE_VERSION,
    featureVector: structuredClone(input.featureVector),
    v2FairProbability: structuredClone(input.v2FairProbability),
    v1: {
      fairProbability: input.native?.fairProbability ?? null,
      marketProbability: input.native?.marketProbability ?? null,
      direction: input.native?.direction ?? null,
      confidence: input.native?.confidence ?? null,
    },
    experimentId: input.experimentId ?? null,
    gitSha: input.gitSha ?? null,
    settlement: {
      status: "unresolved",
      outcome: null,
      labeledAt: null,
      source: null,
      reason: "UNRESOLVED",
    },
  };
}
