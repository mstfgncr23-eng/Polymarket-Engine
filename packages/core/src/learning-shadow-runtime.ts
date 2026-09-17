import type { LoadedLearningModelV1 } from "./learning-model-loader.js";
import type { LearningModelLoadFailureReason } from "./learning-model-loader.js";
import type { LearningModelStatus } from "./learning-model-registry.js";
import {
  predictResidualProbabilityFromFeatures,
} from "./learning-residual-model.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import {
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  type NativeV2ShadowFeatureVector,
} from "./native-v2-shadow-features.js";

export const LEARNED_V2_SHADOW_VERSION = "learned-v2-shadow-v1" as const;

export type LearnedV2ShadowStatus = "ok" | "fail_safe" | "disabled";

export interface LearnedV2ShadowEvaluation {
  version: typeof LEARNED_V2_SHADOW_VERSION;
  status: LearnedV2ShadowStatus;
  /** Fail-soft reason when status !== ok. */
  failReason: LearningModelLoadFailureReason | "NO_MODEL_LOADED" | "FEATURE_SCHEMA_MISMATCH" | "MISSING_MARKET_PRIOR" | "PREDICT_ERROR" | null;
  failDetail: string | null;
  modelId: string | null;
  modelVersion: string | null;
  registryStatus: LearningModelStatus | null;
  featureSchemaVersion: string | null;
  /** Learned fair P(UP). */
  learnedProbabilityUp: number | null;
  learnedProbabilityDown: number | null;
  /** Residual logit (w·x + b). */
  residualLogit: number | null;
  marketPriorUp: number | null;
  /** Deterministic V2 fair for comparison. */
  deterministicV2ProbabilityUp: number | null;
  /** learned − deterministic V2. */
  deltaVsDeterministicV2: number | null;
  evaluatedAt: string;
}

export interface EvaluateLearnedV2ShadowInput {
  enabled: boolean;
  loaded: LoadedLearningModelV1 | null;
  loadFailureReason?: LearningModelLoadFailureReason | null;
  loadFailureDetail?: string | null;
  featureVector: NativeV2ShadowFeatureVector | null;
  deterministicV2: NativeV2ShadowFairProbability | null;
  now?: Date;
}

/**
 * Learned V2 shadow evaluation. Advisory only — never mutates V1 state,
 * never bypasses risk, never submits orders.
 */
export function evaluateLearnedV2Shadow(
  input: EvaluateLearnedV2ShadowInput,
): LearnedV2ShadowEvaluation {
  const evaluatedAt = (input.now ?? new Date()).toISOString();
  const base = {
    version: LEARNED_V2_SHADOW_VERSION,
    modelId: input.loaded?.modelId ?? null,
    modelVersion: input.loaded?.modelVersion ?? null,
    registryStatus: input.loaded?.registryStatus ?? null,
    featureSchemaVersion: input.loaded?.featureSchemaVersion ?? null,
    learnedProbabilityUp: null as number | null,
    learnedProbabilityDown: null as number | null,
    residualLogit: null as number | null,
    marketPriorUp: null as number | null,
    deterministicV2ProbabilityUp: input.deterministicV2?.fairProbabilityUp ?? null,
    deltaVsDeterministicV2: null as number | null,
    evaluatedAt,
  };

  if (!input.enabled) {
    return {
      ...base,
      status: "disabled",
      failReason: null,
      failDetail: null,
    };
  }

  if (!input.loaded) {
    return {
      ...base,
      status: "fail_safe",
      failReason: input.loadFailureReason ?? "NO_MODEL_LOADED",
      failDetail: input.loadFailureDetail ?? "no verified learned model in runtime",
    };
  }

  if (!input.featureVector) {
    return {
      ...base,
      status: "fail_safe",
      failReason: "FEATURE_SCHEMA_MISMATCH",
      failDetail: "missing feature vector",
    };
  }

  if (input.featureVector.version !== input.loaded.featureSchemaVersion) {
    return {
      ...base,
      status: "fail_safe",
      failReason: "FEATURE_SCHEMA_MISMATCH",
      failDetail: `runtime feature ${input.featureVector.version} vs model ${input.loaded.featureSchemaVersion}`,
    };
  }

  if (input.featureVector.version !== NATIVE_V2_SHADOW_FEATURE_VERSION) {
    return {
      ...base,
      status: "fail_safe",
      failReason: "FEATURE_SCHEMA_MISMATCH",
      failDetail: `unsupported runtime feature schema ${input.featureVector.version}`,
    };
  }

  const marketPriorUp = input.featureVector.pair.marketPriorUp;
  if (marketPriorUp === null || marketPriorUp === undefined || !Number.isFinite(marketPriorUp)) {
    return {
      ...base,
      status: "fail_safe",
      failReason: "MISSING_MARKET_PRIOR",
      failDetail: "featureVector.pair.marketPriorUp missing",
    };
  }

  try {
    const pred = predictResidualProbabilityFromFeatures(
      { marketPriorUp, featureVector: input.featureVector },
      input.loaded.model,
    );
    const det = input.deterministicV2?.fairProbabilityUp ?? null;
    return {
      ...base,
      status: "ok",
      failReason: null,
      failDetail: null,
      marketPriorUp,
      learnedProbabilityUp: pred.probabilityUp,
      learnedProbabilityDown: 1 - pred.probabilityUp,
      residualLogit: pred.residualLogit,
      deterministicV2ProbabilityUp: det,
      deltaVsDeterministicV2: det === null ? null : pred.probabilityUp - det,
    };
  } catch (error) {
    return {
      ...base,
      status: "fail_safe",
      failReason: "PREDICT_ERROR",
      failDetail: error instanceof Error ? error.message : String(error),
    };
  }
}
