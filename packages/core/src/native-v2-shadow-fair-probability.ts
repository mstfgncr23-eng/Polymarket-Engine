import type { NativeV2ShadowFeatureVector } from "./native-v2-shadow-features.js";

export const NATIVE_V2_SHADOW_FAIR_PROBABILITY_VERSION = "native-v2-shadow-fair-probability-v1" as const;
export const NATIVE_V2_SHADOW_CALIBRATION_STATUS = "UNVALIDATED_SHADOW" as const;

export interface NativeV2ShadowFairProbabilityConfig {
  marketPriorWeight: number;
  micropricePriorWeight: number;
  maxDirectionalAdjustment: number;
  probabilityFloor: number;
  probabilityCeiling: number;
}

export const defaultNativeV2ShadowFairProbabilityConfig: NativeV2ShadowFairProbabilityConfig = {
  marketPriorWeight: 0.5,
  micropricePriorWeight: 0.5,
  maxDirectionalAdjustment: 0.05,
  probabilityFloor: 0.01,
  probabilityCeiling: 0.99,
};

export interface NativeV2ShadowFairProbability {
  version: typeof NATIVE_V2_SHADOW_FAIR_PROBABILITY_VERSION;
  calibrationStatus: typeof NATIVE_V2_SHADOW_CALIBRATION_STATUS;
  fairProbabilityUp: number;
  fairProbabilityDown: number;
  marketPriorUp: number;
  micropricePriorUp: number;
  baseProbabilityUp: number;
  directionalEvidenceScore: number;
  probabilityAdjustment: number;
  evidenceFamilyCount: number;
  evidenceFamilyWeighting: "equal_available";
  familyScores: {
    bookPressure: number;
    referenceMomentum: number | null;
    orderPressureVelocity: number | null;
  };
  diagnostics: {
    evidenceRange: number;
    freshReferenceUsed: boolean;
    binaryComplementBasis: number;
    pairSourceSkewMs: number;
    timeToExpiryMs: number | null;
    signalPersistenceMs: number;
    signalPersistenceSamples: number;
  };
}

/**
 * Deterministic Phase-2 shadow estimator.
 *
 * This intentionally does not participate in execution authority. The point
 * estimate anchors to two probability-like book estimates (mid prior and
 * microprice prior), then applies a bounded directional adjustment from three
 * separate feature families. Missing or stale reference evidence is omitted,
 * not silently treated as a neutral observation.
 *
 * The defaults are an explicitly unvalidated shadow hypothesis. Persisted
 * predictions are intended to be calibrated against resolved outcomes before
 * any later authority proposal.
 */
export function evaluateNativeV2ShadowFairProbability(
  features: NativeV2ShadowFeatureVector,
  config: NativeV2ShadowFairProbabilityConfig = defaultNativeV2ShadowFairProbabilityConfig,
): NativeV2ShadowFairProbability | null {
  if (!features.synchronization.marketSynchronized) return null;
  if (!validConfig(config)) return null;

  const marketPriorUp = finiteProbability(features.pair.marketPriorUp);
  const upMicro = finiteProbability(features.up.microprice);
  const downMicro = finiteProbability(features.down.microprice);
  if (marketPriorUp === null || upMicro === null || downMicro === null) return null;

  const micropricePriorUp = clamp((upMicro + (1 - downMicro)) / 2, 0, 1);
  const anchorWeight = config.marketPriorWeight + config.micropricePriorWeight;
  const baseProbabilityUp = (
    config.marketPriorWeight * marketPriorUp
    + config.micropricePriorWeight * micropricePriorUp
  ) / anchorWeight;

  const bookPressure = clampFinite(features.pair.bookPressureL5, -1, 1) ?? 0;
  const referenceMomentum = features.synchronization.referenceFreshness === "fresh"
    ? tanhFinite(features.reference.volatilityNormalizedMovement)
    : null;
  const orderPressureVelocity = tanhFinite(features.pair.orderPressureVelocityPerSecond);

  const evidence = [bookPressure, referenceMomentum, orderPressureVelocity]
    .filter((value): value is number => value !== null && Number.isFinite(value));
  if (evidence.length === 0) return null;

  const directionalEvidenceScore = clamp(
    evidence.reduce((sum, value) => sum + value, 0) / evidence.length,
    -1,
    1,
  );
  const probabilityAdjustment = config.maxDirectionalAdjustment * directionalEvidenceScore;
  const fairProbabilityUp = clamp(
    baseProbabilityUp + probabilityAdjustment,
    config.probabilityFloor,
    config.probabilityCeiling,
  );

  return {
    version: NATIVE_V2_SHADOW_FAIR_PROBABILITY_VERSION,
    calibrationStatus: NATIVE_V2_SHADOW_CALIBRATION_STATUS,
    fairProbabilityUp,
    fairProbabilityDown: 1 - fairProbabilityUp,
    marketPriorUp,
    micropricePriorUp,
    baseProbabilityUp,
    directionalEvidenceScore,
    probabilityAdjustment,
    evidenceFamilyCount: evidence.length,
    evidenceFamilyWeighting: "equal_available",
    familyScores: {
      bookPressure,
      referenceMomentum,
      orderPressureVelocity,
    },
    diagnostics: {
      evidenceRange: Math.max(...evidence) - Math.min(...evidence),
      freshReferenceUsed: referenceMomentum !== null,
      binaryComplementBasis: finiteOrZero(features.pair.binaryComplementBasis),
      pairSourceSkewMs: Math.max(0, finiteOrZero(features.timing.pairSourceSkewMs)),
      timeToExpiryMs: finiteOrNull(features.timing.timeToExpiryMs),
      signalPersistenceMs: Math.max(0, finiteOrZero(features.pair.signalPersistenceMs)),
      signalPersistenceSamples: Math.max(0, Math.trunc(finiteOrZero(features.pair.signalPersistenceSamples))),
    },
  };
}

function validConfig(config: NativeV2ShadowFairProbabilityConfig): boolean {
  return Number.isFinite(config.marketPriorWeight)
    && config.marketPriorWeight >= 0
    && Number.isFinite(config.micropricePriorWeight)
    && config.micropricePriorWeight >= 0
    && config.marketPriorWeight + config.micropricePriorWeight > 0
    && Number.isFinite(config.maxDirectionalAdjustment)
    && config.maxDirectionalAdjustment >= 0
    && Number.isFinite(config.probabilityFloor)
    && Number.isFinite(config.probabilityCeiling)
    && config.probabilityFloor >= 0
    && config.probabilityCeiling <= 1
    && config.probabilityFloor < config.probabilityCeiling;
}

function finiteProbability(value: number): number | null {
  return Number.isFinite(value) && value >= 0 && value <= 1 ? value : null;
}

function tanhFinite(value: number | null): number | null {
  return value !== null && Number.isFinite(value) ? Math.tanh(value) : null;
}

function clampFinite(value: number, min: number, max: number): number | null {
  return Number.isFinite(value) ? clamp(value, min, max) : null;
}

function finiteOrZero(value: number): number { return Number.isFinite(value) ? value : 0; }
function finiteOrNull(value: number | null): number | null { return value !== null && Number.isFinite(value) ? value : null; }
function clamp(value: number, min: number, max: number): number { return Math.min(max, Math.max(min, value)); }
