import type { LearningTrainingRowV1 } from "./learning-dataset.js";

export const LEARNING_RESIDUAL_MODEL_VERSION = "learning-residual-logistic-v1" as const;
export const LEARNING_RESIDUAL_FEATURE_NAMES = [
  "bookPressureL5",
  "orderPressureVelocityPerSecond",
  "shortMomentumBps",
  "volatilityNormalizedMovement",
  "invSqrtTimeToExpiryMinutes",
  "bookPressureL5xInvSqrtTte",
  "upMidMinusMarketPrior",
  "binaryComplementBasis",
  "signalDirection",
  "upImbalanceL5",
  "upSpreadBps",
  "upMicropriceOffsetBps",
] as const;

export type LearningResidualFeatureName = (typeof LEARNING_RESIDUAL_FEATURE_NAMES)[number];

export interface LearningResidualModelV1 {
  modelType: "residual_logistic";
  modelVersion: typeof LEARNING_RESIDUAL_MODEL_VERSION;
  featureNames: LearningResidualFeatureName[];
  /** Weights on residual features (same order as featureNames). */
  weights: number[];
  bias: number;
  /** L2 regularization strength used during fit. */
  l2Lambda: number;
  /** Probability floor/ceiling applied after sigmoid. */
  probabilityFloor: number;
  probabilityCeiling: number;
  trainRowCount: number;
  trainMarketCount: number;
  converged: boolean;
  iterations: number;
}

export interface FitResidualLogisticOptions {
  l2Lambda?: number;
  learningRate?: number;
  maxIterations?: number;
  probabilityFloor?: number;
  probabilityCeiling?: number;
}

/**
 * Inspectable residual logistic around market prior:
 *   logit(p_learned) = logit(p_market) + w·x + b
 * Not a BUY/SELL classifier. Pure TypeScript — no GBDT/ML stack.
 */
export function fitResidualLogisticModel(
  rows: readonly LearningTrainingRowV1[],
  options: FitResidualLogisticOptions = {},
): LearningResidualModelV1 {
  const l2Lambda = options.l2Lambda ?? 1e-2;
  const learningRate = options.learningRate ?? 0.1;
  const maxIterations = options.maxIterations ?? 500;
  const probabilityFloor = options.probabilityFloor ?? 0.01;
  const probabilityCeiling = options.probabilityCeiling ?? 0.99;

  const featureNames = [...LEARNING_RESIDUAL_FEATURE_NAMES];
  const dim = featureNames.length;
  const weights = new Array<number>(dim).fill(0);
  let bias = 0;

  if (rows.length === 0) {
    return {
      modelType: "residual_logistic",
      modelVersion: LEARNING_RESIDUAL_MODEL_VERSION,
      featureNames,
      weights,
      bias,
      l2Lambda,
      probabilityFloor,
      probabilityCeiling,
      trainRowCount: 0,
      trainMarketCount: 0,
      converged: true,
      iterations: 0,
    };
  }

  const xs = rows.map((row) => extractResidualFeatures(row));
  const logitsMarket = rows.map((row) => logit(clampProb(row.marketPriorUp, probabilityFloor, probabilityCeiling)));
  const ys = rows.map((row) => row.labelUp);

  let converged = false;
  let iterations = 0;
  for (let iter = 0; iter < maxIterations; iter += 1) {
    iterations = iter + 1;
    const gradW = new Array<number>(dim).fill(0);
    let gradB = 0;
    let maxAbsGrad = 0;

    for (let i = 0; i < rows.length; i += 1) {
      const x = xs[i]!;
      let residual = bias;
      for (let j = 0; j < dim; j += 1) residual += weights[j]! * x[j]!;
      const p = sigmoid(logitsMarket[i]! + residual);
      const err = p - ys[i]!;
      for (let j = 0; j < dim; j += 1) gradW[j]! += err * x[j]!;
      gradB += err;
    }

    const invN = 1 / rows.length;
    for (let j = 0; j < dim; j += 1) {
      gradW[j] = gradW[j]! * invN + l2Lambda * weights[j]!;
      maxAbsGrad = Math.max(maxAbsGrad, Math.abs(gradW[j]!));
    }
    gradB *= invN;
    maxAbsGrad = Math.max(maxAbsGrad, Math.abs(gradB));

    for (let j = 0; j < dim; j += 1) {
      weights[j]! -= learningRate * gradW[j]!;
    }
    bias -= learningRate * gradB;

    if (maxAbsGrad < 1e-8) {
      converged = true;
      break;
    }
  }

  const markets = new Set(rows.map((r) => r.marketId));
  return {
    modelType: "residual_logistic",
    modelVersion: LEARNING_RESIDUAL_MODEL_VERSION,
    featureNames,
    weights: weights.map((w) => roundCoef(w)),
    bias: roundCoef(bias),
    l2Lambda,
    probabilityFloor,
    probabilityCeiling,
    trainRowCount: rows.length,
    trainMarketCount: markets.size,
    converged,
    iterations,
  };
}

export function predictResidualProbabilityUp(
  row: LearningTrainingRowV1,
  model: LearningResidualModelV1,
): number {
  const x = extractResidualFeatures(row);
  const marketLogit = logit(clampProb(row.marketPriorUp, model.probabilityFloor, model.probabilityCeiling));
  let residual = model.bias;
  for (let j = 0; j < model.featureNames.length; j += 1) {
    residual += model.weights[j]! * x[j]!;
  }
  return clampProb(sigmoid(marketLogit + residual), model.probabilityFloor, model.probabilityCeiling);
}

export function extractResidualFeatures(row: LearningTrainingRowV1): number[] {
  const f = row.featureVector;
  return [
    finiteOrZero(f.pair.bookPressureL5),
    finiteOrZero(f.pair.orderPressureVelocityPerSecond),
    finiteOrZero(f.reference.shortMomentumBps) / 100,
    finiteOrZero(f.reference.volatilityNormalizedMovement),
    finiteOrZero(f.resolutionAligned.invSqrtTimeToExpiryMinutes),
    finiteOrZero(f.resolutionAligned.bookPressureL5xInvSqrtTte),
    finiteOrZero(f.resolutionAligned.upMidMinusMarketPrior),
    finiteOrZero(f.resolutionAligned.binaryComplementBasis),
    finiteOrZero(f.pair.signalDirection),
    finiteOrZero(f.up.imbalanceL5),
    finiteOrZero(f.up.spreadBps) / 1000,
    finiteOrZero(f.up.micropriceOffsetBps) / 1000,
  ];
}

export function scoreBaselines(
  rows: readonly LearningTrainingRowV1[],
  model: LearningResidualModelV1 | null,
): {
  marketPrior: Array<{ probabilityUp: number; labelUp: 0 | 1 }>;
  deterministicV2: Array<{ probabilityUp: number; labelUp: 0 | 1 }>;
  residualLearned: Array<{ probabilityUp: number; labelUp: 0 | 1 }>;
} {
  const marketPrior = rows.map((row) => ({
    probabilityUp: row.marketPriorUp,
    labelUp: row.labelUp,
  }));
  const deterministicV2 = rows.map((row) => ({
    probabilityUp: row.v2FairProbabilityUp,
    labelUp: row.labelUp,
  }));
  const residualLearned = model
    ? rows.map((row) => ({
      probabilityUp: predictResidualProbabilityUp(row, model),
      labelUp: row.labelUp,
    }))
    : [];
  return { marketPrior, deterministicV2, residualLearned };
}

function finiteOrZero(value: number | null | undefined): number {
  return value !== null && value !== undefined && Number.isFinite(value) ? value : 0;
}

function clampProb(value: number, floor: number, ceiling: number): number {
  if (!Number.isFinite(value)) return 0.5;
  return Math.min(ceiling, Math.max(floor, value));
}

function logit(p: number): number {
  const q = Math.min(1 - 1e-15, Math.max(1e-15, p));
  return Math.log(q / (1 - q));
}

function sigmoid(z: number): number {
  if (z >= 0) {
    const e = Math.exp(-z);
    return 1 / (1 + e);
  }
  const e = Math.exp(z);
  return e / (1 + e);
}

function roundCoef(value: number): number {
  return Math.round(value * 1e10) / 1e10;
}
