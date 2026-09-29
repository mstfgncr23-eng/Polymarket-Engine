import type { LearningSampleV1 } from "./learning-sample.js";
import {
  buildLearningDataset,
  writeLearningDatasetArtifact,
  type BuildLearningDatasetOptions,
  type BuildLearningDatasetResult,
} from "./learning-dataset.js";
import {
  splitCalibrationEvaluationByMarket,
  splitWalkForwardByMarket,
  type LearningWalkForwardOptions,
  type LearningWalkForwardSplit,
} from "./learning-walk-forward.js";
import {
  applyIsotonicCalibrator,
  fitIsotonicCalibrator,
  scoreProbabilityMetrics,
} from "./learning-metrics.js";
import {
  fitResidualLogisticModel,
  predictResidualProbabilityUp,
  scoreBaselines,
  type FitResidualLogisticOptions,
  type LearningResidualModelV1,
} from "./learning-residual-model.js";
import {
  evaluateLearningEconomics,
  type EvaluateLearningEconomicsOptions,
} from "./learning-economic.js";
import {
  buildLearningCandidateArtifact,
  writeLearningCandidateArtifact,
  type LearningCandidateArtifactV1,
} from "./learning-candidate.js";

export interface RunLearningOfflinePipelineOptions {
  dataset?: BuildLearningDatasetOptions;
  walkForward?: LearningWalkForwardOptions;
  residual?: FitResidualLogisticOptions;
  economics?: EvaluateLearningEconomicsOptions;
  /** Fraction of validation MARKETS reserved for calibration. Default 0.3. */
  calibrationHoldoutFraction?: number;
  gitSha?: string | null;
  now?: Date;
  outputDir?: string;
}

export interface LearningOfflinePipelineResult {
  dataset: BuildLearningDatasetResult;
  split: LearningWalkForwardSplit;
  model: LearningResidualModelV1;
  candidate: LearningCandidateArtifactV1;
  output?: {
    datasetRowsPath: string;
    datasetManifestPath: string;
    candidatePath: string;
  };
}

/**
 * Paper-only offline forecasting pipeline:
 * finalized dataset → label-safe market walk-forward → residual logistic →
 * baselines + optional market-isolated calibration + economic eval → candidate.
 * Does NOT touch runtime trading authority.
 */
export function runLearningOfflinePipeline(
  samples: readonly LearningSampleV1[],
  options: RunLearningOfflinePipelineOptions = {},
): LearningOfflinePipelineResult {
  const dataset = buildLearningDataset(samples, {
    ...options.dataset,
    gitSha: options.dataset?.gitSha ?? options.gitSha ?? null,
    ...(options.now ? { now: options.now } : {}),
  });

  const split = splitWalkForwardByMarket(dataset.rows, options.walkForward);
  if (split.trainRows.length === 0 || split.validationRows.length === 0) {
    throw new Error(
      `LEARNING_PIPELINE_EMPTY_SPLIT: train=${split.trainRows.length} validation=${split.validationRows.length}`,
    );
  }

  const model = fitResidualLogisticModel(split.trainRows, options.residual);

  // Always score the uncalibrated baselines on the complete untouched validation set.
  const validationScores = scoreBaselines(split.validationRows, model);
  const marketPriorMetrics = scoreProbabilityMetrics(validationScores.marketPrior);
  const deterministicV2Metrics = scoreProbabilityMetrics(validationScores.deterministicV2);
  const residualMetrics = scoreProbabilityMetrics(validationScores.residualLearned);

  // Calibration is optional and must preserve both market isolation and historical
  // label availability. If a safe second boundary cannot be formed, omit the
  // calibrated metric rather than leaking labels or manufacturing a score.
  const calibrationFraction = options.calibrationHoldoutFraction ?? 0.3;
  const calibrationSplit = splitCalibrationEvaluationByMarket(
    split.validationRows,
    calibrationFraction,
  );

  let residualCalibratedMetrics: ReturnType<typeof scoreProbabilityMetrics> | undefined;
  if (calibrationSplit) {
    const calibrationScores = calibrationSplit.calibrationRows.map((row) => ({
      probabilityUp: predictResidualProbabilityUp(row, model),
      labelUp: row.labelUp,
    }));
    const calibrator = fitIsotonicCalibrator(calibrationScores);
    const calibratedScores = calibrationSplit.evaluationRows.map((row) => ({
      probabilityUp: applyIsotonicCalibrator(predictResidualProbabilityUp(row, model), calibrator),
      labelUp: row.labelUp,
    }));
    residualCalibratedMetrics = scoreProbabilityMetrics(calibratedScores);
  }

  const residualProbs = split.validationRows.map((row) => predictResidualProbabilityUp(row, model));
  const economicEvaluation = evaluateLearningEconomics(
    split.validationRows,
    residualProbs,
    options.economics,
  );

  const candidate = buildLearningCandidateArtifact({
    model,
    datasetManifest: dataset.manifest,
    split,
    metrics: {
      marketPrior: marketPriorMetrics,
      deterministicV2: deterministicV2Metrics,
      residualLearned: residualMetrics,
      ...(residualCalibratedMetrics ? { residualCalibrated: residualCalibratedMetrics } : {}),
    },
    economicEvaluation,
    gitSha: options.gitSha ?? null,
    ...(options.now ? { now: options.now } : {}),
  });

  if (options.outputDir) {
    const datasetPaths = writeLearningDatasetArtifact(options.outputDir, dataset);
    const candidatePaths = writeLearningCandidateArtifact(options.outputDir, candidate);
    return {
      dataset,
      split,
      model,
      candidate,
      output: {
        datasetRowsPath: datasetPaths.rowsPath,
        datasetManifestPath: datasetPaths.manifestPath,
        candidatePath: candidatePaths.candidatePath,
      },
    };
  }

  return { dataset, split, model, candidate };
}
