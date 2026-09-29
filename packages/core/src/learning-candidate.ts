import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import type { LearningDatasetManifestV1 } from "./learning-dataset.js";
import type { LearningEconomicEvaluation } from "./learning-economic.js";
import type { LearningProbabilityMetrics } from "./learning-metrics.js";
import type { LearningResidualModelV1 } from "./learning-residual-model.js";
import type { LearningWalkForwardSplit } from "./learning-walk-forward.js";

export const LEARNING_CANDIDATE_VERSION = "learning-candidate-v1" as const;

export interface LearningCandidateMetricsBundle {
  marketPrior: LearningProbabilityMetrics;
  deterministicV2: LearningProbabilityMetrics;
  residualLearned: LearningProbabilityMetrics;
  /** Present only when a leakage-safe calibration-market → evaluation-market boundary exists. */
  residualCalibrated?: LearningProbabilityMetrics;
}

export interface LearningCandidateArtifactV1 {
  candidateVersion: typeof LEARNING_CANDIDATE_VERSION;
  modelType: LearningResidualModelV1["modelType"];
  modelVersion: LearningResidualModelV1["modelVersion"];
  featureSchemaVersion: string;
  datasetVersion: string;
  /** Historical model-freeze/data-availability boundary for this fold. */
  trainingCutoffAt: string;
  trainingWindow: {
    startAt: string;
    endAt: string;
  };
  validationWindow: {
    startAt: string;
    endAt: string;
  };
  trainMarketCount: number;
  validationMarketCount: number;
  trainRowCount: number;
  validationRowCount: number;
  metrics: LearningCandidateMetricsBundle;
  economicEvaluation: LearningEconomicEvaluation;
  model: LearningResidualModelV1;
  datasetManifest: LearningDatasetManifestV1;
  gitSha: string | null;
  /** Generation provenance only; intentionally excluded from semantic checksum. */
  createdAt: string;
  /**
   * SHA-256 of deterministic candidate semantics. Generation timestamps
   * (`createdAt` and datasetManifest.createdAt) are intentionally excluded so
   * identical data/config/model content yields the same identity across runs.
   */
  checksumSha256: string;
}

export interface BuildLearningCandidateInput {
  model: LearningResidualModelV1;
  datasetManifest: LearningDatasetManifestV1;
  split: Pick<
    LearningWalkForwardSplit,
    | "trainingCutoffAt"
    | "trainWindowStartAt"
    | "trainWindowEndAt"
    | "validationWindowStartAt"
    | "validationWindowEndAt"
    | "trainMarketIds"
    | "validationMarketIds"
    | "trainRows"
    | "validationRows"
  >;
  metrics: LearningCandidateMetricsBundle;
  economicEvaluation: LearningEconomicEvaluation;
  gitSha?: string | null;
  now?: Date;
}

export function buildLearningCandidateArtifact(
  input: BuildLearningCandidateInput,
): LearningCandidateArtifactV1 {
  const createdAt = (input.now ?? new Date()).toISOString();
  const base = {
    candidateVersion: LEARNING_CANDIDATE_VERSION,
    modelType: input.model.modelType,
    modelVersion: input.model.modelVersion,
    featureSchemaVersion: input.datasetManifest.featureSchemaVersion,
    datasetVersion: input.datasetManifest.datasetVersion,
    trainingCutoffAt: input.split.trainingCutoffAt,
    trainingWindow: {
      startAt: input.split.trainWindowStartAt,
      endAt: input.split.trainWindowEndAt,
    },
    validationWindow: {
      startAt: input.split.validationWindowStartAt,
      endAt: input.split.validationWindowEndAt,
    },
    trainMarketCount: input.split.trainMarketIds.length,
    validationMarketCount: input.split.validationMarketIds.length,
    trainRowCount: input.split.trainRows.length,
    validationRowCount: input.split.validationRows.length,
    metrics: input.metrics,
    economicEvaluation: input.economicEvaluation,
    model: input.model,
    datasetManifest: input.datasetManifest,
    gitSha: input.gitSha ?? input.datasetManifest.gitSha ?? null,
    createdAt,
  } as const;

  const checksumSha256 = checksumCandidatePayload(base);
  return { ...base, checksumSha256 };
}

export function writeLearningCandidateArtifact(
  outputDir: string,
  candidate: LearningCandidateArtifactV1,
): { candidatePath: string } {
  mkdirSync(outputDir, { recursive: true });
  const candidatePath = path.join(outputDir, "candidate.json");
  writeFileSync(candidatePath, `${JSON.stringify(candidate, null, 2)}\n`, "utf8");
  return { candidatePath };
}

type CandidatePayload = Omit<LearningCandidateArtifactV1, "checksumSha256">;

export function checksumCandidatePayload(payload: CandidatePayload): string {
  const { createdAt: _candidateCreatedAt, datasetManifest, ...rest } = payload;
  const { createdAt: _datasetCreatedAt, ...stableDatasetManifest } = datasetManifest;
  const stablePayload = {
    ...rest,
    datasetManifest: stableDatasetManifest,
  };
  return createHash("sha256").update(JSON.stringify(stablePayload)).digest("hex");
}

/** Recompute deterministic semantic checksum and verify integrity. */
export function verifyLearningCandidateChecksum(candidate: LearningCandidateArtifactV1): boolean {
  const { checksumSha256, ...rest } = candidate;
  return checksumCandidatePayload(rest) === checksumSha256;
}
