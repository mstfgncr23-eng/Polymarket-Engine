import { createHash, randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import type { LearningCandidateArtifactV1 } from "./learning-candidate.js";
import { verifyLearningCandidateChecksum } from "./learning-candidate.js";
import { LEARNING_RESIDUAL_MODEL_VERSION } from "./learning-residual-model.js";
import { NATIVE_V2_SHADOW_FEATURE_VERSION } from "./native-v2-shadow-features.js";

export const LEARNING_MODEL_REGISTRY_VERSION = "learning-model-registry-v1" as const;

/** Lifecycle states for learned model artifacts. Never auto-promote to PAPER_APPROVED. */
export const LEARNING_MODEL_STATUSES = [
  "CANDIDATE",
  "SHADOW",
  "PAPER_APPROVED",
  "REJECTED",
  "RETIRED",
] as const;

export type LearningModelStatus = (typeof LEARNING_MODEL_STATUSES)[number];

export interface LearningModelRegistryEntryV1 {
  modelId: string;
  status: LearningModelStatus;
  candidateVersion: string;
  modelType: string;
  modelVersion: string;
  featureSchemaVersion: string;
  datasetVersion: string;
  checksumSha256: string;
  /** Absolute or registry-relative path to candidate.json */
  artifactPath: string;
  registeredAt: string;
  updatedAt: string;
  /** Operator note for human review transitions. */
  reviewNote: string | null;
  gitSha: string | null;
  /** True only when a human/operator explicitly set PAPER_APPROVED. Never set by metrics. */
  paperApprovedByOperator: boolean;
}

export interface LearningModelRegistryManifestV1 {
  registryVersion: typeof LEARNING_MODEL_REGISTRY_VERSION;
  updatedAt: string;
  entries: LearningModelRegistryEntryV1[];
}

export interface RegisterCandidateOptions {
  artifactPath: string;
  candidate: LearningCandidateArtifactV1;
  modelId?: string;
  now?: Date;
  reviewNote?: string | null;
}

export interface TransitionModelStatusOptions {
  modelId: string;
  to: LearningModelStatus;
  /** Required for PAPER_APPROVED — proves human/operator intent. */
  operatorConfirmed?: boolean;
  reviewNote?: string | null;
  now?: Date;
}

const ALLOWED_TRANSITIONS: Record<LearningModelStatus, readonly LearningModelStatus[]> = {
  CANDIDATE: ["SHADOW", "REJECTED", "RETIRED"],
  SHADOW: ["PAPER_APPROVED", "REJECTED", "RETIRED", "CANDIDATE"],
  PAPER_APPROVED: ["SHADOW", "RETIRED", "REJECTED"],
  REJECTED: ["RETIRED"],
  RETIRED: [],
};

/**
 * Register a validated offline candidate as CANDIDATE only.
 * Retrain → CANDIDATE. Never auto SHADOW / PAPER_APPROVED.
 */
export function registerLearningCandidate(
  registryDir: string,
  options: RegisterCandidateOptions,
): LearningModelRegistryEntryV1 {
  if (!verifyLearningCandidateChecksum(options.candidate)) {
    throw new Error("LEARNING_REGISTRY_CHECKSUM_MISMATCH");
  }
  assertSupportedCandidate(options.candidate);

  const now = (options.now ?? new Date()).toISOString();
  const modelId = options.modelId ?? `cand-${options.candidate.checksumSha256.slice(0, 12)}-${randomUUID().slice(0, 8)}`;
  const manifest = loadLearningModelRegistry(registryDir);
  if (manifest.entries.some((e) => e.modelId === modelId)) {
    throw new Error(`LEARNING_REGISTRY_DUPLICATE_MODEL_ID: ${modelId}`);
  }

  const entry: LearningModelRegistryEntryV1 = {
    modelId,
    status: "CANDIDATE",
    candidateVersion: options.candidate.candidateVersion,
    modelType: options.candidate.modelType,
    modelVersion: options.candidate.modelVersion,
    featureSchemaVersion: options.candidate.featureSchemaVersion,
    datasetVersion: options.candidate.datasetVersion,
    checksumSha256: options.candidate.checksumSha256,
    artifactPath: options.artifactPath,
    registeredAt: now,
    updatedAt: now,
    reviewNote: options.reviewNote ?? null,
    gitSha: options.candidate.gitSha,
    paperApprovedByOperator: false,
  };

  manifest.entries.push(entry);
  manifest.updatedAt = now;
  saveLearningModelRegistry(registryDir, manifest);
  return entry;
}

/**
 * Operator-driven status transition. PAPER_APPROVED requires operatorConfirmed=true.
 * Metrics never call this. Auto-promotion is rejected.
 */
export function transitionLearningModelStatus(
  registryDir: string,
  options: TransitionModelStatusOptions,
): LearningModelRegistryEntryV1 {
  const manifest = loadLearningModelRegistry(registryDir);
  const entry = manifest.entries.find((e) => e.modelId === options.modelId);
  if (!entry) throw new Error(`LEARNING_REGISTRY_MODEL_NOT_FOUND: ${options.modelId}`);

  const allowed = ALLOWED_TRANSITIONS[entry.status];
  if (!allowed.includes(options.to)) {
    throw new Error(
      `LEARNING_REGISTRY_INVALID_TRANSITION: ${entry.status} → ${options.to}`,
    );
  }

  if (options.to === "PAPER_APPROVED") {
    if (options.operatorConfirmed !== true) {
      throw new Error("LEARNING_REGISTRY_PAPER_REQUIRES_OPERATOR_CONFIRMATION");
    }
    entry.paperApprovedByOperator = true;
  }

  // Demoting away from PAPER_APPROVED clears the operator flag.
  if (entry.status === "PAPER_APPROVED" && options.to !== "PAPER_APPROVED") {
    entry.paperApprovedByOperator = false;
  }

  entry.status = options.to;
  entry.updatedAt = (options.now ?? new Date()).toISOString();
  if (options.reviewNote !== undefined) entry.reviewNote = options.reviewNote;
  manifest.updatedAt = entry.updatedAt;
  saveLearningModelRegistry(registryDir, manifest);
  return { ...entry };
}

export function getLearningModelEntry(
  registryDir: string,
  modelId: string,
): LearningModelRegistryEntryV1 | null {
  const manifest = loadLearningModelRegistry(registryDir);
  return manifest.entries.find((e) => e.modelId === modelId) ?? null;
}

export function listLearningModelEntries(registryDir: string): LearningModelRegistryEntryV1[] {
  return loadLearningModelRegistry(registryDir).entries.map((e) => ({ ...e }));
}

export function learningModelsDir(learningDataDir: string): string {
  return path.join(learningDataDir, "models");
}

export function loadLearningModelRegistry(registryDir: string): LearningModelRegistryManifestV1 {
  mkdirSync(registryDir, { recursive: true });
  const manifestPath = path.join(registryDir, "registry.json");
  if (!existsSync(manifestPath)) {
    return {
      registryVersion: LEARNING_MODEL_REGISTRY_VERSION,
      updatedAt: new Date(0).toISOString(),
      entries: [],
    };
  }
  const raw = JSON.parse(readFileSync(manifestPath, "utf8")) as LearningModelRegistryManifestV1;
  if (raw.registryVersion !== LEARNING_MODEL_REGISTRY_VERSION) {
    throw new Error(
      `LEARNING_REGISTRY_INCOMPATIBLE_VERSION: expected ${LEARNING_MODEL_REGISTRY_VERSION}, got ${String(raw.registryVersion)}`,
    );
  }
  return {
    registryVersion: LEARNING_MODEL_REGISTRY_VERSION,
    updatedAt: raw.updatedAt,
    entries: Array.isArray(raw.entries) ? raw.entries : [],
  };
}

export function saveLearningModelRegistry(
  registryDir: string,
  manifest: LearningModelRegistryManifestV1,
): void {
  mkdirSync(registryDir, { recursive: true });
  const manifestPath = path.join(registryDir, "registry.json");
  const tmp = `${manifestPath}.${process.pid}.tmp`;
  writeFileSync(tmp, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  renameSync(tmp, manifestPath);
}

/** CANDIDATE / SHADOW / REJECTED / RETIRED never grant PAPER trading authority. */
export function statusGrantsPaperAuthority(status: LearningModelStatus): boolean {
  return status === "PAPER_APPROVED";
}

export function assertSupportedCandidate(candidate: LearningCandidateArtifactV1): void {
  if (candidate.modelVersion !== LEARNING_RESIDUAL_MODEL_VERSION) {
    throw new Error(
      `LEARNING_REGISTRY_UNSUPPORTED_MODEL_VERSION: expected ${LEARNING_RESIDUAL_MODEL_VERSION}, got ${candidate.modelVersion}`,
    );
  }
  if (candidate.featureSchemaVersion !== NATIVE_V2_SHADOW_FEATURE_VERSION) {
    throw new Error(
      `LEARNING_REGISTRY_INCOMPATIBLE_FEATURE_SCHEMA: expected ${NATIVE_V2_SHADOW_FEATURE_VERSION}, got ${candidate.featureSchemaVersion}`,
    );
  }
  if (candidate.modelType !== "residual_logistic") {
    throw new Error(`LEARNING_REGISTRY_UNSUPPORTED_MODEL_TYPE: ${candidate.modelType}`);
  }
}

export function registryManifestChecksum(manifest: LearningModelRegistryManifestV1): string {
  return createHash("sha256").update(JSON.stringify(manifest)).digest("hex");
}
