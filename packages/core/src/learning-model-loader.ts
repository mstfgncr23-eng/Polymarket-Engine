import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import {
  LEARNING_CANDIDATE_VERSION,
  type LearningCandidateArtifactV1,
  verifyLearningCandidateChecksum,
} from "./learning-candidate.js";
import {
  assertSupportedCandidate,
  type LearningModelRegistryEntryV1,
  type LearningModelStatus,
} from "./learning-model-registry.js";
import {
  LEARNING_RESIDUAL_MODEL_VERSION,
  type LearningResidualModelV1,
} from "./learning-residual-model.js";
import { NATIVE_V2_SHADOW_FEATURE_VERSION } from "./native-v2-shadow-features.js";

export const LEARNING_LOADED_MODEL_VERSION = "learning-loaded-model-v1" as const;

export type LearningModelLoadFailureReason =
  | "MISSING_ARTIFACT"
  | "CORRUPT_JSON"
  | "CHECKSUM_MISMATCH"
  | "INCOMPATIBLE_CANDIDATE_VERSION"
  | "INCOMPATIBLE_FEATURE_SCHEMA"
  | "UNSUPPORTED_MODEL_VERSION"
  | "UNSUPPORTED_MODEL_TYPE"
  | "STALE_OR_INCOMPLETE_METADATA"
  | "REGISTRY_STATUS_BLOCKED"
  | "RUNTIME_INIT_ERROR";

export interface LoadedLearningModelV1 {
  loadedVersion: typeof LEARNING_LOADED_MODEL_VERSION;
  modelId: string | null;
  registryStatus: LearningModelStatus | null;
  candidate: LearningCandidateArtifactV1;
  model: LearningResidualModelV1;
  artifactPath: string;
  verifiedChecksumSha256: string;
  featureSchemaVersion: string;
  modelVersion: string;
  datasetVersion: string;
}

export type LearningModelLoadResult =
  | { ok: true; loaded: LoadedLearningModelV1 }
  | { ok: false; reason: LearningModelLoadFailureReason; detail: string };

export interface LoadLearningModelOptions {
  /**
   * When loading for SHADOW runtime, allow CANDIDATE or SHADOW.
   * PAPER_APPROVED is also allowed for shadow observation.
   * REJECTED / RETIRED are never loadable for runtime.
   */
  allowedStatuses?: readonly LearningModelStatus[];
  expectedFeatureSchemaVersion?: string;
  expectedModelVersion?: string;
}

const DEFAULT_SHADOW_STATUSES: readonly LearningModelStatus[] = [
  "CANDIDATE",
  "SHADOW",
  "PAPER_APPROVED",
];

/**
 * Safely load a candidate artifact for runtime shadow use.
 * On any verification failure returns ok:false — callers must fail-soft.
 */
export function loadLearningModelFromArtifact(
  artifactPath: string,
  options: LoadLearningModelOptions & {
    modelId?: string | null;
    registryStatus?: LearningModelStatus | null;
  } = {},
): LearningModelLoadResult {
  if (!existsSync(artifactPath)) {
    return { ok: false, reason: "MISSING_ARTIFACT", detail: artifactPath };
  }

  let candidate: LearningCandidateArtifactV1;
  try {
    candidate = JSON.parse(readFileSync(artifactPath, "utf8")) as LearningCandidateArtifactV1;
  } catch (error) {
    return {
      ok: false,
      reason: "CORRUPT_JSON",
      detail: error instanceof Error ? error.message : String(error),
    };
  }

  return validateAndWrapCandidate(candidate, artifactPath, options);
}

/**
 * Load via registry entry (path + status gate).
 */
export function loadLearningModelFromRegistryEntry(
  entry: LearningModelRegistryEntryV1,
  options: LoadLearningModelOptions = {},
): LearningModelLoadResult {
  const allowed = options.allowedStatuses ?? DEFAULT_SHADOW_STATUSES;
  if (!allowed.includes(entry.status)) {
    return {
      ok: false,
      reason: "REGISTRY_STATUS_BLOCKED",
      detail: `status=${entry.status} not in [${allowed.join(",")}]`,
    };
  }
  const artifactPath = path.isAbsolute(entry.artifactPath)
    ? entry.artifactPath
    : path.resolve(entry.artifactPath);

  const result = loadLearningModelFromArtifact(artifactPath, {
    ...options,
    modelId: entry.modelId,
    registryStatus: entry.status,
  });
  if (!result.ok) return result;

  if (result.loaded.verifiedChecksumSha256 !== entry.checksumSha256) {
    return {
      ok: false,
      reason: "CHECKSUM_MISMATCH",
      detail: "registry checksum does not match verified artifact checksum",
    };
  }
  return result;
}

function validateAndWrapCandidate(
  candidate: LearningCandidateArtifactV1,
  artifactPath: string,
  options: LoadLearningModelOptions & {
    modelId?: string | null;
    registryStatus?: LearningModelStatus | null;
  },
): LearningModelLoadResult {
  if (candidate.candidateVersion !== LEARNING_CANDIDATE_VERSION) {
    return {
      ok: false,
      reason: "INCOMPATIBLE_CANDIDATE_VERSION",
      detail: `expected ${LEARNING_CANDIDATE_VERSION}, got ${String(candidate.candidateVersion)}`,
    };
  }

  if (!candidate.checksumSha256 || !candidate.model || !candidate.datasetVersion) {
    return {
      ok: false,
      reason: "STALE_OR_INCOMPLETE_METADATA",
      detail: "missing checksum, model, or datasetVersion",
    };
  }

  // Check explicit schema/version contracts before integrity verification so an
  // artifact produced for an older runtime reports the actionable compatibility
  // reason. It is still rejected unless the semantic checksum also verifies.
  const expectedFeature =
    options.expectedFeatureSchemaVersion ?? NATIVE_V2_SHADOW_FEATURE_VERSION;
  if (candidate.featureSchemaVersion !== expectedFeature) {
    return {
      ok: false,
      reason: "INCOMPATIBLE_FEATURE_SCHEMA",
      detail: `expected ${expectedFeature}, got ${candidate.featureSchemaVersion}`,
    };
  }

  const expectedModel = options.expectedModelVersion ?? LEARNING_RESIDUAL_MODEL_VERSION;
  if (candidate.modelVersion !== expectedModel) {
    return {
      ok: false,
      reason: "UNSUPPORTED_MODEL_VERSION",
      detail: `expected ${expectedModel}, got ${candidate.modelVersion}`,
    };
  }

  // PR4 semantic checksum intentionally excludes generation-only timestamps.
  if (!verifyLearningCandidateChecksum(candidate)) {
    return { ok: false, reason: "CHECKSUM_MISMATCH", detail: "artifact checksum verification failed" };
  }

  try {
    assertSupportedCandidate(candidate);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("FEATURE_SCHEMA")) {
      return { ok: false, reason: "INCOMPATIBLE_FEATURE_SCHEMA", detail: message };
    }
    if (message.includes("MODEL_VERSION")) {
      return { ok: false, reason: "UNSUPPORTED_MODEL_VERSION", detail: message };
    }
    return { ok: false, reason: "UNSUPPORTED_MODEL_TYPE", detail: message };
  }

  if (candidate.model.modelVersion !== expectedModel) {
    return {
      ok: false,
      reason: "UNSUPPORTED_MODEL_VERSION",
      detail: `embedded model version ${candidate.model.modelVersion}`,
    };
  }

  return {
    ok: true,
    loaded: {
      loadedVersion: LEARNING_LOADED_MODEL_VERSION,
      modelId: options.modelId ?? null,
      registryStatus: options.registryStatus ?? null,
      candidate,
      model: candidate.model,
      artifactPath,
      verifiedChecksumSha256: candidate.checksumSha256,
      featureSchemaVersion: candidate.featureSchemaVersion,
      modelVersion: candidate.modelVersion,
      datasetVersion: candidate.datasetVersion,
    },
  };
}
