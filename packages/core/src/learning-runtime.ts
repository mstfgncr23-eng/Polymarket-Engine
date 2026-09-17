import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import type { LearningCandidateArtifactV1 } from "./learning-candidate.js";
import {
  loadLearningModelFromArtifact,
  loadLearningModelFromRegistryEntry,
  type LearningModelLoadFailureReason,
  type LoadedLearningModelV1,
} from "./learning-model-loader.js";
import {
  getLearningModelEntry,
  learningModelsDir,
  type LearningModelRegistryEntryV1,
} from "./learning-model-registry.js";
import {
  evaluateControlledPaperAuthorityGate,
  type ControlledPaperAuthorityConfig,
  type ControlledPaperAuthorityDecision,
} from "./learning-paper-authority.js";
import {
  buildLearningScorecardRecord,
  LearningScorecardStore,
  learningScorecardDir,
  type LearningScorecardRecordV1,
} from "./learning-scorecard.js";
import {
  evaluateLearnedV2Shadow,
  type LearnedV2ShadowEvaluation,
} from "./learning-shadow-runtime.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import type { NativeV2ShadowFeatureVector } from "./native-v2-shadow-features.js";

export interface LearningRuntimeConfig {
  /** LEARNING_SHADOW_ENABLED — default false. */
  shadowEnabled: boolean;
  learningDataDir: string;
  /** Absolute path to candidate.json, or null. */
  modelArtifactPath: string | null;
  /** Registry model id to load, or null. */
  modelId: string | null;
  /** Controlled PAPER path — default false; execution hook not implemented. */
  paperAuthority: ControlledPaperAuthorityConfig;
}

export interface LearningRuntimeSummary {
  shadowEnabled: boolean;
  modelLoaded: boolean;
  modelId: string | null;
  registryStatus: string | null;
  modelVersion: string | null;
  featureSchemaVersion: string | null;
  loadFailureReason: LearningModelLoadFailureReason | null;
  loadFailureDetail: string | null;
  evaluations: number;
  evaluationsOk: number;
  evaluationsFailSafe: number;
  scorecard: ReturnType<LearningScorecardStore["summary"]> | { enabled: false };
  paperAuthority: ControlledPaperAuthorityDecision;
}

/**
 * Fail-soft learned runtime facade used by the engine.
 * Never mutates V1 ledger/decision/risk/execution authority.
 */
export class LearningRuntime {
  private loaded: LoadedLearningModelV1 | null = null;
  private loadFailureReason: LearningModelLoadFailureReason | null = null;
  private loadFailureDetail: string | null = null;
  private registryEntry: LearningModelRegistryEntryV1 | null = null;
  private scorecardStore: LearningScorecardStore | null = null;
  private evaluations = 0;
  private evaluationsOk = 0;
  private evaluationsFailSafe = 0;

  constructor(private readonly config: LearningRuntimeConfig) {}

  init(): void {
    if (!this.config.shadowEnabled) return;

    try {
      this.scorecardStore = new LearningScorecardStore({
        dataDir: learningScorecardDir(this.config.learningDataDir),
      });
      this.scorecardStore.init();
    } catch {
      // Scorecard is advisory/best-effort. Model load may still work.
      this.scorecardStore = null;
    }

    try {
      const result = this.resolveAndLoad();
      if (result.ok) {
        this.loaded = result.loaded;
        this.loadFailureReason = null;
        this.loadFailureDetail = null;
      } else {
        this.loaded = null;
        this.loadFailureReason = result.reason;
        this.loadFailureDetail = result.detail;
      }
    } catch (error) {
      // Registry corruption, permission errors, or unexpected IO must never
      // escape into engine startup. Learned shadow simply fails safe.
      this.loaded = null;
      this.registryEntry = null;
      this.loadFailureReason = "RUNTIME_INIT_ERROR";
      this.loadFailureDetail = error instanceof Error ? error.message : String(error);
    }
  }

  /**
   * Evaluate learned shadow. Fail-soft: never throws into trading path.
   */
  evaluateShadow(input: {
    featureVector: NativeV2ShadowFeatureVector | null;
    deterministicV2: NativeV2ShadowFairProbability | null;
    marketId: string;
    asset?: string | null;
    experimentId?: string | null;
    triggerId?: string | null;
    traceId?: string | null;
    now?: Date;
    recordScorecard?: boolean;
  }): LearnedV2ShadowEvaluation {
    const evaluation = evaluateLearnedV2Shadow({
      enabled: this.config.shadowEnabled,
      loaded: this.loaded,
      loadFailureReason: this.loadFailureReason,
      loadFailureDetail: this.loadFailureDetail,
      featureVector: input.featureVector,
      deterministicV2: input.deterministicV2,
      ...(input.now ? { now: input.now } : {}),
    });

    if (this.config.shadowEnabled) {
      this.evaluations += 1;
      if (evaluation.status === "ok") this.evaluationsOk += 1;
      else if (evaluation.status === "fail_safe") this.evaluationsFailSafe += 1;
    }

    if (
      this.config.shadowEnabled
      && input.recordScorecard !== false
      && this.scorecardStore
      && evaluation.status !== "disabled"
    ) {
      try {
        const record = buildLearningScorecardRecord({
          evaluation,
          marketId: input.marketId,
          asset: input.asset ?? null,
          experimentId: input.experimentId ?? null,
          triggerId: input.triggerId ?? null,
          traceId: input.traceId ?? null,
        });
        this.scorecardStore.append(record);
      } catch {
        // Scorecard must never affect V1.
      }
    }

    return evaluation;
  }

  /** Snapshot for operator / tests — does not expose mutable V1 state. */
  summary(): LearningRuntimeSummary {
    const paperAuthority = evaluateControlledPaperAuthorityGate({
      config: this.config.paperAuthority,
      modelId: this.loaded?.modelId ?? this.registryEntry?.modelId ?? null,
      registryStatus: this.loaded?.registryStatus ?? this.registryEntry?.status ?? null,
      paperApprovedByOperator: this.registryEntry?.paperApprovedByOperator ?? false,
      deterministicRiskMandatory: true,
      executionGatesMandatory: true,
      tradingMode: "paper",
    });

    return {
      shadowEnabled: this.config.shadowEnabled,
      modelLoaded: this.loaded !== null,
      modelId: this.loaded?.modelId ?? null,
      registryStatus: this.loaded?.registryStatus ?? null,
      modelVersion: this.loaded?.modelVersion ?? null,
      featureSchemaVersion: this.loaded?.featureSchemaVersion ?? null,
      loadFailureReason: this.loadFailureReason,
      loadFailureDetail: this.loadFailureDetail,
      evaluations: this.evaluations,
      evaluationsOk: this.evaluationsOk,
      evaluationsFailSafe: this.evaluationsFailSafe,
      scorecard: this.scorecardStore?.summary() ?? { enabled: false },
      paperAuthority,
    };
  }

  /** Test/inspection access — loaded model is immutable advisory state. */
  getLoadedModel(): LoadedLearningModelV1 | null {
    return this.loaded;
  }

  readScorecardsForTest(): LearningScorecardRecordV1[] {
    return this.scorecardStore?.readAllSync() ?? [];
  }

  private resolveAndLoad() {
    if (this.config.modelId) {
      const registryDir = learningModelsDir(this.config.learningDataDir);
      const entry = getLearningModelEntry(registryDir, this.config.modelId);
      if (!entry) {
        return {
          ok: false as const,
          reason: "MISSING_ARTIFACT" as const,
          detail: `registry modelId not found: ${this.config.modelId}`,
        };
      }
      this.registryEntry = entry;
      return loadLearningModelFromRegistryEntry(entry);
    }

    if (this.config.modelArtifactPath) {
      const artifactPath = path.resolve(this.config.modelArtifactPath);
      // Optional: if a sibling registry exists, leave status null (direct path load).
      if (existsSync(artifactPath)) {
        try {
          const raw = JSON.parse(readFileSync(artifactPath, "utf8")) as LearningCandidateArtifactV1;
          void raw;
        } catch {
          // loadLearningModelFromArtifact handles corrupt JSON
        }
      }
      return loadLearningModelFromArtifact(artifactPath, {
        modelId: null,
        registryStatus: null,
      });
    }

    return {
      ok: false as const,
      reason: "MISSING_ARTIFACT" as const,
      detail: "LEARNING_SHADOW_MODEL_PATH or LEARNING_SHADOW_MODEL_ID required when shadow enabled",
    };
  }
}
