import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import type { LearnedV2ShadowEvaluation } from "./learning-shadow-runtime.js";
import type { Outcome } from "./domain.js";

export const LEARNING_SCORECARD_VERSION = "learning-scorecard-v1" as const;

/**
 * Shadow scorecard row for later Brier/log-loss / economic counterfactual joins.
 * Settlement fields start unresolved; label later via official resolution only.
 */
export interface LearningScorecardRecordV1 {
  schemaVersion: typeof LEARNING_SCORECARD_VERSION;
  recordId: string;
  recordedAt: string;
  marketId: string;
  asset: string | null;
  experimentId: string | null;
  triggerId: string | null;
  traceId: string | null;
  modelId: string | null;
  modelVersion: string | null;
  registryStatus: string | null;
  /** Learned shadow prediction. */
  predictionUp: number | null;
  residualLogit: number | null;
  marketPriorUp: number | null;
  /** Deterministic V2 comparison. */
  deterministicV2ProbabilityUp: number | null;
  deltaVsDeterministicV2: number | null;
  shadowStatus: LearnedV2ShadowEvaluation["status"];
  failReason: string | null;
  /** Filled when official settlement is known. */
  settlement: {
    status: "unresolved" | "resolved";
    outcome: Outcome | null;
    labeledAt: string | null;
  };
  /** Optional per-row metric contributions once labeled. */
  metrics: {
    brierContribution: number | null;
    logLossContribution: number | null;
  };
  /** Optional economic counterfactual join key / result when available. */
  economicCounterfactual: {
    available: boolean;
    realizedPnlPerShare: number | null;
    note: string | null;
  };
}

export function buildLearningScorecardRecord(input: {
  evaluation: LearnedV2ShadowEvaluation;
  marketId: string;
  asset?: string | null;
  experimentId?: string | null;
  triggerId?: string | null;
  traceId?: string | null;
  recordId?: string;
}): LearningScorecardRecordV1 {
  const e = input.evaluation;
  return {
    schemaVersion: LEARNING_SCORECARD_VERSION,
    recordId: input.recordId ?? `${input.marketId}::${e.evaluatedAt}::${e.modelId ?? "none"}`,
    recordedAt: e.evaluatedAt,
    marketId: input.marketId,
    asset: input.asset ?? null,
    experimentId: input.experimentId ?? null,
    triggerId: input.triggerId ?? null,
    traceId: input.traceId ?? null,
    modelId: e.modelId,
    modelVersion: e.modelVersion,
    registryStatus: e.registryStatus,
    predictionUp: e.learnedProbabilityUp,
    residualLogit: e.residualLogit,
    marketPriorUp: e.marketPriorUp,
    deterministicV2ProbabilityUp: e.deterministicV2ProbabilityUp,
    deltaVsDeterministicV2: e.deltaVsDeterministicV2,
    shadowStatus: e.status,
    failReason: e.failReason,
    settlement: {
      status: "unresolved",
      outcome: null,
      labeledAt: null,
    },
    metrics: {
      brierContribution: null,
      logLossContribution: null,
    },
    economicCounterfactual: {
      available: false,
      realizedPnlPerShare: null,
      note: "Counterfactual ledger join not wired in PR5; reserved for follow-up",
    },
  };
}

/** Apply official settlement label and metric contributions (pure). */
export function labelScorecardRecord(
  record: LearningScorecardRecordV1,
  outcome: Outcome,
  labeledAt: string,
): LearningScorecardRecordV1 {
  const labelUp: 0 | 1 = outcome === "UP" ? 1 : 0;
  const p = record.predictionUp;
  let brierContribution: number | null = null;
  let logLossContribution: number | null = null;
  if (p !== null && Number.isFinite(p)) {
    const clamped = Math.min(1 - 1e-15, Math.max(1e-15, p));
    const err = clamped - labelUp;
    brierContribution = err * err;
    logLossContribution = -(labelUp === 1 ? Math.log(clamped) : Math.log(1 - clamped));
  }
  return {
    ...record,
    settlement: { status: "resolved", outcome, labeledAt },
    metrics: { brierContribution, logLossContribution },
  };
}

export function learningScorecardDir(learningDataDir: string): string {
  return path.join(learningDataDir, "scorecards");
}

/**
 * Bounded append-only scorecard writer. Failures throw; callers should catch
 * and fail-soft so V1 trading continues.
 */
export class LearningScorecardStore {
  private segmentIndex = 1;
  private bytes = 0;
  private records = 0;
  private degraded = false;
  private lastError: string | null = null;

  constructor(
    private readonly options: {
      dataDir: string;
      maxBytes?: number;
      maxRecordsInMemory?: number;
    },
  ) {}

  init(): void {
    mkdirSync(this.options.dataDir, { recursive: true });
    mkdirSync(path.join(this.options.dataDir, "segments"), { recursive: true });
  }

  append(record: LearningScorecardRecordV1): void {
    try {
      const line = `${JSON.stringify(record)}\n`;
      const segmentPath = path.join(
        this.options.dataDir,
        "segments",
        `segment-${String(this.segmentIndex).padStart(5, "0")}.jsonl`,
      );
      appendFileSync(segmentPath, line, "utf8");
      this.bytes += Buffer.byteLength(line, "utf8");
      this.records += 1;
      const maxBytes = this.options.maxBytes ?? 64_000_000;
      if (this.bytes > maxBytes) {
        this.degraded = true;
        this.lastError = "SCORECARD_MAX_BYTES";
      }
    } catch (error) {
      this.degraded = true;
      this.lastError = error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  summary(): {
    enabled: true;
    records: number;
    bytes: number;
    degraded: boolean;
    lastError: string | null;
  } {
    return {
      enabled: true,
      records: this.records,
      bytes: this.bytes,
      degraded: this.degraded,
      lastError: this.lastError,
    };
  }

  /** Test helper: read all scorecard lines from disk. */
  readAllSync(): LearningScorecardRecordV1[] {
    const segmentsDir = path.join(this.options.dataDir, "segments");
    if (!existsSync(segmentsDir)) return [];
    const files = readdirSync(segmentsDir).filter((f) => f.endsWith(".jsonl")).sort();
    const out: LearningScorecardRecordV1[] = [];
    for (const file of files) {
      const text = readFileSync(path.join(segmentsDir, file), "utf8");
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        out.push(JSON.parse(line) as LearningScorecardRecordV1);
      }
    }
    return out;
  }
}
