import {
  AUDIT_CRITICAL_EVENT_TYPES,
  LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES,
  MONITOR_CONTROL_EVENT_TYPES,
  RETENTION_DEFAULTS,
  TELEMETRY_EVENT_TYPES,
  assertMcpIdempotencyTtlSafe,
  clampRetentionBatchSize,
  engineEnvSchema,
  type EngineEnv,
} from "@pm/core";
import { isCanonicalFinalReport, type EventJournal } from "@pm/db";
import { LocalJournalArchive } from "./archive.js";
import { loadVerifiedAnalyticsRollup, persistVerifiedAnalyticsRollup, streamExperimentSummary } from "./final-report.js";

export interface RetentionConfig {
  enabled: boolean;
  telemetryTtlMs: number;
  monitorTtlMs: number;
  completedGraceMs: number;
  mcpIdempotencyTtlMs: number;
  checkpointKeep: number;
  batchSize: number;
  cycleBudgetMs: number;
  maxBatchesPerCycle: number;
  maxRowsPerCycle: number;
  compactionEnabled: boolean;
  analyticsRollupEnabled: boolean;
  localArchiveEnabled: boolean;
  localArchiveDir: string;
  localArchiveBucketMs: number;
  compactionReplayKeyTtlMs: number;
  compactionReplayKeyLimit: number;
}

export type RetentionExperiment = { id: string; status: string; startedAt?: string | null } | null;

export interface RetentionContext {
  experiment: RetentionExperiment;
  now: Date;
  degraded: boolean;
  failClosed: boolean;
  databaseUp: boolean;
}

export interface RetentionCycleResult {
  skipped: boolean;
  skipReason: string | null;
  deletedByClass: Record<string, number>;
  failures: number;
  durationMs: number;
  batchCount: number;
  lastBatchSize: number;
  lastBatchDurationMs: number;
  transientEligible: boolean;
  monitorOnlyEligible: boolean;
  durableTransitionsPruned: boolean;
  durableCompaction?: { deleted: number; cursor: number; highWater: number; compactedOperationKeys: number };
  eligibleBacklog: number;
}

type RetentionEnv = Pick<EngineEnv,
  | "RETENTION_ENABLED"
  | "RETENTION_TELEMETRY_TTL_MS"
  | "RETENTION_MONITOR_TTL_MS"
  | "RETENTION_COMPLETED_GRACE_MS"
  | "RETENTION_MCP_IDEMPOTENCY_TTL_MS"
  | "RETENTION_CHECKPOINT_KEEP"
  | "RETENTION_BATCH_SIZE"
  | "RETENTION_CYCLE_BUDGET_MS"
  | "RETENTION_MAX_BATCHES_PER_CYCLE"
  | "RETENTION_MAX_ROWS_PER_CYCLE"
  | "RETENTION_COMPACTION_ENABLED"
  | "RETENTION_ANALYTICS_ROLLUP_ENABLED"
  | "LOCAL_ARCHIVE_ENABLED"
  | "LOCAL_ARCHIVE_DIR"
  | "LOCAL_ARCHIVE_BUCKET_MS"
>;

export function retentionConfigFromEngineEnv(env: RetentionEnv): RetentionConfig {
  return defaultRetentionConfig({
    enabled: env.RETENTION_ENABLED,
    telemetryTtlMs: env.RETENTION_TELEMETRY_TTL_MS,
    monitorTtlMs: env.RETENTION_MONITOR_TTL_MS,
    completedGraceMs: env.RETENTION_COMPLETED_GRACE_MS,
    mcpIdempotencyTtlMs: env.RETENTION_MCP_IDEMPOTENCY_TTL_MS,
    checkpointKeep: env.RETENTION_CHECKPOINT_KEEP,
    batchSize: env.RETENTION_BATCH_SIZE,
    cycleBudgetMs: env.RETENTION_CYCLE_BUDGET_MS,
    maxBatchesPerCycle: env.RETENTION_MAX_BATCHES_PER_CYCLE,
    maxRowsPerCycle: env.RETENTION_MAX_ROWS_PER_CYCLE,
    compactionEnabled: env.RETENTION_COMPACTION_ENABLED,
    analyticsRollupEnabled: env.RETENTION_ANALYTICS_ROLLUP_ENABLED,
    localArchiveEnabled: env.LOCAL_ARCHIVE_ENABLED,
    localArchiveDir: env.LOCAL_ARCHIVE_DIR,
    localArchiveBucketMs: env.LOCAL_ARCHIVE_BUCKET_MS,
  });
}

export function retentionConfigFromEnv(env: NodeJS.ProcessEnv = process.env): RetentionConfig {
  return retentionConfigFromEngineEnv(engineEnvSchema.parse(env));
}

export function defaultRetentionConfig(overrides: Partial<RetentionConfig> = {}): RetentionConfig {
  const config: RetentionConfig = {
    enabled: overrides.enabled ?? RETENTION_DEFAULTS.enabled,
    telemetryTtlMs: overrides.telemetryTtlMs ?? RETENTION_DEFAULTS.telemetryTtlMs,
    monitorTtlMs: overrides.monitorTtlMs ?? RETENTION_DEFAULTS.monitorTtlMs,
    completedGraceMs: overrides.completedGraceMs ?? RETENTION_DEFAULTS.completedGraceMs,
    mcpIdempotencyTtlMs: overrides.mcpIdempotencyTtlMs ?? RETENTION_DEFAULTS.mcpIdempotencyTtlMs,
    checkpointKeep: overrides.checkpointKeep ?? RETENTION_DEFAULTS.checkpointKeep,
    batchSize: clampRetentionBatchSize(overrides.batchSize ?? RETENTION_DEFAULTS.batchSize),
    cycleBudgetMs: overrides.cycleBudgetMs ?? RETENTION_DEFAULTS.cycleBudgetMs,
    maxBatchesPerCycle: overrides.maxBatchesPerCycle ?? RETENTION_DEFAULTS.maxBatchesPerCycle,
    maxRowsPerCycle: overrides.maxRowsPerCycle ?? RETENTION_DEFAULTS.maxRowsPerCycle,
    compactionEnabled: overrides.compactionEnabled ?? false,
    analyticsRollupEnabled: overrides.analyticsRollupEnabled ?? false,
    localArchiveEnabled: overrides.localArchiveEnabled ?? RETENTION_DEFAULTS.localArchiveEnabled,
    localArchiveDir: overrides.localArchiveDir ?? RETENTION_DEFAULTS.localArchiveDir,
    localArchiveBucketMs: overrides.localArchiveBucketMs ?? RETENTION_DEFAULTS.localArchiveBucketMs,
    compactionReplayKeyTtlMs: overrides.compactionReplayKeyTtlMs ?? 7 * 24 * 60 * 60_000,
    compactionReplayKeyLimit: overrides.compactionReplayKeyLimit ?? 4_096,
  };
  assertMcpIdempotencyTtlSafe(config.mcpIdempotencyTtlMs);
  return config;
}

export function experimentIsActive(experiment: { status?: string | null } | null): boolean {
  return experiment?.status === "RUNNING" || experiment?.status === "ENTRY_CLOSED";
}

export function experimentIsMonitorOnly(experiment: RetentionExperiment): boolean {
  if (experiment?.status === "UNKNOWN") return false;
  return experiment == null
    || experiment.status === "IDLE"
    || experiment.status === "MONITOR_ONLY"
    || experiment.status === "ABORTED_START"
    || experiment.status === "";
}

export async function completedReportUnlocksTransient(
  journal: EventJournal,
  experiment: RetentionExperiment,
  now: Date,
  graceMs: number,
): Promise<boolean> {
  if (!experiment || experiment.status !== "COMPLETED") return false;
  const report = await journal.loadExperimentReport(experiment.id, "final");
  if (!report || !isCanonicalFinalReport(report)) return false;
  return now.getTime() - Date.parse(report.persistedAt) >= graceMs;
}

export function monitorCutoff(input: {
  now: Date;
  monitorTtlMs: number;
  experiment: RetentionExperiment;
  monitorOnlyEligible: boolean;
  transientEligible: boolean;
}): Date | null {
  const ttlCutoff = new Date(input.now.getTime() - input.monitorTtlMs);
  if (experimentIsActive(input.experiment)) {
    const startedAt = input.experiment?.startedAt ? Date.parse(input.experiment.startedAt) : Number.NaN;
    if (!Number.isFinite(startedAt)) return null;
    return new Date(Math.min(ttlCutoff.getTime(), startedAt));
  }
  if (input.monitorOnlyEligible || input.transientEligible) return ttlCutoff;
  return null;
}

function fullArchiveBucket(
  oldestOccurredAt: string | null,
  cutoff: Date | null,
  bucketMs: number,
): { from: Date; before: Date } | null {
  if (!oldestOccurredAt || !cutoff) return null;
  const oldest = Date.parse(oldestOccurredAt);
  if (!Number.isFinite(oldest)) return null;
  const fromMs = Math.floor(oldest / bucketMs) * bucketMs;
  const beforeMs = fromMs + bucketMs;
  if (beforeMs > cutoff.getTime()) return null;
  return { from: new Date(fromMs), before: new Date(beforeMs) };
}

export class RetentionManager {
  private running = false;
  private readonly lastSuccessAt = { value: null as string | null };
  private readonly archive: LocalJournalArchive | null;
  runs = 0;
  failures = 0;

  constructor(
    private readonly journal: EventJournal,
    private readonly config: RetentionConfig,
    private readonly metrics?: {
      noteDeleted?(cls: string, n: number): void;
      noteFailure?(): void;
      noteRun?(skipped: boolean): void;
      noteLastSuccess?(iso: string): void;
      noteBatch?(size: number, durationMs: number): void;
    },
    archiveOverride?: LocalJournalArchive | null,
  ) {
    assertMcpIdempotencyTtlSafe(config.mcpIdempotencyTtlMs);
    this.archive = archiveOverride !== undefined
      ? archiveOverride
      : config.localArchiveEnabled
        ? new LocalJournalArchive(config.localArchiveDir)
        : null;
  }

  get inFlight(): boolean { return this.running; }

  async runCycle(context: RetentionContext): Promise<RetentionCycleResult> {
    const started = Date.now();
    const deletedByClass: Record<string, number> = {
      raw_telemetry: 0,
      monitor_control: 0,
      operational_audit: 0,
      mcp_idempotency: 0,
      checkpoint: 0,
      canonical_report_input: 0,
      durable_transition: 0,
    };
    const empty: RetentionCycleResult = {
      skipped: true, skipReason: null, deletedByClass, failures: 0, durationMs: 0,
      batchCount: 0, lastBatchSize: 0, lastBatchDurationMs: 0, transientEligible: false,
      monitorOnlyEligible: false, durableTransitionsPruned: false, eligibleBacklog: 0,
    };
    if (!this.config.enabled) return { ...empty, skipReason: "disabled" };
    if (!context.databaseUp) return { ...empty, skipReason: "database_down" };
    if (context.failClosed) return { ...empty, skipReason: "fail_closed" };
    if (context.degraded) return { ...empty, skipReason: "degraded" };
    if (this.running) return { ...empty, skipReason: "single_flight" };

    this.running = true;
    this.runs += 1;
    this.metrics?.noteRun?.(false);
    let failures = 0;
    let batchCount = 0;
    let lastBatchSize = 0;
    let lastBatchDurationMs = 0;
    let totalDeleted = 0;
    let canonicalBacklog = 0;

    try {
      const transientEligible = await completedReportUnlocksTransient(
        this.journal, context.experiment, context.now, this.config.completedGraceMs,
      );
      const monitorOnlyEligible = experimentIsMonitorOnly(context.experiment);
      const telemetryBefore = new Date(context.now.getTime() - this.config.telemetryTtlMs);
      const monitorBefore = monitorCutoff({
        now: context.now,
        monitorTtlMs: this.config.monitorTtlMs,
        experiment: context.experiment,
        monitorOnlyEligible,
        transientEligible,
      });
      const operationalBefore = new Date(context.now.getTime() - this.config.monitorTtlMs);

      const telemetryEligible = await this.journal.countEligible([...TELEMETRY_EVENT_TYPES], telemetryBefore);
      const monitorEligible = monitorBefore
        ? await this.journal.countEligible([...MONITOR_CONTROL_EVENT_TYPES], monitorBefore)
        : { count: 0, oldestOccurredAt: null };
      const operationalEligible = await this.journal.countEligible(
        [...LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES],
        operationalBefore,
      );

      type Drain = {
        cls: string;
        exhausted: boolean;
        work: (limit: number) => Promise<{ deleted: number }>;
      };
      const journalDrains: Drain[] = [];

      // Canonical experiment detail is allowed to leave hot Postgres only after:
      // canonical final report + stable verified rollup + grace + checksum-verified
      // local raw archive. Historical experiments are considered, not only the
      // engine's currently attached manifest.
      if (this.archive) {
        const candidates = await this.journal.listArchiveCandidates(32);
        for (const candidate of candidates) {
          if (context.now.getTime() - Date.parse(candidate.report.persistedAt) < this.config.completedGraceMs) continue;
          if (!candidate.rollup.sourceFenceOccurredAt || !candidate.rollup.sourceCommitSeq || !candidate.rollup.sourceRecordedAt) continue;
          const backlog = await this.journal.countCanonicalEventsThrough({
            experimentId: candidate.report.experimentId,
            occurredBefore: new Date(candidate.rollup.sourceFenceOccurredAt),
            sourceCommitSeq: candidate.rollup.sourceCommitSeq,
            sourceRecordedAt: new Date(candidate.rollup.sourceRecordedAt),
          });
          if (backlog.count <= 0) continue;
          canonicalBacklog = backlog.count;
          const archived = await this.archive.ensureExperimentCanonical(this.journal, candidate);
          if (!archived.verified || !archived.manifest) throw new Error("LOCAL_ARCHIVE_CANONICAL_VERIFY_FAILED");
          const manifest = archived.manifest;
          journalDrains.push({
            cls: "canonical_report_input",
            exhausted: false,
            work: (limit) => this.journal.deleteCanonicalEventsThrough!({
              experimentId: candidate.report.experimentId,
              occurredBefore: new Date(manifest.sourceFenceOccurredAt),
              sourceCursor: candidate.rollup.sourceCursor,
              sourceCommitSeq: manifest.sourceCommitSeq,
              sourceRecordedAt: new Date(manifest.sourceRecordedAt),
              limit,
            }),
          });
          break;
        }

        const prepareBucket = async (
          cls: string,
          types: readonly string[],
          eligible: { count: number; oldestOccurredAt: string | null },
          cutoff: Date | null,
        ): Promise<void> => {
          if (eligible.count <= 0) return;
          const bucket = fullArchiveBucket(eligible.oldestOccurredAt, cutoff, this.config.localArchiveBucketMs);
          if (!bucket) return;
          const commitSeqTo = await this.journal.latestCommitSeq();
          const recordedAtTo = context.now;
          const archived = await this.archive!.ensureJournalBucket(this.journal, {
            retentionClass: cls,
            eventTypes: types,
            occurredFrom: bucket.from,
            occurredBefore: bucket.before,
            commitSeqTo,
            recordedAtTo,
          });
          if (!archived.verified || !archived.manifest) throw new Error(`LOCAL_ARCHIVE_${cls.toUpperCase()}_VERIFY_FAILED`);
          const manifest = archived.manifest;
          journalDrains.push({
            cls,
            exhausted: false,
            work: (limit) => this.journal.deleteJournalEventsArchivedRange({
              types,
              occurredFrom: new Date(manifest.occurredFrom),
              occurredBefore: new Date(manifest.occurredBefore),
              commitSeqTo: manifest.commitSeqTo,
              recordedAtTo: new Date(manifest.recordedAtTo),
              limit,
            }),
          });
        };

        await prepareBucket("raw_telemetry", TELEMETRY_EVENT_TYPES, telemetryEligible, telemetryBefore);
        await prepareBucket("monitor_control", MONITOR_CONTROL_EVENT_TYPES, monitorEligible, monitorBefore);
        await prepareBucket("operational_audit", LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES, operationalEligible, operationalBefore);
      }

      // Archive work is read/IO work. Start the destructive DML budget only
      // after archive verification so a large export cannot consume the purge
      // budget and leave a half-completed contract.
      const budgetEnd = Date.now() + this.config.cycleBudgetMs;
      const remainingRows = (): number => this.config.maxRowsPerCycle - totalDeleted;
      const runBatch = async (cls: string, work: (limit: number) => Promise<{ deleted: number }>): Promise<number> => {
        const remaining = remainingRows();
        if (Date.now() >= budgetEnd) return 0;
        if (batchCount >= this.config.maxBatchesPerCycle) return 0;
        if (remaining <= 0) return 0;
        const limit = Math.min(this.config.batchSize, remaining);
        const t0 = Date.now();
        const result = await work(limit);
        lastBatchSize = result.deleted;
        lastBatchDurationMs = Date.now() - t0;
        if (result.deleted > 0) {
          batchCount += 1;
          totalDeleted += result.deleted;
          deletedByClass[cls] = (deletedByClass[cls] ?? 0) + result.deleted;
          this.metrics?.noteDeleted?.(cls, result.deleted);
          this.metrics?.noteBatch?.(result.deleted, lastBatchDurationMs);
        }
        return result.deleted;
      };

      const drains: Drain[] = [
        ...journalDrains,
        {
          cls: "mcp_idempotency",
          exhausted: false,
          work: (limit) => this.journal.pruneIdempotencyKeys(
            new Date(context.now.getTime() - this.config.mcpIdempotencyTtlMs),
            limit,
          ),
        },
        {
          cls: "checkpoint",
          exhausted: false,
          work: (limit) => this.journal.pruneOldCheckpoints(this.config.checkpointKeep, limit),
        },
      ];

      let progressed = true;
      while (
        progressed
        && Date.now() < budgetEnd
        && batchCount < this.config.maxBatchesPerCycle
        && totalDeleted < this.config.maxRowsPerCycle
      ) {
        progressed = false;
        for (const drain of drains) {
          if (drain.exhausted) continue;
          if (Date.now() >= budgetEnd || batchCount >= this.config.maxBatchesPerCycle || totalDeleted >= this.config.maxRowsPerCycle) break;
          const requested = Math.min(this.config.batchSize, Math.max(0, remainingRows()));
          if (requested <= 0) break;
          const deleted = await runBatch(drain.cls, drain.work);
          if (deleted < requested) drain.exhausted = true;
          if (deleted > 0) progressed = true;
        }
        if (drains.every((item) => item.exhausted)) break;
      }

      // Rolling analytics can still be built for active experiments, but raw
      // canonical detail is not deleted from an active window. Only a sealed
      // final archive is authoritative for hot-DB purge.
      if (
        this.config.analyticsRollupEnabled
        && (context.experiment?.status === "RUNNING" || context.experiment?.status === "ENTRY_CLOSED")
        && this.journal.persistAnalyticsRollup
        && context.experiment.startedAt
        && Date.now() < budgetEnd
      ) {
        const scanAbort = new AbortController();
        const scanTimer = setTimeout(() => scanAbort.abort(), Math.max(0, budgetEnd - Date.now()));
        let streamed: Awaited<ReturnType<typeof streamExperimentSummary>>;
        try {
          streamed = await streamExperimentSummary(
            this.journal,
            { id: context.experiment.id, startedAt: context.experiment.startedAt },
            {
              status: context.experiment.status === "RUNNING" ? "RUNNING" : "ENTRY_CLOSED",
              reportKind: "interim",
              metricsScope: "rolling",
              complete: false,
            },
            { signal: scanAbort.signal },
          );
        } finally {
          clearTimeout(scanTimer);
        }
        if (streamed.scan.fence.stable && streamed.scan.complete && !streamed.scan.truncated && !streamed.scan.interrupted && streamed.accumulatorSnapshot) {
          await persistVerifiedAnalyticsRollup(this.journal, {
            experimentId: context.experiment.id,
            sourceCursor: await this.journal.latestDurableSequence(),
            sourceFenceId: streamed.scan.fence.through?.id ?? null,
            sourceFenceOccurredAt: streamed.scan.fence.through?.occurredAt ?? null,
            sourceEventCount: streamed.accumulatorSnapshot.eventCount,
            sourceCommitSeq: streamed.scan.fence.commitSeqTo,
            sourceRecordedAt: streamed.scan.fence.recordedAtTo.toISOString(),
            stableRead: streamed.scan.fence.stable,
            sourceCoverageComplete: streamed.scan.complete && !streamed.scan.truncated && !streamed.scan.interrupted,
            baseContentHash: streamed.baseRollupContentHash,
            accumulator: streamed.accumulatorSnapshot as unknown as Record<string, unknown>,
          });
        }
      }

      let durableCompaction: RetentionCycleResult["durableCompaction"];
      if (
        this.config.compactionEnabled
        && this.journal.compactDurableTransitionsThrough
        && this.journal.listVerifiedCheckpoints
      ) {
        const verified = await this.journal.listVerifiedCheckpoints();
        const compactionCheckpoint = verified[1];
        const cursorValue = compactionCheckpoint?.payload.durableCursor;
        const cursor = typeof cursorValue === "number" && Number.isInteger(cursorValue) ? cursorValue : null;
        if (cursor != null && cursor > 0) {
          const compactionStarted = Date.now();
          durableCompaction = await this.journal.compactDurableTransitionsThrough(cursor, {
            ttlMs: this.config.compactionReplayKeyTtlMs,
            maxReplayKeys: this.config.compactionReplayKeyLimit,
            maxRows: this.config.maxRowsPerCycle - totalDeleted,
            maxBatches: this.config.maxBatchesPerCycle - batchCount,
            batchSize: this.config.batchSize,
            budgetMs: Math.max(0, budgetEnd - Date.now()),
          });
          if (durableCompaction.deleted > 0) {
            batchCount += 1;
            totalDeleted += durableCompaction.deleted;
            deletedByClass.durable_transition = (deletedByClass.durable_transition ?? 0) + durableCompaction.deleted;
            lastBatchSize = durableCompaction.deleted;
            lastBatchDurationMs = Date.now() - compactionStarted;
            this.metrics?.noteDeleted?.("durable_transition", durableCompaction.deleted);
            this.metrics?.noteBatch?.(durableCompaction.deleted, lastBatchDurationMs);
          }
        }
      }

      this.lastSuccessAt.value = context.now.toISOString();
      this.metrics?.noteLastSuccess?.(this.lastSuccessAt.value);
      return {
        skipped: false,
        skipReason: null,
        deletedByClass,
        failures,
        durationMs: Date.now() - started,
        batchCount,
        lastBatchSize,
        lastBatchDurationMs,
        transientEligible,
        monitorOnlyEligible,
        durableTransitionsPruned: Boolean(durableCompaction?.deleted),
        durableCompaction,
        eligibleBacklog: telemetryEligible.count + monitorEligible.count + operationalEligible.count + canonicalBacklog,
      };
    } catch (error) {
      failures += 1;
      this.failures += 1;
      this.metrics?.noteFailure?.();
      return {
        skipped: false,
        skipReason: error instanceof Error ? error.message : String(error),
        deletedByClass,
        failures,
        durationMs: Date.now() - started,
        batchCount,
        lastBatchSize,
        lastBatchDurationMs,
        transientEligible: false,
        monitorOnlyEligible: false,
        durableTransitionsPruned: false,
        eligibleBacklog: 0,
      };
    } finally {
      this.running = false;
    }
  }
}

export const PROTECTED_FROM_CLEANUP = {
  durableTransitions: "Only behind verified recovery snapshots; compacted operation-key authority is retained",
  auditCritical: AUDIT_CRITICAL_EVENT_TYPES,
} as const;
