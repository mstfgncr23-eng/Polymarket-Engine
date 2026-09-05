import {
  AUDIT_CRITICAL_EVENT_TYPES,
  MONITOR_CONTROL_EVENT_TYPES,
  RETENTION_DEFAULTS,
  TELEMETRY_EVENT_TYPES,
  assertMcpIdempotencyTtlSafe,
  clampRetentionBatchSize,
  engineEnvSchema,
  type EngineEnv,
} from "@pm/core";
import { isCanonicalFinalReport, type EventJournal } from "@pm/db";

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
  durableTransitionsPruned: false;
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
  };
  assertMcpIdempotencyTtlSafe(config.mcpIdempotencyTtlMs);
  return config;
}

export function experimentIsActive(experiment: { status?: string | null } | null): boolean {
  return experiment?.status === "RUNNING" || experiment?.status === "ENTRY_CLOSED";
}

export function experimentIsMonitorOnly(experiment: RetentionExperiment): boolean {
  return experiment == null
    || experiment.status === "IDLE"
    || experiment.status === "MONITOR_ONLY"
    || experiment.status === "UNKNOWN"
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

export class RetentionManager {
  private running = false;
  private readonly lastSuccessAt = { value: null as string | null };
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
  ) {
    assertMcpIdempotencyTtlSafe(config.mcpIdempotencyTtlMs);
  }

  get inFlight(): boolean { return this.running; }

  async runCycle(context: RetentionContext): Promise<RetentionCycleResult> {
    const started = Date.now();
    const deletedByClass: Record<string, number> = {
      raw_telemetry: 0, monitor_control: 0, mcp_idempotency: 0, checkpoint: 0,
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
    try {
      const transientEligible = await completedReportUnlocksTransient(
        this.journal, context.experiment, context.now, this.config.completedGraceMs,
      );
      const monitorOnlyEligible = experimentIsMonitorOnly(context.experiment);
      const budgetEnd = started + this.config.cycleBudgetMs;
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
        // Zero-delete probes exhaust that class; they must not consume the
        // useful batch/row budget or an empty class starves a real backlog.
        if (result.deleted > 0) {
          batchCount += 1;
          totalDeleted += result.deleted;
          deletedByClass[cls] = (deletedByClass[cls] ?? 0) + result.deleted;
          this.metrics?.noteDeleted?.(cls, result.deleted);
          this.metrics?.noteBatch?.(result.deleted, lastBatchDurationMs);
        }
        return result.deleted;
      };

      const telemetryBefore = new Date(context.now.getTime() - this.config.telemetryTtlMs);
      const monitorBefore = monitorCutoff({
        now: context.now,
        monitorTtlMs: this.config.monitorTtlMs,
        experiment: context.experiment,
        monitorOnlyEligible,
        transientEligible,
      });
      const telemetryEligible = await this.journal.countEligible([...TELEMETRY_EVENT_TYPES], telemetryBefore);
      const monitorEligibleCount = monitorBefore
        ? await this.journal.countEligible([...MONITOR_CONTROL_EVENT_TYPES], monitorBefore)
        : { count: 0, oldestOccurredAt: null };

      const drains: Array<{ cls: string; exhausted: boolean; work: (limit: number) => Promise<{ deleted: number }> }> = [
        {
          cls: "raw_telemetry",
          exhausted: false,
          work: (limit) => this.journal.deleteJournalEventsBounded({
            types: TELEMETRY_EVENT_TYPES,
            occurredBefore: telemetryBefore,
            limit,
          }),
        },
      ];
      if (monitorBefore) {
        drains.push({
          cls: "monitor_control",
          exhausted: false,
          work: (limit) => this.journal.deleteJournalEventsBounded({
            types: MONITOR_CONTROL_EVENT_TYPES,
            occurredBefore: monitorBefore,
            limit,
          }),
        });
      }
      drains.push({
        cls: "mcp_idempotency",
        exhausted: false,
        work: (limit) => this.journal.pruneIdempotencyKeys(
          new Date(context.now.getTime() - this.config.mcpIdempotencyTtlMs),
          limit,
        ),
      });
      drains.push({
        cls: "checkpoint",
        exhausted: false,
        work: () => this.journal.pruneOldCheckpoints(this.config.checkpointKeep),
      });

      let progressed = true;
      while (progressed && Date.now() < budgetEnd && batchCount < this.config.maxBatchesPerCycle && totalDeleted < this.config.maxRowsPerCycle) {
        progressed = false;
        for (const drain of drains) {
          if (drain.exhausted) continue;
          if (Date.now() >= budgetEnd || batchCount >= this.config.maxBatchesPerCycle || totalDeleted >= this.config.maxRowsPerCycle) break;
          const remaining = this.config.maxRowsPerCycle - totalDeleted;
          const requested = Math.min(this.config.batchSize, Math.max(0, remaining));
          const deleted = await runBatch(drain.cls, drain.work);
          if (requested <= 0 || deleted < requested) drain.exhausted = true;
          if (deleted > 0) progressed = true;
        }
        if (drains.every((item) => item.exhausted)) break;
      }

      this.lastSuccessAt.value = context.now.toISOString();
      this.metrics?.noteLastSuccess?.(this.lastSuccessAt.value);
      return {
        skipped: false, skipReason: null, deletedByClass, failures, durationMs: Date.now() - started,
        batchCount, lastBatchSize, lastBatchDurationMs, transientEligible, monitorOnlyEligible,
        durableTransitionsPruned: false,
        eligibleBacklog: telemetryEligible.count + monitorEligibleCount.count,
      };
    } catch (error) {
      failures += 1;
      this.failures += 1;
      this.metrics?.noteFailure?.();
      return {
        skipped: false, skipReason: error instanceof Error ? error.message : String(error),
        deletedByClass, failures, durationMs: Date.now() - started,
        batchCount, lastBatchSize, lastBatchDurationMs, transientEligible: false,
        monitorOnlyEligible: false, durableTransitionsPruned: false, eligibleBacklog: 0,
      };
    } finally {
      this.running = false;
    }
  }
}

export const PROTECTED_FROM_CLEANUP = {
  durableTransitions: "NEVER — recovery-authoritative; no DELETE in Step-4",
  auditCritical: AUDIT_CRITICAL_EVENT_TYPES,
} as const;
