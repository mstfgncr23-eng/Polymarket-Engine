import {
  AUDIT_CRITICAL_EVENT_TYPES,
  MONITOR_CONTROL_EVENT_TYPES,
  RETENTION_DEFAULTS,
  TELEMETRY_EVENT_TYPES,
  assertMcpIdempotencyTtlSafe,
  clampRetentionBatchSize,
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
}

export type RetentionExperiment = { id: string; status: string } | null;

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
  durableTransitionsPruned: false;
}

export function defaultRetentionConfig(overrides: Partial<RetentionConfig> = {}): RetentionConfig {
  const config: RetentionConfig = {
    enabled: overrides.enabled ?? true,
    telemetryTtlMs: overrides.telemetryTtlMs ?? RETENTION_DEFAULTS.telemetryTtlMs,
    monitorTtlMs: overrides.monitorTtlMs ?? RETENTION_DEFAULTS.monitorTtlMs,
    completedGraceMs: overrides.completedGraceMs ?? RETENTION_DEFAULTS.completedGraceMs,
    mcpIdempotencyTtlMs: overrides.mcpIdempotencyTtlMs ?? RETENTION_DEFAULTS.mcpIdempotencyTtlMs,
    checkpointKeep: overrides.checkpointKeep ?? RETENTION_DEFAULTS.checkpointKeep,
    batchSize: clampRetentionBatchSize(overrides.batchSize ?? RETENTION_DEFAULTS.batchSize),
    cycleBudgetMs: overrides.cycleBudgetMs ?? RETENTION_DEFAULTS.cycleBudgetMs,
  };
  assertMcpIdempotencyTtlSafe(config.mcpIdempotencyTtlMs);
  return config;
}

export function experimentIsActive(experiment: { status?: string | null } | null): boolean {
  return experiment?.status === "RUNNING" || experiment?.status === "ENTRY_CLOSED";
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
      durableTransitionsPruned: false,
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
    try {
      const transientEligible = await completedReportUnlocksTransient(
        this.journal, context.experiment, context.now, this.config.completedGraceMs,
      );
      const budgetEnd = started + this.config.cycleBudgetMs;
      const runBatch = async (cls: string, work: () => Promise<{ deleted: number }>) => {
        if (Date.now() >= budgetEnd) return;
        const t0 = Date.now();
        const result = await work();
        lastBatchSize = result.deleted;
        lastBatchDurationMs = Date.now() - t0;
        batchCount += 1;
        deletedByClass[cls] = (deletedByClass[cls] ?? 0) + result.deleted;
        this.metrics?.noteDeleted?.(cls, result.deleted);
        this.metrics?.noteBatch?.(result.deleted, lastBatchDurationMs);
      };

      await runBatch("raw_telemetry", () => this.journal.deleteJournalEventsBounded({
        types: TELEMETRY_EVENT_TYPES,
        occurredBefore: new Date(context.now.getTime() - this.config.telemetryTtlMs),
        limit: this.config.batchSize,
      }));

      if (!experimentIsActive(context.experiment) && transientEligible) {
        await runBatch("monitor_control", () => this.journal.deleteJournalEventsBounded({
          types: MONITOR_CONTROL_EVENT_TYPES,
          occurredBefore: new Date(context.now.getTime() - this.config.monitorTtlMs),
          limit: this.config.batchSize,
        }));
      }

      await runBatch("mcp_idempotency", () => this.journal.pruneIdempotencyKeys(
        new Date(context.now.getTime() - this.config.mcpIdempotencyTtlMs),
        this.config.batchSize,
      ));

      await runBatch("checkpoint", () => this.journal.pruneOldCheckpoints(this.config.checkpointKeep));

      this.lastSuccessAt.value = context.now.toISOString();
      this.metrics?.noteLastSuccess?.(this.lastSuccessAt.value);
      return {
        skipped: false, skipReason: null, deletedByClass, failures, durationMs: Date.now() - started,
        batchCount, lastBatchSize, lastBatchDurationMs, transientEligible, durableTransitionsPruned: false,
      };
    } catch (error) {
      failures += 1;
      this.failures += 1;
      this.metrics?.noteFailure?.();
      return {
        skipped: false, skipReason: error instanceof Error ? error.message : String(error),
        deletedByClass, failures, durationMs: Date.now() - started,
        batchCount, lastBatchSize, lastBatchDurationMs, transientEligible: false,
        durableTransitionsPruned: false,
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
