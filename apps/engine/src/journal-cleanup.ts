import {
  AUDIT_CRITICAL_EVENT_TYPES,
  MONITOR_CONTROL_EVENT_TYPES,
  TELEMETRY_EVENT_TYPES,
  clampRetentionBatchSize,
} from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import {
  completedReportUnlocksTransient,
  defaultRetentionConfig,
  experimentIsActive,
  experimentIsMonitorOnly,
  monitorCutoff,
  type RetentionConfig,
} from "./retention-manager.js";

export interface CleanupClassEstimate {
  totalRows: number;
  eligible: number;
  cutoff: string | null;
  oldestEligibleAt: string | null;
  estimatedBatches: number;
}

export interface CleanupDryRun {
  mode: "dry-run";
  mutated: false;
  journalCounts: Record<string, number>;
  oldestOccurredAt: string | null;
  newestOccurredAt: string | null;
  activeExperimentProtected: boolean;
  monitorOnlyEligible: boolean;
  finalReportPresent: boolean;
  finalReportCanonical: boolean;
  completedGraceGate: boolean;
  protectedCriticalRows: number;
  durableTransitions: "PROTECTED_NO_DELETE";
  telemetry: CleanupClassEstimate;
  monitor: CleanupClassEstimate;
  telemetryCandidates: number;
  monitorCandidates: number;
  estimatedBoundedBatches: number;
  refuseUnsafeExperimentCleanup: boolean;
  effectiveConfig: {
    enabled: boolean;
    telemetryTtlMs: number;
    monitorTtlMs: number;
    completedGraceMs: number;
    batchSize: number;
    maxBatchesPerCycle: number;
    maxRowsPerCycle: number;
    cycleBudgetMs: number;
  };
  /** Exact global bounds APPLY will use. Not a per-class budget and not a rename of effectiveConfig. */
  applyBounds: CleanupApplyBounds;
  notes: string[];
}

export interface CleanupApplyResult {
  mode: "apply";
  deletedByClass: Record<string, number>;
  refused: boolean;
  reason: string | null;
  durableTransitionsDeleted: 0;
  batches: number;
  applyBounds: CleanupApplyBounds;
}

/** Global APPLY budget. Distinct from automatic `effectiveConfig` even when the numbers match. */
export interface CleanupApplyBounds {
  maxBatches: number;
  maxRows: number;
  cycleBudgetMs: number;
  batchSize: number;
}

export interface CleanupApplyOverrides {
  maxBatches?: number;
  maxRows?: number;
  cycleBudgetMs?: number;
  batchSize?: number;
}

export function resolveApplyBounds(config: RetentionConfig, overrides: CleanupApplyOverrides = {}): CleanupApplyBounds {
  return {
    maxBatches: Math.max(1, overrides.maxBatches ?? config.maxBatchesPerCycle),
    maxRows: Math.max(0, overrides.maxRows ?? config.maxRowsPerCycle),
    cycleBudgetMs: Math.max(0, overrides.cycleBudgetMs ?? config.cycleBudgetMs),
    batchSize: clampRetentionBatchSize(overrides.batchSize ?? config.batchSize),
  };
}

function estimate(total: number, eligible: { count: number; oldestOccurredAt: string | null }, cutoff: Date | null, batchSize: number): CleanupClassEstimate {
  return {
    totalRows: total,
    eligible: eligible.count,
    cutoff: cutoff?.toISOString() ?? null,
    oldestEligibleAt: eligible.oldestOccurredAt,
    estimatedBatches: Math.ceil(eligible.count / Math.max(1, batchSize)),
  };
}

export async function dryRunJournalCleanup(
  journal: EventJournal,
  now = new Date(),
  experiment?: { id: string; status: string; startedAt?: string | null } | null,
  config: RetentionConfig = defaultRetentionConfig(),
  applyOverrides: CleanupApplyOverrides = {},
): Promise<CleanupDryRun> {
  const counts = await journal.countByType();
  const oldest = await journal.queryEventsKeyset({ limit: 1 });
  const newest = await journal.peekLatestKeyset({});
  const telemetryTotal = sumTypes(counts, TELEMETRY_EVENT_TYPES);
  const monitorTotal = sumTypes(counts, MONITOR_CONTROL_EVENT_TYPES);
  const protectedCriticalRows = sumTypes(counts, AUDIT_CRITICAL_EVENT_TYPES);
  const report = experiment?.id ? await journal.loadExperimentReport(experiment.id, "final") : null;
  const active = experimentIsActive(experiment ?? null);
  const monitorOnlyEligible = experimentIsMonitorOnly(experiment ?? null);
  const unlocked = experiment
    ? await completedReportUnlocksTransient(journal, experiment, now, config.completedGraceMs)
    : false;
  const telemetryCutoff = new Date(now.getTime() - config.telemetryTtlMs);
  const monitorBefore = monitorCutoff({
    now,
    monitorTtlMs: config.monitorTtlMs,
    experiment: experiment ?? null,
    monitorOnlyEligible,
    transientEligible: unlocked,
  });
  const telemetryEligible = await journal.countEligible([...TELEMETRY_EVENT_TYPES], telemetryCutoff);
  const monitorEligible = monitorBefore
    ? await journal.countEligible([...MONITOR_CONTROL_EVENT_TYPES], monitorBefore)
    : { count: 0, oldestOccurredAt: null };
  const telemetry = estimate(telemetryTotal, telemetryEligible, telemetryCutoff, config.batchSize);
  const monitor = estimate(monitorTotal, monitorEligible, monitorBefore, config.batchSize);
  const notes = [
    "Dry run: zero mutation. No migrate, no DDL, no DML.",
    "Deletes do not shrink PostgreSQL relation size immediately (no VACUUM FULL).",
    "durable_transitions is never deleted.",
    "No automatic startup purge. RETENTION_ENABLED defaults false until operator review.",
  ];
  if (active) notes.push("Active experiment: in-window report-input / monitor rows are protected; pre-start monitor older than TTL may be eligible.");
  if (monitorOnlyEligible) notes.push("Monitor-only / no experiment: monitor rows older than monitor TTL are eligible.");
  if (experiment?.status === "COMPLETED" && !unlocked) notes.push("Completed experiment lacks a canonical final report + grace; in-window monitor cleanup refused.");
  const applyBounds = resolveApplyBounds(config, applyOverrides);
  notes.push(
    `APPLY global bounds (one budget for telemetry+monitor): maxBatches=${applyBounds.maxBatches}, maxRows=${applyBounds.maxRows}, cycleBudgetMs=${applyBounds.cycleBudgetMs}, batchSize=${applyBounds.batchSize}.`,
  );
  return {
    mode: "dry-run",
    mutated: false,
    journalCounts: counts,
    oldestOccurredAt: oldest[0]?.occurredAt ?? null,
    newestOccurredAt: newest?.occurredAt ?? null,
    activeExperimentProtected: active,
    monitorOnlyEligible,
    finalReportPresent: Boolean(report),
    finalReportCanonical: Boolean(report?.complete && !report.truncated),
    completedGraceGate: unlocked,
    protectedCriticalRows,
    durableTransitions: "PROTECTED_NO_DELETE",
    telemetry,
    monitor,
    telemetryCandidates: telemetry.eligible,
    monitorCandidates: monitor.eligible,
    estimatedBoundedBatches: Math.min(
      telemetry.estimatedBatches + monitor.estimatedBatches,
      applyBounds.maxBatches,
    ),
    refuseUnsafeExperimentCleanup: active && !experiment?.startedAt,
    effectiveConfig: {
      enabled: config.enabled,
      telemetryTtlMs: config.telemetryTtlMs,
      monitorTtlMs: config.monitorTtlMs,
      completedGraceMs: config.completedGraceMs,
      batchSize: config.batchSize,
      maxBatchesPerCycle: config.maxBatchesPerCycle,
      maxRowsPerCycle: config.maxRowsPerCycle,
      cycleBudgetMs: config.cycleBudgetMs,
    },
    applyBounds,
    notes,
  };
}

export async function applyJournalCleanup(
  journal: EventJournal,
  input: {
    now?: Date;
    experiment?: { id: string; status: string; startedAt?: string | null } | null;
    batchSize?: number;
    confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP";
    maxBatches?: number;
    maxRows?: number;
    cycleBudgetMs?: number;
    config?: RetentionConfig;
  },
): Promise<CleanupApplyResult> {
  if (input.confirm !== "I_UNDERSTAND_DESTRUCTIVE_CLEANUP") {
    return {
      mode: "apply", deletedByClass: {}, refused: true, reason: "CONFIRMATION_REQUIRED",
      durableTransitionsDeleted: 0, batches: 0,
      applyBounds: {
        maxBatches: 0, maxRows: 0, cycleBudgetMs: 0, batchSize: 0,
      },
    };
  }
  const now = input.now ?? new Date();
  const config = input.config ?? defaultRetentionConfig();
  const applyOverrides: CleanupApplyOverrides = {
    maxBatches: input.maxBatches,
    maxRows: input.maxRows,
    cycleBudgetMs: input.cycleBudgetMs,
    batchSize: input.batchSize,
  };
  const dry = await dryRunJournalCleanup(journal, now, input.experiment, config, applyOverrides);
  const applyBounds = dry.applyBounds;
  const batchSize = applyBounds.batchSize;
  const maxBatches = applyBounds.maxBatches;
  const maxRows = applyBounds.maxRows;
  const budgetEnd = Date.now() + applyBounds.cycleBudgetMs;
  const deletedByClass: Record<string, number> = { raw_telemetry: 0, monitor_control: 0 };
  let batches = 0;
  let totalDeleted = 0;
  const monitorBefore = dry.monitor.cutoff ? new Date(dry.monitor.cutoff) : null;
  const classes: Array<{ cls: "raw_telemetry" | "monitor_control"; exhausted: boolean; work: (limit: number) => Promise<{ deleted: number }> }> = [
    {
      cls: "raw_telemetry",
      exhausted: false,
      work: (limit) => journal.deleteJournalEventsBounded({
        types: TELEMETRY_EVENT_TYPES,
        occurredBefore: new Date(now.getTime() - config.telemetryTtlMs),
        limit,
      }),
    },
  ];
  if (monitorBefore && dry.monitor.eligible > 0) {
    classes.push({
      cls: "monitor_control",
      exhausted: false,
      work: (limit) => journal.deleteJournalEventsBounded({
        types: MONITOR_CONTROL_EVENT_TYPES,
        occurredBefore: monitorBefore,
        limit,
      }),
    });
  }
  let progressed = true;
  while (
    progressed
    && Date.now() < budgetEnd
    && batches < maxBatches
    && totalDeleted < maxRows
  ) {
    progressed = false;
    for (const drain of classes) {
      if (drain.exhausted) continue;
      if (Date.now() >= budgetEnd || batches >= maxBatches || totalDeleted >= maxRows) break;
      const remaining = maxRows - totalDeleted;
      if (remaining <= 0) break;
      const limit = Math.min(batchSize, remaining);
      const result = await drain.work(limit);
      if (result.deleted > 0) {
        batches += 1;
        totalDeleted += result.deleted;
        deletedByClass[drain.cls] = (deletedByClass[drain.cls] ?? 0) + result.deleted;
        progressed = true;
      }
      if (result.deleted < limit) drain.exhausted = true;
    }
    if (classes.every((item) => item.exhausted)) break;
  }
  return {
    mode: "apply",
    deletedByClass,
    refused: false,
    reason: null,
    durableTransitionsDeleted: 0,
    batches,
    applyBounds,
  };
}

function sumTypes(counts: Record<string, number>, types: readonly string[]): number {
  return types.reduce((sum, type) => sum + (counts[type] ?? 0), 0);
}

export function cleanupAuditEvent(result: CleanupDryRun | CleanupApplyResult) {
  return createJournalEvent({
    traceId: "operator", type: "SYSTEM_HEALTH_EVENT", aggregateType: "operator", aggregateId: "journal-cleanup",
    payload: { component: "journal-cleanup", status: result.mode === "dry-run" ? "DRY_RUN" : "APPLY", result },
  });
}
