import {
  AUDIT_CRITICAL_EVENT_TYPES,
  MONITOR_CONTROL_EVENT_TYPES,
  RETENTION_DEFAULTS,
  TELEMETRY_EVENT_TYPES,
  clampRetentionBatchSize,
} from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import { completedReportUnlocksTransient, experimentIsActive } from "./retention-manager.js";

export interface CleanupDryRun {
  mode: "dry-run";
  mutated: false;
  journalCounts: Record<string, number>;
  oldestOccurredAt: string | null;
  newestOccurredAt: string | null;
  activeExperimentProtected: boolean;
  finalReportPresent: boolean;
  finalReportCanonical: boolean;
  protectedCriticalRows: number;
  durableTransitions: "PROTECTED_NO_DELETE";
  telemetryCandidates: number;
  monitorCandidates: number;
  refuseUnsafeExperimentCleanup: boolean;
  notes: string[];
}

export interface CleanupApplyResult {
  mode: "apply";
  deletedByClass: Record<string, number>;
  refused: boolean;
  reason: string | null;
  durableTransitionsDeleted: 0;
  batches: number;
}

export async function dryRunJournalCleanup(
  journal: EventJournal,
  now = new Date(),
  experiment?: { id: string; status: string } | null,
): Promise<CleanupDryRun> {
  const counts = await journal.countByType();
  const oldest = await journal.queryEventsKeyset({ limit: 1 });
  const newest = await journal.peekLatestKeyset({});
  const telemetryCandidates = sumTypes(counts, TELEMETRY_EVENT_TYPES);
  const monitorCandidates = sumTypes(counts, MONITOR_CONTROL_EVENT_TYPES);
  const protectedCriticalRows = sumTypes(counts, AUDIT_CRITICAL_EVENT_TYPES);
  const report = experiment?.id ? await journal.loadExperimentReport(experiment.id, "final") : null;
  const active = experimentIsActive(experiment ?? null);
  const unlocked = experiment
    ? await completedReportUnlocksTransient(journal, experiment, now, RETENTION_DEFAULTS.completedGraceMs)
    : true;
  const notes = [
    "Dry run: zero mutation.",
    "Deletes do not shrink PostgreSQL relation size immediately (no VACUUM FULL).",
    "durable_transitions is never deleted.",
    "No automatic startup purge.",
  ];
  if (active) notes.push("Active experiment: report-input / monitor rows are protected.");
  if (experiment?.status === "COMPLETED" && !unlocked) notes.push("Completed experiment lacks a canonical final report + grace; monitor cleanup refused.");
  return {
    mode: "dry-run",
    mutated: false,
    journalCounts: counts,
    oldestOccurredAt: oldest[0]?.occurredAt ?? null,
    newestOccurredAt: newest?.occurredAt ?? null,
    activeExperimentProtected: active,
    finalReportPresent: Boolean(report),
    finalReportCanonical: Boolean(report?.complete && !report.truncated),
    protectedCriticalRows,
    durableTransitions: "PROTECTED_NO_DELETE",
    telemetryCandidates,
    monitorCandidates,
    refuseUnsafeExperimentCleanup: active || (Boolean(experiment) && !unlocked),
    notes,
  };
}

export async function applyJournalCleanup(
  journal: EventJournal,
  input: {
    now?: Date;
    experiment?: { id: string; status: string } | null;
    batchSize?: number;
    confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP";
    maxBatches?: number;
  },
): Promise<CleanupApplyResult> {
  if (input.confirm !== "I_UNDERSTAND_DESTRUCTIVE_CLEANUP") {
    return { mode: "apply", deletedByClass: {}, refused: true, reason: "CONFIRMATION_REQUIRED", durableTransitionsDeleted: 0, batches: 0 };
  }
  const now = input.now ?? new Date();
  const dry = await dryRunJournalCleanup(journal, now, input.experiment);
  if (dry.refuseUnsafeExperimentCleanup) {
    return {
      mode: "apply", deletedByClass: {}, refused: true,
      reason: dry.activeExperimentProtected ? "ACTIVE_EXPERIMENT_PROTECTED" : "CANONICAL_FINAL_REPORT_REQUIRED",
      durableTransitionsDeleted: 0, batches: 0,
    };
  }
  const limit = clampRetentionBatchSize(input.batchSize ?? RETENTION_DEFAULTS.batchSize);
  const maxBatches = Math.max(1, input.maxBatches ?? 20);
  const deletedByClass: Record<string, number> = { raw_telemetry: 0, monitor_control: 0 };
  let batches = 0;
  for (let i = 0; i < maxBatches; i += 1) {
    const tel = await journal.deleteJournalEventsBounded({
      types: TELEMETRY_EVENT_TYPES,
      occurredBefore: new Date(now.getTime() - RETENTION_DEFAULTS.telemetryTtlMs),
      limit,
    });
    deletedByClass.raw_telemetry = (deletedByClass.raw_telemetry ?? 0) + tel.deleted;
    batches += 1;
    if (tel.deleted === 0) break;
  }
  if (!dry.activeExperimentProtected && dry.finalReportCanonical) {
    for (let i = 0; i < maxBatches; i += 1) {
      const mon = await journal.deleteJournalEventsBounded({
        types: MONITOR_CONTROL_EVENT_TYPES,
        occurredBefore: new Date(now.getTime() - RETENTION_DEFAULTS.monitorTtlMs),
        limit,
      });
      deletedByClass.monitor_control = (deletedByClass.monitor_control ?? 0) + mon.deleted;
      batches += 1;
      if (mon.deleted === 0) break;
    }
  }
  return { mode: "apply", deletedByClass, refused: false, reason: null, durableTransitionsDeleted: 0, batches };
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
