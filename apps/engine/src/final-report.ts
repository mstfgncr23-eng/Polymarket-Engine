import {
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  EXPERIMENT_REPORT_SCHEMA_VERSION,
  renderExperimentSummaryMarkdown,
  type ExperimentSummary,
  type JournalEvent,
} from "@pm/core";
import {
  createJournalEvent,
  canonicalJson,
  hashExperimentReport,
  isCanonicalFinalReport,
  isTransientJournalError,
  reportGeneratedAuditId,
  scanJournalEvents,
  type DurableExperimentReport,
  type DurableExperimentReportInput,
  type EventJournal,
  type PersistReportResult,
  type ScanStats,
} from "@pm/db";
import {
  ExperimentSummaryAccumulator,
  includeCurrentExperimentEvent,
  type ExperimentSummaryAccumulatorSnapshot,
  type ExperimentSummaryExtras,
} from "./report.js";
import { loadSourceSeal } from "./source-seal.js";
import { loadUnresolvedReportSourceIntegrity } from "./report-source-integrity.js";
import { createHash } from "node:crypto";

export interface StreamedExperimentSummary {
  summary: ExperimentSummary;
  scan: ScanStats;
  accumulatorHighWaterBytes: number;
  accumulatorSnapshot?: ExperimentSummaryAccumulatorSnapshot;
}

export const ANALYTICS_ROLLUP_SCHEMA_VERSION = 1;

export function hashAnalyticsRollup(accumulator: Record<string, unknown>): string {
  return createHash("sha256").update(canonicalJson(accumulator)).digest("hex");
}

function hashAnalyticsCoverage(input: { experimentId: string; schemaVersion: number; sourceEventCount: number; sourceCommitSeq: number; sourceFenceId: string | null; sourceFenceOccurredAt: string | null; sourceRecordedAt: string | null; stableRead: boolean; accumulatorHash: string }): string {
  return createHash("sha256").update(canonicalJson(input)).digest("hex");
}

export async function persistVerifiedAnalyticsRollup(
  journal: EventJournal,
  input: { experimentId: string; sourceCursor: number; sourceFenceId?: string | null; sourceFenceOccurredAt?: string | null; sourceEventCount?: number; sourceCommitSeq?: number | null; sourceRecordedAt?: string | null; stableRead?: boolean; previousContentHash?: string | null; accumulator: Record<string, unknown>; rollupKind?: string },
): Promise<import("@pm/db").AnalyticsRollup | null> {
  if (!journal.persistAnalyticsRollup) return null;
  if (input.stableRead === false) return null;
  const coveredCount = input.sourceEventCount ?? 0;
  if (coveredCount > 0 && (!input.sourceFenceId || !input.sourceFenceOccurredAt)) return null;
  if (input.sourceFenceId && journal.readJournalEventById) {
    const fenceEvent = await journal.readJournalEventById(input.sourceFenceId);
    if (!fenceEvent || !CANONICAL_REPORT_INPUT_EVENT_TYPES.includes(fenceEvent.type as (typeof CANONICAL_REPORT_INPUT_EVENT_TYPES)[number])) return null;
    const fenceExperimentId = typeof fenceEvent.payload.experimentId === "string" ? fenceEvent.payload.experimentId : fenceEvent.aggregateId;
    if (fenceExperimentId !== input.experimentId) return null;
    if (input.sourceFenceOccurredAt && fenceEvent.occurredAt !== input.sourceFenceOccurredAt) return null;
  }
  const existing = journal.loadAnalyticsRollup ? await journal.loadAnalyticsRollup(input.experimentId, input.rollupKind ?? "canonical") : null;
  const contentHash = hashAnalyticsRollup(input.accumulator);
  const rollup = {
    experimentId: input.experimentId, rollupKind: input.rollupKind ?? "canonical", schemaVersion: ANALYTICS_ROLLUP_SCHEMA_VERSION,
    sourceCursor: input.sourceCursor, sourceFenceId: input.sourceFenceId ?? null, sourceFenceOccurredAt: input.sourceFenceOccurredAt ?? null,
    sourceEventCount: coveredCount, sourceCommitSeq: input.sourceCommitSeq ?? 0, sourceRecordedAt: input.sourceRecordedAt ?? null,
    stableRead: input.stableRead ?? true, previousContentHash: input.previousContentHash ?? existing?.contentHash ?? null,
    accumulator: input.accumulator, contentHash, verified: true, createdAt: new Date().toISOString(),
  };
  (rollup as { coverageHash?: string }).coverageHash = hashAnalyticsCoverage({ experimentId: rollup.experimentId, schemaVersion: rollup.schemaVersion, sourceEventCount: rollup.sourceEventCount, sourceCommitSeq: rollup.sourceCommitSeq, sourceFenceId: rollup.sourceFenceId, sourceFenceOccurredAt: rollup.sourceFenceOccurredAt, sourceRecordedAt: rollup.sourceRecordedAt, stableRead: rollup.stableRead, accumulatorHash: contentHash });
  return journal.persistAnalyticsRollup(rollup);
}

export async function loadVerifiedAnalyticsRollup(journal: EventJournal, experimentId: string, rollupKind = "canonical"): Promise<import("@pm/db").AnalyticsRollup | null> {
  if (!journal.loadAnalyticsRollup) return null;
  const row = await journal.loadAnalyticsRollup(experimentId, rollupKind);
  if (!row || !row.verified || row.schemaVersion !== ANALYTICS_ROLLUP_SCHEMA_VERSION || row.stableRead === false || hashAnalyticsRollup(row.accumulator) !== row.contentHash) return null;
  if (row.coverageHash && row.coverageHash !== hashAnalyticsCoverage({ experimentId: row.experimentId, schemaVersion: row.schemaVersion, sourceEventCount: row.sourceEventCount ?? 0, sourceCommitSeq: row.sourceCommitSeq ?? 0, sourceFenceId: row.sourceFenceId, sourceFenceOccurredAt: row.sourceFenceOccurredAt, sourceRecordedAt: row.sourceRecordedAt ?? null, stableRead: row.stableRead ?? false, accumulatorHash: row.contentHash })) return null;
  return row;
}

export async function streamExperimentSummary(
  journal: EventJournal,
  experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
  extras: ExperimentSummaryExtras,
  options: { signal?: AbortSignal; pageSize?: number; ceiling?: number } = {},
): Promise<StreamedExperimentSummary> {
  const rollup = await loadVerifiedAnalyticsRollup(journal, experiment.id);
  const acc = new ExperimentSummaryAccumulator(experiment.id, extras);
  const rollupSnapshot = rollup?.accumulator as unknown as ExperimentSummaryAccumulatorSnapshot | undefined;
  if (rollupSnapshot) {
    try { acc.restore({ ...rollupSnapshot, extras }); } catch { /* stale/incompatible rollups are ignored */ }
  }
  const rollupFenceAt = rollup?.sourceFenceOccurredAt ? Date.parse(rollup.sourceFenceOccurredAt) : Number.NaN;
  const rollupFenceId = rollup?.sourceFenceId ?? null;
  const rollupRecordedAt = rollup?.sourceRecordedAt ? Date.parse(rollup.sourceRecordedAt) : Number.NaN;
  let tailScanned = 0;
  const scan = await scanJournalEvents(journal, {
    types: CANONICAL_REPORT_INPUT_EVENT_TYPES,
    from: experiment.startedAt ? new Date(experiment.startedAt) : undefined,
    to: experiment.completedAt ? new Date(experiment.completedAt) : undefined,
  }, {
    pageSize: options.pageSize,
    ceiling: options.ceiling,
    signal: options.signal,
    onPage: async (page) => {
      for (const event of page) {
        if (!includeCurrentExperimentEvent(event, experiment)) continue;
        // A verified rollup already covers its source fence. Fold only the
        // bounded tail so a report remains equivalent after raw cleanup.
        const occurredAt = Date.parse(event.occurredAt);
        const occurredCovered = Number.isFinite(rollupFenceAt) && (occurredAt < rollupFenceAt
          || (occurredAt === rollupFenceAt && rollupFenceId != null && event.id <= rollupFenceId));
        const recordedCovered = Number.isFinite(rollupRecordedAt) ? Date.parse(event.recordedAt) <= rollupRecordedAt : true;
        const eventCommitSeq = journal.eventCommitSeq ? await journal.eventCommitSeq(event.id) : null;
        const committedCovered = rollup?.sourceCommitSeq != null && eventCommitSeq != null
          ? eventCommitSeq <= rollup.sourceCommitSeq
          : recordedCovered;
        if (occurredCovered && committedCovered) continue;
        acc.fold(event);
        tailScanned += 1;
      }
    },
  });
  const summary = acc.finalize();
  summary.complete = Boolean(extras.complete ?? scan.complete) && !scan.truncated && !scan.interrupted && !summary.truncated;
  summary.truncated = scan.truncated || scan.interrupted || summary.truncated;
  summary.truncatedReason = summary.truncated ? (extras.truncatedReason ?? scan.truncatedReason ?? "SCAN_CEILING") : null;
  if (scan.interrupted) summary.complete = false;
  const effectiveFence = rollup && !scan.fence.through && rollup.sourceFenceId && rollup.sourceFenceOccurredAt
    ? { ...scan.fence, through: { id: rollup.sourceFenceId, occurredAt: rollup.sourceFenceOccurredAt }, recordedAtTo: rollup.sourceRecordedAt ? new Date(rollup.sourceRecordedAt) : scan.fence.recordedAtTo, commitSeqTo: rollup.sourceCommitSeq ?? scan.fence.commitSeqTo }
    : scan.fence;
  const effectiveScan = { ...scan, fence: effectiveFence, scanned: (rollup?.sourceEventCount ?? 0) + tailScanned };
  return { summary, scan: effectiveScan, accumulatorHighWaterBytes: acc.highWaterMarkBytes, accumulatorSnapshot: acc.serialize() };
}

export function toDurableReportInput(input: {
  summary: ExperimentSummary;
  scan: ScanStats;
}): DurableExperimentReportInput {
  const summary = input.summary;
  const fence = input.scan.fence;
  const draft = {
    experimentId: summary.experimentId,
    reportKind: "final" as const,
    schemaVersion: EXPERIMENT_REPORT_SCHEMA_VERSION,
    generatedAt: summary.generatedAt,
    complete: summary.complete,
    truncated: summary.truncated,
    sourceEventCount: input.scan.scanned,
    sourceWindowStart: fence.windowStart?.toISOString() ?? null,
    sourceWindowEnd: fence.windowEnd?.toISOString() ?? null,
    sourceFenceOccurredAt: fence.through?.occurredAt ?? null,
    sourceFenceId: fence.through?.id ?? null,
    earliestIncludedAt: summary.earliestOccurredAt,
    latestIncludedAt: summary.latestOccurredAt,
    canonicalSummary: summary as unknown as Record<string, unknown>,
    renderedMarkdown: renderExperimentSummaryMarkdown(summary),
    contentHash: "",
  };
  return { ...draft, contentHash: hashExperimentReport(draft) };
}

export async function persistCanonicalFinalReport(
  journal: EventJournal,
  input: { summary: ExperimentSummary; scan: ScanStats; experimentStatus?: string | null },
): Promise<PersistReportResult | null> {
  if (input.experimentStatus !== "COMPLETED") return null;
  if (!input.scan.fence.stable) return null;
  if (input.scan.interrupted || !input.scan.complete) return null;
  if (!isCanonicalFinalReport({ reportKind: "final", complete: input.summary.complete, truncated: input.summary.truncated })) {
    return null;
  }
  if (!(await loadSourceSeal(journal, input.summary.experimentId))) return null;
  let unresolved: Awaited<ReturnType<typeof loadUnresolvedReportSourceIntegrity>>;
  try {
    unresolved = await loadUnresolvedReportSourceIntegrity(journal, input.summary.experimentId);
  } catch (error) {
    if (!isTransientJournalError(error)) return null;
    try {
      unresolved = await loadUnresolvedReportSourceIntegrity(journal, input.summary.experimentId);
    } catch {
      return null;
    }
  }
  if (unresolved.length > 0) return null;
  return journal.persistExperimentReport(toDurableReportInput(input));
}

export interface AuthoritativeExperiment {
  id: string;
  status: "RUNNING" | "ENTRY_CLOSED" | "COMPLETED" | null;
  startedAt: string | null;
  completedAt: string | null;
  provenCompleted: boolean;
}

export async function resolveAuthoritativeExperiment(
  journal: EventJournal,
  experimentId: string,
): Promise<AuthoritativeExperiment | null> {
  if (!experimentId || experimentId === "unknown" || experimentId === "no-persisted-memory-experiment") return null;
  const started = await journal.queryEventsKeyset({
    types: ["EXPERIMENT_STARTED"],
    experimentId,
    limit: 8,
  });
  const ownStart = started.find((event) => event.aggregateId === experimentId
    || (event.payload as { experimentId?: unknown }).experimentId === experimentId);
  const completed = await journal.queryEventsKeyset({
    types: ["EXPERIMENT_COMPLETED"],
    experimentId,
    limit: 8,
  });
  const ownComplete = completed.find((event) => event.aggregateId === experimentId
    || (event.payload as { experimentId?: unknown }).experimentId === experimentId);
  const entryClosed = await journal.queryEventsKeyset({
    types: ["EXPERIMENT_ENTRY_CLOSED"],
    experimentId,
    limit: 8,
  });
  const ownEntry = entryClosed.find((event) => event.aggregateId === experimentId
    || (event.payload as { experimentId?: unknown }).experimentId === experimentId);
  if (!ownStart && !ownComplete && !ownEntry) return null;
  const status = ownComplete ? "COMPLETED" : ownEntry ? "ENTRY_CLOSED" : "RUNNING";
  return {
    id: experimentId,
    status,
    startedAt: ownStart?.occurredAt ?? null,
    completedAt: ownComplete?.occurredAt ?? null,
    provenCompleted: status === "COMPLETED" && Boolean(ownStart) && Boolean(ownComplete),
  };
}

export async function ensureReportGeneratedAudit(
  journal: EventJournal,
  report: DurableExperimentReport,
  extras: Record<string, unknown> = {},
): Promise<{ repaired: boolean; id: string }> {
  const id = reportGeneratedAuditId(report.experimentId, report.contentHash);
  const existing = await journal.queryEventsKeyset({
    types: ["EXPERIMENT_REPORT_GENERATED"],
    experimentId: report.experimentId,
    limit: 50,
  });
  const found = existing.some((event) => event.id === id
    || (event.payload as { contentHash?: unknown }).contentHash === report.contentHash);
  if (found) return { repaired: false, id };
  await journal.append(createJournalEvent({
    id,
    traceId: report.experimentId,
    type: "EXPERIMENT_REPORT_GENERATED",
    aggregateType: "experiment",
    aggregateId: report.experimentId,
    payload: reportGeneratedPayload(report, extras),
  }));
  return { repaired: true, id };
}

export function summaryFromDurableReport(report: DurableExperimentReport): ExperimentSummary {
  return report.canonicalSummary as unknown as ExperimentSummary;
}

export function reportGeneratedPayload(report: DurableExperimentReport, extras: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    experimentId: report.experimentId,
    reportKind: report.reportKind,
    schemaVersion: report.schemaVersion,
    contentHash: report.contentHash,
    complete: report.complete,
    truncated: report.truncated,
    sourceEventCount: report.sourceEventCount,
    durable: true,
    ...extras,
  };
}

export function foldPage(acc: ExperimentSummaryAccumulator, page: readonly JournalEvent[], experiment: { id: string; startedAt?: string | null; completedAt?: string | null }): void {
  for (const event of page) {
    if (includeCurrentExperimentEvent(event, experiment)) acc.fold(event);
  }
}
