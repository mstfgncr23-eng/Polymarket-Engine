import {
  DECISION_EVENT_TYPES,
  EXPERIMENT_REPORT_SCHEMA_VERSION,
  renderExperimentSummaryMarkdown,
  type ExperimentSummary,
  type JournalEvent,
} from "@pm/core";
import {
  createJournalEvent,
  hashExperimentReport,
  isCanonicalFinalReport,
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
  type ExperimentSummaryExtras,
} from "./report.js";
import { loadSourceSeal } from "./source-seal.js";
import { loadUnresolvedReportSourceIntegrity } from "./report-source-integrity.js";

export interface StreamedExperimentSummary {
  summary: ExperimentSummary;
  scan: ScanStats;
  accumulatorHighWaterBytes: number;
}

export async function streamExperimentSummary(
  journal: EventJournal,
  experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
  extras: ExperimentSummaryExtras,
  options: { signal?: AbortSignal; pageSize?: number; ceiling?: number } = {},
): Promise<StreamedExperimentSummary> {
  const acc = new ExperimentSummaryAccumulator(experiment.id, extras);
  const scan = await scanJournalEvents(journal, {
    types: DECISION_EVENT_TYPES,
    from: experiment.startedAt ? new Date(experiment.startedAt) : undefined,
    to: experiment.completedAt ? new Date(experiment.completedAt) : undefined,
  }, {
    pageSize: options.pageSize,
    ceiling: options.ceiling,
    signal: options.signal,
    onPage: (page) => {
      for (const event of page) {
        if (includeCurrentExperimentEvent(event, experiment)) acc.fold(event);
      }
    },
  });
  const summary = acc.finalize();
  summary.complete = Boolean(extras.complete ?? scan.complete) && !scan.truncated && !scan.interrupted && !summary.truncated;
  summary.truncated = scan.truncated || scan.interrupted || summary.truncated;
  summary.truncatedReason = summary.truncated ? (extras.truncatedReason ?? scan.truncatedReason ?? "SCAN_CEILING") : null;
  if (scan.interrupted) summary.complete = false;
  return { summary, scan, accumulatorHighWaterBytes: acc.highWaterMarkBytes };
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
  try {
    if ((await loadUnresolvedReportSourceIntegrity(journal, input.summary.experimentId)).length > 0) return null;
  } catch {
    return null;
  }
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
