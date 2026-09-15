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
  type DurableAccountingTransition,
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
  /** Content hash of the rollup actually restored before scanning the tail. */
  baseRollupContentHash: string | null;
  /** Durable high-water cursor folded into the accumulator. */
  durableCursor: number;
  durableCostScanComplete: boolean;
}

export const ANALYTICS_ROLLUP_SCHEMA_VERSION = 1;
export const ANALYTICS_ROLLUP_COVERAGE_VERSION = 1;

/**
 * Analytics rollups are persisted as Postgres jsonb. Hash the exact JSON
 * storage representation, not the richer in-memory JS object. In particular,
 * JSON storage drops object properties whose value is undefined and converts
 * non-finite numbers to null. Hashing before this round-trip made a freshly
 * written rollup unverifiable after restart.
 */
function jsonStorageRoundTrip(value: Record<string, unknown>): Record<string, unknown> {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("ANALYTICS_ROLLUP_NOT_JSON_SERIALIZABLE");
  const parsed = JSON.parse(encoded) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("ANALYTICS_ROLLUP_NOT_JSON_OBJECT");
  }
  return parsed as Record<string, unknown>;
}

export function hashAnalyticsRollup(accumulator: Record<string, unknown>): string {
  return createHash("sha256").update(canonicalJson(jsonStorageRoundTrip(accumulator))).digest("hex");
}

/**
 * Compatibility for schema-v1 rollups written before json-storage hashing.
 * The old accumulator serializer emitted `lastLedger: undefined` when no PNL
 * snapshot had been observed. canonicalJson hashed that key as the literal
 * text `undefined`, while jsonb correctly dropped it. Reconstruct only that
 * exact historical shape; every other mismatch remains fail-closed.
 */
function matchesLegacyV1AnalyticsHash(accumulator: Record<string, unknown>, expectedHash: string): boolean {
  if (Object.prototype.hasOwnProperty.call(accumulator, "lastLedger")) return false;
  const legacyShape = { ...accumulator, lastLedger: undefined };
  const legacyHash = createHash("sha256").update(canonicalJson(legacyShape)).digest("hex");
  return legacyHash === expectedHash;
}

function hashAnalyticsCoverage(input: { experimentId: string; schemaVersion: number; sourceEventCount: number; sourceCommitSeq: number; sourceFenceId: string | null; sourceFenceOccurredAt: string | null; sourceRecordedAt: string | null; stableRead: boolean; accumulatorHash: string }): string {
  return createHash("sha256").update(canonicalJson(input)).digest("hex");
}

/**
 * Only this explicit in-JSON marker can authorize deletion of canonical
 * detail. It intentionally lives inside the existing schema-v1 accumulator,
 * so old v1 rows remain readable while remaining insufficient for destructive
 * compaction.
 */
export function analyticsRollupHasDestructiveCoverage(rollup: { schemaVersion: number; verified: boolean; stableRead?: boolean; sourceEventCount?: number; coverageHash?: string; accumulator: Record<string, unknown> }): boolean {
  if (!rollup.verified || rollup.schemaVersion !== ANALYTICS_ROLLUP_SCHEMA_VERSION || rollup.stableRead === false) return false;
  // The coverage marker is only authoritative when the immutable metadata
  // fence that binds it to this exact rollup is present. This keeps old v1
  // rows readable while preventing a legacy row with a copied marker from
  // being treated as a complete prefix during report reconstruction.
  if (!rollup.coverageHash) return false;
  const coverage = rollup.accumulator.analyticsCoverage;
  if (!coverage || typeof coverage !== "object" || Array.isArray(coverage)) return false;
  const marker = coverage as { version?: unknown; economicCostComplete?: unknown; decisionAuditComplete?: unknown; decisionAuditOverflow?: unknown };
  const accumulatorEventCount = rollup.accumulator.eventCount;
  if (typeof rollup.sourceEventCount !== "number" || !Number.isInteger(rollup.sourceEventCount) || rollup.sourceEventCount < 0) return false;
  if (typeof accumulatorEventCount !== "number" || !Number.isInteger(accumulatorEventCount) || rollup.sourceEventCount !== accumulatorEventCount) return false;
  return marker.version === ANALYTICS_ROLLUP_COVERAGE_VERSION
    && marker.economicCostComplete === true
    && marker.decisionAuditComplete === true
    && marker.decisionAuditOverflow === false;
}

async function foldDurableOperatingCosts(
  journal: EventJournal,
  accumulator: ExperimentSummaryAccumulator,
  experimentId: string,
  afterSequence: number,
  throughSequence: number,
  pageSize: number,
  signal?: AbortSignal,
): Promise<{ complete: boolean; cursor: number }> {
  let after = afterSequence;
  let experimentStartObserved = afterSequence > 0;
  let preExperimentGap = false;
  let preExperimentTargetEvidence = false;
  for (;;) {
    if (signal?.aborted) return { complete: false, cursor: after };
    const page = await journal.queryDurableTransitions({
      afterSequence: after,
      limit: Math.max(1, Math.min(pageSize, 10_000)),
    });
    if (page.length === 0) return { complete: after >= throughSequence, cursor: after };
    let advanced = false;
    for (const transition of page) {
      if (transition.sequence > throughSequence) {
        const completeThrough = after >= throughSequence;
        const completeCoverage = experimentStartObserved ? !preExperimentTargetEvidence : !preExperimentGap;
        return { complete: completeThrough && completeCoverage, cursor: after };
      }
      if (!experimentStartObserved) {
        if (transition.sequence !== after + 1) preExperimentGap = true;
        if (durableTransitionStartsExperiment(transition, experimentId)) {
          // Compacted transitions from earlier experiments may leave a global
          // sequence hole. That hole is irrelevant once this experiment's
          // durable start is observed; from that marker onward continuity is
          // mandatory for exact cost reconstruction.
          experimentStartObserved = true;
          after = transition.sequence - 1;
        } else {
          const payload = transition.payload;
          const transitionExperimentId = typeof payload.experimentId === "string" ? payload.experimentId : transition.aggregateId;
          if (transitionExperimentId === experimentId && transition.type === "INFRASTRUCTURE_COST_ACCRUED") {
            preExperimentTargetEvidence = true;
            accumulator.foldDurableTransition(transition as DurableAccountingTransition);
          }
          after = Math.max(after, transition.sequence);
          advanced = true;
          continue;
        }
      }
      if (transition.sequence !== after + 1) return { complete: false, cursor: after };
      if (transition.sequence > throughSequence) return { complete: after >= throughSequence, cursor: after };
      after = Math.max(after, transition.sequence);
      advanced = true;
      const payload = transition.payload;
      const transitionExperimentId = typeof payload.experimentId === "string" ? payload.experimentId : transition.aggregateId;
      if (transitionExperimentId === experimentId && transition.type === "INFRASTRUCTURE_COST_ACCRUED") {
        accumulator.foldDurableTransition(transition as DurableAccountingTransition);
      }
    }
    const complete = experimentStartObserved ? !preExperimentTargetEvidence : !preExperimentGap;
    if (!advanced || after >= throughSequence) return { complete, cursor: after };
    if (page.length < Math.max(1, Math.min(pageSize, 10_000))) return { complete, cursor: after };
  }
}

function durableTransitionStartsExperiment(transition: { type: string; aggregateId: string; payload: Record<string, unknown> }, experimentId: string): boolean {
  if (transition.type !== "EXPERIMENT_STARTED") return false;
  if (transition.aggregateId === experimentId || transition.payload.experimentId === experimentId) return true;
  const experiment = transition.payload.experiment;
  return Boolean(experiment && typeof experiment === "object" && !Array.isArray(experiment)
    && (experiment as { id?: unknown }).id === experimentId);
}

export async function persistVerifiedAnalyticsRollup(
  journal: EventJournal,
  input: { experimentId: string; sourceCursor: number; sourceFenceId?: string | null; sourceFenceOccurredAt?: string | null; sourceEventCount?: number; sourceCommitSeq?: number | null; sourceRecordedAt?: string | null; stableRead?: boolean; sourceCoverageComplete?: boolean; previousContentHash?: string | null; baseContentHash?: string | null; accumulator: Record<string, unknown>; rollupKind?: string },
): Promise<import("@pm/db").AnalyticsRollup | null> {
  if (!journal.persistAnalyticsRollup) return null;
  if (input.stableRead === false) return null;
  if (input.sourceCoverageComplete === false) return null;
  const accumulatorEventCount = typeof input.accumulator.eventCount === "number" && Number.isInteger(input.accumulator.eventCount)
    ? input.accumulator.eventCount : 0;
  const coveredCount = input.sourceEventCount ?? accumulatorEventCount;
  const sourceCommitSeq = input.sourceCommitSeq ?? await journal.latestCommitSeq();
  const sourceRecordedAt = input.sourceRecordedAt ?? new Date().toISOString();
  if (coveredCount > 0 && (!input.sourceFenceId || !input.sourceFenceOccurredAt)) return null;
  if (input.sourceFenceId && journal.readJournalEventById) {
    const fenceEvent = await journal.readJournalEventById(input.sourceFenceId);
    if (!fenceEvent || !CANONICAL_REPORT_INPUT_EVENT_TYPES.includes(fenceEvent.type as (typeof CANONICAL_REPORT_INPUT_EVENT_TYPES)[number])) return null;
    const fencePayload = fenceEvent.payload as Record<string, unknown>;
    const nestedExperiment = fencePayload.experiment;
    const fenceExperimentId = typeof fencePayload.experimentId === "string"
      ? fencePayload.experimentId
      : nestedExperiment && typeof nestedExperiment === "object" && !Array.isArray(nestedExperiment) && typeof (nestedExperiment as { id?: unknown }).id === "string"
        ? (nestedExperiment as { id: string }).id
        : fenceEvent.aggregateType === "experiment" ? fenceEvent.aggregateId : null;
    if (fenceExperimentId !== input.experimentId) return null;
    if (input.sourceFenceOccurredAt && fenceEvent.occurredAt !== input.sourceFenceOccurredAt) return null;
  }
  const existing = journal.loadAnalyticsRollup ? await journal.loadAnalyticsRollup(input.experimentId, input.rollupKind ?? "canonical") : null;
  const storageAccumulator = jsonStorageRoundTrip(input.accumulator);
  const contentHash = hashAnalyticsRollup(storageAccumulator);
  const rollup = {
    experimentId: input.experimentId, rollupKind: input.rollupKind ?? "canonical", schemaVersion: ANALYTICS_ROLLUP_SCHEMA_VERSION,
    sourceCursor: input.sourceCursor, sourceFenceId: input.sourceFenceId ?? null, sourceFenceOccurredAt: input.sourceFenceOccurredAt ?? null,
    sourceEventCount: coveredCount, sourceCommitSeq, sourceRecordedAt,
    stableRead: input.stableRead ?? true,
    previousContentHash: input.baseContentHash !== undefined ? input.baseContentHash : (input.previousContentHash !== undefined ? input.previousContentHash : existing?.contentHash ?? null),
    accumulator: storageAccumulator, contentHash, verified: true, createdAt: new Date().toISOString(),
  };
  (rollup as { coverageHash?: string }).coverageHash = hashAnalyticsCoverage({ experimentId: rollup.experimentId, schemaVersion: rollup.schemaVersion, sourceEventCount: rollup.sourceEventCount, sourceCommitSeq: rollup.sourceCommitSeq, sourceFenceId: rollup.sourceFenceId, sourceFenceOccurredAt: rollup.sourceFenceOccurredAt, sourceRecordedAt: rollup.sourceRecordedAt, stableRead: rollup.stableRead, accumulatorHash: contentHash });
  return journal.persistAnalyticsRollup(rollup);
}

export async function loadVerifiedAnalyticsRollup(journal: EventJournal, experimentId: string, rollupKind = "canonical"): Promise<import("@pm/db").AnalyticsRollup | null> {
  if (!journal.loadAnalyticsRollup) return null;
  const row = await journal.loadAnalyticsRollup(experimentId, rollupKind);
  if (!row || !row.verified || row.schemaVersion !== ANALYTICS_ROLLUP_SCHEMA_VERSION || row.stableRead === false) return null;
  const storageHashMatches = hashAnalyticsRollup(row.accumulator) === row.contentHash;
  const legacyV1HashMatches = !storageHashMatches && matchesLegacyV1AnalyticsHash(row.accumulator, row.contentHash);
  if (!storageHashMatches && !legacyV1HashMatches) return null;
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
  let acc = new ExperimentSummaryAccumulator(experiment.id, extras);
  let rollupRestored = rollup == null;
  const rollupSnapshot = rollup?.accumulator as unknown as ExperimentSummaryAccumulatorSnapshot | undefined;
  if (rollupSnapshot) {
    try {
      acc.restore({ ...rollupSnapshot, extras });
      rollupRestored = true;
    } catch {
      // A hash-valid but locally incompatible accumulator cannot be used as a
      // prefix. Rebuild the available detail from zero, but keep the result
      // non-destructive until a complete replacement rollup is proven.
      acc = new ExperimentSummaryAccumulator(experiment.id, extras);
      rollupRestored = false;
    }
  }
  const usableRollup = rollupRestored ? rollup : null;
  const legacyPrefixProjectionMissing = Boolean(rollup && !rollupRestored
    || usableRollup && !analyticsRollupHasDestructiveCoverage(usableRollup)
      && ((usableRollup.sourceEventCount ?? 0) > 0 || usableRollup.sourceCursor > 0));
  if (legacyPrefixProjectionMissing) {
    acc.setOperatingCostSourceCursor(usableRollup?.sourceCursor ?? 0, false);
    acc.setDecisionAuditCoverage(false);
  }
  const durableHighWater = await journal.latestDurableSequence();
  const durableStart = usableRollup?.sourceCursor ?? 0;
  const durableScan = durableHighWater >= durableStart
    ? await foldDurableOperatingCosts(journal, acc, experiment.id, durableStart, durableHighWater, options.pageSize ?? 500, options.signal)
    : { complete: false, cursor: durableStart };
  const durableCursor = durableScan.cursor;
  const durableCostScanComplete = durableScan.complete;
  acc.setOperatingCostSourceCursor(durableCursor, durableCostScanComplete && !legacyPrefixProjectionMissing);
  let tailScanned = 0;
  const scan = await scanJournalEvents(journal, {
    types: CANONICAL_REPORT_INPUT_EVENT_TYPES,
    from: experiment.startedAt ? new Date(experiment.startedAt) : undefined,
    to: experiment.completedAt ? new Date(experiment.completedAt) : undefined,
    // Commit sequence is the authoritative prefix boundary. Do not use the
    // prior rollup's occurredAt cursor: a newly committed event may carry an
    // older occurredAt and still belongs to the uncovered tail.
    ...(usableRollup ? { commitSeqAfter: usableRollup.sourceCommitSeq ?? 0 } : {}),
  }, {
    pageSize: options.pageSize,
    ceiling: options.ceiling,
    signal: options.signal,
    onPage: async (page) => {
      for (const event of page) {
        if (!includeCurrentExperimentEvent(event, experiment)) continue;
        // The query already excludes the verified rollup prefix by commit_seq.
        // Folding the returned page is therefore O(pages), with no per-event
        // commit-sequence lookup or DB fanout.
        acc.fold(event);
        tailScanned += 1;
      }
    },
  });
  if (!scan.complete || scan.truncated || scan.interrupted) acc.setDecisionAuditCoverage(false);
  const summary = acc.finalize();
  summary.complete = Boolean(extras.complete ?? scan.complete) && !scan.truncated && !scan.interrupted && !summary.truncated;
  summary.truncated = scan.truncated || scan.interrupted || summary.truncated;
  summary.truncatedReason = summary.truncated ? (extras.truncatedReason ?? scan.truncatedReason ?? "SCAN_CEILING") : null;
  if (scan.interrupted) summary.complete = false;
  const baseThrough = usableRollup && usableRollup.sourceFenceId && usableRollup.sourceFenceOccurredAt
    ? { id: usableRollup.sourceFenceId, occurredAt: usableRollup.sourceFenceOccurredAt }
    : null;
  const effectiveThrough = baseThrough && scan.fence.through
    ? (Date.parse(scan.fence.through.occurredAt) > Date.parse(baseThrough.occurredAt)
      || (scan.fence.through.occurredAt === baseThrough.occurredAt && scan.fence.through.id > baseThrough.id) ? scan.fence.through : baseThrough)
    : baseThrough ?? scan.fence.through;
  const effectiveFence = usableRollup && effectiveThrough
    ? { ...scan.fence, through: effectiveThrough }
    : scan.fence;
  const effectiveScan = { ...scan, fence: effectiveFence, scanned: (usableRollup?.sourceEventCount ?? 0) + tailScanned };
  return {
    summary,
    scan: effectiveScan,
    accumulatorHighWaterBytes: acc.highWaterMarkBytes,
    accumulatorSnapshot: acc.serialize(),
    baseRollupContentHash: usableRollup?.contentHash ?? null,
    durableCursor,
    durableCostScanComplete,
  };
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
