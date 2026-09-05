import { persistCanonicalFinalReport, resolveAuthoritativeExperiment, streamExperimentSummary } from "./final-report.js";
import { loadSourceSeal } from "./source-seal.js";
import { loadUnresolvedReportSourceIntegrity } from "./report-source-integrity.js";
import { writeExperimentSummary } from "./report.js";
import type { EventJournal } from "@pm/db";

export async function generateOperatorReport(
  journal: EventJournal,
  input: {
    experimentId?: string;
    reportDirectory: string;
    allowPersist: boolean;
  },
): Promise<{
  persisted: boolean;
  reason: string | null;
  complete: boolean;
  pages: number;
  scanned: number;
  experimentId: string | null;
  status: string | null;
  paths: { jsonPath: string; markdownPath: string };
}> {
  const experimentId = input.experimentId ?? "";
  const authoritative = experimentId ? await resolveAuthoritativeExperiment(journal, experimentId) : null;
  if (!authoritative) {
    const streamed = await streamExperimentSummary(journal, { id: experimentId || "unknown" }, {
      reportKind: "interim", metricsScope: "complete", complete: false, truncated: true, truncatedReason: "EXPERIMENT_IDENTITY_MISSING",
    });
    streamed.summary.complete = false;
    streamed.summary.truncated = true;
    const paths = await writeExperimentSummary(streamed.summary, input.reportDirectory);
    return {
      persisted: false, reason: "EXPERIMENT_IDENTITY_MISSING", complete: false,
      pages: streamed.scan.pages, scanned: streamed.scan.scanned, experimentId: experimentId || null, status: null, paths,
    };
  }
  const extras = {
    status: authoritative.status ?? "unknown",
    reportKind: authoritative.provenCompleted ? "final" as const : "interim" as const,
    metricsScope: "complete" as const,
    complete: authoritative.provenCompleted,
    truncated: false,
  };
  const streamed = await streamExperimentSummary(journal, {
    id: authoritative.id,
    startedAt: authoritative.startedAt,
    completedAt: authoritative.completedAt,
  }, extras);
  streamed.summary.complete = Boolean(authoritative.provenCompleted && streamed.scan.complete && streamed.scan.fence.stable && !streamed.scan.interrupted);
  streamed.summary.truncated = streamed.scan.truncated || streamed.scan.interrupted || !streamed.scan.fence.stable || !authoritative.provenCompleted;
  const sealed = Boolean(await loadSourceSeal(journal, authoritative.id));
  const integrityFaults = await loadUnresolvedReportSourceIntegrity(journal, authoritative.id);
  if (integrityFaults.length > 0) {
    streamed.summary.complete = false;
  }
  if (!authoritative.provenCompleted || !input.allowPersist || !streamed.summary.complete || !sealed || integrityFaults.length > 0) {
    const paths = await writeExperimentSummary(streamed.summary, input.reportDirectory);
    return {
      persisted: false,
      reason: !authoritative.provenCompleted
        ? `NOT_COMPLETED:${authoritative.status}`
        : !sealed
          ? "SOURCE_SEAL_MISSING"
          : integrityFaults.length > 0
            ? "REPORT_SOURCE_INTEGRITY_UNRESOLVED"
          : streamed.summary.truncated ? streamed.summary.truncatedReason : "PERSIST_NOT_REQUESTED",
      complete: streamed.summary.complete,
      pages: streamed.scan.pages,
      scanned: streamed.scan.scanned,
      experimentId: authoritative.id,
      status: authoritative.status,
      paths,
    };
  }
  const persisted = await persistCanonicalFinalReport(journal, {
    summary: streamed.summary,
    scan: streamed.scan,
    experimentStatus: "COMPLETED",
  });
  const paths = await writeExperimentSummary(streamed.summary, input.reportDirectory);
  return {
    persisted: Boolean(persisted),
    reason: persisted ? null : "CANONICAL_PERSIST_REFUSED",
    complete: streamed.summary.complete,
    pages: streamed.scan.pages,
    scanned: streamed.scan.scanned,
    experimentId: authoritative.id,
    status: authoritative.status,
    paths,
  };
}
