import { createHash } from "node:crypto";
import { canonicalJson } from "./durable.js";

export const EXPERIMENT_REPORT_KIND_FINAL = "final" as const;

export interface DurableExperimentReport {
  experimentId: string;
  reportKind: typeof EXPERIMENT_REPORT_KIND_FINAL;
  schemaVersion: number;
  generatedAt: string;
  complete: boolean;
  truncated: boolean;
  sourceEventCount: number;
  sourceWindowStart: string | null;
  sourceWindowEnd: string | null;
  sourceFenceOccurredAt: string | null;
  sourceFenceId: string | null;
  earliestIncludedAt: string | null;
  latestIncludedAt: string | null;
  canonicalSummary: Record<string, unknown>;
  renderedMarkdown: string;
  contentHash: string;
  persistedAt: string;
}

export type DurableExperimentReportInput = Omit<DurableExperimentReport, "persistedAt">;

export interface PersistReportResult {
  inserted: boolean;
  existing: boolean;
  report: DurableExperimentReport;
}

export class ExperimentReportConflictError extends Error {
  readonly code = "EXPERIMENT_REPORT_CONFLICT";
  constructor(
    readonly experimentId: string,
    readonly existingHash: string,
    readonly incomingHash: string,
  ) {
    super(`EXPERIMENT_REPORT_CONFLICT: experiment=${experimentId} existing=${existingHash} incoming=${incomingHash}`);
    this.name = "ExperimentReportConflictError";
  }
}

export function hashExperimentReport(input: {
  experimentId: string;
  reportKind: string;
  schemaVersion: number;
  complete: boolean;
  truncated: boolean;
  sourceEventCount: number;
  sourceWindowStart: string | null;
  sourceWindowEnd: string | null;
  sourceFenceOccurredAt: string | null;
  sourceFenceId: string | null;
  earliestIncludedAt: string | null;
  latestIncludedAt: string | null;
  canonicalSummary: Record<string, unknown>;
}): string {
  const { generatedAt: _generatedAt, ...summary } = input.canonicalSummary;
  return createHash("sha256").update(canonicalJson({
    experimentId: input.experimentId,
    reportKind: input.reportKind,
    schemaVersion: input.schemaVersion,
    complete: input.complete,
    truncated: input.truncated,
    sourceEventCount: input.sourceEventCount,
    sourceWindowStart: input.sourceWindowStart,
    sourceWindowEnd: input.sourceWindowEnd,
    sourceFenceOccurredAt: input.sourceFenceOccurredAt,
    sourceFenceId: input.sourceFenceId,
    earliestIncludedAt: input.earliestIncludedAt,
    latestIncludedAt: input.latestIncludedAt,
    canonicalSummary: summary,
  })).digest("hex");
}

export function isCanonicalFinalReport(report: Pick<DurableExperimentReport, "complete" | "truncated" | "reportKind">): boolean {
  return report.reportKind === "final" && report.complete === true && report.truncated === false;
}
