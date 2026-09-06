import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { canonicalJson, type EventJournal } from "@pm/db";
import { renderExperimentSummaryMarkdown, type ExperimentSummary } from "@pm/core";
import { summaryFromDurableReport, streamExperimentSummary } from "./final-report.js";

export interface ExperimentArchiveManifest {
  archiveSchemaVersion: 1;
  experimentId: string;
  reportSchemaVersion: number;
  contentHash: string;
  canonicalReportContentHash: string;
  archiveBundleHash: string;
  generatedAt: string;
  persistedAt: string;
  sourceEventCount: number;
  sourceFenceId: string | null;
  sourceFenceOccurredAt: string | null;
}

/** Human-only deterministic export. The engine never reads this archive. */
export async function exportExperimentArchive(
  journal: EventJournal,
  input: { experimentId: string; directory: string; manifest?: Record<string, unknown> },
): Promise<{ directory: string; manifestPath: string; summaryPath: string; reportPath: string; contentHash: string }> {
  const report = await journal.loadExperimentReport(input.experimentId, "final");
  let summary: ExperimentSummary;
  let sourceEventCount = 0;
  let sourceFenceId: string | null = null;
  let sourceFenceOccurredAt: string | null = null;
  let reportSchemaVersion = 1;
  if (report) {
    summary = summaryFromDurableReport(report);
    sourceEventCount = report.sourceEventCount; sourceFenceId = report.sourceFenceId; sourceFenceOccurredAt = report.sourceFenceOccurredAt; reportSchemaVersion = report.schemaVersion;
  } else {
    const streamed = await streamExperimentSummary(journal, { id: input.experimentId }, { reportKind: "final", metricsScope: "complete", complete: false, truncated: true });
    summary = streamed.summary; sourceEventCount = streamed.scan.scanned; sourceFenceId = streamed.scan.fence.through?.id ?? null; sourceFenceOccurredAt = streamed.scan.fence.through?.occurredAt ?? null;
  }
  const rendered = renderExperimentSummaryMarkdown(summary);
  const contentHash = createHash("sha256").update(canonicalJson({ summary, rendered })).digest("hex");
  const directory = resolve(input.directory);
  await mkdir(directory, { recursive: true });
  const persistedAt = report?.persistedAt ?? new Date().toISOString();
  const baseManifest: Record<string, unknown> = {
    archiveSchemaVersion: 1, experimentId: input.experimentId, reportSchemaVersion, contentHash,
    canonicalReportContentHash: report?.contentHash ?? contentHash,
    generatedAt: summary.generatedAt, persistedAt, sourceEventCount, sourceFenceId, sourceFenceOccurredAt,
    experimentStatus: summary.status,
    strategyVersion: input.manifest?.strategyVersion ?? null,
    riskConfiguration: input.manifest?.riskConfiguration ?? null,
    modelConfiguration: input.manifest?.modelConfiguration ?? null,
    systemVersion: input.manifest?.systemVersion ?? null,
    experimentStart: input.manifest?.experimentStart ?? null,
    experimentEnd: input.manifest?.experimentEnd ?? null,
    ...(input.manifest ?? {}),
  };
  const archiveBundleHash = createHash("sha256").update(canonicalJson({ manifest: baseManifest, summary, rendered })).digest("hex");
  const manifest: ExperimentArchiveManifest & Record<string, unknown> = { ...baseManifest, archiveBundleHash } as ExperimentArchiveManifest & Record<string, unknown>;
  const manifestPath = resolve(directory, "manifest.json");
  const summaryPath = resolve(directory, "summary.json");
  const reportPath = resolve(directory, "report.md");
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  await writeFile(reportPath, rendered, "utf8");
  return { directory, manifestPath, summaryPath, reportPath, contentHash };
}
