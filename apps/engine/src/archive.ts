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
    const historical = await deriveHistoricalManifest(journal, input.experimentId);
    const streamed = await streamExperimentSummary(journal, {
      id: input.experimentId,
      startedAt: typeof historical.experimentStart === "string" ? historical.experimentStart : null,
      completedAt: typeof historical.experimentEnd === "string" ? historical.experimentEnd : null,
    }, { reportKind: "interim", metricsScope: "rolling", complete: false, truncated: true });
    summary = streamed.summary; sourceEventCount = streamed.scan.scanned; sourceFenceId = streamed.scan.fence.through?.id ?? null; sourceFenceOccurredAt = streamed.scan.fence.through?.occurredAt ?? null;
  }
  const rendered = renderExperimentSummaryMarkdown(summary);
  const contentHash = createHash("sha256").update(canonicalJson({ summary, rendered })).digest("hex");
  const directory = resolve(input.directory);
  await mkdir(directory, { recursive: true });
  const persistedAt = report?.persistedAt ?? new Date().toISOString();
  // Archive callers normally have no manifest input. Recover the compact
  // historical configuration from durable lifecycle evidence/checkpoints when
  // available; the archive remains human-only and is never recovery authority.
  const automatic = await deriveHistoricalManifest(journal, input.experimentId);
  const baseManifest: Record<string, unknown> = {
    archiveSchemaVersion: 1, experimentId: input.experimentId, reportSchemaVersion, contentHash,
    canonicalReportContentHash: report?.contentHash ?? contentHash,
    generatedAt: summary.generatedAt, persistedAt, sourceEventCount, sourceFenceId, sourceFenceOccurredAt,
    strategyVersion: automatic.strategyVersion ?? input.manifest?.strategyVersion ?? null,
    riskConfiguration: automatic.riskConfiguration ?? input.manifest?.riskConfiguration ?? null,
    modelConfiguration: automatic.modelConfiguration ?? input.manifest?.modelConfiguration ?? null,
    systemVersion: automatic.systemVersion ?? input.manifest?.systemVersion ?? null,
    gitSha: automatic.gitSha ?? input.manifest?.gitSha ?? null,
    experimentStart: automatic.experimentStart ?? input.manifest?.experimentStart ?? null,
    experimentEnd: automatic.experimentEnd ?? input.manifest?.experimentEnd ?? null,
    experimentStatus: automatic.experimentStatus ?? summary.status,
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

async function deriveHistoricalManifest(journal: EventJournal, experimentId: string): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = {};
  try {
    const rows = await journal.queryDurableTransitions({
      types: ["EXPERIMENT_STARTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED", "EXPERIMENT_START_ABORTED"],
      limit: 10_000,
    });
    const own = rows.filter((row) => row.aggregateId === experimentId || row.payload.experimentId === experimentId);
    const start = own.find((row) => row.type === "EXPERIMENT_STARTED");
    const terminal = [...own].reverse().find((row) => row.type === "EXPERIMENT_COMPLETED" || row.type === "EXPERIMENT_START_ABORTED");
    const manifest = start?.payload.experiment && typeof start.payload.experiment === "object"
      ? start.payload.experiment as Record<string, unknown> : start?.payload;
    if (manifest && typeof manifest === "object") {
      for (const key of ["strategyVersion", "riskConfiguration", "modelConfiguration", "systemVersion", "gitSha", "startedAt", "completedAt", "status"] as const) {
        if (manifest[key] != null) result[key === "startedAt" ? "experimentStart" : key === "completedAt" ? "experimentEnd" : key === "status" ? "experimentStatus" : key] = manifest[key];
      }
    }
    if (terminal?.type === "EXPERIMENT_COMPLETED") result.experimentStatus = "COMPLETED";
    if (terminal?.type === "EXPERIMENT_START_ABORTED") result.experimentStatus = "ABORTED_START";
  } catch { /* read-only archive export remains best-effort */ }
  if (Object.keys(result).length === 0 && journal.listVerifiedCheckpoints) {
    try {
      const checkpoint = (await journal.listVerifiedCheckpoints()).find((row) => row.experimentId === experimentId || row.payload.experiment && typeof row.payload.experiment === "object" && (row.payload.experiment as { id?: unknown }).id === experimentId);
      const manifest = checkpoint?.payload.experiment;
      if (manifest && typeof manifest === "object") {
        const m = manifest as Record<string, unknown>;
        for (const key of ["strategyVersion", "riskConfiguration", "modelConfiguration", "systemVersion", "gitSha", "startedAt", "completedAt", "status"] as const) {
          if (m[key] != null) result[key === "startedAt" ? "experimentStart" : key === "completedAt" ? "experimentEnd" : key === "status" ? "experimentStatus" : key] = m[key];
        }
      }
    } catch { /* archive metadata is optional */ }
  }
  return result;
}
