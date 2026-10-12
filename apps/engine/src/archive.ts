import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import { basename, dirname, resolve } from "node:path";
import { createHash } from "node:crypto";
import {
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  EVENT_PAGE_SIZE,
  type JournalEvent,
} from "@pm/core";
import {
  canonicalJson,
  hashDurablePayload,
  type EngineCheckpoint,
  isCanonicalFinalReport,
  type EventJournal,
  type JournalArchiveCandidate,
} from "@pm/db";
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

export interface VerifiedArchiveFile {
  file: string;
  sha256: string;
  bytes: number;
  eventCount?: number;
}

export interface ExperimentRawArchiveManifest {
  archiveSchemaVersion: 2;
  kind: "experiment_canonical";
  experimentId: string;
  createdAt: string;
  reportContentHash: string;
  rollupContentHash: string;
  sourceEventCount: number;
  sourceFenceId: string;
  sourceFenceOccurredAt: string;
  sourceCommitSeq: number;
  sourceRecordedAt: string;
  rawEvents: VerifiedArchiveFile & { eventCount: number };
  finalReport: VerifiedArchiveFile;
  canonicalRollup: VerifiedArchiveFile;
}

export interface JournalBucketArchiveManifest {
  archiveSchemaVersion: 1;
  kind: "journal_bucket";
  retentionClass: string;
  createdAt: string;
  occurredFrom: string;
  occurredBefore: string;
  commitSeqTo: number;
  recordedAtTo: string;
  unattributedOnly: boolean;
  eventTypes: string[];
  rawEvents: VerifiedArchiveFile & { eventCount: number };
}

export interface RecoveryArchiveManifest {
  archiveSchemaVersion: 1;
  kind: "engine_recovery";
  createdAt: string;
  replayFromCursor: number;
  throughCursor: number;
  prefixThroughCursor: number;
  checkpoints: VerifiedArchiveFile;
  durableTransitions: VerifiedArchiveFile & { eventCount: number };
}

export interface ArchiveVerification<T> {
  verified: boolean;
  manifest: T | null;
  directory: string;
}

function safePathComponent(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

function sha256(value: Buffer | string): string {
  return createHash("sha256").update(value).digest("hex");
}

async function writeAtomic(path: string, data: Buffer | string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temp = `${path}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(temp, data);
    await rename(temp, path);
  } catch (error) {
    await rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function fileHash(path: string): Promise<{ sha256: string; bytes: number; lines: number }> {
  const hash = createHash("sha256");
  let bytes = 0;
  let lines = 0;
  for await (const chunk of createReadStream(path)) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    hash.update(buffer);
    bytes += buffer.length;
    for (const byte of buffer) if (byte === 10) lines += 1;
  }
  return { sha256: hash.digest("hex"), bytes, lines };
}

async function verifyFile(root: string, file: VerifiedArchiveFile): Promise<boolean> {
  const path = resolve(root, file.file);
  try {
    const actual = await fileHash(path);
    return actual.sha256 === file.sha256
      && actual.bytes === file.bytes
      && (file.eventCount == null || actual.lines === file.eventCount);
  } catch {
    return false;
  }
}

async function writeJsonArchiveFile(root: string, filename: string, value: unknown): Promise<VerifiedArchiveFile> {
  const data = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");
  const path = resolve(root, filename);
  await writeAtomic(path, data);
  return { file: filename, sha256: sha256(data), bytes: data.length };
}

async function writeJournalEvents(
  journal: EventJournal,
  input: {
    path: string;
    query: Parameters<EventJournal["queryEventsKeyset"]>[0];
    include(event: JournalEvent): boolean;
  },
): Promise<VerifiedArchiveFile & { eventCount: number }> {
  await mkdir(dirname(input.path), { recursive: true });
  const temp = `${input.path}.tmp-${process.pid}-${Date.now()}`;
  const stream = createWriteStream(temp, { flags: "wx" });
  const hash = createHash("sha256");
  let bytes = 0;
  let eventCount = 0;
  let after = input.query.after;
  try {
    for (;;) {
      const page = await journal.queryEventsKeyset({ ...input.query, after, limit: EVENT_PAGE_SIZE });
      if (page.length === 0) break;
      for (const event of page) {
        after = { occurredAt: event.occurredAt, id: event.id };
        if (!input.include(event)) continue;
        const line = Buffer.from(`${JSON.stringify(event)}\n`, "utf8");
        hash.update(line);
        bytes += line.length;
        eventCount += 1;
        if (!stream.write(line)) await once(stream, "drain");
      }
      if (page.length < EVENT_PAGE_SIZE) break;
    }
    stream.end();
    await finished(stream);
    await rename(temp, input.path);
    return {
      file: basename(input.path),
      sha256: hash.digest("hex"),
      bytes,
      eventCount,
    };
  } catch (error) {
    stream.destroy();
    await rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}

function assertArchiveCandidate(candidate: JournalArchiveCandidate): void {
  const { report, rollup } = candidate;
  if (!isCanonicalFinalReport(report)) throw new Error("ARCHIVE_REQUIRES_CANONICAL_FINAL_REPORT");
  if (!rollup.verified || rollup.stableRead !== true) throw new Error("ARCHIVE_REQUIRES_VERIFIED_STABLE_ROLLUP");
  if (!rollup.sourceFenceId || !rollup.sourceFenceOccurredAt || !rollup.sourceCommitSeq || !rollup.sourceRecordedAt) {
    throw new Error("ARCHIVE_REQUIRES_COMPLETE_SOURCE_FENCE");
  }
  if (rollup.experimentId !== report.experimentId) throw new Error("ARCHIVE_EXPERIMENT_MISMATCH");
  if (report.sourceFenceId !== rollup.sourceFenceId || report.sourceFenceOccurredAt !== rollup.sourceFenceOccurredAt) {
    throw new Error("ARCHIVE_REPORT_ROLLUP_FENCE_MISMATCH");
  }
  if ((rollup.sourceEventCount ?? 0) !== report.sourceEventCount) {
    throw new Error("ARCHIVE_REPORT_ROLLUP_COUNT_MISMATCH");
  }
}

export const LOCAL_ARCHIVE_SENTINEL = ".polymarket-local-archive-v1";
export const LOCAL_ARCHIVE_SENTINEL_CONTENT = "POLYMARKET_LOCAL_ARCHIVE_V1";

export class LocalJournalArchive {
  constructor(readonly rootDir: string) {}

  async verifyDurableRoot(): Promise<void> {
    const root = resolve(this.rootDir);
    const info = await stat(root).catch(() => null);
    if (!info?.isDirectory()) throw new Error("LOCAL_ARCHIVE_ROOT_MISSING");
    const sentinelPath = resolve(root, LOCAL_ARCHIVE_SENTINEL);
    const sentinel = await readFile(sentinelPath, "utf8").catch(() => null);
    if (sentinel?.trim() !== LOCAL_ARCHIVE_SENTINEL_CONTENT) {
      throw new Error("LOCAL_ARCHIVE_SENTINEL_MISSING");
    }
  }

  experimentDirectory(experimentId: string): string {
    return resolve(this.rootDir, "experiments", safePathComponent(experimentId));
  }

  /** Back up exact checkpoint purge candidates, two usable recovery snapshots,
   * the bounded compaction prefix and their immutable replay tail before DML. */
  async ensureRecoveryBundle(journal: EventJournal, input: {
    retained: EngineCheckpoint[]; candidates: EngineCheckpoint[]; prefixCursor: number; maxPrefixRows: number;
  }): Promise<ArchiveVerification<RecoveryArchiveManifest>> {
    await this.verifyDurableRoot();
    if (input.retained.length < 2) throw new Error("RECOVERY_ARCHIVE_TWO_CHECKPOINTS_REQUIRED");
    const cursors = input.retained.map(c => Number(c.payload.durableCursor));
    if (cursors.some(c => !Number.isSafeInteger(c) || c < 0)) throw new Error("RECOVERY_ARCHIVE_CURSOR_INVALID");
    const replayFromCursor = Math.min(...cursors), throughCursor = await journal.latestDurableSequence();
    if (Math.max(...cursors) > throughCursor) throw new Error("RECOVERY_ARCHIVE_CHECKPOINT_AHEAD");
    const prefix = (await journal.queryDurableTransitions({ afterSequence: 0, limit: input.maxPrefixRows }))
      .filter(row => row.sequence <= Math.min(input.prefixCursor, replayFromCursor));
    const prefixThroughCursor = prefix.at(-1)?.sequence ?? 0;
    const snapshots = [...new Map([...input.retained, ...input.candidates].map(c => [c.id, c])).values()];
    const snapshotHash = sha256(canonicalJson(snapshots)).slice(0, 16);
    const directory = resolve(this.rootDir, "recovery", `c${replayFromCursor}_h${throughCursor}_p${prefixThroughCursor}_${snapshotHash}`);
    const existing = await readFile(resolve(directory, "manifest.json"), "utf8").catch(() => null);
    if (existing) {
      const manifest = JSON.parse(existing) as RecoveryArchiveManifest;
      if (manifest.kind !== "engine_recovery" || manifest.replayFromCursor !== replayFromCursor
        || manifest.throughCursor !== throughCursor || manifest.prefixThroughCursor !== prefixThroughCursor
        || !await verifyFile(directory, manifest.checkpoints) || !await verifyFile(directory, manifest.durableTransitions))
        throw new Error("RECOVERY_ARCHIVE_EXISTING_INVALID");
      return { directory, manifest, verified: true };
    }
    await mkdir(directory, { recursive: true });
    const checkpoints = await writeJsonArchiveFile(directory, "checkpoints.json", snapshots);
    const path = resolve(directory, "durable-transitions.jsonl"), temp = path + ".tmp";
    const stream = createWriteStream(temp), digest = createHash("sha256"); let bytes = 0, count = 0;
    const write = async (row: Awaited<ReturnType<EventJournal["queryDurableTransitions"]>>[number]) => {
      if (row.payloadHash !== hashDurablePayload(row.payload)) throw new Error("RECOVERY_ARCHIVE_DURABLE_HASH");
      const line = Buffer.from(JSON.stringify(row) + "\n"); digest.update(line); bytes += line.length; count++;
      if (!stream.write(line)) await once(stream, "drain");
    };
    try {
      for (const row of prefix) await write(row);
      let cursor = replayFromCursor;
      while (cursor < throughCursor) {
        const rows = await journal.readDurableTransitionsAfter(cursor, Math.min(EVENT_PAGE_SIZE, throughCursor - cursor));
        if (!rows.length) throw new Error("RECOVERY_ARCHIVE_REPLAY_GAP");
        for (const row of rows) {
          if (row.sequence > throughCursor) break;
          if (row.sequence !== cursor + 1) throw new Error("RECOVERY_ARCHIVE_REPLAY_GAP");
          await write(row); cursor = row.sequence;
        }
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      stream.end(); await finished(stream); await rename(temp, path);
    } catch (error) { stream.destroy(); await rm(temp, { force: true }); throw error; }
    const manifest: RecoveryArchiveManifest = { archiveSchemaVersion: 1, kind: "engine_recovery", createdAt: new Date().toISOString(),
      replayFromCursor, throughCursor, prefixThroughCursor, checkpoints,
      durableTransitions: { file: "durable-transitions.jsonl", bytes, sha256: digest.digest("hex"), eventCount: count } };
    await writeAtomic(resolve(directory, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
    if (!await verifyFile(directory, checkpoints) || !await verifyFile(directory, manifest.durableTransitions))
      throw new Error("RECOVERY_ARCHIVE_FILE_VERIFY");
    return { directory, manifest, verified: true };
  }

  async ensureExperimentCanonical(
    journal: EventJournal,
    candidate: JournalArchiveCandidate,
  ): Promise<ArchiveVerification<ExperimentRawArchiveManifest>> {
    await this.verifyDurableRoot();
    assertArchiveCandidate(candidate);
    const { report, rollup } = candidate;
    const directory = this.experimentDirectory(report.experimentId);
    const manifestPath = resolve(directory, "raw-manifest.json");
    const existing = await this.verifyExperimentManifest(manifestPath, candidate);
    if (existing.verified) return { ...existing, directory };
    if (await stat(manifestPath).catch(() => null)) throw new Error("ARCHIVE_EXISTING_CANONICAL_INVALID");

    await mkdir(directory, { recursive: true });
    const rawPath = resolve(directory, "canonical-events.jsonl");
    await rm(rawPath, { force: true });
    const fence = rollup.sourceFenceOccurredAt!;
    const recordedAtTo = new Date(rollup.sourceRecordedAt!);
    const rawEvents = await writeJournalEvents(journal, {
      path: rawPath,
      query: {
        types: CANONICAL_REPORT_INPUT_EVENT_TYPES,
        experimentId: report.experimentId,
        to: new Date(fence),
        commitSeqTo: rollup.sourceCommitSeq,
        recordedAtTo,
        limit: EVENT_PAGE_SIZE,
      },
      include: (event) => (
        event.occurredAt < fence
        && (event.payload as { experimentId?: unknown }).experimentId === report.experimentId
      ),
    });
    if (rawEvents.eventCount === 0 && report.sourceEventCount > 0) {
      await rm(rawPath, { force: true }).catch(() => undefined);
      throw new Error("ARCHIVE_CANONICAL_SOURCE_EMPTY");
    }

    const finalReport = await writeJsonArchiveFile(directory, "final-report.json", report);
    const canonicalRollup = await writeJsonArchiveFile(directory, "canonical-rollup.json", rollup);
    const manifest: ExperimentRawArchiveManifest = {
      archiveSchemaVersion: 2,
      kind: "experiment_canonical",
      experimentId: report.experimentId,
      createdAt: new Date().toISOString(),
      reportContentHash: report.contentHash,
      rollupContentHash: rollup.contentHash,
      sourceEventCount: report.sourceEventCount,
      sourceFenceId: rollup.sourceFenceId!,
      sourceFenceOccurredAt: fence,
      sourceCommitSeq: rollup.sourceCommitSeq!,
      sourceRecordedAt: rollup.sourceRecordedAt!,
      rawEvents,
      finalReport,
      canonicalRollup,
    };
    await writeAtomic(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
    const verified = await this.verifyExperimentManifest(manifestPath, candidate);
    if (!verified.verified) throw new Error("ARCHIVE_EXPERIMENT_VERIFY_FAILED");
    return { ...verified, directory };
  }

  async ensureJournalBucket(
    journal: EventJournal,
    input: {
      retentionClass: string;
      eventTypes: readonly string[];
      occurredFrom: Date;
      occurredBefore: Date;
      commitSeqTo: number;
      recordedAtTo: Date;
      unattributedOnly?: boolean;
    },
  ): Promise<ArchiveVerification<JournalBucketArchiveManifest>> {
    await this.verifyDurableRoot();
    const classDir = resolve(this.rootDir, "journal", safePathComponent(input.retentionClass));
    const start = input.occurredFrom.toISOString();
    const end = input.occurredBefore.toISOString();
    const key = `${safePathComponent(start)}__${safePathComponent(end)}__c${input.commitSeqTo}`;
    const directory = resolve(classDir, key);
    const manifestPath = resolve(directory, "manifest.json");
    const existing = await this.verifyBucketManifest(manifestPath, input);
    if (existing.verified) return { ...existing, directory };
    if (await stat(manifestPath).catch(() => null)) throw new Error("ARCHIVE_EXISTING_BUCKET_INVALID");

    await mkdir(directory, { recursive: true });
    const rawPath = resolve(directory, "events.jsonl");
    await rm(rawPath, { force: true });
    const rawEvents = await writeJournalEvents(journal, {
      path: rawPath,
      query: {
        types: input.eventTypes,
        from: input.occurredFrom,
        to: input.occurredBefore,
        commitSeqTo: input.commitSeqTo,
        recordedAtTo: input.recordedAtTo,
        limit: EVENT_PAGE_SIZE,
      },
      include: (event) => (
        event.occurredAt >= start
        && event.occurredAt < end
        && (!input.unattributedOnly || !((event.payload as { experimentId?: unknown }).experimentId))
      ),
    });
    if (rawEvents.eventCount === 0) {
      await rm(directory, { recursive: true, force: true }).catch(() => undefined);
      throw new Error("ARCHIVE_BUCKET_SOURCE_EMPTY");
    }
    const manifest: JournalBucketArchiveManifest = {
      archiveSchemaVersion: 1,
      kind: "journal_bucket",
      retentionClass: input.retentionClass,
      createdAt: new Date().toISOString(),
      occurredFrom: start,
      occurredBefore: end,
      commitSeqTo: input.commitSeqTo,
      recordedAtTo: input.recordedAtTo.toISOString(),
      unattributedOnly: input.unattributedOnly === true,
      eventTypes: [...input.eventTypes].sort(),
      rawEvents,
    };
    await writeAtomic(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
    const verified = await this.verifyBucketManifest(manifestPath, input);
    if (!verified.verified) throw new Error("ARCHIVE_BUCKET_VERIFY_FAILED");
    return { ...verified, directory };
  }

  private async verifyExperimentManifest(
    manifestPath: string,
    candidate: JournalArchiveCandidate,
  ): Promise<ArchiveVerification<ExperimentRawArchiveManifest>> {
    const directory = dirname(manifestPath);
    try {
      const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as ExperimentRawArchiveManifest;
      if (
        manifest.archiveSchemaVersion !== 2
        || manifest.kind !== "experiment_canonical"
        || manifest.experimentId !== candidate.report.experimentId
        || manifest.reportContentHash !== candidate.report.contentHash
        || manifest.rollupContentHash !== candidate.rollup.contentHash
        || manifest.sourceFenceId !== candidate.rollup.sourceFenceId
        || manifest.sourceFenceOccurredAt !== candidate.rollup.sourceFenceOccurredAt
        || manifest.sourceCommitSeq !== candidate.rollup.sourceCommitSeq
        || manifest.sourceRecordedAt !== candidate.rollup.sourceRecordedAt
      ) return { verified: false, manifest: null, directory };
      const filesOk = await Promise.all([
        verifyFile(directory, manifest.rawEvents),
        verifyFile(directory, manifest.finalReport),
        verifyFile(directory, manifest.canonicalRollup),
      ]);
      return { verified: filesOk.every(Boolean), manifest: filesOk.every(Boolean) ? manifest : null, directory };
    } catch {
      return { verified: false, manifest: null, directory };
    }
  }

  private async verifyBucketManifest(
    manifestPath: string,
    input: {
      retentionClass: string;
      eventTypes: readonly string[];
      occurredFrom: Date;
      occurredBefore: Date;
      commitSeqTo: number;
      recordedAtTo: Date;
      unattributedOnly?: boolean;
    },
  ): Promise<ArchiveVerification<JournalBucketArchiveManifest>> {
    const directory = dirname(manifestPath);
    try {
      const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as JournalBucketArchiveManifest;
      if (
        manifest.archiveSchemaVersion !== 1
        || manifest.kind !== "journal_bucket"
        || manifest.retentionClass !== input.retentionClass
        || manifest.occurredFrom !== input.occurredFrom.toISOString()
        || manifest.occurredBefore !== input.occurredBefore.toISOString()
        || manifest.commitSeqTo !== input.commitSeqTo
        || Date.parse(manifest.recordedAtTo) > input.recordedAtTo.getTime()
        || manifest.unattributedOnly !== (input.unattributedOnly === true)
        || JSON.stringify(manifest.eventTypes) !== JSON.stringify([...input.eventTypes].sort())
      ) return { verified: false, manifest: null, directory };
      const ok = await verifyFile(directory, manifest.rawEvents);
      return { verified: ok, manifest: ok ? manifest : null, directory };
    } catch {
      return { verified: false, manifest: null, directory };
    }
  }
}

/** Human-only deterministic summary export. The engine never reads this archive as trading authority. */
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
