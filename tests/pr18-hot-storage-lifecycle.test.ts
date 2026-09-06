import { mkdtemp, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  createIdleExperiment,
  type JournalEvent,
} from "@pm/core";
import {
  MemoryEventJournal,
  PostgresEventJournal,
  createJournalEvent,
  hashCheckpointPayload,
} from "@pm/db";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import { exportExperimentArchive } from "../apps/engine/src/archive.js";
import {
  hashAnalyticsRollup,
  loadVerifiedAnalyticsRollup,
  persistVerifiedAnalyticsRollup,
  streamExperimentSummary,
} from "../apps/engine/src/final-report.js";
import { defaultRetentionConfig, RetentionManager } from "../apps/engine/src/retention-manager.js";
import { resolveCleanupLifecycle } from "../apps/engine/src/cleanup-lifecycle.js";

function event(type: string, id: string, occurredAt: string, payload: Record<string, unknown> = {}): JournalEvent {
  return createJournalEvent({
    id,
    traceId: "pr18-trace",
    type,
    aggregateType: "experiment",
    aggregateId: "exp-pr18",
    payload: { experimentId: "exp-pr18", ...payload },
    occurredAt: new Date(occurredAt),
  });
}

function checkpointPayload(cursor: number): Record<string, unknown> {
  return {
    schemaVersion: 2,
    durableCursor: cursor,
    ledger: { cashUsd: "100", reservedUsd: "0", realizedPnlUsd: "0", feesUsd: "0" },
    positions: [], restingOrders: [], activeTriggers: [], grokSignals: [],
    makerReservations: {}, managedMarkets: [], settledMarkets: [], marketMetadata: [],
    experiment: { id: "exp-pr18", status: "COMPLETED", startedAt: "2026-01-01T00:00:00.000Z", completedAt: "2026-01-01T00:01:00.000Z" },
  };
}

async function durableRows(journal: MemoryEventJournal): Promise<number> {
  return (await journal.queryDurableTransitions({ limit: 10_000 })).length;
}

describe("PR18 hot operational storage lifecycle", () => {
  it("uses verified snapshots as a fallback and keeps the durable high-water monotonic after compaction", async () => {
    const journal = new MemoryEventJournal();
    let cursor = 0;
    for (let i = 0; i < 5; i += 1) {
      const committed = await journal.commitDurableTransition({
        operationKey: `op:${i}`, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: `p:${i}`,
        payload: { experimentId: "exp-pr18", marketId: `m:${i}`, realizedDelta: "0" },
      }, cursor);
      cursor = committed.sequence;
    }
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 1, payload: checkpointPayload(cursor) });
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 2, payload: checkpointPayload(cursor) });
    const verified = await journal.listVerifiedCheckpoints();
    expect(verified).toHaveLength(2);
    const internal = journal as unknown as { checkpoints: Array<{ sequence: number; payload: Record<string, unknown> }> };
    internal.checkpoints.find((row) => row.sequence === 2)!.payload.ledger = { cashUsd: "corrupt" };
    expect((await journal.listVerifiedCheckpoints())[0]!.sequence).toBe(1);

    const compacted = await journal.compactDurableTransitionsThrough(cursor);
    expect(compacted.deleted).toBe(5);
    expect(await durableRows(journal)).toBe(0);
    const replay = await journal.readDurableByOperationKey("op:0");
    expect(replay?.sequence).toBe(1);
    const next = await journal.commitDurableTransition({
      operationKey: "op:next", type: "POSITION_SETTLED", aggregateType: "position", aggregateId: "p:next", payload: { experimentId: "exp-pr18" },
    }, cursor);
    expect(next.sequence).toBe(6);
    const duplicate = await journal.commitDurableTransition({
      operationKey: "op:0", type: "POSITION_SETTLED", aggregateType: "position", aggregateId: "p:0", payload: { experimentId: "exp-pr18", marketId: "m:0", realizedDelta: "0" },
    }, 6);
    expect(duplicate.inserted).toBe(false);
    expect(duplicate.sequence).toBe(1);
  });

  it("round-trips the complete accumulator and verifies the analytics rollup", async () => {
    const journal = new MemoryEventJournal();
    const first = event("RISK_EVALUATION", "risk-1", "2026-01-01T00:00:01.000Z", { accepted: true, marketId: "m1", asset: "BTC" });
    const fill = event("PAPER_FILL", "fill-1", "2026-01-01T00:00:02.000Z", {
      marketId: "m1", asset: "BTC", side: "BUY", outcome: "UP", quantity: "2", fillPrice: "0.4", feeUsd: "0", slippageUsd: "0", liquidityRole: "TAKER",
    });
    await journal.appendMany([first, fill]);
    const acc = new ExperimentSummaryAccumulator("exp-pr18", { status: "COMPLETED", complete: true, reportKind: "final", metricsScope: "complete" });
    acc.fold(first); acc.fold(fill);
    const restored = ExperimentSummaryAccumulator.fromSnapshot(acc.serialize());
    const originalSummary = acc.finalize();
    const restoredSummary = restored.finalize();
    expect({ ...restoredSummary, generatedAt: null }).toEqual({ ...originalSummary, generatedAt: null });
    const rollup = await persistVerifiedAnalyticsRollup(journal, {
      experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: fill.id, sourceFenceOccurredAt: fill.occurredAt,
      accumulator: acc.serialize() as unknown as Record<string, unknown>,
    });
    expect(rollup?.verified).toBe(true);
    expect(hashAnalyticsRollup(rollup!.accumulator)).toBe(rollup!.contentHash);
    expect((await loadVerifiedAnalyticsRollup(journal, "exp-pr18"))?.contentHash).toBe(rollup!.contentHash);
  });

  it("keeps finalize and restore idempotent after a persisted rollup", async () => {
    const journal = new MemoryEventJournal();
    const startedAt = "2026-01-01T00:00:00.000Z";
    const fill = event("PAPER_FILL", "fill-idempotent", "2026-01-01T00:00:02.000Z", { marketId: "m1", asset: "BTC", side: "BUY", outcome: "UP", quantity: "2", fillPrice: "0.4", feeUsd: "0.01", slippageUsd: "0.02", liquidityRole: "TAKER" });
    await journal.append(fill);
    const acc = new ExperimentSummaryAccumulator("exp-pr18", { status: "RUNNING", generatedAt: startedAt, complete: false, reportKind: "interim", metricsScope: "rolling" });
    acc.fold(fill);
    const first = acc.finalize();
    const persisted = await persistVerifiedAnalyticsRollup(journal, { experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: fill.id, sourceFenceOccurredAt: fill.occurredAt, sourceEventCount: 1, sourceRecordedAt: fill.recordedAt, stableRead: true, accumulator: acc.serialize() as unknown as Record<string, unknown> });
    const restored = ExperimentSummaryAccumulator.fromSnapshot(persisted!.accumulator as unknown as Parameters<typeof ExperimentSummaryAccumulator.fromSnapshot>[0]);
    expect({ ...restored.finalize(), generatedAt: startedAt }).toEqual({ ...first, generatedAt: startedAt });
  });

  it("does not delete a late old-timestamp event until the next rollup", async () => {
    const journal = new MemoryEventJournal();
    const first = event("RISK_EVALUATION", "late-base", "2026-01-01T00:00:10.000Z", { accepted: true, marketId: "m1" });
    await journal.append(first);
    const streamed = await streamExperimentSummary(journal, { id: "exp-pr18", startedAt: "2026-01-01T00:00:00.000Z" }, { status: "RUNNING", complete: false, reportKind: "interim", metricsScope: "rolling" });
    const rollup = await persistVerifiedAnalyticsRollup(journal, { experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: streamed.scan.fence.through!.id, sourceFenceOccurredAt: streamed.scan.fence.through!.occurredAt, sourceEventCount: streamed.accumulatorSnapshot!.eventCount, sourceCommitSeq: streamed.scan.fence.commitSeqTo, sourceRecordedAt: streamed.scan.fence.recordedAtTo.toISOString(), stableRead: streamed.scan.fence.stable, accumulator: streamed.accumulatorSnapshot as unknown as Record<string, unknown> });
    const late = event("RISK_EVALUATION", "late-after-snapshot", "2025-01-01T00:00:01.000Z", { accepted: true, marketId: "m2" });
    await journal.append(late);
    await journal.deleteCanonicalEventsThrough!({ experimentId: "exp-pr18", occurredBefore: new Date(rollup!.sourceFenceOccurredAt!), sourceCursor: 0, sourceCommitSeq: rollup!.sourceCommitSeq, sourceRecordedAt: new Date(rollup!.sourceRecordedAt!) });
    expect(await journal.readJournalEventById(late.id)).not.toBeNull();
    const next = await streamExperimentSummary(journal, { id: "exp-pr18", startedAt: "2025-01-01T00:00:00.000Z" }, { status: "RUNNING", complete: false, reportKind: "interim", metricsScope: "rolling" });
    expect(next.summary.totalOpportunities).toBe(2);
  });

  it("advances rollups monotonically with CAS prefix checks", async () => {
    const journal = new MemoryEventJournal();
    const a = event("RISK_EVALUATION", "mono-a", "2026-01-01T00:00:01.000Z", { accepted: true, marketId: "m1" });
    const b = event("RISK_EVALUATION", "mono-b", "2026-01-01T00:00:02.000Z", { accepted: true, marketId: "m2" });
    await journal.appendMany([a, b]);
    const first = await persistVerifiedAnalyticsRollup(journal, { experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: a.id, sourceFenceOccurredAt: a.occurredAt, sourceEventCount: 1, sourceCommitSeq: 1, sourceRecordedAt: a.recordedAt, accumulator: { eventCount: 1 } });
    const second = await persistVerifiedAnalyticsRollup(journal, { experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: b.id, sourceFenceOccurredAt: b.occurredAt, sourceEventCount: 2, sourceCommitSeq: 2, sourceRecordedAt: b.recordedAt, accumulator: { eventCount: 2 } });
    expect(second!.sourceEventCount).toBe(2);
    const older = await persistVerifiedAnalyticsRollup(journal, { experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: a.id, sourceFenceOccurredAt: a.occurredAt, sourceEventCount: 1, sourceCommitSeq: 1, sourceRecordedAt: a.recordedAt, accumulator: { eventCount: 1 } });
    expect(older!.contentHash).toBe(second!.contentHash);
    expect(first!.sourceEventCount).toBe(1);
  });

  it("enforces report-write capability for rollup persistence", async () => {
    const journal = new MemoryEventJournal();
    const rollup = {
      experimentId: "exp-pr18", rollupKind: "canonical", schemaVersion: 1,
      sourceCursor: 0, sourceFenceId: null, sourceFenceOccurredAt: null,
      sourceEventCount: 0, sourceCommitSeq: 0, sourceRecordedAt: null, stableRead: true,
      accumulator: { eventCount: 0 }, contentHash: "hash", verified: true,
      createdAt: "2026-01-01T00:00:00.000Z",
    };
    await journal.connectWithoutMigrate("readonly");
    await expect(journal.persistAnalyticsRollup!(rollup)).rejects.toMatchObject({ code: "JOURNAL_MUTATE_FORBIDDEN" });
    await journal.connectWithoutMigrate("report-write");
    await expect(journal.persistAnalyticsRollup!(rollup)).resolves.toMatchObject({ experimentId: "exp-pr18" });
  });

  it("rolls active canonical input into a bounded hot set before completion", async () => {
    const journal = new MemoryEventJournal();
    const manager = new RetentionManager(journal, defaultRetentionConfig({ enabled: true, analyticsRollupEnabled: true, maxRowsPerCycle: 500, maxBatchesPerCycle: 20, batchSize: 100 }));
    for (let cycle = 0; cycle < 4; cycle += 1) {
      for (let i = 0; i < 20; i += 1) await journal.append(event("RISK_EVALUATION", `active-${cycle}-${i}`, `2026-01-01T00:0${cycle}:${String(i).padStart(2, "0")}.000Z`, { accepted: i % 2 === 0, marketId: `m${i}` }));
      await manager.runCycle({ experiment: { id: "exp-pr18", status: "RUNNING", startedAt: "2026-01-01T00:00:00.000Z" }, now: new Date("2026-09-06T00:00:00.000Z"), degraded: false, failClosed: false, databaseUp: true });
      const count = (await journal.countByType(CANONICAL_REPORT_INPUT_EVENT_TYPES)).RISK_EVALUATION ?? 0;
      expect(count).toBeLessThanOrEqual(1);
    }
    expect((await journal.loadAnalyticsRollup!("exp-pr18"))?.verified).toBe(true);
  });

  it("falls back from a hash-valid but semantically invalid newest snapshot", async () => {
    const journal = new MemoryEventJournal();
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 1, payload: { durableCursor: 0, ledger: { cashUsd: "100", reservedUsd: "0" }, positions: [] } });
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 2, payload: { durableCursor: 0, ledger: { cashUsd: "100", reservedUsd: "0" }, positions: [] } });
    const internal = journal as unknown as { checkpoints: Array<{ sequence: number; payload: Record<string, unknown>; contentHash?: string }> };
    const newest = internal.checkpoints.find((row) => row.sequence === 2)!;
    newest.payload.ledger = { cashUsd: "-1", reservedUsd: "0" };
    newest.payload.checkpointContentHash = hashCheckpointPayload(newest.payload);
    newest.contentHash = newest.payload.checkpointContentHash as string;
    expect((await journal.listVerifiedCheckpoints())[0]!.sequence).toBe(1);
  });

  it.skipIf(!process.env.STEP4_TEST_DATABASE_URL)("runs indexed replay-key compaction against disposable PG17", { timeout: 30_000 }, async () => {
    const url = new URL(process.env.STEP4_TEST_DATABASE_URL!);
    expect(["localhost", "127.0.0.1"]).toContain(url.hostname);
    const journal = new PostgresEventJournal(process.env.STEP4_TEST_DATABASE_URL!, fileURLToPath(new URL("../packages/db/migrations", import.meta.url)));
    await journal.initialize();
    const key = `pr18-pg-${Date.now()}`;
    try {
      // Use a valid no-op authoritative payload so this disposable CI
      // database can be shared with other Postgres integration tests without
      // poisoning their recovery replay with an intentionally partial fixture.
      const previous = await journal.latestDurableSequence();
      const commit = await journal.commitDurableTransition({ operationKey: key, type: "REPORT_SOURCE_INTEGRITY_FAULT", aggregateType: "report", aggregateId: key, payload: { derivedAudit: [] } }, previous);
      // Compaction preserves the durable high-water and indexed replay key but
      // removes the row itself.  Leave a valid idle checkpoint at that fence so
      // later tests sharing this disposable database do not attempt to replay a
      // compacted prefix from cursor zero.
      const idle = createIdleExperiment({
        id: "exp-pr18-idle", durationHours: 48, strategyVersion: "pr18-test",
        riskConfiguration: {}, modelConfiguration: {}, startingBalance: "100", systemVersion: "test", gitSha: "test",
      });
      const checkpoints = await journal.listCheckpoints();
      await journal.saveCheckpoint({
        experimentId: idle.id,
        sequence: (checkpoints.at(-1)?.sequence ?? 0) + 1,
        payload: {
          schemaVersion: 2,
          durableCursor: commit.sequence,
          ledger: {
            startingBalance: "100", cash: "100", reservedCash: "0", positionValue: "0", equity: "100",
            realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0", aiCost: "0",
            infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", highWaterMark: "100", drawdown: "0",
            updatedAt: new Date().toISOString(),
          },
          positions: [], restingOrders: [], activeTriggers: [], grokSignals: [],
          activationLatency: [], makerReservations: {}, managedMarkets: [], settledMarkets: [], marketMetadata: [],
          lastInfrastructureAccrualAt: Date.now(), derivedAuditCursor: commit.sequence, reportSourceIntegrity: [],
          experiment: idle,
        },
      });
      const compacted = await journal.compactDurableTransitionsThrough(commit.sequence, { maxRows: 1, batchSize: 1, maxReplayKeys: 8, maxBatches: 1, budgetMs: 1000 });
      expect(compacted.deleted).toBe(1);
      expect((await journal.readDurableByOperationKey(key))?.type).toBe("REPORT_SOURCE_INTEGRITY_FAULT");
    } finally {
      await journal.close();
    }
  });

  it("uses a verified rollup for report reconstruction after canonical source cleanup", async () => {
    const journal = new MemoryEventJournal();
    const startedAt = "2026-01-01T00:00:00.000Z";
    const first = event("RISK_EVALUATION", "risk-2", "2026-01-01T00:00:01.000Z", { accepted: true, marketId: "m1", asset: "BTC" });
    await journal.append(first);
    const streamed = await streamExperimentSummary(journal, { id: "exp-pr18", startedAt }, { status: "COMPLETED", complete: true, reportKind: "final", metricsScope: "complete" });
    await persistVerifiedAnalyticsRollup(journal, {
      experimentId: "exp-pr18", sourceCursor: 0, sourceFenceId: first.id, sourceFenceOccurredAt: first.occurredAt,
      accumulator: streamed.accumulatorSnapshot! as unknown as Record<string, unknown>,
    });
    await journal.deleteCanonicalEventsThrough!({ experimentId: "exp-pr18", sourceCursor: 0, limit: 100 });
    expect((await journal.countByType(CANONICAL_REPORT_INPUT_EVENT_TYPES)).RISK_EVALUATION ?? 0).toBe(0);
    const rebuilt = await streamExperimentSummary(journal, { id: "exp-pr18", startedAt }, { status: "COMPLETED", complete: true, reportKind: "final", metricsScope: "complete" });
    expect(rebuilt.summary.sourceEventCount).toBe(streamed.summary.sourceEventCount);
    expect(rebuilt.summary.riskEvaluationCount).toBe(streamed.summary.riskEvaluationCount);
  });

  it("compacts automatically only behind the older of two verified snapshots", async () => {
    const journal = new MemoryEventJournal();
    let cursor = 0;
    for (let i = 0; i < 3; i += 1) {
      const committed = await journal.commitDurableTransition({ operationKey: `ret:${i}`, type: "PAPER_MAKER_RESERVED", aggregateType: "order", aggregateId: `o:${i}`, payload: { experimentId: "exp-pr18" } }, cursor);
      cursor = committed.sequence;
    }
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 1, payload: checkpointPayload(cursor) });
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 2, payload: checkpointPayload(cursor) });
    const manager = new RetentionManager(journal, defaultRetentionConfig({ enabled: true, compactionEnabled: true, checkpointKeep: 4 }));
    const result = await manager.runCycle({ experiment: null, now: new Date("2026-01-01T00:00:00.000Z"), degraded: false, failClosed: false, databaseUp: true });
    expect(result.durableTransitionsPruned).toBe(true);
    expect(result.durableCompaction?.deleted).toBe(3);
    expect(await journal.latestDurableSequence()).toBe(3);
  });

  it("resolves cleanup authority from a verified snapshot after lifecycle rows are compacted", async () => {
    const journal = new MemoryEventJournal();
    let cursor = 0;
    for (const [i, type] of ["EXPERIMENT_STARTED", "EXPERIMENT_START_VALIDATED", "EXPERIMENT_COMPLETED"].entries()) {
      const committed = await journal.commitDurableTransition({
        operationKey: `life:${i}`, type, aggregateType: "experiment", aggregateId: "exp-pr18",
        payload: { experimentId: "exp-pr18", experiment: { id: "exp-pr18", status: type === "EXPERIMENT_COMPLETED" ? "COMPLETED" : "RUNNING", startedAt: "2026-01-01T00:00:00.000Z", completedAt: type === "EXPERIMENT_COMPLETED" ? "2026-01-01T00:01:00.000Z" : null } },
      }, cursor);
      cursor = committed.sequence;
    }
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 1, payload: checkpointPayload(cursor) });
    await journal.saveCheckpoint({ experimentId: "exp-pr18", sequence: 2, payload: checkpointPayload(cursor) });
    await journal.compactDurableTransitionsThrough(cursor);
    const lifecycle = await resolveCleanupLifecycle(journal, { experimentId: "exp-pr18" });
    expect(lifecycle.state).toBe("COMPLETED");
    expect(lifecycle.monitorDeleteAllowed).toBe(true);
    expect(lifecycle.experimentId).toBe("exp-pr18");
  });

  it("exports a deterministic human archive without making recovery depend on it", async () => {
    const journal = new MemoryEventJournal();
    const directory = await mkdtemp(join(tmpdir(), "pr18-archive-"));
    const result = await exportExperimentArchive(journal, { experimentId: "exp-pr18", directory, manifest: { test: true } });
    const manifest = JSON.parse(await readFile(result.manifestPath, "utf8")) as Record<string, unknown>;
    expect(manifest.contentHash).toBe(result.contentHash);
    expect(typeof manifest.canonicalReportContentHash).toBe("string");
    expect(typeof manifest.archiveBundleHash).toBe("string");
    expect((await readFile(result.summaryPath, "utf8")).length).toBeGreaterThan(0);
    expect((await readFile(result.reportPath, "utf8")).length).toBeGreaterThan(0);
  });

  it("returns hot operational counts to the same floor across repeated completed lifecycles", async () => {
    const journal = new MemoryEventJournal();
    const floors: Array<{ durable: number; canonical: number; checkpoints: number; rollups: number }> = [];
    let cursor = 0;
    for (let cycle = 0; cycle < 3; cycle += 1) {
      for (let i = 0; i < 20; i += 1) {
        const committed = await journal.commitDurableTransition({ operationKey: `cycle:${cycle}:settle:${i}`, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: `p:${cycle}:${i}`, payload: { experimentId: `exp:${cycle}` } }, cursor);
        cursor = committed.sequence;
        await journal.append(event("RISK_EVALUATION", `cycle-${cycle}-risk-${i}`, `2026-01-0${cycle + 1}T00:00:${String(i).padStart(2, "0")}.000Z`, { experimentId: `exp:${cycle}`, accepted: i % 2 === 0, marketId: `m:${i}` }));
      }
      await journal.saveCheckpoint({ experimentId: `exp:${cycle}`, sequence: cycle * 2 + 1, payload: { ...checkpointPayload(cursor), experiment: { id: `exp:${cycle}`, status: "COMPLETED", startedAt: "2026-01-01T00:00:00.000Z", completedAt: "2026-01-01T00:01:00.000Z" } } });
      await journal.saveCheckpoint({ experimentId: `exp:${cycle}`, sequence: cycle * 2 + 2, payload: { ...checkpointPayload(cursor), experiment: { id: `exp:${cycle}`, status: "COMPLETED", startedAt: "2026-01-01T00:00:00.000Z", completedAt: "2026-01-01T00:01:00.000Z" } } });
      const acc = new ExperimentSummaryAccumulator(`exp:${cycle}`, { status: "COMPLETED", complete: true, reportKind: "final", metricsScope: "complete" });
      await persistVerifiedAnalyticsRollup(journal, { experimentId: `exp:${cycle}`, sourceCursor: cursor, sourceFenceId: null, sourceFenceOccurredAt: "2026-01-01T00:01:00.000Z", accumulator: acc.serialize() as unknown as Record<string, unknown> });
      await journal.compactDurableTransitionsThrough(cursor);
      await journal.deleteCanonicalEventsThrough!({ experimentId: `exp:${cycle}`, sourceCursor: cursor, limit: 1_000 });
      const counts = journal.memoryFootprintForTest();
      floors.push({ durable: await durableRows(journal), canonical: (await journal.countByType(CANONICAL_REPORT_INPUT_EVENT_TYPES)).RISK_EVALUATION ?? 0, checkpoints: (await journal.listCheckpoints()).length, rollups: cycle + 1 });
      expect(counts.events).toBeLessThanOrEqual(64);
    }
    expect(floors.map((item) => item.durable)).toEqual([0, 0, 0]);
    expect(floors.map((item) => item.canonical)).toEqual([0, 0, 0]);
    expect(floors.map((item) => item.checkpoints)).toEqual([2, 4, 6]);
    expect(floors.map((item) => item.rollups)).toEqual([1, 2, 3]);
    expect(await journal.latestDurableSequence()).toBe(60);
  });
});
