import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  type JournalEvent,
} from "@pm/core";
import {
  MemoryEventJournal,
  createJournalEvent,
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
    expect(JSON.parse(await readFile(result.manifestPath, "utf8")).contentHash).toBe(result.contentHash);
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
