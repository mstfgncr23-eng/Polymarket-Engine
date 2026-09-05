/**
 * Report / retention boundedness diagnostic.
 * Logical events, page count, events/sec, max page retained, accumulator HWM,
 * retention batch size/duration. Not a production capacity claim.
 *
 *   pnpm bench:retention
 */
import { createId, TELEMETRY_EVENT_TYPES } from "@pm/core";
import { MemoryEventJournal, createJournalEvent, scanJournalEvents } from "@pm/db";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.ts";
import { defaultRetentionConfig, RetentionManager } from "../apps/engine/src/retention-manager.ts";

const EVENTS = 100_000;
const PAGE = 500;

function cpuMs(): number {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1000;
}

async function main(): Promise<void> {
  const journal = new MemoryEventJournal();
  const start = Date.parse("2026-01-01T00:00:00.000Z");
  const batch: ReturnType<typeof createJournalEvent>[] = [];
  for (let i = 0; i < EVENTS; i += 1) {
    batch.push(createJournalEvent({
      id: createId("e"),
      traceId: "t",
      type: i % 17 === 0 ? "RISK_EVALUATION" : "MARKET_SNAPSHOT",
      aggregateType: "bench",
      aggregateId: `m-${i % 8}`,
      payload: { accepted: i % 2 === 0, marketId: `m-${i % 8}`, experimentId: "exp-bench" },
      occurredAt: new Date(start + i),
    }));
    if (batch.length === 2_000) {
      await journal.appendMany(batch);
      batch.length = 0;
    }
  }
  if (batch.length) await journal.appendMany(batch);

  const acc = new ExperimentSummaryAccumulator("exp-bench", { reportKind: "final", metricsScope: "complete", generatedAt: "2026-01-01T00:00:00.000Z" });
  const wall0 = performance.now();
  const cpu0 = cpuMs();
  const scan = await scanJournalEvents(journal, { types: ["RISK_EVALUATION", "MARKET_SNAPSHOT"] }, {
    pageSize: PAGE,
    onPage: (page) => { for (const event of page) acc.fold(event); },
  });
  acc.finalize();
  const wallMs = performance.now() - wall0;
  const cpu = cpuMs() - cpu0;

  const retention = new RetentionManager(journal, defaultRetentionConfig({ batchSize: 200, cycleBudgetMs: 10_000 }));
  const t1 = performance.now();
  const cycle = await retention.runCycle({
    experiment: null,
    now: new Date(start + EVENTS + 8 * 60 * 60_000),
    degraded: false,
    failClosed: false,
    databaseUp: true,
  });

  const result = {
    disclaimer: "Boundedness diagnostic only. Not a production capacity claim.",
    logicalEvents: EVENTS,
    pageSize: PAGE,
    pages: scan.pages,
    scanned: scan.scanned,
    maxPageRetained: scan.maxPageRetained,
    accumulatorHighWaterBytes: acc.highWaterMarkBytes,
    eventsPerSec: Math.round(scan.scanned / (wallMs / 1000)),
    scanWallMs: Number(wallMs.toFixed(2)),
    scanCpuMs: Number(cpu.toFixed(2)),
    complete: scan.complete,
    retentionDeleted: cycle.deletedByClass,
    retentionBatchCount: cycle.batchCount,
    lastRetentionBatchSize: cycle.lastBatchSize,
    lastRetentionBatchDurationMs: cycle.lastBatchDurationMs,
    telemetryTypes: TELEMETRY_EVENT_TYPES,
  };
  console.log(JSON.stringify(result, null, 2));
}

await main();
