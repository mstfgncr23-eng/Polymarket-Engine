import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  AUDIT_CRITICAL_EVENT_TYPES,
  DECISION_EVENT_TYPES,
  EVENT_PAGE_SIZE,
  MONITOR_CONTROL_EVENT_TYPES,
  PaperLedger,
  RETENTION_DEFAULTS,
  TELEMETRY_EVENT_TYPES,
  TRACE_PAGE_MAX,
  assertMcpIdempotencyTtlSafe,
  classifyJournalRetention,
  createId,
  engineEnvSchema,
  grokSignalSubmissionSchema,
  type JournalEvent,
} from "@pm/core";
import {
  ExperimentReportConflictError,
  JournalMigrateForbiddenError,
  MemoryEventJournal,
  PostgresEventJournal,
  createJournalEvent,
  evaluateSchemaInspection,
  hashExperimentReport,
  isCanonicalFinalReport,
  persistReportConflictSafe,
  reportGeneratedAuditId,
  scanJournalEvents,
  SchemaVerificationError,
} from "@pm/db";
import { createApiServer } from "../apps/engine/src/server.js";
import { buildExperimentSummary, ExperimentSummaryAccumulator, writeExperimentSummary } from "../apps/engine/src/report.js";
import { ensureReportGeneratedAudit, persistCanonicalFinalReport, resolveAuthoritativeExperiment, streamExperimentSummary, summaryFromDurableReport, toDurableReportInput } from "../apps/engine/src/final-report.js";
import { applyJournalCleanup, dryRunJournalCleanup } from "../apps/engine/src/journal-cleanup.js";
import { generateOperatorReport } from "../apps/engine/src/operator-report.js";
import { defaultRetentionConfig, experimentIsActive, experimentIsMonitorOnly, RetentionManager } from "../apps/engine/src/retention-manager.js";
import { completeMetricsFlightKey, resetStrategyMetricsCacheForTest } from "../apps/engine/src/mcp.js";
import { KeyedResultFlight } from "../apps/engine/src/runtime-supervisor.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { EngineService, RUNTIME_BOUNDS as ENGINE_BOUNDS } from "../apps/engine/src/engine.js";
import { RUNTIME_BOUNDS as SUPERVISOR_BOUNDS, SharedResultFlight } from "../apps/engine/src/runtime-supervisor.js";
import { fill } from "./fixtures.js";
import {
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
} from "./paper-engine-testkit.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";

function ev(type: string, occurredAt: string, payload: Record<string, unknown> = {}, id?: string): JournalEvent {
  return createJournalEvent({
    id: id ?? createId("e"),
    traceId: "t",
    type,
    aggregateType: "test",
    aggregateId: String(payload.aggregateId ?? payload.marketId ?? type),
    payload: { experimentId: "exp-1", ...payload },
    occurredAt: new Date(occurredAt),
  });
}

function fixtureEvents(): JournalEvent[] {
  const t0 = "2026-09-01T00:00:00.000Z";
  const paper = { ...fill("UP", "0.40", "10"), marketId: "m1", feeUsd: "0.02", slippageUsd: "0.01" };
  return [
    ev("EXPERIMENT_STARTED", t0, { status: "RUNNING" }, "e-start"),
    ev("RISK_EVALUATION", "2026-09-01T00:00:01.000Z", { accepted: true, marketId: "m1", asset: "BTC" }, "e-risk"),
    ev("PAPER_ORDER_CREATED", "2026-09-01T00:00:02.000Z", { orderType: "TAKER", marketId: "m1" }, "e-ord"),
    ev("PAPER_FILL", "2026-09-01T00:00:03.000Z", { ...paper, asset: "BTC", liquidityRole: "TAKER" }, "e-fill"),
    ev("POSITION_UPDATED", "2026-09-01T00:00:04.000Z", {
      id: "pos-1", marketId: "m1", upQuantity: "10", downQuantity: "0", unpairedQuantity: "10", pairedQuantity: "0",
      averageUpCost: "0.40", averageDownCost: "0",
    }, "e-pos"),
    ev("FIRST_LEG_FILLED", "2026-09-01T00:00:05.000Z", { marketId: "m1" }, "e-fl"),
    ev("PNL_SNAPSHOT", "2026-09-01T00:00:06.000Z", {
      equity: "1001", netTradingPnl: "1.2", grossPnl: "1.3", fees: "0.02", slippage: "0.01",
      aiCost: "0", infrastructureCost: "0", netEconomicPnl: "1.2", drawdown: "0.1",
    }, "e-pnl"),
    ev("POSITION_SETTLED", "2026-09-01T00:00:07.000Z", {
      marketId: "m1", asset: "BTC", realizedDelta: 6, upQuantity: "10", downQuantity: "10", grossOutcome: "POSITION_WIN",
    }, "e-set"),
    ev("EXPERIMENT_COMPLETED", "2026-09-01T00:00:08.000Z", { status: "COMPLETED" }, "e-done"),
  ];
}

describe("Step-4 streaming summary equivalence", () => {
  it("streamed accumulator matches array summary field-for-field on the authoritative fixture", async () => {
    const events = fixtureEvents();
    const generatedAt = "2026-09-01T12:00:00.000Z";
    const extras = { reportKind: "final" as const, metricsScope: "complete" as const, status: "COMPLETED", generatedAt, complete: true, truncated: false };
    const array = buildExperimentSummary(events, "exp-1", extras);
    const acc = new ExperimentSummaryAccumulator("exp-1", extras);
    for (const event of events) acc.fold(event);
    const streamed = acc.finalize();
    expect(streamed).toEqual(array);
    expect(acc.retainedEventCount).toBe(0);
    expect(acc.highWaterMarkBytes).toBeLessThan(events.length * 2_000);
  });
});

describe("Step-4 keyset scanner bounds", () => {
  it("100k+ scan retains a page + bounded accumulator, not an event-count array", { timeout: 60_000 }, async () => {
    const journal = new MemoryEventJournal();
    const start = Date.parse("2026-01-01T00:00:00.000Z");
    const n = 100_000;
    const batch = [];
    for (let i = 0; i < n; i += 1) {
      batch.push(ev("RISK_EVALUATION", new Date(start + i * 1000).toISOString(), { accepted: i % 2 === 0, marketId: "m", i }, `risk-${i}`));
    }
    await journal.appendMany(batch);
    const acc = new ExperimentSummaryAccumulator("exp-1", { reportKind: "final", generatedAt: "2026-01-02T00:00:00.000Z" });
    const scan = await scanJournalEvents(journal, { types: ["RISK_EVALUATION"] }, {
      pageSize: 250,
      onPage: (page) => { for (const event of page) acc.fold(event); },
    });
    const summary = acc.finalize();
    expect(scan.complete).toBe(true);
    expect(scan.scanned).toBe(n);
    expect(scan.pages).toBe(Math.ceil(n / 250));
    expect(scan.maxPageRetained).toBe(250);
    expect(acc.retainedEventCount).toBe(0);
    expect(summary.totalOpportunities).toBe(n);
    expect(acc.retainedEntityCardinality).toBeLessThan(16);
    expect(acc.retainedMarketCardinality).toBeLessThan(8);
  });

  it("equal occurredAt neither duplicates nor skips", async () => {
    const journal = new MemoryEventJournal();
    const at = "2026-09-01T00:00:00.000Z";
    await journal.appendMany([
      ev("RISK_EVALUATION", at, { accepted: true, marketId: "m" }, "id-a"),
      ev("RISK_EVALUATION", at, { accepted: true, marketId: "m" }, "id-b"),
      ev("RISK_EVALUATION", at, { accepted: false, marketId: "m" }, "id-c"),
    ]);
    const seen = new Set<string>();
    await scanJournalEvents(journal, { types: ["RISK_EVALUATION"] }, {
      pageSize: 1,
      onPage: (page) => { for (const event of page) seen.add(event.id); },
    });
    expect([...seen].sort()).toEqual(["id-a", "id-b", "id-c"]);
  });

  it("scan interruption marks complete=false and does not persist a canonical final", async () => {
    const journal = new MemoryEventJournal();
    await journal.appendMany(fixtureEvents());
    const abort = new AbortController();
    abort.abort();
    const streamed = await streamExperimentSummary(journal, {
      id: "exp-1", startedAt: "2026-09-01T00:00:00.000Z", completedAt: "2026-09-01T00:00:08.000Z",
    }, { reportKind: "final", metricsScope: "complete" }, { signal: abort.signal });
    expect(streamed.scan.interrupted).toBe(true);
    expect(streamed.summary.complete).toBe(false);
    const persisted = await persistCanonicalFinalReport(journal, { summary: streamed.summary, scan: streamed.scan, experimentStatus: "COMPLETED" });
    expect(persisted).toBeNull();
    expect(await journal.loadExperimentReport("exp-1")).toBeNull();
  });

  it("source fence excludes late writes deterministically", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("RISK_EVALUATION", "2026-09-01T00:00:00.000Z", { accepted: true, marketId: "m" }, "early"));
    const ids: string[] = [];
    const scan = scanJournalEvents(journal, { types: ["RISK_EVALUATION"] }, {
      onPage: async (page) => {
        ids.push(...page.map((item) => item.id));
        await journal.append(ev("RISK_EVALUATION", "2026-09-01T00:00:01.000Z", { accepted: true, marketId: "m" }, "late"));
      },
    });
    const stats = await scan;
    expect(ids).toEqual(["early"]);
    expect(stats.complete).toBe(true);
    expect(stats.fence.through?.id).toBe("early");
  });
});

describe("Step-4 durable final report", () => {
  it("same hash is idempotent; conflicting hash fails closed; FS failure keeps canonical", async () => {
    const journal = new MemoryEventJournal();
    await journal.appendMany(fixtureEvents());
    const streamed = await streamExperimentSummary(journal, {
      id: "exp-1", startedAt: "2026-09-01T00:00:00.000Z", completedAt: "2026-09-01T00:00:08.000Z",
    }, { reportKind: "final", metricsScope: "complete", generatedAt: "2026-09-01T12:00:00.000Z", complete: true, truncated: false });
    streamed.summary.complete = true;
    streamed.summary.truncated = false;
    const first = await persistCanonicalFinalReport(journal, { summary: streamed.summary, scan: streamed.scan, experimentStatus: "COMPLETED" });
    expect(first?.inserted).toBe(true);
    const second = await persistCanonicalFinalReport(journal, { summary: streamed.summary, scan: streamed.scan, experimentStatus: "COMPLETED" });
    expect(second?.inserted).toBe(false);
    expect(second?.report.contentHash).toBe(first?.report.contentHash);
    const conflict = { ...streamed.summary, totalFills: 99 };
    const bad = toDurableReportInput({ summary: conflict, scan: streamed.scan });
    await expect(journal.persistExperimentReport(bad)).rejects.toBeInstanceOf(ExperimentReportConflictError);
    const dir = await mkdtemp(join(tmpdir(), "pm-report-"));
    await writeFile(join(dir, "blocked"), "x", { mode: 0o400 });
    await expect(writeExperimentSummary(streamed.summary, join(dir, "blocked"))).rejects.toBeTruthy();
    const kept = await journal.loadExperimentReport("exp-1");
    expect(kept && isCanonicalFinalReport(kept)).toBe(true);
    expect(hashExperimentReport(kept!)).toBe(kept!.contentHash);
  });
});

describe("Step-4 MCP / API heavy-read closure", () => {
  it("completed complete-scope reads the persisted report; running complete does not allocate a full-history array", async () => {
    const journal = new MemoryEventJournal();
    await journal.appendMany(fixtureEvents());
    const streamed = await streamExperimentSummary(journal, {
      id: "exp-1", startedAt: "2026-09-01T00:00:00.000Z", completedAt: "2026-09-01T00:00:08.000Z",
    }, { reportKind: "final", generatedAt: "2026-09-01T12:00:00.000Z", complete: true, truncated: false });
    streamed.summary.complete = true;
    streamed.summary.truncated = false;
    await persistCanonicalFinalReport(journal, { summary: streamed.summary, scan: streamed.scan, experimentStatus: "COMPLETED" });
    const loaded = await journal.loadExperimentReport("exp-1");
    expect(loaded && isCanonicalFinalReport(loaded)).toBe(true);
    expect(summaryFromDurableReport(loaded!).experimentId).toBe("exp-1");
    expect(summaryFromDurableReport(loaded!).complete).toBe(true);
    const acc = new ExperimentSummaryAccumulator("exp-run", { reportKind: "final" });
    for (let i = 0; i < 5_000; i += 1) acc.fold(ev("RISK_EVALUATION", "2026-09-01T00:00:00.000Z", { accepted: true, marketId: "m" }, `r-${i}`));
    expect(acc.retainedEventCount).toBe(0);
    expect(acc.highWaterMarkBytes).toBeLessThan(5_000 * 200);
    const flight = new SharedResultFlight<number>();
    let scans = 0;
    const work = () => { scans += 1; return new Promise<number>((resolve) => setTimeout(() => resolve(1), 20)); };
    await Promise.all([flight.run(work), flight.run(work), flight.run(work)]);
    expect(scans).toBe(1);
    expect(flight.coalesced).toBe(2);
  });

  it("GET /api/traces enforces a hard per-request page cap", async () => {
    const journal = new MemoryEventJournal();
    const traceId = "trace-cap";
    for (let i = 0; i < TRACE_PAGE_MAX + 50; i += 1) {
      await journal.append(createJournalEvent({
        id: `e-${String(i).padStart(3, "0")}`, traceId, type: "TRIGGER_EVENT", aggregateType: "t", aggregateId: "t",
        payload: { i }, occurredAt: new Date(Date.parse("2026-09-01T00:00:00.000Z") + i),
      }));
    }
    const ledger = new PaperLedger(1000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    const app = await createApiServer({
      state, journal, metrics: new EngineMetrics(), sse: { add: () => () => undefined } as never,
      dashboardOrigin: "http://localhost:3000",
      mcp: { bearerTokens: "", allowedOrigins: "", rateLimitPerMinute: 60 },
      submitGrokSignal: async () => ({}),
      logger: false,
    });
    const res = await app.inject({ method: "GET", url: `/api/traces/${traceId}?limit=5000` });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { events: unknown[]; limit: number; nextCursor: string | null };
    expect(body.limit).toBe(TRACE_PAGE_MAX);
    expect(body.events.length).toBe(TRACE_PAGE_MAX);
    expect(body.nextCursor).toBeTruthy();
    await app.close();
  });
});

describe("Step-4 retention classes and gates", () => {
  it("classifies telemetry, monitor, and audit; durable_transitions is never a journal delete class", () => {
    expect(classifyJournalRetention("MARKET_SNAPSHOT")).toBe("raw_telemetry");
    expect(classifyJournalRetention("TRIGGER_EVENT")).toBe("monitor_control");
    expect(classifyJournalRetention("PAPER_FILL")).toBe("long_lived_audit");
    expect(classifyJournalRetention("EXPERIMENT_COMPLETED")).toBe("long_lived_audit");
    expect(AUDIT_CRITICAL_EVENT_TYPES).toContain("PAPER_FILL");
    expect(TELEMETRY_EVENT_TYPES).not.toContain("PAPER_FILL");
  });

  it("active experiment report-input is not pruned; completed without final report is not pruned; truncated cannot unlock", async () => {
    const journal = new MemoryEventJournal();
    const now = new Date("2026-09-08T00:00:00.000Z");
    await journal.append(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", { kind: "BOOK_IMBALANCE" }, "trig-old"));
    await journal.append(ev("MARKET_SNAPSHOT", "2026-09-01T00:00:00.000Z", { marketId: "m" }, "snap-old"));
    await journal.append(ev("PAPER_FILL", "2026-09-01T00:00:00.000Z", { ...fill("UP", "0.40") }, "fill-old"));
    const manager = new RetentionManager(journal, defaultRetentionConfig({ enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, completedGraceMs: 1 }));
    const active = await manager.runCycle({
      experiment: { id: "exp-1", status: "RUNNING" }, now, degraded: false, failClosed: false, databaseUp: true,
    });
    expect(active.deletedByClass.monitor_control).toBe(0);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).length).toBe(1);
    expect(active.durableTransitionsPruned).toBe(false);
    const incomplete = await manager.runCycle({
      experiment: { id: "exp-1", status: "COMPLETED" }, now, degraded: false, failClosed: false, databaseUp: true,
    });
    expect(incomplete.transientEligible).toBe(false);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).length).toBe(1);
    await journal.persistExperimentReport({
      experimentId: "exp-1", reportKind: "final", schemaVersion: 1, generatedAt: now.toISOString(),
      complete: false, truncated: true, sourceEventCount: 1,
      sourceWindowStart: null, sourceWindowEnd: null, sourceFenceOccurredAt: null, sourceFenceId: null,
      earliestIncludedAt: null, latestIncludedAt: null, canonicalSummary: {}, renderedMarkdown: "x",
      contentHash: "trunc",
    });
    const trunc = await manager.runCycle({
      experiment: { id: "exp-1", status: "COMPLETED" }, now, degraded: false, failClosed: false, databaseUp: true,
    });
    expect(trunc.transientEligible).toBe(false);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).length).toBe(1);
    expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
  });

  it("complete+grace unlocks monitor/telemetry; telemetry delete never removes execution evidence; batches ≤ cap", async () => {
    const journal = new MemoryEventJournal();
    const persistedAt = new Date("2026-09-01T00:00:00.000Z");
    await journal.append(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", { kind: "BOOK_IMBALANCE" }, "trig-old"));
    await journal.append(ev("MARKET_SNAPSHOT", "2026-09-01T00:00:00.000Z", { marketId: "m" }, "snap-old"));
    await journal.append(ev("PAPER_FILL", "2026-09-01T00:00:00.000Z", { ...fill("UP", "0.40") }, "fill-old"));
    await journal.persistExperimentReport({
      experimentId: "exp-1", reportKind: "final", schemaVersion: 1, generatedAt: persistedAt.toISOString(),
      complete: true, truncated: false, sourceEventCount: 3,
      sourceWindowStart: null, sourceWindowEnd: null, sourceFenceOccurredAt: null, sourceFenceId: null,
      earliestIncludedAt: null, latestIncludedAt: null, canonicalSummary: { ok: true }, renderedMarkdown: "ok",
      contentHash: "ok",
    });
    const manager = new RetentionManager(journal, defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, completedGraceMs: 1, batchSize: 200,
    }));
    const result = await manager.runCycle({
      experiment: { id: "exp-1", status: "COMPLETED" },
      now: new Date("2026-09-10T00:00:00.000Z"),
      degraded: false, failClosed: false, databaseUp: true,
    });
    expect(result.transientEligible).toBe(true);
    expect(result.lastBatchSize).toBeLessThanOrEqual(200);
    expect((await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).length).toBe(0);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).length).toBe(0);
    expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
    expect(result.durableTransitionsPruned).toBe(false);
  });

  it("retention failures do not corrupt trading state and skip when degraded", async () => {
    const journal = new MemoryEventJournal();
    journal.available = false;
    const manager = new RetentionManager(journal, defaultRetentionConfig({ enabled: true }));
    const skipped = await manager.runCycle({
      experiment: null, now: new Date(), degraded: true, failClosed: false, databaseUp: true,
    });
    expect(skipped.skipped).toBe(true);
    expect(skipped.skipReason).toBe("degraded");
    journal.available = true;
    await journal.append(ev("PAPER_FILL", "2026-09-01T00:00:00.000Z", { ...fill("UP", "0.40") }, "fill-keep"));
    const down = await manager.runCycle({
      experiment: null, now: new Date(), degraded: false, failClosed: false, databaseUp: false,
    });
    expect(down.skipped).toBe(true);
    expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
  });

  it("dry-run cleanup mutates nothing and APPLY refuses unsafe experiment cleanup", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("MARKET_SNAPSHOT", "2026-09-01T00:00:00.000Z", {}, "snap"));
    await journal.append(ev("PAPER_FILL", "2026-09-01T00:00:00.000Z", { ...fill("UP", "0.40") }, "fill"));
    const dry = await dryRunJournalCleanup(journal, new Date(), { id: "exp-1", status: "RUNNING" });
    expect(dry.mutated).toBe(false);
    expect(dry.refuseUnsafeExperimentCleanup).toBe(true);
    expect(dry.durableTransitions).toBe("PROTECTED_NO_DELETE");
    expect((await journal.queryEvents({})).length).toBe(2);
    const apply = await applyJournalCleanup(journal, {
      experiment: { id: "exp-1", status: "RUNNING" },
      confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP",
    });
    expect(apply.refused).toBe(false);
    expect(apply.durableTransitionsDeleted).toBe(0);
    expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
    expect((await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).length).toBe(0);
  });
});

describe("Step-4 MCP idempotency TTL cannot resurrect stale", () => {
  it("expired key removed still rejects a stale replay", async () => {
    assertMcpIdempotencyTtlSafe(RETENTION_DEFAULTS.mcpIdempotencyTtlMs);
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const engine = new EngineService(qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" }), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(nowMs),
    });
    await engine.start();
    try {
      const trigger = {
        id: "trig-1", traceId: "tr-1", marketId: "btc-mkt", asset: "BTC", kind: "BOOK_IMBALANCE" as const,
        severity: 2 as const, score: 1, dedupeKey: "d", evidence: {}, sourceEventIds: ["e"], source: "polymarket" as const,
        createdAt: new Date(nowMs).toISOString(), expiresAt: new Date(nowMs + 30_000).toISOString(),
      };
      await engine.registerTriggerForResearch(trigger);
      const submission = grokSignalSubmissionSchema.parse({
        submissionId: "sub-1", traceId: "tr-1", triggerId: "trig-1", correlationId: "c", marketId: "btc-mkt",
        asset: "BTC", agentId: "fair_probability_analyst", role: "fair_probability_analyst",
        timestamp: new Date(nowMs).toISOString(),
        output: {
          confidence: 0.8, evidenceIds: ["e"], expiresAt: new Date(nowMs + 30_000).toISOString(),
          reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04,
        },
      });
      expect(await engine.submitGrokSignal(submission, "fair_probability_analyst")).toMatchObject({ status: "accepted" });
      nowMs += 120_000;
      // Memory claim timestamps use wall-clock created_at; prune by that column, not the injected engine clock.
      await journal.pruneIdempotencyKeys(new Date(Date.now() + 60_000), 10);
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", "fair_probability_analyst:sub-1")).toBeNull();
      expect(await engine.submitGrokSignal(submission, "fair_probability_analyst")).toMatchObject({ status: expect.stringMatching(/stale|unknown_trigger|replay_rejected/) });
    } finally {
      await engine.stop();
    }
  });
});

describe("Step-4 checkpoint retention preserves latest usable", () => {
  it("prunes older checkpoints and leaves the latest sequence", async () => {
    const journal = new MemoryEventJournal();
    for (let sequence = 1; sequence <= 6; sequence += 1) {
      await journal.saveCheckpoint({ experimentId: "exp", sequence, payload: { schemaVersion: 2, durableCursor: 0 } });
    }
    const pruned = await journal.pruneOldCheckpoints(2);
    expect(pruned.latestSequence).toBe(6);
    const left = await journal.listCheckpoints();
    expect(left.map((row) => row.sequence).sort((a, b) => a - b)).toEqual([5, 6]);
    expect((await journal.loadLatestCheckpoint())?.sequence).toBe(6);
  });

  it("engine recovers after older checkpoints are pruned", async () => {
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const first = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await first.start();
    await first.beginExperiment();
    await first.stop();
    await journal.pruneOldCheckpoints(1);
    const latest = await journal.loadLatestCheckpoint();
    expect(latest).toBeTruthy();
    const adapter2 = new ScriptedMarketAdapter();
    adapter2.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const second = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter2 as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await second.start();
    try {
      expect(["IDLE", "RUNNING", "ENTRY_CLOSED", "COMPLETED"]).toContain(second.state.experimentStatus);
      expect(second.overloadState()).not.toBe("FAIL_CLOSED");
    } finally {
      await second.stop();
    }
  });
});

describe("Step-4 schema verification fails closed", () => {
  it("missing durable_transitions fails closed; RLS invariants are required on new tables", () => {
    expect(() => evaluateSchemaInspection({
      tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "experiment_reports"],
      indexes: [],
      rlsEnabled: [],
      rlsForced: [],
    })).toThrow(SchemaVerificationError);
    try {
      evaluateSchemaInspection({
        tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "experiment_reports"],
        indexes: [],
        rlsEnabled: [],
        rlsForced: [],
      });
    } catch (error) {
      expect(String(error)).toMatch(/durable_transitions/);
    }
    expect(() => evaluateSchemaInspection({
      tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
      indexes: [
        "journal_trace_idx", "journal_aggregate_idx", "journal_type_time_idx", "journal_experiment_idx",
        "journal_occurred_id_idx", "journal_type_occurred_id_idx", "checkpoint_sequence_idx",
        "mcp_idempotency_scope_key_idx", "durable_transitions_operation_key_uidx", "durable_transitions_sequence_idx",
        "experiment_reports_pkey", "experiment_reports_hash_idx", "experiment_reports_complete_idx",
      ],
      rlsEnabled: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
      rlsForced: [],
    })).not.toThrow();
    expect(() => evaluateSchemaInspection({
      tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
      indexes: [
        "journal_trace_idx", "journal_aggregate_idx", "journal_type_time_idx", "journal_experiment_idx",
        "journal_occurred_id_idx", "journal_type_occurred_id_idx", "checkpoint_sequence_idx",
        "mcp_idempotency_scope_key_idx", "durable_transitions_operation_key_uidx", "durable_transitions_sequence_idx",
        "experiment_reports_pkey", "experiment_reports_hash_idx", "experiment_reports_complete_idx",
      ],
      rlsEnabled: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
      rlsForced: [],
      invalidIndexes: ["journal_occurred_id_idx"],
    })).toThrow(/invalid index journal_occurred_id_idx/);
  });
});

describe("Step-4 PR #12 bounds unchanged", () => {
  it("keeps supervisor and journal admission constants", () => {
    expect(SUPERVISOR_BOUNDS.trigger.p0).toEqual({ concurrency: 4, queueLimit: 256, highWater: 192 });
    expect(SUPERVISOR_BOUNDS.trigger.p3).toEqual({ concurrency: 4, queueLimit: 1024, highWater: 768 });
    expect(SUPERVISOR_BOUNDS.journal).toEqual({
      active: 4, noncriticalActive: 3, waiting: 128, highWater: 96, criticalWaiting: 32, noncriticalWaiting: 96,
    });
    expect(SUPERVISOR_BOUNDS.heavyRead).toEqual({ concurrency: 2, waiting: 16 });
    expect(ENGINE_BOUNDS).toBe(SUPERVISOR_BOUNDS);
    expect(EVENT_PAGE_SIZE).toBe(500);
    expect(experimentIsActive({ status: "RUNNING" })).toBe(true);
    expect(MONITOR_CONTROL_EVENT_TYPES.length).toBeGreaterThan(0);
    expect(DECISION_EVENT_TYPES).toContain("EXPERIMENT_REPORT_GENERATED");
  });
});

describe("Step-4 acceptance: operator access never migrates", () => {
  it("dry-run and readonly access cannot invoke migrate or mutating SQL", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("MARKET_SNAPSHOT", "2026-09-01T00:00:00.000Z", {}, "snap"));
    journal.mutations.length = 0;
    journal.migrateInvocations = 0;
    await journal.connectWithoutMigrate("readonly");
    await expect(journal.initialize()).rejects.toBeInstanceOf(JournalMigrateForbiddenError);
    expect(journal.migrateInvocations).toBe(0);
    const dry = await dryRunJournalCleanup(journal, new Date("2026-09-08T00:00:00.000Z"));
    expect(dry.mutated).toBe(false);
    expect(journal.migrateInvocations).toBe(0);
    expect(journal.mutations).toEqual([]);
    expect(dry.telemetry.eligible).toBeGreaterThanOrEqual(1);
    expect(dry.telemetry.cutoff).toBeTruthy();
    expect(dry.durableTransitions).toBe("PROTECTED_NO_DELETE");
    await expect(journal.deleteJournalEventsBounded({
      types: TELEMETRY_EVENT_TYPES,
      occurredBefore: new Date(),
      limit: 10,
    })).rejects.toThrow(/JOURNAL_MUTATE_FORBIDDEN/);
    const pg = new PostgresEventJournal("postgres://operator:operator@127.0.0.1:1/none", "/tmp", { access: "readonly" });
    await expect(pg.initialize()).rejects.toBeInstanceOf(JournalMigrateForbiddenError);
    expect(pg.migrateInvocations).toBe(0);
    await pg.close().catch(() => undefined);
  });
});

describe("Step-4 acceptance: operator report CLI gates", () => {
  it("persists a canonical final only for the proven COMPLETED experiment", async () => {
    const journal = new MemoryEventJournal();
    await journal.appendMany(fixtureEvents());
    await journal.append(ev("EXPERIMENT_STARTED", "2026-08-01T00:00:00.000Z", { experimentId: "exp-old", aggregateId: "exp-old" }, "old-start"));
    const dir = await mkdtemp(join(tmpdir(), "pm-op-report-"));
    const running = await generateOperatorReport(journal, { experimentId: "exp-1", reportDirectory: dir, allowPersist: true });
    // fixture has COMPLETED
    expect(running.persisted).toBe(true);
    expect(running.status).toBe("COMPLETED");
    const runningJournal = new MemoryEventJournal();
    await runningJournal.append(ev("EXPERIMENT_STARTED", "2026-09-01T00:00:00.000Z", { aggregateId: "exp-run" }, "s"));
    const run = await generateOperatorReport(runningJournal, { experimentId: "exp-run", reportDirectory: dir, allowPersist: true });
    expect(run.persisted).toBe(false);
    expect(run.reason).toMatch(/NOT_COMPLETED:RUNNING/);
    expect(await runningJournal.loadExperimentReport("exp-run")).toBeNull();
    const closed = new MemoryEventJournal();
    await closed.append(ev("EXPERIMENT_STARTED", "2026-09-01T00:00:00.000Z", { aggregateId: "exp-c" }, "cs"));
    await closed.append(ev("EXPERIMENT_ENTRY_CLOSED", "2026-09-01T01:00:00.000Z", { aggregateId: "exp-c" }, "cc"));
    const entry = await generateOperatorReport(closed, { experimentId: "exp-c", reportDirectory: dir, allowPersist: true });
    expect(entry.persisted).toBe(false);
    expect(entry.reason).toMatch(/ENTRY_CLOSED/);
    const missing = await generateOperatorReport(journal, { experimentId: "exp-missing", reportDirectory: dir, allowPersist: true });
    expect(missing.persisted).toBe(false);
    expect(missing.reason).toBe("EXPERIMENT_IDENTITY_MISSING");
    const resolved = await resolveAuthoritativeExperiment(journal, "exp-1");
    expect(resolved?.startedAt).toBe("2026-09-01T00:00:00.000Z");
    expect(resolved?.startedAt).not.toBe("2026-08-01T00:00:00.000Z");
    const isolated = new MemoryEventJournal();
    await isolated.append(ev("ENGINE_STARTED", "2026-07-01T00:00:00.000Z", {}, "eng"));
    await isolated.append(ev("EXPERIMENT_STARTED", "2026-09-01T00:00:00.000Z", { experimentId: "exp-b", aggregateId: "exp-b" }, "b-start"));
    await isolated.append(ev("EXPERIMENT_COMPLETED", "2026-09-01T02:00:00.000Z", { experimentId: "exp-b", aggregateId: "exp-b" }, "b-done"));
    const own = await resolveAuthoritativeExperiment(isolated, "exp-b");
    expect(own?.startedAt).toBe("2026-09-01T00:00:00.000Z");
    expect(own?.startedAt).not.toBe("2026-07-01T00:00:00.000Z");
    const conflictJournal = new MemoryEventJournal();
    await conflictJournal.appendMany(fixtureEvents());
    await conflictJournal.persistExperimentReport({
      experimentId: "exp-1", reportKind: "final", schemaVersion: 1, generatedAt: "2026-09-01T00:00:00.000Z",
      complete: true, truncated: false, sourceEventCount: 1,
      sourceWindowStart: null, sourceWindowEnd: null, sourceFenceOccurredAt: null, sourceFenceId: null,
      earliestIncludedAt: null, latestIncludedAt: null, canonicalSummary: { other: true }, renderedMarkdown: "other",
      contentHash: "not-the-scan-hash",
    });
    await expect(generateOperatorReport(conflictJournal, { experimentId: "exp-1", reportDirectory: dir, allowPersist: true }))
      .rejects.toBeInstanceOf(ExperimentReportConflictError);
  });
});

describe("Step-4 acceptance: commit-stable source fence", () => {
  it("excludes an event created before the fence but committed after capture", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("RISK_EVALUATION", "2026-09-01T00:00:00.000Z", { accepted: true, marketId: "m" }, "early"));
    const delayed = ev("RISK_EVALUATION", "2026-08-31T23:59:59.000Z", { accepted: true, marketId: "m" }, "delayed");
    journal.stageUncommitted(delayed);
    const ids: string[] = [];
    const stats = await scanJournalEvents(journal, { types: ["RISK_EVALUATION"] }, {
      pageSize: 1,
      onPage: async (page) => {
        ids.push(...page.map((item) => item.id));
        journal.commitStaged("delayed");
        await journal.append(ev("RISK_EVALUATION", "2026-09-01T00:00:02.000Z", { accepted: true, marketId: "m" }, "late-audit"));
      },
    });
    expect(ids).toEqual(["early"]);
    expect(stats.fence.stable).toBe(true);
    expect(stats.complete).toBe(true);
    const again = await scanJournalEvents(journal, { types: ["RISK_EVALUATION"] });
    expect(again.scanned).toBe(3);
    expect(again.fence.stable).toBe(true);
  });
});

describe("Step-4 acceptance: monitor-only and drain", () => {
  it("monitor-only / no experiment allows monitor TTL; active protects in-window; completed needs final+grace", async () => {
    const journal = new MemoryEventJournal();
    const now = new Date("2026-09-08T00:00:00.000Z");
    await journal.append(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", {}, "pre"));
    const manager = new RetentionManager(journal, defaultRetentionConfig({ enabled: true, monitorTtlMs: 1, telemetryTtlMs: 1, completedGraceMs: 1, batchSize: 50, maxBatchesPerCycle: 8, maxRowsPerCycle: 400 }));
    expect(experimentIsMonitorOnly(null)).toBe(true);
    const idle = await manager.runCycle({ experiment: null, now, degraded: false, failClosed: false, databaseUp: true });
    expect(idle.monitorOnlyEligible).toBe(true);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).length).toBe(0);
    await journal.append(ev("TRIGGER_EVENT", "2026-09-07T00:00:00.000Z", {}, "in-window"));
    await journal.append(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", {}, "pre2"));
    const active = await manager.runCycle({
      experiment: { id: "exp-1", status: "RUNNING", startedAt: "2026-09-06T00:00:00.000Z" },
      now, degraded: false, failClosed: false, databaseUp: true,
    });
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).map((item) => item.id)).toEqual(["in-window"]);
    expect(active.deletedByClass.monitor_control).toBe(1);
    await journal.append(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", {}, "done-old"));
    const completed = await manager.runCycle({
      experiment: { id: "exp-1", status: "COMPLETED", startedAt: "2026-09-06T00:00:00.000Z" },
      now, degraded: false, failClosed: false, databaseUp: true,
    });
    expect(completed.transientEligible).toBe(false);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).some((item) => item.id === "done-old")).toBe(true);
    const apply = await applyJournalCleanup(journal, { now, experiment: null, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP" });
    expect(apply.refused).toBe(false);
    expect(apply.deletedByClass.monitor_control).toBeGreaterThanOrEqual(1);
  });

  it("multi-batch drain converges an eligible backlog instead of growing unbounded", async () => {
    const journal = new MemoryEventJournal();
    const manager = new RetentionManager(journal, defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 60_000, monitorTtlMs: 60_000, batchSize: 20,
      maxBatchesPerCycle: 5, maxRowsPerCycle: 100, cycleBudgetMs: 5_000,
    }));
    let now = Date.parse("2026-09-01T00:00:00.000Z");
    const eligible: number[] = [];
    for (let cycle = 0; cycle < 12; cycle += 1) {
      now += 60_000;
      for (let i = 0; i < 80; i += 1) {
        await journal.append(ev("MARKET_SNAPSHOT", new Date(now - 90_000).toISOString(), { i }, `tel-${cycle}-${i}`));
      }
      const result = await manager.runCycle({
        experiment: null, now: new Date(now), degraded: false, failClosed: false, databaseUp: true,
      });
      eligible.push(result.eligibleBacklog);
      expect(result.deletedByClass.raw_telemetry).toBeLessThanOrEqual(100);
      expect(result.batchCount).toBeLessThanOrEqual(5 + 4);
    }
    const tail = eligible.slice(-4);
    const first = eligible[0]!;
    expect(Math.max(...tail)).toBeLessThan(first * 3);
    expect(Math.max(...tail) - Math.min(...tail)).toBeLessThan(200);
  });
});

describe("Step-4 acceptance: retention opt-in, persist races, audit repair, MCP keying", () => {
  it("missing RETENTION_ENABLED does not enable automatic journal deletion", async () => {
    const parsed = engineEnvSchema.parse({});
    expect(parsed.RETENTION_ENABLED).toBe(false);
    const journal = new MemoryEventJournal();
    await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", {}, "snap"));
    const manager = new RetentionManager(journal, defaultRetentionConfig());
    const result = await manager.runCycle({
      experiment: null, now: new Date(), degraded: false, failClosed: false, databaseUp: true,
    });
    expect(result.skipped).toBe(true);
    expect(result.skipReason).toBe("disabled");
    expect((await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).length).toBe(1);
  });

  it("concurrent same-hash persist is idempotent; conflicting hash fails closed", async () => {
    const journal = new MemoryEventJournal();
    const input = {
      experimentId: "exp-1", reportKind: "final" as const, schemaVersion: 1, generatedAt: "2026-09-01T00:00:00.000Z",
      complete: true, truncated: false, sourceEventCount: 1,
      sourceWindowStart: null, sourceWindowEnd: null, sourceFenceOccurredAt: null, sourceFenceId: null,
      earliestIncludedAt: null, latestIncludedAt: null, canonicalSummary: { a: 1 }, renderedMarkdown: "ok",
      contentHash: "abc",
    };
    const [a, b] = await Promise.all([journal.persistExperimentReport(input), journal.persistExperimentReport(input)]);
    expect([a.inserted, b.inserted].sort()).toEqual([false, true]);
    expect(a.report.contentHash).toBe("abc");
    const conflict = persistReportConflictSafe({ ...input, contentHash: "zzz", canonicalSummary: { a: 2 } }, {
      insert: async () => "conflict",
      load: async () => ({ ...input, persistedAt: "2026-09-01T00:00:00.000Z" }),
    });
    await expect(conflict).rejects.toBeInstanceOf(ExperimentReportConflictError);
  });

  it("repairs EXPERIMENT_REPORT_GENERATED exactly once after persist-then-audit failure", async () => {
    const journal = new MemoryEventJournal();
    await journal.appendMany(fixtureEvents());
    const streamed = await streamExperimentSummary(journal, {
      id: "exp-1", startedAt: "2026-09-01T00:00:00.000Z", completedAt: "2026-09-01T00:00:08.000Z",
    }, { reportKind: "final", complete: true, truncated: false, generatedAt: "2026-09-01T12:00:00.000Z" });
    streamed.summary.complete = true;
    streamed.summary.truncated = false;
    const persisted = await persistCanonicalFinalReport(journal, { summary: streamed.summary, scan: streamed.scan, experimentStatus: "COMPLETED" });
    expect(persisted).toBeTruthy();
    journal.failAppendTypes.add("EXPERIMENT_REPORT_GENERATED");
    await expect(ensureReportGeneratedAudit(journal, persisted!.report)).rejects.toThrow(/JOURNAL_APPEND_FAILED/);
    journal.failAppendTypes.delete("EXPERIMENT_REPORT_GENERATED");
    const first = await ensureReportGeneratedAudit(journal, persisted!.report);
    const second = await ensureReportGeneratedAudit(journal, persisted!.report);
    expect(first.repaired).toBe(true);
    expect(second.repaired).toBe(false);
    expect(first.id).toBe(reportGeneratedAuditId("exp-1", persisted!.report.contentHash));
    const audits = await journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] });
    expect(audits.filter((item) => item.id === first.id).length).toBe(1);
  });

  it("complete-metrics flights are keyed; experiment B does not receive A", async () => {
    resetStrategyMetricsCacheForTest();
    const flights = new KeyedResultFlight<string>();
    let releaseA!: (value: string) => void;
    const a = flights.run("exp-A|start|", () => new Promise<string>((resolve) => { releaseA = resolve; }));
    const b = flights.run("exp-B|start|", async () => "summary-B");
    expect(await b).toBe("summary-B");
    releaseA("summary-A");
    expect(await a).toBe("summary-A");
    expect(completeMetricsFlightKey("exp-A", "s", "c")).not.toBe(completeMetricsFlightKey("exp-B", "s", "c"));
  });

  it("accumulator retained state follows entity cardinality, not event count", () => {
    const acc = new ExperimentSummaryAccumulator("exp-1", { reportKind: "final" });
    for (let i = 0; i < 2_000; i += 1) {
      acc.fold(ev("RISK_EVALUATION", "2026-09-01T00:00:00.000Z", { accepted: true, marketId: `m-${i % 6}` }, `r-${i}`));
    }
    expect(acc.sourceEventCount).toBe(2_000);
    expect(acc.retainedEventCount).toBe(0);
    expect(acc.retainedEntityCardinality).toBeLessThan(20);
    expect(acc.highWaterMarkBytes).toBeGreaterThan(0);
  });
});

