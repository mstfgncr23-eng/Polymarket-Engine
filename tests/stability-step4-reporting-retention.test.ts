import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  AUDIT_CRITICAL_EVENT_TYPES,
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  classifyReportInput,
  DECISION_EVENT_TYPES,
  OPERATIONAL_MONITOR_EVENT_TYPES,
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
  triggerEventSchema,
  type JournalEvent,
} from "@pm/core";
import {
  ExperimentReportConflictError,
  JournalMigrateForbiddenError,
  JournalMutateForbiddenError,
  MemoryEventJournal,
  PostgresEventJournal,
  createJournalEvent,
  evaluateSchemaInspection,
  inspectPublicSchema,
  hashExperimentReport,
  verifyRequiredSchema,
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
import { resolveCleanupLifecycle, seedDurableLifecycle } from "../apps/engine/src/cleanup-lifecycle.js";
import {
  boundIntegrityFaults,
  INTEGRITY_OVERFLOW_EVENT_ID,
  REPORT_SOURCE_INTEGRITY_FAULT,
  REPORT_SOURCE_INTEGRITY_HARD_CAP,
  REPORT_SOURCE_INTEGRITY_REPAIRED,
  shouldLatchReportSourceIntegrity,
} from "../apps/engine/src/report-source-integrity.js";
import { isSourceSealExemptType } from "../apps/engine/src/source-seal.js";
import { isCriticalPersistenceType } from "../apps/engine/src/runtime-supervisor.js";
import { readFileSync } from "node:fs";
import { BoundedJournalWriter } from "../apps/engine/src/journal-writer.js";
import { generateOperatorReport } from "../apps/engine/src/operator-report.js";
import { defaultRetentionConfig, experimentIsActive, experimentIsMonitorOnly, RetentionManager } from "../apps/engine/src/retention-manager.js";
import { loadSourceSeal } from "../apps/engine/src/source-seal.js";
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
    ev("EXPERIMENT_SOURCE_SEALED", "2026-09-01T00:00:09.000Z", { experimentId: "exp-1", sealed: true }, "sourceseal:exp-1"),
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
      invalidIndexes: [],
    })).toThrow(SchemaVerificationError);
    try {
      evaluateSchemaInspection({
        tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "experiment_reports"],
        indexes: [],
        rlsEnabled: [],
        rlsForced: [],
        invalidIndexes: [],
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
      invalidIndexes: [],
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
    })).rejects.toBeInstanceOf(JournalMutateForbiddenError);
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
    const unsealed = new MemoryEventJournal();
    await unsealed.appendMany(fixtureEvents().filter((event) => event.type !== "EXPERIMENT_SOURCE_SEALED"));
    const noSeal = await generateOperatorReport(unsealed, { experimentId: "exp-1", reportDirectory: dir, allowPersist: true });
    expect(noSeal.persisted).toBe(false);
    expect(noSeal.reason).toBe("SOURCE_SEAL_MISSING");
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
    const hwm = Math.max(...eligible);
    const plateau = Math.max(...tail);
    expect(plateau).toBeLessThan(first * 3);
    expect(Math.max(...tail) - Math.min(...tail)).toBeLessThan(200);
    expect(hwm).toBeLessThan(250);
    expect(plateau).toBeLessThanOrEqual(120);
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

async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!(await predicate())) {
    if (Date.now() - started > timeoutMs) throw new Error("WAIT_TIMEOUT");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe("Step-4 acceptance: application-level source seal", () => {
  it("holds a pre-completion TRIGGER_EVENT until the source seal drains", async () => {
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      const started = await engine.beginExperiment();
      expect(started).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      engine.setSourceSealTimeoutForTest(80);
      let release!: () => void;
      engine.setTriggerEventPersistHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
      const trigger = triggerEventSchema.parse({
        id: "trig-seal-1", traceId: "tr-seal-1", marketId: "btc-mkt", asset: "BTC",
        kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "seal", evidence: {},
        sourceEventIds: ["e-seal"], source: "polymarket",
        createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 30_000).toISOString(),
      });
      engine.enqueueTriggerStormForTest([trigger]);
      await waitUntil(() => engine.journalAdmissionSnapshot().active > 0);
      await engine.completeExperimentForTest();
      await engine.writeFinalReportForTest();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(engine.reportGeneratedForTest()).toBe(false);
      release();
      engine.setTriggerEventPersistHoldForTest(null);
      engine.setSourceSealTimeoutForTest(5_000);
      await waitUntil(async () => (await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).some((event) => event.payload.id === "trig-seal-1"));
      await engine.writeFinalReportForTest();
      expect(await loadSourceSeal(journal, experimentId)).toBeTruthy();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      const canonical = first!.canonicalSummary as {
        status?: string; closePolicy?: string; unresolvedPositionCount?: number; metricsScope?: string;
      };
      const streamed = await streamExperimentSummary(journal, {
        id: experimentId,
        startedAt: engine.experimentManifest()?.startedAt,
        completedAt: engine.experimentManifest()?.completedAt,
      }, {
        status: canonical.status,
        closePolicy: canonical.closePolicy,
        reportKind: "final",
        unresolvedPositionCount: canonical.unresolvedPositionCount,
        metricsScope: "complete",
        complete: true,
        truncated: false,
        generatedAt: first!.generatedAt,
      });
      streamed.summary.complete = true;
      streamed.summary.truncated = false;
      expect(toDurableReportInput({ summary: streamed.summary, scan: streamed.scan }).contentHash).toBe(first!.contentHash);
      expect(streamed.scan.scanned).toBe(first!.sourceEventCount);
      expect(streamed.scan.fence.through).toEqual({
        occurredAt: first!.sourceFenceOccurredAt,
        id: first!.sourceFenceId,
      });
      expect((await journal.queryEvents({
        types: ["TRIGGER_EVENT"],
        from: engine.experimentManifest()?.startedAt ? new Date(engine.experimentManifest()!.startedAt!) : undefined,
        to: engine.experimentManifest()?.completedAt ? new Date(engine.experimentManifest()!.completedAt!) : undefined,
      })).some((event) => event.payload.id === "trig-seal-1")).toBe(true);
      await engine.stop();
      const adapter2 = new ScriptedMarketAdapter();
      adapter2.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
      const restarted = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
        market: adapter2 as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
      });
      await restarted.start();
      try {
        const recovered = await journal.loadExperimentReport(experimentId);
        expect(recovered?.contentHash).toBe(first!.contentHash);
        await restarted.writeFinalReportForTest();
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(first!.contentHash);
      } finally {
        await restarted.stop();
      }
    } finally {
      if (!engine.state.draining) await engine.stop();
    }
  });

  it("source-seal timeout refuses a canonical final", async () => {
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    let release!: () => void;
    try {
      const started = await engine.beginExperiment();
      expect(started).toMatchObject({ ok: true });
      engine.setSourceSealTimeoutForTest(40);
      engine.setTriggerEventPersistHoldForTest(new Promise<void>((resolve) => { release = resolve; }));
      engine.enqueueTriggerStormForTest([triggerEventSchema.parse({
        id: "trig-timeout", traceId: "tr-timeout", marketId: "btc-mkt", asset: "BTC",
        kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "timeout", evidence: {},
        sourceEventIds: ["e-timeout"], source: "polymarket",
        createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 30_000).toISOString(),
      })]);
      await waitUntil(() => engine.journalAdmissionSnapshot().active > 0);
      await engine.completeExperimentForTest();
      const seal = await engine.ensureSourceSealForTest();
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("SOURCE_SEAL_TIMEOUT");
      expect(await journal.loadExperimentReport(engine.experimentManifest()!.id)).toBeNull();
      expect(await loadSourceSeal(journal, engine.experimentManifest()!.id)).toBeNull();
    } finally {
      release?.();
      engine.setTriggerEventPersistHoldForTest(null);
      await engine.stop();
    }
  });
});

describe("Step-4 acceptance: journal access capability matrix", () => {
  it("rejects every mutating method under readonly and verify-only", async () => {
    const report = {
      experimentId: "exp-1", reportKind: "final" as const, schemaVersion: 1, generatedAt: "2026-09-01T00:00:00.000Z",
      complete: true, truncated: false, sourceEventCount: 1,
      sourceWindowStart: null, sourceWindowEnd: null, sourceFenceOccurredAt: null, sourceFenceId: null,
      earliestIncludedAt: null, latestIncludedAt: null, canonicalSummary: {}, renderedMarkdown: "x",
      contentHash: "x",
    };
    const event = ev("MARKET_SNAPSHOT", "2026-09-01T00:00:00.000Z", {}, "snap-mut");
    for (const mode of ["readonly", "verify-only"] as const) {
      const journal = new MemoryEventJournal();
      await journal.connectWithoutMigrate(mode);
      await expect(journal.append(event)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.appendMany([event])).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.claimIdempotency("s", "k")).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.persistExperimentReport(report)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.deleteJournalEventsBounded({ types: TELEMETRY_EVENT_TYPES, occurredBefore: new Date(), limit: 1 }))
        .rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.pruneIdempotencyKeys(new Date(), 1)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.pruneOldCheckpoints(1)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.saveCheckpoint({ experimentId: "e", sequence: 1, payload: {} }))
        .rejects.toBeInstanceOf(JournalMutateForbiddenError);
      await expect(journal.commitDurableTransition({
        operationKey: "op", type: "EXPERIMENT_STARTED", aggregateType: "experiment", aggregateId: "e", payload: {},
      }, 0)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
    }
    const writer = new MemoryEventJournal();
    await writer.connectWithoutMigrate("report-write");
    await expect(writer.append(event)).rejects.toBeInstanceOf(JournalMutateForbiddenError);
    await expect(writer.deleteJournalEventsBounded({ types: TELEMETRY_EVENT_TYPES, occurredBefore: new Date(), limit: 1 }))
      .rejects.toBeInstanceOf(JournalMutateForbiddenError);
    await expect(writer.persistExperimentReport(report)).resolves.toMatchObject({ inserted: true });
  });
});

describe("Step-4 acceptance: hard maxRowsPerCycle and effective retention config", () => {
  it("never deletes more than maxRowsPerCycle for non-divisible budgets", async () => {
    const seed = async (journal: MemoryEventJournal, n: number) => {
      for (let i = 0; i < n; i += 1) {
        await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", { i }, `cap-${n}-${i}`));
      }
    };
    const run = async (batchSize: number, maxRowsPerCycle: number) => {
      const journal = new MemoryEventJournal();
      await seed(journal, 400);
      const manager = new RetentionManager(journal, defaultRetentionConfig({
        enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, batchSize, maxRowsPerCycle,
        maxBatchesPerCycle: 20, cycleBudgetMs: 10_000,
      }));
      return manager.runCycle({ experiment: null, now: new Date(), degraded: false, failClosed: false, databaseUp: true });
    };
    const a = await run(200, 250);
    expect(a.deletedByClass.raw_telemetry).toBeLessThanOrEqual(250);
    expect(a.deletedByClass.raw_telemetry).toBe(250);
    const b = await run(200, 350);
    expect(b.deletedByClass.raw_telemetry).toBe(350);
    const c = await run(200, 100);
    expect(c.deletedByClass.raw_telemetry).toBe(100);
  });

  it("dry-run cutoffs match RetentionManager for a non-default config", async () => {
    const journal = new MemoryEventJournal();
    const now = new Date("2026-09-08T00:00:00.000Z");
    await journal.append(ev("MARKET_SNAPSHOT", "2026-09-07T21:00:00.000Z", {}, "tel-2h"));
    await journal.append(ev("TRIGGER_EVENT", "2026-09-07T10:00:00.000Z", {}, "mon-12h"));
    const config = defaultRetentionConfig({
      enabled: true,
      telemetryTtlMs: 2 * 60 * 60_000,
      monitorTtlMs: 12 * 60 * 60_000,
      completedGraceMs: 2 * 24 * 60 * 60_000,
      batchSize: 50,
      maxBatchesPerCycle: 7,
      maxRowsPerCycle: 150,
    });
    const dry = await dryRunJournalCleanup(journal, now, null, config);
    expect(dry.effectiveConfig.telemetryTtlMs).toBe(2 * 60 * 60_000);
    expect(dry.effectiveConfig.monitorTtlMs).toBe(12 * 60 * 60_000);
    expect(dry.effectiveConfig.completedGraceMs).toBe(2 * 24 * 60 * 60_000);
    expect(dry.effectiveConfig.batchSize).toBe(50);
    expect(dry.telemetry.cutoff).toBe(new Date(now.getTime() - config.telemetryTtlMs).toISOString());
    expect(dry.monitor.cutoff).toBe(new Date(now.getTime() - config.monitorTtlMs).toISOString());
    expect(dry.telemetry.eligible).toBe(1);
    expect(dry.monitor.eligible).toBe(1);
    const manager = new RetentionManager(journal, config);
    const cycle = await manager.runCycle({ experiment: null, now, degraded: false, failClosed: false, databaseUp: true });
    expect(cycle.eligibleBacklog).toBe(dry.telemetry.eligible + dry.monitor.eligible);
    const applyJournal = new MemoryEventJournal();
    await applyJournal.append(ev("MARKET_SNAPSHOT", "2026-09-07T21:00:00.000Z", {}, "tel-2h"));
    await applyJournal.append(ev("TRIGGER_EVENT", "2026-09-07T10:00:00.000Z", {}, "mon-12h"));
    const apply = await applyJournalCleanup(applyJournal, {
      now, experiment: null, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config,
    });
    expect(apply.deletedByClass.raw_telemetry).toBe(1);
    expect(apply.deletedByClass.monitor_control).toBe(1);
  });
});

describe("Step-4 acceptance: invalid index inspection fail-closed", () => {
  const required = {
    tables: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
    indexes: [
      "journal_trace_idx", "journal_aggregate_idx", "journal_type_time_idx", "journal_experiment_idx",
      "journal_occurred_id_idx", "journal_type_occurred_id_idx", "checkpoint_sequence_idx",
      "mcp_idempotency_scope_key_idx", "durable_transitions_operation_key_uidx", "durable_transitions_sequence_idx",
      "experiment_reports_pkey", "experiment_reports_hash_idx", "experiment_reports_complete_idx",
    ],
    rlsEnabled: ["journal_events", "engine_checkpoints", "mcp_idempotency_keys", "durable_transitions", "experiment_reports"],
    rlsForced: [] as string[],
  };

  it("fails when a required index is missing", () => {
    expect(() => evaluateSchemaInspection({
      ...required,
      indexes: required.indexes.filter((name) => name !== "journal_occurred_id_idx"),
      invalidIndexes: [],
    })).toThrow(/missing index journal_occurred_id_idx/);
  });

  it("fails when a required index is INVALID", () => {
    expect(() => evaluateSchemaInspection({
      ...required,
      invalidIndexes: ["journal_occurred_id_idx"],
    })).toThrow(/invalid index journal_occurred_id_idx/);
  });

  it("fails closed when index-validity inspection throws", async () => {
    await expect(inspectPublicSchema(async (sql) => {
      if (sql.includes("indisvalid")) throw new Error("pg_index unavailable");
      return [];
    })).rejects.toThrow(/pg_index unavailable/);
    await expect(verifyRequiredSchema(async (sql) => {
      if (sql.includes("indisvalid")) throw new Error("pg_index unavailable");
      return [];
    })).rejects.toThrow(/pg_index unavailable/);
    expect(() => evaluateSchemaInspection(required)).toThrow(/index validity not inspected/);
  });

  it("passes when every required index is present and valid", () => {
    expect(() => evaluateSchemaInspection({ ...required, invalidIndexes: [] })).not.toThrow();
  });
});

describe("Step-4 residual: experiment research ingress fence", () => {
  it("held Grok submission is drained or rejected and cannot accept after the source seal", async () => {
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    let releaseGrok!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      const now = Date.now();
      const trigger = triggerEventSchema.parse({
        id: "trig-grok-seal", traceId: "tr-grok-seal", marketId: "btc-mkt", asset: "BTC",
        kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "grok-seal", evidence: {},
        sourceEventIds: ["e-grok-seal"], source: "polymarket",
        createdAt: new Date(now).toISOString(), expiresAt: new Date(now + 60_000).toISOString(),
      });
      await engine.registerTriggerForResearch(trigger);
      const submission = grokSignalSubmissionSchema.parse({
        submissionId: "sub-grok-seal", traceId: "tr-grok-seal", triggerId: "trig-grok-seal",
        correlationId: "c-grok-seal", marketId: "btc-mkt", asset: "BTC",
        agentId: "fair_probability_analyst", role: "fair_probability_analyst",
        timestamp: new Date(now).toISOString(),
        output: {
          confidence: 0.8, evidenceIds: ["e"], expiresAt: new Date(now + 60_000).toISOString(),
          reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04,
        },
      });
      engine.setGrokClaimHoldForTest(new Promise<void>((resolve) => { releaseGrok = resolve; }));
      const grokPromise = engine.submitGrokSignal(submission, "fair_probability_analyst");
      await waitUntil(() => engine.researchIngressInFlightForTest() > 0);
      await engine.completeExperimentForTest();
      expect(engine.sourceSealStateForTest().sealing).toBe(true);
      const sealPromise = engine.ensureSourceSealForTest();
      await waitUntil(() => engine.sourceSealStateForTest().generation > 0);
      expect(engine.researchIngressInFlightForTest()).toBeGreaterThan(0);
      releaseGrok();
      engine.setGrokClaimHoldForTest(null);
      const [grok, seal] = await Promise.all([grokPromise, sealPromise]);
      expect(grok.status).toBe("source_sealed");
      expect(seal.sealed).toBe(true);
      expect(await loadSourceSeal(journal, experimentId)).toBeTruthy();
      expect((await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).length).toBe(0);
      expect((await journal.queryEvents({ types: ["STRATEGY_CHANGE_PROPOSED"] })).length).toBe(0);
      expect((await journal.queryEvents({ types: ["CONSENSUS_DECISION"] })).length).toBe(0);
      expect((await journal.queryEvents({ types: ["RISK_EVALUATION"] })).length).toBe(0);
      await engine.writeFinalReportForTest();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      const streamed = await streamExperimentSummary(journal, {
        id: experimentId,
        startedAt: engine.experimentManifest()?.startedAt,
        completedAt: engine.experimentManifest()?.completedAt,
      }, {
        status: "COMPLETED",
        reportKind: "final",
        metricsScope: "complete",
        complete: true,
        truncated: false,
        generatedAt: first!.generatedAt,
      });
      streamed.summary.complete = true;
      streamed.summary.truncated = false;
      expect(toDurableReportInput({ summary: streamed.summary, scan: streamed.scan }).contentHash).toBe(first!.contentHash);
    } finally {
      releaseGrok?.();
      engine.setGrokClaimHoldForTest(null);
      await engine.stop();
    }
  });
});

describe("Step-4 residual: source-seal persist deadline", () => {
  it("PRE-APPEND HOLD: stalls SOURCE_SEAL persistence itself and cannot later create a seal after timeout", async () => {
    const journal = new MemoryEventJournal();
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    let releasePersist!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      engine.setSourceSealTimeoutForTest(40);
      engine.setSourceSealPersistHoldForTest(new Promise<void>((resolve) => { releasePersist = resolve; }));
      await engine.completeExperimentForTest();
      const seal = await engine.ensureSourceSealForTest();
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("SOURCE_SEAL_TIMEOUT");
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      releasePersist();
      engine.setSourceSealPersistHoldForTest(null);
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      engine.setSourceSealTimeoutForTest(5_000);
      const retry = await engine.ensureSourceSealForTest();
      expect(retry.sealed).toBe(true);
      expect(await loadSourceSeal(journal, experimentId)).toBeTruthy();
      await engine.writeFinalReportForTest();
      expect(await journal.loadExperimentReport(experimentId)).toMatchObject({ complete: true, truncated: false });
    } finally {
      releasePersist?.();
      engine.setSourceSealPersistHoldForTest(null);
      await engine.stop();
    }
  });
});

describe("Step-4 residual: checkpoint prune honors remaining cycle rows", () => {
  async function seedCheckpoints(journal: MemoryEventJournal, n: number): Promise<void> {
    for (let sequence = 1; sequence <= n; sequence += 1) {
      await journal.saveCheckpoint({ experimentId: "exp", sequence, payload: { schemaVersion: 2, durableCursor: 0 } });
    }
  }

  it("deletes at most the remaining 50-row cycle budget from a 200+ checkpoint backlog", async () => {
    const journal = new MemoryEventJournal();
    await seedCheckpoints(journal, 220);
    const manager = new RetentionManager(journal, defaultRetentionConfig({
      enabled: true, checkpointKeep: 1, batchSize: 200, maxRowsPerCycle: 50,
      maxBatchesPerCycle: 8, cycleBudgetMs: 10_000, telemetryTtlMs: 1, monitorTtlMs: 1,
    }));
    const result = await manager.runCycle({
      experiment: null, now: new Date(), degraded: false, failClosed: false, databaseUp: true,
    });
    expect(result.deletedByClass.checkpoint).toBeLessThanOrEqual(50);
    expect(result.deletedByClass.checkpoint).toBe(50);
    const left = await journal.listCheckpoints();
    expect(left.some((row) => row.sequence === 220)).toBe(true);
    expect((await journal.loadLatestCheckpoint())?.sequence).toBe(220);
    expect(left.length).toBe(170);
  });

  it("after telemetry consumes part of the budget, checkpoint deletes only the remainder", async () => {
    const journal = new MemoryEventJournal();
    for (let i = 0; i < 30; i += 1) {
      await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", { i }, `tel-cp-${i}`));
    }
    await seedCheckpoints(journal, 220);
    const manager = new RetentionManager(journal, defaultRetentionConfig({
      enabled: true, checkpointKeep: 1, batchSize: 200, maxRowsPerCycle: 50,
      maxBatchesPerCycle: 8, cycleBudgetMs: 10_000, telemetryTtlMs: 1, monitorTtlMs: 1,
    }));
    const result = await manager.runCycle({
      experiment: null, now: new Date(), degraded: false, failClosed: false, databaseUp: true,
    });
    expect(result.deletedByClass.raw_telemetry).toBe(30);
    expect(result.deletedByClass.checkpoint).toBe(20);
    expect(result.deletedByClass.raw_telemetry + result.deletedByClass.checkpoint).toBe(50);
    expect((await journal.loadLatestCheckpoint())?.sequence).toBe(220);
  });

  it("journal prune API never deletes more than the caller limit and keeps the latest usable", async () => {
    const journal = new MemoryEventJournal();
    await seedCheckpoints(journal, 210);
    const pruned = await journal.pruneOldCheckpoints(1, 50);
    expect(pruned.deleted).toBe(50);
    expect(pruned.latestSequence).toBe(210);
    expect((await journal.loadLatestCheckpoint())?.sequence).toBe(210);
    expect((await journal.pruneOldCheckpoints(1, 0)).deleted).toBe(0);
    await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", {}, "tel-limit-0"));
    expect((await journal.deleteJournalEventsBounded({
      types: TELEMETRY_EVENT_TYPES,
      occurredBefore: new Date(),
      limit: 0,
    })).deleted).toBe(0);
    expect((await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).length).toBe(1);
  });
});

describe("Step-4 residual: operator APPLY global budget", () => {
  async function seedCleanupBacklog(journal: MemoryEventJournal, telemetry: number, monitor: number): Promise<void> {
    for (let i = 0; i < telemetry; i += 1) {
      await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", { i }, `apply-tel-${i}`));
    }
    for (let i = 0; i < monitor; i += 1) {
      await journal.append(ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", { i }, `apply-mon-${i}`));
    }
  }

  it("maxBatches=3 is a global cap across telemetry and monitor, not 3 per class", async () => {
    const journal = new MemoryEventJournal();
    await seedCleanupBacklog(journal, 80, 80);
    const config = defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, batchSize: 10,
      maxBatchesPerCycle: 20, maxRowsPerCycle: 10_000, cycleBudgetMs: 10_000,
    });
    const apply = await applyJournalCleanup(journal, {
      now: new Date("2026-09-08T00:00:00.000Z"), experiment: null,
      confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, maxBatches: 3,
    });
    expect(apply.batches).toBeLessThanOrEqual(3);
    expect(apply.batches).toBe(3);
    expect((apply.deletedByClass.raw_telemetry ?? 0) + (apply.deletedByClass.monitor_control ?? 0)).toBe(30);
    expect(apply.applyBounds.maxBatches).toBe(3);
    expect(apply.durableTransitionsDeleted).toBe(0);
  });

  it("non-divisible maxRows is never exceeded", async () => {
    const journal = new MemoryEventJournal();
    await seedCleanupBacklog(journal, 80, 80);
    const config = defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, batchSize: 10,
      maxBatchesPerCycle: 20, maxRowsPerCycle: 4_000, cycleBudgetMs: 10_000,
    });
    const apply = await applyJournalCleanup(journal, {
      now: new Date("2026-09-08T00:00:00.000Z"), experiment: null,
      confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, maxRows: 25,
    });
    const deleted = (apply.deletedByClass.raw_telemetry ?? 0) + (apply.deletedByClass.monitor_control ?? 0);
    expect(deleted).toBeLessThanOrEqual(25);
    expect(deleted).toBe(25);
    expect(apply.applyBounds.maxRows).toBe(25);
    expect(apply.applyBounds.maxRows).not.toBe(config.maxRowsPerCycle);
  });

  it("time budget stops further batches", async () => {
    const journal = new MemoryEventJournal();
    await seedCleanupBacklog(journal, 80, 80);
    const original = journal.deleteJournalEventsBounded.bind(journal);
    journal.deleteJournalEventsBounded = async (input) => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      return original(input);
    };
    const config = defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, batchSize: 10,
      maxBatchesPerCycle: 20, maxRowsPerCycle: 10_000, cycleBudgetMs: 250,
    });
    const apply = await applyJournalCleanup(journal, {
      now: new Date("2026-09-08T00:00:00.000Z"), experiment: null,
      confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, cycleBudgetMs: 50, maxBatches: 20,
    });
    expect(apply.batches).toBeLessThan(8);
    expect(apply.batches).toBeGreaterThanOrEqual(1);
    expect(apply.applyBounds.cycleBudgetMs).toBe(50);
  });

  it("dry-run applyBounds equal the bounds APPLY actually uses", async () => {
    const journal = new MemoryEventJournal();
    await seedCleanupBacklog(journal, 40, 40);
    const config = defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, batchSize: 10,
      maxBatchesPerCycle: 20, maxRowsPerCycle: 4_000, cycleBudgetMs: 5_000,
    });
    const now = new Date("2026-09-08T00:00:00.000Z");
    const overrides = { maxBatches: 3, maxRows: 25, cycleBudgetMs: 5_000, batchSize: 10 };
    const dry = await dryRunJournalCleanup(journal, now, null, config, overrides);
    expect(dry.effectiveConfig.maxBatchesPerCycle).toBe(20);
    expect(dry.effectiveConfig.maxRowsPerCycle).toBe(4_000);
    expect(dry.applyBounds).toEqual(overrides);
    expect(dry.estimatedBoundedBatches).toBe(3);
    const applyJournal = new MemoryEventJournal();
    await seedCleanupBacklog(applyJournal, 40, 40);
    const apply = await applyJournalCleanup(applyJournal, {
      now, experiment: null, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, ...overrides,
    });
    expect(apply.applyBounds).toEqual(dry.applyBounds);
    expect(apply.batches).toBeLessThanOrEqual(dry.applyBounds.maxBatches);
    const deleted = (apply.deletedByClass.raw_telemetry ?? 0) + (apply.deletedByClass.monitor_control ?? 0);
    expect(deleted).toBeLessThanOrEqual(dry.applyBounds.maxRows);
  });
});

function paperEngine(journal: MemoryEventJournal) {
  const adapter = new ScriptedMarketAdapter();
  adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
  return new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
    market: adapter as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
  });
}

describe("Step-4 closure: all REPORT_INPUT producers participate in source seal", () => {
  it("proves MCP_INTERACTION and writer/engine paths: no untracked report-input append", () => {
    const mcp = readFileSync(new URL("../apps/engine/src/mcp.ts", import.meta.url), "utf8");
    expect(mcp).not.toMatch(/journal\.append\s*\(/);
    expect(mcp).toMatch(/persistReportInput/);
    const writer = readFileSync(new URL("../apps/engine/src/journal-writer.ts", import.meta.url), "utf8");
    expect(writer).toMatch(/REPORT_INPUT_MUST_USE_TRACKED_ADMISSION/);
    const engine = readFileSync(new URL("../apps/engine/src/engine.ts", import.meta.url), "utf8");
    expect(engine).toMatch(/persistReportInput\(event/);
    expect(engine).toMatch(/reportInput: this\.isCanonicalWindowAdmission/);
    expect(shouldLatchReportSourceIntegrity({ type: "MARKET_SNAPSHOT" }, { id: "e", status: "RUNNING" })).toBe(false);
    expect(shouldLatchReportSourceIntegrity({ type: "SYSTEM_HEALTH_EVENT" }, { id: "e", status: "RUNNING" })).toBe(false);
    expect(shouldLatchReportSourceIntegrity({ type: "MCP_INTERACTION" }, { id: "e", status: "RUNNING" })).toBe(true);
    expect(shouldLatchReportSourceIntegrity({ type: "GROK_SIGNAL_ACCEPTED" }, { id: "e", status: "IDLE" })).toBe(false);
    expect(shouldLatchReportSourceIntegrity({ type: "MCP_INTERACTION", payload: {} }, { id: "e", status: "COMPLETED" })).toBe(false);
    expect(shouldLatchReportSourceIntegrity({ type: "MCP_INTERACTION", payload: { experimentId: "e" } }, { id: "e", status: "COMPLETED" })).toBe(true);
  });

  it("BoundedJournalWriter refuses report-input instead of appending untracked", async () => {
    const journal = new MemoryEventJournal();
    const writer = new BoundedJournalWriter(journal, () => undefined, { error: () => undefined });
    await expect(writer.persist(ev("TRIGGER_EVENT", "2026-09-01T00:00:00.000Z", {}, "writer-trig")))
      .rejects.toThrow(/REPORT_INPUT_MUST_USE_TRACKED_ADMISSION/);
    expect(await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).toEqual([]);
  });

  it("IN-FLIGHT MCP_INTERACTION persist cannot be sealed ahead of; release yields stable hash across restart", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    let release!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["MCP_INTERACTION"]);
      const mcpEvent = createJournalEvent({
        id: "mcp-inflight-1",
        traceId: "tr-mcp-seal",
        type: "MCP_INTERACTION",
        aggregateType: "mcp_client",
        aggregateId: "client-1",
        payload: { tool: "get_strategy_metrics", status: "ok" },
      });
      const persistP = engine.persistReportInput(mcpEvent);
      await waitUntil(() => journal.inFlightAppends > 0);
      await engine.completeExperimentForTest();
      const sealPromise = engine.ensureSourceSealForTest();
      await new Promise((resolve) => setTimeout(resolve, 40));
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      expect((await journal.queryEvents({ types: ["MCP_INTERACTION"] })).length).toBe(0);
      release();
      journal.setAppendInFlightHoldForTest(null);
      await persistP;
      const seal = await sealPromise;
      expect(seal.sealed).toBe(true);
      expect((await journal.queryEvents({ types: ["MCP_INTERACTION"] })).some((event) => event.id === "mcp-inflight-1")).toBe(true);
      await engine.writeFinalReportForTest();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      const streamed = await streamExperimentSummary(journal, {
        id: experimentId,
        startedAt: engine.experimentManifest()?.startedAt,
        completedAt: engine.experimentManifest()?.completedAt,
      }, {
        status: "COMPLETED", reportKind: "final", metricsScope: "complete", complete: true, truncated: false,
        generatedAt: first!.generatedAt,
      });
      streamed.summary.complete = true;
      streamed.summary.truncated = false;
      const rerun = toDurableReportInput({ summary: streamed.summary, scan: streamed.scan });
      expect(rerun.contentHash).toBe(first!.contentHash);
      expect(streamed.scan.scanned).toBe(first!.sourceEventCount);
      expect(streamed.scan.fence.through).toEqual({
        occurredAt: first!.sourceFenceOccurredAt,
        id: first!.sourceFenceId,
      });
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(first!.contentHash);
        await restarted.writeFinalReportForTest();
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(first!.contentHash);
      } finally {
        await restarted.stop();
      }
    } finally {
      release?.();
      journal.setAppendInFlightHoldForTest(null);
      if (!engine.state.draining) await engine.stop();
    }
  });
});

describe("Step-4 closure: experiment-scoped report-source integrity", () => {
  it("A: GROK_SIGNAL_ACCEPTED loss is sticky; later unrelated success does not clear; sampled telemetry does not poison", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      journal.failAppendTypes.add("MARKET_SNAPSHOT");
      const lost = createJournalEvent({
        id: "grok-lost-1", traceId: "tr-grok-lost", type: "GROK_SIGNAL_ACCEPTED",
        aggregateType: "grok_signal", aggregateId: "sig-lost",
        payload: { experimentId, signalId: "sig-lost" },
      });
      await engine.persistEventForTest(lost);
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-lost-1")).toBe(true);
      expect((await journal.queryEvents({ types: [REPORT_SOURCE_INTEGRITY_FAULT] })).length).toBe(1);
      await engine.persistEventForTest(ev("MARKET_SNAPSHOT", new Date().toISOString(), { experimentId }, "snap-fail"));
      expect(engine.reportSourceIntegrityForTest().every((fault) => fault.eventType !== "MARKET_SNAPSHOT")).toBe(true);
      journal.failAppendTypes.delete("MARKET_SNAPSHOT");
      journal.failAppendTypes.delete("GROK_SIGNAL_ACCEPTED");
      await engine.persistEventForTest(ev("SYSTEM_HEALTH_EVENT", new Date().toISOString(), { experimentId, status: "OK" }, "health-ok"));
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-lost-1")).toBe(true);
      await engine.completeExperimentForTest();
      const seal = await engine.ensureSourceSealForTest();
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
      await engine.writeFinalReportForTest();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
    } finally {
      await engine.stop();
    }
  });

  it("B: TRIGGER_EVENT loss refuses seal and canonical until verified durable repair", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("TRIGGER_EVENT");
      const lost = createJournalEvent({
        id: "trig-lost-1", traceId: "tr-trig-lost", type: "TRIGGER_EVENT",
        aggregateType: "trigger", aggregateId: "trig-lost-1",
        payload: { experimentId, id: "trig-lost-1" },
      });
      await engine.persistEventForTest(lost);
      journal.failAppendTypes.delete("TRIGGER_EVENT");
      await engine.persistEventForTest(ev("SYSTEM_HEALTH_EVENT", new Date().toISOString(), { experimentId }, "health-2"));
      await engine.completeExperimentForTest();
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
      expect(await engine.repairReportSourceIntegrityForTest(lost)).toBe(true);
      expect(engine.reportSourceIntegrityForTest().length).toBe(0);
      expect((await journal.queryEvents({ types: [REPORT_SOURCE_INTEGRITY_REPAIRED] })).length).toBe(1);
      const seal = await engine.ensureSourceSealForTest();
      expect(seal.sealed).toBe(true);
      await engine.writeFinalReportForTest();
      expect(await journal.loadExperimentReport(experimentId)).toMatchObject({ complete: true, truncated: false });
    } finally {
      await engine.stop();
    }
  });

  it("C2: stale checkpoint fault is dropped after durable REPAIRED", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("TRIGGER_EVENT");
      const lost = createJournalEvent({
        id: "trig-stale-cp", traceId: "tr-stale-cp", type: "TRIGGER_EVENT",
        aggregateType: "trigger", aggregateId: "trig-stale-cp",
        payload: { experimentId, id: "trig-stale-cp" },
      });
      await engine.persistEventForTest(lost);
      journal.failAppendTypes.delete("TRIGGER_EVENT");
      await engine.completeExperimentForTest();
      expect(await engine.repairReportSourceIntegrityForTest(lost)).toBe(true);
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect(restarted.reportSourceIntegrityForTest().some((fault) => fault.eventId === "trig-stale-cp")).toBe(false);
        expect((await restarted.ensureSourceSealForTest()).sealed).toBe(true);
      } finally {
        await restarted.stop();
      }
    } finally {
      if (!engine.state.draining) await engine.stop();
    }
  });

  it("C: restart with unresolved integrity must not silently forget or complete", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    const lost = createJournalEvent({
      id: "grok-lost-restart", traceId: "tr-grok-restart", type: "GROK_SIGNAL_ACCEPTED",
      aggregateType: "grok_signal", aggregateId: "sig-restart",
      payload: { signalId: "sig-restart" },
    });
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      await engine.persistEventForTest(lost);
      journal.failAppendTypes.delete("GROK_SIGNAL_ACCEPTED");
      await engine.completeExperimentForTest();
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect(restarted.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-lost-restart")).toBe(true);
        expect((await restarted.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
        await restarted.writeFinalReportForTest();
        expect(await journal.loadExperimentReport(experimentId)).toBeNull();
        expect(await restarted.repairReportSourceIntegrityForTest({
          ...lost,
          payload: { ...lost.payload, experimentId },
        })).toBe(true);
        expect((await restarted.ensureSourceSealForTest()).sealed).toBe(true);
        await restarted.writeFinalReportForTest();
        expect(await journal.loadExperimentReport(restarted.experimentManifest()!.id)).toMatchObject({ complete: true, truncated: false });
      } finally {
        await restarted.stop();
      }
    } finally {
      if (!engine.state.draining) await engine.stop();
    }
  });
});

describe("Step-4 closure: authoritative cleanup lifecycle", () => {
  const now = new Date("2026-09-08T00:00:00.000Z");
  const config = defaultRetentionConfig({
    enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, completedGraceMs: 1, batchSize: 50,
    maxBatchesPerCycle: 10, maxRowsPerCycle: 1_000, cycleBudgetMs: 5_000,
  });

  it("A: resolves ACTIVE_RUNNING from durable EXPERIMENT_STARTED, not env", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-live", startedAt: "2026-09-07T00:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T00:00:00.000Z", { experimentId: "exp-live", aggregateId: "exp-live" }, "start-live"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("ACTIVE_RUNNING");
    expect(lifecycle.source).toBe("durable_transitions");
    expect(lifecycle.experimentId).toBe("exp-live");
    expect(lifecycle.status).toBe("RUNNING");
    expect(lifecycle.monitorDeleteAllowed).toBe(true);
    expect(lifecycle.experiment).toMatchObject({ id: "exp-live", status: "RUNNING" });
  });

  it("B: env hint mismatch fail-closes; never becomes PROVEN_NO_ACTIVE", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-a", startedAt: "2026-09-07T00:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T00:00:00.000Z", { experimentId: "exp-a", aggregateId: "exp-a" }, "start-a"));
    const lifecycle = await resolveCleanupLifecycle(journal, { experimentId: "exp-other", status: "COMPLETED" });
    expect(lifecycle.hintMismatch).toBe(true);
    expect(lifecycle.state === "CONFLICTING" || lifecycle.state === "UNKNOWN").toBe(true);
    expect(lifecycle.state).not.toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
    expect(lifecycle.monitorDeleteAllowed).toBe(false);
    const apply = await applyJournalCleanup(journal, {
      now, experiment: lifecycle.experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, lifecycle,
    });
    expect(apply.refused).toBe(true);
  });

  it("C: missing EXPERIMENT_ID is not monitor-only while an experiment is active", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-c", startedAt: "2026-09-07T12:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T12:00:00.000Z", { experimentId: "exp-c", aggregateId: "exp-c" }, "start-c"));
    await journal.append(ev("TRIGGER_EVENT", "2026-09-07T13:00:00.000Z", { experimentId: "exp-c" }, "in-window-c"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("ACTIVE_RUNNING");
    const dry = await dryRunJournalCleanup(journal, now, null, config, {}, lifecycle);
    expect(dry.monitorOnlyEligible).toBe(false);
    expect(dry.lifecycle?.state).toBe("ACTIVE_RUNNING");
    expect(dry.monitor.eligible).toBe(0);
  });

  it("D: UNKNOWN forbids monitor delete, is visible on dry-run, and refuses APPLY", async () => {
    class UnreadableLifecycleJournal extends MemoryEventJournal {
      override async queryEventsKeyset(query: Parameters<MemoryEventJournal["queryEventsKeyset"]>[0]) {
        if (query.types?.includes("EXPERIMENT_STARTED")) throw new Error("JOURNAL_QUERY_FAILED");
        return super.queryEventsKeyset(query);
      }
    }
    const journal = new UnreadableLifecycleJournal();
    await MemoryEventJournal.prototype.append.call(journal, ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", {}, "mon-unknown"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("UNKNOWN");
    expect(lifecycle.state).not.toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
    expect(lifecycle.monitorDeleteAllowed).toBe(false);
    const dry = await dryRunJournalCleanup(journal, now, null, config, {}, lifecycle);
    expect(dry.monitor.eligible).toBe(0);
    expect(dry.refuseUnsafeExperimentCleanup).toBe(true);
    expect(dry.notes.some((note) => note.includes("UNKNOWN"))).toBe(true);
    expect(dry.lifecycle).toMatchObject({
      state: "UNKNOWN", source: "unresolved", monitorDeleteAllowed: false,
    });
    const apply = await applyJournalCleanup(journal, {
      now, experiment: lifecycle.experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, lifecycle,
    });
    expect(apply.refused).toBe(true);
    expect(apply.reason).toBe("UNKNOWN_EXPERIMENT_LIFECYCLE");
    expect(apply.deletedByClass.monitor_control ?? 0).toBe(0);
  });

  it("E: RUNNING/ENTRY_CLOSED protect in-window; missing startedAt forbids monitor delete", async () => {
    const running = new MemoryEventJournal();
    await seedDurableLifecycle(running, { id: "exp-e", startedAt: "2026-09-07T12:00:00.000Z" });
    await running.append(ev("EXPERIMENT_STARTED", "2026-09-07T12:00:00.000Z", { experimentId: "exp-e", aggregateId: "exp-e" }, "start-e"));
    await running.append(ev("TRIGGER_EVENT", "2026-09-07T13:00:00.000Z", { experimentId: "exp-e" }, "win-e"));
    await running.append(ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", {}, "pre-e"));
    const runningLife = await resolveCleanupLifecycle(running);
    const runningDry = await dryRunJournalCleanup(running, now, runningLife.experiment, config, {}, runningLife);
    expect(runningDry.monitor.eligible).toBe(1);
    const closed = new MemoryEventJournal();
    await seedDurableLifecycle(closed, { id: "exp-closed", entryClosedAt: "2026-09-07T12:00:00.000Z" });
    await closed.append(ev("EXPERIMENT_ENTRY_CLOSED", "2026-09-07T12:00:00.000Z", { experimentId: "exp-closed", aggregateId: "exp-closed" }, "closed-only"));
    await closed.append(ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", {}, "mon-closed"));
    const closedLife = await resolveCleanupLifecycle(closed);
    expect(closedLife.state).toBe("ACTIVE_ENTRY_CLOSED");
    expect(closedLife.startedAt).toBeNull();
    expect(closedLife.monitorDeleteAllowed).toBe(false);
    const closedDry = await dryRunJournalCleanup(closed, now, closedLife.experiment, config, {}, closedLife);
    expect(closedDry.monitor.eligible).toBe(0);
  });

  it("F: COMPLETED requires canonical complete+non-truncated + grace", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, {
      id: "exp-1",
      startedAt: "2026-09-01T00:00:00.000Z",
      completedAt: "2026-09-01T00:00:08.000Z",
    });
    await journal.appendMany(fixtureEvents());
    const lifecycle = await resolveCleanupLifecycle(journal, { experimentId: "exp-1" });
    expect(lifecycle.state).toBe("COMPLETED");
    const blocked = await dryRunJournalCleanup(journal, new Date("2026-09-01T00:00:10.000Z"), lifecycle.experiment, defaultRetentionConfig({
      enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, completedGraceMs: 7 * 24 * 60 * 60_000,
    }), {}, lifecycle);
    expect(blocked.completedGraceGate).toBe(false);
    expect(blocked.monitor.eligible).toBe(0);
  });

  it("G: PROVEN_NO_ACTIVE_EXPERIMENT is monitor TTL eligible", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", {}, "orphan-mon"));
    await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", {}, "orphan-tel"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
    expect(lifecycle.experiment).toBeNull();
    expect(lifecycle.monitorDeleteAllowed).toBe(true);
    const dry = await dryRunJournalCleanup(journal, now, null, config, {}, lifecycle);
    expect(dry.monitorOnlyEligible).toBe(true);
    expect(dry.monitor.eligible).toBe(1);
  });

  it("H: dry-run shows lifecycle state/source/id/status/times/allowed/reason", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-h", startedAt: "2026-09-07T00:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T00:00:00.000Z", { experimentId: "exp-h", aggregateId: "exp-h" }, "start-h"));
    const lifecycle = await resolveCleanupLifecycle(journal, { experimentId: "exp-h", status: "RUNNING" });
    const dry = await dryRunJournalCleanup(journal, now, lifecycle.experiment, config, {}, lifecycle);
    expect(dry.lifecycle).toEqual(expect.objectContaining({
      state: "ACTIVE_RUNNING",
      source: "hint_verified",
      experimentId: "exp-h",
      status: "RUNNING",
      startedAt: "2026-09-07T00:00:00.000Z",
      completedAt: null,
      monitorDeleteAllowed: true,
    }));
    expect(dry.lifecycle?.reason.length).toBeGreaterThan(10);
    expect(dry.notes.some((note) => note.includes("Authoritative lifecycle: ACTIVE_RUNNING"))).toBe(true);
  });

  it("DATABASE_URL-only (no EXPERIMENT_ID) while active experiment is SAFE", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-db", startedAt: "2026-09-07T12:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T12:00:00.000Z", { experimentId: "exp-db", aggregateId: "exp-db" }, "start-db"));
    await journal.append(ev("TRIGGER_EVENT", "2026-09-07T13:00:00.000Z", { experimentId: "exp-db" }, "live-mon"));
    await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", {}, "old-tel"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("ACTIVE_RUNNING");
    const apply = await applyJournalCleanup(journal, {
      now, experiment: lifecycle.experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, lifecycle,
    });
    expect(apply.refused).toBe(false);
    expect(apply.deletedByClass.raw_telemetry).toBe(1);
    expect(apply.deletedByClass.monitor_control ?? 0).toBe(0);
    expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).some((event) => event.id === "live-mon")).toBe(true);
    expect((await journal.queryEvents({ types: ["MARKET_SNAPSHOT"] })).length).toBe(0);
  });
});

describe("Step-4 closure: in-flight source-seal database write hold", () => {
  it("IN-FLIGHT DATABASE WRITE HOLD: stall inside EventJournal source-seal append cannot later commit a timed-out seal", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    let release!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      engine.setSourceSealTimeoutForTest(250);
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["EXPERIMENT_SOURCE_SEALED"]);
      await engine.completeExperimentForTest();
      const sealPromise = engine.ensureSourceSealForTest();
      await waitUntil(() => journal.inFlightAppends > 0 || engine.sourceSealPersistInFlightForTest());
      expect(engine.sourceSealPersistInFlightForTest() || journal.inFlightAppends > 0).toBe(true);
      const seal = await sealPromise;
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("SOURCE_SEAL_TIMEOUT");
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      release();
      journal.setAppendInFlightHoldForTest(null);
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
      engine.setSourceSealTimeoutForTest(5_000);
      const retry = await engine.ensureSourceSealForTest();
      expect(retry.sealed).toBe(true);
      expect(await loadSourceSeal(journal, experimentId)).toBeTruthy();
      await engine.writeFinalReportForTest();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(first!.contentHash);
      } finally {
        await restarted.stop();
      }
    } finally {
      release?.();
      journal.setAppendInFlightHoldForTest(null);
      if (!engine.state.draining) await engine.stop();
    }
  });
});

describe("Step-4 follow-up: cleanup authority from durable_transitions", () => {
  const now = new Date("2026-09-08T00:00:00.000Z");
  const config = defaultRetentionConfig({
    enabled: true, telemetryTtlMs: 1, monitorTtlMs: 1, completedGraceMs: 1, batchSize: 50,
    maxBatchesPerCycle: 10, maxRowsPerCycle: 1_000, cycleBudgetMs: 5_000,
  });

  it("A: durable EXPERIMENT_STARTED without derived audit is ACTIVE_RUNNING", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-auditless", startedAt: "2026-09-07T12:00:00.000Z" });
    expect((await journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).length).toBe(0);
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("ACTIVE_RUNNING");
    expect(lifecycle.source).toBe("durable_transitions");
    expect(lifecycle.journalCrossCheck).toBe("missing_audit");
    expect(lifecycle.experimentId).toBe("exp-auditless");
    expect(lifecycle.monitorDeleteAllowed).toBe(true);
  });

  it("B: two durable active identities are CONFLICTING", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-one", startedAt: "2026-09-07T10:00:00.000Z" });
    await seedDurableLifecycle(journal, { id: "exp-two", startedAt: "2026-09-07T11:00:00.000Z" });
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("CONFLICTING");
    expect(lifecycle.monitorDeleteAllowed).toBe(false);
    expect(lifecycle.state).not.toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
  });

  it("C: only no durable and no legacy journal lifecycle is PROVEN_NO_ACTIVE", async () => {
    const journal = new MemoryEventJournal();
    await journal.append(ev("TRIGGER_EVENT", "2026-01-01T00:00:00.000Z", {}, "orphan"));
    const none = await resolveCleanupLifecycle(journal, {});
    expect(none.state).toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T00:00:00.000Z", { experimentId: "legacy-only", aggregateId: "legacy-only" }, "legacy-start"));
    const legacy = await resolveCleanupLifecycle(journal, {});
    expect(legacy.state).toBe("UNKNOWN");
    expect(legacy.source).toBe("legacy_journal");
    expect(legacy.journalCrossCheck).toBe("legacy_only");
    expect(legacy.state).not.toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
    expect(legacy.monitorDeleteAllowed).toBe(false);
  });

  it("D: DATABASE_URL-only APPLY is SAFE in AFTER_APPLY_BEFORE_DERIVED_AUDIT", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      engine.setCrashInjectionForTest("AFTER_APPLY_BEFORE_DERIVED_AUDIT");
      await expect(engine.beginExperiment()).rejects.toThrow(/CRASH_INJECTED/);
      const experimentId = (await journal.readDurableTransitionsAfter(0, 8))
        .find((row) => row.type === "EXPERIMENT_STARTED")?.aggregateId;
      expect(experimentId).toBeTruthy();
      expect((await journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).length).toBe(0);
      await journal.append(ev("TRIGGER_EVENT", "2026-09-07T13:00:00.000Z", { experimentId }, "live-after-apply"));
      await journal.append(ev("MARKET_SNAPSHOT", "2026-01-01T00:00:00.000Z", {}, "old-tel-after-apply"));
      const lifecycle = await resolveCleanupLifecycle(journal, {});
      expect(lifecycle.state).toBe("ACTIVE_RUNNING");
      expect(lifecycle.source).toBe("durable_transitions");
      expect(lifecycle.journalCrossCheck).toBe("missing_audit");
      expect(lifecycle.experimentId).toBe(experimentId);
      const apply = await applyJournalCleanup(journal, {
        now, experiment: lifecycle.experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, lifecycle,
      });
      expect(apply.refused).toBe(false);
      expect(apply.deletedByClass.raw_telemetry).toBe(1);
      expect((await journal.queryEvents({ types: ["TRIGGER_EVENT"] })).some((event) => event.id === "live-after-apply")).toBe(true);
      expect((await journal.readDurableTransitionsAfter(0, 32)).some((row) => row.type === "EXPERIMENT_STARTED")).toBe(true);
    } finally {
      if (!engine.state.draining) await engine.stop().catch(() => undefined);
    }
  });

  it("E: journal must not override durable ACTIVE to no-active or CONFLICTING", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-durable", startedAt: "2026-09-07T12:00:00.000Z" });
    await journal.append(ev("EXPERIMENT_STARTED", "2026-09-07T13:00:00.000Z", { experimentId: "exp-journal-extra", aggregateId: "exp-journal-extra" }, "extra-start"));
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    expect(lifecycle.state).toBe("ACTIVE_RUNNING");
    expect(lifecycle.experimentId).toBe("exp-durable");
    expect(lifecycle.journalCrossCheck).toBe("conflict");
    expect(lifecycle.state).not.toBe("CONFLICTING");
    expect(lifecycle.state).not.toBe("PROVEN_NO_ACTIVE_EXPERIMENT");
  });

  it("F: durable_transitions are never pruned by APPLY", async () => {
    const journal = new MemoryEventJournal();
    await seedDurableLifecycle(journal, { id: "exp-keep", startedAt: "2026-09-07T00:00:00.000Z" });
    const before = await journal.readDurableTransitionsAfter(0, 32);
    expect(before.length).toBeGreaterThan(0);
    const lifecycle = await resolveCleanupLifecycle(journal, {});
    const apply = await applyJournalCleanup(journal, {
      now, experiment: lifecycle.experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config, lifecycle,
    });
    expect(apply.refused).toBe(false);
    expect(await journal.readDurableTransitionsAfter(0, 32)).toEqual(before);
  });
});

describe("Step-4 follow-up: canonical report-input inventory", () => {
  it("classifies every DECISION type exactly once; every canonical type has integrity or fail-closed semantics", () => {
    const seen = new Set<string>();
    for (const type of CANONICAL_REPORT_INPUT_EVENT_TYPES) {
      expect(classifyReportInput(type)).toBe("canonical");
      expect(seen.has(type)).toBe(false);
      seen.add(type);
    }
    for (const type of OPERATIONAL_MONITOR_EVENT_TYPES) {
      expect(classifyReportInput(type)).toBe("operational");
      expect(seen.has(type)).toBe(false);
      seen.add(type);
    }
    for (const type of DECISION_EVENT_TYPES) {
      expect(classifyReportInput(type), type).not.toBeNull();
    }
    for (const type of CANONICAL_REPORT_INPUT_EVENT_TYPES) {
      const swallowable = !isCriticalPersistenceType(type);
      const exempt = isSourceSealExemptType(type);
      const latches = shouldLatchReportSourceIntegrity({ type, payload: { experimentId: "e" } }, { id: "e", status: "RUNNING" });
      if (swallowable && !exempt) expect(latches, type).toBe(true);
      if (exempt) expect(latches, type).toBe(false);
      expect(swallowable && !exempt && !latches, `${type} swallowable without integrity`).toBe(false);
    }
    for (const type of OPERATIONAL_MONITOR_EVENT_TYPES) {
      expect(shouldLatchReportSourceIntegrity({ type }, { id: "e", status: "RUNNING" }), type).toBe(false);
    }
  });
});

describe("Step-4 follow-up: crash-durable bounded integrity", () => {
  it("an in-flight Grok claim is not a successful duplicate and does not poison its healthy first caller", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    let release!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const now = Date.now();
      const trigger = triggerEventSchema.parse({
        id: "trig-concurrent", traceId: "trace-concurrent", marketId: "btc-mkt", asset: "BTC",
        kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "concurrent", evidence: {},
        sourceEventIds: ["e-concurrent"], source: "polymarket",
        createdAt: new Date(now).toISOString(), expiresAt: new Date(now + 60_000).toISOString(),
      });
      await engine.registerTriggerForResearch(trigger);
      const submission = grokSignalSubmissionSchema.parse({
        submissionId: "sub-concurrent", traceId: trigger.traceId, triggerId: trigger.id,
        correlationId: "c-concurrent", marketId: "btc-mkt", asset: "BTC",
        agentId: "fair_probability_analyst", role: "fair_probability_analyst", timestamp: new Date(now).toISOString(),
        output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: trigger.expiresAt,
          reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
      });
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["GROK_SIGNAL_ACCEPTED"]);
      const first = engine.submitGrokSignal(submission, submission.agentId);
      await waitUntil(() => journal.inFlightAppends > 0);
      await expect(engine.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
      expect(engine.recoveryFailClosedForTest()).toBe(false);
      expect(engine.state.grokActivity.acceptedSignals).toBe(0);
      release();
      expect(await first).toMatchObject({ status: "accepted" });
      expect(await engine.submitGrokSignal(submission, submission.agentId)).toEqual({ status: "duplicate" });
      expect(engine.state.grokActivity.acceptedSignals).toBe(1);
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
    } finally {
      release?.();
      journal.setAppendInFlightHoldForTest(null);
      await engine.stop();
    }
  });

  it("triple persistence failure rejects Grok, stays fail closed, and never reinterprets the orphan claim after crash", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    const now = Date.now();
    const trigger = triggerEventSchema.parse({
      id: "trig-triple", traceId: "trace-triple", marketId: "btc-mkt", asset: "BTC",
      kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "triple", evidence: {},
      sourceEventIds: ["e-triple"], source: "polymarket",
      createdAt: new Date(now).toISOString(), expiresAt: new Date(now + 60_000).toISOString(),
    });
    const submission = grokSignalSubmissionSchema.parse({
      submissionId: "sub-triple", traceId: trigger.traceId, triggerId: trigger.id,
      correlationId: "c-triple", marketId: "btc-mkt", asset: "BTC",
      agentId: "fair_probability_analyst", role: "fair_probability_analyst", timestamp: new Date(now).toISOString(),
      output: { confidence: 0.8, evidenceIds: ["e-triple"], expiresAt: trigger.expiresAt,
        reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
    });
    let checkpointsBefore = 0;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.registerTriggerForResearch(trigger);
      checkpointsBefore = (await journal.listCheckpoints()).length;
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      journal.failAppendTypes.add(REPORT_SOURCE_INTEGRITY_FAULT);
      journal.failNextDurableInsert = true;
      await expect(engine.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("JOURNAL_APPEND_FAILED");
      expect(journal.failNextDurableInsert).toBe(false);
      expect(engine.recoveryFailClosedForTest()).toBe(true);
      expect(engine.state.overloadState).toBe("FAIL_CLOSED");
      expect(engine.state.ready).toBe(false);
      expect(engine.state.grokActivity.acceptedSignals).toBe(0);
      expect(engine.state.latestAgents.size).toBe(0);
      expect(engine.reportSourceIntegrityForTest()).toHaveLength(1);
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", `${submission.agentId}:${submission.submissionId}`)).toBeTruthy();
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED", REPORT_SOURCE_INTEGRITY_FAULT] })).toEqual([]);
      expect(await journal.queryDurableTransitions({ types: [REPORT_SOURCE_INTEGRITY_FAULT], limit: 8 })).toEqual([]);
      journal.failAppendTypes.clear();
      await engine.persistEventForTest(createJournalEvent({ traceId: "unrelated", type: "SYSTEM_HEALTH_EVENT",
        aggregateType: "engine", aggregateId: "engine", payload: { status: "UP" } }));
      await expect(engine.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("REPORT_SOURCE_INTEGRITY_FAIL_CLOSED");
      expect(engine.reportSourceIntegrityForTest()).toHaveLength(1);
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
    } finally {
      await engine.stop(); // Existing sticky integrity state forbids the shutdown checkpoint.
    }
    expect((await journal.listCheckpoints()).length).toBe(checkpointsBefore);
    const restarted = paperEngine(journal);
    await restarted.start();
    try {
      expect(restarted.reportSourceIntegrityForTest()).toEqual([]); // No hidden checkpoint/marker rescued this test.
      await restarted.registerTriggerForResearch(trigger);
      await expect(restarted.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
      await expect(restarted.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
      expect(restarted.state.grokActivity.acceptedSignals).toBe(0);
      expect(restarted.state.grokActivity.duplicates).toBe(0);
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED", REPORT_SOURCE_INTEGRITY_FAULT] })).toEqual([]);
    } finally {
      await restarted.stop();
    }
  });

  it("strategy proposal persistence failure never reinterprets an accepted-only claim as a duplicate after restart", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    const now = Date.now();
    const trigger = triggerEventSchema.parse({
      id: "trig-strategy-orphan", traceId: "trace-strategy-orphan", marketId: "btc-mkt", asset: "BTC",
      kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "strategy-orphan", evidence: {},
      sourceEventIds: ["e-strategy-orphan"], source: "polymarket",
      createdAt: new Date(now).toISOString(), expiresAt: new Date(now + 60_000).toISOString(),
    });
    const submission = grokSignalSubmissionSchema.parse({
      submissionId: "sub-strategy-orphan", traceId: trigger.traceId, triggerId: trigger.id,
      correlationId: "c-strategy-orphan", marketId: "btc-mkt", asset: "BTC",
      agentId: "strategy_supervisor", role: "strategy_supervisor", timestamp: new Date(now).toISOString(),
      output: {
        confidence: 0.8, evidenceIds: ["e-strategy-orphan"], expiresAt: trigger.expiresAt, reasonCodes: ["T"],
        diagnosis: "regime shift", regime: "VOLATILE",
        proposedChange: { status: "PROPOSED", summary: "raise entry threshold", parameterPatch: { entryThreshold: 0.07 } },
      },
    });
    let checkpointsBefore = 0;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.registerTriggerForResearch(trigger);
      checkpointsBefore = (await journal.listCheckpoints()).length;
      journal.failAppendTypes.add("STRATEGY_CHANGE_PROPOSED");
      journal.failAppendTypes.add(REPORT_SOURCE_INTEGRITY_FAULT);
      journal.failNextDurableInsert = true;
      await expect(engine.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("JOURNAL_APPEND_FAILED");
      expect(journal.failNextDurableInsert).toBe(false);
      expect(engine.recoveryFailClosedForTest()).toBe(true);
      expect(engine.state.overloadState).toBe("FAIL_CLOSED");
      expect(engine.state.grokActivity.acceptedSignals).toBe(0);
      expect(await journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).toHaveLength(1);
      expect(await journal.queryEvents({ types: ["STRATEGY_CHANGE_PROPOSED"] })).toEqual([]);
      expect(await journal.queryEvents({ types: [REPORT_SOURCE_INTEGRITY_FAULT] })).toEqual([]);
      expect(await journal.queryDurableTransitions({ types: [REPORT_SOURCE_INTEGRITY_FAULT], limit: 8 })).toEqual([]);
      expect(await journal.idempotencyCreatedAt("grok_signal_submission", `${submission.agentId}:${submission.submissionId}`)).toBeTruthy();
      journal.failAppendTypes.clear();
    } finally {
      await engine.stop();
    }
    expect((await journal.listCheckpoints()).length).toBe(checkpointsBefore);

    const restarted = paperEngine(journal);
    await restarted.start();
    try {
      expect(restarted.reportSourceIntegrityForTest()).toEqual([]);
      await restarted.registerTriggerForResearch(trigger);
      await expect(restarted.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
      await expect(restarted.submitGrokSignal(submission, submission.agentId)).rejects.toThrow("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
      expect(restarted.state.grokActivity.acceptedSignals).toBe(0);
      expect(restarted.state.grokActivity.duplicates).toBe(0);
    } finally {
      await restarted.stop();
    }
  });

  it("A: double-failure + crash before checkpoint → restart refuses seal/canonical", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      const checkpointsBefore = (await journal.listCheckpoints()).length;
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      journal.failAppendTypes.add(REPORT_SOURCE_INTEGRITY_FAULT);
      const lost = createJournalEvent({
        id: "grok-double-fail", traceId: "tr-double", type: "GROK_SIGNAL_ACCEPTED",
        aggregateType: "grok_signal", aggregateId: "sig-double",
        payload: { experimentId, signalId: "sig-double" },
      });
      await engine.persistEventForTest(lost);
      expect((await journal.queryEvents({ types: [REPORT_SOURCE_INTEGRITY_FAULT] })).length).toBe(0);
      expect((await journal.queryDurableTransitions({ types: [REPORT_SOURCE_INTEGRITY_FAULT], limit: 8 })).length).toBe(1);
      expect((await journal.listCheckpoints()).length).toBe(checkpointsBefore);
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-double-fail")).toBe(true);
    } finally {
      await engine.stop();
    }
    const restarted = paperEngine(journal);
    await restarted.start();
    try {
      expect(restarted.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-double-fail")).toBe(true);
      expect((await restarted.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
      await restarted.writeFinalReportForTest();
      expect(await journal.loadExperimentReport(restarted.experimentManifest()!.id)).toBeNull();
    } finally {
      await restarted.stop();
    }
  });

  it("B: 10k/100k unique failures stay hard-bounded and fail closed", { timeout: 60_000 }, async () => {
    const hundredK = Array.from({ length: 100_000 }, (_, i) => ({
      experimentId: "exp-bound", eventType: "TRIGGER_EVENT", eventId: `f-${i}`, reason: "x", at: "2026-09-01T00:00:00.000Z",
    }));
    const bounded = boundIntegrityFaults(hundredK, "exp-bound");
    expect(bounded).toEqual([expect.objectContaining({ eventId: INTEGRITY_OVERFLOW_EVENT_ID })]);
    expect(JSON.stringify(bounded).length).toBeLessThan(2_000);

    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("TRIGGER_EVENT");
      journal.failAppendTypes.add(REPORT_SOURCE_INTEGRITY_FAULT);
      for (let i = 0; i < 10_000; i += 1) {
        await engine.persistEventForTest(createJournalEvent({
          id: `trig-flood-${i}`, traceId: "tr-flood", type: "TRIGGER_EVENT",
          aggregateType: "trigger", aggregateId: `trig-flood-${i}`,
          payload: { experimentId, i },
        }));
      }
      const faults = engine.reportSourceIntegrityForTest().filter((fault) => fault.experimentId === experimentId);
      expect(faults.length).toBe(1);
      expect(faults[0]?.eventId).toBe(INTEGRITY_OVERFLOW_EVENT_ID);
      expect((await journal.queryEvents({ types: [REPORT_SOURCE_INTEGRITY_FAULT] })).length).toBeLessThanOrEqual(REPORT_SOURCE_INTEGRITY_HARD_CAP + 1);
      expect((await journal.queryDurableTransitions({ types: [REPORT_SOURCE_INTEGRITY_FAULT], limit: 256 })).length).toBeLessThanOrEqual(REPORT_SOURCE_INTEGRITY_HARD_CAP + 1);
      await engine.completeExperimentForTest();
      const checkpoint = (await journal.listCheckpoints()).at(-1);
      const payloadFaults = Array.isArray(checkpoint?.payload.reportSourceIntegrity) ? checkpoint!.payload.reportSourceIntegrity : [];
      expect(payloadFaults.length).toBe(1);
      expect(JSON.stringify(payloadFaults).length).toBeLessThan(2_000);
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
    } finally {
      await engine.stop();
    }
  });

  it("C: unrelated success cannot clear a latched fault", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      await engine.persistEventForTest(createJournalEvent({
        id: "grok-sticky", traceId: "tr-sticky", type: "GROK_SIGNAL_ACCEPTED",
        aggregateType: "grok_signal", aggregateId: "sig-sticky",
        payload: { experimentId },
      }));
      journal.failAppendTypes.delete("GROK_SIGNAL_ACCEPTED");
      await engine.persistEventForTest(ev("SYSTEM_HEALTH_EVENT", new Date().toISOString(), { experimentId }, "health-clear"));
      await engine.persistEventForTest(ev("TRIGGER_EVENT", new Date().toISOString(), { experimentId }, "trig-ok"));
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === "grok-sticky")).toBe(true);
      await engine.completeExperimentForTest();
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
    } finally {
      await engine.stop();
    }
  });

  it("D: verified repair is exact-id; overflow cannot be cleared by one proven row", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.failAppendTypes.add("TRIGGER_EVENT");
      const first = createJournalEvent({
        id: "trig-repair-1", traceId: "tr-repair", type: "TRIGGER_EVENT",
        aggregateType: "trigger", aggregateId: "trig-repair-1",
        payload: { experimentId },
      });
      await engine.persistEventForTest(first);
      journal.failAppendTypes.delete("TRIGGER_EVENT");
      expect(await engine.repairReportSourceIntegrityForTest(first)).toBe(true);
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === "trig-repair-1")).toBe(false);

      journal.failAppendTypes.add("TRIGGER_EVENT");
      for (let i = 0; i < REPORT_SOURCE_INTEGRITY_HARD_CAP + 2; i += 1) {
        await engine.persistEventForTest(createJournalEvent({
          id: `trig-ovf-${i}`, traceId: "tr-ovf", type: "TRIGGER_EVENT",
          aggregateType: "trigger", aggregateId: `trig-ovf-${i}`,
          payload: { experimentId },
        }));
      }
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === INTEGRITY_OVERFLOW_EVENT_ID)).toBe(true);
      journal.failAppendTypes.delete("TRIGGER_EVENT");
      const one = createJournalEvent({
        id: "trig-ovf-0", traceId: "tr-ovf", type: "TRIGGER_EVENT",
        aggregateType: "trigger", aggregateId: "trig-ovf-0",
        payload: { experimentId },
      });
      expect(await engine.repairReportSourceIntegrityForTest(one)).toBe(false);
      expect(engine.reportSourceIntegrityForTest().some((fault) => fault.eventId === INTEGRITY_OVERFLOW_EVENT_ID)).toBe(true);
      await engine.completeExperimentForTest();
      expect((await engine.ensureSourceSealForTest()).reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
    } finally {
      await engine.stop();
    }
  });
});

describe("Step-4 follow-up: seal recheck after drain + window-only work", () => {
  it("fault during drain refuses seal and leaves no EXPERIMENT_SOURCE_SEALED row", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    let release!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      journal.setAppendInFlightHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["GROK_SIGNAL_ACCEPTED"]);
      const lost = createJournalEvent({
        id: "grok-drain-fail", traceId: "tr-drain", type: "GROK_SIGNAL_ACCEPTED",
        aggregateType: "grok_signal", aggregateId: "sig-drain",
        payload: { experimentId, signalId: "sig-drain" },
      });
      const persist = engine.persistEventForTest(lost);
      await waitUntil(() => journal.inFlightAppends > 0);
      expect(engine.reportSourceIntegrityForTest()).toEqual([]);
      await engine.completeExperimentForTest();
      const sealPromise = engine.ensureSourceSealForTest();
      journal.failAppendTypes.add("GROK_SIGNAL_ACCEPTED");
      release();
      await persist;
      const seal = await sealPromise;
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("REPORT_SOURCE_INTEGRITY_UNRESOLVED");
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      expect(await journal.loadExperimentReport(experimentId)).toBeNull();
    } finally {
      release?.();
      journal.setAppendInFlightHoldForTest(null);
      if (!engine.state.draining) await engine.stop();
    }
  });

  it("COMPLETED+sealing with continuous post-window activity seals boundedly and keeps hash stable", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      await engine.persistEventForTest(ev("TRIGGER_EVENT", new Date().toISOString(), { experimentId }, "pre-window-trig"));
      await engine.completeExperimentForTest();
      let stop = false;
      let posted = 0;
      const flood = (async () => {
        while (!stop) {
          posted += 1;
          await engine.persistEventForTest(ev("SYSTEM_HEALTH_EVENT", new Date().toISOString(), { status: "OK" }, `post-health-${posted}`));
          await engine.persistEventForTest(createJournalEvent({
            id: `post-mcp-${posted}`, traceId: "tr-post", type: "MCP_INTERACTION",
            aggregateType: "mcp_client", aggregateId: "post-window",
            payload: { clientId: "post-window", method: "tools/call", status: "ok" },
          }));
          await engine.persistEventForTest(ev("DISCOVERY_RECONCILED", new Date().toISOString(), { markets: 1 }, `post-disc-${posted}`));
        }
      })();
      const started = Date.now();
      const seal = await engine.ensureSourceSealForTest();
      expect(Date.now() - started).toBeLessThan(5_000);
      expect(seal.sealed).toBe(true);
      stop = true;
      await flood;
      expect(posted).toBeGreaterThan(0);
      await engine.writeFinalReportForTest();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      const streamed = await streamExperimentSummary(journal, {
        id: experimentId,
        startedAt: engine.experimentManifest()?.startedAt,
        completedAt: engine.experimentManifest()?.completedAt,
      }, {
        status: "COMPLETED", reportKind: "final", metricsScope: "complete", complete: true, truncated: false,
        generatedAt: first!.generatedAt,
      });
      streamed.summary.complete = true;
      streamed.summary.truncated = false;
      expect(toDurableReportInput({ summary: streamed.summary, scan: streamed.scan }).contentHash).toBe(first!.contentHash);
      expect(streamed.scan.scanned).toBe(first!.sourceEventCount);
      expect(engine.state.engineAlive()).toBe(true);
    } finally {
      if (!engine.state.draining) await engine.stop();
    }
  });
});

describe("Step-4 follow-up: source-seal deadline covers acquired transaction", () => {
  it("acquire-hold analogue: txn obtained near deadline cannot later commit a timed-out seal", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    let release!: () => void;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const experimentId = engine.experimentManifest()!.id;
      engine.setSourceSealTimeoutForTest(200);
      journal.setAppendAcquireHoldForTest(new Promise<void>((resolve) => { release = resolve; }), ["EXPERIMENT_SOURCE_SEALED"]);
      await engine.completeExperimentForTest();
      const sealPromise = engine.ensureSourceSealForTest();
      await waitUntil(() => journal.inFlightAppends > 0);
      await new Promise((resolve) => setTimeout(resolve, 220));
      release();
      const seal = await sealPromise;
      expect(seal.sealed).toBe(false);
      expect(seal.reason).toBe("SOURCE_SEAL_TIMEOUT");
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      journal.setAppendAcquireHoldForTest(null);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      engine.setSourceSealTimeoutForTest(5_000);
      const retry = await engine.ensureSourceSealForTest();
      expect(retry.sealed).toBe(true);
      expect((await journal.queryEvents({ types: ["EXPERIMENT_SOURCE_SEALED"] })).length).toBe(1);
      await engine.writeFinalReportForTest();
      const first = await journal.loadExperimentReport(experimentId);
      expect(first && isCanonicalFinalReport(first)).toBe(true);
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(first!.contentHash);
        expect((await restarted.ensureSourceSealForTest()).sealed).toBe(true);
      } finally {
        await restarted.stop();
      }
    } finally {
      release?.();
      journal.setAppendAcquireHoldForTest(null);
      if (!engine.state.draining) await engine.stop();
    }
  });
});


