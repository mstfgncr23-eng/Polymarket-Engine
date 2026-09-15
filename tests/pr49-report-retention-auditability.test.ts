import { describe, expect, it } from "vitest";
import { createJournalEvent, MemoryEventJournal } from "@pm/db";
import { type JournalEvent } from "@pm/core";
import {
  analyticsRollupHasDestructiveCoverage,
  hashAnalyticsRollup,
  loadVerifiedAnalyticsRollup,
  persistVerifiedAnalyticsRollup,
  streamExperimentSummary,
} from "../apps/engine/src/final-report.js";
import { ExperimentSummaryAccumulator, type DurableAccountingTransition } from "../apps/engine/src/report.js";

const EXPERIMENT = "pr49-audit";
const STARTED_AT = "2026-01-01T00:00:00.000Z";

function cost(sequence: number, operationKey: string, aiCost: string, infrastructureCost: string): DurableAccountingTransition {
  return {
    sequence, operationKey, type: "INFRASTRUCTURE_COST_ACCRUED", aggregateId: EXPERIMENT,
    occurredAt: new Date(`2026-01-01T00:00:0${sequence}.000Z`).toISOString(),
    payload: { experimentId: EXPERIMENT, aiCost, infrastructureCost, source: aiCost === "0" ? "INFRA" : "ASTRA" },
  };
}

function canonical(type: string, id: string, occurredAt: string, payload: Record<string, unknown>): JournalEvent {
  return createJournalEvent({
    id, traceId: payload.traceId as string ?? id, type, aggregateType: "astra", aggregateId: id,
    occurredAt: new Date(occurredAt), payload: { experimentId: EXPERIMENT, ...payload },
  });
}

function astraDecision(id: string, traceId: string, decision: Record<string, unknown>, extra: Record<string, unknown> = {}): JournalEvent {
  return canonical("ASTRA_DECISION", id, "2026-01-01T00:00:01.000Z", {
    traceId, authority: "ASTRA_BRAIN_V1", marketId: `market-${id}`, asset: "BTC", decision, ...extra,
  });
}

function providerDecision(id: string, action: "BUY_UP" | "BUY_DOWN" | "NO_TRADE", direction: "UP" | "DOWN" | "FLAT", stages = ["FAST"]): JournalEvent {
  return astraDecision(id, `trace-${id}`, {
    action, direction, fairProbabilityUp: 0.55, confidence: 0.8, uncertainty: 0.1,
    reasonCodes: ["BOOK_EDGE"], rationaleCode: "BOOK_EDGE", stages,
  }, { stages });
}

describe("PR49 report cost and auditability", () => {
  it("A: reconstructs a cost-only experiment without orders, fills, or PNL snapshot", async () => {
    const direct = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true, generatedAt: "2026-01-02T00:00:00.000Z" });
    direct.foldDurableTransition(cost(1, "astra-cost:1", "0.02752", "0"));
    direct.foldDurableTransition(cost(2, "infra:1", "0", "0.00526537"));
    const expected = direct.finalize();

    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({ operationKey: "astra-cost:1", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0.02752", infrastructureCost: "0" } }, 0);
    await journal.commitDurableTransition({ operationKey: "infra:1", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0", infrastructureCost: "0.00526537" } }, 1);
    const streamed = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true, generatedAt: "2026-01-02T00:00:00.000Z" });

    expect(streamed.summary.aiCostUsd).toBeCloseTo(0.02752, 10);
    expect(streamed.summary.estimatedInfrastructureCostUsd).toBeCloseTo(0.00526537, 10);
    expect(streamed.summary.economicPnlUsd).toBeCloseTo(-0.03278537, 10);
    expect(streamed.summary.economicCostCompleteness).toBe("COMPLETE");
    expect(streamed.summary.totalPaperOrders).toBe(0);
    expect(streamed.summary.totalFills).toBe(0);
    expect(streamed.summary.netTradingPnlUsd).toBe(0);
    expect({ ai: streamed.summary.aiCostUsd, infra: streamed.summary.estimatedInfrastructureCostUsd, economic: streamed.summary.economicPnlUsd })
      .toEqual({ ai: expected.aiCostUsd, infra: expected.estimatedInfrastructureCostUsd, economic: expected.economicPnlUsd });
  });

  it("B: folds cost prefix plus durable tail exactly once across a verified rollup boundary", async () => {
    const journal = new MemoryEventJournal();
    const fence = canonical("EXPERIMENT_STARTED", "start", STARTED_AT, { status: "RUNNING" });
    await journal.append(fence);
    await journal.commitDurableTransition({ operationKey: "cost-prefix", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0.02752", infrastructureCost: "0" } }, 0);
    const prefix = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "interim", metricsScope: "rolling", complete: false });
    const persisted = await persistVerifiedAnalyticsRollup(journal, {
      experimentId: EXPERIMENT, sourceCursor: prefix.durableCursor, sourceFenceId: prefix.scan.fence.through!.id,
      sourceFenceOccurredAt: prefix.scan.fence.through!.occurredAt, sourceEventCount: prefix.accumulatorSnapshot!.eventCount,
      sourceCommitSeq: prefix.scan.fence.commitSeqTo, sourceRecordedAt: prefix.scan.fence.recordedAtTo.toISOString(),
      stableRead: prefix.scan.fence.stable, sourceCoverageComplete: prefix.scan.complete,
      accumulator: prefix.accumulatorSnapshot! as unknown as Record<string, unknown>,
    });
    await journal.commitDurableTransition({ operationKey: "cost-tail", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0", infrastructureCost: "0.00526537" } }, prefix.durableCursor);

    const rebuilt = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "interim", metricsScope: "rolling", complete: false });
    expect(persisted?.sourceCursor).toBe(prefix.durableCursor);
    expect(rebuilt.summary.aiCostUsd).toBeCloseTo(0.02752, 10);
    expect(rebuilt.summary.estimatedInfrastructureCostUsd).toBeCloseTo(0.00526537, 10);
    expect(rebuilt.summary.economicPnlUsd).toBeCloseTo(-0.03278537, 10);
    expect(rebuilt.summary.aiCostUsd).toBe(prefix.summary.aiCostUsd);
      // The rollup prefix and tail are disjoint by durable sequence; a second
      // rebuild before the next rollup must remain identical.
    const repeated = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "interim", metricsScope: "rolling", complete: false });
    expect(repeated.summary.aiCostUsd).toBe(rebuilt.summary.aiCostUsd);
    expect(repeated.summary.estimatedInfrastructureCostUsd).toBe(rebuilt.summary.estimatedInfrastructureCostUsd);
  });

  it("C: restored accumulator plus a continued durable tail matches direct accumulation", () => {
    const first = cost(1, "cost-prefix", "0.02752", "0");
    const second = cost(2, "cost-tail", "0", "0.00526537");
    const direct = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true, generatedAt: "2026-01-02T00:00:00.000Z" });
    direct.foldDurableTransition(first); direct.foldDurableTransition(second);
    const restored = ExperimentSummaryAccumulator.fromSnapshot((() => {
      const prefix = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true, generatedAt: "2026-01-02T00:00:00.000Z" });
      prefix.foldDurableTransition(first);
      return prefix.serialize();
    })());
    restored.foldDurableTransition(second);
    expect(restored.finalize().aiCostUsd).toBe(direct.finalize().aiCostUsd);
    expect(restored.finalize().estimatedInfrastructureCostUsd).toBe(direct.finalize().estimatedInfrastructureCostUsd);
    expect(restored.finalize().economicPnlUsd).toBe(direct.finalize().economicPnlUsd);
  });

  it("C2: a compacted durable prefix cannot be mistaken for a complete zero-cost history", async () => {
    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({ operationKey: "compacted-cost", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0.02752", infrastructureCost: "0" } }, 0);
    await journal.compactDurableTransitionsThrough!(1);
    const rebuilt = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    expect(rebuilt.summary.economicCostCompleteness).toBe("UNKNOWN");
    expect(rebuilt.durableCostScanComplete).toBe(false);
  });

  it("does not fold a durable cost appended after the report high-water snapshot", async () => {
    const journal = new MemoryEventJournal();
    await journal.commitDurableTransition({ operationKey: "snapshot-cost", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0.02752", infrastructureCost: "0" } }, 0);
    const originalQuery = journal.queryDurableTransitions.bind(journal);
    let appended = false;
    const hookedJournal = journal as unknown as { queryDurableTransitions: typeof journal.queryDurableTransitions };
    hookedJournal.queryDurableTransitions = async (query) => {
      const page = await originalQuery(query);
      if (!appended) {
        appended = true;
        await journal.commitDurableTransition({ operationKey: "post-snapshot-cost", type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId: EXPERIMENT, payload: { experimentId: EXPERIMENT, aiCost: "0", infrastructureCost: "0.00526537" } }, 1);
      }
      return page;
    };
    const streamed = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    expect(streamed.durableCursor).toBe(1);
    expect(streamed.durableCostScanComplete).toBe(true);
    expect(streamed.summary.aiCostUsd).toBeCloseTo(0.02752, 10);
    expect(streamed.summary.estimatedInfrastructureCostUsd).toBe(0);
  });

  it("D/E: old v1 rows remain readable but cannot authorize deletion or invent audit evidence", async () => {
    const journal = new MemoryEventJournal();
    const decision = providerDecision("legacy-decision", "BUY_UP", "UP");
    await journal.append(decision);
    const streamed = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    const current = streamed.accumulatorSnapshot! as unknown as Record<string, unknown>;
    const legacy = { ...current };
    delete legacy.analyticsCoverage;
    delete legacy.astraDecisionAudit;
    delete legacy.operatingAiCostUsd;
    delete legacy.operatingInfrastructureCostUsd;
    delete legacy.operatingCostTransitionCount;
    delete legacy.operatingCostSourceCursor;
    delete legacy.operatingCostCoverage;
    const row = await journal.persistAnalyticsRollup!({
      experimentId: EXPERIMENT, rollupKind: "canonical", schemaVersion: 1, sourceCursor: 0,
      sourceFenceId: decision.id, sourceFenceOccurredAt: decision.occurredAt, sourceEventCount: 1,
      sourceCommitSeq: streamed.scan.fence.commitSeqTo!, sourceRecordedAt: streamed.scan.fence.recordedAtTo.toISOString(),
      stableRead: true, previousContentHash: null, accumulator: legacy, contentHash: hashAnalyticsRollup(legacy), verified: true,
      createdAt: "2026-01-02T00:00:00.000Z",
    });
    expect(await loadVerifiedAnalyticsRollup(journal, EXPERIMENT)).not.toBeNull();
    expect(analyticsRollupHasDestructiveCoverage(row)).toBe(false);
    const deleted = await journal.deleteCanonicalEventsThrough!({
      experimentId: EXPERIMENT, limit: 10, sourceCursor: 0, sourceCommitSeq: row.sourceCommitSeq,
      occurredBefore: new Date(row.sourceFenceOccurredAt!), sourceRecordedAt: new Date(row.sourceRecordedAt!),
    });
    expect(deleted.deleted).toBe(0);
    expect(analyticsRollupHasDestructiveCoverage({
      ...row,
      accumulator: {
        ...legacy,
        analyticsCoverage: { version: 1, economicCostComplete: true, decisionAuditComplete: true, decisionAuditOverflow: false },
      },
    })).toBe(false);
    const rebuilt = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    expect(rebuilt.summary.astraDecisionAudit.providerDecisionCount).toBe(0);
    expect(rebuilt.summary.astraDecisionAudit.coverage).toBe("UNKNOWN");
  });

  it("refuses a rollup whose source fence belongs to another experiment", async () => {
    const journal = new MemoryEventJournal();
    const current = canonical("RISK_EVALUATION", "current-fence", "2026-01-01T00:00:01.000Z", { accepted: true });
    const other = createJournalEvent({
      id: "other-fence", traceId: "other-fence", type: "RISK_EVALUATION", aggregateType: "astra", aggregateId: "other-fence",
      occurredAt: new Date("2026-01-01T00:00:02.000Z"), payload: { experimentId: "other-experiment", accepted: true },
    });
    await journal.appendMany([current, other]);
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    acc.fold(current);
    const refused = await persistVerifiedAnalyticsRollup(journal, {
      experimentId: EXPERIMENT, sourceCursor: 0, sourceFenceId: other.id, sourceFenceOccurredAt: other.occurredAt,
      sourceEventCount: 1, sourceCommitSeq: 2, sourceRecordedAt: other.recordedAt, stableRead: true,
      sourceCoverageComplete: true, accumulator: acc.serialize() as unknown as Record<string, unknown>,
    });
    expect(refused).toBeNull();
  });

  it("keeps economic completeness UNKNOWN when a dispatched provider outcome is unknown", () => {
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    acc.foldDurableTransition(cost(1, "known-cost", "0.02752", "0"));
    acc.fold(canonical("ASTRA_PROVIDER_DISPATCH_AUTHORIZED", "unknown-authorized", "2026-01-01T00:00:01.000Z", {
      dispatchId: "unknown:FAST", stage: "FAST", authority: "ASTRA_BRAIN_V1",
    }));
    acc.fold(canonical("ASTRA_PROVIDER_DISPATCH_STARTED", "unknown-started", "2026-01-01T00:00:01.100Z", {
      dispatchId: "unknown:FAST", stage: "FAST", authority: "ASTRA_BRAIN_V1",
    }));
    acc.fold(canonical("ASTRA_FAILURE", "unknown-failure", "2026-01-01T00:00:02.000Z", {
      dispatchId: "unknown:FAST", stage: "FAST", authority: "ASTRA_BRAIN_V1", dispatchState: "DISPATCHED_UNKNOWN", reason: "TIMEOUT",
    }));
    const summary = acc.finalize();
    expect(summary.providerCostCompleteness).toBe("NONE");
    expect(summary.economicCostCompleteness).toBe("UNKNOWN");
  });

  it("F–H: provider actions are counted separately from fail-closed, hedge, and gate outcomes", () => {
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    acc.fold(providerDecision("buy-up", "BUY_UP", "UP"));
    acc.fold(providerDecision("buy-down", "BUY_DOWN", "DOWN", ["FAST", "DEEP"]));
    acc.fold(providerDecision("no-trade", "NO_TRADE", "FLAT"));
    acc.fold(astraDecision("fail-closed", "fail-trace", { action: "NO_TRADE", direction: "FLAT", reasonCodes: ["TIMEOUT"], rationaleCode: "TIMEOUT" }, { decisionSource: "FAIL_CLOSED", failureReason: "TIMEOUT", stages: ["FAST", "DEEP"] }));
    acc.fold(astraDecision("hedge", "hedge-trace", {}, { bypass: "DETERMINISTIC_HEDGE", reason: "EXISTING_UNPAIRED_POSITION" }));
    acc.fold(astraDecision("gate", "gate-trace", {}, { accepted: false, gateReason: "STALE_BOOK" }));
    acc.fold(canonical("ASTRA_FAILURE", "pre-failure", "2026-01-01T00:00:02.000Z", { traceId: "pre-trace", authority: "ASTRA_BRAIN_V1", stage: "FAST", dispatchState: "NOT_DISPATCHED", reason: "CLIENT_UNAVAILABLE" }));
    acc.fold(canonical("ASTRA_BUDGET_BLOCK", "pre-budget", "2026-01-01T00:00:02.500Z", { traceId: "budget-trace", authority: "ASTRA_BRAIN_V1", stage: "FAST", reason: "ROLLING_HOUR_CAP" }));
    acc.fold(canonical("RISK_EVALUATION", "risk-buy-up", "2026-01-01T00:00:03.000Z", { traceId: "trace-buy-up", accepted: false, vetoes: ["MAX_DRAWDOWN"] }));
    acc.fold(canonical("BOOK_REVALIDATION_REJECTED", "revalidation-buy-up", "2026-01-01T00:00:04.000Z", { traceId: "trace-buy-up", reason: "CURRENT_BOOK_NOT_EXECUTABLE" }));
    acc.fold(canonical("PAPER_ORDER_CREATED", "order-buy-up", "2026-01-01T00:00:05.000Z", { traceId: "trace-buy-up", orderType: "TAKER" }));
    const audit = acc.finalize().astraDecisionAudit;
    expect(audit.providerDecisionCount).toBe(3);
    expect(audit.buyUpCount).toBe(1);
    expect(audit.buyDownCount).toBe(1);
    expect(audit.noTradeCount).toBe(1);
    expect(audit.directionUpCount).toBe(1);
    expect(audit.directionDownCount).toBe(1);
    expect(audit.directionFlatCount).toBe(1);
    expect(audit.fastOnlyCount).toBe(2);
    expect(audit.deepDecisionCount).toBe(1);
    expect(audit.failClosedCount).toBe(1);
    expect(audit.deterministicHedgeBypassCount).toBe(1);
    expect(audit.gateRejectedCount).toBe(1);
    expect(audit.preProviderRejectedCount).toBe(2);
    expect(audit.digests.find((item) => item.id === "buy-up")?.outcome).toBe("ORDER_CREATED");
    expect(audit.digests.find((item) => item.id === "gate")?.outcome).toBe("GATE_REJECTED");
    expect(audit.digests.find((item) => item.id === "fail-closed")?.source).toBe("FAIL_CLOSED");
  });

  it("I: FAST+DEEP is one economic decision with separate deep observability", () => {
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    acc.fold(providerDecision("deep-once", "BUY_DOWN", "DOWN", ["FAST", "DEEP"]));
    acc.fold(canonical("ASTRA_MODEL_INVOCATION", "fast-invocation", "2026-01-01T00:00:02.000Z", { traceId: "deep-once", authority: "ASTRA_BRAIN_V1", dispatchId: "deep-once:FAST", stage: "FAST", usage: { costUsd: 0.01 } }));
    acc.fold(canonical("ASTRA_MODEL_INVOCATION", "deep-invocation", "2026-01-01T00:00:03.000Z", { traceId: "deep-once", authority: "ASTRA_BRAIN_V1", dispatchId: "deep-once:DEEP", stage: "DEEP", usage: { costUsd: 0.02 } }));
    const summary = acc.finalize();
    expect(summary.astraDecisionAudit.providerDecisionCount).toBe(1);
    expect(summary.astraDecisionAudit.deepDecisionCount).toBe(1);
    expect(summary.astraDecisionAudit.fastOnlyCount).toBe(0);
    expect(summary.modelInvocationCounts.ASTRA_FAST).toBe(1);
    expect(summary.modelInvocationCounts.ASTRA_DEEP).toBe(1);
  });

  it("J: decision digest and reason evidence remain hard-bounded with explicit overflow", () => {
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    for (let index = 0; index < 300; index += 1) {
      acc.fold(astraDecision(`overflow-${index}`, `overflow-trace-${index}`, {
        action: "NO_TRADE", direction: "FLAT", reasonCodes: [`REASON_${index}`], rationaleCode: `REASON_${index}`,
      }));
    }
    const summary = acc.finalize();
    expect(summary.astraDecisionAudit.providerDecisionCount).toBe(300);
    expect(summary.astraDecisionAudit.digests).toHaveLength(256);
    expect(summary.astraDecisionAudit.overflow).toBe(true);
    expect(summary.astraDecisionAudit.coverage).toBe("UNKNOWN");
    expect(summary.astraDecisionAudit.maxDigestCount).toBe(256);
    expect(acc.highWaterMarkBytes).toBeLessThan(300_000);
    expect(JSON.stringify(acc.serialize()).length).toBeLessThan(300_000);
  });

  it("K: covered Astra decision detail can be deleted and reconstructed from the verified rollup", async () => {
    const journal = new MemoryEventJournal();
    const decision = providerDecision("compaction-decision", "NO_TRADE", "FLAT");
    const fence = canonical("ASTRA_COUNTERFACTUAL", "compaction-fence", "2026-01-01T00:00:02.000Z", { available: false, reason: "EXECUTION_LINK_NOT_REPRODUCIBLE" });
    await journal.appendMany([decision, fence]);
    const streamed = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    const rollup = await persistVerifiedAnalyticsRollup(journal, {
      experimentId: EXPERIMENT, sourceCursor: streamed.durableCursor, sourceFenceId: fence.id,
      sourceFenceOccurredAt: fence.occurredAt, sourceEventCount: streamed.accumulatorSnapshot!.eventCount,
      sourceCommitSeq: streamed.scan.fence.commitSeqTo, sourceRecordedAt: streamed.scan.fence.recordedAtTo.toISOString(),
      stableRead: streamed.scan.fence.stable, sourceCoverageComplete: streamed.scan.complete,
      accumulator: streamed.accumulatorSnapshot! as unknown as Record<string, unknown>,
    });
    expect(rollup && analyticsRollupHasDestructiveCoverage(rollup)).toBe(true);
    const deleted = await journal.deleteCanonicalEventsThrough!({
      experimentId: EXPERIMENT, limit: 10, sourceCursor: rollup!.sourceCursor, sourceCommitSeq: rollup!.sourceCommitSeq,
      occurredBefore: new Date(rollup!.sourceFenceOccurredAt!), sourceRecordedAt: new Date(rollup!.sourceRecordedAt!),
    });
    expect(deleted.deleted).toBe(2);
    expect(await journal.readJournalEventById(decision.id)).toBeNull();
    const rebuilt = await streamExperimentSummary(journal, { id: EXPERIMENT, startedAt: STARTED_AT }, { reportKind: "final", metricsScope: "complete", complete: true });
    expect(rebuilt.summary.astraDecisionAudit.providerDecisionCount).toBe(1);
    expect(rebuilt.summary.astraDecisionAudit.noTradeCount).toBe(1);
    expect(rebuilt.summary.astraDecisionAudit.digests[0]?.reasonCodes).toContain("BOOK_EDGE");
  });

  it("L: execution counterfactuals distinguish no candidate, gate rejection, and order creation", () => {
    const acc = new ExperimentSummaryAccumulator(EXPERIMENT, { reportKind: "final", metricsScope: "complete", complete: true });
    acc.fold(providerDecision("no-candidate", "NO_TRADE", "FLAT"));
    acc.fold(canonical("ASTRA_COUNTERFACTUAL", "no-candidate-outcome", "2026-01-01T00:00:02.000Z", { traceId: "trace-no-candidate", executionOutcome: "NO_EXECUTABLE_CANDIDATE" }));
    acc.fold(providerDecision("gate-outcome", "BUY_UP", "UP"));
    acc.fold(canonical("ASTRA_COUNTERFACTUAL", "gate-outcome-event", "2026-01-01T00:00:03.000Z", { traceId: "trace-gate-outcome", executionOutcome: "EXECUTION_GATE_REJECTED" }));
    acc.fold(providerDecision("order-outcome", "BUY_DOWN", "DOWN"));
    acc.fold(canonical("ASTRA_COUNTERFACTUAL", "order-outcome-event", "2026-01-01T00:00:04.000Z", { traceId: "trace-order-outcome", executionOutcome: "ORDER_CREATED" }));
    const digests = acc.finalize().astraDecisionAudit.digests;
    expect(digests.find((item) => item.id === "no-candidate")?.outcome).toBe("NO_EXECUTABLE_CANDIDATE");
    expect(digests.find((item) => item.id === "gate-outcome")?.outcome).toBe("EXECUTION_GATE_REJECTED");
    expect(digests.find((item) => item.id === "order-outcome")?.outcome).toBe("ORDER_CREATED");
  });
});
