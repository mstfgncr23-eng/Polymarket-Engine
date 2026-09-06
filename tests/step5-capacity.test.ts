import { describe, expect, it } from "vitest";
import { eventLoopUtilizationDelta, runSustainedCapacityHarness } from "../scripts/sustained-capacity-harness.ts";

describe("Step 5 sustained capacity harness", () => {
  it("reaches a bounded plateau under multi-market normal, burst, reference and reconnect traffic", async () => {
    const report = await runSustainedCapacityHarness({
      durationMs: 900, warmupMs: 150, sampleIntervalMs: 40, markets: 3,
      marketRatePerSec: 22, referenceRatePerSec: 8, dashboard: "connected",
    });
    expect(report.locality).toBe("local-synthetic");
    expect(report.workload.sourceEventsSent).toBeGreaterThan(0);
    expect(report.workload.burstEvents).toBeGreaterThan(0);
    expect(report.workload.referenceTicksSent).toBeGreaterThan(0);
    expect(report.workload.discoveryRefreshes).toBe(1);
    expect(report.invariants).toMatchObject({
      boundedIngress: true, boundedSupervisor: true, boundedJournalAdmission: true,
      boundedReferenceWindow: true, boundedSse: true, boundedRuntimeBookkeeping: true,
      rawMarketAndReferenceEventsPersisted: 0,
    });
    expect(report.plateau.metrics.triggerQueue.classification).not.toBe("monotonic_growth");
    expect(report.plateau.metrics.journalWaiting.classification).not.toBe("monotonic_growth");
    expect(report.plateau.metrics.ingressDepth.classification).not.toBe("monotonic_growth");
  }, 15_000);

  it("keeps admission bounded during slow journal writes and a slow dashboard client", async () => {
    const report = await runSustainedCapacityHarness({
      durationMs: 850, warmupMs: 150, sampleIntervalMs: 40, markets: 2,
      marketRatePerSec: 30, referenceRatePerSec: 10, dashboard: "slow", database: "delayed", databaseDelayMs: 4,
    });
    expect(report.invariants.boundedJournalAdmission).toBe(true);
    expect(report.invariants.boundedSse).toBe(true);
    expect(report.invariants.dashboardSlowClientDisconnected).toBe(true);
    expect(report.plateau.metrics.sseBufferedBytes.classification).not.toBe("monotonic_growth");
  }, 15_000);

  it("recovers from a bounded journal outage without persisting raw observations", async () => {
    const report = await runSustainedCapacityHarness({
      durationMs: 900, warmupMs: 150, sampleIntervalMs: 40, markets: 2,
      marketRatePerSec: 18, referenceRatePerSec: 8, dashboard: "disconnected", database: "unavailable",
    });
    expect(report.invariants.rawMarketAndReferenceEventsPersisted).toBe(0);
    expect(report.invariants.boundedIngress).toBe(true);
    expect(report.invariants.boundedSupervisor).toBe(true);
    expect(report.invariants.boundedJournalAdmission).toBe(true);
    expect(report.plateau.metrics.journalWaiting.classification).not.toBe("monotonic_growth");
  }, 15_000);

  it("exercises overflow -> gap -> full-book resync and returns to trusted processing", async () => {
    const report = await runSustainedCapacityHarness({
      durationMs: 1_600, warmupMs: 200, sampleIntervalMs: 40, markets: 2,
      marketRatePerSec: 18, referenceRatePerSec: 8, dashboard: "disconnected",
      database: "healthy", reconnectStorm: false, ingressOverflow: true,
    });
    expect(report.workload.sourceEventsRejected).toBeGreaterThan(0);
    expect(report.invariants.ingressOverflowObserved).toBe(true);
    expect(report.invariants.resyncRecovered).toBe(true);
    expect(report.invariants.finalMarketTruthTrusted).toBe(true);
  }, 15_000);

  it("runs multiple reconnect cycles without retaining stale generations", async () => {
    const report = await runSustainedCapacityHarness({
      durationMs: 5_000, warmupMs: 250, sampleIntervalMs: 50, markets: 2,
      marketRatePerSec: 16, referenceRatePerSec: 8, dashboard: "disconnected",
      reconnectStorm: true, reconnectCycles: 2,
    });
    expect(report.workload.reconnectsRequested).toBeGreaterThan(1);
    expect(report.workload.reconnectCyclesCompleted).toBeGreaterThan(1);
    expect(report.invariants.finalMarketTruthTrusted).toBe(true);
    expect(report.invariants.resyncRecovered).toBe(true);
    expect(report.invariants.boundedRuntimeBookkeeping).toBe(true);
  }, 15_000);

  it("uses absolute ELU snapshots and labels short memory runs insufficient", async () => {
    expect(eventLoopUtilizationDelta({ active: 100, idle: 100, utilization: 0.5 }, { active: 150, idle: 150, utilization: 0.5 }).utilization).toBe(0.5);
    expect(eventLoopUtilizationDelta({ active: 150, idle: 150, utilization: 0.5 }, { active: 180, idle: 220, utilization: 0.45 }).utilization).toBe(0.3);
    const report = await runSustainedCapacityHarness({ durationMs: 500, warmupMs: 100, sampleIntervalMs: 40, markets: 1, reconnectStorm: false, dashboard: "disconnected" });
    expect(report.verdict.status).toBe("NOT_READY");
    expect(report.verdict.blockers).toContain("INSUFFICIENT_LOCAL_EVIDENCE:memory");
    expect(report.verdict.localMemory).toBe("INSUFFICIENT_LOCAL_EVIDENCE");
  }, 15_000);
});
