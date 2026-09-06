import { describe, expect, it } from "vitest";
import { runSustainedCapacityHarness } from "../scripts/sustained-capacity-harness.ts";

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
});
