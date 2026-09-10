import { describe, expect, it } from "vitest";
import { PR31_CANDIDATE, runEnvelopeOnce } from "../scripts/pr31-production-envelope-harness.ts";

describe("PR31 production-shaped falsification seam", () => {
  it("uses the real transport, adapter, Engine snapshot path, and TriggerEngine for a deterministic mixed probe", async () => {
    const result = await runEnvelopeOnce({
      scenario: {
        segments: [{ label: "probe", durationMs: 1_000, ratePerSec: 1_000 }],
      },
    });

    expect(PR31_CANDIDATE).toEqual({ maxDeliveryEventsPerSlice: 128, maxDeliverySliceMs: 0.5 });
    expect(result.pass).toBe(true);
    expect(result.initialBooksAttempted).toBe(80);
    expect(result.initialBooksAdmitted).toBe(80);
    expect(result.initialBooksDelivered).toBe(80);
    expect(result.initialBooksProcessed).toBe(80);
    expect(result.allInitialBooksProcessed).toBe(true);
    expect(result.workloadEventsScheduled).toBe(1_000);
    expect(result.workloadEventsAttempted).toBe(1_000);
    expect(result.workloadEventsAdmitted).toBe(1_000);
    expect(result.workloadEventsDelivered).toBe(1_000);
    expect(result.workloadEventsProcessed).toBe(1_000);
    expect(result.workloadSourceCountsDelivered).toEqual({
      book: 13,
      price_change: 981,
      last_trade_price: 6,
      total: 1_000,
    });
    expect(result.workloadPriceChangeEntriesDelivered).toBe(1_962);
    expect(result.workloadNormalizedSnapshots).toBe(1_975);
    expect(result.workloadNormalizedSnapshotsExpected).toBe(1_975);
    expect(result.duplicateEvents).toBe(0);
    expect(result.fifoViolations).toBe(0);
    expect(result.perTokenOrderingViolations).toBe(0);
    expect(result.loss).toBe(0);
    expect(result.resyncs).toBe(0);
    expect(result.unsynchronizedTokens).toBe(0);
    expect(result.queueFinalDepth).toBe(0);
    expect(result.queueFinalBytes).toBe(0);
    expect(result.fairnessProbeCallbacksWhileBacklogged).toBeGreaterThan(0);
    expect(result.engineOnMarketSnapshot.count).toBe(80 + 1_975);
    expect(result.triggerEngineIngestMarket.count).toBe(80 + 1_975);
    expect(result.adapterNormalization.count).toBe(80 + 1_975);
  }, 30_000);
});
