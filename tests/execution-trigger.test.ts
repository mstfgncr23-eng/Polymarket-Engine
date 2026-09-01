import { describe, expect, it } from "vitest";
import { LiveExecutionDisabledAdapter, TriggerEngine, createRestingMakerOrder, evolveMakerOrder, executePaperTaker, triggerConfigSchema } from "@pm/core";
import { order, snapshot } from "./fixtures.js";

describe("paper execution", () => {
  it("taker fills only against sufficient post-latency depth", () => {
    const result = executePaperTaker({ order: order(), snapshotAfterLatency: snapshot(), measuredLatencyMs: 150 });
    expect(result.fillPrice).toBe("0.49");
    expect(result.latencyMs).toBe(150);
    expect(() => executePaperTaker({ order: order("TAKER", { quantity: "1000" }), snapshotAfterLatency: snapshot(), measuredLatencyMs: 1 })).toThrow("INSUFFICIENT_DEPTH");
  });

  it("maker never instantly fills and consumes queue conservatively on a later event", () => {
    const maker = order("MAKER", { quantity: "10" });
    const first = snapshot({ bids: [{ price: "0.47", quantity: "12" }], sequence: 1 });
    const resting = createRestingMakerOrder(maker, first);
    expect(resting.queueAhead).toBe("12");
    expect(evolveMakerOrder(resting, first, snapshot({ bids: [{ price: "0.47", quantity: "5" }], sequence: 2, receivedAt: "2026-08-31T12:00:01.000Z" })).fill).toBeNull();
    const result = evolveMakerOrder({ ...resting, queueAhead: "2" }, first, snapshot({ bids: [{ price: "0.47", quantity: "2" }], sequence: 2, receivedAt: "2026-08-31T12:00:01.000Z" }));
    expect(result.fill?.quantity).toBe("4");
    expect(result.fill?.fillConfidence).toBe(0.5);
  });

  it("hard-disables all live submission", async () => {
    await expect(new LiveExecutionDisabledAdapter().submit(order(), snapshot())).rejects.toThrow("LIVE_TRADING_DISABLED");
  });
});

describe("trigger dedupe", () => {
  it("suppresses duplicated market conditions during cooldown", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 60_000, imbalanceThreshold: 0.5 }));
    const book = snapshot({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] });
    expect(engine.ingestMarket(book).length).toBeGreaterThan(0);
    expect(engine.ingestMarket({ ...book, eventId: "duplicate", sequence: 2 }).length).toBe(0);
  });
});
