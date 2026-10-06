import { describe, expect, it, vi } from "vitest";
import { createJournalEvent } from "@pm/db";
import { buildExperimentSummary } from "../apps/engine/src/report.js";
import { book, drain, economicFixture, fixture, trigger } from "../scripts/native-hotpath-fixture.js";
import { paperFill } from "./paper-engine-testkit.js";

describe("Native V1 hot-path fossil removal", () => {
  it("preserves canonical decisions without allocating, cloning or journaling activation history", async () => {
    const s = await fixture();
    try {
      await s.market.emitSnapshot(book(s, "native-0", "UP", 1));
      await s.market.emitSnapshot(book(s, "native-0", "DOWN", 1));
      await drain(s.engine);
      const ensure = vi.spyOn(s.engine.activation, "ensureLatency");
      const snapshot = vi.spyOn(s.engine.activation, "snapshot");
      const update = vi.spyOn(s.engine.state, "setActivationLatency");
      await Promise.all(Array.from({ length: 32 }, (_, i) => s.engine.registerTriggerForResearch(trigger(s, i))));
      expect(ensure).not.toHaveBeenCalled();
      expect(snapshot).not.toHaveBeenCalled();
      expect(update).not.toHaveBeenCalled();
      const events = await s.journal.queryEvents({ types: ["TRIGGER_EVENT", "CONSENSUS_DECISION", "GROK_ACTIVATION_DECISION"] });
      expect(events.filter((e) => e.type === "TRIGGER_EVENT").length).toBeGreaterThanOrEqual(32);
      expect(events.filter((e) => e.type === "CONSENSUS_DECISION").length).toBeGreaterThanOrEqual(32);
      expect(events.some((e) => e.type === "GROK_ACTIVATION_DECISION")).toBe(false);
      expect(s.engine.state.snapshot().activationLatency).toEqual([]);
      expect(s.engine.state.snapshot()).not.toHaveProperty("activations");
      expect(s.engine.experimentManifest()?.modelConfiguration).toMatchObject({ authority: "NATIVE_DETERMINISTIC_V1", externalInference: false });
      // Unknown IDs and generic polls do not repopulate the attribution map.
      s.engine.noteMcpRequest("get_recent_triggers");
      s.engine.noteMcpRequest("get_recent_triggers", "unknown-trigger");
      expect(s.engine.activation.snapshot()).toEqual([]);
      expect(s.errors).toEqual([]);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it("recovers old activation payloads while preserving the ledger and durable cursor", async () => {
    let s = await fixture();
    const journal = s.journal;
    const historical = [{ triggerId: "historical-trigger", triggerCreatedAt: new Date(s.clock.ms).toISOString(),
      activationRequestedAt: null, activationSentAt: null, firstMcpRequestAt: null, firstAcceptedSignalAt: null }];
    try {
      await s.engine.applyObservedFill(paperFill("native-0", "UP", "0.60", {
        tokenId: "native-0-up", quantity: "5", timestamp: new Date(s.clock.ms).toISOString(), feeUsd: "0" }));
      const oldRow = createJournalEvent({ traceId: "historical-trace", type: "GROK_ACTIVATION_DECISION",
        aggregateType: "activation", aggregateId: "historical-trigger", payload: {
          reason: "NATIVE_DETERMINISTIC_ONLY", activate: false, authority: "NATIVE_DETERMINISTIC_V1",
          experimentId: s.engine.experimentManifest()!.id } });
      await journal.append(oldRow);
      const ledger = s.engine.ledger.snapshot(), position = structuredClone(s.engine.ledger.positions.get("native-0"));
      const cursor = s.engine.lastAppliedDurableCursorForTest();
      await s.engine.stop();
      const checkpoint = (await journal.loadLatestCheckpoint())!;
      expect(checkpoint).toBeTruthy();
      await journal.saveCheckpoint({ experimentId: checkpoint.experimentId, sequence: checkpoint.sequence + 1,
        payload: { ...checkpoint.payload, activationLatency: historical, grokSignals: [],
          activations: [{ triggerId: "historical-trigger", reason: "NATIVE_DETERMINISTIC_ONLY", grokWakeObserved: false }] } });
      s = await fixture(1, journal, false, s.clock);
      expect(s.engine.state.database).toBe("UP");
      expect(s.engine.ledger.snapshot()).toEqual(ledger);
      expect(s.engine.ledger.positions.get("native-0")).toEqual(position);
      expect(s.engine.lastAppliedDurableCursorForTest()).toBe(cursor);
      expect(s.engine.state.snapshot().activationLatency).toEqual(historical);
      const update = vi.spyOn(s.engine.state, "setActivationLatency");
      await s.engine.registerTriggerForResearch(trigger(s, 100));
      expect(update).not.toHaveBeenCalled();
      expect(s.engine.activation.snapshot()).toEqual(historical);
      expect(await journal.queryEvents({ types: ["GROK_ACTIVATION_DECISION"] })).toContainEqual(oldRow);
      const events = await journal.readAll();
      const experimentId = s.engine.experimentManifest()!.id;
      const before = buildExperimentSummary(events, experimentId);
      const after = buildExperimentSummary(events.filter((e) => e.type !== "GROK_ACTIVATION_DECISION"), experimentId);
      // Source counts reflect the rows actually scanned; economics remain identical.
      expect(before.decisionAccountingEventCount).toBe(after.decisionAccountingEventCount + 1);
      const { generatedAt: _beforeAt, decisionAccountingEventCount: _beforeCount, ...beforeSemantics } = before;
      const { generatedAt: _afterAt, decisionAccountingEventCount: _afterCount, ...afterSemantics } = after;
      expect(beforeSemantics).toEqual(afterSemantics);
    } finally { vi.restoreAllMocks(); await s.engine.stop(); }
  });

  it("exercises Native first-leg economics and .05/.02 profit trailing with a fixed stream", async () => {
    const result = await economicFixture() as { events: Array<{ type: string; payload: Record<string, unknown> }> };
    const orders = result.events.filter((e) => e.type === "PAPER_ORDER_CREATED");
    expect(orders.some((e) => e.payload.intent === "OPEN_FIRST_LEG")).toBe(true);
    expect(orders.some((e) => e.payload.intent === "HEDGE_SECOND_LEG" && e.payload.side === "BUY")).toBe(true);
  });
});
