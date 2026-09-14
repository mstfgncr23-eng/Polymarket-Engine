import { ASTRA_BRAIN_AUTHORITY } from "@pm/core";
import { createJournalEvent, MemoryEventJournal } from "@pm/db";
import { describe, expect, it } from "vitest";
import { decisionActivityLabel, decisionActivityTone } from "../apps/dashboard/lib/dashboard-selectors.js";
import {
  installAstraLiveDecisionBridge,
  projectCanonicalAstraDecisionEvent,
  type LiveDecisionFeedItem,
} from "../apps/engine/src/astra/live-decision-bridge.js";

const at = "2026-09-14T14:45:00.000Z";
const trigger = {
  id: "trigger-live-1",
  traceId: "trace-live-1",
  marketId: "market-live-1",
  asset: "BTC",
};

function event(type: string, payload: Record<string, unknown>, suffix = type.toLowerCase()) {
  return createJournalEvent({
    id: `event-${suffix}`,
    traceId: trigger.traceId,
    type,
    aggregateType: "astra",
    aggregateId: trigger.id,
    occurredAt: new Date(at),
    payload,
  });
}

describe("canonical Astra -> live dashboard bridge", () => {
  it("projects exact gate/failure semantics without leaking bulky canonical payloads", () => {
    const gate = event("ASTRA_DECISION", {
      authority: ASTRA_BRAIN_AUTHORITY,
      accepted: false,
      gateReason: "NO_LIQUIDITY",
      marketId: trigger.marketId,
      asset: trigger.asset,
      featurePacket: { triggerId: trigger.id, giantBook: "must-not-reach-live-feed".repeat(200) },
      usage: { inputTokens: 50_000, outputTokens: 10_000 },
    }, "gate");

    const projected = projectCanonicalAstraDecisionEvent(gate, [trigger]);
    expect(projected).toMatchObject({
      id: gate.id,
      at,
      type: "ASTRA_DECISION",
      traceId: trigger.traceId,
      marketId: trigger.marketId,
      asset: "BTC",
      detail: { authority: ASTRA_BRAIN_AUTHORITY, accepted: false, gateReason: "NO_LIQUIDITY", triggerId: trigger.id },
    });
    expect(projected?.detail).not.toHaveProperty("featurePacket");
    expect(projected?.detail).not.toHaveProperty("usage");

    const failure = event("ASTRA_FAILURE", {
      authority: ASTRA_BRAIN_AUTHORITY,
      stage: "DEEP",
      reason: "DEEP_TIMEOUT",
    }, "failure");
    expect(projectCanonicalAstraDecisionEvent(failure, [trigger])?.detail).toMatchObject({
      authority: ASTRA_BRAIN_AUTHORITY,
      stage: "DEEP",
      reason: "DEEP_TIMEOUT",
    });
  });

  it("projects bounded successful decision/consensus detail and rejects non-Astra or batch learning noise", () => {
    const decision = event("ASTRA_DECISION", {
      authority: ASTRA_BRAIN_AUTHORITY,
      marketId: trigger.marketId,
      asset: trigger.asset,
      stages: ["FAST", "DEEP"],
      decision: {
        action: "BUY_UP",
        direction: "UP",
        confidence: 0.82,
        uncertainty: 0.11,
        rationaleCode: "EDGE_CONFIRMED",
        reasonCodes: ["EDGE_CONFIRMED", "LIQUIDITY_OK"],
      },
    }, "success");
    expect(projectCanonicalAstraDecisionEvent(decision, [trigger])?.detail).toMatchObject({
      authority: ASTRA_BRAIN_AUTHORITY,
      action: "BUY_UP",
      direction: "UP",
      confidence: 0.82,
      uncertainty: 0.11,
      rationaleCode: "EDGE_CONFIRMED",
      reason: "EDGE_CONFIRMED",
      stages: ["FAST", "DEEP"],
      reasonCodes: ["EDGE_CONFIRMED", "LIQUIDITY_OK"],
    });

    const consensus = event("CONSENSUS_DECISION", {
      authority: ASTRA_BRAIN_AUTHORITY,
      consensusDirection: "UP",
      consensusConfidence: 0.82,
      estimatedEdge: 0.025,
      triggerId: trigger.id,
    }, "consensus");
    expect(projectCanonicalAstraDecisionEvent(consensus, [trigger])?.detail).toMatchObject({
      authority: ASTRA_BRAIN_AUTHORITY,
      direction: "UP",
      confidence: 0.82,
      estimatedEdge: 0.025,
      triggerId: trigger.id,
    });

    const nativeConsensus = event("CONSENSUS_DECISION", { authority: "NATIVE_DETERMINISTIC_V1" }, "native");
    expect(projectCanonicalAstraDecisionEvent(nativeConsensus, [trigger])).toBeNull();
    const batch = event("ASTRA_FAILURE", { authority: ASTRA_BRAIN_AUTHORITY, stage: "BATCH", reason: "TIMEOUT" }, "batch");
    expect(projectCanonicalAstraDecisionEvent(batch, [trigger])).toBeNull();
  });

  it("keeps gate rejection and fail-closed decision semantics distinct in the dashboard", () => {
    const gate = projectCanonicalAstraDecisionEvent(event("ASTRA_DECISION", {
      authority: ASTRA_BRAIN_AUTHORITY,
      accepted: false,
      gateReason: "NO_LIQUIDITY",
    }, "label-gate"), [trigger])!;
    const failClosed = projectCanonicalAstraDecisionEvent(event("ASTRA_DECISION", {
      authority: ASTRA_BRAIN_AUTHORITY,
      accepted: false,
      decisionSource: "FAIL_CLOSED",
      failureReason: "DEEP_TIMEOUT",
      decision: { direction: "FLAT", rationaleCode: "NO_TRADE" },
    }, "label-fail-closed"), [trigger])!;

    expect(decisionActivityLabel(gate)).toBe("ASTRA GATE REJECTED");
    expect(decisionActivityTone(gate)).toBe("warning");
    expect(decisionActivityLabel(failClosed)).toBe("ASTRA FAIL-CLOSED");
    expect(decisionActivityTone(failClosed)).toBe("critical");
    expect(failClosed.detail.reason).toBe("DEEP_TIMEOUT");
  });

  it("publishes only after persistence succeeds, dedupes retries, and waits for appendMany completion", async () => {
    const journal = new MemoryEventJournal();
    const published: LiveDecisionFeedItem[] = [];
    const errors: Array<{ error: unknown; type: string }> = [];
    const uninstall = installAstraLiveDecisionBridge(journal, {
      recentTriggers: () => [trigger],
      publish: (item) => published.push(item),
      onError: (error, canonical) => errors.push({ error, type: canonical.type }),
    });

    try {
      const gate = event("ASTRA_DECISION", { authority: ASTRA_BRAIN_AUTHORITY, accepted: false, gateReason: "STALE", triggerId: trigger.id }, "persisted-gate");
      await journal.append(gate);
      expect(published.map((item) => item.id)).toEqual([gate.id]);

      // The journal treats an exact-id retry as idempotent; the live projection must too.
      await journal.append(gate);
      expect(published.map((item) => item.id)).toEqual([gate.id]);

      let release!: () => void;
      const hold = new Promise<void>((resolve) => { release = resolve; });
      journal.setAppendInFlightHoldForTest(hold, ["ASTRA_MODEL_INVOCATION"]);
      const invocation = event("ASTRA_MODEL_INVOCATION", { authority: ASTRA_BRAIN_AUTHORITY, stage: "FAST", status: "SUCCESS" }, "batch-invocation");
      const decision = event("ASTRA_DECISION", {
        authority: ASTRA_BRAIN_AUTHORITY,
        decision: { action: "NO_TRADE", direction: "FLAT", confidence: 0.7, uncertainty: 0.2, rationaleCode: "NO_EDGE" },
      }, "batch-decision");
      const pendingBatch = journal.appendMany([invocation, decision]);
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(published.map((item) => item.id)).toEqual([gate.id]);
      release();
      await pendingBatch;
      journal.setAppendInFlightHoldForTest(null);
      expect(published.map((item) => item.id)).toEqual([gate.id, invocation.id, decision.id]);

      const failed = event("ASTRA_FAILURE", { authority: ASTRA_BRAIN_AUTHORITY, reason: "CLIENT_UNAVAILABLE" }, "failed-append");
      journal.failAppendTypes.add("ASTRA_FAILURE");
      await expect(journal.append(failed)).rejects.toThrow("JOURNAL_APPEND_FAILED");
      expect(published.some((item) => item.id === failed.id)).toBe(false);
      expect(errors).toEqual([]);
    } finally {
      journal.setAppendInFlightHoldForTest(null);
      uninstall();
    }

    const afterUninstall = event("ASTRA_DECISION", { authority: ASTRA_BRAIN_AUTHORITY, accepted: false, gateReason: "NO_LIQUIDITY" }, "after-uninstall");
    await journal.append(afterUninstall);
    expect(published.some((item) => item.id === afterUninstall.id)).toBe(false);
  });

  it("isolates live projection failure after commit so observability cannot rewrite economic persistence semantics", async () => {
    const journal = new MemoryEventJournal();
    const failures: string[] = [];
    const canonical = event("ASTRA_DECISION", { authority: ASTRA_BRAIN_AUTHORITY, accepted: false, gateReason: "NO_LIQUIDITY" }, "sink-failure");
    const uninstall = installAstraLiveDecisionBridge(journal, {
      recentTriggers: () => [trigger],
      publish: () => { throw new Error("UI_SINK_FAILED"); },
      onError: (_error, failedEvent) => failures.push(failedEvent.id),
    });
    try {
      await expect(journal.append(canonical)).resolves.toBeUndefined();
      expect(await journal.readJournalEventById(canonical.id)).toMatchObject({ id: canonical.id, type: canonical.type });
      expect(failures).toEqual([canonical.id]);
    } finally {
      uninstall();
    }
  });
});
