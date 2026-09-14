import { describe, expect, it } from "vitest";
import {
  equityPoints,
  eventSummary,
  formatBasisPoints,
  formatMoney,
  formatPercent,
  decisionAuthorityLabel,
  decisionAuthorityTone,
  missionEvents,
  nativeDecisionEvents,
  pulseToken,
  referenceTone,
  executionEvents,
  triggerEvents,
} from "../apps/dashboard/lib/dashboard-selectors.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

function snapshot(overrides: Partial<DashboardSnapshot> = {}): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-12T00:00:00.000Z",
    engine: { ready: true, draining: false, alive: true, uptimeSeconds: 5, queueDepth: 0, experimentId: "exp-1", experimentStatus: "RUNNING", discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 1, overloadState: "NORMAL", journalDegraded: false, telemetryDegraded: false, newPaperEntryAllowed: true },
    balance: { startingBalance: "1000", cash: "980", reservedCash: "10", positionValue: "20", equity: "1000", realizedPnl: "1", unrealizedPnl: "2", grossPnl: "3", fees: "0.10", slippage: "0.20", aiCost: "0", infrastructureCost: "0", netTradingPnl: "2.70", netEconomicPnl: "2.70", highWaterMark: "1000", drawdown: "0", updatedAt: "2026-09-12T00:00:00.000Z" },
    positions: [], latestAgents: {}, consensus: null, risk: null, scanner: [], decisionFeed: [], grokActivity: { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null, byAgent: {} }, recentTriggers: [], activations: [], activationLatency: [], unattributedMcp: { lastAt: null, count: 0, lastTool: null }, discovery: { qualified: [], rejected: [] }, health: { engineAlive: true, discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 1, polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, referenceFeeds: [], database: "UP", journalDegraded: false, telemetryDegraded: false, overloadState: "NORMAL", eventLatencyMs: 4, lastBtcMessageAt: null, unattributedMcp: { lastAt: null, count: 0, lastTool: null } }, equityHistory: [],
    ...overrides,
  };
}

describe("dashboard data selectors", () => {
  it("keeps event families derived from the authoritative decision feed", () => {
    const data = snapshot({ decisionFeed: [
      { id: "trigger", at: "2026-09-12T00:00:01.000Z", type: "TRIGGER_LEVEL_2", traceId: "trace-1", marketId: "market-1", asset: "BTC", detail: { kind: "REFERENCE_MOVE", direction: "UP" } },
      { id: "decision", at: "2026-09-12T00:00:02.000Z", type: "NATIVE_DECISION_COMPLETE", traceId: "trace-2", marketId: "market-1", detail: { direction: "UP" } },
      { id: "astra", at: "2026-09-12T00:00:02.500Z", type: "ASTRA_DECISION", traceId: "trace-2b", marketId: "market-1", detail: { direction: "UP" } },
      { id: "fill", at: "2026-09-12T00:00:03.000Z", type: "PAPER_FILL", traceId: "trace-3", marketId: "market-1", detail: { fillId: "fill-1", orderId: "order-1", price: "0.40", quantity: "2", role: "TAKER" } },
      { id: "noise", at: "2026-09-12T00:00:04.000Z", type: "GROK_SIGNAL_ACCEPTED", traceId: "trace-4", marketId: null, detail: {} },
    ] });
    expect(triggerEvents(data).map((event) => event.id)).toEqual(["trigger"]);
    expect(nativeDecisionEvents(data).map((event) => event.id)).toEqual(["decision"]);
    expect(executionEvents(data).map((event) => event.id)).toEqual(["fill"]);
    expect(missionEvents(data).map((event) => event.id)).toEqual(["trigger", "decision", "astra", "fill"]);
    expect(pulseToken(data, "core")).toBe("decision");
    expect(eventSummary(executionEvents(data)[0]!)).toContain("fill fill-1");
  });

  it("uses dynamic reference feed states and preserves empty-state semantics", () => {
    const connected = snapshot({ health: { ...snapshot().health, referenceFeeds: [{ asset: "BTC", state: "CONNECTED", lastMessageAt: null, reconnects: 0 }] } });
    const stale = snapshot({ health: { ...snapshot().health, referenceFeeds: [{ asset: "BTC", state: "STALE", lastMessageAt: null, reconnects: 2 }] } });
    expect(referenceTone(connected)).toBe("healthy");
    expect(referenceTone(stale)).toBe("warning");
    expect(referenceTone(snapshot())).toBe("neutral");
  });

  it("never infers native authority for historical or incomplete consensus rows", () => {
    const native: NonNullable<DashboardSnapshot["consensus"]> = { consensusDirection: "UP", consensusConfidence: .8, agentAgreement: .9, signalFreshness: 1, conflicts: [], estimatedEdge: .02, calculatedAt: "2026-09-12T00:00:00.000Z", authority: "NATIVE_DETERMINISTIC_V1" };
    const external = { ...native, authority: "EXTERNAL_CONSENSUS_V0" };
    const { authority: _historicalAuthority, ...historical } = native;
    expect(decisionAuthorityLabel(native)).toBe("NATIVE_DETERMINISTIC_V1");
    expect(decisionAuthorityTone(native)).toBe("healthy");
    expect(decisionAuthorityLabel(external)).toBe("EXTERNAL_CONSENSUS_V0");
    expect(decisionAuthorityTone(external)).toBe("warning");
    expect(decisionAuthorityLabel(historical)).toBe("AUTHORITY UNAVAILABLE");
    expect(decisionAuthorityTone(historical)).toBe("warning");
  });

  it("formats real scalar values without inventing fallbacks", () => {
    expect(formatMoney("12.30")).toBe("$12.30");
    expect(formatMoney(null)).toBe("—");
    expect(formatPercent(.425)).toBe("42.5%");
    expect(formatPercent(undefined)).toBe("—");
    expect(formatBasisPoints(.0025)).toBe("25bp");
    expect(formatBasisPoints(undefined)).toBe("—");
    expect(equityPoints([{ at: "a", equity: "1", drawdown: "0" }])).toBeNull();
  });
});
