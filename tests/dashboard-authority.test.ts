import { describe, expect, it } from "vitest";
import {
  ASTRA_AUTHORITY,
  NATIVE_AUTHORITY,
  decisionAuthorityLabel,
  decisionAuthorityTone,
  missionEvents,
  pulseToken,
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

const baseConsensus: NonNullable<DashboardSnapshot["consensus"]> = {
  consensusDirection: "UP",
  consensusConfidence: 0.8,
  agentAgreement: 0.9,
  signalFreshness: 1,
  conflicts: [],
  estimatedEdge: 0.02,
  calculatedAt: "2026-09-12T00:00:00.000Z",
};

describe("Astra / Native decision authority presentation", () => {
  it("treats ASTRA_BRAIN_V1 as healthy with raw authority label", () => {
    const consensus = { ...baseConsensus, authority: ASTRA_AUTHORITY };
    expect(decisionAuthorityLabel(consensus)).toBe("ASTRA_BRAIN_V1");
    expect(decisionAuthorityTone(consensus)).toBe("healthy");
  });

  it("keeps historical Native healthy", () => {
    const consensus = { ...baseConsensus, authority: NATIVE_AUTHORITY };
    expect(decisionAuthorityLabel(consensus)).toBe("NATIVE_DETERMINISTIC_V1");
    expect(decisionAuthorityTone(consensus)).toBe("healthy");
  });

  it("marks missing authority on a consensus object as unavailable/warning", () => {
    expect(decisionAuthorityLabel(baseConsensus)).toBe("AUTHORITY UNAVAILABLE");
    expect(decisionAuthorityTone(baseConsensus)).toBe("warning");
  });

  it("surfaces unknown authority strings without inventing a substitute", () => {
    const consensus = { ...baseConsensus, authority: "EXTERNAL_CONSENSUS_V0" };
    expect(decisionAuthorityLabel(consensus)).toBe("EXTERNAL_CONSENSUS_V0");
    expect(decisionAuthorityTone(consensus)).toBe("warning");
  });

  it("keeps null consensus as dash / neutral", () => {
    expect(decisionAuthorityLabel(null)).toBe("—");
    expect(decisionAuthorityTone(null)).toBe("neutral");
  });

  it("includes ASTRA_DECISION in mission events and core pulse matching", () => {
    const data = snapshot({
      decisionFeed: [
        { id: "astra-1", at: "2026-09-12T00:00:02.000Z", type: "ASTRA_DECISION", traceId: "trace-a", marketId: "market-1", asset: "BTC", detail: { direction: "UP" } },
      ],
    });
    expect(missionEvents(data).map((event) => event.id)).toEqual(["astra-1"]);
    expect(pulseToken(data, "core")).toBe("astra-1");
  });
});