import { createElement } from "../apps/dashboard/node_modules/react/index.js";
import { renderToStaticMarkup } from "../apps/dashboard/node_modules/react-dom/server.node.js";
import { describe, expect, it } from "vitest";
import { DecisionIntelligence } from "../apps/dashboard/app/components/decision-intelligence.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

function snapshot(consensus: DashboardSnapshot["consensus"]): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-12T12:00:00.000Z",
    engine: { ready: true, draining: false, alive: true, uptimeSeconds: 5, queueDepth: 0, experimentId: "exp-1", experimentStatus: "IDLE", discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 0, overloadState: "NORMAL", journalDegraded: false, telemetryDegraded: false, newPaperEntryAllowed: true },
    balance: { startingBalance: "1000", cash: "1000", reservedCash: "0", positionValue: "0", equity: "1000", realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0", aiCost: "0", infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", highWaterMark: "1000", drawdown: "0", updatedAt: "2026-09-12T12:00:00.000Z" },
    positions: [], latestAgents: {}, consensus, risk: null, scanner: [], decisionFeed: [],
    grokActivity: { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null, byAgent: {} },
    recentTriggers: [], activationLatency: [], unattributedMcp: { lastAt: null, count: 0, lastTool: null }, discovery: { qualified: [], rejected: [] },
    health: { engineAlive: true, discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 0, polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, referenceFeeds: [], database: "UP", journalDegraded: false, telemetryDegraded: false, overloadState: "NORMAL", eventLatencyMs: 1, lastBtcMessageAt: null, unattributedMcp: { lastAt: null, count: 0, lastTool: null } },
    equityHistory: [],
  };
}

const decisionBase: NonNullable<DashboardSnapshot["consensus"]> = {
  consensusDirection: "FLAT",
  consensusConfidence: .51,
  agentAgreement: .75,
  signalFreshness: .9,
  conflicts: [],
  estimatedEdge: -.001,
  calculatedAt: "2026-09-12T12:00:00.000Z",
};

function render(snapshotValue: DashboardSnapshot): string {
  return renderToStaticMarkup(createElement(DecisionIntelligence, { snapshot: snapshotValue }));
}

describe("DecisionIntelligence authority rendering", () => {
  it("renders historical missing authority and optional analytics as unavailable", () => {
    const html = render(snapshot(decisionBase));
    expect(html).toContain("AUTHORITY UNAVAILABLE");
    expect(html).not.toContain("NATIVE_DETERMINISTIC_V1");
    expect(html).toContain("No feature vector detail in snapshot");
    expect(html).toContain("Fair probability");
    expect(html).toContain("Market probability");
    expect(html.match(/>—</g)?.length).toBeGreaterThanOrEqual(2);
  });

  it("renders the actual external authority without native styling text", () => {
    const html = render(snapshot({ ...decisionBase, authority: "EXTERNAL_CONSENSUS_V0" }));
    expect(html).toContain("EXTERNAL_CONSENSUS_V0");
    expect(html).not.toContain("NATIVE_DETERMINISTIC_V1");
  });

  it("renders native authority only when explicitly present", () => {
    const html = render(snapshot({ ...decisionBase, authority: "NATIVE_DETERMINISTIC_V1" }));
    expect(html).toContain("NATIVE_DETERMINISTIC_V1");
  });
});
