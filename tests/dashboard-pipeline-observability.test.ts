import { createElement } from "../apps/dashboard/node_modules/react/index.js";
import { renderToStaticMarkup } from "../apps/dashboard/node_modules/react-dom/server.node.js";
import { describe, expect, it } from "vitest";
import { DecisionIntelligence } from "../apps/dashboard/app/components/decision-intelligence.js";
import { RiskPanel } from "../apps/dashboard/app/components/risk-panel.js";
import { ExecutionStream } from "../apps/dashboard/app/components/execution-stream.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

function baseSnapshot(overrides: Partial<DashboardSnapshot> = {}): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-14T12:20:00.000Z",
    engine: { ready: true, draining: false, alive: true, uptimeSeconds: 60, queueDepth: 0, experimentId: "exp-astra", experimentStatus: "RUNNING", discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 40, overloadState: "NORMAL", journalDegraded: false, telemetryDegraded: false, newPaperEntryAllowed: true },
    balance: { startingBalance: "1000", cash: "1000", reservedCash: "0", positionValue: "0", equity: "1000", realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0", aiCost: "0", infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", highWaterMark: "1000", drawdown: "0", updatedAt: "2026-09-14T12:20:00.000Z" },
    positions: [], latestAgents: {}, consensus: null, risk: null, scanner: [], decisionFeed: [],
    grokActivity: { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null, byAgent: {} },
    recentTriggers: [{ id: "trigger-1", kind: "SPREAD_CHANGE", severity: 1, marketId: "market-1", asset: "BTC", createdAt: "2026-09-14T12:19:59.000Z", expiresAt: "2026-09-14T12:20:29.000Z" }],
    activations: [], activationLatency: [], unattributedMcp: { lastAt: null, count: 0, lastTool: null }, discovery: { qualified: [], rejected: [] },
    health: { engineAlive: true, discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 40, polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, referenceFeeds: [], database: "UP", journalDegraded: false, telemetryDegraded: false, overloadState: "NORMAL", eventLatencyMs: 1, lastBtcMessageAt: null, unattributedMcp: { lastAt: null, count: 0, lastTool: null } },
    equityHistory: [],
    ...overrides,
  };
}

function render(component: (props: { snapshot: DashboardSnapshot }) => JSX.Element, snapshot: DashboardSnapshot): string {
  return renderToStaticMarkup(createElement(component, { snapshot }));
}

describe("dashboard pipeline observability", () => {
  it("shows trigger progress instead of pretending the pipeline is idle", () => {
    const data = baseSnapshot();
    expect(render(DecisionIntelligence, data)).toContain("TRIGGER OBSERVED");
    expect(render(RiskPanel, data)).toContain("TRIGGER OBSERVED · RISK WAITING");
    expect(render(ExecutionStream, data)).toContain("TRIGGER OBSERVED · EXECUTION WAITING");
  });

  it("shows an Astra deterministic gate rejection and explains that risk was never reached", () => {
    const data = baseSnapshot({ decisionFeed: [{ id: "gate-1", at: "2026-09-14T12:20:00.000Z", type: "ASTRA_DECISION", traceId: "trace-1", marketId: "market-1", asset: "BTC", detail: { accepted: false, gateReason: "NO_LIQUIDITY" } }] });
    const decision = render(DecisionIntelligence, data);
    const risk = render(RiskPanel, data);
    expect(decision).toContain("ASTRA GATE REJECTED");
    expect(decision).toContain("NO_LIQUIDITY");
    expect(risk).toContain("RISK NOT REACHED");
    expect(risk).toContain("NO_LIQUIDITY");
  });

  it("keeps risk and execution semantics truthful after a real decision", () => {
    const consensus: NonNullable<DashboardSnapshot["consensus"]> = {
      consensusDirection: "UP", consensusConfidence: .8, agentAgreement: 1, signalFreshness: 1, conflicts: [], estimatedEdge: .02,
      calculatedAt: "2026-09-14T12:20:01.000Z", authority: "ASTRA_BRAIN_V1", fairProbability: .6, marketProbability: .58,
    };
    const decisionOnly = baseSnapshot({ consensus });
    expect(render(RiskPanel, decisionOnly)).toContain("DECISION READY · RISK PENDING");
    expect(render(ExecutionStream, decisionOnly)).toContain("DECISION READY · RISK WAITING");

    const vetoed = baseSnapshot({ consensus, risk: { accepted: false, vetoes: ["STALE_FEED"], utilization: {}, evaluatedAt: "2026-09-14T12:20:02.000Z" } });
    expect(render(ExecutionStream, vetoed)).toContain("EXECUTION BLOCKED BY RISK");
    expect(render(ExecutionStream, vetoed)).toContain("STALE_FEED");
  });
});
