import { describe, expect, it } from "vitest";
import {
  advanceGalaxyMotionState,
  buildGalaxySceneModel,
  expireGalaxyMotionState,
  initializeGalaxyMotionState,
  MAX_GALAXY_TRANSIENTS,
} from "../apps/dashboard/lib/galaxy-scene.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

function snapshot(overrides: Partial<DashboardSnapshot> = {}): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-12T12:00:00.000Z",
    engine: { ready: true, draining: false, alive: true, uptimeSeconds: 12, queueDepth: 0, experimentId: "exp-real", experimentStatus: "IDLE", discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 2, overloadState: "NORMAL", journalDegraded: false, telemetryDegraded: false, newPaperEntryAllowed: true },
    balance: { startingBalance: "1000", cash: "1000", reservedCash: "0", positionValue: "0", equity: "1000", realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0", aiCost: "0", infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", highWaterMark: "1000", drawdown: "0", updatedAt: "2026-09-12T12:00:00.000Z" },
    positions: [], latestAgents: {}, consensus: null, risk: null, scanner: [], decisionFeed: [],
    grokActivity: { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null, byAgent: {} },
    recentTriggers: [], activations: [], activationLatency: [], unattributedMcp: { lastAt: null, count: 0, lastTool: null }, discovery: { qualified: [], rejected: [] },
    health: { engineAlive: true, discoveryStatus: "MONITORING", discoveryLoopActive: true, marketMonitoring: true, qualifiedMarketCount: 2, polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, referenceFeeds: [], database: "UP", journalDegraded: false, telemetryDegraded: false, overloadState: "NORMAL", eventLatencyMs: 1, lastBtcMessageAt: null, unattributedMcp: { lastAt: null, count: 0, lastTool: null } },
    equityHistory: [],
    ...overrides,
  };
}

function event(id: string, type: string): DashboardSnapshot["decisionFeed"][number] {
  return { id, at: "2026-09-12T12:00:00.000Z", type, traceId: `trace-${id}`, marketId: "market-1", asset: "BTC", detail: {} };
}

describe("Galaxy scene model", () => {
  it("keeps absent operational data explicitly unavailable", () => {
    const model = buildGalaxySceneModel(snapshot());
    expect(model.core).toMatchObject({ available: false, authority: "—", fairProbability: "—", marketProbability: "—" });
    expect(model.reference).toMatchObject({ connected: 0, tone: "neutral", feeds: [] });
    expect(model.risk).toMatchObject({ status: "NOT RUN", tone: "neutral" });
    expect(model.portfolio).toMatchObject({ status: "EMPTY", positions: 0, equity: "$1000.00" });
  });

  it("preserves historical and external decision authority without inventing native fields", () => {
    const baseDecision: NonNullable<DashboardSnapshot["consensus"]> = {
      consensusDirection: "UP",
      consensusConfidence: .61,
      agentAgreement: .8,
      signalFreshness: .9,
      conflicts: [],
      estimatedEdge: .02,
      calculatedAt: "2026-09-12T12:00:00.000Z",
    };
    const historical = buildGalaxySceneModel(snapshot({ consensus: baseDecision }));
    expect(historical.core).toMatchObject({
      authority: "AUTHORITY UNAVAILABLE",
      authorityTone: "warning",
      fairProbability: "—",
      marketProbability: "—",
    });

    const external = buildGalaxySceneModel(snapshot({ consensus: { ...baseDecision, authority: "EXTERNAL_CONSENSUS_V0" } }));
    expect(external.core).toMatchObject({ authority: "EXTERNAL_CONSENSUS_V0", authorityTone: "warning" });
  });

  it("maps every real reference feed and preserves disconnected state", () => {
    const data = snapshot({ health: { ...snapshot().health, referenceFeeds: [
      { asset: "BTC", state: "CONNECTED", lastMessageAt: "2026-09-12T11:59:59.000Z", reconnects: 0 },
      { asset: "ETH", state: "DISCONNECTED", lastMessageAt: null, reconnects: 3 },
    ] } });
    const model = buildGalaxySceneModel(data);
    expect(model.reference.feeds.map((feed) => [feed.asset, feed.tone])).toEqual([["BTC", "healthy"], ["ETH", "critical"]]);
    expect(model.reference.tone).toBe("critical");
  });
});

describe("bounded Galaxy motion", () => {
  it("treats historical semantic events in the initial snapshot as baseline only", () => {
    const data = snapshot({ decisionFeed: [
      event("historical-decision", "NATIVE_DECISION_COMPLETE"),
      event("historical-trigger", "TRIGGER_LEVEL_2"),
    ] });
    const initial = initializeGalaxyMotionState(data, 20, 1_000);
    expect(initial.seenEventIds).toEqual(["historical-decision", "historical-trigger"]);
    expect(initial.transients).toEqual([]);

    const repeated = advanceGalaxyMotionState(initial, data, 24, 1_200);
    expect(repeated.transients).toEqual([]);
  });

  it("creates a semantic transient only for an event first observed in a later snapshot", () => {
    const initialData = snapshot({ decisionFeed: [event("historical-decision", "NATIVE_DECISION_COMPLETE")] });
    const initial = initializeGalaxyMotionState(initialData, 20, 1_000);
    const later = snapshot({ decisionFeed: [
      event("new-fill", "PAPER_FILL"),
      ...initialData.decisionFeed,
    ] });

    const advanced = advanceGalaxyMotionState(initial, later, 30, 1_500);
    expect(advanced.transients).toEqual([expect.objectContaining({ id: "new-fill", kind: "fill", startFrame: 30 })]);
  });

  it("coalesces real reference and market timestamp changes without inventing pulses", () => {
    const first = snapshot({
      health: { ...snapshot().health, referenceFeeds: [{ asset: "BTC", state: "CONNECTED", lastMessageAt: "2026-09-12T12:00:00.000Z", reconnects: 0 }] },
      scanner: [{ marketId: "market-1", asset: "BTC", outcome: "UP", bid: "0.4", ask: "0.5", spread: .1, depthUsd: 10, lastMessageAt: "2026-09-12T12:00:00.000Z" }],
    });
    const initial = initializeGalaxyMotionState(first, 0, 1_000);
    expect(initial.transients).toHaveLength(0);

    const changed = snapshot({
      decisionFeed: [event("risk-1", "RISK_ACCEPT")],
      health: { ...first.health, referenceFeeds: [{ asset: "BTC", state: "CONNECTED", lastMessageAt: "2026-09-12T12:00:01.000Z", reconnects: 0 }] },
      scanner: [{ ...first.scanner[0]!, lastMessageAt: "2026-09-12T12:00:01.000Z" }],
    });
    const advanced = advanceGalaxyMotionState(initial, changed, 30, 2_500);
    expect(advanced.transients.map((item) => item.kind)).toEqual(expect.arrayContaining(["risk-accept", "reference", "market"]));
    expect(advanced.transients.find((item) => item.kind === "reference")?.asset).toBe("BTC");

    const repeated = advanceGalaxyMotionState(advanced, changed, 38, 2_800);
    expect(repeated.transients.map((item) => item.id)).toEqual(advanced.transients.map((item) => item.id));
  });

  it("caps bursts and expires transient animation state", () => {
    const initial = initializeGalaxyMotionState(snapshot(), 0, 1_000);
    const burst = snapshot({ decisionFeed: Array.from({ length: 40 }, (_, index) => event(`burst-${index}`, "PAPER_FILL")) });
    let state = initial;
    for (let batch = 0; batch < 8; batch += 1) {
      state = advanceGalaxyMotionState(state, { ...burst, decisionFeed: burst.decisionFeed.slice(batch * 5) }, batch * 10, 2_000 + batch * 50);
      expect(state.transients.length).toBeLessThanOrEqual(MAX_GALAXY_TRANSIENTS);
    }
    expect(expireGalaxyMotionState(state, 10_000).transients).toEqual([]);
  });
});
