"use client";

import { useMemo, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { GalaxyOrbitalVisual } from "../components/galaxy-orbital-visual";
import { buildGalaxySceneModel, GALAXY_FPS } from "../../lib/galaxy-scene";
import type { DashboardSnapshot } from "../../lib/types";

function harnessSnapshot(): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-14T00:00:00.000Z",
    engine: {
      ready: true,
      draining: false,
      alive: true,
      uptimeSeconds: 120,
      queueDepth: 0,
      experimentId: "harness",
      experimentStatus: "IDLE",
      discoveryStatus: "MONITORING",
      discoveryLoopActive: true,
      marketMonitoring: true,
      qualifiedMarketCount: 2,
      overloadState: "NORMAL",
      journalDegraded: false,
      telemetryDegraded: false,
      newPaperEntryAllowed: true,
    },
    balance: {
      startingBalance: "1000",
      cash: "1000",
      reservedCash: "0",
      positionValue: "0",
      equity: "1000",
      realizedPnl: "0",
      unrealizedPnl: "0",
      grossPnl: "0",
      fees: "0",
      slippage: "0",
      aiCost: "0",
      infrastructureCost: "0",
      netTradingPnl: "0",
      netEconomicPnl: "0",
      highWaterMark: "1000",
      drawdown: "0",
      updatedAt: "2026-09-14T00:00:00.000Z",
    },
    positions: [],
    latestAgents: {},
    consensus: {
      consensusDirection: "UP",
      consensusConfidence: 0.62,
      agentAgreement: 0.8,
      signalFreshness: 0.9,
      conflicts: [],
      estimatedEdge: 0.03,
      calculatedAt: "2026-09-14T00:00:00.000Z",
      authority: "NATIVE_DETERMINISTIC_V1",
      fairProbability: 0.55,
      marketProbability: 0.52,
    },
    risk: {
      accepted: true,
      vetoes: [],
      utilization: {},
      evaluatedAt: "2026-09-14T00:00:00.000Z",
    },
    scanner: [],
    decisionFeed: [],
    grokActivity: {
      acceptedSignals: 0,
      rejectedSignals: 0,
      duplicates: 0,
      unauthorizedCalls: 0,
      rateLimitedCalls: 0,
      lastSeenAt: null,
      byAgent: {},
    },
    recentTriggers: [],
    activationLatency: [],
    unattributedMcp: { lastAt: null, count: 0, lastTool: null },
    discovery: { qualified: [], rejected: [] },
    health: {
      engineAlive: true,
      discoveryStatus: "MONITORING",
      discoveryLoopActive: true,
      marketMonitoring: true,
      qualifiedMarketCount: 2,
      polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 },
      btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 },
      referenceFeeds: [
        { asset: "BTC", state: "CONNECTED", lastMessageAt: null, reconnects: 0 },
        { asset: "ETH", state: "CONNECTED", lastMessageAt: null, reconnects: 0 },
      ],
      database: "UP",
      journalDegraded: false,
      telemetryDegraded: false,
      overloadState: "NORMAL",
      eventLatencyMs: 1,
      lastBtcMessageAt: null,
      unattributedMcp: { lastAt: null, count: 0, lastTool: null },
    },
    equityHistory: [],
  };
}

function HarnessInner() {
  const params = useSearchParams();
  const forceFallback = params.get("fallback") === "1";
  const tParam = params.get("t");
  const freezeTimeSec = tParam != null && tParam !== "" ? Number(tParam) : null;
  const frame = freezeTimeSec != null
    ? Math.round(freezeTimeSec * GALAXY_FPS)
    : Math.round(3 * GALAXY_FPS);

  const model = useMemo(() => buildGalaxySceneModel(harnessSnapshot()), []);

  return (
    <main
      style={{
        margin: 0,
        padding: 0,
        width: "100vw",
        height: "100vh",
        background: "#050308",
        overflow: "hidden",
      }}
      data-galaxy-harness="1"
      data-force-fallback={forceFallback ? "1" : "0"}
    >
      <div style={{ position: "relative", width: 1200, height: 760, margin: "0 auto" }}>
        <GalaxyOrbitalVisual
          model={model}
          transients={[]}
          frame={frame}
          reducedMotion={false}
          durationInFrames={Math.max(1, GALAXY_FPS * 60)}
          forceFieldFallback={forceFallback}
          freezeTimeSec={forceFallback ? 0 : freezeTimeSec}
        />
      </div>
    </main>
  );
}

export default function GalaxyHarnessPage() {
  return (
    <Suspense fallback={<div data-galaxy-harness="loading" style={{ background: "#050308", height: "100vh" }} />}>
      <HarnessInner />
    </Suspense>
  );
}
