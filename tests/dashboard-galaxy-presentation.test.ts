import { describe, expect, it } from "vitest";
import {
  GALAXY_ORBITS,
  GALAXY_VIEW,
  labelAnchor,
  MAX_MARKET_TICKS,
  MAX_VISIBLE_POSITIONS,
  MAX_VISIBLE_REFERENCES,
  buildGalaxyVisualSlots,
  marketTickCount,
  orbitDepthPaths,
  orbitPolyline,
  projectOrbitPoint,
  visiblePositionCount,
  visibleReferenceCount,
} from "../apps/dashboard/lib/galaxy-presentation.js";
import { buildGalaxySceneModel } from "../apps/dashboard/lib/galaxy-scene.js";
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

describe("Galaxy orbital presentation", () => {
  it("keeps a shared center and distinct tilted orbits that stay inside the composition", () => {
    expect(GALAXY_ORBITS).toHaveLength(7);
    const ids = GALAXY_ORBITS.map((orbit) => orbit.id);
    expect(new Set(ids).size).toBe(7);
    expect(GALAXY_ORBITS[0]!.radius).toBeLessThan(GALAXY_ORBITS.at(-1)!.radius);
    expect(new Set(GALAXY_ORBITS.map((orbit) => `${orbit.rotateX}:${orbit.rotateZ}`)).size).toBeGreaterThan(3);

    for (const orbit of GALAXY_ORBITS) {
      const samples = [0, Math.PI / 2, Math.PI, (3 * Math.PI) / 2].map((theta) => projectOrbitPoint(orbit, theta));
      for (const point of samples) {
        expect(point.x).toBeGreaterThan(20);
        expect(point.x).toBeLessThan(GALAXY_VIEW.width - 20);
        expect(point.y).toBeGreaterThan(20);
        expect(point.y).toBeLessThan(GALAXY_VIEW.height - 20);
      }
      const path = orbitPolyline(orbit, 48);
      expect(path.startsWith("M ")).toBe(true);
      expect(path.split("L ").length).toBeGreaterThan(40);
      const depth = orbitDepthPaths(orbit, 64);
      expect(depth.far.length + depth.near.length).toBeGreaterThan(0);
      expect(depth.far.length).toBeGreaterThan(0);
      expect(depth.near.length).toBeGreaterThan(0);
      for (const point of samples) {
        const label = labelAnchor(point);
        expect(label.x).toBeGreaterThanOrEqual(48);
        expect(label.x).toBeLessThanOrEqual(GALAXY_VIEW.width - 48);
        expect(label.y).toBeGreaterThanOrEqual(28);
        expect(label.y).toBeLessThanOrEqual(GALAXY_VIEW.height - 52);
      }
    }
  });

  it("caps visual density from real counts and does not invent unavailable nodes", () => {
    expect(marketTickCount(0)).toBe(0);
    expect(marketTickCount(4)).toBe(4);
    expect(marketTickCount(80)).toBe(MAX_MARKET_TICKS);
    expect(visibleReferenceCount(0)).toBe(0);
    expect(visibleReferenceCount(30)).toBe(MAX_VISIBLE_REFERENCES);
    expect(visiblePositionCount(0)).toBe(0);
    expect(visiblePositionCount(20)).toBe(MAX_VISIBLE_POSITIONS);

    const empty = buildGalaxyVisualSlots(buildGalaxySceneModel(snapshot()));
    expect(empty.filter((slot) => slot.kind === "density" || slot.kind === "position" || slot.kind === "reference")).toEqual([]);
    expect(empty.some((slot) => slot.kind === "core")).toBe(true);
    expect(empty.find((slot) => slot.kind === "core")?.available).toBe(false);

    const live = buildGalaxyVisualSlots(buildGalaxySceneModel(snapshot({
      scanner: Array.from({ length: 6 }, (_, index) => ({
        marketId: `market-${index}`,
        asset: "BTC",
        outcome: index % 2 === 0 ? "UP" : "DOWN",
        bid: "0.4",
        ask: "0.5",
        spread: .1,
        depthUsd: 10,
        lastMessageAt: "2026-09-12T12:00:00.000Z",
      })),
      positions: [{ id: "pos-1", marketId: "market-open", upQuantity: "1", downQuantity: "0", pairedQuantity: "0", unpairedQuantity: "1", unpairedOutcome: "UP", averageUpCost: "0.4", averageDownCost: "0", openedAt: "2026-09-12T11:00:00.000Z", updatedAt: "2026-09-12T11:00:00.000Z" }],
      health: {
        ...snapshot().health,
        referenceFeeds: [
          { asset: "BTC", state: "CONNECTED", lastMessageAt: "2026-09-12T12:00:00.000Z", reconnects: 0 },
          { asset: "ETH", state: "DISCONNECTED", lastMessageAt: null, reconnects: 2 },
        ],
      },
    })));

    expect(live.filter((slot) => slot.kind === "density")).toHaveLength(6);
    expect(live.filter((slot) => slot.kind === "position")).toEqual([expect.objectContaining({ id: "pos-1" })]);
    expect(live.filter((slot) => slot.kind === "reference").map((slot) => slot.label)).toEqual(["BTC", "ETH"]);
  });
});
