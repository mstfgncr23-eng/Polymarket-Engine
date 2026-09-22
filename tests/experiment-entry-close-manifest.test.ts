import { describe, expect, it } from "vitest";
import { createId, refreshExperiment, startExperiment, type MarketSnapshot, type PaperFill } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-01-01T00:00:00.000Z");

type Clock = { nowMs: number };

function iso(clock: Clock): string { return new Date(clock.nowMs).toISOString(); }

function book(
  clock: Clock,
  outcome: "UP" | "DOWN",
  sequence: number,
  bids: Array<{ price: string; quantity: string }>,
  asks = [{ price: "0.40", quantity: "300" }],
): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return snapshot({
    eventId: createId("market"), traceId: createId("trace"), marketId: "btc-mkt", tokenId, outcome, asset: "XRP",
    bids, asks, sourceTimestamp: iso(clock), receivedAt: iso(clock), sequence,
  });
}

async function setupScenario(opts: {
  journal?: MemoryEventJournal;
  clock?: Clock;
  startNow?: boolean;
  resolutionFetcher?: (id: string) => Promise<Record<string, unknown> | null>;
} = {}) {
  const clock = opts.clock ?? { nowMs: START_MS };
  const journal = opts.journal ?? new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + 3_600_000).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "XRP", "btc-up", "btc-down", endDate)]));
  const engine = new EngineService(qualifiedPaperEnv({
    DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "0",
    // Short entry window so maintenance at START + 4s crosses the deadline.
    EXPERIMENT_DURATION_HOURS: "0.001",
  }), journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.nowMs),
    resolutionFetcher: opts.resolutionFetcher ?? (async () => null),
  });
  if (opts.startNow !== false) {
    await engine.start();
    await market.waitForHandlers();
  }
  return { engine, journal, market, clock };
}

function firstLeg(clock: Clock, sourceEventId: string): PaperFill {
  return {
    id: createId("fill-first"), orderId: createId("order-first"), traceId: createId("trace-first"),
    marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", side: "BUY", requestedPrice: "0.50",
    fillPrice: "0.50", quantity: "10", liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: iso(clock), sourceMarketEventId: sourceEventId, fillConfidence: 1,
    fillModel: "manifest-test:first-leg", feeSchedule: null,
  };
}

describe("ENTRY_CLOSED manifest unresolved position count", () => {
  it("RUNNING -> ENTRY_CLOSED with 0 unresolved records 0", () => {
    const running = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now: new Date(START_MS),
      closePolicy: "observe_until_settlement",
    });
    const closed = refreshExperiment(running, new Date(START_MS + 49 * 3_600_000), {
      openPositions: true, unresolvedCount: 0,
    });
    expect(closed.status).toBe("ENTRY_CLOSED");
    expect(closed.unresolvedPositionCount).toBe(0);
  });

  it("RUNNING -> ENTRY_CLOSED with >0 unresolved carries the actual count", () => {
    const running = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now: new Date(START_MS),
      closePolicy: "observe_until_settlement",
    });
    const closed = refreshExperiment(running, new Date(START_MS + 49 * 3_600_000), {
      openPositions: true, unresolvedCount: 3,
    });
    expect(closed.status).toBe("ENTRY_CLOSED");
    expect(closed.unresolvedPositionCount).toBe(3);
  });

  it("ENTRY_CLOSED -> COMPLETED observes settlement and ends with 0 unresolved", () => {
    const running = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now: new Date(START_MS),
      closePolicy: "observe_until_settlement",
    });
    const closed = refreshExperiment(running, new Date(START_MS + 49 * 3_600_000), {
      openPositions: true, unresolvedCount: 3,
    });
    expect(closed.status).toBe("ENTRY_CLOSED");
    // observe_until_settlement: still open positions keep it ENTRY_CLOSED and
    // the boundary count is preserved across refreshes (restart-safe).
    const stillOpen = refreshExperiment(closed, new Date(START_MS + 50 * 3_600_000), { openPositions: true, unresolvedCount: 3 });
    expect(stillOpen.status).toBe("ENTRY_CLOSED");
    expect(stillOpen.unresolvedPositionCount).toBe(3);
    // All positions settled: COMPLETED ends with 0 unresolved.
    const done = refreshExperiment(closed, new Date(START_MS + 51 * 3_600_000), { openPositions: false, unresolvedCount: 0 });
    expect(done.status).toBe("COMPLETED");
    expect(done.unresolvedPositionCount).toBe(0);
  });

  it("RUNNING -> ENTRY_CLOSED without an explicit count preserves the manifest value", () => {
    const running = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now: new Date(START_MS),
      closePolicy: "observe_until_settlement",
    });
    const closed = refreshExperiment(running, new Date(START_MS + 49 * 3_600_000), { openPositions: true });
    expect(closed.status).toBe("ENTRY_CLOSED");
    expect(closed.unresolvedPositionCount).toBe(0);
  });
});
describe("durable ENTRY_CLOSED manifest through restart/replay", () => {
  it("writes the unresolved count durably and reproduces it after restart, then COMPLETED ends at 0", async () => {
    const journal = new MemoryEventJournal();
    const resolutions = new Map<string, Record<string, unknown>>();
    const fetcher = async (id: string) => resolutions.get(id) ?? null;
    const first = await setupScenario({ journal, resolutionFetcher: fetcher });
    try {
      expect((await first.engine.beginExperiment()).ok).toBe(true);
      const downBook = book(first.clock, "DOWN", 1, [{ price: "0.40", quantity: "10" }]);
      await first.market.emitSnapshot(book(first.clock, "UP", 1, [{ price: "0.59", quantity: "300" }]));
      await first.engine.applyObservedFill({ ...firstLeg(first.clock, downBook.eventId), feeSchedule: downBook.feeSchedule });
      expect(first.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");

      // Cross the 0.001h (3.6s) entry deadline with the position still open:
      // the durable ENTRY_CLOSED manifest must carry unresolvedPositionCount=1.
      first.clock.nowMs = START_MS + 4_000;
      await first.engine.tickMaintenance();
      expect(first.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(first.engine.experimentManifest()?.unresolvedPositionCount).toBe(1);
      const rows = await journal.readDurableTransitionsAfter(0, 256);
      const entryRow = rows.find((row) => row.type === "EXPERIMENT_ENTRY_CLOSED");
      expect(entryRow).toBeTruthy();
      expect((entryRow!.payload.experiment as { unresolvedPositionCount?: number }).unresolvedPositionCount).toBe(1);
    } finally {
      await first.engine.stop();
    }

    // Restart/replay from the same journal and clock: the same authoritative
    // count must be reconstructed from the durable manifest.
    const recovered = await setupScenario({ journal, clock: { nowMs: START_MS + 4_000 }, resolutionFetcher: fetcher });
    try {
      expect(recovered.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(recovered.engine.experimentManifest()?.unresolvedPositionCount).toBe(1);
      expect(recovered.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("10");

      // After the market ends and the position settles, COMPLETED ends with 0.
      recovered.clock.nowMs = START_MS + 3_600_000 + 1_000;
      resolutions.set("btc-mkt", {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Down", price: "1" }, no: { label: "Up", price: "0" } },
      });
      await recovered.engine.tickMaintenance();
      await recovered.engine.tickMaintenance();
      expect(recovered.engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(recovered.engine.experimentManifest()?.unresolvedPositionCount).toBe(0);
      const completedRow = [...(await journal.readDurableTransitionsAfter(0, 512))]
        .reverse().find((row) => row.type === "EXPERIMENT_COMPLETED");
      expect(completedRow).toBeTruthy();
      expect((completedRow!.payload.experiment as { unresolvedPositionCount?: number }).unresolvedPositionCount).toBe(0);
    } finally {
      await recovered.engine.stop();
    }
  });
});