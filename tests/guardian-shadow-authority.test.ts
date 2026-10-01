import { describe, expect, it } from "vitest";
import { createId, type MarketSnapshot, type PaperFill } from "@pm/core";
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
import { feeSchedule, snapshot as fixtureSnapshot } from "./fixtures.js";

const START = Date.parse("2026-01-01T00:00:00.000Z");
const at = () => new Date(START).toISOString();

function book(
  outcome: "UP" | "DOWN",
  bids: Array<{ price: string; quantity: string }>,
  asks: Array<{ price: string; quantity: string }>,
): MarketSnapshot {
  return fixtureSnapshot({
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId: outcome === "UP" ? "btc-up" : "btc-down",
    outcome,
    asset: "XRP",
    bids,
    asks,
    feeSchedule,
    sourceTimestamp: at(),
    receivedAt: at(),
    sequence: outcome === "UP" ? 1 : 2,
  });
}

function firstLeg(): PaperFill {
  return {
    id: createId("fill"),
    orderId: createId("order"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId: "btc-up",
    outcome: "UP",
    side: "BUY",
    requestedPrice: "0.80",
    fillPrice: "0.80",
    quantity: "6.25",
    liquidityRole: "TAKER",
    latencyMs: 0,
    slippageUsd: "0",
    feeUsd: "0",
    timestamp: at(),
    sourceMarketEventId: createId("event"),
    fillConfidence: 1,
    fillModel: "guardian-shadow-authority-test",
    feeSchedule,
  };
}

describe("guardian shadow canonical authority surface", () => {
  it("is observation-only and does not perturb PR3/runtime/ledger/order state", async () => {
    const journal = new MemoryEventJournal();
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([
      qualified("btc-mkt", "XRP", "btc-up", "btc-down", new Date(START + 3_600_000).toISOString()),
    ]));
    const engine = new EngineService(
      qualifiedPaperEnv({
        DISCOVERY_INTERVAL_MS: "3600000",
        ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
        PAPER_EXECUTION_STYLE: "TAKER",
        PAPER_EXECUTION_LATENCY_MS: "0",
      }),
      journal,
      { error: () => undefined },
      {
        market: market as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(START),
        resolutionFetcher: async () => null,
      },
    );

    await engine.start();
    try {
      await market.waitForHandlers();
      await market.emitSnapshot(book("UP", [{ price: "0.70", quantity: "1000" }], [{ price: "0.81", quantity: "1000" }]));
      await market.emitSnapshot(book("DOWN", [{ price: "0.04", quantity: "1000" }], [{ price: "0.05", quantity: "10000" }]));
      await engine.applyObservedFill(firstLeg(), "OPEN_FIRST_LEG");

      const ledgerBefore = engine.ledger.snapshot();
      const positionBefore = structuredClone(engine.ledger.positions.get("btc-mkt"));
      const ordersBefore = engine.restingOrderCount();
      const reservedBefore = engine.reservedCash();
      const pr3CountBefore = engine.pr3HedgeDecisionCountForTest();
      const runtimeAssetBefore = engine.runtimeBasisAssetForTest();
      const journalBefore = (await journal.queryEvents({})).length;

      const first = engine.guardianShadowReadState() as {
        paperMode: boolean;
        riskConfiguration: Record<string, unknown>;
        positions: Array<{
          position: { marketId: string };
          normalHedgeAdmissible: boolean;
          normalHedgeVetoes: string[];
          hedgeMakerOwnsResidual: boolean;
          riskReductionInFlight: boolean;
        }>;
        tokenMap: Array<{ tokenId: string }>;
      };
      const second = engine.guardianShadowReadState();

      expect(first.paperMode).toBe(true);
      expect(first.positions).toHaveLength(1);
      expect(first.positions[0]?.position.marketId).toBe("btc-mkt");
      expect(first.riskConfiguration).toMatchObject({
        maxOrderSizeUsd: 25,
        minExpectedEdge: 0.015,
        staleFeedTimeoutMs: 15000,
        catastrophicPairLockMinUnhedgedMs: 10000,
        catastrophicPairLockTriggerLossUsd: 0.35,
        catastrophicPairLockMaxLockedLossUsd: 0.60,
      });
      expect(new Set(first.tokenMap.map((row) => row.tokenId))).toEqual(new Set(["btc-up", "btc-down"]));
      expect(second).toEqual(first);

      expect(engine.ledger.snapshot()).toEqual(ledgerBefore);
      expect(engine.ledger.positions.get("btc-mkt")).toEqual(positionBefore);
      expect(engine.restingOrderCount()).toBe(ordersBefore);
      expect(engine.reservedCash()).toBe(reservedBefore);
      expect(engine.pr3HedgeDecisionCountForTest()).toBe(pr3CountBefore);
      expect(engine.runtimeBasisAssetForTest()).toBe(runtimeAssetBefore);
      expect((await journal.queryEvents({})).toHaveLength(journalBefore);
    } finally {
      await engine.stop();
    }
  });
});
