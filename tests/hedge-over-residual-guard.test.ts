import { describe, expect, it } from "vitest";
import {
  createId,
  engineEnvSchema,
  type PaperFill,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";
import { feeSchedule, snapshot } from "./fixtures.js";

const TEST_NOW = new Date("2026-09-24T18:35:57.379Z");

async function startEngine() {
  const journal = new MemoryEventJournal();
  const market = new ScriptedMarketAdapter();
  const env = engineEnvSchema.parse({
    NODE_ENV: "test",
    JOURNAL_MODE: "memory",
    TRADING_MODE: "paper",
    POLYMARKET_TOKEN_IDS: "up,down",
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => TEST_NOW,
  });
  await engine.start();
  return { engine, journal, market };
}

function paperFill(
  marketId: string,
  outcome: "UP" | "DOWN",
  price: string,
  extra: Partial<PaperFill> = {},
): PaperFill {
  const tokenId = outcome === "UP" ? "up" : "down";
  return {
    id: createId("fill"), orderId: createId("order"), traceId: createId("trace"),
    marketId, tokenId, outcome, side: "BUY", requestedPrice: price, fillPrice: price,
    quantity: "10", liquidityRole: "TAKER", latencyMs: 0, slippageUsd: "0", feeUsd: "0",
    timestamp: new Date().toISOString(), sourceMarketEventId: createId("event"),
    fillConfidence: 1, fillModel: "hedge-guard-test", feeSchedule, ...extra,
  };
}

describe("hedge residual guard survives the confidence-exemption change", () => {
  it("J/K: over-residual and post-pair hedges are rejected; the residual is never double-consumed", async () => {
    const { engine, journal, market } = await startEngine();
    try {
      // First leg: unpaired DOWN 10.
      await engine.applyObservedFill(paperFill("race-mkt", "DOWN", "0.50", {
        id: "first-leg", orderId: "order-first", tokenId: "down",
      }));
      expect(engine.ledger.positions.get("race-mkt")?.unpairedQuantity).toBe("10");

      // A live, fresh, synchronized DOWN book is required before any protective
      // SELL can fill (the de-risk book guard).
      const downBook = snapshot({
        eventId: createId("market-book"),
        marketId: "race-mkt", tokenId: "down", outcome: "DOWN",
        bids: [{ price: "0.48", quantity: "50" }], asks: [{ price: "0.49", quantity: "50" }],
        sourceTimestamp: TEST_NOW.toISOString(), receivedAt: TEST_NOW.toISOString(),
      });
      await market.emitSnapshot(downBook);

      // Protective SELL wins half the race: residual is now DOWN 6.
      await engine.applyObservedFill(paperFill("race-mkt", "DOWN", "0.48", {
        id: "protective-sell", orderId: "order-protective", tokenId: "down", side: "SELL", quantity: "4",
        sourceMarketEventId: downBook.eventId,
      }), "PROTECTIVE_DE_RISK");
      expect(engine.ledger.positions.get("race-mkt")).toMatchObject({
        downQuantity: "6", unpairedOutcome: "DOWN", unpairedQuantity: "6",
      });

      // A simultaneous/earlier hedge for the ORIGINAL residual is stale and must
      // be rejected — it can never consume more than the current residual.
      await expect(engine.applyObservedFill(paperFill("race-mkt", "UP", "0.48", {
        id: "stale-hedge", orderId: "order-stale-hedge", tokenId: "up", quantity: "10",
      }), "HEDGE_SECOND_LEG")).rejects.toThrow("QUANTITY_EXCEEDS_RESIDUAL");
      expect(engine.ledger.positions.get("race-mkt")).toMatchObject({
        upQuantity: "0", downQuantity: "6", unpairedOutcome: "DOWN", unpairedQuantity: "6",
      });

      // The fresh, correct-sized hedge completes the pair.
      await engine.applyObservedFill(paperFill("race-mkt", "UP", "0.48", {
        id: "fresh-hedge", orderId: "order-fresh-hedge", tokenId: "up", quantity: "6",
      }), "HEDGE_SECOND_LEG");
      expect(engine.ledger.positions.get("race-mkt")).toMatchObject({
        upQuantity: "6", downQuantity: "6", unpairedOutcome: null, unpairedQuantity: "0",
      });

      // A retry/duplicate hedge on the now-paired position is rejected: no
      // second hedge can reopen or over-pair an already paired position.
      await expect(engine.applyObservedFill(paperFill("race-mkt", "UP", "0.48", {
        id: "second-hedge", orderId: "order-second-hedge", tokenId: "up", quantity: "1",
      }), "HEDGE_SECOND_LEG")).rejects.toThrow("NO_UNPAIRED_RESIDUAL");
      expect(engine.ledger.positions.get("race-mkt")).toMatchObject({
        upQuantity: "6", downQuantity: "6", unpairedOutcome: null, unpairedQuantity: "0",
      });

      // Durable economy is exactly: first leg + protective sell + successful hedge.
      const durableFills = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 10 });
      expect(durableFills.map((row) => (row.payload.fill as { id: string }).id).sort()).toEqual([
        "first-leg", "fresh-hedge", "protective-sell",
      ]);
      expect(engine.integrityFault()).toBe(false);
    } finally {
      await engine.stop();
    }
  });
});