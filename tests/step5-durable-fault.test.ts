import { describe, expect, it } from "vitest";
import { MemoryEventJournal } from "@pm/db";
import { paperOrderSchema, type RestingPaperOrder } from "@pm/core";
import { EngineService } from "../apps/engine/src/engine.js";
import { durableIdentityConflicts } from "../apps/engine/src/recovery.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const logger = { error: () => undefined };

async function startPaper(journal: MemoryEventJournal) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("step5-durable", "BTC", "step5-up", "step5-down")]));
  const engine = new EngineService(qualifiedPaperEnv(), journal, logger, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
  });
  await engine.start();
  return engine;
}

function durableInput(operationKey: string) {
  return {
    operationKey,
    type: "EXPERIMENT_STARTED",
    aggregateType: "experiment",
    aggregateId: operationKey,
    payload: { experiment: { id: operationKey, status: "RUNNING", durationHours: 48, strategyVersion: "step5", riskConfiguration: {}, modelConfiguration: {}, startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", closePolicy: "observe_until_settlement", startedAt: new Date().toISOString(), entryClosesAt: new Date(Date.now() + 48 * 3_600_000).toISOString(), completedAt: null, unresolvedPositionCount: 0, markToMarketAt: null } },
  };
}

describe("Step 5 durable DB fault path", () => {
  it("bounds delayed durable commit, outage recovery, and duplicate retries", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startPaper(journal);
    try {
      const firstInput = durableInput("step5-durable-delayed");
      let release!: () => void;
      let entered = false;
      const hold = new Promise<void>((resolve) => { release = resolve; });
      const original = journal.commitDurableTransition.bind(journal);
      journal.commitDurableTransition = async (input, expected) => {
        if (input.operationKey === firstInput.operationKey) { entered = true; await hold; }
        return original(input, expected);
      };
      const delayed = engine.commitDurableForTest(firstInput, 0);
      while (!entered) await new Promise((resolve) => setTimeout(resolve, 1));
      expect(await journal.latestDurableSequence()).toBe(0);
      release();
      expect((await delayed).inserted).toBe(true);

      const outageInput = durableInput("step5-durable-outage");
      journal.available = false;
      await expect(engine.commitDurableForTest(outageInput, 1)).rejects.toThrow("DATABASE_UNAVAILABLE");
      journal.available = true;
      expect((await engine.commitDurableForTest(outageInput, 1)).inserted).toBe(true);
      const retries = await Promise.all(Array.from({ length: 12 }, () => engine.commitDurableForTest(outageInput, 1)));
      expect(retries.every((result) => result.existing && !result.inserted)).toBe(true);
      const rows = await journal.queryDurableTransitions({ limit: 20 });
      expect(rows.filter((row) => row.operationKey === outageInput.operationKey)).toHaveLength(1);
      expect(rows).toHaveLength(2);
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("gates an actual engine fill on durable success and replays it exactly once", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startPaper(journal);
    const fill = paperFill("step5-durable", "UP", "0.40", { id: "step5-product-fill" });
    try {
      let release!: () => void;
      let entered = false;
      const hold = new Promise<void>((resolve) => { release = resolve; });
      const original = journal.commitDurableTransition.bind(journal);
      journal.commitDurableTransition = async (input, expected) => {
        if (input.operationKey === `fill:${fill.id}`) { entered = true; await hold; }
        return original(input, expected);
      };
      const pending = engine.applyObservedFill(fill);
      while (!entered) await new Promise((resolve) => setTimeout(resolve, 1));
      expect(engine.state.snapshot().positions).toHaveLength(0);
      release();
      await pending;
      const accepted = engine.state.snapshot().positions.find((position) => position.marketId === fill.marketId);
      expect(accepted?.upQuantity).toBe(fill.quantity);
      const beforeOutage = engine.state.snapshot().balance;
      const outage = paperFill("step5-durable", "DOWN", "0.48", { id: "step5-product-outage" });
      journal.available = false;
      await expect(engine.applyObservedFill(outage)).rejects.toThrow("DATABASE_UNAVAILABLE");
      expect(engine.state.snapshot().balance).toEqual(beforeOutage);
      journal.available = true;
      await engine.applyObservedFill(outage);
      await engine.applyObservedFill(outage);
      expect((await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 20 })).filter((row) => row.operationKey === `fill:${outage.id}`)).toHaveLength(1);
      const recovered = await startPaper(journal);
      try {
        expect(recovered.state.snapshot().balance).toEqual(engine.state.snapshot().balance);
      } finally {
        await recovered.stop();
      }
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("treats durable maker intent as economic identity while preserving legacy rows", () => {
    const resting = { order: { id: "o1" }, remaining: "10" };
    const base = { orderId: "o1", reserved: "5", resting };
    expect(durableIdentityConflicts(
      "PAPER_MAKER_RESERVED",
      { ...base, intent: "OPEN_FIRST_LEG" },
      { ...base, intent: "HEDGE_SECOND_LEG" },
    )).toBe(true);
    expect(durableIdentityConflicts(
      "PAPER_MAKER_RESERVED",
      base,
      { ...base, intent: "OPEN_FIRST_LEG" },
    )).toBe(false);
  });

  it("persists maker lifecycle intent across restart", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startPaper(journal);
    const maker = paperOrderSchema.parse({
      id: "maker-first-intent",
      traceId: "trace-maker-first-intent",
      triggerId: "trigger-maker-first-intent",
      strategyId: "temporal-pair-hedge",
      marketId: "step5-durable",
      tokenId: "step5-up",
      outcome: "UP",
      side: "BUY",
      orderType: "MAKER",
      requestedPrice: "0.40",
      quantity: "10",
      status: "PENDING",
      createdAt: new Date().toISOString(),
    });
    const book = snapshot({
      marketId: "step5-durable",
      tokenId: "step5-up",
      outcome: "UP",
      bids: [{ price: "0.40", quantity: "100" }],
      asks: [{ price: "0.41", quantity: "100" }],
    });
    try {
      await engine.restMakerForTest(maker, book, "OPEN_FIRST_LEG");
      expect(engine.restingOrdersForTest().get(maker.id)?.intent).toBe("OPEN_FIRST_LEG");

      // Simulate a pre-PR1 checkpoint that persisted the resting order before
      // RestingPaperOrder itself carried intent. The durable reserve already
      // has the authoritative top-level intent and recovery must reattach it.
      const internals = engine as unknown as { restingOrders: Map<string, RestingPaperOrder> };
      const current = internals.restingOrders.get(maker.id);
      if (!current) throw new Error("missing resting maker");
      const legacyResting = structuredClone(current);
      delete legacyResting.intent;
      internals.restingOrders.set(maker.id, legacyResting);
      expect(engine.restingOrdersForTest().get(maker.id)?.intent).toBeUndefined();
    } finally {
      await engine.stop();
    }

    const recovered = await startPaper(journal);
    try {
      expect(recovered.restingOrdersForTest().get(maker.id)?.intent).toBe("OPEN_FIRST_LEG");
    } finally {
      await recovered.stop();
    }
  });

  it("rejects and cancels a stale first-leg maker remainder after the market becomes paired", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startPaper(journal);
    const maker = paperOrderSchema.parse({
      id: "maker-stale-first-leg",
      traceId: "trace-maker-stale-first-leg",
      triggerId: "trigger-maker-stale-first-leg",
      strategyId: "temporal-pair-hedge",
      marketId: "step5-durable",
      tokenId: "step5-up",
      outcome: "UP",
      side: "BUY",
      orderType: "MAKER",
      requestedPrice: "0.40",
      quantity: "10",
      status: "PENDING",
      createdAt: new Date().toISOString(),
    });
    const book = snapshot({
      marketId: "step5-durable",
      tokenId: "step5-up",
      outcome: "UP",
      bids: [{ price: "0.40", quantity: "100" }],
      asks: [{ price: "0.41", quantity: "100" }],
    });
    try {
      await engine.restMakerForTest(maker, book, "OPEN_FIRST_LEG");
      const first = paperFill("step5-durable", "UP", "0.40", {
        id: "maker-first-partial",
        orderId: maker.id,
        tokenId: "step5-up",
        quantity: "5",
        liquidityRole: "MAKER",
        feeUsd: "0",
      });
      await engine.applyObservedFill(first);
      expect(engine.state.snapshot().positions[0]?.unpairedQuantity).toBe("5");

      const hedge = paperFill("step5-durable", "DOWN", "0.50", {
        id: "paired-hedge",
        tokenId: "step5-down",
        quantity: "5",
        feeUsd: "0",
      });
      await engine.applyObservedFill(hedge);
      const paired = engine.state.snapshot().positions[0];
      expect(paired?.pairedQuantity).toBe("5");
      expect(paired?.unpairedQuantity).toBe("0");

      const stale = paperFill("step5-durable", "UP", "0.40", {
        id: "maker-stale-remainder",
        orderId: maker.id,
        tokenId: "step5-up",
        quantity: "1",
        liquidityRole: "MAKER",
        feeUsd: "0",
      });
      await engine.applyObservedFill(stale);

      const after = engine.state.snapshot().positions[0];
      expect(after?.upQuantity).toBe("5");
      expect(after?.downQuantity).toBe("5");
      expect(after?.unpairedQuantity).toBe("0");
      expect(engine.restingOrdersForTest().has(maker.id)).toBe(false);
      const rows = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 50 });
      expect(rows.some((row) => row.operationKey === `fill:${stale.id}`)).toBe(false);
    } finally {
      await engine.stop();
    }
  }, 15_000);
});
