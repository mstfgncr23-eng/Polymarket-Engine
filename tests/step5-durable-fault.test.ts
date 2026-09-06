import { describe, expect, it } from "vitest";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry, paperFill } from "./paper-engine-testkit.js";

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
});
