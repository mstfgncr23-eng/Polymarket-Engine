import { describe, expect, it } from "vitest";
import { MemoryEventJournal } from "@pm/db";
import type { PaperFill } from "@pm/core";
import { EngineService, TakerCashCommitGuardRejectedError } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  discoveryReport,
  paperFill,
  qualified,
  qualifiedPaperEnv,
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
} from "./paper-engine-testkit.js";
import { order, snapshot } from "./fixtures.js";

/**
 * Taker BUY cash commit fence (PR fix/taker-cash-commit-fence).
 *
 * Root invariant under test: no NEW authoritative BUY fill commits when its
 * actual fee-inclusive cash requirement exceeds CURRENT unreserved cash at the
 * serialized durable mutation boundary. Two TAKER BUYs planned against the
 * same pre-fill cash must never both commit and drive ledger.cash negative.
 */
const logger = { error: () => undefined };

async function startEngine(journal: MemoryEventJournal, overrides: Record<string, string | undefined> = {}) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("fence-mkt", "BTC", "fence-up", "fence-down")]));
  const engine = new EngineService(qualifiedPaperEnv(overrides), journal, logger, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
  });
  await engine.start();
  return engine;
}

/**
 * A TAKER BUY with an exact fee-inclusive cash requirement of 1.00:
 * quantity 2 @ 0.49 + fee 0.02 = 1.00.
 */
function cashFill(id: string, marketId: string): PaperFill {
  return paperFill(marketId, "UP", "0.49", {
    id,
    marketId,
    tokenId: `${marketId}-up`,
    quantity: "2",
    feeUsd: "0.02",
  });
}

async function economicFillRows(journal: MemoryEventJournal) {
  const rows = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 50 });
  return rows.filter((row) => Object.keys(row.payload).length > 0);
}

/**
 * Deterministically blocks a specific durable commit inside the journal so a
 * second plan can complete every pre-gate admission read (including its
 * priorFillAuthority lookup) while the first commit is still in flight.
 */
function holdDurableCommit(journal: MemoryEventJournal, operationKey: string) {
  let release!: () => void;
  let entered = false;
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const original = journal.commitDurableTransition.bind(journal);
  journal.commitDurableTransition = async (input, expected, options) => {
    if (input.operationKey === operationKey) { entered = true; await hold; }
    return original(input, expected, options);
  };
  return {
    release,
    waitUntilEntered: async () => {
      while (!entered) await new Promise((resolve) => setTimeout(resolve, 1));
    },
  };
}

function settled(reasons: PromiseSettledResult<unknown>[]) {
  return (reasons[1] as PromiseRejectedResult).reason as Error;
}

describe("taker BUY cash commit fence", () => {
it("1: exactly one of two concurrent NEW TAKER BUYs on different markets commits when combined cash is insufficient", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "1.5" });
    try {
      const fillA = cashFill("fill-fence-a", "fence-a");
      const fillB = cashFill("fill-fence-b", "fence-b");
      expect(Number(engine.ledger.snapshot().cash)).toBeCloseTo(1.5, 8);

      const hold = holdDurableCommit(journal, `fill:${fillA.id}`);
      // Both plans are admitted against the same pre-fill cash (1.50). A is
      // held inside its serialized durable commit before the ledger mutation;
      // B completes its admission reads (priorFillAuthority = null) and waits
      // at the same DurableMutationGate.
      const planA = engine.applyObservedFill(fillA);
      await hold.waitUntilEntered();
      const planB = engine.applyObservedFill(fillB);
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const [resultA, resultB] = await Promise.allSettled([planA, planB]);
      expect(resultA.status).toBe("fulfilled");
      expect(resultB.status).toBe("rejected");
      expect(settled([resultA, resultB])).toBeInstanceOf(TakerCashCommitGuardRejectedError);

      const filled = await economicFillRows(journal);
      expect(filled).toHaveLength(1);
      expect(filled[0]!.operationKey).toBe(`fill:${fillA.id}`);

      const cash = Number(engine.ledger.snapshot().cash);
      expect(cash).toBeGreaterThanOrEqual(0);
      expect(cash).toBeCloseTo(0.5, 8);
      expect(engine.ledger.positions.size).toBe(1);
      expect(engine.ledger.positions.has("fence-a")).toBe(true);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("2: two concurrent NEW TAKER BUYs on different markets both commit when cash is sufficient", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "3" });
    try {
      const fillA = cashFill("fill-fence-c", "fence-c");
      const fillB = cashFill("fill-fence-d", "fence-d");

      const hold = holdDurableCommit(journal, `fill:${fillA.id}`);
      const planA = engine.applyObservedFill(fillA);
      await hold.waitUntilEntered();
      const planB = engine.applyObservedFill(fillB);
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const results = await Promise.allSettled([planA, planB]);
      expect(results[0].status).toBe("fulfilled");
      expect(results[1].status).toBe("fulfilled");

      const filled = await economicFillRows(journal);
      expect(filled).toHaveLength(2);

      const cash = Number(engine.ledger.snapshot().cash);
      expect(cash).toBeGreaterThanOrEqual(0);
      expect(cash).toBeCloseTo(1.0, 8);
      expect(engine.ledger.positions.size).toBe(2);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("3: a concurrent same-fill retry resolves as the existing durable identity without a second cash debit", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "1.5" });
    try {
      const fill = cashFill("fill-fence-same", "fence-same");

      const hold = holdDurableCommit(journal, `fill:${fill.id}`);
      // Both calls read priorFillAuthority = null before the first commit;
      // the retry must not be misread as a NEW cash-consuming fill once the
      // durable identity exists.
      const first = engine.applyObservedFill(fill);
      await hold.waitUntilEntered();
      const retry = engine.applyObservedFill(fill);
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const results = await Promise.allSettled([first, retry]);
      expect(results[0].status).toBe("fulfilled");
      expect(results[1].status).toBe("fulfilled");

      const filled = await economicFillRows(journal);
      expect(filled).toHaveLength(1);
      expect(filled[0]!.operationKey).toBe(`fill:${fill.id}`);

      const cash = Number(engine.ledger.snapshot().cash);
      expect(cash).toBeGreaterThanOrEqual(0);
      expect(cash).toBeCloseTo(0.5, 8);
      expect(engine.ledger.positions.get("fence-same")?.upQuantity).toBe("2");
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("4: MAKER BUY regression — reserved cash is not double-counted by the new fence", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "5" });
    try {
      const makerOrder = order("MAKER", {
        id: "maker-fence", marketId: "fence-mkt", tokenId: "fence-up", outcome: "UP", side: "BUY",
        requestedPrice: "0.40", quantity: "10",
      });
      const book = snapshot({
        marketId: "fence-mkt", tokenId: "fence-up", outcome: "UP",
        bids: [{ price: "0.40", quantity: "100" }], asks: [{ price: "0.41", quantity: "100" }],
      });
      await engine.restMakerForTest(makerOrder, book, "OPEN_FIRST_LEG");

      const reservedAfterRest = Number(engine.reservedCash());
      const availableBefore = Number(engine.ledger.availableCash());
      const cashBefore = Number(engine.ledger.snapshot().cash);
      expect(reservedAfterRest).toBeCloseTo(4.0, 8);
      expect(availableBefore).toBeCloseTo(1.0, 8);

      // MAKER BUY fill cost = 5 * 0.40 + fee 0 = 2.00, paid from the reservation.
      const makerFill = paperFill("fence-mkt", "UP", "0.40", {
        id: "fill-fence-maker", orderId: "maker-fence", tokenId: "fence-up",
        quantity: "5", liquidityRole: "MAKER", feeUsd: "0",
      });
      await engine.applyObservedFill(makerFill);
      expect(engine.ledger.positions.get("fence-mkt")?.upQuantity).toBe("5");

      // cash drops by the fill cost and the reservation is released by exactly
      // the same amount: availableCash is unchanged (no double-count).
      expect(Number(engine.ledger.snapshot().cash)).toBeCloseTo(cashBefore - 2.0, 8);
      expect(Number(engine.reservedCash())).toBeCloseTo(reservedAfterRest - 2.0, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(availableBefore, 8);
      expect(Number(engine.ledger.snapshot().cash)).toBeGreaterThanOrEqual(0);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("5: SELL regression — low available cash does not block a valid SELL", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "1.5" });
    try {
      const buy = paperFill("fence-sell", "UP", "0.10", {
        id: "fill-fence-sell-buy", tokenId: "fence-sell-up", quantity: "10", feeUsd: "0",
      });
      await engine.applyObservedFill(buy);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(0.5, 8);

      const sell = paperFill("fence-sell", "UP", "0.10", {
        id: "fill-fence-sell-sell", tokenId: "fence-sell-up", quantity: "10", side: "SELL", feeUsd: "0",
      });
      await engine.applyObservedFill(sell);

      expect(Number(engine.ledger.snapshot().cash)).toBeCloseTo(1.5, 8);
      expect(Number(engine.ledger.snapshot().cash)).toBeGreaterThanOrEqual(0);
      expect(engine.ledger.positions.size).toBe(0);
      const filled = await economicFillRows(journal);
      expect(filled).toHaveLength(2);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
    } finally {
      await engine.stop();
    }
  }, 15_000);

  it("6: recovery/replay preserves cash >= 0 and no cash_invalid after the cash fence", async () => {
    const journal = new MemoryEventJournal();
    const first = await startEngine(journal, { PAPER_START_BALANCE: "1.5" });
    try {
      const fillA = cashFill("fill-fence-rec-a", "fence-rec-a");
      const fillB = cashFill("fill-fence-rec-b", "fence-rec-b");

      const hold = holdDurableCommit(journal, `fill:${fillA.id}`);
      const planA = first.applyObservedFill(fillA);
      await hold.waitUntilEntered();
      const planB = first.applyObservedFill(fillB);
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const [resultA, resultB] = await Promise.allSettled([planA, planB]);
      expect(resultA.status).toBe("fulfilled");
      expect(resultB.status).toBe("rejected");
      expect(Number(first.ledger.snapshot().cash)).toBeCloseTo(0.5, 8);
    } finally {
      await first.stop();
    }

    // Restart from the same durable journal. restoreAndRecover runs
    // validatePostReplayInvariants; a cash_invalid report would throw
    // RecoveryFailClosedError and start() would fail.
    const second = await startEngine(journal, { PAPER_START_BALANCE: "1.5" });
    try {
      expect(Number(second.ledger.snapshot().cash)).toBeCloseTo(0.5, 8);
      expect(Number(second.ledger.availableCash())).toBeGreaterThanOrEqual(0);
      expect(second.ledger.positions.has("fence-rec-a")).toBe(true);
      expect(second.ledger.positions.has("fence-rec-b")).toBe(false);
      const filled = await economicFillRows(journal);
      expect(filled).toHaveLength(1);
      expect(filled[0]!.operationKey).toBe("fill:fill-fence-rec-a");
      expect(second.integrityFault()).toBe(false);
      expect(second.overloadState()).not.toBe("FAIL_CLOSED");
    } finally {
      await second.stop();
    }
  }, 15_000);
});
