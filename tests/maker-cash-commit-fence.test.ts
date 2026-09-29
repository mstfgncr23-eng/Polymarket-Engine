import { describe, expect, it } from "vitest";
import { MemoryEventJournal } from "@pm/db";
import { EngineService, MakerReservationCashCommitGuardRejectedError } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
} from "./paper-engine-testkit.js";
import { order, snapshot } from "./fixtures.js";

/**
 * Maker cash reservation commit fence (fix/taker-cash-commit-fence follow-up).
 *
 * Root invariant under test: no NEW PAPER_MAKER_RESERVED may durably commit
 * when its required reservation exceeds CURRENT unreserved cash at the
 * serialized durable mutation boundary. Two different-market maker admissions
 * planned against the same pre-reservation cash must never both durably
 * commit — a single reservation that would immediately fail ledger.reserve()
 * is rejected BEFORE it is written, so memory and durable state never diverge.
 */
const logger = { error: () => undefined };

async function startEngine(journal: MemoryEventJournal, overrides: Record<string, string | undefined> = {}, makerMarketIds: string[] = []) {
  const market = new ScriptedMarketAdapter();
  // Discovery reports are authoritative market sets. A recovered engine must
  // discover the markets that hold its resting makers, otherwise discovery
  // reconciliation treats them as removed and cancels the reservations.
  market.queue(discoveryReport([
    qualified("fence-mkt", "BTC", "fence-up", "fence-down"),
    ...makerMarketIds.map((id) => qualified(id, "BTC", `${id}-up`, `${id}-down`)),
  ]));
  const engine = new EngineService(
    qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0", ...overrides }),
    journal,
    logger,
    {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
    },
  );
  await engine.start();
  return engine;
}

/**
 * A maker BUY with an exact cash reservation of 2.50 under the fixture fee
 * schedule (taker-only fees, so no maker fee): quantity 5 @ 0.50.
 */
function makerBuy(id: string, marketId: string) {
  return order("MAKER", {
    id,
    marketId,
    tokenId: `${marketId}-up`,
    outcome: "UP",
    side: "BUY",
    requestedPrice: "0.50",
    quantity: "5",
  });
}

function makerBook(marketId: string) {
  return snapshot({
    marketId,
    tokenId: `${marketId}-up`,
    outcome: "UP",
    bids: [{ price: "0.50", quantity: "50" }],
    asks: [{ price: "0.51", quantity: "50" }],
  });
}

async function economicReserveRows(journal: MemoryEventJournal) {
  const rows = await journal.queryDurableTransitions({ types: ["PAPER_MAKER_RESERVED"], limit: 50 });
  return rows.filter((row) => Object.keys(row.payload).length > 0);
}

async function durableReservedTotal(journal: MemoryEventJournal): Promise<number> {
  const rows = await economicReserveRows(journal);
  const total = rows.reduce<number>((sum, row) => sum + Number((row.payload as { reserved?: string }).reserved ?? 0), 0);
  return Number(total.toFixed(8));
}

/**
 * Deterministically blocks a specific durable commit inside the journal so a
 * second plan can complete every pre-gate admission read (including the
 * non-serialized reserveMaker cash preflight) while the first commit is still
 * in flight and its reservation is not yet applied.
 */
function holdDurableCommit(journal: MemoryEventJournal, operationKey: string) {
  let release!: () => void;
  let entered = false;
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const original = journal.commitDurableTransition.bind(journal);
  journal.commitDurableTransition = async (input, expected) => {
    if (input.operationKey === operationKey) { entered = true; await hold; }
    return original(input, expected);
  };
  return {
    release,
    waitUntilEntered: async () => {
      while (!entered) await new Promise((resolve) => setTimeout(resolve, 1));
    },
  };
}
describe("maker cash reservation commit fence", () => {
  it("1: exactly one of two concurrent NEW maker reservations on different markets commits when combined cash is insufficient", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "4" });
    try {
      const orderA = makerBuy("maker-cash-a", "mkr-a");
      const orderB = makerBuy("maker-cash-b", "mkr-b");
      expect(Number(engine.ledger.snapshot().cash)).toBeCloseTo(4.0, 8);

      // Both plans are admitted against the same pre-reservation cash (4.00);
      // each reservation alone is 2.50. A is held inside its serialized
      // durable commit before the reservation is applied; B completes its
      // admission reads (preflight sees 4.00) and waits on the gate.
      const hold = holdDurableCommit(journal, `maker:${orderA.id}:reserve`);
      const planA = engine.restMakerForTest(orderA, makerBook("mkr-a"), "OPEN_FIRST_LEG");
      await hold.waitUntilEntered();
      const planB = engine.restMakerForTest(orderB, makerBook("mkr-b"), "OPEN_FIRST_LEG");
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const results = await Promise.allSettled([planA, planB]);
      expect(results[0].status).toBe("fulfilled");
      expect(results[1].status).toBe("rejected");
      expect((results[1] as PromiseRejectedResult).reason).toBeInstanceOf(MakerReservationCashCommitGuardRejectedError);

      const reserved = await economicReserveRows(journal);
      expect(reserved).toHaveLength(1);
      expect(reserved[0]!.operationKey).toBe(`maker:${orderA.id}:reserve`);

      const reservedCash = Number(engine.reservedCash());
      const cash = Number(engine.ledger.snapshot().cash);
      expect(reservedCash).toBeCloseTo(2.5, 8);
      expect(cash).toBeCloseTo(4.0, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(1.5, 8);
      // reservedCash never exceeds cash, and memory agrees with durable state.
      expect(reservedCash).toBeLessThanOrEqual(cash);
      expect(reservedCash).toBeCloseTo(await durableReservedTotal(journal), 8);
      expect(engine.makerReservationsForTest().has("maker-cash-b")).toBe(false);
      expect(engine.restingOrdersForTest().has("maker-cash-b")).toBe(false);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);
it("2: two concurrent NEW maker reservations on different markets both commit when cash is sufficient", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "6" });
    try {
      const orderA = makerBuy("maker-cash-c", "mkr-c");
      const orderB = makerBuy("maker-cash-d", "mkr-d");

      const hold = holdDurableCommit(journal, `maker:${orderA.id}:reserve`);
      const planA = engine.restMakerForTest(orderA, makerBook("mkr-c"), "OPEN_FIRST_LEG");
      await hold.waitUntilEntered();
      const planB = engine.restMakerForTest(orderB, makerBook("mkr-d"), "OPEN_FIRST_LEG");
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const results = await Promise.allSettled([planA, planB]);
      expect(results[0].status).toBe("fulfilled");
      expect(results[1].status).toBe("fulfilled");

      const reserved = await economicReserveRows(journal);
      expect(reserved).toHaveLength(2);

      const reservedCash = Number(engine.reservedCash());
      expect(reservedCash).toBeCloseTo(5.0, 8);
      expect(Number(engine.ledger.snapshot().cash)).toBeCloseTo(6.0, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(1.0, 8);
      expect(reservedCash).toBeLessThanOrEqual(Number(engine.ledger.snapshot().cash));
      expect(reservedCash).toBeCloseTo(await durableReservedTotal(journal), 8);
      expect(engine.makerReservationsForTest().size).toBe(2);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);
it("3: a concurrent same-operationKey retry resolves as the existing durable identity without a second cash reservation", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "4" });
    try {
      const orderX = makerBuy("maker-cash-same", "mkr-same");

      // Both calls read priorReserve = null before the first commit; the retry
      // must resolve as the existing durable identity once it exists, not be
      // misread as a NEW reservation (no false insufficient-cash rejection, no
      // double reserve).
      const hold = holdDurableCommit(journal, `maker:${orderX.id}:reserve`);
      const first = engine.restMakerForTest(orderX, makerBook("mkr-same"), "OPEN_FIRST_LEG");
      await hold.waitUntilEntered();
      const retry = engine.restMakerForTest(orderX, makerBook("mkr-same"), "OPEN_FIRST_LEG");
      await new Promise((resolve) => setTimeout(resolve, 25));
      hold.release();

      const results = await Promise.allSettled([first, retry]);
      expect(results[0].status).toBe("fulfilled");
      expect(results[1].status).toBe("fulfilled");

      const reserved = await economicReserveRows(journal);
      expect(reserved).toHaveLength(1);
      expect(reserved[0]!.operationKey).toBe(`maker:${orderX.id}:reserve`);

      // Exactly one economic mutation: one reservation of 2.50, not two.
      expect(Number(engine.reservedCash())).toBeCloseTo(2.5, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(1.5, 8);
      expect(engine.makerReservationsForTest().get(orderX.id)).toBe("2.5");
      expect(Number(engine.reservedCash())).toBeCloseTo(await durableReservedTotal(journal), 8);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);
it("4: cancel/release regression — an accepted maker reservation still cancels and cash becomes available again", async () => {
    const journal = new MemoryEventJournal();
    const engine = await startEngine(journal, { PAPER_START_BALANCE: "4" });
    try {
      const orderX = makerBuy("maker-cash-rel", "mkr-rel");
      await engine.restMakerForTest(orderX, makerBook("mkr-rel"), "OPEN_FIRST_LEG");
      expect(Number(engine.reservedCash())).toBeCloseTo(2.5, 8);

      await engine.cancelMakerForTest(orderX.id);
      expect(Number(engine.reservedCash())).toBeCloseTo(0, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(4.0, 8);
      expect(engine.makerReservationsForTest().has(orderX.id)).toBe(false);
      expect(engine.restingOrdersForTest().has(orderX.id)).toBe(false);
      const cancels = (await journal.queryDurableTransitions({ types: ["PAPER_ORDER_CANCELLED"], limit: 50 }))
        .filter((row) => Object.keys(row.payload).length > 0);
      expect(cancels).toHaveLength(1);

      // The freed cash is usable again by a new maker admission.
      const orderY = makerBuy("maker-cash-rel-2", "mkr-rel-2");
      await engine.restMakerForTest(orderY, makerBook("mkr-rel-2"), "OPEN_FIRST_LEG");
      expect(Number(engine.reservedCash())).toBeCloseTo(2.5, 8);
      expect(Number(engine.ledger.availableCash())).toBeCloseTo(1.5, 8);
      expect(engine.integrityFault()).toBe(false);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
    } finally {
      await engine.stop();
    }
  }, 15_000);
it("5: recovery/replay — accepted maker reservations replay to matching ledger reservedCash with no invariant violation", async () => {
    const journal = new MemoryEventJournal();
    const recMarkets = ["mkr-rec-a", "mkr-rec-b"];
    // Both engines must discover the maker markets: discovery reports are
    // authoritative market sets and a recovered engine cancels resting makers
    // whose markets are absent from its report.
    const first = await startEngine(journal, { PAPER_START_BALANCE: "10" }, recMarkets);
    try {
      await first.restMakerForTest(makerBuy("maker-cash-rec-a", "mkr-rec-a"), makerBook("mkr-rec-a"), "OPEN_FIRST_LEG");
      await first.restMakerForTest(makerBuy("maker-cash-rec-b", "mkr-rec-b"), makerBook("mkr-rec-b"), "OPEN_FIRST_LEG");
      expect(Number(first.reservedCash())).toBeCloseTo(5.0, 8);
      expect(Number(first.ledger.availableCash())).toBeCloseTo(5.0, 8);
    } finally {
      await first.stop();
    }

    // Restart from the same durable journal. restoreAndRecover runs
    // validatePostReplayInvariants; a reservation-vs-reservedCash mismatch or
    // cash_invalid report would throw RecoveryFailClosedError and start()
    // would fail closed.
    const second = await startEngine(journal, { PAPER_START_BALANCE: "10" }, recMarkets);
    try {
      expect(Number(second.reservedCash())).toBeCloseTo(5.0, 8);
      expect(Number(second.ledger.snapshot().cash)).toBeCloseTo(10.0, 8);
      expect(Number(second.ledger.availableCash())).toBeCloseTo(5.0, 8);
      expect(second.ledger.availableCash().gte(0)).toBe(true);
      expect(second.makerReservationsForTest().size).toBe(2);
      expect(second.restingOrdersForTest().size).toBe(2);
      expect(Number(second.reservedCash())).toBeCloseTo(await durableReservedTotal(journal), 8);
      expect(second.ledger.snapshot().reservedCash).toBe("5");
      expect(second.integrityFault()).toBe(false);
      expect(second.overloadState()).not.toBe("FAIL_CLOSED");
      expect(second.state.database).toBe("UP");
    } finally {
      await second.stop();
    }
  }, 15_000);
});