import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
  NativeV2CounterfactualStore,
  PaperLedger,
  counterfactualDataDir,
  createId,
  nativeV2CounterfactualActionKey,
  type MarketSnapshot,
  type PaperOrder,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-cf-"));
  dirs.push(dir);
  return dir;
}

const NOW = new Date("2026-09-17T08:00:00.000Z");

function snapshot(marketId = "m1"): MarketSnapshot {
  const iso = NOW.toISOString();
  return {
    eventId: "event-m1-up",
    traceId: "trace-1",
    marketId,
    tokenId: "token-up",
    outcome: "UP",
    asset: "BTC",
    bids: [{ price: "0.53", quantity: "200" }],
    asks: [{ price: "0.54", quantity: "4" }, { price: "0.56", quantity: "200" }],
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: {
      rate: "0",
      exponent: 1,
      takerOnly: true,
      rebateRate: "0",
      source: "configured_test_fixture",
      effectiveAt: iso,
    },
    sourceTimestamp: iso,
    receivedAt: iso,
    sequence: 1,
    hash: null,
  };
}

function order(marketId = "m1"): PaperOrder {
  return {
    id: createId("order"),
    traceId: "trace-1",
    triggerId: "trigger-1",
    strategyId: "native-v2-shadow-opportunity",
    marketId,
    tokenId: "token-up",
    outcome: "UP",
    side: "BUY",
    orderType: "TAKER",
    requestedPrice: "0.56",
    quantity: "5",
    status: "PENDING",
    createdAt: NOW.toISOString(),
  };
}

describe("Native V2 counterfactual state", () => {
  it("places durable files under LEARNING_DATA_DIR/counterfactual", () => {
    // path.join is platform-specific; assert the CF subdir contract, not POSIX separators.
    expect(counterfactualDataDir("/data/learning")).toBe(join("/data/learning", "counterfactual"));
  });

  it("first hypothetical would-submit changes V2-only state; V1 ledger unchanged", () => {
    const dataDir = tempDir();
    const v1 = new PaperLedger(1000, NOW);
    const v1Before = v1.snapshot();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();

    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    const first = store.tryCommitWouldSubmit({
      actionKey,
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    });
    expect(first.applied).toBe(true);
    if (!first.applied) throw new Error("expected apply");

    const after = store.decisionState();
    expect(after.stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
    expect(after.positions).toHaveLength(1);
    expect(after.positions[0]?.marketId).toBe("m1");
    expect(Number(after.positions[0]?.upQuantity)).toBeGreaterThan(0);
    expect(Number(after.availableCashUsd)).toBeLessThan(1000);
    expect(store.summary().fillCount).toBe(1);

    // V1 untouched
    expect(v1.snapshot()).toEqual(v1Before);
    expect(v1.positions.size).toBe(0);
    expect(v1.availableCash().toFixed()).toBe("1000");
  });

  it("subsequent evaluation sees updated V2 state (unpaired + lower cash)", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    expect(store.tryCommitWouldSubmit({
      actionKey,
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    }).applied).toBe(true);

    const state = store.decisionState();
    expect(store.hasUnpairedPosition("m1")).toBe(true);
    expect(store.hasAction(actionKey)).toBe(true);
    expect(Number(state.ledger.cash)).toBeLessThan(1000);
    expect(Number(state.positions[0]?.unpairedQuantity)).toBeGreaterThan(0);
  });

  it("duplicate action is idempotent", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    const input = { actionKey, order: order(), snapshot: snapshot(), at: NOW };
    expect(store.tryCommitWouldSubmit(input).applied).toBe(true);
    const mid = store.summary();
    const dup = store.tryCommitWouldSubmit({
      ...input,
      order: order(), // new order id, same action key
    });
    expect(dup.applied).toBe(false);
    if (dup.applied) throw new Error("expected duplicate");
    expect(dup.reason).toBe("DUPLICATE_ACTION");
    expect(store.summary().fillCount).toBe(mid.fillCount);
    expect(store.summary().availableCashUsd).toBe(mid.availableCashUsd);
  });

  it("settlement realizes counterfactual PnL from authoritative labels", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    }).applied).toBe(true);

    const beforeSettle = store.summary();
    const settled = store.trySettleFromLearningLabel("m1", {
      status: "resolved",
      outcome: "UP",
      labeledAt: NOW.toISOString(),
      source: "public_polymarket",
      reason: "RESOLVED",
    }, NOW);
    expect(settled.applied).toBe(true);
    if (!settled.applied) throw new Error("expected settle");
    expect(Number(settled.realizedDelta)).not.toBeNaN();
    expect(store.openPositionMarketIds()).toEqual([]);
    expect(store.summary().settlementCount).toBe(1);
    expect(Number(store.summary().realizedPnl)).toBe(Number(beforeSettle.realizedPnl) + Number(settled.realizedDelta));
    // After settlement, action key cleared so a later cycle may open again.
    expect(store.hasAction(nativeV2CounterfactualActionKey("m1", "UP"))).toBe(false);
  });

  it("restart/recovery replays durable events into equivalent CF state", () => {
    const dataDir = tempDir();
    const first = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    first.init();
    expect(first.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    }).applied).toBe(true);
    expect(first.trySettleMarket("m1", "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "public_polymarket",
      reason: "RESOLVED",
    }).applied).toBe(true);
    const expected = first.summary();

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    const got = recovered.summary();
    expect(got.fillCount).toBe(expected.fillCount);
    expect(got.settlementCount).toBe(expected.settlementCount);
    expect(got.availableCashUsd).toBe(expected.availableCashUsd);
    expect(got.realizedPnl).toBe(expected.realizedPnl);
    expect(got.fees).toBe(expected.fees);
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.decisionState().stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
  });

  it("unresolved learning labels do not invent CF settlement", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    }).applied).toBe(true);
    const rejected = store.trySettleFromLearningLabel("m1", {
      status: "unresolved",
      outcome: null,
      labeledAt: null,
      source: null,
      reason: "UNRESOLVED",
    });
    expect(rejected.applied).toBe(false);
    if (rejected.applied) throw new Error("expected reject");
    expect(rejected.reason).toBe("UNRESOLVED_LABEL");
    expect(store.openPositionMarketIds()).toEqual(["m1"]);
  });

  it("fill durable append failure leaves memory and restart state unchanged", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    const before = store.summary();
    store.faultNextAppendForTest(new Error("FORCED_FILL_APPEND_FAILURE"));
    const result = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    });
    expect(result.applied).toBe(false);
    if (result.applied) throw new Error("expected failure");
    expect(result.reason).toBe("EXECUTION_ERROR");
    expect(store.summary().fillCount).toBe(before.fillCount);
    expect(store.summary().availableCashUsd).toBe(before.availableCashUsd);
    expect(store.openPositionMarketIds()).toEqual([]);
    expect(store.summary().degraded).toBe(true);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.summary().fillCount).toBe(0);
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.summary().availableCashUsd).toBe(before.availableCashUsd);
  });

  it("settlement durable append failure leaves memory and restart state unchanged", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    }).applied).toBe(true);
    const before = store.summary();
    expect(store.openPositionMarketIds()).toEqual(["m1"]);

    store.faultNextAppendForTest(new Error("FORCED_SETTLEMENT_APPEND_FAILURE"));
    const settled = store.trySettleMarket("m1", "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "public_polymarket",
      reason: "RESOLVED",
    });
    expect(settled.applied).toBe(false);
    if (settled.applied) throw new Error("expected failure");
    expect(settled.reason).toBe("EXECUTION_ERROR");
    expect(store.summary().settlementCount).toBe(before.settlementCount);
    expect(store.openPositionMarketIds()).toEqual(["m1"]);
    expect(store.summary().availableCashUsd).toBe(before.availableCashUsd);
    expect(store.summary().degraded).toBe(true);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.openPositionMarketIds()).toEqual(["m1"]);
    expect(recovered.summary().settlementCount).toBe(0);
    expect(recovered.summary().fillCount).toBe(1);
    expect(recovered.summary().availableCashUsd).toBe(before.availableCashUsd);
  });

  it("CF portfolio is continuous across experiment boundaries (matches V1 PaperLedger lifetime)", () => {
    // KNOWN: V1 ledger is process-lifetime (PAPER_START_BALANCE once); beginExperiment
    // carries snapshot().startingBalance and only residue-gates open exposure.
    // Shadow store resets on START_VALIDATED; CF must NOT invent a per-experiment wipe.
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order("m1"),
      snapshot: snapshot("m1"),
      at: NOW,
    }).applied).toBe(true);
    expect(store.trySettleMarket("m1", "1", "0", { at: NOW }).applied).toBe(true);
    const afterExperimentA = store.summary();
    expect(afterExperimentA.settlementCount).toBe(1);
    expect(Number(afterExperimentA.availableCashUsd)).not.toBe(1000);

    // Simulated experiment B boundary: no CF reset; cash/PnL accumulate.
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m2", "UP"),
      order: order("m2"),
      snapshot: snapshot("m2"),
      at: NOW,
    }).applied).toBe(true);
    const afterExperimentB = store.summary();
    expect(afterExperimentB.fillCount).toBe(2);
    expect(afterExperimentB.settlementCount).toBe(1);
    expect(Number(afterExperimentB.availableCashUsd)).toBeLessThan(Number(afterExperimentA.availableCashUsd));

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.summary().fillCount).toBe(2);
    expect(recovered.summary().settlementCount).toBe(1);
    expect(recovered.summary().availableCashUsd).toBe(afterExperimentB.availableCashUsd);
  });

  it("total maxBytes bound rejects fills without mutating CF or V1-visible authority", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes: 400,
      segmentMaxBytes: 200,
    });
    store.init();
    // First fill may succeed; keep committing until capacity rejects.
    let rejected = null;
    for (let i = 0; i < 30; i += 1) {
      const marketId = `m${i}`;
      const result = store.tryCommitWouldSubmit({
        actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
        order: order(marketId),
        snapshot: snapshot(marketId),
        at: NOW,
      });
      if (!result.applied) {
        rejected = result;
        break;
      }
      // Settle to free action keys / keep writing more fill events.
      store.trySettleMarket(marketId, "1", "0", { at: NOW });
    }
    expect(rejected).not.toBeNull();
    expect(rejected?.reason).toBe("CAPACITY_EXCEEDED");
    expect(store.summary().degraded).toBe(true);
    expect(store.summary().bytes).toBeLessThanOrEqual(store.summary().maxBytes);

    const fillsBefore = store.summary().fillCount;
    const recovered = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes: 400,
      segmentMaxBytes: 200,
    });
    recovered.init();
    expect(recovered.summary().fillCount).toBe(fillsBefore);
    expect(recovered.summary().bytes).toBe(store.summary().bytes);
  });
});
