import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
  NativeV2CounterfactualStore,
  counterfactualDataDir,
  nativeV2CounterfactualActionKey,
  type MarketSnapshot,
  type PaperOrder,
  createId,
} from "@pm/core";
import { startQualifiedEngine } from "./paper-engine-testkit.js";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function learningDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-engine-cf-"));
  dirs.push(dir);
  return dir;
}

const NOW = new Date("2026-09-17T08:00:00.000Z");

function snapshot(marketId = "btc-mkt"): MarketSnapshot {
  const iso = NOW.toISOString();
  return {
    eventId: "event-btc-up",
    traceId: "trace-1",
    marketId,
    tokenId: "btc-up",
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

function order(marketId = "btc-mkt"): PaperOrder {
  return {
    id: createId("order"),
    traceId: "trace-1",
    triggerId: "trigger-1",
    strategyId: "native-v2-shadow-opportunity",
    marketId,
    tokenId: "btc-up",
    outcome: "UP",
    side: "BUY",
    orderType: "TAKER",
    requestedPrice: "0.56",
    quantity: "5",
    status: "PENDING",
    createdAt: NOW.toISOString(),
  };
}

type EngineWithCf = {
  counterfactualStore: NativeV2CounterfactualStore;
  ledger: { snapshot: () => unknown; positions: Map<string, unknown>; availableCash: () => { toFixed: () => string } };
  counterfactualSummary: () => {
    fillCount: number;
    settlementCount: number;
    availableCashUsd: string;
    recoveryReady: boolean;
    realizedPnl: string;
    fees: string;
  };
  stop: () => Promise<void>;
};

describe("EngineService PR2 counterfactual integration", () => {
  it("shadow CF advances, subsequent eval observes CF, V1 unchanged, settle+restart preserve", async () => {
    const learningDataDir = learningDir();
    const { engine, env } = await startQualifiedEngine({
      LEARNING_DATA_DIR: learningDataDir,
      PAPER_START_BALANCE: "1000",
    });
    const typed = engine as unknown as EngineWithCf;
    try {
      expect(env.LEARNING_DATA_DIR).toBe(learningDataDir);
      expect(counterfactualDataDir(env.LEARNING_DATA_DIR)).toBe(join(learningDataDir, "counterfactual"));

      const cf = typed.counterfactualStore;
      expect(cf.isRecoveryReady()).toBe(true);
      const v1Before = typed.ledger.snapshot();
      const v1CashBefore = typed.ledger.availableCash().toFixed();

      // First V2 shadow would-submit (same store EngineService uses for Phase-4 commits).
      const actionKey = nativeV2CounterfactualActionKey("btc-mkt", "UP", "OPEN_FIRST_LEG");
      const first = cf.tryCommitWouldSubmit({
        actionKey,
        order: order(),
        snapshot: snapshot(),
        measuredLatencyMs: 0,
        at: NOW,
      });
      expect(first.applied).toBe(true);

      const summaryAfterFirst = engine.counterfactualSummary();
      expect(summaryAfterFirst.fillCount).toBe(1);
      expect(summaryAfterFirst.recoveryReady).toBe(true);
      expect(Number(summaryAfterFirst.availableCashUsd)).toBeLessThan(1000);

      // Subsequent evaluation observes CF cash / exposure / duplicate / cooldown inputs.
      const decision = cf.decisionState();
      expect(decision.stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
      expect(decision.recoveryReady).toBe(true);
      expect(decision.positions).toHaveLength(1);
      expect(Number(decision.availableCashUsd)).toBe(Number(summaryAfterFirst.availableCashUsd));
      expect(cf.hasUnpairedPosition("btc-mkt")).toBe(true);
      expect(cf.hasAction(actionKey)).toBe(true);
      expect(cf.tryCommitWouldSubmit({
        actionKey,
        order: order(),
        snapshot: snapshot(),
        at: NOW,
      }).applied).toBe(false);

      // V1 PaperLedger unchanged.
      expect(typed.ledger.snapshot()).toEqual(v1Before);
      expect(typed.ledger.positions.size).toBe(0);
      expect(typed.ledger.availableCash().toFixed()).toBe(v1CashBefore);

      // Authoritative resolution settles CF (same path shape as pollLearningSettlements).
      const settled = cf.trySettleMarket("btc-mkt", "1", "0", {
        at: new Date("2026-09-17T09:00:00.000Z"),
        source: "public_polymarket",
        reason: "RESOLVED",
      });
      expect(settled.applied).toBe(true);
      expect(engine.counterfactualSummary().settlementCount).toBe(1);
      expect(cf.hasSettledMarket("btc-mkt")).toBe(true);
      expect(cf.openPositionMarketIds()).toEqual([]);

      // Still no V1 mutation.
      expect(typed.ledger.positions.size).toBe(0);
      expect(typed.ledger.availableCash().toFixed()).toBe(v1CashBefore);
    } finally {
      await engine.stop();
    }

    // Restart/recovery preserves CF via EngineService wiring (same LEARNING_DATA_DIR).
    const restarted = await startQualifiedEngine({
      LEARNING_DATA_DIR: learningDataDir,
      PAPER_START_BALANCE: "1000",
    });
    try {
      const summary = restarted.engine.counterfactualSummary();
      expect(summary.recoveryReady).toBe(true);
      expect(summary.fillCount).toBe(1);
      expect(summary.settlementCount).toBe(1);
      const cf2 = (restarted.engine as unknown as EngineWithCf).counterfactualStore;
      expect(cf2.hasSettledMarket("btc-mkt")).toBe(true);
      expect(cf2.openPositionMarketIds()).toEqual([]);
      expect(restarted.engine.ledger.positions.size).toBe(0);
      expect(restarted.engine.ledger.availableCash().toFixed()).toBe("1000");
    } finally {
      await restarted.engine.stop();
    }
  });
});
