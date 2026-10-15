import { describe, expect, it } from "vitest";
import { createJournalEvent } from "@pm/db";
import type { Position } from "@pm/core";
import { buildExperimentSummary } from "../apps/engine/src/report.js";
import { emptyResultAccounting, economicResults, tradingResults } from "../apps/engine/src/trading-results.js";
import { PaperLedger } from "@pm/core";

const baseAt = new Date("2026-09-16T15:00:00.000Z");

function pos(overrides: Partial<Position> = {}): Position {
  return {
    id: "pos:m1",
    marketId: "m1",
    upQuantity: "10",
    downQuantity: "0",
    averageUpCost: "0.5",
    averageDownCost: "0",
    pairedQuantity: "0",
    unpairedOutcome: "UP",
    unpairedQuantity: "10",
    combinedPairCost: "0.5",
    totalFees: "0",
    totalSlippage: "0",
    openedAt: baseAt.toISOString(),
    updatedAt: baseAt.toISOString(),
    ...overrides,
  };
}

function event(type: string, aggregateType: string, aggregateId: string, payload: Record<string, unknown>, seconds: number) {
  return createJournalEvent({
    traceId: `trace-${seconds}`,
    type,
    aggregateType,
    aggregateId,
    payload,
    occurredAt: new Date(baseAt.getTime() + seconds * 1000),
  });
}

describe("native final report semantics", () => {
  it("uses durable whole-trade dollar totals instead of a settlement WIN count", () => {
    const accounting = emptyResultAccounting();
    accounting.completed = 1; accounting.losses = 1;
    accounting.economics = { trades: 1, meteors: 1, completedNetPnl: "-2.7", completedFees: "0.2", worstTradeNetPnl: "-2.7",
      buySpend: "8", sellProceeds: "0.5", settlementPayout: "5", fees: "0.2" };
    const ledger = new PaperLedger("1000").snapshot();
    ledger.realizedPnl = "-2.5"; ledger.fees = "0.2"; ledger.netTradingPnl = "-2.7";
    const summary = buildExperimentSummary([
      event("POSITION_WIN", "position", "pos:m1", { marketId: "m1" }, 1),
      event("PNL_SNAPSHOT", "ledger", "exp", { ...ledger, economicResults: economicResults(accounting), tradingResults: tradingResults(accounting) }, 2),
    ], "exp");
    expect(summary.winLossDistribution).toEqual({ wins: 0, losses: 1, breakeven: 0 });
    expect(summary.economicResults).toEqual(economicResults(accounting));
    expect(summary.realizedNetTradingPnlUsd).toBe(-2.7);
  });
  it("preserves historical directional exposure after the position settles", () => {
    const position = pos();
    const summary = buildExperimentSummary([
      event("POSITION_UPDATED", "position", position.id, position as unknown as Record<string, unknown>, 1),
      event("POSITION_SETTLED", "position", position.id, {
        marketId: position.marketId,
        upQuantity: position.upQuantity,
        downQuantity: position.downQuantity,
        realizedDelta: "-5",
        grossOutcome: "POSITION_LOSS",
      }, 2),
    ], "exp-report");

    expect(summary.maximumDirectionalExposureUsd).toBe(5);
    expect(summary.incompleteUnhedgedPositions).toBe(0);
  });

  it("attributes canonical settled position losses to acceptedTradesThatLost", () => {
    const summary = buildExperimentSummary([
      event("RISK_EVALUATION", "risk", "risk-1", { accepted: true, marketId: "m1", asset: "BTC" }, 1),
      event("POSITION_LOSS", "position", "pos:m1", { marketId: "m1", realizedDelta: "-4" }, 2),
    ], "exp-loss");

    expect(summary.acceptedTradesThatLost).toBe(1);
    expect(summary.winLossDistribution.losses).toBe(1);
  });

  it("keeps protective risk exits out of alpha opportunity and race-failure metrics", () => {
    const summary = buildExperimentSummary([
      event("RISK_EVALUATION", "risk", "alpha-risk", {
        accepted: true,
        marketId: "m1",
        asset: "BTC",
      }, 1),
      event("RISK_EVALUATION", "risk", "derisk-risk", {
        accepted: false,
        marketId: "m1",
        asset: "BTC",
        intent: "PROTECTIVE_DE_RISK",
        deRisk: { retryCount: 1 },
      }, 2),
      event("PAPER_EXECUTION_ERROR", "paper_order", "race-order", {
        marketId: "m1",
        intent: "PROTECTIVE_DE_RISK",
        stateChanged: true,
      }, 3),
      event("PAPER_EXECUTION_ERROR", "paper_order", "real-error", {
        marketId: "m1",
        message: "INSUFFICIENT_DEPTH",
      }, 4),
    ], "exp-report-separation");

    expect(summary.totalOpportunities).toBe(1);
    expect(summary.acceptedOpportunities).toBe(1);
    expect(summary.rejectedOpportunities).toBe(0);
    expect(summary.failureStatistics.PAPER_EXECUTION_ERROR).toBe(1);
  });

  it("closes failed-hedge episodes without counting protective neutralization as a pair hedge", () => {
    const summary = buildExperimentSummary([
      event("FIRST_LEG_FILLED", "position", "pos:m1", { marketId: "m1", unpairedQuantity: "10" }, 1),
      event("TRIGGER_EVENT", "trigger", "failed-1", { marketId: "m1", kind: "FAILED_HEDGE" }, 2),
      event("HEDGE_COMPLETE", "position", "pos:m1", {
        marketId: "m1",
        pairedQuantity: "10",
        unhedgedDurationMs: 301_000,
        completionMode: "PROTECTIVE_DE_RISK",
        intent: "PROTECTIVE_DE_RISK",
      }, 3),
      // A later episode for the same market must count again, proving the
      // protective neutralization closed the prior failed-hedge episode.
      event("TRIGGER_EVENT", "trigger", "failed-2", { marketId: "m1", kind: "FAILED_HEDGE" }, 4),
    ], "exp-protective-completion");

    expect(summary.firstLegCount).toBe(1);
    expect(summary.failedHedgeCount).toBe(2);
    expect(summary.hedgeCompleteCount).toBe(0);
    expect(summary.averageUnhedgedDurationMs).toBe(0);
  });

  it("counts fresh first-leg cycles after a fully paired position reopens", () => {
    const initial = pos();
    const paired = pos({
      downQuantity: "10",
      averageDownCost: "0.4",
      pairedQuantity: "10",
      unpairedOutcome: null,
      unpairedQuantity: "0",
      combinedPairCost: "0.9",
    });
    const reopened = pos({
      downQuantity: "20",
      averageDownCost: "0.45",
      pairedQuantity: "10",
      unpairedOutcome: "DOWN",
      unpairedQuantity: "10",
      combinedPairCost: "0.95",
    });

    const summary = buildExperimentSummary([
      event("FIRST_LEG_FILLED", "position", initial.id, { marketId: "m1", unpairedQuantity: "10" }, 1),
      event("POSITION_UPDATED", "position", initial.id, initial as unknown as Record<string, unknown>, 2),
      event("HEDGE_COMPLETE", "position", initial.id, { marketId: "m1", pairedQuantity: "10", unhedgedDurationMs: 1000 }, 3),
      event("POSITION_UPDATED", "position", paired.id, paired as unknown as Record<string, unknown>, 4),
      event("POSITION_UPDATED", "position", reopened.id, reopened as unknown as Record<string, unknown>, 5),
      event("TRIGGER_EVENT", "trigger", "failed-1", { marketId: "m1", kind: "FAILED_HEDGE" }, 6),
      event("TRIGGER_EVENT", "trigger", "failed-2", { marketId: "m1", kind: "FAILED_HEDGE" }, 7),
      event("POSITION_SETTLED", "position", reopened.id, { marketId: "m1", realizedDelta: "0" }, 8),
    ], "exp-cycles");

    expect(summary.firstLegCount).toBe(2);
    expect(summary.failedHedgeCount).toBe(1);
    expect(summary.hedgeFailureRate).toBe(0.5);
  });
});
