import { describe, expect, it } from "vitest";
import { TemporalPairHedgeStrategy, consensusDecisionSchema, positionSchema } from "@pm/core";
import { now, snapshot } from "./fixtures.js";

function consensus(direction: "UP" | "DOWN" | "FLAT") {
  return consensusDecisionSchema.parse({
    id: "decision-first-leg-cap",
    traceId: "trace-first-leg-cap",
    triggerId: "trigger-first-leg-cap",
    consensusDirection: direction,
    consensusConfidence: 0.9,
    agentAgreement: 1,
    signalFreshness: 1,
    conflicts: [],
    estimatedEdge: 0.05,
    evidenceIds: ["evidence-first-leg-cap"],
    calculatedAt: now.toISOString(),
    authority: "NATIVE_DETERMINISTIC_V1",
  });
}

describe("temporal pair hedge first-leg risk cap", () => {
  it("caps a fresh unhedged first leg at $5 notional by default", () => {
    const strategy = new TemporalPairHedgeStrategy();
    const up = snapshot({
      marketId: "cap-market",
      tokenId: "up-cap",
      outcome: "UP",
      bids: [{ price: "0.47", quantity: "100" }],
      asks: [{ price: "0.50", quantity: "100" }],
    });
    const down = snapshot({
      marketId: "cap-market",
      tokenId: "down-cap",
      outcome: "DOWN",
      bids: [{ price: "0.49", quantity: "100" }],
      asks: [{ price: "0.50", quantity: "100" }],
    });

    const candidate = strategy.evaluate({
      consensus: consensus("UP"),
      snapshots: { UP: up, DOWN: down },
      position: null,
      maxOrderSizeUsd: 25,
      minimumLockedPairEdge: 0.015,
    });

    expect(candidate?.intent).toBe("OPEN_FIRST_LEG");
    expect(candidate?.strategyVersion).toBe("temporal-pair-hedge-v1.1");
    expect(candidate?.reasonCodes).toContain("FIRST_LEG_RISK_CAPPED");
    expect(Number(candidate?.quantity) * 0.5).toBeCloseTo(5, 8);
  });

  it("does not apply the first-leg cap to a risk-reducing hedge", () => {
    const strategy = new TemporalPairHedgeStrategy();
    const up = snapshot({ marketId: "hedge-market", tokenId: "up-hedge", outcome: "UP", asks: [{ price: "0.45", quantity: "100" }] });
    const down = snapshot({ marketId: "hedge-market", tokenId: "down-hedge", outcome: "DOWN", asks: [{ price: "0.50", quantity: "100" }] });
    const position = positionSchema.parse({
      id: "pos-hedge-market",
      marketId: "hedge-market",
      upQuantity: "20",
      downQuantity: "0",
      averageUpCost: "0.45",
      averageDownCost: "0",
      pairedQuantity: "0",
      unpairedOutcome: "UP",
      unpairedQuantity: "20",
      combinedPairCost: "0.45",
      totalFees: "0",
      totalSlippage: "0",
      openedAt: now.toISOString(),
      updatedAt: now.toISOString(),
    });

    const candidate = strategy.evaluate({
      consensus: consensus("UP"),
      snapshots: { UP: up, DOWN: down },
      position,
      maxOrderSizeUsd: 25,
      minimumLockedPairEdge: 0.015,
    });

    expect(candidate?.intent).toBe("HEDGE_SECOND_LEG");
    expect(candidate?.quantity).toBe("20");
    expect(candidate?.reasonCodes).not.toContain("FIRST_LEG_RISK_CAPPED");
  });

  it("allows a smaller explicit cap without changing hedge capacity", () => {
    const strategy = new TemporalPairHedgeStrategy();
    const up = snapshot({ marketId: "override-market", tokenId: "up-override", outcome: "UP", asks: [{ price: "0.40", quantity: "100" }] });
    const down = snapshot({ marketId: "override-market", tokenId: "down-override", outcome: "DOWN", asks: [{ price: "0.60", quantity: "100" }] });

    const candidate = strategy.evaluate({
      consensus: consensus("UP"),
      snapshots: { UP: up, DOWN: down },
      position: null,
      maxOrderSizeUsd: 25,
      firstLegRiskCapUsd: 2,
      minimumLockedPairEdge: 0.015,
    });

    expect(candidate?.intent).toBe("OPEN_FIRST_LEG");
    expect(Number(candidate?.quantity) * 0.4).toBeCloseTo(2, 8);
  });
});
