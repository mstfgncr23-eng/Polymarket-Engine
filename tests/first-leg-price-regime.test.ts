import { describe, expect, it } from "vitest";
import {
  TemporalPairHedgeStrategy,
  createId,
  decidePaperOrder,
  evaluateRisk,
  riskLimitsSchema,
  type ConsensusDecision,
  type Position,
  type RiskInput,
  type StrategyCandidate,
} from "@pm/core";
import { now, safeRisk, snapshot } from "./fixtures.js";

const limits = riskLimitsSchema.parse({});
const STRICT_EDGE = limits.minExpectedEdge; // 0.015
const STRICT_ROI = limits.minExpectedROI; // 0.020
const NORMAL_EDGE = limits.normalFirstLegMinExpectedEdge; // 0.010
const NORMAL_ROI = limits.normalFirstLegMinExpectedROI; // 0.010
const BOUNDARY = limits.normalFirstLegMaxPriceExclusive; // 0.90 exclusive

const MARKET = "4886202";
const TOKEN = "up-token";
const LOW_CONFIDENCE = 0.6 - 0.001;

function openRisk(overrides: Partial<RiskInput> = {}): RiskInput {
  return {
    ...safeRisk({ intent: "OPEN_FIRST_LEG", signalConfidence: 0.9 }),
    orderSizeUsd: "5",
    executablePrice: "0.85",
    expectedEdge: STRICT_EDGE,
    expectedROI: STRICT_ROI,
    ...overrides,
  };
}

function openCandidate(overrides: Partial<StrategyCandidate> = {}): StrategyCandidate {
  return {
    strategyId: "temporal-pair-hedge", strategyVersion: "v1",
    marketId: MARKET, tokenId: TOKEN, outcome: "UP", side: "BUY",
    quantity: "10", limitPrice: "0.91", intent: "OPEN_FIRST_LEG",
    rawExpectedEdge: 0.02, reasonCodes: ["CONSENSUS_DIRECTION"],
    ...overrides,
  };
}

function openConsensus(confidence = 0.9): ConsensusDecision {
  return {
    id: createId("consensus"), traceId: "t", triggerId: "g", consensusDirection: "UP",
    consensusConfidence: confidence, agentAgreement: 1, signalFreshness: 1, conflicts: [],
    estimatedEdge: 0.02, evidenceIds: ["e1"], calculatedAt: now.toISOString(),
    authority: "NATIVE_DETERMINISTIC_V1", fairProbability: 0.54, marketProbability: 0.51,
    featureVector: {},
  };
}

function openDecisionBaseRisk(): Parameters<typeof decidePaperOrder>[0]["baseRisk"] {
  return {
    now, marketFeedAt: now, btcFeedAt: now, databaseAvailable: true, timestampDisorder: false,
    riskStateAvailable: true, dailyPnlUsd: "0", drawdownUsd: "0", marketExposureUsd: "0",
    totalExposureUsd: "0", directionalExposureUsd: "0", unhedgedExposureUsd: "0",
    unhedgedDurationMs: 0, intent: "OPEN_FIRST_LEG",
    projectedMarketExposureUsd: "5", projectedTotalExposureUsd: "5",
    projectedDirectionalExposureUsd: "5", projectedUnhedgedExposureUsd: "5",
    projectedAssetExposureUsd: "5", availableCashUsd: "100", marketEntryQualified: true,
    externalSignalsLastMinute: 0, lastStrategyTradeAt: null, duplicateTrade: false,
    experimentEntryOpen: true, executionErrorsLastHour: 0, externalRiskRecommendedVeto: false,
    shuttingDown: false,
  };
}

describe("first-leg price-regime economic admission", () => {
  it("1: STRICT boundary exactly 0.90 with exact strict economics passes", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.90", expectedEdge: STRICT_EDGE, expectedROI: STRICT_ROI,
    }), limits);
    expect(risk.vetoes).toEqual([]);
    expect(risk.accepted).toBe(true);
  });

  it("2: price exactly 0.90 with ROI just below strict floor is rejected", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.90", expectedEdge: STRICT_EDGE, expectedROI: STRICT_ROI - 0.00001,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(false);
  });

  it("3: historical DOGE-style near-$1 regression (~0.91 / 1.878% ROI) is still rejected", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.91", expectedEdge: 0.02, expectedROI: 0.01878,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(false);
  });

  it("4: historical ETH-style near-$1 regression (~0.97 / 1.852% ROI) is still rejected", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.97", expectedEdge: 0.02, expectedROI: 0.01852,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(false);
  });

  it("5: NORMAL valid case below 0.90 is accepted economically", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.016,
    }), limits);
    expect(risk.vetoes).not.toContain("MINIMUM_EXPECTED_EDGE");
    expect(risk.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(true);
  });

  it("6: NORMAL edge floor failure (0.00999 < 0.010)", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.00999, expectedROI: NORMAL_ROI,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(risk.accepted).toBe(false);
  });

  it("7: NORMAL ROI floor failure (0.00999 < 0.010)", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.00999,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(false);
  });

  it("8: boundary just below 0.90 (0.899999) selects NORMAL", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.899999", expectedEdge: NORMAL_EDGE, expectedROI: NORMAL_ROI,
    }), limits);
    expect(risk.vetoes).toEqual([]);
    expect(risk.accepted).toBe(true);
  });
it("9: VWAP classification — best ask below 0.90 but full-quantity walk VWAP >= 0.90 stays STRICT", () => {
    const walkVwapBook = snapshot({
      marketId: MARKET, tokenId: TOKEN, outcome: "UP",
      bids: [{ price: "0.86", quantity: "50" }],
      asks: [{ price: "0.88", quantity: "1" }, { price: "0.91", quantity: "199" }],
    });
    const decision = decidePaperOrder({
      candidate: openCandidate({ quantity: "10", limitPrice: "0.91", rawExpectedEdge: 0.02 }),
      consensus: openConsensus(),
      snapshotAfterDecisionLatency: walkVwapBook,
      limits,
      triggerId: "g",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: openDecisionBaseRisk(),
    });
    // The regime price is the post-latency book-walk VWAP for the full quantity,
    // NOT the 0.88 top ask.
    expect(decision.executablePrice).not.toBe("0.88");
    expect(Number(decision.executablePrice)).toBeGreaterThanOrEqual(0.9);
    expect(Number(decision.executablePrice)).toBeLessThan(0.91);
    // net EV/share ≈ 0.0141, ROI ≈ 0.0155: pass NORMAL, fail STRICT.
    expect(decision.executablePrice).toBe("0.907");
    expect(decision.risk.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(decision.risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(decision.accepted).toBe(false);
    expect(decision.order).toBeNull();

    // Same economics at the 0.88 top-ask price WOULD be NORMAL — proving the
    // decision correctly refuses to relax first-leg economics here.
    const wrongTopAsk = evaluateRisk(openRisk({
      executablePrice: "0.88", expectedEdge: 0.0140955, expectedROI: 0.01738,
    }), limits);
    expect(wrongTopAsk.vetoes).not.toContain("MINIMUM_EXPECTED_EDGE");
    expect(wrongTopAsk.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
    expect(wrongTopAsk.accepted).toBe(true);
  });

  it("10: insufficient depth keeps INSUFFICIENT_DEPTH and never falls back to NORMAL", () => {
    const thinBook = snapshot({
      marketId: MARKET, tokenId: TOKEN, outcome: "UP",
      bids: [{ price: "0.84", quantity: "50" }],
      asks: [{ price: "0.85", quantity: "3" }],
    });
    const decision = decidePaperOrder({
      candidate: openCandidate({ quantity: "10", limitPrice: "0.85", rawExpectedEdge: 0.05 }),
      consensus: openConsensus(),
      snapshotAfterDecisionLatency: thinBook,
      limits,
      triggerId: "g",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: openDecisionBaseRisk(),
    });
    expect(decision.executablePrice).toBeNull();
    expect(decision.risk.vetoes).toContain("INSUFFICIENT_DEPTH");
    expect(decision.accepted).toBe(false);
  });

  it("11: unknown fee schedule fails closed through the existing controls", () => {
    const noFeeBook = snapshot({
      marketId: MARKET, tokenId: TOKEN, outcome: "UP",
      bids: [{ price: "0.84", quantity: "50" }],
      asks: [{ price: "0.85", quantity: "50" }],
      feeSchedule: null,
    });
    const decision = decidePaperOrder({
      candidate: openCandidate({ quantity: "10", limitPrice: "0.85", rawExpectedEdge: 0.05 }),
      consensus: openConsensus(),
      snapshotAfterDecisionLatency: noFeeBook,
      limits,
      triggerId: "g",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: openDecisionBaseRisk(),
    });
    expect(decision.risk.vetoes).toContain("UNKNOWN_FEE_SCHEDULE");
    expect(decision.accepted).toBe(false);
  });

  it("12: confidence below 0.60 still rejects a fresh first leg with NORMAL economics", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.016,
      signalConfidence: 0.5,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(risk.accepted).toBe(false);
  });

  it("13: low liquidity still rejects a fresh first leg with NORMAL economics", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.016,
      availableLiquidityUsd: "99",
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_LIQUIDITY");
    expect(risk.accepted).toBe(false);
  });

  it("14: excessive spread still rejects a fresh first leg with NORMAL economics", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.016,
      spread: 0.09,
    }), limits);
    expect(risk.vetoes).toContain("MAXIMUM_SPREAD");
    expect(risk.accepted).toBe(false);
  });

it("15: verified HEDGE_SECOND_LEG below 0.90 gets NO normal first-leg policy", () => {
    const hedgeBase = {
      ...safeRisk({ intent: "HEDGE_SECOND_LEG", signalConfidence: LOW_CONFIDENCE }),
      orderSizeUsd: "5", requiredCashUsd: "5", availableCashUsd: "100",
      directionalExposureUsd: "0.5", unhedgedExposureUsd: "4.9995",
      projectedDirectionalExposureUsd: "0", projectedUnhedgedExposureUsd: "0",
      executablePrice: "0.85",
      hedgeCandidate: { marketId: MARKET, tokenId: TOKEN, outcome: "UP", side: "BUY", quantity: "10" },
      hedgePosition: { marketId: MARKET, unpairedOutcome: "DOWN", unpairedQuantity: "10", expectedOppositeTokenId: TOKEN },
    };
    // Hedge edge floor stays STRICT (0.015): 0.014 must still veto even below 0.90.
    const edgeAt0o014 = evaluateRisk({ ...hedgeBase, expectedEdge: 0.014, expectedROI: 0.05 }, limits);
    expect(edgeAt0o014.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(edgeAt0o014.vetoes).not.toContain("MINIMUM_SIGNAL_CONFIDENCE");
    // PR79 semantics intact: confidence exempt + ROI exempt for a verified hedge.
    const verified = evaluateRisk({ ...hedgeBase, expectedEdge: 0.02, expectedROI: 0.01 }, limits);
    expect(verified.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
    expect(verified.vetoes).not.toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(verified.accepted).toBe(true);
  });

  it("16: fake/unverified hedge gets neither hedge exemptions nor NORMAL relaxation", () => {
    const fake = evaluateRisk({
      ...safeRisk({ intent: "HEDGE_SECOND_LEG", signalConfidence: LOW_CONFIDENCE }),
      orderSizeUsd: "5", availableCashUsd: "100",
      directionalExposureUsd: "0.5", unhedgedExposureUsd: "4.9995",
      projectedDirectionalExposureUsd: "0", projectedUnhedgedExposureUsd: "0",
      executablePrice: "0.85", expectedEdge: 0.014, expectedROI: 0.016,
      hedgeCandidate: null, hedgePosition: null,
    }, limits);
    expect(fake.vetoes).toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(fake.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(fake.accepted).toBe(false);
  });

  it("17: protective de-risk behavior is unchanged under the new policy", () => {
    const risk = evaluateRisk({
      ...safeRisk({ intent: "PROTECTIVE_DE_RISK" }),
      orderSizeUsd: "5", executablePrice: "0.85", expectedEdge: 0.005, expectedROI: 0.005,
      signalConfidence: 0.3,
      directionalExposureUsd: "0.5", unhedgedExposureUsd: "4.9995",
      projectedDirectionalExposureUsd: "0", projectedUnhedgedExposureUsd: "0",
      marketEntryQualified: false,
    }, limits);
    expect(risk.vetoes).not.toContain("MINIMUM_EXPECTED_EDGE");
    expect(risk.vetoes).not.toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.vetoes).not.toContain("MINIMUM_SIGNAL_CONFIDENCE");
    expect(risk.accepted).toBe(true);
  });

  it("18: a fully paired unsettled market can never create a fresh OPEN_FIRST_LEG", () => {
    const paired: Position = {
      id: createId("pos"), marketId: MARKET,
      upQuantity: "10", downQuantity: "10",
      averageUpCost: "0.50", averageDownCost: "0.50",
      pairedQuantity: "10", unpairedOutcome: null, unpairedQuantity: "0",
      unhedgedStartedAt: null, combinedPairCost: "1.00",
      totalFees: "0", totalSlippage: "0",
      openedAt: now.toISOString(), updatedAt: now.toISOString(),
    };
    const snapshotUp = snapshot({
      marketId: MARKET, tokenId: TOKEN, outcome: "UP",
      bids: [{ price: "0.47", quantity: "500" }], asks: [{ price: "0.48", quantity: "500" }],
    });
    const snapshotDown = snapshot({
      marketId: MARKET, tokenId: "down-token", outcome: "DOWN",
      bids: [{ price: "0.51", quantity: "500" }], asks: [{ price: "0.52", quantity: "500" }],
    });
    const candidate = new TemporalPairHedgeStrategy().evaluate({
      consensus: openConsensus(),
      snapshots: { UP: snapshotUp, DOWN: snapshotDown },
      position: paired,
      maxOrderSizeUsd: 25,
      minimumLockedPairEdge: 0.015,
    });
    expect(candidate).toBeNull();
  });

  it("19: duplicate admission remains vetoed even with NORMAL economics", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: "0.85", expectedEdge: 0.013, expectedROI: 0.016,
      duplicateTrade: true,
    }), limits);
    expect(risk.vetoes).toContain("DUPLICATE_TRADE");
    expect(risk.accepted).toBe(false);
  });

  it("fail-closed A: OPEN_FIRST_LEG without an established executable price stays STRICT", () => {
    const risk = evaluateRisk(openRisk({
      executablePrice: undefined, expectedEdge: 0.013, expectedROI: 0.016,
    }), limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(risk.accepted).toBe(false);
  });

  it("fail-closed B: unknown/missing intent never receives NORMAL thresholds", () => {
    const risk = evaluateRisk({
      ...safeRisk({ intent: undefined }),
      orderSizeUsd: "5", executablePrice: "0.85",
      expectedEdge: 0.013, expectedROI: 0.016,
    }, limits);
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
    expect(risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(risk.accepted).toBe(false);
  });
  });

describe("full-cycle first-leg economic budget",()=>{
  it("subtracts the exit allowance from net EV while preserving actual entry fee reporting",()=>{
    const book=snapshot({marketId:MARKET,tokenId:TOKEN,bids:[{price:"0.94",quantity:"1000"}],asks:[{price:"0.95",quantity:"1000"}]});
    const input={candidate:openCandidate({quantity:"5",limitPrice:"0.95",rawExpectedEdge:0.02}),consensus:openConsensus(),
      snapshotAfterDecisionLatency:book,limits:riskLimitsSchema.parse({freshEntryNetRoiFloor:0.01}),triggerId:"g",
      signalExpiresAt:new Date(now.getTime()+60000),baseRisk:openDecisionBaseRisk()};
    const legacy=decidePaperOrder(input);
    const cycle=decidePaperOrder({...input,limits:{...input.limits,freshEntryExitFeeReserve:true}});
    expect(legacy.accepted).toBe(true);
    expect(cycle.risk.vetoes).toContain("MINIMUM_EXPECTED_ROI");
    expect(cycle.exitFeeReserveUsd).toBe("0.08750");
    expect(cycle.feeUsdTotal).toBe(legacy.feeUsdTotal);
    expect(Number(legacy.netEvUsdTotal)-Number(cycle.netEvUsdTotal)).toBeCloseTo(0.0875,8);
    expect(cycle.executableNotionalUsd).toBe(legacy.executableNotionalUsd);
    const dual=decidePaperOrder({...input,limits:{...input.limits,freshEntryExitFeeReserve:true},baseRisk:{...input.baseRisk,dualPairEconomics:{netRoi:0.02,netUsdPerShare:0.019}}});
    expect(dual.exitFeeReserveUsd).toBe("0");
  });
});
