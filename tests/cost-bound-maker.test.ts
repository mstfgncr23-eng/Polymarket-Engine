import { describe, expect, it } from "vitest";
import { costBoundMakerBuyPrice, engineEnvSchema, planDualMakerQuotes, singleFillHedgeEconomics } from "@pm/core";
import { snapshot } from "./fixtures.js";

const T = Date.parse("2026-10-15T18:00:00Z");
const base = () => snapshot({
  marketId: "dual-pair", asset: "ETH",
  sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
});

function up(overrides: Record<string, unknown> = {}) {
  return snapshot({
    ...base(), tokenId: "dual-up", outcome: "UP",
    bids: [{ price: "0.95", quantity: "100" }], asks: [{ price: "0.96", quantity: "100" }],
    ...overrides,
  });
}
function down(overrides: Record<string, unknown> = {}) {
  return snapshot({
    ...base(), tokenId: "dual-down", outcome: "DOWN",
    bids: [{ price: "0.02", quantity: "100" }], asks: [{ price: "0.04", quantity: "100" }],
    ...overrides,
  });
}
function candidate(overrides: Record<string, unknown> = {}) {
  return planDualMakerQuotes({
    up: up(), down: down(), quantity: "5",
    perSideRiskCapUsd: "5", availableCashUsd: "1000",
    minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000,
    ...overrides,
  });
}

describe("cost-bound maker quote (maliyet sınırlı kotasyon)", () => {
  it("is a monotonic function of the ROI floor and returns a passive floored-tick price", () => {
    const book = up();
    const opposite = down();
    const p005 = costBoundMakerBuyPrice({ book, opposite, quantity: "5", minPairNetRoi: 0.005 });
    const p015 = costBoundMakerBuyPrice({ book, opposite, quantity: "5", minPairNetRoi: 0.015 });
    const p03 = costBoundMakerBuyPrice({ book, opposite, quantity: "5", minPairNetRoi: 0.03 });
    expect("price" in p005 && "price" in p015 && "price" in p03).toBe(true);
    if (!("price" in p005) || !("price" in p015) || !("price" in p03)) throw new Error("missing price");
    expect(Number(p005.price)).toBeGreaterThan(Number(p015.price));
    expect(Number(p015.price)).toBeGreaterThan(Number(p03.price));
    for (const r of [p005, p015, p03]) {
      // on tick (100x integer), strictly below the quoted side's ask, never crossed
      const priceCents = Number(r.price) * 100;
      expect(Math.abs(priceCents - Math.round(priceCents))).toBeLessThan(1e-6);
      expect(Number(r.price)).toBeLessThan(0.96);
    }
  });
  it("rejects when the opposite ASK depth cannot cover the full residual quantity", () => {
    const book = up();
    const shallow = down({ asks: [{ price: "0.04", quantity: "1" }] });
    const r = costBoundMakerBuyPrice({ book, opposite: shallow, quantity: "5", minPairNetRoi: 0.015 });
    expect(r).toEqual({ reason: "INSUFFICIENT_OPPOSITE_DEPTH" });
  });
  it("rejects empty opposite book, missing fee schedule, missing tick and invalid inputs", () => {
    const book = up();
    const noAsk = down({ asks: [] });
    const noFee = down({ feeSchedule: null });
    const noTick = down({ tickSize: null });
    expect(costBoundMakerBuyPrice({ book, opposite: noAsk, quantity: "5", minPairNetRoi: 0.015 })).toEqual({ reason: "NO_OPPOSITE_ASK" });
    expect(costBoundMakerBuyPrice({ book, opposite: noFee, quantity: "5", minPairNetRoi: 0.015 })).toEqual({ reason: "NO_OPPOSITE_FEE_SCHEDULE" });
    expect(costBoundMakerBuyPrice({ book, opposite: noTick, quantity: "5", minPairNetRoi: 0.015 })).toEqual({ reason: "NO_TICK_SIZE" });
    expect(costBoundMakerBuyPrice({ book, opposite: down(), quantity: "0", minPairNetRoi: 0.015 })).toEqual({ reason: "ROI_UNREACHABLE" });
    expect(costBoundMakerBuyPrice({ book, opposite: down(), quantity: "5", minPairNetRoi: -1 })).toEqual({ reason: "ROI_UNREACHABLE" });
  });
});

describe("planDualMakerQuotes cost-bound integration", () => {
  it("preserves historical join-the-bid behavior when costBound is disabled (default)", () => {
    const result = candidate();
    expect(result.reason).toBeNull();
    if (!result.plan) throw new Error("missing plan");
    expect(result.plan.up.price).toBe("0.95");
    expect(result.plan.down.price).toBe("0.02");
    expect(result.plan.worstCaseSingleFill).toBeUndefined();
    expect(result.plan.fillAssumption).toBe("BOTH_QUOTES_FILL_AT_LIMIT");
  });
  it("rejects a high-priced plan whose worst-case single-fill taker hedge is not covered", () => {
    // 0.95 UP / 0.02 DOWN join: the paired spread looks fine, but if only one leg
    // fills as a maker, the residual must be hedged at the opposite ASK (0.04 /
    // 0.96) which does not clear the ROI floor -> the cost-bound gate refuses.
    const result = candidate({ costBound: true });
    expect(result.plan).toBeNull();
    expect(result.reason).toBe("HEDGE_COST_NOT_COVERED");
  });
  it("admits a plan whose single-fill worst case is covered and keeps the best-bid join prices", () => {
    const result = planDualMakerQuotes({
      up: snapshot({ ...up(), bids: [{ price: "0.70", quantity: "100" }], asks: [{ price: "0.71", quantity: "100" }] }),
      down: snapshot({ ...down(), bids: [{ price: "0.20", quantity: "100" }], asks: [{ price: "0.21", quantity: "100" }] }),
      quantity: "2", perSideRiskCapUsd: "5", availableCashUsd: "1000",
      minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000, costBound: true,
    });
    expect(result.reason).toBeNull();
    if (!result.plan) throw new Error("missing plan");
    expect(result.plan.up.price).toBe("0.7");
    expect(result.plan.down.price).toBe("0.2");
    expect(result.plan.worstCaseSingleFill).toBeDefined();
    expect(result.plan.worstCaseSingleFill!.up.covered).toBe(true);
    expect(result.plan.worstCaseSingleFill!.down.covered).toBe(true);
    expect(Number(result.plan.netPairPnlUsd)).toBeGreaterThan(0);
  });
  it("rejects with HEDGE_COST_NOT_COVERED when the opposite depth is insufficient for the residual", () => {
    const shallow = down({ asks: [{ price: "0.04", quantity: "1" }] });
    const result = planDualMakerQuotes({
      up: up(), down: shallow, quantity: "5",
      perSideRiskCapUsd: "5", availableCashUsd: "1000",
      minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000, costBound: true,
    });
    expect(result.plan).toBeNull();
    expect(result.reason).toBe("HEDGE_COST_NOT_COVERED");
  });
  it("does not weaken the existing admission guards when costBound is enabled", () => {
    // mismatched identity is still rejected first, before any cost-bound work
    expect(candidate({ down: snapshot({ ...down(), marketId: "different" }), costBound: true }).reason).toBe("MARKET_IDENTITY");
    // stale books still rejected
    expect(candidate({ nowMs: T + 3000, costBound: true }).reason).toBe("STALE_OR_GAPPED_BOOK");
    // sub-minimum quantity still rejected
    expect(candidate({ quantity: ".5", costBound: true }).reason).toBe("MINIMUM_SIZE");
  });
  it("is exported and the env flag is default OFF", () => {
    expect(engineEnvSchema.parse({}).PAPER_DUAL_MAKER_COST_BOUND_ENABLED).toBe(false);
    expect(engineEnvSchema.parse({ PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true" }).PAPER_DUAL_MAKER_COST_BOUND_ENABLED).toBe(true);
  });
});

describe("singleFillHedgeEconomics", () => {
  it("reports uncovered worst-case economics when the hedge is too expensive", () => {
    const econ = singleFillHedgeEconomics({
      book: up(), opposite: down(), quantity: "5", price: "0.95", makerFeeUsd: "0", minPairNetRoi: 0.015,
    });
    // 0.95 UP fill + 0.04 DOWN taker hedge does not clear the 1.5% ROI floor.
    expect(econ.covered).toBe(false);
    expect(econ.netRoi).toBeLessThan(0.015);
  });
  it("reports covered worst-case economics at the cost-bound capped price", () => {
    const econ = singleFillHedgeEconomics({
      book: up(), opposite: down(), quantity: "5", price: "0.94", makerFeeUsd: "0", minPairNetRoi: 0.015,
    });
    expect(econ.covered).toBe(true);
    expect(econ.netRoi).toBeGreaterThanOrEqual(0.015);
    expect(Number(econ.hedgeCostUsd)).toBeGreaterThan(0);
  });
});

