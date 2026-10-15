import { describe, expect, it } from "vitest";
import { planDualMakerQuotes } from "@pm/core";
import { snapshot } from "./fixtures.js";

const T = Date.parse("2026-10-15T18:00:00Z");
const up = () => snapshot({
  marketId: "dual-pair", tokenId: "dual-up", outcome: "UP", asset: "ETH",
  bids: [{ price: "0.8", quantity: "100" }],
  asks: [{ price: "0.81", quantity: "100" }],
  sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
});
const down = () => snapshot({
  marketId: "dual-pair", tokenId: "dual-down", outcome: "DOWN", asset: "ETH",
  bids: [{ price: "0.16", quantity: "100" }],
  asks: [{ price: "0.17", quantity: "100" }],
  sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
});
function candidate(overrides: Record<string, unknown> = {}) {
  return planDualMakerQuotes({
    up: up(), down: down(), quantity: "6.25",
    perSideRiskCapUsd: "5", availableCashUsd: "1000",
    minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000,
    ...overrides,
  });
}
describe("two-sided maker economic planner, no runtime authorization", () => {
  it("prices two passive quotes as a conditional net pair, including the low-price side", () => {
    const result = candidate();
    expect(result.reason).toBeNull();
    if (!result.plan) throw new Error("missing plan");
    expect(result.plan.up.price).toBe("0.8");
    expect(result.plan.down.price).toBe("0.16");
    expect(result.plan.up.reserveUsd).toBe("5");
    expect(result.plan.down.reserveUsd).toBe("1");
    expect(result.plan.netPairPnlUsd).toBe("0.25");
    expect(result.plan.maxUnpairedDownsideUsd).toBe("5");
    expect(result.plan.fillAssumption).toBe("BOTH_QUOTES_FILL_AT_LIMIT");
  });
  it("does not claim a guaranteed fill or realized PnL", () => {
    const result = candidate();
    expect(result.plan?.fillAssumption).toBe("BOTH_QUOTES_FILL_AT_LIMIT");
  });
  it("rejects an unprofitable combined price", () => {
    expect(candidate({down:snapshot({...down(),bids:[{price:"0.19",quantity:"100"}],asks:[{price:"0.2",quantity:"100"}]})}).reason)
      .toBe("INSUFFICIENT_LOCKED_EDGE");
  });
  it("enforces a per-side loss budget independently of the cheap opposite leg", () => {
    expect(candidate({perSideRiskCapUsd:"4.9"}).reason).toBe("SIDE_EXPOSURE_CAP");
    expect(candidate({availableCashUsd:"5.99"}).reason).toBe("INSUFFICIENT_CASH");
  });
  it("rejects mismatched identities, stale books, nonpassive quotes and subminimum quantity", () => {
    expect(candidate({down:snapshot({...down(),marketId:"different"})}).reason).toBe("MARKET_IDENTITY");
    expect(candidate({nowMs:T+3000}).reason).toBe("STALE_OR_GAPPED_BOOK");
    expect(candidate({up:snapshot({...up(),asks:[{price:"0.8",quantity:"100"}]})}).reason).toBe("NON_PASSIVE_QUOTE");
    expect(candidate({quantity:".5"}).reason).toBe("MINIMUM_SIZE");
  });
});
