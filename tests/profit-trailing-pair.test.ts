import { describe, expect, it } from "vitest";
import { evaluateProfitTrailingPair, type ProfitTrailingPairState } from "@pm/core";

const base = {
  marketId: "trail-mkt",
  positionId: "pos:trail-mkt",
  unhedgedStartedAt: "2026-10-05T00:00:00.000Z",
  targetEdge: 0.12,
  armEdge: 0.05,
  retraceEdge: 0.02,
};

function evaluate(edge: string, pnl: string, state: ProfitTrailingPairState | null, second: number) {
  return evaluateProfitTrailingPair({
    ...base,
    state,
    now: new Date(Date.parse(base.unhedgedStartedAt) + second * 1000),
    currentGrossEdge: edge,
    currentLockedPnl: pnl,
  });
}

describe("profit trailing pair policy", () => {
  it("does nothing below arm", () => {
    const result = evaluate("0.049", "0.1", null, 1);
    expect(result).toMatchObject({ action: "NONE", reason: "BELOW_ARM", nextState: null, stateChanged: false });
  });

  it("arms at .05, follows high-water, then trails after a .02 retrace", () => {
    const armed = evaluate("0.05", "0.10", null, 1);
    expect(armed.action).toBe("NONE");
    expect(armed.stateChanged).toBe(true);
    expect(armed.nextState?.highWaterEdge).toBe("0.05");

    const high = evaluate("0.10", "0.30", armed.nextState, 2);
    expect(high.action).toBe("NONE");
    expect(high.stateChanged).toBe(true);
    expect(high.nextState?.highWaterEdge).toBe("0.1");

    const trail = evaluate("0.08", "0.20", high.nextState, 3);
    expect(trail).toMatchObject({ action: "TRAIL", reason: "TRAIL_TRIGGERED", stateChanged: false });
    expect(trail.nextState?.highWaterEdge).toBe("0.1");
  });

  it("takes .12 target before waiting for retrace", () => {
    const result = evaluate("0.12", "0.50", null, 1);
    expect(result.action).toBe("TARGET");
    expect(result.reason).toBe("TARGET_REACHED");
    expect(result.nextState?.highWaterEdge).toBe("0.12");
  });

  it("never target-locks or trail-locks a fee-inclusive loss", () => {
    const target = evaluate("0.12", "-0.01", null, 1);
    expect(target.action).toBe("NONE");

    const armed = evaluate("0.10", "0.20", null, 2);
    const retrace = evaluate("0.08", "0", armed.nextState, 3);
    expect(retrace.action).toBe("NONE");
  });

  it("resets authority when the unhedged episode changes", () => {
    const old = evaluate("0.10", "0.2", null, 1).nextState!;
    const next = evaluateProfitTrailingPair({
      ...base,
      state: old,
      positionId: "pos:trail-mkt:new",
      unhedgedStartedAt: "2026-10-05T00:01:00.000Z",
      now: new Date("2026-10-05T00:01:01.000Z"),
      currentGrossEdge: "0.06",
      currentLockedPnl: "0.1",
    });
    expect(next.stateChanged).toBe(true);
    expect(next.nextState).toMatchObject({ positionId: "pos:trail-mkt:new", highWaterEdge: "0.06", revision: 1 });
  });
});
