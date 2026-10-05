import { describe, expect, it } from "vitest";
import { evaluateReversalProtection, reversalProtectionStateSchema, type ReversalProtectionState, engineEnvSchema } from "@pm/core";

const START = Date.parse("2026-10-05T00:00:00Z");
const episode = { marketId: "m", positionId: "p", unhedgedStartedAt: new Date(START).toISOString(), heldOutcome: "UP" as const };
function step(state: ReversalProtectionState | null, direction: "UP" | "DOWN" | "FLAT", id: string, ms = 1000, current = episode) {
  return evaluateReversalProtection({ state, episode: current, consensusEventId: id, consensusDirection: direction, now: new Date(START + ms) });
}
describe("reversal protection pure classifier", () => {
  it("RP-CORE-01/02/03: cumulative opposites ignore same and FLAT without reset", () => {
    let state: ReversalProtectionState | null = null;
    for (const [i, direction] of (["DOWN", "UP", "FLAT", "DOWN", "UP", "DOWN"] as const).entries()) {
      state = step(state, direction, `event-${i}`, (i + 1) * 1000).nextState;
      expect(state?.oppositeCount).toBe([1, 1, 1, 2, 2, 3][i]);
    }
    expect(state?.confirmedByConsensusEventId).toBe("event-5");
    expect(reversalProtectionStateSchema.safeParse(state).success).toBe(true);
  });
  it("RP-CORE-04/05: two do not confirm; third at 46s confirms immediately", () => {
    let state = step(null, "DOWN", "1").nextState;
    state = step(state, "DOWN", "2", 2000).nextState;
    expect(state?.confirmedAt).toBeNull();
    state = step(state, "DOWN", "3", 46000).nextState;
    expect(state?.confirmedAt).toBe(new Date(START + 46000).toISOString());
  });
  it.each([360000, 360001])("RP-CORE-06: inclusive boundary at %sms", (ms) => {
    const state = step(step(null, "DOWN", "1").nextState, "DOWN", "2", 2000).nextState;
    const next = step(state, "DOWN", "3", ms);
    expect(next.stateChanged).toBe(ms === 360000);
    expect(!!next.nextState?.confirmedAt).toBe(ms === 360000);
  });
  it("RP-CORE-07: exact event duplicate counts once", () => {
    const state = step(null, "DOWN", "same").nextState;
    expect(step(state, "DOWN", "same")).toEqual({ nextState: state, stateChanged: false });
  });
  it("RP-CORE-08: confirmed state is irreversible and bounded", () => {
    let state: ReversalProtectionState | null = null;
    for (let i = 1; i <= 3; i++) state = step(state, "DOWN", String(i), i * 1000).nextState;
    for (const direction of ["UP", "FLAT", "DOWN"] as const) expect(step(state, direction, "4", 400000)).toEqual({ nextState: state, stateChanged: false });
    expect(state?.countedConsensusEventIds).toHaveLength(3);
  });
  it.each(["positionId", "unhedgedStartedAt"] as const)("RP-CORE-09: new %s cannot inherit", (field) => {
    let state: ReversalProtectionState | null = null;
    for (let i = 1; i <= 3; i++) state = step(state, "DOWN", String(i), i * 1000).nextState;
    const changed = { ...episode, [field]: field === "positionId" ? "new-p" : new Date(START + 1000).toISOString() };
    const result = step(state, "DOWN", "new", 4000, changed).nextState;
    expect(result?.oppositeCount).toBe(1); expect(result?.confirmedAt).toBeNull();
  });
  it("no residual or pre-entry event is a no-op", () => {
    expect(evaluateReversalProtection({ state: null, episode: null, consensusEventId: "x", consensusDirection: "DOWN", now: new Date(START) }).stateChanged).toBe(false);
    expect(step(null, "DOWN", "old", -1).stateChanged).toBe(false);
  });
  it("defaults OFF and validates positive window/integer count", () => {
    expect(engineEnvSchema.parse({}).REVERSAL_PROTECTION_ENABLED).toBe(false);
    for (const input of [{ REVERSAL_PROTECTION_WINDOW_MS: 0 }, { REVERSAL_PROTECTION_OPPOSITE_COUNT: 0 }, { REVERSAL_PROTECTION_OPPOSITE_COUNT: 2.5 }]) expect(engineEnvSchema.safeParse(input).success).toBe(false);
  });
});
