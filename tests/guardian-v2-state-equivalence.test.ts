import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { evaluateGuardianV2, type GuardianV2State } from "@pm/core";

/** Golden digest captured from BEFORE the G2D optimization on stage-08 frozen Guardian V2. */
const BASELINE_SHA256 = "420a79582087070595d5965c29e5b6d3ece2606ea471db26d82f10aa02c3ef17";

const START = Date.parse("2026-10-06T10:00:00.000Z");
const episode = {
  marketId: "g2d-market", positionId: "g2d-position",
  heldOutcome: "DOWN" as const, unhedgedStartedAt: new Date(START).toISOString(),
};

describe("Guardian G2D state-equivalence: no semantic or durable envelope changes", () => {
  it("replays 720 timestamped events and preserves exact baseline output bytes", () => {
    let state: GuardianV2State | null = null;
    const hash = createHash("sha256");
    for (let i = 0; i < 720; i++) {
      const heldPrice = i < 600 ? "0.80" : i % 8 === 0 ? "0.77" : "0.81";
      const now = new Date(START + i * 15);
      const priorBytes = JSON.stringify(state);
      const next = evaluateGuardianV2({
        generation: "BAD_REGION_V3", state, episode, entryPrice: "0.81", now,
        observation: { eventId: `g2d-event-${i}`, heldPrice, bookPressure: "0.25" },
      });
      // Evaluator must not mutate the prior state even with hundreds of history samples.
      expect(JSON.stringify(state)).toBe(priorBytes);
      hash.update(JSON.stringify({ ...next, state: next.nextState }) + "\n");
      state = next.nextState;
    }
    const digest = hash.digest("hex");
    expect(digest).toBe(BASELINE_SHA256);
  });
});
