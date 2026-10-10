import { describe, expect, it } from "vitest";
import {
  guardianMarketObservationSchema, guardianProtectionRequestSchema,
  precheckGuardianProtectionRequest, type GuardianProtectionRequest,
  type GuardianRequestPrecheckContext,
} from "@pm/core";

const START = "2026-10-10T10:00:00.000Z";
const AT = "2026-10-10T10:01:00.000Z";
const CONFIRMED = "2026-10-10T10:01:01.000Z";
const REQUESTED = "2026-10-10T10:01:02.000Z";
const EXPIRES = "2026-10-10T10:01:08.000Z";

function request(): GuardianProtectionRequest {
  return {
    version: 1, mode: "PAPER", policyGeneration: 3,
    requestId: "guardian-req-00001",
    experimentId: "experiment-test-001",
    episode: {
      marketId: "market-7", positionId: "pos:market-7",
      unhedgedStartedAt: START, heldOutcome: "DOWN",
    },
    expectedResidualQty: "5.81395349", sourceDurableCursor: 60214,
    requestedKind: "PROTECTIVE_SELL", decisionReason: "EARLY_TIMEOUT",
    evidence: { upEventId: "poly-up-001", downEventId: "poly-down-002", syncEpoch: 5, observedAt: AT },
    confirmedAt: CONFIRMED, requestedAt: REQUESTED, expiresAt: EXPIRES,
  };
}

function context(): GuardianRequestPrecheckContext {
  return {
    mode: "PAPER", currentEpisode: { ...request().episode },
    currentResidualQty: "5.8139534900", currentDurableCursor: 60214,
    now: new Date("2026-10-10T10:01:03.000Z"),
    maxObservationAgeMs: 15_000, maxClockSkewMs: 1000,
  };
}

const expectRejected = (candidate: unknown, ctx: GuardianRequestPrecheckContext, reason: string) =>
  expect(precheckGuardianProtectionRequest(candidate, ctx)).toEqual({ status: "REJECTED", reason });

describe("G1 Guardian request envelope, no runtime and NO SHADOW", () => {
  it("G1-01 rejects unknown fields, unsupported versions, live mode and arbitrary actions", () => {
    const valid = request();
    expect(guardianProtectionRequestSchema.safeParse(valid).success).toBe(true);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, backdoorExecute: true }).success).toBe(false);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, version: 2 }).success).toBe(false);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, mode: "LIVE" }).success).toBe(false);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, policyGeneration: 4 }).success).toBe(false);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, requestedKind: "OPEN_FIRST_LEG" }).success).toBe(false);
    expect(guardianProtectionRequestSchema.safeParse({ ...valid, decisionReason: "MAX_UNHEDGED_DURATION" }).success).toBe(false);
  });

  it.each(["0", "-1", "NaN", "Infinity", "1e3", " 5", "05", "0.0", "1.1234567890123456789", "1."])(
    "G1-02 rejects malformed/nonpositive quantity %s", qty => {
      expectRejected({ ...request(), expectedResidualQty: qty }, context(), "INVALID_REQUEST");
    },
  );

  it("G1-03 rejects reopening same market under new episode, even stable position id", () => {
    const old = request();
    const newer = { ...context(), currentEpisode: { ...old.episode, unhedgedStartedAt: "2026-10-10T10:01:00Z" } };
    expectRejected(old, newer, "EPISODE_MISMATCH");
    expectRejected(old, { ...context(), currentEpisode: { ...old.episode, positionId: "pos:replacement" } }, "EPISODE_MISMATCH");
    expectRejected(old, { ...context(), currentEpisode: null }, "EPISODE_MISMATCH");
    expectRejected(old, { ...context(), currentEpisode: { ...old.episode, heldOutcome: "UP" } }, "EPISODE_MISMATCH");
  });

  it("G1-04 blocks stale residual/future cursor but tolerates unrelated journal progress", () => {
    expectRejected(request(), { ...context(), currentResidualQty: "3.000" }, "RESIDUAL_CHANGED");
    expectRejected(request(), { ...context(), currentResidualQty: "0" }, "RESIDUAL_CHANGED");
    expect(precheckGuardianProtectionRequest(request(), { ...context(), currentDurableCursor: 60215 }).status).toBe("CANDIDATE_ONLY");
    expectRejected(request(), { ...context(), currentDurableCursor: 60213 }, "SOURCE_CURSOR_AHEAD");
    expectRejected(request(), { ...context(), currentDurableCursor: Number.NaN }, "SOURCE_CURSOR_AHEAD");
  });

  it("G1-05 enforces request and market timestamp ordering and expiry", () => {
    const valid = request();
    expectRejected({ ...valid, evidence: { ...valid.evidence, observedAt: "2026-10-10T09:59:59Z" } }, context(), "INVALID_REQUEST");
    expectRejected({ ...valid, confirmedAt: "2026-10-10T10:00:59Z" }, context(), "INVALID_REQUEST");
    expectRejected({ ...valid, expiresAt: "2026-10-10T10:01:42Z" }, context(), "INVALID_REQUEST");
    expectRejected({ ...valid, expiresAt: REQUESTED }, context(), "INVALID_REQUEST");
    expectRejected(valid, { ...context(), now: new Date("2026-10-10T10:01:20Z") }, "STALE_OBSERVATION");
    expectRejected(valid, { ...context(), now: new Date(EXPIRES) }, "EXPIRED_REQUEST");
    expectRejected(valid, { ...context(), now: new Date("2026-10-10T10:00:58Z") }, "FUTURE_OBSERVATION");
    expectRejected(valid, { ...context(), now: new Date("2026-10-10T10:01:00Z") }, "FUTURE_REQUEST");
  });

  it("G1-06 a matching duplicate never re-executes, and a conflicting id fails closed", () => {
    const valid = request();
    const reordered = { requestedAt: REQUESTED, ...valid };
    expect(precheckGuardianProtectionRequest(valid, { ...context(), existingRequest: reordered }))
      .toEqual({ status: "REPLAY_REQUIRES_DURABLE_RESULT_LOOKUP" });
    expectRejected(valid, { ...context(), existingRequest: { ...valid, expectedResidualQty: "5.81395348" } },
      "IDEMPOTENCY_KEY_CONFLICT");
    expectRejected(valid, { ...context(), existingRequest: { ...valid, requestId: "guardian-req-00002" } },
      "IDEMPOTENCY_KEY_CONFLICT");
    expectRejected(valid, { ...context(), existingRequest: { garbage: true } }, "IDEMPOTENCY_KEY_CONFLICT");
  });

  it("G1-07 precheck success is a candidate, NEVER economic authorization", () => {
    expect(precheckGuardianProtectionRequest(request(), context())).toEqual({ status: "CANDIDATE_ONLY", request: request() });
    expectRejected(request(), { ...context(), mode: "LIVE" }, "MODE_MISMATCH");
    expectRejected(request(), { ...context(), maxObservationAgeMs: -1 }, "INVALID_REQUEST");
  });
});

describe("G1 versioned market observation model", () => {
  const observation = () => ({
    version: 1, source: "CLOB", marketId: "market-7", syncEpoch: 5, observedAt: AT,
    up: { tokenId: "UP-token", eventId: "UP-event", receivedAt: AT,
      bestBid: "0.46", bestAsk: "0.47", bidSize: "200", askSize: "100" },
    down: { tokenId: "DOWN-token", eventId: "DOWN-event", receivedAt: AT,
      bestBid: "0.53", bestAsk: "0.54", bidSize: "100", askSize: "200" },
  });

  it("G1-08 validates paired CLOB envelope only, not book freshness or executable depth", () => {
    expect(guardianMarketObservationSchema.safeParse(observation()).success).toBe(true);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), extra: "x" }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), syncEpoch: -1 }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), down: { ...observation().down, tokenId: "UP-token" } }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), down: { ...observation().down, bestAsk: "0.50" } }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), down: { ...observation().down, receivedAt: "2026-10-10T10:01:01Z" } }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), up: { ...observation().up, bestAsk: " NaN" } }).success).toBe(false);
    expect(guardianMarketObservationSchema.safeParse({ ...observation(), up: { ...observation().up, bidSize: "9".repeat(200) } }).success).toBe(false);
  });
});
