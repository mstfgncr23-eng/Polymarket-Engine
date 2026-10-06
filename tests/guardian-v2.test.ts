import { describe, expect, it } from "vitest";
import { evaluateGuardianV2, guardianV2StateSchema, GUARDIAN_V2_CONFIG, isLockedGuardianV2Config,
  normalizeOppositeBookPressure, type GuardianV2State } from "@pm/core";

const START = Date.parse("2026-10-06T00:00:00Z");
const episode = { marketId: "m", positionId: "p", heldOutcome: "UP" as const, unhedgedStartedAt: new Date(START).toISOString() };
function step(state: GuardianV2State | null, ms: number, heldRatio: string | null, bookPressure = "0.20", consensus?: {
  eventId: string; direction: "UP" | "DOWN" | "FLAT"; confidence: number; estimatedEdge: number;
}, id = `sample-${ms}`) {
  return evaluateGuardianV2({ state, episode, entryPrice: "1", now: new Date(START + ms),
    observation: heldRatio === null ? null : { eventId: id, heldPrice: heldRatio, bookPressure }, ...(consensus ? { consensus } : {}) });
}
const opposite = (id = "late") => ({ eventId: id, direction: "DOWN" as const, confidence: .65, estimatedEdge: .015 });
function early(ratio = ".70", pressure = ".20", end = 10000) {
  let s = step(null, 0, ".75").nextState;
  s = step(s, 5000, ratio).nextState;
  return step(s, end, ratio, pressure);
}
const late = (ratio = ".60", ms = 180001, confidence = .65, estimatedEdge = .015) =>
  step(null, ms, ratio, ".20", { ...opposite(), confidence, estimatedEdge });

describe("locked Guardian V2 sensors", () => {
  it("uses exactly the specified locked values", () => {
    expect(GUARDIAN_V2_CONFIG).toEqual({ EARLY_WINDOW_MS: 180000, EARLY_HELD_RATIO_MAX: .70,
      EARLY_DROP_LOOKBACK_MS: 10000, EARLY_MIN_DROP_RATIO: .05, EARLY_BAD_REGION_PERSIST_MS: 5000,
      EARLY_OPPOSITE_BOOK_PRESSURE_MIN: .20, EARLY_RECOVERY_WATCH_MS: 60000, EARLY_TRUE_RECOVERY_RATIO: .95,
      EARLY_EMERGENCY_RATIO: .15, LATE_WINDOW_START_MS: 180000, LATE_WINDOW_END_MS: 600000,
      LATE_MIN_CONFIDENCE: .65, LATE_MIN_EDGE: .015, LATE_MIN_HELD_RATIO: .20, LATE_MAX_HELD_RATIO: .60,
      LATE_CONFIRM_MS: 30000, LATE_REQUIRED_RATIO_DETERIORATION: .026,
      LATE_OPPOSITE_BOOK_PRESSURE_MIN: .20, LATE_EMERGENCY_RATIO: .15 });
    expect(isLockedGuardianV2Config(GUARDIAN_V2_CONFIG)).toBe(true);
    expect(isLockedGuardianV2Config({ ...GUARDIAN_V2_CONFIG, LATE_CONFIRM_MS: 29999 })).toBe(false);
  });
  it.each([[".700001", ".20"], [".70", ".199999"]])("EARLY no arm at ratio %s pressure %s", (r, p) => {
    expect(early(r, p).nextState?.phase).toBe("OBSERVING");
  });
  it("ratio alone, insufficient drop, missing lookback or <5s persistence cannot arm", () => {
    expect(step(null, 10000, ".70").nextState?.phase).toBe("OBSERVING");
    let s = step(null, 0, ".749999").nextState; s = step(s, 5000, ".70").nextState;
    expect(step(s, 10000, ".70").nextState?.phase).toBe("OBSERVING");
    s = step(null, 0, ".75").nextState; s = step(s, 5001, ".70").nextState;
    expect(step(s, 10000, ".70").nextState?.phase).toBe("OBSERVING");
  });
  it("EARLY exact ratio/drop/persistence/book boundaries arm without exit", () => {
    const result = early(); expect(result.events).toEqual(["EARLY_DETERIORATION_ARMED"]);
    expect(result.nextState?.phase).toBe("EARLY_RECOVERY_WATCH"); expect(result.nextState?.confirmedAt).toBeNull();
  });
  it("inclusive EARLY 180s boundary; later snapshots cannot arm", () => {
    let s = step(null, 170000, ".75").nextState; s = step(s, 175000, ".70").nextState;
    expect(step(s, 180000, ".70").nextState?.phase).toBe("EARLY_RECOVERY_WATCH");
    expect(step(s, 180001, ".70").nextState?.phase).toBe("OBSERVING");
  });
  it("irregular cadence selects latest sample at/before t-10s, never a newer sample", () => {
    let s = step(null, 0, ".76").nextState; s = step(s, 1234, ".75").nextState;
    s = step(s, 7000, ".70").nextState;
    expect(step(s, 12555, ".70").nextState?.phase).toBe("EARLY_RECOVERY_WATCH");
    s = step(null, 3000, ".75").nextState; s = step(s, 7000, ".70").nextState;
    expect(step(s, 12555, ".70").nextState?.phase).toBe("OBSERVING");
  });
  it("bad-region excursion or observation gap resets persistence/history", () => {
    let s = step(null, 0, ".80").nextState; s = step(s, 5000, ".70").nextState;
    s = step(s, 7000, ".71").nextState; s = step(s, 8000, ".70").nextState;
    expect(step(s, 10000, ".70").nextState?.phase).toBe("OBSERVING");
    s = step(s, 11000, null).nextState;
    expect(s?.badRegionSince).toBeNull(); expect(s?.history).toHaveLength(0);
    expect(step(s, 15000, ".20").nextState?.phase).toBe("OBSERVING");
  });
  it.each(["UP", "DOWN"] as const)("normalizes opposite book pressure for held %s", held => {
    const pressure = normalizeOppositeBookPressure(held, held === "UP" ? "-.20" : ".20");
    expect(pressure).toBe("0.2");
    let s: GuardianV2State | null = null;
    for (const [ms, price] of [[0, ".75"], [5000, ".70"], [10000, ".70"]] as const) {
      s = evaluateGuardianV2({ state: s, episode: { ...episode, heldOutcome: held }, entryPrice: "1", now: new Date(START + ms),
        observation: { eventId: String(ms), heldPrice: price, bookPressure: pressure } }).nextState;
    }
    expect(s?.phase).toBe("EARLY_RECOVERY_WATCH");
  });
  it.each([180000, 180001, 600000, 600001])("LATE window at %sms", ms => {
    expect(late(".60", ms).nextState?.phase === "LATE_CONFIRMATION").toBe(ms > 180000 && ms <= 600000);
  });
  it.each([[".199999", .65, .015], [".600001", .65, .015], [".20", .649999, .015], [".60", .65, .014999]])(
    "LATE gates at %s/%s/%s", (r, c, e) => { expect(late(r, 200000, c, e).events).toEqual([]); });
  it.each([".20", ".60"])("LATE salvage boundary %s arms without immediate exit", r => {
    expect(late(r).events).toEqual(["LATE_SEVERITY_ARMED"]); expect(late(r).nextState?.confirmedAt).toBeNull();
  });
  it.each(["UP", "FLAT"] as const)("LATE held or FLAT %s cannot arm", direction => {
    expect(step(null, 200000, ".40", ".20", { ...opposite(), direction }).events).toEqual([]);
  });
});

describe("strict recovery and confirmation deadlines", () => {
  it.each([[59999, ".95", "OBSERVING"], [59999, ".949999", "EARLY_RECOVERY_WATCH"],
    [60000, ".95", "PROTECTION_PENDING"], [60001, "1", "PROTECTION_PENDING"], [59999, ".75", "EARLY_RECOVERY_WATCH"]])(
    "EARLY elapsed %s ratio %s -> %s", (elapsed, r, phase) => {
      expect(step(early().nextState, 10000 + elapsed, r).nextState?.phase).toBe(phase);
    });
  it("no recovery or no valid snapshot at deadline still protects", () => {
    expect(step(early().nextState, 70000, null).events).toEqual(["EARLY_TIMEOUT_PROTECT"]);
    expect(step(early().nextState, 70000, ".80").events).toEqual(["EARLY_TIMEOUT_PROTECT"]);
  });
  it(".15 emergency only acts inside an armed EARLY episode", () => {
    expect(step(early().nextState, 10001, ".15").events).toEqual(["EARLY_EMERGENCY_PROTECT"]);
    expect(step(null, 10001, ".15").nextState?.confirmedAt).toBeNull();
    expect(step(early().nextState, 10001, ".150001").nextState?.confirmedAt).toBeNull();
  });
  it("true recovery releases with no extra sustain; another arm requires fresh history", () => {
    const recovered = step(early().nextState, 10001, ".95");
    expect(recovered.events).toEqual(["EARLY_TRUE_RECOVERY"]); expect(recovered.nextState?.history).toHaveLength(0);
    expect(step(recovered.nextState, 11000, ".60").nextState?.phase).toBe("OBSERVING");
  });
  it.each([[29999, ".574", ".20", "LATE_CONFIRMATION"], [30000, ".574001", ".20", "OBSERVING"],
    [30000, ".574", ".199999", "OBSERVING"], [30000, ".574", ".20", "PROTECTION_PENDING"],
    [30000, ".57", "-.20", "OBSERVING"], [30000, ".58", ".20", "OBSERVING"]])(
    "LATE elapsed %s ratio %s book %s -> %s", (elapsed, r, p, phase) => {
      expect(step(late().nextState, 180001 + elapsed, r, p).nextState?.phase).toBe(phase);
    });
  it("LATE .15 emergency preempts confirmation", () => {
    expect(step(late().nextState, 180002, ".15").events).toEqual(["LATE_EMERGENCY_PROTECT"]);
  });
  it("untrusted book at LATE deadline releases, can rearm on new consensus in window", () => {
    const release = step(late().nextState, 210001, null);
    expect(release.events).toEqual(["LATE_CONFIRMATION_RELEASED"]);
    expect(step(release.nextState, 210002, ".40", ".20", opposite("new")).events).toEqual(["LATE_SEVERITY_ARMED"]);
  });
  it("late confirmation can finish after 600s when armed at 600s", () => {
    expect(step(late(".60", 600000).nextState, 630000, ".574").events).toEqual(["LATE_CONFIRMATION_ACCEPTED"]);
  });
  it("duplicate, stale, pre-entry and wrong-episode inputs cannot regress or count twice", () => {
    const s = early().nextState!;
    expect(step(s, 10000, ".70").stateChanged).toBe(false);
    expect(step(s, 9999, ".15").stateChanged).toBe(false);
    expect(step(null, -1, ".70").stateChanged).toBe(false);
    expect(evaluateGuardianV2({ state: s, episode: { ...episode, positionId: "new" }, entryPrice: "1", now: new Date(START + 11000) }).stateChanged).toBe(false);
    const pending = step(s, 70000, ".70").nextState!;
    expect(step(pending, 70001, "1").stateChanged).toBe(false);
    expect(guardianV2StateSchema.safeParse({ ...s, deadlineAt: s.armedAt }).success).toBe(false);
  });
});
