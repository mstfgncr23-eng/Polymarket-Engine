import { z } from "zod";
import { d } from "./calculations.js";
import type { ReversalEpisode } from "./reversal-protection.js";

/** Locked candidate. Values and units are implementation authority, not tuning knobs. */
export const GUARDIAN_V2_CONFIG = Object.freeze({
  EARLY_WINDOW_MS: 180000, EARLY_HELD_RATIO_MAX: 0.70,
  EARLY_DROP_LOOKBACK_MS: 10000, EARLY_MIN_DROP_RATIO: 0.05,
  EARLY_BAD_REGION_PERSIST_MS: 5000, EARLY_OPPOSITE_BOOK_PRESSURE_MIN: 0.20,
  EARLY_RECOVERY_WATCH_MS: 65000, EARLY_TRUE_RECOVERY_RATIO: 0.95, EARLY_EMERGENCY_RATIO: 0.15,
  LATE_WINDOW_START_MS: 180000, LATE_WINDOW_END_MS: 600000,
  LATE_MIN_CONFIDENCE: 0.65, LATE_MIN_EDGE: 0.015, LATE_MIN_HELD_RATIO: 0.20, LATE_MAX_HELD_RATIO: 0.60,
  LATE_CONFIRM_MS: 30000, LATE_REQUIRED_RATIO_DETERIORATION: 0.026,
  LATE_OPPOSITE_BOOK_PRESSURE_MIN: 0.20, LATE_EMERGENCY_RATIO: 0.15,
});
const C = GUARDIAN_V2_CONFIG;
const timestamp = z.string().datetime({ offset: true });
const ratio = z.string().refine(v => { try { return d(v).isFinite() && d(v).gte(0); } catch { return false; } });
export const guardianV2StateSchema = z.object({
  version: z.literal(2), formatGeneration: z.literal(3).optional(), marketId: z.string().min(1), positionId: z.string().min(1),
  unhedgedStartedAt: timestamp, heldOutcome: z.enum(["UP", "DOWN"]),
  entryPrice: ratio.refine(v => d(v).gt(0) && d(v).lte(1)),
  phase: z.enum(["OBSERVING", "EARLY_RECOVERY_WATCH", "LATE_CONFIRMATION", "PROTECTION_PENDING"]),
  sensor: z.enum(["EARLY", "LATE"]).nullable(),
  armedAt: timestamp.nullable(), deadlineAt: timestamp.nullable(), armRatio: ratio.nullable(),
  badRegionSince: timestamp.nullable(),
  history: z.array(z.object({ at: timestamp, heldRatio: ratio })),
  // Observation time watermark also rejects old/duplicate source events after checkpoint compaction.
  lastObservationAt: timestamp.nullable(), lastObservationEventId: z.string().min(1).nullable(),
  lastConsensusAt: timestamp.nullable(), lastConsensusEventId: z.string().min(1).nullable(),
  confirmedAt: timestamp.nullable(),
  protectionReason: z.enum(["EARLY_EMERGENCY", "EARLY_TIMEOUT", "LATE_EMERGENCY", "LATE_CONFIRMED"]).nullable(),
  revision: z.number().int().positive(), updatedAt: timestamp,
}).superRefine((s, ctx) => {
  const start = Date.parse(s.unhedgedStartedAt), updated = Date.parse(s.updatedAt);
  const observing = s.phase === "OBSERVING", pending = s.phase === "PROTECTION_PENDING";
  let invalid = updated < start || (s.confirmedAt !== null) !== pending
    || (s.protectionReason !== null) !== pending
    || observing !== (s.sensor === null && s.armedAt === null && s.deadlineAt === null && s.armRatio === null)
    || (!observing && (s.sensor === null || s.armedAt === null || s.deadlineAt === null || s.armRatio === null))
    || (s.phase === "EARLY_RECOVERY_WATCH" && s.sensor !== "EARLY")
    || (s.phase === "LATE_CONFIRMATION" && s.sensor !== "LATE")
    || ((s.lastObservationAt === null) !== (s.lastObservationEventId === null))
    || ((s.lastConsensusAt === null) !== (s.lastConsensusEventId === null));
  if (s.armedAt && s.deadlineAt) {
    const arm = Date.parse(s.armedAt), age = arm - start;
    const expectedDeadline = s.sensor === "EARLY"
      ? (s.badRegionSince ? Date.parse(s.badRegionSince) + C.EARLY_RECOVERY_WATCH_MS : s.formatGeneration == null ? arm + 60000 : Number.NaN)
      : arm + C.LATE_CONFIRM_MS;
    invalid ||= arm > updated || Date.parse(s.deadlineAt) !== expectedDeadline
      || (s.sensor === "EARLY" ? age < 0 || age > C.EARLY_WINDOW_MS : age <= C.LATE_WINDOW_START_MS || age > C.LATE_WINDOW_END_MS);
  }
  if (s.protectionReason) {
    invalid ||= !s.protectionReason.startsWith(`${s.sensor}_`)
      || Date.parse(s.confirmedAt!) < Date.parse(s.armedAt!);
    if (s.protectionReason === "EARLY_TIMEOUT" || s.protectionReason === "LATE_CONFIRMED") {
      invalid ||= Date.parse(s.confirmedAt!) < Date.parse(s.deadlineAt!);
    }
    if (s.protectionReason === "EARLY_EMERGENCY") invalid ||= Date.parse(s.confirmedAt!) >= Date.parse(s.deadlineAt!);
  }
  for (const at of [s.badRegionSince, s.lastObservationAt, s.lastConsensusAt, s.confirmedAt]) {
    if (at) invalid ||= Date.parse(at) < start || Date.parse(at) > updated;
  }
  let prior = start - 1;
  for (const sample of s.history) {
    const at = Date.parse(sample.at); invalid ||= at <= prior || at > updated; prior = at;
  }
  if (invalid) ctx.addIssue({ code: "custom", message: "invalid locked Guardian V2 state" });
});
export type GuardianV2State = z.infer<typeof guardianV2StateSchema>;
export type GuardianV2Event = "EARLY_DETERIORATION_ARMED" | "EARLY_TRUE_RECOVERY" | "EARLY_EMERGENCY_PROTECT"
  | "EARLY_TIMEOUT_PROTECT" | "LATE_SEVERITY_ARMED" | "LATE_CONFIRMATION_RELEASED"
  | "LATE_CONFIRMATION_ACCEPTED" | "LATE_EMERGENCY_PROTECT";

export function isGuardianV2State(state: unknown): state is GuardianV2State {
  return !!state && typeof state === "object" && (state as { version?: unknown }).version === 2;
}
export function isLockedGuardianV2Config(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const config = value as Record<string, unknown>;
  return Object.keys(config).length === Object.keys(C).length && Object.entries(C).every(([key, v]) =>
    config[key] === v || (key === "EARLY_RECOVERY_WATCH_MS" && config[key] === 60000));
}
/** Bounded readers: HEAD arm+60s, frozen unmarked bad-region+65s, explicit new format. */
export type GuardianGeneration = "HEAD_V2" | "FROZEN_V2" | "BAD_REGION_V3";
export function lockedGuardianGeneration(risk: Record<string, unknown>): GuardianGeneration | null {
  if (risk.guardianVersion !== 2 || !isLockedGuardianV2Config(risk.guardianV2Config)) return null;
  const watch = (risk.guardianV2Config as Record<string, unknown>).EARLY_RECOVERY_WATCH_MS;
  if (risk.guardianFormatGeneration === 3) return watch === 65000 ? "BAD_REGION_V3" : null;
  if (risk.guardianFormatGeneration != null) return null;
  return watch === 60000 ? "HEAD_V2" : "FROZEN_V2";
}
export function guardianStateMatchesGeneration(state: GuardianV2State, generation: GuardianGeneration | null): boolean {
  if (!generation || (state.formatGeneration === 3) !== (generation === "BAD_REGION_V3")) return false;
  if (state.sensor !== "EARLY") return true;
  return generation === "HEAD_V2"
    ? state.badRegionSince === null && Date.parse(state.deadlineAt!) === Date.parse(state.armedAt!) + 60000
    : state.badRegionSince !== null && Date.parse(state.deadlineAt!) === Date.parse(state.badRegionSince) + 65000;
}
export function normalizeOppositeBookPressure(held: "UP" | "DOWN", upPressure: string | number): string {
  return d(upPressure).times(held === "UP" ? -1 : 1).toFixed();
}

/** Only observed values are used. No interpolation or extrapolated recovery. Latest sample <= t-10s is the anchor. */
export function evaluateGuardianV2(input: {
  generation?: GuardianGeneration; state: GuardianV2State | null; episode: ReversalEpisode | null; entryPrice: string; now: Date;
  observation?: { eventId: string; heldPrice: string; bookPressure: string } | null;
  observationGap?: boolean;
  consensus?: { eventId: string; direction: "UP" | "DOWN" | "FLAT"; confidence: number; estimatedEdge: number };
}): { nextState: GuardianV2State | null; stateChanged: boolean; events: GuardianV2Event[] } {
  const episode = input.episode, nowMs = input.now.getTime();
  const previous = input.state;
  const generation = input.generation ?? (previous && previous.formatGeneration == null
    ? previous.sensor === "EARLY" && previous.badRegionSince === null ? "HEAD_V2" : "FROZEN_V2" : "BAD_REGION_V3");
  const noop = { nextState: previous, stateChanged: false, events: [] as GuardianV2Event[] };
  if (!episode || !Number.isFinite(nowMs) || nowMs < Date.parse(episode.unhedgedStartedAt)) return noop;
  if (previous && (previous.positionId !== episode.positionId || previous.unhedgedStartedAt !== episode.unhedgedStartedAt
    || previous.marketId !== episode.marketId || previous.heldOutcome !== episode.heldOutcome)) return noop;
  if (previous?.confirmedAt || (previous && nowMs < Date.parse(previous.updatedAt))) return noop;
  const entry = d(previous?.entryPrice ?? input.entryPrice);
  if (!entry.isFinite() || entry.lte(0) || entry.gt(1)) return noop;
  // No nested Guardian field is modified in place: history is replaced on every change.
  // Avoid deep-copying hundreds of EARLY samples on each market tick.
  const s: GuardianV2State = previous ? { ...previous } : {
    ...episode, version: 2, ...(generation === "BAD_REGION_V3" ? { formatGeneration: 3 as const } : {}), entryPrice: entry.toFixed(), phase: "OBSERVING", sensor: null,
    armedAt: null, deadlineAt: null, armRatio: null, badRegionSince: null, history: [],
    lastObservationAt: null, lastObservationEventId: null, lastConsensusAt: null, lastConsensusEventId: null,
    confirmedAt: null, protectionReason: null, revision: 0, updatedAt: input.now.toISOString(),
  };
  const at = input.now.toISOString(), age = nowMs - Date.parse(episode.unhedgedStartedAt);
  const events: GuardianV2Event[] = [];
  const observation = input.observation;
  const fresh = !!observation && !!observation.eventId
    && observation.eventId !== s.lastObservationEventId
    && (!s.lastObservationAt || nowMs > Date.parse(s.lastObservationAt));
  const heldRatio = observation ? d(observation.heldPrice).div(entry) : null;
  const pressure = observation ? d(observation.bookPressure) : null;
  const valid = heldRatio?.isFinite() && heldRatio.gte(0) && d(observation!.heldPrice).lte(1)
    && pressure?.isFinite() && pressure.abs().lte(1);
  const release = () => {
    s.phase = "OBSERVING"; s.sensor = null; s.armedAt = null; s.deadlineAt = null; s.armRatio = null;
    s.badRegionSince = null; s.history = [];
  };
  const protect = (reason: GuardianV2State["protectionReason"], event: GuardianV2Event) => {
    s.phase = "PROTECTION_PENDING"; s.confirmedAt = at; s.protectionReason = reason; events.push(event);
  };
  if (s.phase === "OBSERVING" && input.observationGap) { s.history = []; s.badRegionSince = null; }
  const arm = (sensor: "EARLY" | "LATE") => {
    s.phase = sensor === "EARLY" ? "EARLY_RECOVERY_WATCH" : "LATE_CONFIRMATION";
    s.sensor = sensor; s.armedAt = at; s.armRatio = heldRatio!.toFixed();
    const deadlineBaseMs = sensor === "EARLY" && generation !== "HEAD_V2" ? Date.parse(s.badRegionSince!) : nowMs;
    s.deadlineAt = new Date(deadlineBaseMs + (sensor === "EARLY" ? generation === "HEAD_V2" ? 60000 : C.EARLY_RECOVERY_WATCH_MS : C.LATE_CONFIRM_MS)).toISOString();
    s.history = [];
    if (sensor === "LATE" || generation === "HEAD_V2") s.badRegionSince = null;
    events.push(sensor === "EARLY" ? "EARLY_DETERIORATION_ARMED" : "LATE_SEVERITY_ARMED");
  };
  // STRICT deadline is first: a .95 observation at/after EARLY expiry cannot rescue the position.
  if (s.phase === "EARLY_RECOVERY_WATCH") {
    if (nowMs >= Date.parse(s.deadlineAt!)) protect("EARLY_TIMEOUT", "EARLY_TIMEOUT_PROTECT");
    else if (fresh && valid && heldRatio!.lte(C.EARLY_EMERGENCY_RATIO)) protect("EARLY_EMERGENCY", "EARLY_EMERGENCY_PROTECT");
    else if (fresh && valid && heldRatio!.gte(C.EARLY_TRUE_RECOVERY_RATIO)) { release(); events.push("EARLY_TRUE_RECOVERY"); }
  } else if (s.phase === "LATE_CONFIRMATION") {
    if (fresh && valid && heldRatio!.lte(C.LATE_EMERGENCY_RATIO)) protect("LATE_EMERGENCY", "LATE_EMERGENCY_PROTECT");
    else if (nowMs >= Date.parse(s.deadlineAt!)) {
      if (valid && heldRatio!.minus(s.armRatio!).lte(d(C.LATE_REQUIRED_RATIO_DETERIORATION).neg())
        && pressure!.gte(C.LATE_OPPOSITE_BOOK_PRESSURE_MIN)) protect("LATE_CONFIRMED", "LATE_CONFIRMATION_ACCEPTED");
      else { release(); events.push("LATE_CONFIRMATION_RELEASED"); }
    }
  } else if (age <= C.EARLY_WINDOW_MS && fresh && valid) {
    const cutoff = nowMs - C.EARLY_DROP_LOOKBACK_MS;
    // History is already timestamp-ordered by the schema. One backward scan
    // finds the most recent anchor <= cutoff, preserving exactly one anchor
    // and every newer sample without filtering the entire array twice.
    let lastAnchorIndex = s.history.length - 1;
    while (lastAnchorIndex >= 0 && Date.parse(s.history[lastAnchorIndex]!.at) > cutoff) lastAnchorIndex--;
    const anchor = lastAnchorIndex >= 0 ? s.history[lastAnchorIndex]! : null;
    s.history = [...(anchor ? [anchor] : []), ...s.history.slice(lastAnchorIndex + 1), { at, heldRatio: heldRatio!.toFixed() }];
    s.badRegionSince = heldRatio!.lte(C.EARLY_HELD_RATIO_MAX) ? s.badRegionSince ?? at : null;
    if (heldRatio!.lte(C.EARLY_HELD_RATIO_MAX) && anchor && d(anchor.heldRatio).minus(heldRatio!).gte(C.EARLY_MIN_DROP_RATIO)
      && s.badRegionSince && nowMs - Date.parse(s.badRegionSince) >= C.EARLY_BAD_REGION_PERSIST_MS
      && pressure!.gte(C.EARLY_OPPOSITE_BOOK_PRESSURE_MIN)) {
      arm("EARLY");
      if (nowMs >= Date.parse(s.deadlineAt!)) protect("EARLY_TIMEOUT", "EARLY_TIMEOUT_PROTECT");
      else if (heldRatio!.lte(C.EARLY_EMERGENCY_RATIO)) protect("EARLY_EMERGENCY", "EARLY_EMERGENCY_PROTECT");
    }
  } else if (s.phase === "OBSERVING" && age > C.LATE_WINDOW_START_MS && age <= C.LATE_WINDOW_END_MS
    && valid && input.consensus && input.consensus.eventId !== s.lastConsensusEventId
    && (!s.lastConsensusAt || nowMs > Date.parse(s.lastConsensusAt))
    && input.consensus.direction !== "FLAT" && input.consensus.direction !== episode.heldOutcome
    && Number.isFinite(input.consensus.confidence) && d(input.consensus.confidence).gte(C.LATE_MIN_CONFIDENCE)
    && Number.isFinite(input.consensus.estimatedEdge) && d(input.consensus.estimatedEdge).gte(C.LATE_MIN_EDGE)
    && heldRatio!.gte(C.LATE_MIN_HELD_RATIO) && heldRatio!.lte(C.LATE_MAX_HELD_RATIO)) arm("LATE");

  if (observation === null && s.phase === "OBSERVING") { s.badRegionSince = null; s.history = []; }
  if (fresh && valid) { s.lastObservationAt = at; s.lastObservationEventId = observation!.eventId; }
  if (input.consensus && (!s.lastConsensusAt || nowMs > Date.parse(s.lastConsensusAt))) {
    s.lastConsensusAt = at; s.lastConsensusEventId = input.consensus.eventId;
  }
  // Idle late samples need no journal mutation; watches and EARLY history are durable.
  const useful = events.length > 0 || (age <= C.EARLY_WINDOW_MS && (fresh || observation === null))
    || s.phase === "EARLY_RECOVERY_WATCH" || s.phase === "LATE_CONFIRMATION";
  if (!useful || JSON.stringify({ ...s, updatedAt: previous?.updatedAt, revision: previous?.revision }) === JSON.stringify(previous)) return noop;
  if (!previous && !fresh && !events.length) return noop;
  s.revision = (previous?.revision ?? 0) + 1; s.updatedAt = at;
  return { nextState: guardianV2StateSchema.parse(s), stateChanged: true, events };
}
