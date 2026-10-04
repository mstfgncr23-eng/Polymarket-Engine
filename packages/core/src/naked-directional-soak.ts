import { z } from "zod";
import { calculateFee, d, walkBook } from "./calculations.js";
import { feeScheduleSchema, type MarketSnapshot, type PaperFill, type Position } from "./domain.js";

const numberString = z.string().refine((v) => { try { return d(v).isFinite(); } catch { return false; } });
const at = z.string().datetime({ offset: true });
const extremum = z.object({ value: numberString, at, ageMs: z.number().nonnegative(),
  directionalPnl: numberString.nullable(), grossPairEdge: numberString.nullable() }).nullable();
export const SOAK_PATH_LIMIT = 128;
export const SOAK_ANOMALY_LIMIT = 64;
export const SOAK_PERSIST_INTERVAL_MS = 5_000;
export const SOAK_PERSIST_PATH_LIMIT = 8;
const freshness = z.enum(["FRESH", "MISSING", "UNSYNCHRONIZED", "STALE", "FUTURE"]);
const pointSchema = z.object({
  sequence: z.number().int().positive(),
  at, ageMs: z.number().nonnegative(), quantity: numberString, entryPrice: numberString, entryFee: numberString,
  directionalMark: numberString.nullable(), directionalPnl: numberString.nullable(),
  grossPairEdge: numberString.nullable(), netPairEdge: numberString.nullable(), lockedPairPnl: numberString.nullable(),
  oppositeDepth: numberString, oppositeExecutableQuantity: numberString, oppositeVwap: numberString.nullable(),
  oppositeFee: numberString.nullable(), oppositeFeeSchedule: feeScheduleSchema.nullable(),
  heldFreshness: freshness, oppositeFreshness: freshness,
  heldSourceTimestamp: at.nullable(), oppositeSourceTimestamp: at.nullable(),
  heldReceivedAt: at.nullable(), oppositeReceivedAt: at.nullable(),
  feedState: z.string(), meaningfulPairChange: z.boolean(), sincePreviousPairChangeMs: z.number().nonnegative().nullable(),
});
export const nakedDirectionalDiagnosticSchema = z.object({
  positionId: z.string().min(1), marketId: z.string().min(1), entryTime: at, entryDirection: z.enum(["UP", "DOWN"]),
  entryPrice: numberString, entryQuantity: numberString, entryFee: numberString,
  revision: z.number().int().nonnegative(), observationCount: z.number().int().nonnegative(),
  minDirectionalPnl: extremum, maxDirectionalPnl: extremum, maxExecutablePairEdge: extremum,
  maxNetExecutablePairEdge: extremum, current: pointSchema.nullable(), path: z.array(pointSchema).max(SOAK_PATH_LIMIT),
  pathCompactions: z.number().int().nonnegative(), lastMeaningfulPairEdge: numberString.nullable(),
  lastMeaningfulPairChangeAt: at.nullable(),
  anomalyCounts: z.record(z.string(), z.number().int().nonnegative()),
  anomalies: z.array(z.object({ at, ageMs: z.number().nonnegative(), kind: z.string(), detail: z.record(z.string(), z.unknown()) })).max(SOAK_ANOMALY_LIMIT),
});
export type NakedDirectionalDiagnostic = z.infer<typeof nakedDirectionalDiagnosticSchema>;

/** Created only by an actual authoritative first-leg fill; no simulated order. */
export function createNakedDirectionalDiagnostic(position: Position, fill: PaperFill): NakedDirectionalDiagnostic {
  return { positionId: position.id, marketId: position.marketId, entryTime: fill.timestamp, entryDirection: fill.outcome,
    entryPrice: fill.fillPrice, entryQuantity: fill.quantity, entryFee: fill.feeUsd,
    revision: 0, observationCount: 0, minDirectionalPnl: null, maxDirectionalPnl: null, maxExecutablePairEdge: null,
    maxNetExecutablePairEdge: null, current: null, path: [], pathCompactions: 0,
    lastMeaningfulPairEdge: null, lastMeaningfulPairChangeAt: null, anomalyCounts: {}, anomalies: [] };
}

function bookFreshness(book: MarketSnapshot | null, synchronized: boolean, nowMs: number, timeoutMs: number) {
  if (!book) return "MISSING" as const;
  if (!synchronized) return "UNSYNCHRONIZED" as const;
  const received = Date.parse(book.receivedAt), source = Date.parse(book.sourceTimestamp);
  if (received > nowMs || source > nowMs) return "FUTURE" as const;
  if (nowMs - received > timeoutMs || nowMs - source > timeoutMs) return "STALE" as const;
  return "FRESH" as const;
}

/** Synchronous, in-memory observation on every accepted book; extrema are never sampled. */
export function observeNakedDirectionalPosition(state: NakedDirectionalDiagnostic, position: Position, input: {
  held: MarketSnapshot | null; opposite: MarketSnapshot | null; heldSynchronized: boolean; oppositeSynchronized: boolean;
  now: Date; staleTimeoutMs: number; feedState: string;
}): void {
  const nowMs = input.now.getTime(), timestamp = input.now.toISOString();
  const ageMs = Math.max(0, nowMs - Date.parse(state.entryTime));
  const quantity = position.unpairedQuantity;
  const cost = state.entryDirection === "UP" ? position.averageUpCost : position.averageDownCost;
  state.entryPrice = cost; state.entryQuantity = quantity; state.entryFee = position.totalFees;
  const heldFreshness = bookFreshness(input.held, input.heldSynchronized, nowMs, input.staleTimeoutMs);
  const oppositeFreshness = bookFreshness(input.opposite, input.oppositeSynchronized, nowMs, input.staleTimeoutMs);
  const mark = heldFreshness === "FRESH" ? input.held?.bids[0]?.price ?? null : null;
  const pnl = mark === null ? null : d(quantity).mul(d(mark).minus(cost)).minus(position.totalFees).toFixed();
  const walk = input.opposite && d(quantity).gt(0) ? walkBook(input.opposite.asks, quantity, "1", "BUY") : null;
  const depth = input.opposite?.asks.reduce((sum, level) => sum.plus(level.quantity), d(0)).toFixed() ?? "0";
  const executable = oppositeFreshness === "FRESH" && input.feedState === "CONNECTED" && walk && d(walk.filledQuantity).gt(0);
  const full = executable && walk!.sufficientDepth && input.opposite?.feeSchedule;
  const oppositeFee = executable && input.opposite?.feeSchedule
    ? calculateFee(walk!.filledQuantity, walk!.vwap, "TAKER", input.opposite.feeSchedule) : null;
  const edge = full ? d(1).minus(cost).minus(walk!.vwap).toFixed() : null;
  const lockedPnl = edge === null ? null : d(quantity).mul(edge).minus(position.totalFees).minus(oppositeFee!).toFixed();
  const netEdge = lockedPnl === null ? null : d(lockedPnl).div(quantity).toFixed();
  const meaningful = edge !== null && (state.lastMeaningfulPairEdge === null || d(edge).minus(state.lastMeaningfulPairEdge).abs().gte("0.001"));
  const sincePreviousPairChangeMs = meaningful && state.lastMeaningfulPairChangeAt
    ? Math.max(0, nowMs - Date.parse(state.lastMeaningfulPairChangeAt)) : null;
  if (meaningful) { state.lastMeaningfulPairEdge = edge; state.lastMeaningfulPairChangeAt = timestamp; }
  const point: z.infer<typeof pointSchema> = { sequence: state.observationCount + 1, at: timestamp, ageMs, quantity, entryPrice: cost, entryFee: position.totalFees,
    directionalMark: mark, directionalPnl: pnl, grossPairEdge: edge, netPairEdge: netEdge, lockedPairPnl: lockedPnl,
    oppositeDepth: depth, oppositeExecutableQuantity: executable ? walk!.filledQuantity : "0", oppositeVwap: executable ? walk!.vwap : null,
    oppositeFee, oppositeFeeSchedule: input.opposite?.feeSchedule ?? null,
    heldFreshness, oppositeFreshness, heldSourceTimestamp: input.held?.sourceTimestamp ?? null,
    oppositeSourceTimestamp: input.opposite?.sourceTimestamp ?? null, heldReceivedAt: input.held?.receivedAt ?? null,
    oppositeReceivedAt: input.opposite?.receivedAt ?? null, feedState: input.feedState,
    meaningfulPairChange: meaningful, sincePreviousPairChangeMs };
  const update = (key: "minDirectionalPnl" | "maxDirectionalPnl" | "maxExecutablePairEdge" | "maxNetExecutablePairEdge", value: string | null, min = false) => {
    if (value === null) return;
    const previous = state[key];
    if (!previous || (min ? d(value).lt(previous.value) : d(value).gt(previous.value))) {
      state[key] = { value, at: timestamp, ageMs, directionalPnl: pnl, grossPairEdge: edge };
    }
  };
  update("minDirectionalPnl", pnl, true); update("maxDirectionalPnl", pnl);
  update("maxExecutablePairEdge", edge); update("maxNetExecutablePairEdge", netEdge);
  const previous = state.current;
  // Time samples plus arbitrary meaningful changes, availability/freshness changes,
  // and mark movement. Thinning retains early and late lifetime coverage.
  if (!previous || meaningful || nowMs - Date.parse(state.path.at(-1)!.at) >= 1_000
    || (edge === null) !== (previous.grossPairEdge === null)
    || heldFreshness !== previous.heldFreshness || oppositeFreshness !== previous.oppositeFreshness
    || (pnl !== null && state.path.at(-1)?.directionalPnl != null
      && d(pnl).minus(state.path.at(-1)!.directionalPnl!).abs().gte("0.05"))) {
    if (state.path.length === SOAK_PATH_LIMIT) {
      state.path = state.path.filter((_, index) => index === 0 || index % 2 === 1);
      state.pathCompactions += 1;
    }
    state.path.push(point);
  }
  state.current = point; state.observationCount += 1; state.revision += 1;
}

/** Durable batches store new points only, with a hard per-position write cap. */
export function compactNakedDirectionalObservation(state: NakedDirectionalDiagnostic, afterObservation: number): NakedDirectionalDiagnostic {
  const snapshot = structuredClone(state);
  const points = snapshot.path.filter((point) => point.sequence > afterObservation);
  snapshot.path = points.length <= SOAK_PERSIST_PATH_LIMIT ? points : Array.from({ length: SOAK_PERSIST_PATH_LIMIT }, (_, i) =>
    points[Math.round(i * (points.length - 1) / (SOAK_PERSIST_PATH_LIMIT - 1))]!);
  return snapshot;
}

export function mergeNakedDirectionalObservation(previous: NakedDirectionalDiagnostic | undefined, incoming: NakedDirectionalDiagnostic): NakedDirectionalDiagnostic {
  if (!previous) return incoming;
  const merged = structuredClone(incoming);
  merged.path = [...previous.path, ...incoming.path.filter((point) => point.sequence > previous.observationCount)];
  while (merged.path.length > SOAK_PATH_LIMIT) {
    merged.path = merged.path.filter((_, i) => i === 0 || i % 2 === 1);
    merged.pathCompactions += 1;
  }
  return merged;
}

export function noteNakedDirectionalAnomaly(state: NakedDirectionalDiagnostic, now: Date, kind: string, detail: Record<string, unknown>): void {
  state.anomalyCounts[kind] = (state.anomalyCounts[kind] ?? 0) + 1;
  if (state.anomalies.length === SOAK_ANOMALY_LIMIT) state.anomalies.splice(1, 1); // first + most recent
  state.anomalies.push({ at: now.toISOString(), ageMs: Math.max(0, now.getTime() - Date.parse(state.entryTime)), kind, detail });
  state.revision += 1;
}

export function nakedDirectionalSummary(state: NakedDirectionalDiagnostic, winner: "UP" | "DOWN", settledAt: Date, grossResult: string) {
  return { ...structuredClone(state), winner, settledAt: settledAt.toISOString(),
    grossResult, actualPaperResult: d(grossResult).minus(state.entryFee).toFixed(),
    lifetimeMs: Math.max(0, settledAt.getTime() - Date.parse(state.entryTime)) };
}
