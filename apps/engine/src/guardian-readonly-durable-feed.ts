import type { DurableTransition } from "@pm/db";
import {
  advanceGuardianOfflineSource,
  type GuardianOfflineState,
} from "./guardian-offline-durable-source.js";

/**
 * G2H source adapter: one bounded, read-only page from the canonical global
 * durable log. NO process, timer, queue, shadow, DB write, or order authority.
 *
 * A caller MUST HALT Guardian decisions on any exception, then reconcile from
 * a fresh independently verified checkpoint. Never retry the same state and
 * pretend it is current after an uncertain source read.
 */
export interface GuardianDurableReadSource {
  latestDurableSequence(): Promise<number>;
  readDurableTransitionsAfter(cursor: number, limit: number): Promise<DurableTransition[]>;
}
export type GuardianFeedHaltReason =
  | "INVALID_LIMITS" | "SOURCE_UNAVAILABLE" | "SOURCE_CURSOR_REGRESSION"
  | "BACKLOG_EXCEEDED" | "DURABLE_SOURCE_GAP" | "DURABLE_PAGE_INVALID"
  | "DURABLE_REPLAY_FAILED";
export class GuardianFeedHalted extends Error {
  constructor(readonly reason: GuardianFeedHaltReason, readonly detail: string) {
    super(`GUARDIAN_FEED_HALTED:${reason}:${detail}`);
  }
}
function halt(reason: GuardianFeedHaltReason, detail: string): never {
  throw new GuardianFeedHalted(reason, detail);
}

export async function pullGuardianDurablePage(input: {
  source: GuardianDurableReadSource;
  state: GuardianOfflineState;
  /** Small bounded read. No in-memory queue. */
  pageSize: number;
  /** Fail closed if the Guardian source is further behind than this. */
  maxBacklogRows: number;
}): Promise<{ state: GuardianOfflineState; readCount: number; highWater: number }> {
  const { source, state, pageSize, maxBacklogRows } = input;
  if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 512
    || !Number.isSafeInteger(maxBacklogRows) || maxBacklogRows < pageSize
    || maxBacklogRows > 100_000) halt("INVALID_LIMITS", "page or backlog bounds invalid");

  let highWater: number;
  try { highWater = await source.latestDurableSequence(); }
  catch { return halt("SOURCE_UNAVAILABLE", "latestDurableSequence"); }
  if (!Number.isSafeInteger(highWater) || highWater < state.projection.cursor) {
    return halt("SOURCE_CURSOR_REGRESSION", `cursor=${state.projection.cursor},highWater=${highWater}`);
  }
  const behind = highWater - state.projection.cursor;
  if (behind > maxBacklogRows) return halt("BACKLOG_EXCEEDED", `behind=${behind},max=${maxBacklogRows}`);
  if (behind === 0) return { state, readCount: 0, highWater };

  const count = Math.min(pageSize, behind);
  let rows: DurableTransition[];
  try { rows = await source.readDurableTransitionsAfter(state.projection.cursor, count); }
  catch { return halt("SOURCE_UNAVAILABLE", "readDurableTransitionsAfter"); }
  if (!Array.isArray(rows) || rows.length < 1 || rows.length > count) {
    return halt("DURABLE_PAGE_INVALID", `expected=1..${count},returned=${rows?.length}`);
  }
  let next = state;
  for (const row of rows) {
    if (!row || !Number.isSafeInteger(row.sequence) || row.sequence !== next.projection.cursor + 1
      || row.sequence > highWater) {
      return halt("DURABLE_SOURCE_GAP", `next=${next.projection.cursor+1},actual=${row?.sequence}`);
    }
    try { next = advanceGuardianOfflineSource(next, row); }
    catch (error) {
      // Compacted payload, invalid hash, economic invariant or unknown durable
      // event. None can be reinterpreted as an ignorable "heartbeat".
      return halt("DURABLE_REPLAY_FAILED", error instanceof Error ? error.message : "unknown");
    }
  }
  return { state: next, readCount: rows.length, highWater };
}
