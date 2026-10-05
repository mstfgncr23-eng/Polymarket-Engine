import { z } from "zod";

export const REVERSAL_PROTECTION_WINDOW_MS = 360_000;
export const REVERSAL_PROTECTION_OPPOSITE_COUNT = 3;
export const REVERSAL_PROTECTION_MIN_OPPOSITE_SPAN_MS = 2_000;
const timestamp = z.string().datetime({ offset: true });
export const reversalProtectionStateSchema = z.object({
  marketId: z.string().min(1), positionId: z.string().min(1),
  unhedgedStartedAt: timestamp, heldOutcome: z.enum(["UP", "DOWN"]),
  oppositeCount: z.number().int().min(1).max(3),
  countedConsensusEventIds: z.array(z.string().min(1)).min(1).max(3),
  firstOppositeAt: timestamp.optional(),
  windowEndsAt: timestamp, confirmedAt: timestamp.nullable(),
  confirmedByConsensusEventId: z.string().min(1).nullable(),
  revision: z.number().int().positive(), updatedAt: timestamp,
}).superRefine((state, ctx) => {
  const firstOppositeAt = state.firstOppositeAt === undefined ? null : Date.parse(state.firstOppositeAt);
  if (state.oppositeCount !== state.countedConsensusEventIds.length
    || new Set(state.countedConsensusEventIds).size !== state.oppositeCount
    || Date.parse(state.windowEndsAt) !== Date.parse(state.unhedgedStartedAt) + REVERSAL_PROTECTION_WINDOW_MS
    || state.revision !== state.oppositeCount
    || (state.oppositeCount === 3) !== (state.confirmedAt !== null)
    || (state.confirmedAt === null) !== (state.confirmedByConsensusEventId === null)
    || (firstOppositeAt !== null && (!Number.isFinite(firstOppositeAt)
      || firstOppositeAt < Date.parse(state.unhedgedStartedAt) || firstOppositeAt > Date.parse(state.updatedAt)))
    || (state.confirmedAt !== null && (Date.parse(state.confirmedAt) < Date.parse(state.unhedgedStartedAt)
      || Date.parse(state.confirmedAt) > Date.parse(state.windowEndsAt)
      || state.confirmedByConsensusEventId !== state.countedConsensusEventIds[2]
      || (firstOppositeAt !== null
        && Date.parse(state.confirmedAt) - firstOppositeAt < REVERSAL_PROTECTION_MIN_OPPOSITE_SPAN_MS)))) {
    ctx.addIssue({ code: "custom", message: "invalid reversal protection state" });
  }
});
export type ReversalProtectionState = z.infer<typeof reversalProtectionStateSchema>;
export type ReversalEpisode = Pick<ReversalProtectionState, "marketId" | "positionId" | "unhedgedStartedAt" | "heldOutcome">;

export function sameReversalEpisode(state: ReversalEpisode | null | undefined, episode: ReversalEpisode): boolean {
  return !!state && state.marketId === episode.marketId && state.positionId === episode.positionId
    && state.unhedgedStartedAt === episode.unhedgedStartedAt && state.heldOutcome === episode.heldOutcome;
}

/** Raw journal opposites remain cumulative, but confirmation requires >=2s from the first counted opposite. */
export function evaluateReversalProtection(input: {
  state: ReversalProtectionState | null; episode: ReversalEpisode | null;
  consensusEventId: string; consensusDirection: "UP" | "DOWN" | "FLAT"; now: Date;
}): { nextState: ReversalProtectionState | null; stateChanged: boolean } {
  const episode = input.episode;
  if (!episode) return { nextState: null, stateChanged: false };
  const previous = sameReversalEpisode(input.state, episode) ? input.state : null;
  const noop = { nextState: previous, stateChanged: false };
  const started = Date.parse(episode.unhedgedStartedAt);
  const now = input.now.getTime();
  if (previous?.confirmedAt || input.consensusDirection === "FLAT"
    || input.consensusDirection === episode.heldOutcome || !input.consensusEventId
    || !Number.isFinite(started) || !Number.isFinite(now) || now < started
    || now > started + REVERSAL_PROTECTION_WINDOW_MS
    || previous?.countedConsensusEventIds.includes(input.consensusEventId)) return noop;

  const firstOppositeAt = previous?.firstOppositeAt
    ? Date.parse(previous.firstOppositeAt)
    : previous
      ? Date.parse(previous.updatedAt)
      : now;
  if (previous?.oppositeCount === 2
    && now - firstOppositeAt < REVERSAL_PROTECTION_MIN_OPPOSITE_SPAN_MS) return noop;

  const count = (previous?.oppositeCount ?? 0) + 1;
  const firstOppositeIso = previous?.firstOppositeAt
    ?? (previous ? previous.updatedAt : input.now.toISOString());
  return { stateChanged: true, nextState: {
    ...episode, oppositeCount: count,
    countedConsensusEventIds: [...(previous?.countedConsensusEventIds ?? []), input.consensusEventId],
    firstOppositeAt: firstOppositeIso,
    windowEndsAt: new Date(started + REVERSAL_PROTECTION_WINDOW_MS).toISOString(),
    confirmedAt: count === 3 ? input.now.toISOString() : null,
    confirmedByConsensusEventId: count === 3 ? input.consensusEventId : null,
    revision: count, updatedAt: input.now.toISOString(),
  } };
}
