import { z } from "zod";
import { guardianV2StateSchema } from "./guardian-v2.js";

export const REVERSAL_PROTECTION_WINDOW_MS = 180_000;
export const REVERSAL_PROTECTION_OPPOSITE_COUNT = 3;
const timestamp = z.string().datetime({ offset: true });
export const legacyReversalProtectionStateSchema = z.object({
  marketId: z.string().min(1), positionId: z.string().min(1),
  unhedgedStartedAt: timestamp, heldOutcome: z.enum(["UP", "DOWN"]),
  oppositeCount: z.number().int().min(1).max(3),
  countedConsensusEventIds: z.array(z.string().min(1)).min(1).max(3),
  windowEndsAt: timestamp, confirmedAt: timestamp.nullable(),
  confirmedByConsensusEventId: z.string().min(1).nullable(),
  revision: z.number().int().positive(), updatedAt: timestamp,
}).superRefine((state, ctx) => {
  if (state.oppositeCount !== state.countedConsensusEventIds.length
    || new Set(state.countedConsensusEventIds).size !== state.oppositeCount
    || ![180000, 360000].includes(Date.parse(state.windowEndsAt) - Date.parse(state.unhedgedStartedAt))
    || state.revision !== state.oppositeCount
    || (state.oppositeCount === 3) !== (state.confirmedAt !== null)
    || (state.confirmedAt === null) !== (state.confirmedByConsensusEventId === null)
    || (state.confirmedAt !== null && (Date.parse(state.confirmedAt) < Date.parse(state.unhedgedStartedAt)
      || Date.parse(state.confirmedAt) > Date.parse(state.windowEndsAt)
      || state.confirmedByConsensusEventId !== state.countedConsensusEventIds[2]))) {
    ctx.addIssue({ code: "custom", message: "invalid reversal protection state" });
  }
});
export const reversalProtectionStateSchema = z.union([guardianV2StateSchema, legacyReversalProtectionStateSchema]);
export type ReversalProtectionState = z.infer<typeof reversalProtectionStateSchema>;
export type LegacyReversalProtectionState = z.infer<typeof legacyReversalProtectionStateSchema>;
export type ReversalEpisode = Pick<ReversalProtectionState, "marketId" | "positionId" | "unhedgedStartedAt" | "heldOutcome">;

export function sameReversalEpisode(state: ReversalEpisode | null | undefined, episode: ReversalEpisode): boolean {
  return !!state && state.marketId === episode.marketId && state.positionId === episode.positionId
    && state.unhedgedStartedAt === episode.unhedgedStartedAt && state.heldOutcome === episode.heldOutcome;
}

/** Legacy V1 classifier retained for historical replay tests; not runtime V2 authority. */
export function evaluateReversalProtection(input: {
  state: LegacyReversalProtectionState | null; episode: ReversalEpisode | null;
  consensusEventId: string; consensusDirection: "UP" | "DOWN" | "FLAT"; now: Date;
}): { nextState: LegacyReversalProtectionState | null; stateChanged: boolean } {
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
  const count = (previous?.oppositeCount ?? 0) + 1;
  return { stateChanged: true, nextState: {
    ...episode, oppositeCount: count,
    countedConsensusEventIds: [...(previous?.countedConsensusEventIds ?? []), input.consensusEventId],
    windowEndsAt: new Date(started + REVERSAL_PROTECTION_WINDOW_MS).toISOString(),
    confirmedAt: count === 3 ? input.now.toISOString() : null,
    confirmedByConsensusEventId: count === 3 ? input.consensusEventId : null,
    revision: count, updatedAt: input.now.toISOString(),
  } };
}
