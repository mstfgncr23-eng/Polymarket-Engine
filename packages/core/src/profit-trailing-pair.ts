import { z } from "zod";
import { d } from "./calculations.js";

const numberString = z.string().refine((value) => {
  try { return d(value).isFinite(); } catch { return false; }
});
const timestamp = z.string().datetime({ offset: true });

export const profitTrailingPairStateSchema = z.object({
  marketId: z.string().min(1),
  positionId: z.string().min(1),
  unhedgedStartedAt: timestamp,
  armedAt: timestamp,
  highWaterEdge: numberString,
  revision: z.number().int().positive(),
  updatedAt: timestamp,
});
export type ProfitTrailingPairState = z.infer<typeof profitTrailingPairStateSchema>;

export type ProfitTrailingPairAction = "NONE" | "TARGET" | "TRAIL";

export interface ProfitTrailingPairEvaluation {
  action: ProfitTrailingPairAction;
  reason: "BELOW_ARM" | "ARMED" | "HIGH_WATER" | "TARGET_REACHED" | "RETRACE_NOT_REACHED" | "TRAIL_TRIGGERED";
  nextState: ProfitTrailingPairState | null;
  stateChanged: boolean;
}

export function evaluateProfitTrailingPair(input: {
  state: ProfitTrailingPairState | null;
  marketId: string;
  positionId: string;
  unhedgedStartedAt: string;
  now: Date;
  currentGrossEdge: string;
  currentLockedPnl: string;
  targetEdge: number;
  armEdge: number;
  retraceEdge: number;
}): ProfitTrailingPairEvaluation {
  const current = d(input.currentGrossEdge);
  const lockedPnl = d(input.currentLockedPnl);
  const target = d(input.targetEdge);
  const arm = d(input.armEdge);
  const retrace = d(input.retraceEdge);
  if (!current.isFinite() || !lockedPnl.isFinite() || !target.isFinite() || !arm.isFinite() || !retrace.isFinite()
    || target.lte(0) || arm.lt(0) || retrace.lte(0) || arm.gte(target) || retrace.gte(target)) {
    throw new Error("INVALID_PROFIT_TRAILING_PAIR_INPUT");
  }

  const sameEpisode = input.state
    && input.state.marketId === input.marketId
    && input.state.positionId === input.positionId
    && input.state.unhedgedStartedAt === input.unhedgedStartedAt
    ? input.state : null;

  if (!sameEpisode && current.lt(arm) && current.lt(target)) {
    return { action: "NONE", reason: "BELOW_ARM", nextState: null, stateChanged: false };
  }

  const now = input.now.toISOString();
  let nextState: ProfitTrailingPairState;
  let stateChanged = false;
  if (!sameEpisode) {
    nextState = {
      marketId: input.marketId,
      positionId: input.positionId,
      unhedgedStartedAt: input.unhedgedStartedAt,
      armedAt: now,
      highWaterEdge: current.toFixed(),
      revision: 1,
      updatedAt: now,
    };
    stateChanged = true;
  } else if (current.gt(sameEpisode.highWaterEdge)) {
    nextState = {
      ...sameEpisode,
      highWaterEdge: current.toFixed(),
      revision: sameEpisode.revision + 1,
      updatedAt: now,
    };
    stateChanged = true;
  } else {
    nextState = sameEpisode;
  }

  if (current.gte(target)) {
    return { action: "TARGET", reason: "TARGET_REACHED", nextState, stateChanged };
  }

  const drawdown = d(nextState.highWaterEdge).minus(current);
  if (drawdown.gte(retrace) && lockedPnl.gt(0)) {
    return { action: "TRAIL", reason: "TRAIL_TRIGGERED", nextState, stateChanged };
  }
  return {
    action: "NONE",
    reason: stateChanged ? (nextState.revision === 1 ? "ARMED" : "HIGH_WATER") : "RETRACE_NOT_REACHED",
    nextState,
    stateChanged,
  };
}
