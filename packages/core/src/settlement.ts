import type { Outcome, Position } from "./domain.js";
import { d, decimal } from "./calculations.js";

export type PublicResolutionStatus = "unresolved" | "resolved";

export interface PublicBinaryResolution {
  status: PublicResolutionStatus;
  winner: Outcome | null;
  upPayoff: "0" | "1" | null;
  downPayoff: "0" | "1" | null;
  umaResolutionStatus: string | null;
  closed: boolean;
  source: string;
  reason: string;
}

const TERMINAL_UMA = new Set(["resolved", "settled"]);

function binaryPayoff(value: unknown): "0" | "1" | null {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (Math.abs(n - 1) <= 1e-9) return "1";
  if (Math.abs(n) <= 1e-9) return "0";
  return null;
}

function outcomeFromLabel(label: unknown): Outcome | null {
  const value = String(label ?? "").trim().toLowerCase();
  if (value === "up" || value === "higher" || value === "yes") return "UP";
  if (value === "down" || value === "lower" || value === "no") return "DOWN";
  return null;
}

/**
 * Parse official/public Polymarket resolution. Never guesses a winner.
 * Canonical fields: resolution.umaResolutionStatus, state.closed, outcomes.yes/no.price
 * (Gamma outcomePrices 0/1). Winning token pays $1, losing $0 only when those prices are exact.
 */
export function parsePublicBinaryResolution(raw: Record<string, unknown>, yesMapsTo: Outcome = "UP", noMapsTo: Outcome = "DOWN"): PublicBinaryResolution {
  const state = raw.state && typeof raw.state === "object" ? raw.state as Record<string, unknown> : {};
  const resolution = raw.resolution && typeof raw.resolution === "object" ? raw.resolution as Record<string, unknown> : {};
  const outcomes = raw.outcomes && typeof raw.outcomes === "object" ? raw.outcomes as Record<string, unknown> : {};
  const yes = outcomes.yes && typeof outcomes.yes === "object" ? outcomes.yes as Record<string, unknown> : {};
  const no = outcomes.no && typeof outcomes.no === "object" ? outcomes.no as Record<string, unknown> : {};
  const closed = state.closed === true || raw.closed === true;
  const uma = String(resolution.umaResolutionStatus ?? raw.umaResolutionStatus ?? "").toLowerCase() || null;
  const yesPrice = binaryPayoff(yes.price ?? (Array.isArray(raw.outcomePrices) ? raw.outcomePrices[0] : null));
  const noPrice = binaryPayoff(no.price ?? (Array.isArray(raw.outcomePrices) ? raw.outcomePrices[1] : null));
  const yesOutcome = outcomeFromLabel(yes.label ?? yes.name) ?? yesMapsTo;
  const noOutcome = outcomeFromLabel(no.label ?? no.name) ?? noMapsTo;
  const unresolved = (reason: string): PublicBinaryResolution => ({
    status: "unresolved", winner: null, upPayoff: null, downPayoff: null,
    umaResolutionStatus: uma, closed, source: "public_polymarket", reason,
  });
  if (!closed && !(uma && TERMINAL_UMA.has(uma))) return unresolved("NOT_TERMINAL");
  if (yesPrice === null || noPrice === null) return unresolved("PAYOFF_NOT_BINARY");
  if (yesPrice === noPrice) return unresolved("PAYOFF_NOT_BINARY");
  const yesWin = yesPrice === "1";
  const winner = yesWin ? yesOutcome : noOutcome;
  if (winner !== "UP" && winner !== "DOWN") return unresolved("WINNER_UNMAPPED");
  const upPayoff: "0" | "1" = winner === "UP" ? "1" : "0";
  const downPayoff: "0" | "1" = winner === "DOWN" ? "1" : "0";
  return {
    status: "resolved", winner, upPayoff, downPayoff, umaResolutionStatus: uma, closed,
    source: "public_polymarket", reason: "RESOLVED",
  };
}

export function settlementCashDelta(position: Position, upPayoff: "0" | "1", downPayoff: "0" | "1"): string {
  return decimal(d(position.upQuantity).mul(upPayoff).plus(d(position.downQuantity).mul(downPayoff)));
}

export function settlementRealizedDelta(position: Position, upPayoff: "0" | "1", downPayoff: "0" | "1"): string {
  const proceeds = d(settlementCashDelta(position, upPayoff, downPayoff));
  const cost = d(position.upQuantity).mul(position.averageUpCost).plus(d(position.downQuantity).mul(position.averageDownCost));
  return decimal(proceeds.minus(cost));
}

export function classifySettledOutcome(realizedDelta: string): "POSITION_WIN" | "POSITION_LOSS" | "POSITION_BREAKEVEN" {
  const value = d(realizedDelta);
  if (value.gt(0)) return "POSITION_WIN";
  if (value.lt(0)) return "POSITION_LOSS";
  return "POSITION_BREAKEVEN";
}
