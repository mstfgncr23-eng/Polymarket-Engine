import type { Outcome, Position } from "./domain.js";
import { d, decimal } from "./calculations.js";
import { directionalOutcomeFromLabel } from "./qualification.js";

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

function slotLabel(value: unknown): string | null {
  if (typeof value === "string" && value.trim()) return value;
  if (!value || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  for (const key of ["label", "name", "title", "outcome"] as const) {
    if (typeof row[key] === "string" && String(row[key]).trim()) return String(row[key]);
  }
  return null;
}

function slotPrice(value: unknown): unknown {
  if (!value || typeof value !== "object") return null;
  return (value as Record<string, unknown>).price;
}

function mapToken(label: unknown, yesMapsTo: Outcome, noMapsTo: Outcome): Outcome | null {
  const directional = directionalOutcomeFromLabel(typeof label === "string" ? label : slotLabel(label));
  if (directional) return directional;
  const token = String(typeof label === "string" ? label : slotLabel(label) ?? "").trim().toLowerCase();
  if (token === "yes" || token === "y") return yesMapsTo;
  if (token === "no" || token === "n") return noMapsTo;
  return null;
}

type JsonField = { ok: true; value: unknown } | { ok: false; reason: "MALFORMED_RESOLUTION_JSON" };

function parsePossiblyJsonEncoded(value: unknown): JsonField {
  if (typeof value !== "string") return { ok: true, value };
  const trimmed = value.trim();
  if (!(trimmed.startsWith("[") || trimmed.startsWith("{"))) return { ok: true, value };
  try {
    return { ok: true, value: JSON.parse(trimmed) };
  } catch {
    return { ok: false, reason: "MALFORMED_RESOLUTION_JSON" };
  }
}

function outcomeSlots(raw: Record<string, unknown>): { yesLabel: unknown; noLabel: unknown; yesPrice: unknown; noPrice: unknown } {
  const arrayPrices = Array.isArray(raw.outcomePrices) ? raw.outcomePrices : null;
  if (Array.isArray(raw.outcomes)) {
    return {
      yesLabel: slotLabel(raw.outcomes[0]) ?? raw.outcomes[0],
      noLabel: slotLabel(raw.outcomes[1]) ?? raw.outcomes[1],
      yesPrice: slotPrice(raw.outcomes[0]) ?? arrayPrices?.[0] ?? null,
      noPrice: slotPrice(raw.outcomes[1]) ?? arrayPrices?.[1] ?? null,
    };
  }
  const outcomes = raw.outcomes && typeof raw.outcomes === "object" ? raw.outcomes as Record<string, unknown> : {};
  const yes = outcomes.yes && typeof outcomes.yes === "object" ? outcomes.yes as Record<string, unknown> : {};
  const no = outcomes.no && typeof outcomes.no === "object" ? outcomes.no as Record<string, unknown> : {};
  return {
    yesLabel: yes.label ?? yes.name ?? yes.title,
    noLabel: no.label ?? no.name ?? no.title,
    yesPrice: yes.price ?? arrayPrices?.[0] ?? null,
    noPrice: no.price ?? arrayPrices?.[1] ?? null,
  };
}

/**
 * Parse official/public Polymarket resolution. Never guesses a winner.
 * Canonical fields: resolution.umaResolutionStatus, state.closed, outcomes.yes/no.price
 * (Gamma outcomePrices 0/1). Winning token pays $1, losing $0 only when those prices are exact.
 * YES/NO tokens follow the persisted qualification mapping; directional labels override.
 */
export function parsePublicBinaryResolution(
  raw: Record<string, unknown>,
  yesMapsTo: Outcome = "UP",
  noMapsTo: Outcome = "DOWN",
): PublicBinaryResolution {
  const state = raw.state && typeof raw.state === "object" ? raw.state as Record<string, unknown> : {};
  const resolution = raw.resolution && typeof raw.resolution === "object" ? raw.resolution as Record<string, unknown> : {};
  const closed = state.closed === true || raw.closed === true;
  const uma = String(resolution.umaResolutionStatus ?? raw.umaResolutionStatus ?? "").toLowerCase() || null;
  const unresolved = (reason: string): PublicBinaryResolution => ({
    status: "unresolved", winner: null, upPayoff: null, downPayoff: null,
    umaResolutionStatus: uma, closed, source: "public_polymarket", reason,
  });
  const outcomesField = parsePossiblyJsonEncoded(raw.outcomes);
  const pricesField = parsePossiblyJsonEncoded(raw.outcomePrices);
  if (!outcomesField.ok || !pricesField.ok) return unresolved("MALFORMED_RESOLUTION_JSON");
  const slots = outcomeSlots({ ...raw, outcomes: outcomesField.value, outcomePrices: pricesField.value });
  const yesPrice = binaryPayoff(slots.yesPrice);
  const noPrice = binaryPayoff(slots.noPrice);
  const yesOutcome = mapToken(slots.yesLabel, yesMapsTo, noMapsTo) ?? yesMapsTo;
  const noOutcome = mapToken(slots.noLabel, yesMapsTo, noMapsTo) ?? noMapsTo;
  if (!closed && !(uma && TERMINAL_UMA.has(uma))) return unresolved("NOT_TERMINAL");
  if (yesPrice === null || noPrice === null) return unresolved("PAYOFF_NOT_BINARY");
  if (yesPrice === noPrice) return unresolved("PAYOFF_NOT_BINARY");
  if (yesOutcome === noOutcome) return unresolved("WINNER_UNMAPPED");
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

/** Conservative realizable mark for a long inventory: best bid. Null when unavailable. */
export function conservativeRealizableMark(snapshot: { bids?: Array<{ price: string }> } | null | undefined): string | null {
  const bid = snapshot?.bids?.[0]?.price;
  if (bid === undefined) return null;
  const n = Number(bid);
  if (!Number.isFinite(n) || n < 0 || n > 1) return null;
  return bid;
}

export function settlementCashDelta(position: Position, upPayoff: "0" | "1", downPayoff: "0" | "1"): string {
  return decimal(d(position.upQuantity).mul(upPayoff).plus(d(position.downQuantity).mul(downPayoff)));
}

export function settlementRealizedDelta(position: Position, upPayoff: "0" | "1", downPayoff: "0" | "1"): string {
  const proceeds = d(settlementCashDelta(position, upPayoff, downPayoff));
  const cost = d(position.upQuantity).mul(position.averageUpCost).plus(d(position.downQuantity).mul(position.averageDownCost));
  return decimal(proceeds.minus(cost));
}

/** Win/loss is net of fees. Pass feesUsd=0 for the fee-exclusive gross classification. */
export function classifySettledOutcome(
  realizedDelta: string,
  feesUsd: string | number = "0",
): "POSITION_WIN" | "POSITION_LOSS" | "POSITION_BREAKEVEN" {
  const value = d(realizedDelta).minus(feesUsd);
  if (value.gt(0)) return "POSITION_WIN";
  if (value.lt(0)) return "POSITION_LOSS";
  return "POSITION_BREAKEVEN";
}
