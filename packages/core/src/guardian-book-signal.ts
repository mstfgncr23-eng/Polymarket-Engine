import { d } from "./calculations.js";
import { normalizeOppositeBookPressure } from "./guardian-v2.js";
import type { MarketSnapshot, Outcome } from "./domain.js";

/**
 * G2B: the transport-free part of Guardian's paired-book observation.
 *
 * Inputs must ALREADY have passed engine-owned token identity, same-market,
 * freshness, synchronization and current-epoch gates. This pure projection
 * grants no risk/execution authority and starts no second Guardian.
 *
 * Preserves the existing UP-positive pressure sign and event-ID ordering.
 */
export function projectGuardianBookSignal(
  up: MarketSnapshot,
  down: MarketSnapshot,
  heldOutcome: Outcome,
): { eventId: string; heldPrice: string; bookPressure: string } | null {
  const top = (book: MarketSnapshot) => {
    const bid = book.bids[0];
    const ask = book.asks[0];
    if (!bid || !ask) return null;
    const bp = d(bid.price), ap = d(ask.price), bq = d(bid.quantity), aq = d(ask.quantity);
    if (![bp, ap, bq, aq].every((value) => value.isFinite()) || bp.lt(0) || ap.gt(1) || bp.gt(ap)
      || bq.lt(0) || aq.lt(0) || bq.plus(aq).lte(0)) return null;
    return { mid: bp.plus(ap).div(2), imbalance: bq.minus(aq).div(bq.plus(aq)) };
  };
  const u = top(up), v = top(down);
  if (!u || !v) return null;
  return {
    eventId: `${up.eventId}:${down.eventId}`,
    heldPrice: (heldOutcome === "UP" ? u : v).mid.toFixed(),
    bookPressure: normalizeOppositeBookPressure(heldOutcome, u.imbalance.minus(v.imbalance).div(2).toFixed()),
  };
}
