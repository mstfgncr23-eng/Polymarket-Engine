import type { MarketSnapshot, TriggerEvent } from "./domain.js";
import { astraFeaturePacketSchema, type AstraFeaturePacket } from "./astra.js";
import type { NativeReferenceFeatures } from "./native-decision.js";

export interface AstraFeatureInput {
  trigger: TriggerEvent;
  up: MarketSnapshot;
  down: MarketSnapshot;
  reference: NativeReferenceFeatures | null;
  now: Date;
  phase?: "RUNNING" | "ENTRY_CLOSED" | "COMPLETED" | "IDLE";
  referenceRequired?: boolean;
  context?: {
    currentMarketExposureUsd?: number;
    currentTotalExposureUsd?: number;
    currentDirectionalExposureUsd?: number;
    currentUnhedgedExposureUsd?: number;
    drawdownUtilization?: number;
    availableCashUsd?: number;
    currentPositionSummary?: string | null;
  };
}

export function buildAstraFeaturePacket(input: AstraFeatureInput): AstraFeaturePacket | null {
  if (!validDate(input.now) || input.up.marketId !== input.down.marketId || input.up.outcome !== "UP" || input.down.outcome !== "DOWN") return null;
  const up = bookFeatures(input.up);
  const down = bookFeatures(input.down);
  if (!up || !down) return null;
  const expiresAtMs = Date.parse(input.trigger.expiresAt);
  // Expiry is an admission concern, not a feature-extraction failure. Build a
  // valid packet with timeToExpiryMs=0 and let the deterministic opportunity
  // gate reject it as STALE before any provider dispatch or budget reservation.
  if (!Number.isFinite(expiresAtMs)) return null;
  const reference = input.reference;
  const usableReference = reference && reference.freshness === "fresh" ? reference : null;
  const move = finiteOrNull(usableReference?.recentMoveBps);
  const volatility = nonnegativeOrNull(usableReference?.recentVolatilityBps);
  const context = input.context ?? {};
  const packet = {
    schemaVersion: 1 as const,
    marketId: input.up.marketId,
    asset: input.trigger.asset ?? input.up.asset ?? input.down.asset ?? null,
    triggerId: input.trigger.id,
    traceId: input.trigger.traceId,
    phase: input.phase ?? "RUNNING",
    observedAt: input.now.toISOString(),
    expiresAt: input.trigger.expiresAt,
    upBid: up.bid,
    upAsk: up.ask,
    downBid: down.bid,
    downAsk: down.ask,
    upMid: up.mid,
    downMid: down.mid,
    upImbalance: up.imbalance,
    downImbalance: down.imbalance,
    bookPressure: (up.imbalance - down.imbalance) / 2,
    upSpreadBps: up.spreadBps,
    downSpreadBps: down.spreadBps,
    upDepthUsd: up.depthUsd,
    downDepthUsd: down.depthUsd,
    pairAskCost: up.ask + down.ask,
    recentMoveBps: move,
    recentVolatilityBps: volatility,
    referenceVenue: reference?.venue ?? null,
    referenceFreshness: reference?.freshness ?? "unknown",
    referenceRequired: input.referenceRequired ?? false,
    referenceFreshnessMs: finiteOrNull(reference?.freshnessMs),
    timeToExpiryMs: Math.max(0, expiresAtMs - input.now.getTime()),
    regime: classifyAstraRegime({ move, volatility, upSpreadBps: up.spreadBps, downSpreadBps: down.spreadBps, pairAskCost: up.ask + down.ask }),
    triggerKind: input.trigger.kind,
    triggerSeverity: input.trigger.severity,
    triggerScore: input.trigger.score,
    currentMarketExposureUsd: finiteOrZero(context.currentMarketExposureUsd),
    currentTotalExposureUsd: finiteOrZero(context.currentTotalExposureUsd),
    currentDirectionalExposureUsd: finiteOrZero(context.currentDirectionalExposureUsd),
    currentUnhedgedExposureUsd: finiteOrZero(context.currentUnhedgedExposureUsd),
    drawdownUtilization: clamp01(context.drawdownUtilization),
    availableCashUsd: finiteOrZero(context.availableCashUsd),
    currentPositionSummary: context.currentPositionSummary ?? null,
  };
  const parsed = astraFeaturePacketSchema.safeParse(packet);
  return parsed.success ? parsed.data : null;
}

export function classifyAstraRegime(input: {
  move: number | null;
  volatility: number | null;
  upSpreadBps: number;
  downSpreadBps: number;
  pairAskCost: number;
}): AstraFeaturePacket["regime"] {
  const spread = Math.max(input.upSpreadBps, input.downSpreadBps);
  if (input.pairAskCost > 1.02 || spread > 800) return "DISLOCATED";
  if ((input.volatility ?? 0) >= 80 || Math.abs(input.move ?? 0) >= 80) return "VOLATILE";
  if (Math.abs(input.move ?? 0) >= 15) return "TRENDING";
  if (input.move === null && input.volatility === null) return "UNKNOWN";
  return "CALM";
}

interface BookFeatures { mid: number; bid: number; ask: number; imbalance: number; spreadBps: number; depthUsd: number; }
function bookFeatures(snapshot: MarketSnapshot): BookFeatures | null {
  const bid = snapshot.bids[0];
  const ask = snapshot.asks[0];
  // A one-sided/empty book is valid market state but not a model feature
  // failure. First reject malformed levels that are present; otherwise encode
  // a zero-depth sentinel so the deterministic gate owns NO_LIQUIDITY.
  if (!bid || !ask) {
    if ((bid && !validTopLevel(bid)) || (ask && !validTopLevel(ask))) return null;
    return emptyBookFeatures();
  }
  const bidPrice = Number(bid.price); const askPrice = Number(ask.price);
  const bidQty = Number(bid.quantity); const askQty = Number(ask.quantity);
  if (![bidPrice, askPrice, bidQty, askQty].every(Number.isFinite) || askPrice < bidPrice || bidQty < 0 || askQty < 0) return null;
  if (bidPrice < 0 || askPrice > 1) return null;
  const denominator = bidQty + askQty;
  if (denominator <= 0) {
    return { mid: (bidPrice + askPrice) / 2, bid: bidPrice, ask: askPrice, imbalance: 0, spreadBps: (askPrice - bidPrice) * 10_000, depthUsd: 0 };
  }
  const depthUsd = snapshot.asks.slice(0, 5).reduce((sum, level) => sum + Number(level.price) * Number(level.quantity), 0);
  if (!Number.isFinite(depthUsd) || depthUsd < 0) return null;
  return {
    mid: (bidPrice + askPrice) / 2,
    bid: bidPrice,
    ask: askPrice,
    imbalance: (bidQty - askQty) / denominator,
    spreadBps: (askPrice - bidPrice) * 10_000,
    depthUsd,
  };
}
function validTopLevel(level: { price: string; quantity: string }): boolean {
  const price = Number(level.price);
  const quantity = Number(level.quantity);
  return Number.isFinite(price) && Number.isFinite(quantity) && price >= 0 && price <= 1 && quantity >= 0;
}
function emptyBookFeatures(): BookFeatures { return { mid: 0, bid: 0, ask: 0, imbalance: 0, spreadBps: 0, depthUsd: 0 }; }
function validDate(value: Date): boolean { return value instanceof Date && Number.isFinite(value.getTime()); }
function finiteOrNull(value: number | null | undefined): number | null { return value !== null && value !== undefined && Number.isFinite(value) ? value : null; }
function nonnegativeOrNull(value: number | null | undefined): number | null { const next = finiteOrNull(value); return next === null ? null : Math.max(0, next); }
function finiteOrZero(value: number | null | undefined): number { return value !== null && value !== undefined && Number.isFinite(value) ? value : 0; }
function clamp01(value: number | null | undefined): number { return Math.max(0, Math.min(1, finiteOrZero(value))); }
