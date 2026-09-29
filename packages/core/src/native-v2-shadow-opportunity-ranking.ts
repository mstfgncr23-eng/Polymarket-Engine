import { calculateFee, d, decimal, orderBookLiquidityUsd } from "./calculations.js";
import type { MarketSnapshot, Outcome } from "./domain.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import { DEFAULT_FIRST_LEG_RISK_CAP_USD } from "./strategy.js";

export const NATIVE_V2_SHADOW_OPPORTUNITY_VERSION = "native-v2-shadow-opportunity-v2" as const;
export const NATIVE_V2_SHADOW_RANKING_VERSION = "native-v2-shadow-ranking-v2" as const;

const DEFAULT_MAX_MARKETS = 512;

export interface NativeV2ShadowOpportunityCandidate {
  marketId: string;
  outcome: Outcome;
  fairProbability: number;
  targetNotionalUsd: number;
  bestAsk: number;
  executableVwap: number;
  worstPrice: number;
  levelsConsumed: number;
  quantity: string;
  executableNotionalUsd: string;
  requiredCashUsd: string;
  availableAskLiquidityUsd: string;
  feeUsd: string;
  feeUsdPerShare: number;
  slippageUsdPerShare: number;
  grossEdgeUsdPerShare: number;
  netEvUsdPerShare: number;
  expectedEvUsdTotal: number;
  expectedROI: number;
  actionable: boolean;
  minimumNetEvUsdPerShare: number;
}

export interface NativeV2ShadowMarketOpportunity {
  version: typeof NATIVE_V2_SHADOW_OPPORTUNITY_VERSION;
  marketId: string;
  observedAt: string;
  fairProbabilityVersion: NativeV2ShadowFairProbability["version"];
  calibrationStatus: NativeV2ShadowFairProbability["calibrationStatus"];
  targetNotionalUsd: number;
  candidates: Record<Outcome, NativeV2ShadowOpportunityCandidate | null>;
  best: NativeV2ShadowOpportunityCandidate | null;
}

export interface NativeV2ShadowOpportunitySummary {
  marketId: string;
  outcome: Outcome;
  expectedROI: number;
  expectedEvUsdTotal: number;
  netEvUsdPerShare: number;
  executableVwap: number;
  fairProbability: number;
  actionable: boolean;
}

export interface NativeV2ShadowOpportunityRanking {
  version: typeof NATIVE_V2_SHADOW_RANKING_VERSION;
  rankedAt: string;
  marketId: string;
  marketRank: number | null;
  rankedMarketCount: number;
  actionableMarketCount: number;
  current: NativeV2ShadowOpportunitySummary | null;
  top: NativeV2ShadowOpportunitySummary | null;
}

/**
 * Phase-3 shadow opportunity economics.
 *
 * For each binary side, spend a fixed first-leg paper-risk notional through the
 * CURRENT ask book, calculate VWAP and the venue fee, then compare that cash
 * cost with the V2 fair probability. This is deliberately independent of the
 * executable V1 strategy/risk path; it only produces shadow economics.
 */
export function evaluateNativeV2ShadowMarketOpportunity(input: {
  fair: NativeV2ShadowFairProbability;
  snapshots: Record<Outcome, MarketSnapshot | null>;
  now: Date;
  targetNotionalUsd?: number;
  minimumNetEvUsdPerShare?: number;
}): NativeV2ShadowMarketOpportunity | null {
  const up = input.snapshots.UP;
  const down = input.snapshots.DOWN;
  if (!up || !down || up.marketId !== down.marketId) return null;

  const targetNotionalUsd = input.targetNotionalUsd ?? DEFAULT_FIRST_LEG_RISK_CAP_USD;
  const minimumNetEvUsdPerShare = input.minimumNetEvUsdPerShare ?? 0;
  if (!Number.isFinite(targetNotionalUsd) || targetNotionalUsd <= 0) return null;
  if (!Number.isFinite(minimumNetEvUsdPerShare)) return null;

  const candidates: Record<Outcome, NativeV2ShadowOpportunityCandidate | null> = {
    UP: evaluateCandidate(up, input.fair.fairProbabilityUp, targetNotionalUsd, minimumNetEvUsdPerShare),
    DOWN: evaluateCandidate(down, input.fair.fairProbabilityDown, targetNotionalUsd, minimumNetEvUsdPerShare),
  };
  const winnerOutcome: Outcome | null = input.fair.fairProbabilityUp > input.fair.fairProbabilityDown
    ? "UP"
    : input.fair.fairProbabilityDown > input.fair.fairProbabilityUp ? "DOWN" : null;
  // Preserve loser-side economics for diagnostics, but never expose it as an
  // actionable first-leg opportunity in a winner-direction engine.
  if (winnerOutcome === "UP" && candidates.DOWN) {
    candidates.DOWN = { ...candidates.DOWN, actionable: false };
  } else if (winnerOutcome === "DOWN" && candidates.UP) {
    candidates.UP = { ...candidates.UP, actionable: false };
  } else if (winnerOutcome === null) {
    if (candidates.UP) candidates.UP = { ...candidates.UP, actionable: false };
    if (candidates.DOWN) candidates.DOWN = { ...candidates.DOWN, actionable: false };
  }
  const best = winnerOutcome ? candidates[winnerOutcome] : null;

  return {
    version: NATIVE_V2_SHADOW_OPPORTUNITY_VERSION,
    marketId: up.marketId,
    observedAt: input.now.toISOString(),
    fairProbabilityVersion: input.fair.version,
    calibrationStatus: input.fair.calibrationStatus,
    targetNotionalUsd,
    candidates,
    best,
  };
}

/**
 * Bounded latest-value ranker across markets. Ranking is lexicographic, not a
 * hand-tuned weighted score: capital efficiency (ROI), then total expected
 * dollars, then per-share EV, then stable market/outcome identifiers.
 */
export class NativeV2ShadowOpportunityRanker {
  private readonly latest = new Map<string, NativeV2ShadowMarketOpportunity>();

  constructor(private readonly maxMarkets = DEFAULT_MAX_MARKETS) {
    if (!Number.isInteger(maxMarkets) || maxMarkets <= 0) throw new Error("INVALID_MAX_MARKETS");
  }

  upsertAndRank(
    opportunity: NativeV2ShadowMarketOpportunity,
    now: Date,
    maxAgeMs: number,
  ): NativeV2ShadowOpportunityRanking {
    if (!Number.isFinite(maxAgeMs) || maxAgeMs < 0) throw new Error("INVALID_MAX_AGE_MS");
    this.latest.set(opportunity.marketId, opportunity);
    this.evictStale(now.getTime(), maxAgeMs);
    this.enforceBound();

    const ranked = [...this.latest.values()]
      .filter((item): item is NativeV2ShadowMarketOpportunity & { best: NativeV2ShadowOpportunityCandidate } => item.best !== null)
      .sort(compareMarketOpportunities);
    const index = ranked.findIndex((item) => item.marketId === opportunity.marketId);
    const top = ranked[0]?.best ?? null;

    return {
      version: NATIVE_V2_SHADOW_RANKING_VERSION,
      rankedAt: now.toISOString(),
      marketId: opportunity.marketId,
      marketRank: index >= 0 ? index + 1 : null,
      rankedMarketCount: ranked.length,
      actionableMarketCount: ranked.reduce((count, item) => count + (item.best.actionable ? 1 : 0), 0),
      current: opportunity.best ? summarize(opportunity.best) : null,
      top: top ? summarize(top) : null,
    };
  }

  dropMarket(marketId: string): void {
    this.latest.delete(marketId);
  }

  size(): number {
    return this.latest.size;
  }

  private evictStale(nowMs: number, maxAgeMs: number): void {
    const cutoff = nowMs - maxAgeMs;
    for (const [marketId, item] of this.latest) {
      const observedMs = Date.parse(item.observedAt);
      if (!Number.isFinite(observedMs) || observedMs < cutoff) this.latest.delete(marketId);
    }
  }

  private enforceBound(): void {
    if (this.latest.size <= this.maxMarkets) return;
    const oldest = [...this.latest.values()].sort((a, b) => {
      const time = Date.parse(a.observedAt) - Date.parse(b.observedAt);
      return time !== 0 ? time : a.marketId.localeCompare(b.marketId);
    });
    for (let i = 0; i < oldest.length - this.maxMarkets; i += 1) {
      this.latest.delete(oldest[i]!.marketId);
    }
  }
}

function evaluateCandidate(
  snapshot: MarketSnapshot,
  fairProbability: number,
  targetNotionalUsd: number,
  minimumNetEvUsdPerShare: number,
): NativeV2ShadowOpportunityCandidate | null {
  if (!Number.isFinite(fairProbability) || fairProbability < 0 || fairProbability > 1) return null;
  if (!snapshot.feeSchedule) return null;
  const bestAskRaw = snapshot.asks[0]?.price;
  if (!bestAskRaw) return null;
  const bestAsk = Number(bestAskRaw);
  if (!Number.isFinite(bestAsk) || bestAsk <= 0 || bestAsk > 1) return null;

  const walk = walkAskBookByNotional(snapshot, targetNotionalUsd);
  if (!walk) return null;

  const quantity = d(walk.quantity);
  const feeUsd = calculateFee(walk.quantity, walk.vwap, "TAKER", snapshot.feeSchedule);
  const feeUsdPerShare = d(feeUsd).div(quantity).toNumber();
  const executableVwap = Number(walk.vwap);
  const grossEdgeUsdPerShare = fairProbability - executableVwap;
  const netEvUsdPerShare = grossEdgeUsdPerShare - feeUsdPerShare;
  const expectedEvUsdTotal = d(netEvUsdPerShare).mul(quantity).toNumber();
  const requiredCash = d(walk.notional).plus(feeUsd);
  const expectedROI = requiredCash.gt(0) ? d(expectedEvUsdTotal).div(requiredCash).toNumber() : Number.NEGATIVE_INFINITY;

  return {
    marketId: snapshot.marketId,
    outcome: snapshot.outcome,
    fairProbability,
    targetNotionalUsd,
    bestAsk,
    executableVwap,
    worstPrice: Number(walk.worstPrice),
    levelsConsumed: walk.levelsConsumed,
    quantity: walk.quantity,
    executableNotionalUsd: walk.notional,
    requiredCashUsd: decimal(requiredCash),
    availableAskLiquidityUsd: orderBookLiquidityUsd(snapshot, "BUY"),
    feeUsd,
    feeUsdPerShare,
    slippageUsdPerShare: Math.max(0, executableVwap - bestAsk),
    grossEdgeUsdPerShare,
    netEvUsdPerShare,
    expectedEvUsdTotal,
    expectedROI,
    actionable: netEvUsdPerShare >= minimumNetEvUsdPerShare,
    minimumNetEvUsdPerShare,
  };
}

function walkAskBookByNotional(snapshot: MarketSnapshot, targetNotionalUsd: number): {
  quantity: string;
  notional: string;
  vwap: string;
  worstPrice: string;
  levelsConsumed: number;
} | null {
  let remaining = d(targetNotionalUsd);
  let quantity = d(0);
  let notional = d(0);
  let worstPrice = d(0);
  let levelsConsumed = 0;

  for (const level of snapshot.asks) {
    const price = d(level.price);
    const available = d(level.quantity);
    if (price.lte(0) || price.gt(1) || available.lte(0)) continue;
    const levelNotional = price.mul(available);
    const spend = levelNotional.lt(remaining) ? levelNotional : remaining;
    const take = spend.div(price);
    quantity = quantity.plus(take);
    notional = notional.plus(spend);
    remaining = remaining.minus(spend);
    worstPrice = price;
    levelsConsumed += 1;
    if (remaining.eq(0)) break;
  }

  if (remaining.gt(0) || quantity.lte(0)) return null;
  return {
    quantity: decimal(quantity),
    notional: decimal(notional),
    vwap: decimal(notional.div(quantity)),
    worstPrice: decimal(worstPrice),
    levelsConsumed,
  };
}

function compareCandidates(a: NativeV2ShadowOpportunityCandidate, b: NativeV2ShadowOpportunityCandidate): number {
  const roi = descending(a.expectedROI, b.expectedROI);
  if (roi !== 0) return roi;
  const total = descending(a.expectedEvUsdTotal, b.expectedEvUsdTotal);
  if (total !== 0) return total;
  const perShare = descending(a.netEvUsdPerShare, b.netEvUsdPerShare);
  if (perShare !== 0) return perShare;
  const market = a.marketId.localeCompare(b.marketId);
  return market !== 0 ? market : a.outcome.localeCompare(b.outcome);
}

function compareMarketOpportunities(
  a: NativeV2ShadowMarketOpportunity & { best: NativeV2ShadowOpportunityCandidate },
  b: NativeV2ShadowMarketOpportunity & { best: NativeV2ShadowOpportunityCandidate },
): number {
  // Non-actionable winner diagnostics must never starve an actionable market
  // from the global rank-1 execution slot.
  if (a.best.actionable !== b.best.actionable) return a.best.actionable ? -1 : 1;
  return compareCandidates(a.best, b.best);
}

function descending(a: number, b: number): number {
  if (a === b) return 0;
  return a > b ? -1 : 1;
}

function summarize(candidate: NativeV2ShadowOpportunityCandidate): NativeV2ShadowOpportunitySummary {
  return {
    marketId: candidate.marketId,
    outcome: candidate.outcome,
    expectedROI: candidate.expectedROI,
    expectedEvUsdTotal: candidate.expectedEvUsdTotal,
    netEvUsdPerShare: candidate.netEvUsdPerShare,
    executableVwap: candidate.executableVwap,
    fairProbability: candidate.fairProbability,
    actionable: candidate.actionable,
  };
}
