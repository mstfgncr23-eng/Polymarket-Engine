import type { Direction, MarketSnapshot, TriggerEvent } from "./domain.js";

/** The single economic decision authority used by the native paper experiment. */
export const NATIVE_DETERMINISTIC_AUTHORITY = "NATIVE_DETERMINISTIC_V1" as const;

export interface NativeReferenceFeatures {
  asset: string;
  venue: string | null;
  freshness: "fresh" | "stale" | "unknown";
  freshnessMs: number | null;
  recentMoveBps: number | null;
  recentVolatilityBps: number | null;
}

export interface NativeDecisionConfig {
  /** Explicit score weights for the V1 heuristic. */
  bookPressureWeight: number;
  referenceStrengthWeight: number;
  /** Maximum absolute probability correction applied to the market prior. */
  maxProbabilityAdjustment: number;
  probabilityFloor: number;
  probabilityCeiling: number;
  directionThreshold: number;
  disagreementSignalThreshold: number;
  disagreementConfidencePenalty: number;
  referenceRapidMoveBps: number;
}

export const defaultNativeDecisionConfig: NativeDecisionConfig = {
  bookPressureWeight: 0.5,
  referenceStrengthWeight: 0.5,
  maxProbabilityAdjustment: 0.05,
  probabilityFloor: 0.01,
  probabilityCeiling: 0.99,
  directionThreshold: 0.15,
  disagreementSignalThreshold: 0.1,
  disagreementConfidencePenalty: 0.15,
  referenceRapidMoveBps: 15,
};

export interface NativeDecisionInput {
  trigger: TriggerEvent;
  up: MarketSnapshot;
  down: MarketSnapshot;
  reference: NativeReferenceFeatures | null;
  referenceRequired: boolean;
  /** Supplied by the Engine after its token-sync and epoch-fence checks. */
  marketSynchronized: boolean;
  /** Supplied by the Engine after its trigger provenance check. */
  triggerProvenanceValid: boolean;
  now: Date;
  config?: Partial<NativeDecisionConfig>;
}

export interface NativeFeatureVector {
  upMid: number;
  downMid: number;
  marketPriorUp: number;
  upImbalance: number;
  downImbalance: number;
  bookPressure: number;
  referenceMoveBps: number | null;
  referenceVolatilityBps: number | null;
  referenceStrength: number;
  directionalScore: number;
  probabilityAdjustment: number;
  fairProbabilityUp: number;
  pairAskCost: number | null;
}

export interface NativeDecision {
  direction: Direction;
  /** Deterministic confidence bounded to [0, 1]. */
  confidence: number;
  /** Probability of the selected direction; for FLAT this is the UP probability. */
  fairProbability: number;
  /** Market-prior probability of the selected direction; for FLAT this is the UP prior. */
  marketProbability: number;
  expiresAt: string;
  evidenceIds: string[];
  featureVector: NativeFeatureVector;
}

/**
 * Conservative, deterministic V1 heuristic for the paper soak. It is a
 * bounded measurement model, not a statistically calibrated probability model.
 * It has no I/O, randomness, clock reads, or mutable state.
 */
export function evaluateNativeDecision(input: NativeDecisionInput): NativeDecision | null {
  const config: NativeDecisionConfig = { ...defaultNativeDecisionConfig, ...input.config };
  const expiresAt = Date.parse(input.trigger.expiresAt);
  if (!validDate(input.now) || !Number.isFinite(expiresAt) || input.now.getTime() >= expiresAt || !validConfig(config)) return null;
  if (!input.marketSynchronized || !input.triggerProvenanceValid) return null;
  if (input.up.marketId !== input.down.marketId || input.up.outcome !== "UP" || input.down.outcome !== "DOWN") return null;

  const upBook = topOfBook(input.up);
  const downBook = topOfBook(input.down);
  if (!upBook || !downBook) return null;

  const upMid = (upBook.bidPrice + upBook.askPrice) / 2;
  const downMid = (downBook.bidPrice + downBook.askPrice) / 2;
  if (!probabilityValue(upMid) || !probabilityValue(downMid)) return null;

  const upImbalance = imbalance(upBook.bidQuantity, upBook.askQuantity);
  const downImbalance = imbalance(downBook.bidQuantity, downBook.askQuantity);
  if (upImbalance === null || downImbalance === null) return null;

  const marketPriorUp = clamp((upMid + (1 - downMid)) / 2, config.probabilityFloor, config.probabilityCeiling);
  const bookPressure = clamp((upImbalance - downImbalance) / 2, -1, 1);

  const reference = usableReference(input.reference);
  if (input.referenceRequired && reference === null) return null;
  const referenceMoveBps = reference?.recentMoveBps ?? null;
  const referenceVolatilityBps = reference?.recentVolatilityBps ?? null;
  const referenceStrength = referenceMoveBps === null
    ? 0
    : clamp(referenceMoveBps / config.referenceRapidMoveBps, -2, 2) / 2;
  const directionalScore = clamp(
    config.bookPressureWeight * bookPressure + config.referenceStrengthWeight * referenceStrength,
    -1,
    1,
  );
  const probabilityAdjustment = config.maxProbabilityAdjustment * directionalScore;
  const fairProbabilityUp = clamp(
    marketPriorUp + probabilityAdjustment,
    config.probabilityFloor,
    config.probabilityCeiling,
  );
  const direction: Direction = directionalScore > config.directionThreshold
    ? "UP"
    : directionalScore < -config.directionThreshold ? "DOWN" : "FLAT";

  let confidence = 0.5 + 0.25 * Math.abs(bookPressure) + 0.25 * Math.abs(referenceStrength);
  if (
    Math.abs(bookPressure) >= config.disagreementSignalThreshold
    && Math.abs(referenceStrength) >= config.disagreementSignalThreshold
    && Math.sign(bookPressure) !== Math.sign(referenceStrength)
  ) {
    confidence -= config.disagreementConfidencePenalty;
  }
  confidence = clamp(confidence, 0, 1);

  const pairAskCost = upBook.askPrice + downBook.askPrice;
  const featureVector: NativeFeatureVector = {
    upMid, downMid, marketPriorUp, upImbalance, downImbalance, bookPressure,
    referenceMoveBps, referenceVolatilityBps, referenceStrength, directionalScore,
    probabilityAdjustment, fairProbabilityUp,
    pairAskCost: Number.isFinite(pairAskCost) ? pairAskCost : null,
  };
  const fairProbability = direction === "DOWN" ? 1 - fairProbabilityUp : fairProbabilityUp;
  const marketProbability = direction === "DOWN" ? 1 - marketPriorUp : marketPriorUp;
  return {
    direction, confidence, fairProbability, marketProbability,
    expiresAt: input.trigger.expiresAt,
    evidenceIds: [...new Set([
      ...input.trigger.sourceEventIds,
      ...(input.trigger.evidenceIds ?? []),
      input.up.eventId,
      input.down.eventId,
    ])],
    featureVector,
  };
}

interface TopOfBook {
  bidPrice: number;
  askPrice: number;
  bidQuantity: number;
  askQuantity: number;
}

function topOfBook(snapshot: MarketSnapshot): TopOfBook | null {
  const bid = snapshot.bids[0];
  const ask = snapshot.asks[0];
  if (!bid || !ask) return null;
  const bidPrice = Number(bid.price);
  const askPrice = Number(ask.price);
  const bidQuantity = Number(bid.quantity);
  const askQuantity = Number(ask.quantity);
  if (![bidPrice, askPrice, bidQuantity, askQuantity].every(Number.isFinite)) return null;
  if (!probabilityValue(bidPrice) || !probabilityValue(askPrice)) return null;
  if (askPrice < bidPrice) return null;
  if (bidQuantity < 0 || askQuantity < 0 || bidQuantity + askQuantity <= 0) return null;
  return { bidPrice, askPrice, bidQuantity, askQuantity };
}

function imbalance(bidQuantity: number, askQuantity: number): number | null {
  const denominator = bidQuantity + askQuantity;
  return denominator > 0 ? clamp((bidQuantity - askQuantity) / denominator, -1, 1) : null;
}

function usableReference(reference: NativeReferenceFeatures | null): NativeReferenceFeatures | null {
  if (!reference || reference.freshness !== "fresh") return null;
  if (reference.freshnessMs === null || !Number.isFinite(reference.freshnessMs) || reference.freshnessMs < 0) return null;
  if (reference.recentMoveBps !== null && !Number.isFinite(reference.recentMoveBps)) return null;
  if (reference.recentVolatilityBps !== null && !Number.isFinite(reference.recentVolatilityBps)) return null;
  if (reference.recentMoveBps === null) return null;
  return reference;
}

function validDate(value: Date): boolean { return value instanceof Date && Number.isFinite(value.getTime()); }
function probabilityValue(value: number): boolean { return Number.isFinite(value) && value >= 0 && value <= 1; }
function clamp(value: number, min: number, max: number): number { return Math.min(max, Math.max(min, value)); }

function validConfig(config: NativeDecisionConfig): boolean {
  return [
    config.bookPressureWeight, config.referenceStrengthWeight, config.maxProbabilityAdjustment,
    config.probabilityFloor, config.probabilityCeiling, config.directionThreshold,
    config.disagreementSignalThreshold, config.disagreementConfidencePenalty,
    config.referenceRapidMoveBps,
  ].every(Number.isFinite)
    && config.maxProbabilityAdjustment >= 0
    && config.probabilityFloor >= 0 && config.probabilityFloor <= config.probabilityCeiling && config.probabilityCeiling <= 1
    && config.directionThreshold >= 0 && config.disagreementSignalThreshold >= 0
    && config.disagreementConfidencePenalty >= 0 && config.referenceRapidMoveBps > 0;
}
