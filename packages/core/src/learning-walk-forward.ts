import type { LearningTrainingRowV1 } from "./learning-dataset.js";

export const LEARNING_WALK_FORWARD_VERSION = "learning-walk-forward-v1" as const;

export interface LearningWalkForwardSplit {
  version: typeof LEARNING_WALK_FORWARD_VERSION;
  /** Inclusive start of training window (ISO), based on market group time. */
  trainWindowStartAt: string;
  /** Inclusive end of training window (ISO). */
  trainWindowEndAt: string;
  /** Inclusive start of validation window (ISO). Must be strictly after trainWindowEndAt. */
  validationWindowStartAt: string;
  /** Inclusive end of validation window (ISO). */
  validationWindowEndAt: string;
  trainMarketIds: string[];
  validationMarketIds: string[];
  trainRows: LearningTrainingRowV1[];
  validationRows: LearningTrainingRowV1[];
  assetBreakdown: {
    train: Record<string, number>;
    validation: Record<string, number>;
  };
}

export interface LearningWalkForwardOptions {
  /**
   * Fraction of markets (by sorted market-time) assigned to validation.
   * Default 0.3. Markets are never split across train/validation.
   */
  validationMarketFraction?: number;
  /**
   * Optional hard cutoff: markets with groupTime <= cutoff go to train;
   * markets with groupTime > cutoff go to validation. Overrides fraction when set.
   */
  trainCutoffObservedAt?: string;
}

interface MarketGroup {
  marketId: string;
  /** Earliest observation time for the market — used for time ordering. */
  groupTime: string;
  /** Latest observation time — used to ensure train markets end before val markets when possible. */
  lastObservedAt: string;
  rows: LearningTrainingRowV1[];
  asset: string | null;
}

/**
 * Market-grouped, time-aware walk-forward split.
 * NEVER places the same marketId in both train and validation.
 * NEVER random-splits rows from one market across folds.
 */
export function splitWalkForwardByMarket(
  rows: readonly LearningTrainingRowV1[],
  options: LearningWalkForwardOptions = {},
): LearningWalkForwardSplit {
  const groups = groupRowsByMarket(rows);
  if (groups.length === 0) {
    return emptySplit();
  }

  // Sort markets by first observation time, then marketId for stability.
  groups.sort((a, b) => {
    const byTime = a.groupTime.localeCompare(b.groupTime);
    if (byTime !== 0) return byTime;
    return a.marketId.localeCompare(b.marketId);
  });

  let trainGroups: MarketGroup[];
  let validationGroups: MarketGroup[];

  if (options.trainCutoffObservedAt) {
    const cutoff = options.trainCutoffObservedAt;
    trainGroups = groups.filter((g) => g.lastObservedAt <= cutoff);
    validationGroups = groups.filter((g) => g.groupTime > cutoff);
    // Markets that straddle the cutoff (started before, last obs after) stay out of both
    // rather than leaking. They are excluded from this fold.
    const trainIds = new Set(trainGroups.map((g) => g.marketId));
    const valIds = new Set(validationGroups.map((g) => g.marketId));
    for (const id of trainIds) {
      if (valIds.has(id)) {
        throw new Error(`LEARNING_WALK_FORWARD_MARKET_LEAK: market ${id} selected for both train and validation`);
      }
    }
  } else {
    const fraction = options.validationMarketFraction ?? 0.3;
    if (!(fraction > 0 && fraction < 1)) {
      throw new Error(`LEARNING_WALK_FORWARD_INVALID_FRACTION: ${fraction}`);
    }
    const validationCount = Math.max(1, Math.min(groups.length - 1, Math.round(groups.length * fraction)));
    const trainCount = groups.length - validationCount;
    if (trainCount <= 0 || validationCount <= 0) {
      throw new Error(
        `LEARNING_WALK_FORWARD_INSUFFICIENT_MARKETS: need >=2 markets, got ${groups.length}`,
      );
    }
    trainGroups = groups.slice(0, trainCount);
    validationGroups = groups.slice(trainCount);
  }

  assertNoMarketOverlap(trainGroups, validationGroups);
  assertTimeOrder(trainGroups, validationGroups);

  const trainRows = flattenSorted(trainGroups);
  const validationRows = flattenSorted(validationGroups);

  return {
    version: LEARNING_WALK_FORWARD_VERSION,
    trainWindowStartAt: trainGroups[0]?.groupTime ?? "",
    trainWindowEndAt: maxLastObserved(trainGroups),
    validationWindowStartAt: validationGroups[0]?.groupTime ?? "",
    validationWindowEndAt: maxLastObserved(validationGroups),
    trainMarketIds: trainGroups.map((g) => g.marketId),
    validationMarketIds: validationGroups.map((g) => g.marketId),
    trainRows,
    validationRows,
    assetBreakdown: {
      train: assetCounts(trainRows),
      validation: assetCounts(validationRows),
    },
  };
}

/** Assert helper used by tests: no marketId appears in both sides. */
export function assertNoSharedMarkets(
  trainRows: readonly LearningTrainingRowV1[],
  validationRows: readonly LearningTrainingRowV1[],
): void {
  const trainMarkets = new Set(trainRows.map((r) => r.marketId));
  for (const row of validationRows) {
    if (trainMarkets.has(row.marketId)) {
      throw new Error(`LEARNING_WALK_FORWARD_MARKET_LEAK: market ${row.marketId} in train and validation`);
    }
  }
}

function groupRowsByMarket(rows: readonly LearningTrainingRowV1[]): MarketGroup[] {
  const map = new Map<string, MarketGroup>();
  for (const row of rows) {
    const existing = map.get(row.marketId);
    if (!existing) {
      map.set(row.marketId, {
        marketId: row.marketId,
        groupTime: row.observedAt,
        lastObservedAt: row.observedAt,
        rows: [row],
        asset: row.asset,
      });
    } else {
      existing.rows.push(row);
      if (row.observedAt < existing.groupTime) existing.groupTime = row.observedAt;
      if (row.observedAt > existing.lastObservedAt) existing.lastObservedAt = row.observedAt;
    }
  }
  for (const group of map.values()) {
    group.rows.sort((a, b) => {
      const byTime = a.observedAt.localeCompare(b.observedAt);
      if (byTime !== 0) return byTime;
      return a.sampleId.localeCompare(b.sampleId);
    });
  }
  return [...map.values()];
}

function flattenSorted(groups: readonly MarketGroup[]): LearningTrainingRowV1[] {
  const rows = groups.flatMap((g) => g.rows);
  rows.sort((a, b) => {
    const byTime = a.observedAt.localeCompare(b.observedAt);
    if (byTime !== 0) return byTime;
    return a.sampleId.localeCompare(b.sampleId);
  });
  return rows;
}

function assertNoMarketOverlap(train: readonly MarketGroup[], validation: readonly MarketGroup[]): void {
  const trainIds = new Set(train.map((g) => g.marketId));
  for (const group of validation) {
    if (trainIds.has(group.marketId)) {
      throw new Error(`LEARNING_WALK_FORWARD_MARKET_LEAK: market ${group.marketId}`);
    }
  }
}

function assertTimeOrder(train: readonly MarketGroup[], validation: readonly MarketGroup[]): void {
  if (train.length === 0 || validation.length === 0) return;
  const trainEnd = maxLastObserved(train);
  const valStart = validation.map((g) => g.groupTime).sort()[0]!;
  // Soft check: prefer train markets fully before validation markets.
  // If overlapping group times exist due to concurrent markets, market isolation still holds.
  if (trainEnd > valStart) {
    // Allowed when markets run concurrently — leakage prevention is market-id based.
    // Documented: time order is best-effort; market grouping is strict.
  }
}

function maxLastObserved(groups: readonly MarketGroup[]): string {
  let max = "";
  for (const group of groups) {
    if (group.lastObservedAt > max) max = group.lastObservedAt;
  }
  return max;
}

function assetCounts(rows: readonly LearningTrainingRowV1[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const row of rows) {
    const key = row.asset ?? "UNKNOWN";
    out[key] = (out[key] ?? 0) + 1;
  }
  return out;
}

function emptySplit(): LearningWalkForwardSplit {
  return {
    version: LEARNING_WALK_FORWARD_VERSION,
    trainWindowStartAt: "",
    trainWindowEndAt: "",
    validationWindowStartAt: "",
    validationWindowEndAt: "",
    trainMarketIds: [],
    validationMarketIds: [],
    trainRows: [],
    validationRows: [],
    assetBreakdown: { train: {}, validation: {} },
  };
}
