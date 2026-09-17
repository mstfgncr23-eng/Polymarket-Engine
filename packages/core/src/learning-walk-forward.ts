import type { LearningTrainingRowV1 } from "./learning-dataset.js";

export const LEARNING_WALK_FORWARD_VERSION = "learning-walk-forward-v1" as const;

export interface LearningWalkForwardSplit {
  version: typeof LEARNING_WALK_FORWARD_VERSION;
  /** Model freeze / data-availability boundary used for this fold. */
  trainingCutoffAt: string;
  /** Inclusive start of training observation window (ISO), based on market group time. */
  trainWindowStartAt: string;
  /** Inclusive end of training observation window (ISO). */
  trainWindowEndAt: string;
  /** Inclusive start of validation observation window (ISO). Strictly after all admitted train observations/labels. */
  validationWindowStartAt: string;
  /** Inclusive end of validation observation window (ISO). */
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
   * Fraction of markets (by sorted market-time) reserved for validation.
   * Default 0.3. Markets are never split across train/validation.
   */
  validationMarketFraction?: number;
  /**
   * Optional hard model-freeze cutoff. Training markets must be fully observed
   * AND have authoritative labels available by the cutoff; validation markets
   * must start after it. Overrides fraction when set.
   */
  trainCutoffObservedAt?: string;
}

export interface LearningCalibrationEvaluationSplit {
  calibrationMarketIds: string[];
  evaluationMarketIds: string[];
  calibrationRows: LearningTrainingRowV1[];
  evaluationRows: LearningTrainingRowV1[];
  evaluationWindowStartAt: string;
}

interface MarketGroup {
  marketId: string;
  /** Earliest observation time for the market — used for time ordering. */
  groupTime: string;
  /** Latest observation time for the market. */
  lastObservedAt: string;
  /** Latest authoritative label timestamp among rows for this market. */
  labelAvailableAt: string;
  rows: LearningTrainingRowV1[];
  asset: string | null;
}

/**
 * Market-grouped, label-availability-aware walk-forward split.
 *
 * Invariant: every admitted training market is fully observed and has its
 * authoritative settlement label available before validation begins. Merely
 * observing a market earlier is not enough: using a label that arrived later
 * would leak future information into historical validation.
 */
export function splitWalkForwardByMarket(
  rows: readonly LearningTrainingRowV1[],
  options: LearningWalkForwardOptions = {},
): LearningWalkForwardSplit {
  const groups = groupRowsByMarket(rows);
  if (groups.length === 0) return emptySplit();

  groups.sort(compareMarketGroups);

  let trainingCutoffAt: string;
  let trainGroups: MarketGroup[];
  let validationGroups: MarketGroup[];

  if (options.trainCutoffObservedAt) {
    const cutoff = requireIso(options.trainCutoffObservedAt, "LEARNING_WALK_FORWARD_INVALID_CUTOFF");
    trainingCutoffAt = cutoff;
    trainGroups = groups.filter((g) => g.lastObservedAt <= cutoff && g.labelAvailableAt <= cutoff);
    validationGroups = groups.filter((g) => g.groupTime > cutoff);
    // Groups that straddle the cutoff or whose labels arrived after it are
    // intentionally excluded from this fold rather than leaked into training.
  } else {
    const fraction = options.validationMarketFraction ?? 0.3;
    if (!(fraction > 0 && fraction < 1)) {
      throw new Error(`LEARNING_WALK_FORWARD_INVALID_FRACTION: ${fraction}`);
    }
    if (groups.length < 2) {
      throw new Error(`LEARNING_WALK_FORWARD_INSUFFICIENT_MARKETS: need >=2 markets, got ${groups.length}`);
    }

    const validationCount = Math.max(1, Math.min(groups.length - 1, Math.round(groups.length * fraction)));
    const trainCount = groups.length - validationCount;
    validationGroups = groups.slice(trainCount);
    const validationStart = validationGroups[0]!.groupTime;
    trainingCutoffAt = validationStart;

    // Preserve chronological market selection, then admit only markets whose
    // observations AND labels were available before the validation boundary.
    trainGroups = groups.slice(0, trainCount).filter((g) =>
      g.lastObservedAt < validationStart && g.labelAvailableAt < validationStart
    );
  }

  if (trainGroups.length === 0 || validationGroups.length === 0) {
    throw new Error(
      `LEARNING_WALK_FORWARD_INSUFFICIENT_LEAKAGE_SAFE_MARKETS: train=${trainGroups.length} validation=${validationGroups.length}`,
    );
  }

  assertNoMarketOverlap(trainGroups, validationGroups);
  assertStrictHistoricalAvailability(trainGroups, validationGroups);

  const trainRows = flattenSorted(trainGroups);
  const validationRows = flattenSorted(validationGroups);

  return {
    version: LEARNING_WALK_FORWARD_VERSION,
    trainingCutoffAt,
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

/**
 * Split an already-held-out validation set into calibration and final scoring
 * markets. Calibration is optional: return null when a leakage-safe market/time
 * boundary cannot be formed. The same market can never contribute a label to
 * calibration and then be scored by the calibrated model.
 */
export function splitCalibrationEvaluationByMarket(
  rows: readonly LearningTrainingRowV1[],
  calibrationMarketFraction = 0.3,
): LearningCalibrationEvaluationSplit | null {
  if (!(calibrationMarketFraction > 0 && calibrationMarketFraction < 1)) {
    throw new Error(`LEARNING_CALIBRATION_INVALID_FRACTION: ${calibrationMarketFraction}`);
  }

  const groups = groupRowsByMarket(rows).sort(compareMarketGroups);
  if (groups.length < 2) return null;

  const calibrationCount = Math.max(
    1,
    Math.min(groups.length - 1, Math.floor(groups.length * calibrationMarketFraction)),
  );
  const candidateCalibration = groups.slice(0, calibrationCount);
  const evaluationGroups = groups.slice(calibrationCount);
  if (evaluationGroups.length === 0) return null;

  const evaluationStart = evaluationGroups[0]!.groupTime;
  const calibrationGroups = candidateCalibration.filter((g) =>
    g.lastObservedAt < evaluationStart && g.labelAvailableAt < evaluationStart
  );
  if (calibrationGroups.length === 0) return null;

  assertNoMarketOverlap(calibrationGroups, evaluationGroups);
  assertStrictHistoricalAvailability(calibrationGroups, evaluationGroups);

  return {
    calibrationMarketIds: calibrationGroups.map((g) => g.marketId),
    evaluationMarketIds: evaluationGroups.map((g) => g.marketId),
    calibrationRows: flattenSorted(calibrationGroups),
    evaluationRows: flattenSorted(evaluationGroups),
    evaluationWindowStartAt: evaluationStart,
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
    const observedAt = requireIso(row.observedAt, `LEARNING_WALK_FORWARD_INVALID_OBSERVED_AT:${row.sampleId}`);
    const labeledAt = requireIso(
      row.labeledAt,
      `LEARNING_WALK_FORWARD_MISSING_LABEL_AVAILABLE_AT:${row.sampleId}`,
    );

    const existing = map.get(row.marketId);
    if (!existing) {
      map.set(row.marketId, {
        marketId: row.marketId,
        groupTime: observedAt,
        lastObservedAt: observedAt,
        labelAvailableAt: labeledAt,
        rows: [row],
        asset: row.asset,
      });
    } else {
      existing.rows.push(row);
      if (observedAt < existing.groupTime) existing.groupTime = observedAt;
      if (observedAt > existing.lastObservedAt) existing.lastObservedAt = observedAt;
      if (labeledAt > existing.labelAvailableAt) existing.labelAvailableAt = labeledAt;
    }
  }

  for (const group of map.values()) {
    group.rows.sort(compareTrainingRows);
  }
  return [...map.values()];
}

function flattenSorted(groups: readonly MarketGroup[]): LearningTrainingRowV1[] {
  const rows = groups.flatMap((g) => g.rows);
  rows.sort(compareTrainingRows);
  return rows;
}

function compareTrainingRows(a: LearningTrainingRowV1, b: LearningTrainingRowV1): number {
  const byTime = a.observedAt.localeCompare(b.observedAt);
  return byTime !== 0 ? byTime : a.sampleId.localeCompare(b.sampleId);
}

function compareMarketGroups(a: MarketGroup, b: MarketGroup): number {
  const byTime = a.groupTime.localeCompare(b.groupTime);
  return byTime !== 0 ? byTime : a.marketId.localeCompare(b.marketId);
}

function assertNoMarketOverlap(train: readonly MarketGroup[], validation: readonly MarketGroup[]): void {
  const trainIds = new Set(train.map((g) => g.marketId));
  for (const group of validation) {
    if (trainIds.has(group.marketId)) {
      throw new Error(`LEARNING_WALK_FORWARD_MARKET_LEAK: market ${group.marketId}`);
    }
  }
}

function assertStrictHistoricalAvailability(
  earlier: readonly MarketGroup[],
  later: readonly MarketGroup[],
): void {
  if (earlier.length === 0 || later.length === 0) return;
  const laterStart = later.map((g) => g.groupTime).sort()[0]!;
  const observationEnd = maxLastObserved(earlier);
  const labelEnd = maxLabelAvailable(earlier);
  if (observationEnd >= laterStart) {
    throw new Error(
      `LEARNING_WALK_FORWARD_TEMPORAL_LEAK: earlierObservationEnd=${observationEnd} laterStart=${laterStart}`,
    );
  }
  if (labelEnd >= laterStart) {
    throw new Error(
      `LEARNING_WALK_FORWARD_LABEL_LEAK: labelAvailableAt=${labelEnd} laterStart=${laterStart}`,
    );
  }
}

function maxLastObserved(groups: readonly MarketGroup[]): string {
  let max = "";
  for (const group of groups) if (group.lastObservedAt > max) max = group.lastObservedAt;
  return max;
}

function maxLabelAvailable(groups: readonly MarketGroup[]): string {
  let max = "";
  for (const group of groups) if (group.labelAvailableAt > max) max = group.labelAvailableAt;
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

function requireIso(value: string | null | undefined, code: string): string {
  if (!value || !Number.isFinite(Date.parse(value))) throw new Error(code);
  return value;
}

function emptySplit(): LearningWalkForwardSplit {
  return {
    version: LEARNING_WALK_FORWARD_VERSION,
    trainingCutoffAt: "",
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
