export const LEARNING_METRICS_VERSION = "learning-metrics-v1" as const;

export interface ProbabilityScoreRow {
  probabilityUp: number;
  labelUp: 0 | 1;
}

export interface CalibrationBucket {
  bucketIndex: number;
  /** Inclusive lower bound of predicted probability. */
  lower: number;
  /** Exclusive upper bound (inclusive for last bucket). */
  upper: number;
  count: number;
  meanPredicted: number | null;
  meanObserved: number | null;
}

export interface LearningProbabilityMetrics {
  version: typeof LEARNING_METRICS_VERSION;
  n: number;
  brier: number;
  logLoss: number;
  calibrationBuckets: CalibrationBucket[];
}

export interface FitIsotonicCalibrationResult {
  /** Piecewise-constant map: predicted -> calibrated, fit only on provided rows. */
  breakpoints: Array<{ x: number; y: number }>;
}

/**
 * Deterministic Brier / log-loss / calibration buckets.
 * Uses fixed float rounding to keep metrics stable across runs.
 */
export function scoreProbabilityMetrics(
  rows: readonly ProbabilityScoreRow[],
  bucketCount = 10,
): LearningProbabilityMetrics {
  if (!Number.isInteger(bucketCount) || bucketCount <= 0) {
    throw new Error(`LEARNING_METRICS_INVALID_BUCKET_COUNT: ${bucketCount}`);
  }
  const n = rows.length;
  if (n === 0) {
    return {
      version: LEARNING_METRICS_VERSION,
      n: 0,
      brier: 0,
      logLoss: 0,
      calibrationBuckets: emptyBuckets(bucketCount),
    };
  }

  let brierSum = 0;
  let logLossSum = 0;
  const bucketSums = Array.from({ length: bucketCount }, () => ({
    count: 0,
    predSum: 0,
    obsSum: 0,
  }));

  for (const row of rows) {
    const p = clampProbability(row.probabilityUp);
    const y = row.labelUp;
    const err = p - y;
    brierSum += err * err;
    logLossSum += -(y === 1 ? Math.log(p) : Math.log(1 - p));
    const idx = bucketIndex(p, bucketCount);
    const bucket = bucketSums[idx]!;
    bucket.count += 1;
    bucket.predSum += p;
    bucket.obsSum += y;
  }

  return {
    version: LEARNING_METRICS_VERSION,
    n,
    brier: roundMetric(brierSum / n),
    logLoss: roundMetric(logLossSum / n),
    calibrationBuckets: bucketSums.map((bucket, bucketIndex_) => {
      const lower = bucketIndex_ / bucketCount;
      const upper = (bucketIndex_ + 1) / bucketCount;
      return {
        bucketIndex: bucketIndex_,
        lower: roundMetric(lower),
        upper: roundMetric(upper),
        count: bucket.count,
        meanPredicted: bucket.count > 0 ? roundMetric(bucket.predSum / bucket.count) : null,
        meanObserved: bucket.count > 0 ? roundMetric(bucket.obsSum / bucket.count) : null,
      };
    }),
  };
}

/**
 * Fit a simple isotonic (PAV) calibrator on held-out rows only.
 * Never score the same rows used to fit.
 */
export function fitIsotonicCalibrator(
  rows: readonly ProbabilityScoreRow[],
): FitIsotonicCalibrationResult {
  if (rows.length === 0) return { breakpoints: [] };
  const sorted = rows
    .map((row) => ({ x: clampProbability(row.probabilityUp), y: row.labelUp as number }))
    .sort((a, b) => a.x - b.x || a.y - b.y);

  // Pool Adjacent Violators
  type Block = { sumX: number; sumY: number; n: number };
  const blocks: Block[] = [];
  for (const point of sorted) {
    blocks.push({ sumX: point.x, sumY: point.y, n: 1 });
    while (blocks.length >= 2) {
      const b = blocks[blocks.length - 1]!;
      const a = blocks[blocks.length - 2]!;
      const meanA = a.sumY / a.n;
      const meanB = b.sumY / b.n;
      if (meanA <= meanB) break;
      blocks[blocks.length - 2] = {
        sumX: a.sumX + b.sumX,
        sumY: a.sumY + b.sumY,
        n: a.n + b.n,
      };
      blocks.pop();
    }
  }

  const breakpoints: Array<{ x: number; y: number }> = [];
  for (const block of blocks) {
    breakpoints.push({
      x: roundMetric(block.sumX / block.n),
      y: roundMetric(clampProbability(block.sumY / block.n)),
    });
  }
  return { breakpoints };
}

export function applyIsotonicCalibrator(
  probabilityUp: number,
  calibrator: FitIsotonicCalibrationResult,
): number {
  const p = clampProbability(probabilityUp);
  const bps = calibrator.breakpoints;
  if (bps.length === 0) return p;
  if (p <= bps[0]!.x) return bps[0]!.y;
  for (let i = 1; i < bps.length; i += 1) {
    const prev = bps[i - 1]!;
    const cur = bps[i]!;
    if (p <= cur.x) {
      if (cur.x === prev.x) return cur.y;
      const t = (p - prev.x) / (cur.x - prev.x);
      return clampProbability(prev.y + t * (cur.y - prev.y));
    }
  }
  return bps[bps.length - 1]!.y;
}

function bucketIndex(probability: number, bucketCount: number): number {
  if (probability >= 1) return bucketCount - 1;
  return Math.min(bucketCount - 1, Math.max(0, Math.floor(probability * bucketCount)));
}

function emptyBuckets(bucketCount: number): CalibrationBucket[] {
  return Array.from({ length: bucketCount }, (_, bucketIndex_) => ({
    bucketIndex: bucketIndex_,
    lower: roundMetric(bucketIndex_ / bucketCount),
    upper: roundMetric((bucketIndex_ + 1) / bucketCount),
    count: 0,
    meanPredicted: null,
    meanObserved: null,
  }));
}

function clampProbability(value: number): number {
  if (!Number.isFinite(value)) return 0.5;
  // Avoid log(0)
  return Math.min(1 - 1e-15, Math.max(1e-15, value));
}

function roundMetric(value: number): number {
  return Math.round(value * 1e12) / 1e12;
}
