import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  LEARNING_SAMPLE_SCHEMA_VERSION,
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  assertNoSharedMarkets,
  buildLearningDataset,
  buildLearningSampleV1,
  checksumTrainingRows,
  evaluateLearningEconomics,
  evaluateNativeV2ShadowFairProbability,
  fitResidualLogisticModel,
  predictResidualProbabilityUp,
  runLearningOfflinePipeline,
  scoreBaselines,
  scoreProbabilityMetrics,
  splitWalkForwardByMarket,
  verifyLearningCandidateChecksum,
  type LearningSampleV1,
  type NativeV2ShadowFeatureVector,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-learning-pr4-"));
  dirs.push(dir);
  return dir;
}

function vector(overrides: {
  marketId?: string;
  timeToExpiryMs?: number | null;
  synchronized?: boolean;
} = {}): NativeV2ShadowFeatureVector {
  const book = (microprice: number) => ({
    mid: 0.5,
    spread: 0.02,
    spreadBps: 400,
    microprice,
    micropriceOffsetBps: 0,
    imbalanceL1: 0,
    imbalanceL3: 0,
    imbalanceL5: 0,
    bidDepthUsdL5: 100,
    askDepthUsdL5: 100,
    depthAsymmetryL5: 0,
    bidDepthSlope: 0,
    askDepthSlope: 0,
    depthSlopeAsymmetry: 0,
    bidLiquidityConcentrationL1InL5: 0.2,
    askLiquidityConcentrationL1InL5: 0.2,
    spreadVelocityBpsPerSecond: null,
    midVelocityBpsPerSecond: null,
    sourceTimestamp: "2026-09-16T17:00:00.000Z",
    receivedAt: "2026-09-16T17:00:00.000Z",
    sequence: 1,
  });

  return {
    version: NATIVE_V2_SHADOW_FEATURE_VERSION,
    marketId: overrides.marketId ?? "m1",
    asset: "BTC",
    observedAt: "2026-09-16T17:00:01.000Z",
    up: book(0.55),
    down: book(0.45),
    pair: {
      marketPriorUp: 0.55,
      pairAskCost: 1.02,
      binaryComplementBasis: 0,
      bookPressureL1: 0.2,
      bookPressureL3: 0.2,
      bookPressureL5: 0.2,
      orderPressureVelocityPerSecond: 0.1,
      signalDirection: 1,
      signalPersistenceMs: 2_000,
      signalPersistenceSamples: 3,
    },
    reference: {
      latestPrice: 100,
      shortMomentumBps: 5,
      mediumMomentumBps: 10,
      realizedVolatilityBps: 10,
      volatilityNormalizedMovement: 0.5,
      sourceTimestamp: "2026-09-16T17:00:00.000Z",
      sourceAgeMs: 1_000,
      marketReferenceSourceSkewMs: 0,
    },
    timing: {
      timeToExpiryMs: overrides.timeToExpiryMs === undefined ? 60_000 : overrides.timeToExpiryMs,
      upSourceAgeMs: 1_000,
      downSourceAgeMs: 1_000,
      pairSourceSkewMs: 0,
    },
    synchronization: {
      marketSynchronized: overrides.synchronized ?? true,
      referenceFreshness: "fresh",
      referenceFreshnessMs: 1_000,
    },
    eventFlow: {
      available: false,
      reason: "CLOB_TRANSPORT_LACKS_ADD_CANCEL_TRADE_DISCRIMINATION",
      windowMs: null,
      orderFlowImbalance: null,
      bidAddImbalance: null,
      askAddImbalance: null,
      bidCancelImbalance: null,
      askCancelImbalance: null,
      touchDepletion: null,
      touchReplenishment: null,
      aggressiveBuyVolume: null,
      aggressiveSellVolume: null,
      eventArrivalRatePerSecond: null,
      ofiSlope: null,
      ofiVelocity: null,
    },
    resolutionAligned: {
      timeToExpiryMs: 60_000,
      invSqrtTimeToExpiryMinutes: 1,
      bookPressureL5xInvSqrtTte: 0,
      shortMomentumBpsxInvSqrtTte: 0,
      volNormMovementxInvSqrtTte: 0,
      upMidMinusMarketPrior: 0,
      downMidMinusMarketPriorComplement: 0,
      binaryComplementBasis: 0,
      referenceStrikeDistanceBps: null,
      referenceStrikeDistanceAvailable: false,
      referenceVenue: "binance",
      referenceSymbol: "BTCUSDT",
      referenceSourceAgeMs: 1_000,
      marketReferenceSourceSkewMs: 0,
      referenceFreshness: "fresh",
      marketSynchronized: true,
    },
    crossVenueOracle: {
      binance: {
        available: true,
        price: 100,
        sourceAgeMs: 1_000,
        symbol: "BTCUSDT",
        venue: "binance",
      },
      chainlink: {
        available: false,
        price: null,
        sourceAgeMs: null,
        reason: "NO_EXISTING_INTEGRATION",
      },
      coinbase: {
        available: false,
        price: null,
        sourceAgeMs: null,
        reason: "NO_EXISTING_INTEGRATION",
      },
    },
  };
}


function makeResolvedSample(input: {
  marketId: string;
  checkpointSeconds: number;
  observedAt: string;
  asset?: string | null;
  outcome: "UP" | "DOWN";
  marketPriorUp?: number;
}): LearningSampleV1 {
  const features = vector({
    marketId: input.marketId,
    timeToExpiryMs: input.checkpointSeconds * 1000,
    synchronized: true,
  });
  if (input.marketPriorUp !== undefined) {
    features.pair.marketPriorUp = input.marketPriorUp;
    features.resolutionAligned.upMidMinusMarketPrior = features.up.mid - input.marketPriorUp;
  }
  features.observedAt = input.observedAt;
  features.asset = input.asset ?? "BTC";
  const fair = evaluateNativeV2ShadowFairProbability(features);
  if (!fair) throw new Error("fair probability unavailable in fixture");
  const sample = buildLearningSampleV1({
    marketId: input.marketId,
    checkpointId: `T-${input.checkpointSeconds}`,
    checkpointSeconds: input.checkpointSeconds,
    asset: input.asset ?? "BTC",
    observedAt: input.observedAt,
    timeToExpiryMs: input.checkpointSeconds * 1000,
    featureVector: features,
    v2FairProbability: fair,
    experimentId: "pr4-test",
    gitSha: "testsha",
  });
  sample.settlement = {
    status: "resolved",
    outcome: input.outcome,
    labeledAt: input.observedAt,
    source: "test",
    reason: "RESOLVED",
  };
  return sample;
}

function fixtureSamples(): LearningSampleV1[] {
  const specs: Array<{
    marketId: string;
    observedAt: string;
    outcome: "UP" | "DOWN";
    marketPriorUp: number;
    asset: string;
    checkpoints: number[];
  }> = [
    { marketId: "m-early-1", observedAt: "2026-09-01T10:00:00.000Z", outcome: "UP", marketPriorUp: 0.62, asset: "BTC", checkpoints: [60, 30] },
    { marketId: "m-early-2", observedAt: "2026-09-01T11:00:00.000Z", outcome: "DOWN", marketPriorUp: 0.38, asset: "BTC", checkpoints: [60, 30] },
    { marketId: "m-mid-1", observedAt: "2026-09-02T10:00:00.000Z", outcome: "UP", marketPriorUp: 0.7, asset: "ETH", checkpoints: [60, 30] },
    { marketId: "m-mid-2", observedAt: "2026-09-02T12:00:00.000Z", outcome: "DOWN", marketPriorUp: 0.32, asset: "ETH", checkpoints: [60, 30] },
    { marketId: "m-late-1", observedAt: "2026-09-03T10:00:00.000Z", outcome: "UP", marketPriorUp: 0.65, asset: "BTC", checkpoints: [60, 30] },
    { marketId: "m-late-2", observedAt: "2026-09-03T11:00:00.000Z", outcome: "DOWN", marketPriorUp: 0.35, asset: "SOL", checkpoints: [60, 30] },
  ];
  const out: LearningSampleV1[] = [];
  for (const spec of specs) {
    for (const seconds of spec.checkpoints) {
      out.push(makeResolvedSample({
        marketId: spec.marketId,
        checkpointSeconds: seconds,
        observedAt: spec.observedAt,
        asset: spec.asset,
        outcome: spec.outcome,
        marketPriorUp: spec.marketPriorUp,
      }));
    }
  }
  return out;
}

describe("PR4 learning dataset builder", () => {
  it("keeps only resolved samples with market/checkpoint identity and deterministic ordering", () => {
    const samples = fixtureSamples();
    const unresolved = buildLearningSampleV1({
      marketId: "m-open",
      checkpointId: "T-60",
      checkpointSeconds: 60,
      asset: "BTC",
      observedAt: "2026-09-04T10:00:00.000Z",
      timeToExpiryMs: 60_000,
      featureVector: vector({ marketId: "m-open" }),
      v2FairProbability: evaluateNativeV2ShadowFairProbability(vector({ marketId: "m-open" }))!,
    });
    const result = buildLearningDataset([...samples, unresolved], {
      gitSha: "abc",
      now: new Date("2026-09-17T08:00:00.000Z"),
    });
    expect(result.rows.every((row) => row.settlementOutcome === "UP" || row.settlementOutcome === "DOWN")).toBe(true);
    expect(result.rows.some((row) => row.marketId === "m-open")).toBe(false);
    expect(result.manifest.rowCount).toBe(samples.length);
    expect(result.manifest.artifactFormat).toBe("jsonl");
    expect(result.manifest.featureSchemaVersion).toBe(NATIVE_V2_SHADOW_FEATURE_VERSION);
    expect(result.manifest.sampleSchemaVersion).toBe(LEARNING_SAMPLE_SCHEMA_VERSION);
    for (let i = 1; i < result.rows.length; i += 1) {
      const prev = result.rows[i - 1]!;
      const cur = result.rows[i]!;
      expect(prev.observedAt <= cur.observedAt).toBe(true);
      if (prev.observedAt === cur.observedAt) {
        expect(prev.sampleId <= cur.sampleId).toBe(true);
      }
    }
    const keys = new Set(result.rows.map((r) => `${r.marketId}::${r.checkpointId}`));
    expect(keys.size).toBe(result.rows.length);
    expect(checksumTrainingRows(result.rows)).toBe(result.manifest.rowsChecksumSha256);
    expect(checksumTrainingRows(result.rows)).toBe(checksumTrainingRows(result.rows));
  });

  it("fails clearly on incompatible feature schema", () => {
    const sample = fixtureSamples()[0]!;
    (sample as { featureSchemaVersion: string }).featureSchemaVersion = "native-v2-shadow-features-v1";
    expect(() => buildLearningDataset([sample])).toThrow(/LEARNING_DATASET_INCOMPATIBLE_FEATURE_SCHEMA|INCOMPATIBLE_FEATURE/);
  });

  it("caps busy-market dominance via maxRowsPerMarket", () => {
    const busy: LearningSampleV1[] = [];
    for (const seconds of [240, 180, 120, 60, 30, 15, 5]) {
      busy.push(makeResolvedSample({
        marketId: "busy",
        checkpointSeconds: seconds,
        observedAt: "2026-09-01T09:00:00.000Z",
        outcome: "UP",
        marketPriorUp: 0.6,
      }));
    }
    const other = makeResolvedSample({
      marketId: "quiet",
      checkpointSeconds: 60,
      observedAt: "2026-09-01T09:30:00.000Z",
      outcome: "DOWN",
      marketPriorUp: 0.4,
    });
    const result = buildLearningDataset([...busy, other], { maxRowsPerMarket: 2 });
    expect(result.rows.filter((r) => r.marketId === "busy")).toHaveLength(2);
    expect(result.rows.filter((r) => r.marketId === "quiet")).toHaveLength(1);
    expect(result.manifest.maxRowsPerMarket).toBe(2);
  });
});

describe("PR4 walk-forward leakage prevention", () => {
  it("never places the same market in train and validation", () => {
    const { rows } = buildLearningDataset(fixtureSamples());
    const split = splitWalkForwardByMarket(rows, { validationMarketFraction: 0.34 });
    expect(split.trainMarketIds.length).toBeGreaterThan(0);
    expect(split.validationMarketIds.length).toBeGreaterThan(0);
    const trainSet = new Set(split.trainMarketIds);
    for (const id of split.validationMarketIds) {
      expect(trainSet.has(id)).toBe(false);
    }
    assertNoSharedMarkets(split.trainRows, split.validationRows);
    for (const row of split.trainRows) {
      expect(split.validationMarketIds.includes(row.marketId)).toBe(false);
    }
    for (const row of split.validationRows) {
      expect(split.trainMarketIds.includes(row.marketId)).toBe(false);
    }
  });

  it("supports explicit time cutoff without market straddling leakage", () => {
    const { rows } = buildLearningDataset(fixtureSamples());
    const split = splitWalkForwardByMarket(rows, {
      trainCutoffObservedAt: "2026-09-02T12:00:00.000Z",
    });
    for (const row of split.trainRows) {
      expect(row.observedAt <= "2026-09-02T12:00:00.000Z").toBe(true);
    }
    for (const row of split.validationRows) {
      expect(row.observedAt > "2026-09-02T12:00:00.000Z").toBe(true);
    }
    assertNoSharedMarkets(split.trainRows, split.validationRows);
  });
});

describe("PR4 residual model + baselines + metrics", () => {
  it("fits inspectable residual logistic and scores all three baselines deterministically", () => {
    const { rows } = buildLearningDataset(fixtureSamples());
    const split = splitWalkForwardByMarket(rows, { validationMarketFraction: 0.34 });
    const model = fitResidualLogisticModel(split.trainRows, { maxIterations: 200, l2Lambda: 0.01 });
    expect(model.modelType).toBe("residual_logistic");
    expect(model.featureNames.length).toBe(model.weights.length);
    expect(model.trainRowCount).toBe(split.trainRows.length);

    const scores = scoreBaselines(split.validationRows, model);
    expect(scores.marketPrior.length).toBe(split.validationRows.length);
    expect(scores.deterministicV2.length).toBe(split.validationRows.length);
    expect(scores.residualLearned.length).toBe(split.validationRows.length);

    const m1 = scoreProbabilityMetrics(scores.residualLearned);
    const m2 = scoreProbabilityMetrics(scores.residualLearned);
    expect(m1).toEqual(m2);
    expect(m1.n).toBe(split.validationRows.length);
    expect(Number.isFinite(m1.brier)).toBe(true);
    expect(Number.isFinite(m1.logLoss)).toBe(true);
    expect(m1.calibrationBuckets.length).toBe(10);

    const row = split.validationRows[0]!;
    const p = predictResidualProbabilityUp(row, model);
    expect(p).toBeGreaterThan(0);
    expect(p).toBeLessThan(1);
  });
});

describe("PR4 economic evaluation + candidate artifact", () => {
  it("reports available approx-edge metrics and explicit unavailable fee/VWAP/CF fields", () => {
    const { rows } = buildLearningDataset(fixtureSamples());
    const probs = rows.map((r) => r.v2FairProbabilityUp);
    const evaluation = evaluateLearningEconomics(rows, probs, {
      minExpectedGrossEdge: 0.0,
      assumedFeePerShare: 0.01,
    });
    expect(evaluation.capabilities.approximateExecutablePriceFromMidSpread).toBe(true);
    expect(evaluation.capabilities.feeScheduleOnSample).toBe(false);
    expect(evaluation.capabilities.vwapDepthWalkOnSample).toBe(false);
    expect(evaluation.capabilities.counterfactualLedgerJoin).toBe(false);
    expect(evaluation.unavailable.length).toBeGreaterThan(0);
    expect(evaluation.nRows).toBe(rows.length);
  });

  it("writes reproducible dataset + candidate artifacts with stable checksums", () => {
    const samples = fixtureSamples();
    const outA = tempDir();
    const outB = tempDir();
    const shared = {
      gitSha: "deadbeef",
      now: new Date("2026-09-17T08:00:00.000Z"),
      walkForward: { validationMarketFraction: 0.34 },
      residual: { maxIterations: 150 },
      economics: { minExpectedGrossEdge: 0.0, assumedFeePerShare: 0.01 },
    } as const;
    const a = runLearningOfflinePipeline(samples, { ...shared, outputDir: outA });
    const b = runLearningOfflinePipeline(samples, { ...shared, outputDir: outB });

    expect(a.candidate.checksumSha256).toBe(b.candidate.checksumSha256);
    expect(verifyLearningCandidateChecksum(a.candidate)).toBe(true);
    expect(a.dataset.manifest.rowsChecksumSha256).toBe(b.dataset.manifest.rowsChecksumSha256);
    expect(a.candidate.modelType).toBe("residual_logistic");
    expect(a.candidate.featureSchemaVersion).toBe(NATIVE_V2_SHADOW_FEATURE_VERSION);
    expect(a.candidate.gitSha).toBe("deadbeef");
    expect(a.output?.candidatePath).toBeTruthy();

    const written = JSON.parse(readFileSync(a.output!.candidatePath, "utf8"));
    expect(written.checksumSha256).toBe(a.candidate.checksumSha256);
    expect(verifyLearningCandidateChecksum(written)).toBe(true);
    expect(readFileSync(a.output!.datasetRowsPath, "utf8").trim().split("\n").length).toBe(a.dataset.rows.length);
  });
});
