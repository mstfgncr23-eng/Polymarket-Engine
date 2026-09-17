import { describe, expect, it } from "vitest";
import {
  checksumCandidatePayload,
  splitCalibrationEvaluationByMarket,
  splitWalkForwardByMarket,
  type LearningTrainingRowV1,
} from "@pm/core";

function row(input: {
  sampleId: string;
  marketId: string;
  observedAt: string;
  labeledAt: string | null;
  checkpointId?: string;
}): LearningTrainingRowV1 {
  // These tests exercise only temporal/grouping fields. The offline split must
  // not inspect prediction features to decide whether a label was historically available.
  return {
    sampleId: input.sampleId,
    marketId: input.marketId,
    observedAt: input.observedAt,
    labeledAt: input.labeledAt,
    checkpointId: input.checkpointId ?? "T-60",
    asset: "BTC",
  } as unknown as LearningTrainingRowV1;
}

describe("PR4 leakage-safe historical boundaries", () => {
  it("excludes an early-observed market whose authoritative label arrived after validation began", () => {
    const rows = [
      row({
        sampleId: "good::T-60",
        marketId: "good",
        observedAt: "2026-09-01T10:00:00.000Z",
        labeledAt: "2026-09-01T12:00:00.000Z",
      }),
      row({
        sampleId: "future-label::T-60",
        marketId: "future-label",
        observedAt: "2026-09-01T11:00:00.000Z",
        labeledAt: "2026-09-05T00:00:00.000Z",
      }),
      row({
        sampleId: "val-1::T-60",
        marketId: "val-1",
        observedAt: "2026-09-03T10:00:00.000Z",
        labeledAt: "2026-09-03T12:00:00.000Z",
      }),
      row({
        sampleId: "val-2::T-60",
        marketId: "val-2",
        observedAt: "2026-09-04T10:00:00.000Z",
        labeledAt: "2026-09-04T12:00:00.000Z",
      }),
    ];

    const split = splitWalkForwardByMarket(rows, { validationMarketFraction: 0.5 });
    expect(split.validationWindowStartAt).toBe("2026-09-03T10:00:00.000Z");
    expect(split.trainMarketIds).toEqual(["good"]);
    expect(split.trainMarketIds).not.toContain("future-label");
    expect(split.validationMarketIds).toEqual(["val-1", "val-2"]);
  });

  it("keeps every checkpoint of one validation market on one side of calibration/evaluation", () => {
    const heldOut = [
      row({
        sampleId: "cal::T-60",
        marketId: "cal",
        checkpointId: "T-60",
        observedAt: "2026-09-03T10:00:00.000Z",
        labeledAt: "2026-09-03T12:00:00.000Z",
      }),
      row({
        sampleId: "cal::T-30",
        marketId: "cal",
        checkpointId: "T-30",
        observedAt: "2026-09-03T10:00:30.000Z",
        labeledAt: "2026-09-03T12:00:00.000Z",
      }),
      row({
        sampleId: "eval::T-60",
        marketId: "eval",
        observedAt: "2026-09-04T10:00:00.000Z",
        labeledAt: "2026-09-04T12:00:00.000Z",
      }),
    ];

    const split = splitCalibrationEvaluationByMarket(heldOut, 0.5);
    expect(split).not.toBeNull();
    expect(split!.calibrationMarketIds).toEqual(["cal"]);
    expect(split!.evaluationMarketIds).toEqual(["eval"]);
    expect(new Set(split!.calibrationRows.map((r) => r.marketId))).toEqual(new Set(["cal"]));
    expect(new Set(split!.evaluationRows.map((r) => r.marketId))).toEqual(new Set(["eval"]));
  });

  it("omits calibration when its label would only become available after evaluation starts", () => {
    const heldOut = [
      row({
        sampleId: "late-cal::T-60",
        marketId: "late-cal",
        observedAt: "2026-09-03T10:00:00.000Z",
        labeledAt: "2026-09-05T12:00:00.000Z",
      }),
      row({
        sampleId: "eval::T-60",
        marketId: "eval",
        observedAt: "2026-09-04T10:00:00.000Z",
        labeledAt: "2026-09-04T12:00:00.000Z",
      }),
    ];

    expect(splitCalibrationEvaluationByMarket(heldOut, 0.5)).toBeNull();
  });
});

describe("PR4 deterministic candidate identity", () => {
  it("ignores generation timestamps but changes when semantic model content changes", () => {
    const payload = {
      candidateVersion: "learning-candidate-v1",
      modelType: "residual_logistic",
      modelVersion: "learning-residual-logistic-v1",
      featureSchemaVersion: "native-v2-shadow-features-v2",
      datasetVersion: "learning-dataset-v1",
      trainingCutoffAt: "2026-09-03T10:00:00.000Z",
      trainingWindow: { startAt: "2026-09-01T10:00:00.000Z", endAt: "2026-09-02T10:00:00.000Z" },
      validationWindow: { startAt: "2026-09-03T10:00:00.000Z", endAt: "2026-09-04T10:00:00.000Z" },
      trainMarketCount: 2,
      validationMarketCount: 2,
      trainRowCount: 4,
      validationRowCount: 4,
      metrics: {},
      economicEvaluation: {},
      model: { weights: [0.1], bias: 0.2 },
      datasetManifest: {
        createdAt: "2026-09-17T08:00:00.000Z",
        rowsChecksumSha256: "rows",
      },
      gitSha: "abc",
      createdAt: "2026-09-17T08:00:00.000Z",
    } as any;

    const regenerated = structuredClone(payload);
    regenerated.createdAt = "2026-09-18T09:00:00.000Z";
    regenerated.datasetManifest.createdAt = "2026-09-18T09:00:00.000Z";

    expect(checksumCandidatePayload(payload)).toBe(checksumCandidatePayload(regenerated));

    regenerated.model.bias = 0.3;
    expect(checksumCandidatePayload(payload)).not.toBe(checksumCandidatePayload(regenerated));
  });
});
