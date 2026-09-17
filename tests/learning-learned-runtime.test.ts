import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  buildLearningSampleV1,
  evaluateNativeV2ShadowFairProbability,
  evaluateControlledPaperAuthorityGate,
  candidateCannotObtainPaperAuthority,
  registerLearningCandidate,
  transitionLearningModelStatus,
  learningModelsDir,
  loadLearningModelFromArtifact,
  LearningRuntime,
  evaluateLearnedV2Shadow,
  buildLearningScorecardRecord,
  labelScorecardRecord,
  runLearningOfflinePipeline,
  verifyLearningCandidateChecksum,
  statusGrantsPaperAuthority,
  type LearningSampleV1,
  type NativeV2ShadowFeatureVector,
  type LearningCandidateArtifactV1,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(prefix = "pm-learning-pr5-"): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}

function vector(overrides: {
  marketId?: string;
  timeToExpiryMs?: number | null;
  synchronized?: boolean;
  marketPriorUp?: number;
  bookPressureL5?: number;
} = {}): NativeV2ShadowFeatureVector {
  const book = (microprice: number) => ({
    mid: 0.5,
    spread: 0.02,
    spreadBps: 400,
    microprice,
    micropriceOffsetBps: 0,
    imbalanceL1: 0,
    imbalanceL3: 0,
    imbalanceL5: 0.1,
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

  const tte = overrides.timeToExpiryMs === undefined ? 60_000 : overrides.timeToExpiryMs;
  const marketPriorUp = overrides.marketPriorUp ?? 0.55;
  const bookPressureL5 = overrides.bookPressureL5 ?? 0.2;

  return {
    version: NATIVE_V2_SHADOW_FEATURE_VERSION,
    marketId: overrides.marketId ?? "m1",
    asset: "BTC",
    observedAt: "2026-09-16T17:00:01.000Z",
    up: book(0.55),
    down: book(0.45),
    pair: {
      marketPriorUp,
      pairAskCost: 1.02,
      binaryComplementBasis: 0,
      bookPressureL1: bookPressureL5,
      bookPressureL3: bookPressureL5,
      bookPressureL5,
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
      timeToExpiryMs: tte,
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
      timeToExpiryMs: tte,
      invSqrtTimeToExpiryMinutes: 1,
      bookPressureL5xInvSqrtTte: bookPressureL5,
      shortMomentumBpsxInvSqrtTte: 5,
      volNormMovementxInvSqrtTte: 0.5,
      upMidMinusMarketPrior: 0.5 - marketPriorUp,
      downMidMinusMarketPriorComplement: 0.5 - (1 - marketPriorUp),
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
  outcome: "UP" | "DOWN";
  observedAt: string;
  marketPriorUp?: number;
}): LearningSampleV1 {
  const fv = vector({
    marketId: input.marketId,
    timeToExpiryMs: input.checkpointSeconds * 1000,
    marketPriorUp: input.marketPriorUp,
  });
  fv.observedAt = input.observedAt;
  const fair = evaluateNativeV2ShadowFairProbability(fv);
  if (!fair) throw new Error("fair null");
  const sample = buildLearningSampleV1({
    marketId: input.marketId,
    checkpointId: `T-${input.checkpointSeconds}`,
    checkpointSeconds: input.checkpointSeconds,
    asset: "BTC",
    observedAt: input.observedAt,
    timeToExpiryMs: input.checkpointSeconds * 1000,
    featureVector: fv,
    v2FairProbability: fair,
    experimentId: null,
    gitSha: "test",
  });
  return {
    ...sample,
    settlement: {
      status: "resolved",
      outcome: input.outcome,
      labeledAt: input.observedAt,
      source: "test",
      reason: null,
    },
  };
}

function buildCandidateArtifact(outDir: string): LearningCandidateArtifactV1 {
  const samples: LearningSampleV1[] = [];
  const markets = ["mA", "mB", "mC", "mD"];
  for (let i = 0; i < markets.length; i += 1) {
    samples.push(makeResolvedSample({
      marketId: markets[i]!,
      checkpointSeconds: 60,
      outcome: i % 2 === 0 ? "UP" : "DOWN",
      observedAt: `2026-09-0${1 + i}T12:00:00.000Z`,
      marketPriorUp: 0.45 + i * 0.05,
    }));
    samples.push(makeResolvedSample({
      marketId: markets[i]!,
      checkpointSeconds: 30,
      outcome: i % 2 === 0 ? "UP" : "DOWN",
      observedAt: `2026-09-0${1 + i}T12:00:30.000Z`,
      marketPriorUp: 0.48 + i * 0.04,
    }));
  }
  const result = runLearningOfflinePipeline(samples, {
    outputDir: outDir,
    gitSha: "pr5-test",
    walkForward: { validationMarketFraction: 0.5 },
    residual: { maxIterations: 50, l2Lambda: 0.01 },
  });
  expect(verifyLearningCandidateChecksum(result.candidate)).toBe(true);
  return result.candidate;
}

describe("PR5 learned runtime — model load safety", () => {
  it("loads a valid candidate model", () => {
    const out = tempDir();
    const candidate = buildCandidateArtifact(out);
    const path = join(out, "candidate.json");
    const result = loadLearningModelFromArtifact(path);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.loaded.verifiedChecksumSha256).toBe(candidate.checksumSha256);
    expect(result.loaded.featureSchemaVersion).toBe(NATIVE_V2_SHADOW_FEATURE_VERSION);
  });

  it("rejects checksum mismatch", () => {
    const out = tempDir();
    buildCandidateArtifact(out);
    const path = join(out, "candidate.json");
    const raw = JSON.parse(readFileSync(path, "utf8")) as LearningCandidateArtifactV1;
    raw.checksumSha256 = "0".repeat(64);
    writeFileSync(path, `${JSON.stringify(raw, null, 2)}\n`);
    const result = loadLearningModelFromArtifact(path);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("CHECKSUM_MISMATCH");
  });

  it("rejects feature schema mismatch", () => {
    const out = tempDir();
    buildCandidateArtifact(out);
    const path = join(out, "candidate.json");
    const raw = JSON.parse(readFileSync(path, "utf8")) as LearningCandidateArtifactV1;
    // Break schema then recompute would still fail assertSupported; mutate without valid checksum path:
    const broken = {
      ...raw,
      featureSchemaVersion: "native-v2-shadow-features-v1",
    };
    // Keep old checksum so checksum fails OR schema fails — force schema by fixing checksum of broken payload
    const { checksumSha256: _c, ...rest } = broken as LearningCandidateArtifactV1;
    const checksumSha256 = createHash("sha256").update(JSON.stringify(rest)).digest("hex");
    writeFileSync(path, `${JSON.stringify({ ...rest, checksumSha256 }, null, 2)}\n`);
    const result = loadLearningModelFromArtifact(path);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("INCOMPATIBLE_FEATURE_SCHEMA");
  });

  it("missing model leaves runtime fail-safe (V1 unchanged conceptually)", () => {
    const dataDir = tempDir();
    const runtime = new LearningRuntime({
      shadowEnabled: true,
      learningDataDir: dataDir,
      modelArtifactPath: join(dataDir, "does-not-exist.json"),
      modelId: null,
      paperAuthority: { paperAuthorityEnabled: false, approvedModelId: null },
    });
    runtime.init();
    const summary = runtime.summary();
    expect(summary.modelLoaded).toBe(false);
    expect(summary.loadFailureReason).toBe("MISSING_ARTIFACT");

    const fv = vector();
    const fair = evaluateNativeV2ShadowFairProbability(fv);
    const ev = runtime.evaluateShadow({
      featureVector: fv,
      deterministicV2: fair,
      marketId: "m1",
    });
    expect(ev.status).toBe("fail_safe");
    // Deterministic V2 still present on the evaluation for comparison fields
    expect(ev.deterministicV2ProbabilityUp).toBe(fair?.fairProbabilityUp ?? null);
  });
});

describe("PR5 learned runtime — shadow isolation", () => {
  it("learned shadow cannot mutate a frozen V1 snapshot object", () => {
    const out = tempDir();
    buildCandidateArtifact(out);
    const runtime = new LearningRuntime({
      shadowEnabled: true,
      learningDataDir: out,
      modelArtifactPath: join(out, "candidate.json"),
      modelId: null,
      paperAuthority: { paperAuthorityEnabled: false, approvedModelId: null },
    });
    runtime.init();
    expect(runtime.summary().modelLoaded).toBe(true);

    const v1State = Object.freeze({
      cashUsd: 1000,
      fairProbability: 0.5,
      positions: Object.freeze([] as string[]),
    });
    const before = structuredClone(v1State);
    const fv = vector({ bookPressureL5: 0.4 });
    const fair = evaluateNativeV2ShadowFairProbability(fv);
    const ev = runtime.evaluateShadow({
      featureVector: fv,
      deterministicV2: fair,
      marketId: "m1",
      recordScorecard: true,
    });
    expect(ev.status).toBe("ok");
    expect(ev.learnedProbabilityUp).toBeTypeOf("number");
    expect(v1State).toEqual(before);
    expect(Object.isFrozen(v1State)).toBe(true);
  });

  it("disabled shadow does not evaluate", () => {
    const runtime = new LearningRuntime({
      shadowEnabled: false,
      learningDataDir: tempDir(),
      modelArtifactPath: null,
      modelId: null,
      paperAuthority: { paperAuthorityEnabled: false, approvedModelId: null },
    });
    runtime.init();
    const ev = runtime.evaluateShadow({
      featureVector: vector(),
      deterministicV2: null,
      marketId: "m1",
    });
    expect(ev.status).toBe("disabled");
    expect(runtime.summary().evaluations).toBe(0);
  });

  it("records scorecard with prediction + deterministic V2 comparison fields", () => {
    const out = tempDir();
    buildCandidateArtifact(out);
    const runtime = new LearningRuntime({
      shadowEnabled: true,
      learningDataDir: out,
      modelArtifactPath: join(out, "candidate.json"),
      modelId: null,
      paperAuthority: { paperAuthorityEnabled: false, approvedModelId: null },
    });
    runtime.init();
    const fv = vector();
    const fair = evaluateNativeV2ShadowFairProbability(fv)!;
    const ev = runtime.evaluateShadow({
      featureVector: fv,
      deterministicV2: fair,
      marketId: "m-score",
      triggerId: "trig-1",
    });
    expect(ev.status).toBe("ok");
    const cards = runtime.readScorecardsForTest();
    expect(cards.length).toBe(1);
    expect(cards[0]!.predictionUp).toBe(ev.learnedProbabilityUp);
    expect(cards[0]!.deterministicV2ProbabilityUp).toBe(fair.fairProbabilityUp);
    expect(cards[0]!.modelVersion).toBeTruthy();
    expect(cards[0]!.settlement.status).toBe("unresolved");

    const labeled = labelScorecardRecord(cards[0]!, "UP", "2026-09-17T00:00:00.000Z");
    expect(labeled.settlement.status).toBe("resolved");
    expect(labeled.metrics.brierContribution).toBeTypeOf("number");
    expect(labeled.metrics.logLossContribution).toBeTypeOf("number");
  });
});

describe("PR5 model registry + PAPER authority gates", () => {
  it("retrain registers as CANDIDATE only", () => {
    const out = tempDir();
    const dataDir = tempDir();
    const candidate = buildCandidateArtifact(out);
    const entry = registerLearningCandidate(learningModelsDir(dataDir), {
      artifactPath: join(out, "candidate.json"),
      candidate,
    });
    expect(entry.status).toBe("CANDIDATE");
    expect(entry.paperApprovedByOperator).toBe(false);
    expect(statusGrantsPaperAuthority(entry.status)).toBe(false);
  });

  it("CANDIDATE cannot obtain PAPER authority", () => {
    expect(candidateCannotObtainPaperAuthority("CANDIDATE")).toBe(true);
    expect(candidateCannotObtainPaperAuthority("SHADOW")).toBe(true);
    expect(candidateCannotObtainPaperAuthority("PAPER_APPROVED")).toBe(false);

    const decision = evaluateControlledPaperAuthorityGate({
      config: {
        paperAuthorityEnabled: true,
        approvedModelId: "cand-1",
      },
      modelId: "cand-1",
      registryStatus: "CANDIDATE",
      paperApprovedByOperator: false,
      deterministicRiskMandatory: true,
      executionGatesMandatory: true,
      tradingMode: "paper",
    });
    expect(decision.configAllows).toBe(false);
    expect(decision.maySubmitOrders).toBe(false);
    expect(decision.engineHookImplemented).toBe(false);
    expect(decision.blockReasons).toContain("STATUS_NOT_PAPER_APPROVED");
    expect(decision.blockReasons).toContain("ENGINE_HOOK_NOT_IMPLEMENTED");
  });

  it("only explicit PAPER_APPROVED + explicit config could pass config gate — still cannot submit", () => {
    const out = tempDir();
    const dataDir = tempDir();
    const candidate = buildCandidateArtifact(out);
    const entry = registerLearningCandidate(learningModelsDir(dataDir), {
      artifactPath: join(out, "candidate.json"),
      candidate,
      modelId: "model-paper-1",
    });
    transitionLearningModelStatus(learningModelsDir(dataDir), {
      modelId: entry.modelId,
      to: "SHADOW",
      reviewNote: "operator → shadow",
    });
    const approved = transitionLearningModelStatus(learningModelsDir(dataDir), {
      modelId: entry.modelId,
      to: "PAPER_APPROVED",
      operatorConfirmed: true,
      reviewNote: "operator review",
    });
    expect(approved.status).toBe("PAPER_APPROVED");
    expect(approved.paperApprovedByOperator).toBe(true);

    // Without operatorConfirmed, PAPER_APPROVED is rejected
    const out2 = tempDir();
    const dataDir2 = tempDir();
    const c2 = buildCandidateArtifact(out2);
    const e2 = registerLearningCandidate(learningModelsDir(dataDir2), {
      artifactPath: join(out2, "candidate.json"),
      candidate: c2,
      modelId: "model-x",
    });
    transitionLearningModelStatus(learningModelsDir(dataDir2), {
      modelId: e2.modelId,
      to: "SHADOW",
    });
    expect(() =>
      transitionLearningModelStatus(learningModelsDir(dataDir2), {
        modelId: e2.modelId,
        to: "PAPER_APPROVED",
        operatorConfirmed: false,
      }),
    ).toThrow(/PAPER_REQUIRES_OPERATOR/);

    const decision = evaluateControlledPaperAuthorityGate({
      config: {
        paperAuthorityEnabled: true,
        approvedModelId: "model-paper-1",
      },
      modelId: "model-paper-1",
      registryStatus: "PAPER_APPROVED",
      paperApprovedByOperator: true,
      deterministicRiskMandatory: true,
      executionGatesMandatory: true,
      tradingMode: "paper",
    });
    expect(decision.configAllows).toBe(true);
    expect(decision.maySubmitOrders).toBe(false);
    expect(decision.blockReasons).toContain("ENGINE_HOOK_NOT_IMPLEMENTED");
  });

  it("deterministic risk still required for any future PAPER path", () => {
    const decision = evaluateControlledPaperAuthorityGate({
      config: { paperAuthorityEnabled: true, approvedModelId: "m1" },
      modelId: "m1",
      registryStatus: "PAPER_APPROVED",
      paperApprovedByOperator: true,
      deterministicRiskMandatory: false,
      executionGatesMandatory: true,
      tradingMode: "paper",
    });
    expect(decision.configAllows).toBe(false);
    expect(decision.blockReasons).toContain("DETERMINISTIC_RISK_NOT_MANDATORY");
    expect(decision.maySubmitOrders).toBe(false);
  });

  it("feature schema mismatch at evaluate time fail-safes", () => {
    const out = tempDir();
    buildCandidateArtifact(out);
    const loaded = loadLearningModelFromArtifact(join(out, "candidate.json"));
    expect(loaded.ok).toBe(true);
    if (!loaded.ok) return;
    const badFv = {
      ...vector(),
      version: "native-v2-shadow-features-v1",
    } as unknown as NativeV2ShadowFeatureVector;
    const ev = evaluateLearnedV2Shadow({
      enabled: true,
      loaded: loaded.loaded,
      featureVector: badFv,
      deterministicV2: null,
    });
    expect(ev.status).toBe("fail_safe");
    expect(ev.failReason).toBe("FEATURE_SCHEMA_MISMATCH");
  });
});

describe("PR5 scorecard builder", () => {
  it("buildLearningScorecardRecord preserves comparison fields", () => {
    const ev = evaluateLearnedV2Shadow({
      enabled: false,
      loaded: null,
      featureVector: null,
      deterministicV2: null,
    });
    const rec = buildLearningScorecardRecord({
      evaluation: ev,
      marketId: "m1",
    });
    expect(rec.schemaVersion).toBe("learning-scorecard-v1");
    expect(rec.shadowStatus).toBe("disabled");
    expect(rec.economicCounterfactual.available).toBe(false);
  });
});
