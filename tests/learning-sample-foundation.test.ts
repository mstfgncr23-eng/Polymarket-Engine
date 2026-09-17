import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  LEARNING_SAMPLE_SCHEMA_VERSION,
  LearningSampleStore,
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  buildLearningSampleV1,
  checkpointIdFromSeconds,
  evaluateNativeV2ShadowFairProbability,
  learningSampleId,
  parseLearningCheckpointSeconds,
  selectCrossedCheckpoints,
  tryObserveLearningSample,
  type NativeV2ShadowFeatureVector,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-learning-"));
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
  };
}

function resolvedGamma(winner: "UP" | "DOWN" = "UP"): Record<string, unknown> {
  return {
    closed: true,
    umaResolutionStatus: "resolved",
    outcomes: ["Yes", "No"],
    outcomePrices: winner === "UP" ? ["1", "0"] : ["0", "1"],
  };
}

describe("learning checkpoint crossing + identity", () => {
  it("parses default checkpoint schedule deterministically", () => {
    expect(parseLearningCheckpointSeconds(undefined)).toEqual([240, 180, 120, 60, 30, 15, 5]);
    expect(parseLearningCheckpointSeconds("60,30,bogus,-1,30")).toEqual([60, 30]);
    expect(checkpointIdFromSeconds(60)).toBe("T-60");
    expect(learningSampleId("m1", "T-60")).toBe("m1::T-60");
  });

  it("selects first crossing observation and skips already sampled + invalid horizon checkpoints", () => {
    const schedule = [240, 180, 120, 60, 30, 15, 5];
    const first = selectCrossedCheckpoints({
      timeToExpiryMs: 50_000,
      checkpointSeconds: schedule,
      alreadySampledCheckpointIds: new Set(),
    });
    expect(first.map((item) => item.checkpointId)).toEqual(["T-240", "T-180", "T-120", "T-60"]);

    const second = selectCrossedCheckpoints({
      timeToExpiryMs: 50_000,
      checkpointSeconds: schedule,
      alreadySampledCheckpointIds: new Set(first.map((item) => item.checkpointId)),
    });
    expect(second).toEqual([]);

    const later = selectCrossedCheckpoints({
      timeToExpiryMs: 4_000,
      checkpointSeconds: schedule,
      alreadySampledCheckpointIds: new Set(first.map((item) => item.checkpointId)),
    });
    expect(later.map((item) => item.checkpointId)).toEqual(["T-30", "T-15", "T-5"]);

    expect(selectCrossedCheckpoints({
      timeToExpiryMs: null,
      checkpointSeconds: schedule,
      alreadySampledCheckpointIds: new Set(),
    })).toEqual([]);

    expect(selectCrossedCheckpoints({
      timeToExpiryMs: 50_000,
      checkpointSeconds: schedule,
      alreadySampledCheckpointIds: new Set(),
      horizonMs: 90_000,
    }).map((item) => item.checkpointId)).toEqual(["T-60"]);
  });

  it("builds versioned LearningSampleV1 with unresolved settlement by default", () => {
    const features = vector({ timeToExpiryMs: 55_000 });
    const fair = evaluateNativeV2ShadowFairProbability(features)!;
    const sample = buildLearningSampleV1({
      marketId: "m1",
      checkpointId: "T-60",
      checkpointSeconds: 60,
      asset: "BTC",
      observedAt: new Date("2026-09-16T17:00:01.000Z"),
      timeToExpiryMs: 55_000,
      featureVector: features,
      v2FairProbability: fair,
      native: {
        fairProbability: 0.61,
        marketProbability: 0.55,
        direction: "UP",
        confidence: 0.7,
      },
      experimentId: "exp-1",
      gitSha: "abc123",
    });
    expect(sample.schemaVersion).toBe(LEARNING_SAMPLE_SCHEMA_VERSION);
    expect(sample.featureSchemaVersion).toBe(NATIVE_V2_SHADOW_FEATURE_VERSION);
    expect(sample.sampleId).toBe("m1::T-60");
    expect(sample.settlement.status).toBe("unresolved");
    expect(sample.settlement.outcome).toBeNull();
    expect(sample.v1.fairProbability).toBe(0.61);
    expect(sample.v2FairProbability.fairProbabilityUp).toBe(fair.fairProbabilityUp);
  });
});

describe("LearningSampleStore durability + labeling", () => {
  it("dedupes one sample per market/checkpoint and survives writer reinitialization", () => {
    const dataDir = tempDir();
    const features = vector({ timeToExpiryMs: 55_000 });
    const fair = evaluateNativeV2ShadowFairProbability(features)!;
    const store = new LearningSampleStore({
      dataDir,
      checkpointSeconds: "240,180,120,60,30,15,5",
      gitSha: "deadbeef",
      maxBytes: 10_000_000,
    });
    store.init();

    const first = store.observe({
      marketId: "m1",
      asset: "BTC",
      now: new Date("2026-09-16T17:00:01.000Z"),
      featureVector: features,
      v2FairProbability: fair,
      experimentId: "exp-1",
      gitSha: "deadbeef",
    });
    expect(first.map((s) => s.checkpointId).sort()).toEqual(["T-120", "T-180", "T-240", "T-60"]);

    const again = store.observe({
      marketId: "m1",
      asset: "BTC",
      now: new Date("2026-09-16T17:00:02.000Z"),
      featureVector: features,
      v2FairProbability: fair,
    });
    expect(again).toEqual([]);

    const before = store.summary();
    expect(before.sampleCount).toBe(4);
    expect(before.unresolvedMarketCount).toBe(1);
    expect(store.getManifest().schemaVersion).toBe(LEARNING_SAMPLE_SCHEMA_VERSION);
    expect(store.getManifest().gitSha).toBe("deadbeef");

    
    const reopened = new LearningSampleStore({
      dataDir,
      checkpointSeconds: "240,180,120,60,30,15,5",
      gitSha: "deadbeef",
    });
    reopened.init();
    expect(reopened.summary().sampleCount).toBe(4);
    expect(reopened.unresolvedLearningMarketIds()).toEqual(["m1"]);
    const third = reopened.observe({
      marketId: "m1",
      asset: "BTC",
      now: new Date("2026-09-16T17:00:03.000Z"),
      featureVector: features,
      v2FairProbability: fair,
    });
    expect(third).toEqual([]);
  });

  it("keeps unresolved unlabeled until authoritative resolution; labels without economic settlement", () => {
    const dataDir = tempDir();
    const features = vector({ marketId: "orphan-mkt", timeToExpiryMs: 20_000 });
    const fair = evaluateNativeV2ShadowFairProbability(features)!;
    const store = new LearningSampleStore({ dataDir, checkpointSeconds: "30,15,5" });
    store.init();
    store.observe({
      marketId: "orphan-mkt",
      asset: "ETH",
      now: new Date(),
      featureVector: features,
      v2FairProbability: fair,
    });

    const unresolved = store.readSamplesForTest();
    expect(unresolved.length).toBeGreaterThan(0);
    expect(unresolved.every((s) => s.settlement.status === "unresolved")).toBe(true);

    // Non-terminal public payload must not invent a label.
    expect(store.labelMarket("orphan-mkt", { closed: false, outcomes: ["Yes", "No"], outcomePrices: ["0.6", "0.4"] })).toBe(0);
    expect(store.readSamplesForTest().every((s) => s.settlement.status === "unresolved")).toBe(true);

    const labeled = store.labelMarket("orphan-mkt", resolvedGamma("DOWN"));
    expect(labeled).toBe(unresolved.length);
    expect(store.unresolvedLearningMarketIds()).toEqual([]);
    expect(store.readSamplesForTest().every((s) => s.settlement.status === "resolved" && s.settlement.outcome === "DOWN")).toBe(true);
    // No ledger interaction exists on this API — labeling alone is the proof.
    expect(store.summary().labelsApplied).toBe(unresolved.length);
  });

  it("capacity / write failure degrades learning without throwing into caller", () => {
    const dataDir = tempDir();
    const features = vector({ timeToExpiryMs: 55_000 });
    const fair = evaluateNativeV2ShadowFairProbability(features)!;
    const store = new LearningSampleStore({
      dataDir,
      checkpointSeconds: "240,180,120,60",
      maxBytes: 200, // tiny — first write may still succeed depending on line size; force via tryObserve path
    });
    store.init();

    // Unit-level: tryObserve never throws even if observe would fail.
    const throwing = {
      tryObserve() {
        throw new Error("boom");
      },
    };
    expect(() => tryObserveLearningSample(throwing as never, {
      marketId: "m1",
      asset: null,
      now: new Date(),
      featureVector: features,
      v2FairProbability: fair,
    })).not.toThrow();

    // Capacity: tiny maxBytes marks degraded and does not throw.
    expect(() => store.tryObserve({
      marketId: "m1",
      asset: "BTC",
      now: new Date(),
      featureVector: features,
      v2FairProbability: fair,
    })).not.toThrow();
    // Either accepted some then capacity-stopped, or degraded on first oversize write.
    const summary = store.summary();
    expect(summary.observeAttempts).toBeGreaterThan(0);
    if (summary.sampleCount === 0) {
      expect(summary.degraded).toBe(true);
      expect(summary.lastError).toBeTruthy();
    } else {
      // Fill until capacity
      for (let i = 0; i < 50; i += 1) {
        store.tryObserve({
          marketId: `m-${i}`,
          asset: "BTC",
          now: new Date(),
          featureVector: vector({ marketId: `m-${i}`, timeToExpiryMs: 55_000 }),
          v2FairProbability: fair,
        });
      }
      expect(store.summary().degraded).toBe(true);
      expect(store.summary().lastError).toContain("LEARNING_CAPACITY");
    }
  });

  it("skips sampling when timeToExpiryMs is null", () => {
    const dataDir = tempDir();
    const features = vector({ timeToExpiryMs: null });
    // fair may still evaluate; store must skip on null timing
    const fair = evaluateNativeV2ShadowFairProbability(vector({ timeToExpiryMs: 60_000 }))!;
    const store = new LearningSampleStore({ dataDir, checkpointSeconds: "60,30" });
    store.init();
    expect(store.observe({
      marketId: "m1",
      asset: "BTC",
      now: new Date(),
      featureVector: features,
      v2FairProbability: fair,
    })).toEqual([]);
  });
});
