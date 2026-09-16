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
