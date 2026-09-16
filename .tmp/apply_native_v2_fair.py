from pathlib import Path

path = Path("apps/engine/src/engine.ts")
text = path.read_text()


def once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match, got {count}: {old[:150]!r}")
    text = text.replace(old, new, 1)


once(
    '  NativeV2ShadowFeatureExtractor,\n  rolesForTriggerKind,',
    '  NativeV2ShadowFeatureExtractor,\n  evaluateNativeV2ShadowFairProbability,\n  rolesForTriggerKind,',
)

once(
    '''    const shadowV2FeatureVector = this.nativeV2Shadow.extract({
      marketId: trigger.marketId,
      asset,
      now: this.nowFn(),
      endDate: this.marketsById.get(trigger.marketId)?.endDate ?? null,
      marketSynchronized: true,
      referenceFreshness: referenceContext?.freshness ?? "unknown",
      referenceFreshnessMs: referenceContext?.freshnessMs ?? null,
    });

    const consensus = this.nativeCompatibilityDecision(trigger, native);''',
    '''    const shadowV2FeatureVector = this.nativeV2Shadow.extract({
      marketId: trigger.marketId,
      asset,
      now: this.nowFn(),
      endDate: this.marketsById.get(trigger.marketId)?.endDate ?? null,
      marketSynchronized: true,
      referenceFreshness: referenceContext?.freshness ?? "unknown",
      referenceFreshnessMs: referenceContext?.freshnessMs ?? null,
    });
    const shadowV2FairProbability = shadowV2FeatureVector
      ? evaluateNativeV2ShadowFairProbability(shadowV2FeatureVector)
      : null;

    const consensus = this.nativeCompatibilityDecision(trigger, native);''',
)

once(
    '      featureVector: native.featureVector, shadowV2FeatureVector, triggerId: trigger.id,',
    '      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, triggerId: trigger.id,',
)

once(
    '        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector,\n        reference: reference ? {',
    '        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability,\n        reference: reference ? {',
)

path.write_text(text)
