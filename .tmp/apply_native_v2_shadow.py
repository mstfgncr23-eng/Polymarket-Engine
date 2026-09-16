from pathlib import Path

path = Path("apps/engine/src/engine.ts")
text = path.read_text()


def once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match, got {count}: {old[:140]!r}")
    text = text.replace(old, new, 1)


once(
    '  type NativeDecisionConfig,\n  rolesForTriggerKind,\n} from "@pm/core";',
    '  type NativeDecisionConfig,\n  NativeV2ShadowFeatureExtractor,\n  rolesForTriggerKind,\n} from "@pm/core";',
)
once(
    '  private readonly nativeDecisionConfig: NativeDecisionConfig;\n  private readonly strategy = new TemporalPairHedgeStrategy();',
    '  private readonly nativeDecisionConfig: NativeDecisionConfig;\n  private readonly nativeV2Shadow = new NativeV2ShadowFeatureExtractor();\n  private readonly strategy = new TemporalPairHedgeStrategy();',
)
once(
    '  private clearMarketPairProjections(marketId: string): void {\n    this.triggerEngine.forgetMarketBooks(marketId);\n    this.snapshots.delete(marketId);',
    '  private clearMarketPairProjections(marketId: string): void {\n    this.triggerEngine.forgetMarketBooks(marketId);\n    this.nativeV2Shadow.dropMarket(marketId);\n    this.snapshots.delete(marketId);',
)
once(
    '      this.triggerEngine.dropMarket(marketId);\n      this.assetIndex.removeMarket(marketId);',
    '      this.triggerEngine.dropMarket(marketId);\n      this.nativeV2Shadow.dropMarket(marketId);\n      this.assetIndex.removeMarket(marketId);',
)
once(
    '      this.triggerEngine.dropAsset(asset);\n      this.assetIndex.dropAsset(asset);',
    '      this.triggerEngine.dropAsset(asset);\n      this.nativeV2Shadow.dropAsset(asset);\n      this.assetIndex.dropAsset(asset);',
)
once(
    '    pair[snapshot.outcome] = snapshot;\n    this.snapshots.set(snapshot.marketId, pair);\n    if (snapshot.asset) {',
    '    pair[snapshot.outcome] = snapshot;\n    this.snapshots.set(snapshot.marketId, pair);\n    this.nativeV2Shadow.ingestMarket(snapshot);\n    if (snapshot.asset) {',
)
once(
    '    if (admission.overflow) {\n      this.noteReferenceLogicalLoss(tick.asset);',
    '    if (admission.overflow) {\n      this.nativeV2Shadow.resetReference(tick.asset);\n      this.noteReferenceLogicalLoss(tick.asset);',
)
once(
    '    const wasUnreliable = this.referenceUnreliable.has(tick.asset);\n    this.state.noteReferenceTick(tick.asset, tick.receivedAt, {',
    '    const wasUnreliable = this.referenceUnreliable.has(tick.asset);\n    this.nativeV2Shadow.ingestReference(tick);\n    this.state.noteReferenceTick(tick.asset, tick.receivedAt, {',
)

reset = '        this.triggerEngine.resetReferenceAfterReconnect(asset);'
count = text.count(reset)
if count != 3:
    raise SystemExit(f"expected three reference reset matches, got {count}")
text = text.replace(
    reset,
    '        this.triggerEngine.resetReferenceAfterReconnect(asset);\n        this.nativeV2Shadow.resetReference(asset);',
)

once(
    '    if (!native) return;\n\n    const consensus = this.nativeCompatibilityDecision(trigger, native);',
    '''    if (!native) return;

    const shadowV2FeatureVector = this.nativeV2Shadow.extract({
      marketId: trigger.marketId,
      asset,
      now: this.nowFn(),
      endDate: this.marketsById.get(trigger.marketId)?.endDate ?? null,
      marketSynchronized: true,
      referenceFreshness: referenceContext?.freshness ?? "unknown",
      referenceFreshnessMs: referenceContext?.freshnessMs ?? null,
    });

    const consensus = this.nativeCompatibilityDecision(trigger, native);''',
)
once(
    '      featureVector: native.featureVector, triggerId: trigger.id,',
    '      featureVector: native.featureVector, shadowV2FeatureVector, triggerId: trigger.id,',
)
once(
    '        expiresAt: native.expiresAt, featureVector: native.featureVector,\n        reference: reference ? {',
    '        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector,\n        reference: reference ? {',
)

path.write_text(text)
