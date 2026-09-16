from pathlib import Path


def once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match, got {count}: {old[:180]!r}")
    return text.replace(old, new, 1)

# Keep persisted ranking compact: full UP/DOWN economics stay in memory only.
rank_path = Path("packages/core/src/native-v2-shadow-opportunity-ranking.ts")
rank = rank_path.read_text()
rank = once(
    rank,
    '  current: NativeV2ShadowMarketOpportunity;\n  top: NativeV2ShadowOpportunitySummary | null;',
    '  current: NativeV2ShadowOpportunitySummary | null;\n  top: NativeV2ShadowOpportunitySummary | null;',
)
rank = once(
    rank,
    '      current: opportunity,\n      top: top ? summarize(top) : null,',
    '      current: opportunity.best ? summarize(opportunity.best) : null,\n      top: top ? summarize(top) : null,',
)
rank_path.write_text(rank)

engine_path = Path("apps/engine/src/engine.ts")
text = engine_path.read_text()

text = once(
    text,
    '  NativeV2ShadowFeatureExtractor,\n  evaluateNativeV2ShadowFairProbability,\n  rolesForTriggerKind,',
    '  NativeV2ShadowFeatureExtractor,\n  evaluateNativeV2ShadowFairProbability,\n  NativeV2ShadowOpportunityRanker,\n  evaluateNativeV2ShadowMarketOpportunity,\n  DEFAULT_FIRST_LEG_RISK_CAP_USD,\n  rolesForTriggerKind,',
)

text = once(
    text,
    '  private readonly nativeV2Shadow = new NativeV2ShadowFeatureExtractor();\n  private readonly strategy = new TemporalPairHedgeStrategy();',
    '  private readonly nativeV2Shadow = new NativeV2ShadowFeatureExtractor();\n  private readonly nativeV2OpportunityRanker = new NativeV2ShadowOpportunityRanker();\n  private readonly strategy = new TemporalPairHedgeStrategy();',
)

text = once(
    text,
    '      this.triggerEngine.dropMarket(marketId);\n      this.nativeV2Shadow.dropMarket(marketId);\n      this.assetIndex.removeMarket(marketId);',
    '      this.triggerEngine.dropMarket(marketId);\n      this.nativeV2Shadow.dropMarket(marketId);\n      this.nativeV2OpportunityRanker.dropMarket(marketId);\n      this.assetIndex.removeMarket(marketId);',
)

text = once(
    text,
    '    this.triggerEngine.forgetMarketBooks(marketId);\n    this.nativeV2Shadow.dropMarket(marketId);\n    this.snapshots.delete(marketId);',
    '    this.triggerEngine.forgetMarketBooks(marketId);\n    this.nativeV2Shadow.dropMarket(marketId);\n    this.nativeV2OpportunityRanker.dropMarket(marketId);\n    this.snapshots.delete(marketId);',
)

old_eval = '''    const shadowV2FairProbability = shadowV2FeatureVector
      ? evaluateNativeV2ShadowFairProbability(shadowV2FeatureVector)
      : null;

    const consensus = this.nativeCompatibilityDecision(trigger, native);'''
new_eval = '''    const shadowV2FairProbability = shadowV2FeatureVector
      ? evaluateNativeV2ShadowFairProbability(shadowV2FeatureVector)
      : null;
    const shadowNow = this.nowFn();
    const shadowV2MarketOpportunity = shadowV2FairProbability
      ? evaluateNativeV2ShadowMarketOpportunity({
        fair: shadowV2FairProbability,
        snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null },
        now: shadowNow,
        targetNotionalUsd: Math.min(this.riskLimits.maxOrderSizeUsd, DEFAULT_FIRST_LEG_RISK_CAP_USD),
        minimumNetEvUsdPerShare: this.riskLimits.minExpectedEdge,
      })
      : null;
    const shadowV2OpportunityRanking = shadowV2MarketOpportunity
      ? this.nativeV2OpportunityRanker.upsertAndRank(
        shadowV2MarketOpportunity,
        shadowNow,
        this.riskLimits.staleFeedTimeoutMs,
      )
      : null;
    if (!shadowV2MarketOpportunity) this.nativeV2OpportunityRanker.dropMarket(trigger.marketId);

    const consensus = this.nativeCompatibilityDecision(trigger, native);'''
text = once(text, old_eval, new_eval)

text = once(
    text,
    '      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, triggerId: trigger.id,',
    '      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, triggerId: trigger.id,',
)

text = once(
    text,
    '        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability,\n        reference: reference ? {',
    '        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking,\n        reference: reference ? {',
)

engine_path.write_text(text)
