from pathlib import Path

path = Path("apps/engine/src/engine.ts")
text = path.read_text()


def once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match, got {count}: {old[:180]!r}")
    text = text.replace(old, new, 1)

once(
    '''  NativeV2ShadowOpportunityRanker,
  evaluateNativeV2ShadowMarketOpportunity,
  DEFAULT_FIRST_LEG_RISK_CAP_USD,
  rolesForTriggerKind,''',
    '''  NativeV2ShadowOpportunityRanker,
  evaluateNativeV2ShadowMarketOpportunity,
  buildNativeV2ShadowExecutionIntent,
  type NativeV2ShadowFairProbability,
  type NativeV2ShadowMarketOpportunity,
  type NativeV2ShadowOpportunityRanking,
  DEFAULT_FIRST_LEG_RISK_CAP_USD,
  rolesForTriggerKind,''',
)

once(
    '''    if (!shadowV2MarketOpportunity) this.nativeV2OpportunityRanker.dropMarket(trigger.marketId);

    const consensus = this.nativeCompatibilityDecision(trigger, native);''',
    '''    if (!shadowV2MarketOpportunity) this.nativeV2OpportunityRanker.dropMarket(trigger.marketId);
    const shadowV2ExecutionEvaluation = shadowV2FairProbability && shadowV2MarketOpportunity && shadowV2OpportunityRanking
      ? await this.evaluateNativeV2ShadowExecution({
        trigger,
        fair: shadowV2FairProbability,
        opportunity: shadowV2MarketOpportunity,
        ranking: shadowV2OpportunityRanking,
      })
      : null;

    const consensus = this.nativeCompatibilityDecision(trigger, native);''',
)

once(
    '''      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, triggerId: trigger.id,''',
    '''      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, shadowV2ExecutionEvaluation, triggerId: trigger.id,''',
)

once(
    '''        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking,
        reference: reference ? {''',
    '''        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, shadowV2ExecutionEvaluation,
        reference: reference ? {''',
)

anchor = '''  private nativeCompatibilityDecision(trigger: TriggerEvent, native: NativeDecision): NonNullable<EngineStateStore["latestConsensus"]> {'''
method = '''  private async evaluateNativeV2ShadowExecution(input: {
    trigger: TriggerEvent;
    fair: NativeV2ShadowFairProbability;
    opportunity: NativeV2ShadowMarketOpportunity;
    ranking: NativeV2ShadowOpportunityRanking;
  }): Promise<Record<string, unknown> | null> {
    const selectedAtDecision = input.opportunity.best;
    if (input.ranking.marketRank !== 1 || !selectedAtDecision?.actionable) return null;

    const now = this.nowFn();
    const marketId = input.opportunity.marketId;
    const selectedOutcome = selectedAtDecision.outcome;
    const currentPair = this.snapshots.get(marketId);
    const currentSnapshot = currentPair?.[selectedOutcome] ?? null;
    const targetNotionalUsd = Math.min(this.riskLimits.maxOrderSizeUsd, DEFAULT_FIRST_LEG_RISK_CAP_USD);
    const refreshedOpportunity = currentPair
      ? evaluateNativeV2ShadowMarketOpportunity({
        fair: input.fair,
        snapshots: { UP: currentPair.UP ?? null, DOWN: currentPair.DOWN ?? null },
        now,
        targetNotionalUsd,
        minimumNetEvUsdPerShare: this.riskLimits.minExpectedEdge,
      })
      : null;
    const selected = refreshedOpportunity?.candidates[selectedOutcome] ?? null;
    const asset = input.trigger.asset ?? this.assetByMarket.get(marketId) ?? currentSnapshot?.asset ?? null;
    const blockers: string[] = [];

    if (this.env.PAPER_EXECUTION_STYLE !== "TAKER") blockers.push("SHADOW_TAKER_MODEL_REQUIRED");
    if (this.researchIngressBlocked()) blockers.push("RESEARCH_INGRESS_BLOCKED");
    if (!this.isTriggerActionable(input.trigger.id)) blockers.push("TRIGGER_NOT_ACTIONABLE");
    if (this.experimentStartPending) blockers.push("EXPERIMENT_START_PENDING");
    if (this.overload.state === "FAIL_CLOSED") blockers.push("OVERLOAD_FAIL_CLOSED");
    if (!experimentAllowsFirstLeg(this.experiment, now)) blockers.push("EXPERIMENT_ENTRY_CLOSED");
    if (!this.entryQualified.has(marketId)) blockers.push("MARKET_NOT_ENTRY_QUALIFIED");
    if (!this.isMarketSynchronized(marketId)) blockers.push("MARKET_UNSYNCHRONIZED");
    if (assetNeedsReliableReference(this.referenceRequired.get(marketId), this.referenceUnreliable, asset)) blockers.push("REFERENCE_PROVIDER_BLOCKED");
    if (this.ledger.positions.get(marketId)?.unpairedOutcome) blockers.push("EXISTING_UNPAIRED_POSITION");
    if (!currentSnapshot || !selected) blockers.push("CURRENT_BOOK_NOT_EXECUTABLE");
    if (currentSnapshot && !this.adapterTokenSynchronized(currentSnapshot.tokenId)) blockers.push("TOKEN_UNSYNCHRONIZED");

    const compactBase = {
      version: "native-v2-shadow-execution-v1",
      stateBasis: "CURRENT_V1_PAPER_STATE",
      evaluatedAt: now.toISOString(),
      marketId,
      outcome: selectedOutcome,
      marketRank: input.ranking.marketRank,
      selectedFairProbability: selected?.fairProbability ?? selectedAtDecision.fairProbability,
      currentExecutableVwap: selected?.executableVwap ?? null,
      currentGrossEdgeUsdPerShare: selected?.grossEdgeUsdPerShare ?? null,
      preflightBlockers: [...new Set(blockers)],
    };
    if (blockers.length > 0 || !currentSnapshot || !selected) {
      return {
        ...compactBase,
        executionConfidence: null,
        evidenceCoverage: null,
        riskAccepted: null,
        riskVetoes: [],
        wouldSubmitOrder: false,
        order: null,
        economics: null,
      };
    }

    const intent = buildNativeV2ShadowExecutionIntent({
      triggerId: input.trigger.id,
      traceId: input.trigger.traceId,
      expiresAt: input.trigger.expiresAt,
      evidenceIds: [
        ...input.trigger.sourceEventIds,
        ...(input.trigger.evidenceIds ?? []),
        currentPair?.UP?.eventId ?? "",
        currentPair?.DOWN?.eventId ?? "",
      ].filter(Boolean),
      fair: input.fair,
      selected,
      snapshot: currentSnapshot,
      now,
    });
    if (!intent) {
      return {
        ...compactBase,
        preflightBlockers: [...new Set([...blockers, "INVALID_SHADOW_EXECUTION_INTENT"])],
        executionConfidence: null,
        evidenceCoverage: null,
        riskAccepted: null,
        riskVetoes: [],
        wouldSubmitOrder: false,
        order: null,
        economics: null,
      };
    }

    const candidate = intent.candidate;
    const exposure = computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket, now.getTime());
    const projected = projectPaperExposures({
      current: exposure,
      positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
      candidate: {
        marketId: candidate.marketId,
        outcome: candidate.outcome,
        quantity: candidate.quantity,
        price: candidate.limitPrice,
        intent: candidate.intent,
        asset,
      },
      nowMs: now.getTime(),
    });
    const referenceRequired = this.referenceRequired.get(marketId) === true;
    const referenceFeedAt = asset ? dateOrNull(this.state.referenceLastMessageAt(asset)) : null;
    const blocked = asset ? this.referenceBlocked.get(asset) : undefined;
    const marketFeedAt = collectMarketFeedAt(currentPair, candidate.outcome, candidate.intent, currentSnapshot.receivedAt);
    const duplicateKey = `${candidate.marketId}:${candidate.outcome}:${candidate.intent}`;
    const ledgerSnapshot = this.ledger.snapshot();
    const decision = decidePaperOrder({
      candidate,
      consensus: intent.consensus,
      snapshotAfterDecisionLatency: currentSnapshot,
      limits: this.riskLimits,
      triggerId: input.trigger.id,
      signalExpiresAt: new Date(input.trigger.expiresAt),
      orderType: "TAKER",
      baseRisk: {
        now,
        marketFeedAt,
        btcFeedAt: asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now,
        referenceFeedAt,
        referenceRequired,
        asset,
        assetExposureUsd: exposure.asset,
        databaseAvailable: await this.journal.healthCheck(),
        timestampDisorder: this.timestampDisorder,
        riskStateAvailable: true,
        dailyPnlUsd: ledgerSnapshot.netTradingPnl,
        drawdownUsd: ledgerSnapshot.drawdown,
        marketExposureUsd: exposure.market,
        totalExposureUsd: exposure.total,
        directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged,
        unhedgedDurationMs: exposure.unhedgedDurationMs,
        intent: candidate.intent,
        projectedMarketExposureUsd: projected.market,
        projectedTotalExposureUsd: projected.total,
        projectedDirectionalExposureUsd: projected.directional,
        projectedUnhedgedExposureUsd: projected.unhedged,
        projectedAssetExposureUsd: projected.asset,
        availableCashUsd: this.ledger.availableCash().toFixed(),
        referenceProviderBlocked: Boolean(blocked),
        marketEntryQualified: this.entryQualified.has(marketId),
        externalSignalsLastMinute: 0,
        lastStrategyTradeAt: this.lastTradeAt,
        duplicateTrade: (this.recentTrades.get(duplicateKey) ?? 0) > now.getTime() - this.riskLimits.strategyCooldownMs,
        experimentEntryOpen: experimentAllowsFirstLeg(this.experiment, now),
        executionErrorsLastHour: this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length,
        externalRiskRecommendedVeto: false,
        shuttingDown: this.state.draining,
      },
    });

    return {
      ...compactBase,
      executionConfidence: intent.confidence,
      evidenceCoverage: intent.evidenceCoverage,
      riskAccepted: decision.risk.accepted,
      riskVetoes: decision.risk.vetoes,
      wouldSubmitOrder: decision.accepted && decision.order !== null,
      order: decision.order ? {
        orderType: decision.order.orderType,
        requestedPrice: decision.order.requestedPrice,
        quantity: decision.order.quantity,
      } : null,
      economics: {
        rawEdgeUsdPerShare: decision.rawEdgeUsdPerShare,
        feeUsdPerShare: decision.feeUsdPerShare,
        slippageUsdPerShare: decision.slippageUsdPerShare,
        costUsdPerShare: decision.costUsdPerShare,
        netEvUsdPerShare: decision.netEvUsdPerShare,
        grossEvUsdTotal: decision.grossEvUsdTotal,
        netEvUsdTotal: decision.netEvUsdTotal,
        executableNotionalUsd: decision.executableNotionalUsd,
        expectedROI: decision.expectedROI,
        bookVwap: decision.bookVwap,
      },
    };
  }

'''
if text.count(anchor) != 1:
    raise SystemExit(f"expected one nativeCompatibilityDecision anchor, got {text.count(anchor)}")
text = text.replace(anchor, method + anchor, 1)

path.write_text(text)
