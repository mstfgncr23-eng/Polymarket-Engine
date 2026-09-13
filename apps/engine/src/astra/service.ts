import {
  ASTRA_BRAIN_AUTHORITY,
  AstraDailyBudget,
  AstraMemoryStore,
  type AstraClient,
  type AstraBatchRequest,
  type AstraBatchResult,
  type AstraDecision,
  type AstraDecisionRequest,
  type AstraDecisionRequestContext,
  type AstraFeaturePacket,
  type AstraModelResult,
  type AstraStage,
} from "@pm/core";
import { AstraClientError } from "./client.js";

export interface AstraServiceOptions {
  client: AstraClient;
  deepUncertaintyThreshold: number;
  deepDrawdownUtilizationThreshold: number;
  deepUnhedgedUtilizationThreshold: number;
  memory: AstraMemoryStore;
  budget: AstraDailyBudget;
  maxOutputTokens: Record<AstraStage, number>;
  inputUsdPerM: number;
  outputUsdPerM: number;
}

export type AstraServiceResult = {
  decision: AstraDecision;
  stages: AstraStage[];
  invocations: AstraModelResult[];
};

/** Orchestrates Fast -> selective Deep while enforcing a hard daily budget. */
export class AstraDecisionService {
  constructor(private readonly options: AstraServiceOptions) {}

  async decide(input: Omit<AstraDecisionRequestContext, "memory">): Promise<AstraServiceResult> {
    const requestBase = { ...input, memory: this.options.memory.relevant(8, { marketId: input.featurePacket.marketId, asset: input.featurePacket.asset, regime: input.featurePacket.regime }) };
    const fast = await this.invoke({ ...requestBase, stage: "FAST" });
    const invocations = [fast.result];
    const stages: AstraStage[] = ["FAST"];
    let selected = fast.result;
    const packetDisagreement = Math.abs(fast.result.output.fairProbabilityUp - ((input.featurePacket.upMid + (1 - input.featurePacket.downMid)) / 2));
    const highRisk = fast.result.output.uncertainty >= this.options.deepUncertaintyThreshold
      || packetDisagreement >= 0.1
      || input.portfolio.drawdownUtilization >= this.options.deepDrawdownUtilizationThreshold
      || input.portfolio.unhedgedUtilization >= this.options.deepUnhedgedUtilizationThreshold
      || input.featurePacket.regime === "VOLATILE"
      || input.featurePacket.regime === "DISLOCATED";
    if (fast.result.output.action !== "NO_TRADE" && highRisk) {
      const deep = await this.invoke({ ...requestBase, stage: "DEEP", fastProposal: fast.result.output });
      selected = deep.result;
      invocations.push(deep.result);
      stages.push("DEEP");
    }
    const decision: AstraDecision = {
      authority: ASTRA_BRAIN_AUTHORITY,
      action: selected.output.action,
      direction: selected.output.action === "BUY_UP" ? "UP" : selected.output.action === "BUY_DOWN" ? "DOWN" : "FLAT",
      fairProbabilityUp: selected.output.fairProbabilityUp,
      confidence: selected.output.confidence,
      uncertainty: selected.output.uncertainty,
      expiresAt: input.featurePacket.expiresAt,
      reasonCodes: [...selected.output.reasonCodes],
      rationaleCode: selected.output.reasonCodes[0] ?? "NO_TRADE",
      usage: invocations.reduce((sum, result) => ({
        inputTokens: sum.inputTokens + result.usage.inputTokens,
        cachedInputTokens: sum.cachedInputTokens + result.usage.cachedInputTokens,
        cacheWriteTokens: sum.cacheWriteTokens + result.usage.cacheWriteTokens,
        outputTokens: sum.outputTokens + result.usage.outputTokens,
        reasoningTokens: sum.reasoningTokens + result.usage.reasoningTokens,
        costUsd: sum.costUsd + result.usage.costUsd,
      }), { inputTokens: 0, cachedInputTokens: 0, cacheWriteTokens: 0, outputTokens: 0, reasoningTokens: 0, costUsd: 0 }),
      providerRequestId: selected.providerRequestId,
    };
    return { decision, stages, invocations };
  }

  async learn(input: Omit<AstraBatchRequest, "stage">): Promise<AstraBatchResult | null> {
    if (!this.options.client.learn) return null;
    const request: AstraBatchRequest = { ...input, stage: "BATCH" };
    const now = new Date(request.observedAt);
    const inputTokensEstimate = Math.ceil(JSON.stringify(request.aggregate).length / 4);
    const reserve = (inputTokensEstimate / 1_000_000) * this.options.inputUsdPerM
      + (this.options.maxOutputTokens.BATCH / 1_000_000) * this.options.outputUsdPerM;
    if (!this.options.budget.reserve(reserve, now)) throw new AstraBudgetError();
    try {
      const result = await this.options.client.learn(request);
      this.options.budget.settle(reserve, result.usage.costUsd, now);
      return result;
    } catch (error) {
      this.options.budget.settle(reserve, this.isUnknownBillableFailure(error) ? reserve : 0, now);
      throw error;
    }
  }

  private async invoke(request: AstraDecisionRequest): Promise<{ result: AstraModelResult; reservedUsd: number }> {
    const now = new Date(request.featurePacket.observedAt);
    const inputTokensEstimate = Math.ceil(JSON.stringify({ featurePacket: request.featurePacket, memory: request.memory, portfolio: request.portfolio }).length / 4);
    const reserve = (inputTokensEstimate / 1_000_000) * this.options.inputUsdPerM
      + (this.options.maxOutputTokens[request.stage] / 1_000_000) * this.options.outputUsdPerM;
    if (!this.options.budget.reserve(reserve, now)) throw new AstraBudgetError();
    try {
      const result = await this.options.client.decide(request);
      this.options.budget.settle(reserve, result.usage.costUsd, now);
      return { result, reservedUsd: reserve };
    } catch (error) {
      this.options.budget.settle(reserve, this.isUnknownBillableFailure(error) ? reserve : 0, now);
      throw error;
    }
  }

  private isUnknownBillableFailure(error: unknown): boolean {
    return !(error instanceof AstraClientError) || error.dispatchState !== "NOT_DISPATCHED";
  }
}

export class AstraBudgetError extends Error {
  constructor() { super("ASTRA_DAILY_BUDGET_EXHAUSTED"); this.name = "AstraBudgetError"; }
}
