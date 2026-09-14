import {
  ASTRA_BRAIN_AUTHORITY,
  AstraMemoryStore,
  type AstraClient,
  type AstraBatchRequest,
  type AstraBatchResult,
  type AstraDecision,
  type AstraDecisionRequest,
  type AstraDecisionRequestContext,
  type AstraModelOutput,
  type AstraModelResult,
  type AstraProviderDispatchContext,
  type AstraStage,
} from "@pm/core";
import { AstraClientError } from "./client.js";

export interface AstraStageSuccess {
  stage: AstraStage;
  traceId: string;
  experimentId?: string | null;
  runGeneration?: number;
  result: AstraModelResult | AstraBatchResult;
}

export interface AstraStageFailure {
  stage: AstraStage;
  traceId: string;
  triggerId: string;
  experimentId?: string | null;
  runGeneration?: number;
  error: AstraClientError;
}

export interface AstraServiceOptions {
  client: AstraClient;
  deepUncertaintyThreshold: number;
  deepDrawdownUtilizationThreshold: number;
  deepUnhedgedUtilizationThreshold: number;
  memory: AstraMemoryStore;
  beforeStageDispatch?: (stage: AstraStage) => Promise<boolean>;
  onStageSuccess?: (input: AstraStageSuccess) => Promise<boolean | void>;
  onStageFailure?: (input: AstraStageFailure) => Promise<boolean | void>;

  /** @deprecated Ignored. Kept only so an old EngineService build can recover while the gate is removed. */
  budget?: unknown;
  /** @deprecated Budget-reservation inputs are no longer consumed by this service. */
  maxOutputTokens?: Record<AstraStage, number>;
  /** @deprecated Actual provider usage remains authoritative for cost accounting. */
  inputUsdPerM?: number;
  /** @deprecated Actual provider usage remains authoritative for cost accounting. */
  cachedInputUsdPerM?: number;
  /** @deprecated Actual provider usage remains authoritative for cost accounting. */
  cacheWriteUsdPerM?: number;
  /** @deprecated Actual provider usage remains authoritative for cost accounting. */
  outputUsdPerM?: number;
  /** @deprecated No calendar-day budget clock is used. */
  nowFn?: () => Date;
  /** @deprecated No budget reservation/settlement notifications are emitted. */
  onBudgetChange?: (stage: AstraStage, change?: unknown) => Promise<void>;
  model?: string;
}

export type AstraServiceResult = {
  decision: AstraDecision;
  stages: AstraStage[];
  invocations: AstraModelResult[];
};

/** Orchestrates Fast -> selective Deep behind deterministic admission gates. */
export class AstraDecisionService {
  constructor(private readonly options: AstraServiceOptions) {}

  async decide(input: Omit<AstraDecisionRequestContext, "memory">): Promise<AstraServiceResult> {
    const requestBase = {
      ...input,
      memory: this.options.memory.relevant(8, {
        marketId: input.featurePacket.marketId,
        asset: input.featurePacket.asset,
        regime: input.featurePacket.regime,
      }),
    };
    const fast = await this.invoke({ ...requestBase, stage: "FAST" });
    const invocations = [fast];
    const stages: AstraStage[] = ["FAST"];
    let selected = fast;
    const packetDisagreement = Math.abs(
      fast.output.fairProbabilityUp - ((input.featurePacket.upMid + (1 - input.featurePacket.downMid)) / 2),
    );
    const highRisk = fast.output.uncertainty >= this.options.deepUncertaintyThreshold
      || packetDisagreement >= 0.1
      || input.portfolio.drawdownUtilization >= this.options.deepDrawdownUtilizationThreshold
      || input.portfolio.unhedgedUtilization >= this.options.deepUnhedgedUtilizationThreshold
      || input.featurePacket.regime === "VOLATILE"
      || input.featurePacket.regime === "DISLOCATED";
    if (fast.output.action !== "NO_TRADE" && highRisk) {
      let deep: AstraModelResult;
      try {
        deep = await this.invoke({ ...requestBase, stage: "DEEP", fastProposal: fast.output });
      } catch (error) {
        const accountingError = error instanceof AstraStageAccountingError ? error : undefined;
        const providerError = error instanceof AstraClientError ? error : accountingError?.providerError;
        throw new AstraDeepFailureError(
          deepFailureReason(providerError ?? error),
          fast.output,
          providerError,
          accountingError !== undefined,
        );
      }
      selected = deep;
      invocations.push(deep);
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
    if (this.options.beforeStageDispatch && !(await this.options.beforeStageDispatch("BATCH"))) return null;
    let result: AstraBatchResult;
    try {
      const clientOwnsBoundary = this.options.client.ownsProviderDispatchBoundary === true;
      if (request.providerDispatchAuthorization && !clientOwnsBoundary) {
        let began = false;
        await request.providerDispatchAuthorization(
          this.batchDispatchContext(request),
          () => { began = true; return true; },
        );
        if (!began) throw new AstraClientError("ASTRA_PROVIDER_DISPATCH_BLOCKED", request.stage, "NOT_DISPATCHED");
      }
      result = await this.options.client.learn(request);
    } catch (error) {
      await this.notifyStageFailure({
        stage: request.stage,
        traceId: request.traceId,
        triggerId: request.traceId,
        experimentId: request.experimentId,
        runGeneration: request.runGeneration,
        error: asAstraClientError(error, request.stage),
      });
      throw error;
    }
    await this.notifyStageSuccessWithRecovery({
      stage: "BATCH",
      traceId: request.traceId,
      experimentId: request.experimentId,
      runGeneration: request.runGeneration,
      result,
    }, request.traceId);
    return result;
  }

  private async invoke(request: AstraDecisionRequest): Promise<AstraModelResult> {
    if (this.options.beforeStageDispatch && !(await this.options.beforeStageDispatch(request.stage))) {
      throw new AstraStageAdmissionError(request.stage);
    }
    let result: AstraModelResult;
    try {
      const clientOwnsBoundary = this.options.client.ownsProviderDispatchBoundary === true;
      if (request.providerDispatchAuthorization && !clientOwnsBoundary) {
        let began = false;
        await request.providerDispatchAuthorization(
          this.dispatchContext(request),
          () => { began = true; return true; },
        );
        if (!began) throw new AstraClientError("ASTRA_PROVIDER_DISPATCH_BLOCKED", request.stage, "NOT_DISPATCHED");
      }
      result = await this.options.client.decide(request);
    } catch (error) {
      await this.notifyStageFailure({
        stage: request.stage,
        traceId: request.traceId,
        triggerId: request.triggerId,
        experimentId: request.experimentId,
        runGeneration: request.runGeneration,
        error: asAstraClientError(error, request.stage),
      });
      throw error;
    }
    await this.notifyStageSuccessWithRecovery({
      stage: request.stage,
      traceId: request.traceId,
      experimentId: request.experimentId,
      runGeneration: request.runGeneration,
      result,
    }, request.triggerId);
    return result;
  }

  private async notifyStageSuccess(input: AstraStageSuccess): Promise<void> {
    let accounted: boolean | void;
    try {
      accounted = await this.options.onStageSuccess?.(input);
    } catch (error) {
      throw new AstraStageAccountingError(input.stage, input.result, error);
    }
    if (accounted === false) throw new AstraStageAccountingError(input.stage, input.result);
  }

  private async notifyStageSuccessWithRecovery(input: AstraStageSuccess, triggerId: string): Promise<void> {
    try {
      await this.notifyStageSuccess(input);
    } catch (error) {
      const accounting = error instanceof AstraStageAccountingError
        ? error
        : new AstraStageAccountingError(input.stage, input.result, error);
      await this.notifyStageFailure({
        stage: input.stage,
        traceId: input.traceId,
        triggerId,
        experimentId: input.experimentId,
        runGeneration: input.runGeneration,
        error: asAstraClientError(accounting, input.stage),
      });
      throw accounting;
    }
  }

  private async notifyStageFailure(input: AstraStageFailure): Promise<void> {
    let accounted: boolean | void;
    try {
      accounted = await this.options.onStageFailure?.(input);
    } catch (error) {
      throw new AstraStageAccountingError(input.stage, undefined, error, input.error);
    }
    if (accounted === false) throw new AstraStageAccountingError(input.stage, undefined, undefined, input.error);
  }

  private dispatchContext(request: AstraDecisionRequest): AstraProviderDispatchContext {
    return {
      experimentId: request.experimentId ?? "unknown",
      runGeneration: request.runGeneration ?? 0,
      traceId: request.traceId,
      triggerId: request.triggerId,
      stage: request.stage,
      model: this.options.model ?? "astra",
      triggerKind: request.featurePacket.triggerKind,
      marketId: request.featurePacket.marketId,
    };
  }

  private batchDispatchContext(request: AstraBatchRequest): AstraProviderDispatchContext {
    return {
      experimentId: request.experimentId ?? "unknown",
      runGeneration: request.runGeneration ?? 0,
      traceId: request.traceId,
      triggerId: request.traceId,
      stage: "BATCH",
      model: this.options.model ?? "astra",
      triggerKind: "BATCH",
      marketId: null,
    };
  }
}

export class AstraStageAccountingError extends Error {
  constructor(
    readonly stage: AstraStage,
    readonly result?: AstraModelResult | AstraBatchResult,
    readonly cause?: unknown,
    readonly providerError?: AstraClientError,
  ) { super(`ASTRA_STAGE_ACCOUNTING_FAILED:${stage}`); this.name = "AstraStageAccountingError"; }
}

export class AstraStageAdmissionError extends Error {
  constructor(readonly stage: AstraStage) {
    super(`ASTRA_STAGE_ADMISSION_CLOSED:${stage}`);
    this.name = "AstraStageAdmissionError";
  }
}

export type AstraDeepFailureReason = "DEEP_TIMEOUT" | "DEEP_INVALID_RESPONSE" | "DEEP_FAILED";
export class AstraDeepFailureError extends Error {
  constructor(
    readonly reason: AstraDeepFailureReason,
    readonly fastProposal: AstraModelOutput,
    readonly providerError?: AstraClientError,
    readonly accountingFailed = false,
  ) { super(reason); this.name = "AstraDeepFailureError"; }
}

function deepFailureReason(error: unknown): AstraDeepFailureReason {
  if (error instanceof AstraClientError && error.message === "ASTRA_REQUEST_TIMEOUT") return "DEEP_TIMEOUT";
  if (error instanceof AstraClientError && ["ASTRA_STRUCTURED_OUTPUT_MISSING", "ASTRA_STRUCTURED_OUTPUT_INVALID"].includes(error.message)) {
    return "DEEP_INVALID_RESPONSE";
  }
  return "DEEP_FAILED";
}

function asAstraClientError(error: unknown, stage: AstraStage): AstraClientError {
  if (error instanceof AstraClientError) return error;
  if (error instanceof AstraStageAccountingError && error.result) {
    return new AstraClientError("ASTRA_REQUEST_FAILED", stage, "RESPONSE_RECEIVED", {
      providerRequestId: error.result.providerRequestId,
      usage: error.result.usage,
      knownUsage: true,
      latencyMs: error.result.latencyMs,
    });
  }
  return new AstraClientError("ASTRA_REQUEST_FAILED", stage, "DISPATCHED_UNKNOWN");
}

/**
 * Historical source-compatibility only. The service never throws this after
 * removal of the calendar-day budget gate.
 * @deprecated
 */
export class AstraBudgetError extends Error {
  constructor() {
    super("ASTRA_DAILY_BUDGET_REMOVED");
    this.name = "AstraBudgetError";
  }
}
