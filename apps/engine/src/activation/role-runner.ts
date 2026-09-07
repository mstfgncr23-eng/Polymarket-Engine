import { toJSONSchema } from "zod";
import {
  externalAgentRoleDefinitions,
  grokSignalSubmissionSchema,
  type ExternalAgentRoleId,
  type GrokSignalSubmission,
  type GrokActivationRequest,
} from "@pm/core";
import { XaiInferenceClient, XaiInferenceError, type XaiInferenceResponse, type XaiUsage } from "./xai-client.js";
import { isScoutRole } from "./hot-research-cache.js";

export interface GrokRoleContext {
  trigger: Record<string, unknown>;
  market: {
    marketId: string;
    asset: string | null;
    synchronized: boolean;
    snapshots: readonly Record<string, unknown>[];
    features?: Record<string, unknown>;
  } | null;
  reference: Record<string, unknown> | null;
  portfolio: {
    cash: string;
    equity: string;
    reservedCash: string;
    positions: readonly Record<string, unknown>[];
    exposure?: Record<string, unknown>;
    hedgeState?: Record<string, unknown> | null;
  };
  experiment: Record<string, unknown> | null;
  recentStrategyRisk: readonly Record<string, unknown>[];
  /** Fresh, bounded scout research supplied only to Alpha. */
  scoutResearch?: readonly Record<string, unknown>[];
  provenance?: Record<string, unknown>;
  researchToolsAvailable: false;
}

export type GrokSubmissionMode = "mcp" | "scout" | "alpha";

export interface GrokRoleRunnerConfig {
  client: XaiInferenceClient;
  submitSignal: (submission: GrokSignalSubmission, clientId: ExternalAgentRoleId, mode?: GrokSubmissionMode) => Promise<{ status: string; signalId?: string }>;
  contextProvider: (request: GrokActivationRequest, role: ExternalAgentRoleId) => Promise<GrokRoleContext> | GrokRoleContext;
  now?: () => Date;
}

export interface RoleRunOutcome {
  role: ExternalAgentRoleId;
  submission: GrokSignalSubmission;
  submissionResult: { status: string; signalId?: string };
  inference: XaiInferenceResponse;
  context: GrokRoleContext;
}

export class GrokRoleRuntimeError extends Error {
  constructor(readonly code: string, message = code, readonly cause?: unknown) {
    super(message);
    this.name = "GrokRoleRuntimeError";
  }
}

/** One shared role runner; schemas and identity come from externalAgentRoleDefinitions. */
export class GrokRoleRunner {
  private readonly now: () => Date;

  constructor(private readonly config: GrokRoleRunnerConfig) {
    this.now = config.now ?? (() => new Date());
  }

  async run(
    request: GrokActivationRequest,
    role: ExternalAgentRoleId,
    signal?: AbortSignal,
  ): Promise<RoleRunOutcome> {
    const definition = externalAgentRoleDefinitions.find((item) => item.id === role);
    if (!definition) throw new GrokRoleRuntimeError("ROLE_NOT_REGISTERED");
    const deadlineAt = new Date(request.expiresAt).getTime();
    if (!Number.isFinite(deadlineAt) || deadlineAt <= this.now().getTime()) throw new GrokRoleRuntimeError("TRIGGER_EXPIRED");
    const context = await this.config.contextProvider(request, role);
    const boundedContext = boundedContextForPrompt(context, role);
    const systemPrompt = [
      `You are the ${definition.name} role (agentId=${role}).`,
      definition.instructions,
      "Analyze ONLY the supplied market and underlying asset.",
      "Do not invent missing observations; distinguish observed evidence from inference.",
      "Never execute, approve, or suggest an executable trade, wallet action, or order tool call.",
      "Return only the JSON object required by the supplied structured output schema.",
      "researchToolsAvailable is false unless explicitly supplied as true; do not fabricate web/X evidence.",
    ].join(" ");
    const userPrompt = JSON.stringify({ context: boundedContext, outputRequirements: "Return only the role output object; evidenceIds must refer to supplied evidence when present." });
    let inference: XaiInferenceResponse;
    try {
      inference = await this.config.client.infer({
        role,
        systemPrompt,
        userPrompt,
        responseSchema: toJSONSchema(definition.schema as never) as Record<string, unknown>,
        signal,
        deadlineAt,
      });
    } catch (error) {
      if (error instanceof XaiInferenceError) throw new GrokRoleRuntimeError(error.code, error.message, error);
      throw error;
    }
    const parsedOutput = definition.schema.safeParse(inference.output);
    if (!parsedOutput.success) throw new GrokRoleRuntimeError("XAI_SCHEMA_INVALID", "xAI output failed role schema validation", parsedOutput.error);
    const output = parsedOutput.data as Record<string, unknown>;
    if (signal?.aborted || this.now().getTime() >= deadlineAt) throw new GrokRoleRuntimeError("TRIGGER_EXPIRED");
    const outputExpiry = typeof output.expiresAt === "string" ? new Date(output.expiresAt).getTime() : Number.NaN;
    if (!Number.isFinite(outputExpiry) || outputExpiry <= this.now().getTime() || outputExpiry > deadlineAt) {
      throw new GrokRoleRuntimeError("XAI_OUTPUT_EXPIRED", "role output is stale or outlives its trigger");
    }
    const submission = grokSignalSubmissionSchema.parse({
      submissionId: `grok_${request.triggerId}_${role}`,
      triggerId: request.triggerId,
      traceId: request.traceId,
      correlationId: request.traceId,
      marketId: request.marketId ?? "none",
      asset: request.asset ?? null,
      timestamp: this.now().toISOString(),
      agentId: role,
      role,
      output,
    });
    const submissionResult = await this.config.submitSignal(submission, role, isScoutRole(role) ? "scout" : role === "fair_probability_analyst" ? "alpha" : "mcp");
    if (!["accepted", "duplicate"].includes(submissionResult.status)) {
      throw new GrokRoleRuntimeError(`SUBMISSION_${submissionResult.status.toUpperCase()}`, `engine rejected ${role} submission`);
    }
    return { role, submission, submissionResult, inference, context: boundedContext };
  }
}

export interface RoleRuntimeMetric {
  role: ExternalAgentRoleId;
  model: string;
  requests: number;
  successes: number;
  failures: number;
  totalLatencyMs: number;
  inputTokens: number;
  outputTokens: number;
}

export function emptyRoleRuntimeMetrics(model: string): RoleRuntimeMetric[] {
  return externalAgentRoleDefinitions.map(({ id }) => ({
    role: id, model, requests: 0, successes: 0, failures: 0, totalLatencyMs: 0, inputTokens: 0, outputTokens: 0,
  }));
}

export function boundedContextForPrompt(context: GrokRoleContext, role: ExternalAgentRoleId): GrokRoleContext {
  const base = structuredClone(context);
  base.trigger = {
    id: base.trigger.id, traceId: base.trigger.traceId, marketId: base.trigger.marketId ?? null,
    asset: base.trigger.asset ?? null, kind: base.trigger.kind, severity: base.trigger.severity,
    evidenceIds: Array.isArray(base.trigger.evidenceIds) ? base.trigger.evidenceIds.slice(0, 32) : [],
    sourceEventIds: Array.isArray(base.trigger.sourceEventIds) ? base.trigger.sourceEventIds.slice(0, 32) : [],
    createdAt: base.trigger.createdAt, expiresAt: base.trigger.expiresAt,
  };
  if (base.market) {
    base.market.synchronized = base.market.synchronized === true;
    base.market.snapshots = base.market.synchronized ? base.market.snapshots.slice(0, 2).map((snapshot) => {
      const value = { ...snapshot };
      for (const key of ["bids", "asks"]) if (Array.isArray(value[key])) value[key] = (value[key] as unknown[]).slice(0, 20);
      return value;
    }) : [];
  }
  base.portfolio.positions = base.portfolio.positions.slice(0, 100);
  base.recentStrategyRisk = base.recentStrategyRisk.slice(0, 20);
  base.scoutResearch = role === "fair_probability_analyst"
    ? (base.scoutResearch ?? []).slice(0, 3).map((item) => {
      const value = { ...item };
      if (Array.isArray(value.evidenceIds)) value.evidenceIds = (value.evidenceIds as unknown[]).slice(0, 32);
      if (Array.isArray(value.sourceEventIds)) value.sourceEventIds = (value.sourceEventIds as unknown[]).slice(0, 32);
      if (Array.isArray(value.featureIds)) value.featureIds = (value.featureIds as unknown[]).slice(0, 32);
      if (Array.isArray(value.derivedFrom)) value.derivedFrom = (value.derivedFrom as unknown[]).slice(0, 32);
      return value;
    })
    : [];
  if (role !== "fair_probability_analyst") base.provenance = undefined;
  if (!["crypto_market_scanner", "news_x_intelligence_scanner", "fair_probability_analyst"].includes(role)) base.reference = null;
  if (role !== "risk_hedge_analyst") {
    base.portfolio = { cash: "[omitted]", equity: "[omitted]", reservedCash: "[omitted]", positions: [], hedgeState: null };
  }
  if (role === "market_microstructure_scanner" || role === "crypto_market_scanner" || role === "news_x_intelligence_scanner" || role === "fair_probability_analyst") {
    base.portfolio = { cash: "[omitted]", equity: "[omitted]", reservedCash: "[omitted]", positions: [], hedgeState: null };
    base.recentStrategyRisk = [];
  }
  if (role !== "strategy_supervisor") base.recentStrategyRisk = [];
  if (role !== "fair_probability_analyst") base.scoutResearch = [];
  return base;
}

export function usageNumbers(usage: XaiUsage): { input: number; output: number } {
  return { input: usage.inputTokens ?? 0, output: usage.outputTokens ?? 0 };
}
