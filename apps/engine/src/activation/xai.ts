import {
  grokActivationRequestSchema,
  activationIdempotencyKey,
  type ExternalAgentRoleId,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokActivationResult,
  type GrokSignalSubmission,
} from "@pm/core";
import { GrokRoleRunner, GrokRoleRuntimeError, emptyRoleRuntimeMetrics, usageNumbers, type GrokRoleContext, type GrokSubmissionMode, type RoleRunOutcome, type RoleRuntimeMetric } from "./role-runner.js";
import { XaiInferenceClient, XaiInferenceError, type XaiInferenceConfig } from "./xai-client.js";

export interface XaiGrokActivationConfig extends Omit<XaiInferenceConfig, "fetchImpl" | "now"> {
  fetchImpl?: typeof fetch;
  contextProvider?: (request: GrokActivationRequest, role: ExternalAgentRoleId) => Promise<GrokRoleContext> | GrokRoleContext;
  submitSignal?: (submission: GrokSignalSubmission, clientId: ExternalAgentRoleId, mode?: GrokSubmissionMode) => Promise<{ status: string; signalId?: string }>;
  onRoleResult?: (request: GrokActivationRequest, outcome: RoleRunOutcome) => void | Promise<void>;
  maxAttempts?: number;
  delayImpl?: (ms: number) => Promise<void>;
  now?: () => Date;
}

/** Opt-in real xAI provider. It uses the same EngineService submission path as MCP. */
export class XaiGrokActivationBridge implements GrokActivationBridge {
  readonly provider = "xai";
  private readonly client: XaiInferenceClient;
  private readonly runner: GrokRoleRunner;
  private readonly maxAttempts: number;
  private readonly delayImpl: (ms: number) => Promise<void>;
  private readonly now: () => Date;
  private readonly runtimeMetrics: RoleRuntimeMetric[];

  constructor(private readonly config: XaiGrokActivationConfig) {
    this.now = config.now ?? (() => new Date());
    this.client = new XaiInferenceClient({ ...config, now: () => this.now().getTime() });
    this.maxAttempts = Math.max(1, Math.min(config.maxAttempts ?? 2, 3));
    this.delayImpl = config.delayImpl ?? delay;
    this.runtimeMetrics = emptyRoleRuntimeMetrics(config.model);
    const submitSignal = config.submitSignal ?? (async () => ({ status: "runtime_not_bound" }));
    const contextProvider = config.contextProvider ?? (() => { throw new GrokRoleRuntimeError("RUNTIME_CONTEXT_UNAVAILABLE"); });
    this.runner = new GrokRoleRunner({ client: this.client, submitSignal, contextProvider, now: this.now });
  }

  async activate(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    const parsed = grokActivationRequestSchema.parse(request);
    const results: GrokActivationResult[] = [];
    for (const role of parsed.targetRoles) results.push(await this.runRole(parsed, role));
    return results;
  }

  metricsSnapshot(): RoleRuntimeMetric[] { return this.runtimeMetrics.map((item) => ({ ...item })); }

  private async runRole(request: GrokActivationRequest, role: ExternalAgentRoleId): Promise<GrokActivationResult> {
    const requestedAt = this.now().toISOString();
    const metric = this.runtimeMetrics.find((item) => item.role === role)!;
    let lastError: unknown = new GrokRoleRuntimeError("XAI_REQUEST_FAILED");
    for (let attempt = 0; attempt < this.maxAttempts; attempt += 1) {
      if (this.expired(request.expiresAt)) {
        lastError = new GrokRoleRuntimeError("TRIGGER_EXPIRED");
        break;
      }
      metric.requests += 1;
      try {
        const outcome = await this.runner.run(request, role);
        await this.config.onRoleResult?.(request, outcome);
        metric.successes += 1;
        metric.totalLatencyMs += outcome.inference.latencyMs;
        const usage = usageNumbers(outcome.inference.usage);
        metric.inputTokens += usage.input;
        metric.outputTokens += usage.output;
        return {
          provider: this.provider, ok: true, skipped: false, targetRole: role,
          requestedAt, sentAt: this.now().toISOString(), errorCode: null, httpStatus: null,
          grokWakeObserved: false, idempotencyKey: activationIdempotencyKey(request.triggerId, role),
        };
      } catch (error) {
        lastError = error;
        if (!this.retryable(error) || attempt >= this.maxAttempts - 1) break;
        const retryAfter = error instanceof GrokRoleRuntimeError && error.cause instanceof XaiInferenceError ? error.cause.retryAfterMs : null;
        const waitMs = Math.min(5_000, retryAfter ?? 200 * 2 ** attempt);
        if (this.now().getTime() + waitMs >= new Date(request.expiresAt).getTime()) break;
        await this.delayImpl(waitMs);
      }
    }
    metric.failures += 1;
    const detail = errorDetail(lastError);
    return {
      provider: this.provider, ok: false, skipped: false, targetRole: role,
      requestedAt, sentAt: null, errorCode: detail.code, httpStatus: detail.httpStatus,
      grokWakeObserved: false, idempotencyKey: activationIdempotencyKey(request.triggerId, role),
    };
  }

  private retryable(error: unknown): boolean {
    const code = error instanceof GrokRoleRuntimeError ? error.code : "";
    return code === "XAI_RATE_LIMITED" || code === "XAI_SERVER_ERROR" || code === "XAI_HTTP_ERROR" || code === "XAI_TIMEOUT";
  }

  private expired(expiresAt: string): boolean { return new Date(expiresAt).getTime() <= this.now().getTime(); }
}

function errorDetail(error: unknown): { code: string; httpStatus: number | null } {
  if (error instanceof GrokRoleRuntimeError) {
    const cause = error.cause instanceof XaiInferenceError ? error.cause : null;
    return { code: sanitizeCode(error.code), httpStatus: cause?.httpStatus ?? null };
  }
  return { code: "XAI_REQUEST_FAILED", httpStatus: null };
}

function sanitizeCode(value: string): string {
  return value.replace(/[^A-Z0-9_]/gi, "_").toUpperCase().slice(0, 80) || "XAI_REQUEST_FAILED";
}

function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
