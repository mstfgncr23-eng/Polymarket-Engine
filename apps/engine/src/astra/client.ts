import OpenAI from "openai";
import { performance } from "node:perf_hooks";
import { zodTextFormat } from "openai/helpers/zod";
import {
  astraBatchOutputSchema,
  astraModelOutputSchema,
  type AstraClient,
  type AstraBatchRequest,
  type AstraBatchResult,
  type AstraDecisionRequest,
  type AstraModelResult,
  type AstraProviderDispatchContext,
  type AstraStage,
  type AstraUsage,
} from "@pm/core";

const FAST_SYSTEM_PROMPT = "You are Astra Brain V1 Fast. Return only the requested structured economic decision. Use BUY_UP, BUY_DOWN, or NO_TRADE. Do not invent missing facts or expose chain-of-thought.";
const DEEP_SYSTEM_PROMPT = "You are Astra Brain V1 Deep Review. Return only the requested structured economic decision. Treat the exact Fast proposal as a proposal to audit: explicitly challenge it, look for reasons it is wrong, and then confirm it, veto it with NO_TRADE, or flip its direction. Do not invent missing facts or expose chain-of-thought.";
const BATCH_SYSTEM_PROMPT = "You are Astra Batch Learner V1. Return only a bounded list of concise lessons as structured output. Do not change configuration or risk policy. Do not invent missing facts.";

type ProviderResponse = {
  id?: string | null;
  output_parsed?: unknown;
  usage?: {
    input_tokens?: number;
    input_tokens_details?: { cached_tokens?: number; cache_write_tokens?: number };
    output_tokens?: number;
    output_tokens_details?: { reasoning_tokens?: number };
  } | null;
};

function decisionTask(request: AstraDecisionRequest): Record<string, unknown> {
  return {
    task: "Evaluate this already-gated binary-market opportunity.",
    stage: request.stage,
    featurePacket: request.featurePacket,
    memory: request.memory.slice(-8),
    portfolio: request.portfolio,
    ...(request.stage === "DEEP" ? { fastProposal: request.fastProposal } : {}),
  };
}

function boundedInputTokenUpperBound(value: unknown): number {
  return Math.ceil(Buffer.byteLength(JSON.stringify(value), "utf8") * 1.25) + 512;
}

export function estimateAstraDecisionInputTokens(request: AstraDecisionRequest): number {
  return boundedInputTokenUpperBound({
    prompt_cache_key: `astra-brain-v1:${request.stage}`,
    input: [
      { role: "system", content: request.stage === "DEEP" ? DEEP_SYSTEM_PROMPT : FAST_SYSTEM_PROMPT },
      { role: "user", content: JSON.stringify(decisionTask(request)) },
    ],
    text: { format: zodTextFormat(astraModelOutputSchema, "astra_decision") },
  });
}

export function estimateAstraBatchInputTokens(request: AstraBatchRequest): number {
  return boundedInputTokenUpperBound({
    prompt_cache_key: "astra-brain-v1:BATCH",
    input: [
      { role: "system", content: BATCH_SYSTEM_PROMPT },
      { role: "user", content: JSON.stringify({
        task: "Summarize compact settled-outcome statistics into reusable lessons.",
        stage: "BATCH",
        aggregate: request.aggregate,
      }) },
    ],
    text: { format: zodTextFormat(astraBatchOutputSchema, "astra_batch_lessons") },
  });
}

export interface AstraOpenAIOptions {
  apiKey: string;
  model: string;
  reasoning: Record<AstraStage, "low" | "medium" | "high" | "xhigh" | "max">;
  timeoutMs: Record<AstraStage, number>;
  maxOutputTokens: Record<AstraStage, number>;
  maxConcurrency: Record<"FAST" | "DEEP", number>;
  maxWaiters?: Partial<Record<"FAST" | "DEEP" | "BATCH", number>>;
  /** Optional independent bound for queued admission; never arms the provider timer. */
  admissionTimeoutMs?: Partial<Record<"FAST" | "DEEP" | "BATCH", number>>;
  pricing: { inputUsdPerM: number; cachedInputUsdPerM: number; cacheWriteUsdPerM?: number; outputUsdPerM: number };
  client?: OpenAI;
}

/** Dedicated Responses API client. No tools, web, files, raw history, or fallback provider. */
export class OpenAIAstraClient implements AstraClient {
  readonly ownsProviderDispatchBoundary = true as const;
  private readonly client: OpenAI;
  private readonly fast: BoundedSemaphore;
  private readonly deep: BoundedSemaphore;
  private readonly batch: BoundedSemaphore;

  constructor(private readonly options: AstraOpenAIOptions) {
    // Engine policy is one admission -> one SDK attempt.
    this.client = options.client ?? new OpenAI({ apiKey: options.apiKey, maxRetries: 0 });
    this.fast = new BoundedSemaphore(options.maxConcurrency.FAST, options.maxWaiters?.FAST ?? 0, options.admissionTimeoutMs?.FAST ?? options.timeoutMs.FAST);
    this.deep = new BoundedSemaphore(options.maxConcurrency.DEEP, options.maxWaiters?.DEEP ?? 0, options.admissionTimeoutMs?.DEEP ?? options.timeoutMs.DEEP);
    this.batch = new BoundedSemaphore(1, options.maxWaiters?.BATCH ?? 0, options.admissionTimeoutMs?.BATCH ?? options.timeoutMs.BATCH);
  }

  async decide(request: AstraDecisionRequest): Promise<AstraModelResult> {
    const semaphore = request.stage === "DEEP" ? this.deep : this.fast;
    const started = performance.now();
    // This admission wait is finite but is not part of the provider-response
    // deadline. The response timer is armed only by beginDispatch().
    const lease = await semaphore.acquire(request.stage, request.signal);
    const controller = new AbortController();
    let timeout: NodeJS.Timeout | undefined;
    let rejectDeadline: ((error: AstraClientError) => void) | undefined;
    let rejectAbort: ((error: AstraClientError) => void) | undefined;
    let dispatched = false;
    let dispatchStarted = false;
    const deadline = new Promise<never>((_, reject) => { rejectDeadline = reject; });
    const aborted = new Promise<never>((_, reject) => { rejectAbort = reject; });
    const abortRequest = () => {
      controller.abort();
      if (dispatched) rejectAbort?.(new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "DISPATCHED_UNKNOWN", { latencyMs: performance.now() - started }));
    };
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    let providerRequest: Promise<ProviderResponse> | undefined;
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      const beginDispatch = (): boolean => {
        if (dispatchStarted || request.signal?.aborted) return false;
        dispatchStarted = true;
        // Arm the maximum provider-operation deadline at the exact SDK
        // dispatch boundary, before entering the SDK call itself. Admission,
        // semaphore waiting, and authorization durability remain outside this
        // timer; a synchronous SDK call cannot silently consume unbounded
        // provider time before the timer is installed.
        timeout = setTimeout(() => {
          controller.abort();
          rejectDeadline?.(new AstraClientError("ASTRA_REQUEST_TIMEOUT", request.stage, "DISPATCHED_UNKNOWN", { latencyMs: performance.now() - started }));
        }, this.options.timeoutMs[request.stage]);
        try {
          const requestPromise = this.client.responses.parse({
            model: this.options.model,
            prompt_cache_key: `astra-brain-v1:${request.stage}`,
            reasoning: { effort: this.options.reasoning[request.stage] },
            input: [
              { role: "system", content: request.stage === "DEEP" ? DEEP_SYSTEM_PROMPT : FAST_SYSTEM_PROMPT },
              { role: "user", content: JSON.stringify(decisionTask(request)) },
            ],
            text: { format: zodTextFormat(astraModelOutputSchema, "astra_decision") },
            max_output_tokens: this.options.maxOutputTokens[request.stage],
            store: false,
          }, { signal: controller.signal }) as Promise<ProviderResponse>;
          if (!requestPromise || typeof requestPromise.then !== "function") return false;
          providerRequest = requestPromise;
          dispatched = true;
        } catch (error) {
          if (timeout) { clearTimeout(timeout); timeout = undefined; }
          return false;
        }
        return true;
      };
      const context = this.dispatchContext(request);
      if (request.providerDispatchAuthorization) await request.providerDispatchAuthorization(context, beginDispatch);
      else beginDispatch();
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
      if (!providerRequest) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      const response = await Promise.race([providerRequest, deadline, aborted]);
      const measured = usageFromResponse(response, this.options.pricing);
      const details = { providerRequestId: response.id ?? null, usage: measured.usage, knownUsage: measured.known, latencyMs: performance.now() - started };
      if (response.output_parsed == null) throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_MISSING", request.stage, "RESPONSE_RECEIVED", details);
      let output;
      try {
        output = astraModelOutputSchema.parse(response.output_parsed);
      } catch {
        throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_INVALID", request.stage, "RESPONSE_RECEIVED", details);
      }
      return { output, usage: measured.usage, providerRequestId: response.id ?? null, latencyMs: performance.now() - started };
    } catch (error) {
      if (error instanceof AstraClientError) throw error;
      if (controller.signal.aborted) {
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED", { latencyMs: performance.now() - started });
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED", { latencyMs: performance.now() - started });
    } finally {
      if (timeout) clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      lease.release();
    }
  }

  async learn(request: AstraBatchRequest): Promise<AstraBatchResult> {
    const started = performance.now();
    const lease = await this.batch.acquire(request.stage, request.signal);
    const controller = new AbortController();
    let timeout: NodeJS.Timeout | undefined;
    let rejectDeadline: ((error: AstraClientError) => void) | undefined;
    let rejectAbort: ((error: AstraClientError) => void) | undefined;
    let dispatched = false;
    let dispatchStarted = false;
    const deadline = new Promise<never>((_, reject) => { rejectDeadline = reject; });
    const aborted = new Promise<never>((_, reject) => { rejectAbort = reject; });
    const abortRequest = () => {
      controller.abort();
      if (dispatched) rejectAbort?.(new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "DISPATCHED_UNKNOWN", { latencyMs: performance.now() - started }));
    };
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    let providerRequest: Promise<ProviderResponse> | undefined;
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      const beginDispatch = (): boolean => {
        if (dispatchStarted || request.signal?.aborted) return false;
        dispatchStarted = true;
        // The provider deadline begins at this SDK boundary, never while
        // waiting for admission or canonical authorization evidence.
        timeout = setTimeout(() => {
          controller.abort();
          rejectDeadline?.(new AstraClientError("ASTRA_REQUEST_TIMEOUT", request.stage, "DISPATCHED_UNKNOWN", { latencyMs: performance.now() - started }));
        }, this.options.timeoutMs.BATCH);
        try {
          const requestPromise = this.client.responses.parse({
            model: this.options.model,
            prompt_cache_key: "astra-brain-v1:BATCH",
            reasoning: { effort: this.options.reasoning.BATCH },
            input: [
              { role: "system", content: BATCH_SYSTEM_PROMPT },
              { role: "user", content: JSON.stringify({
                task: "Summarize compact settled-outcome statistics into reusable lessons.",
                stage: "BATCH",
                aggregate: request.aggregate,
              }) },
            ],
            text: { format: zodTextFormat(astraBatchOutputSchema, "astra_batch_lessons") },
            max_output_tokens: this.options.maxOutputTokens[request.stage],
            store: false,
          }, { signal: controller.signal }) as Promise<ProviderResponse>;
          if (!requestPromise || typeof requestPromise.then !== "function") return false;
          providerRequest = requestPromise;
          dispatched = true;
        } catch (error) {
          if (timeout) { clearTimeout(timeout); timeout = undefined; }
          return false;
        }
        return true;
      };
      if (request.providerDispatchAuthorization) {
        await request.providerDispatchAuthorization({
          experimentId: request.experimentId ?? "unknown",
          runGeneration: request.runGeneration ?? 0,
          traceId: request.traceId,
          triggerId: request.traceId,
          stage: "BATCH",
          model: this.options.model,
          triggerKind: "BATCH",
          marketId: null,
        }, beginDispatch);
      } else beginDispatch();
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
      if (!providerRequest) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      const response = await Promise.race([providerRequest, deadline, aborted]);
      const measured = usageFromResponse(response, this.options.pricing);
      const details = { providerRequestId: response.id ?? null, usage: measured.usage, knownUsage: measured.known, latencyMs: performance.now() - started };
      if (response.output_parsed == null) throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_MISSING", request.stage, "RESPONSE_RECEIVED", details);
      let output;
      try {
        output = astraBatchOutputSchema.parse(response.output_parsed);
      } catch {
        throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_INVALID", request.stage, "RESPONSE_RECEIVED", details);
      }
      return { output, usage: measured.usage, providerRequestId: response.id ?? null, latencyMs: performance.now() - started };
    } catch (error) {
      if (error instanceof AstraClientError) throw error;
      if (controller.signal.aborted) {
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED", { latencyMs: performance.now() - started });
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED", { latencyMs: performance.now() - started });
    } finally {
      if (timeout) clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      lease.release();
    }
  }

  concurrencySnapshot(stage: "FAST" | "DEEP" | "BATCH"): { active: number; waiting: number; maxWaiters: number; limit: number } {
    return (stage === "FAST" ? this.fast : stage === "DEEP" ? this.deep : this.batch).snapshot();
  }

  private dispatchContext(request: AstraDecisionRequest): AstraProviderDispatchContext {
    return {
      experimentId: request.experimentId ?? "unknown",
      runGeneration: request.runGeneration ?? 0,
      traceId: request.traceId,
      triggerId: request.triggerId,
      stage: request.stage,
      model: this.options.model,
      triggerKind: request.featurePacket.triggerKind,
      marketId: request.featurePacket.marketId,
    };
  }
}

export type AstraDispatchState = "NOT_DISPATCHED" | "DISPATCHED_UNKNOWN" | "RESPONSE_RECEIVED";
export type AstraFailureCode =
  | "TIMEOUT"
  | "CONCURRENCY_SATURATED"
  | "REQUEST_FAILED"
  | "REQUEST_ABORTED"
  | "STRUCTURED_OUTPUT_MISSING"
  | "STRUCTURED_OUTPUT_INVALID"
  | "STAGE_ADMISSION_CLOSED"
  | "ACCOUNTING_FAILED"
  | "PERSISTENCE_FAILED";

export class AstraClientError extends Error {
  readonly failureCode: AstraFailureCode;
  readonly providerRequestId: string | null;
  readonly usage: AstraUsage | null;
  readonly knownUsage: boolean;
  readonly latencyMs: number | null;
  readonly admissionReason: string | null;
  constructor(
    message: string,
    readonly stage?: AstraStage,
    readonly dispatchState: AstraDispatchState = "DISPATCHED_UNKNOWN",
    details: { providerRequestId?: string | null; usage?: AstraUsage; knownUsage?: boolean; latencyMs?: number; admissionReason?: string } = {},
  ) {
    super(message); this.name = "AstraClientError";
    this.failureCode = failureCodeFor(message);
    this.providerRequestId = details.providerRequestId ?? null;
    this.usage = details.usage ?? null;
    this.knownUsage = details.knownUsage === true;
    this.latencyMs = details.latencyMs ?? null;
    this.admissionReason = details.admissionReason ?? null;
  }
}

function failureCodeFor(message: string): AstraFailureCode {
  switch (message) {
    case "ASTRA_REQUEST_TIMEOUT": return "TIMEOUT";
    case "ASTRA_CONCURRENCY_SATURATED": return "CONCURRENCY_SATURATED";
    case "ASTRA_REQUEST_ABORTED": return "REQUEST_ABORTED";
    case "ASTRA_STRUCTURED_OUTPUT_MISSING": return "STRUCTURED_OUTPUT_MISSING";
    case "ASTRA_STRUCTURED_OUTPUT_INVALID": return "STRUCTURED_OUTPUT_INVALID";
    case "ASTRA_PROVIDER_DISPATCH_BLOCKED": return "STAGE_ADMISSION_CLOSED";
    case "ASTRA_PROVIDER_DISPATCH_EVIDENCE_FAILED": return "PERSISTENCE_FAILED";
    default: return "REQUEST_FAILED";
  }
}

function usageFromResponse(response: ProviderResponse, pricing: AstraOpenAIOptions["pricing"]): { usage: AstraUsage; known: boolean } {
  const raw = response.usage;
  const inputTokens = raw?.input_tokens ?? 0;
  const cachedInputTokens = raw?.input_tokens_details?.cached_tokens ?? 0;
  const outputTokens = raw?.output_tokens ?? 0;
  const reasoningTokens = raw?.output_tokens_details?.reasoning_tokens ?? 0;
  const cacheWriteTokens = raw?.input_tokens_details?.cache_write_tokens ?? 0;
  const costUsd = (Math.max(0, inputTokens - cachedInputTokens - cacheWriteTokens) / 1_000_000) * pricing.inputUsdPerM
    + (cachedInputTokens / 1_000_000) * pricing.cachedInputUsdPerM
    + (cacheWriteTokens / 1_000_000) * (pricing.cacheWriteUsdPerM ?? pricing.inputUsdPerM)
    + (outputTokens / 1_000_000) * pricing.outputUsdPerM;
  return { usage: { inputTokens, cachedInputTokens, cacheWriteTokens, outputTokens, reasoningTokens, costUsd }, known: raw != null };
}

class BoundedSemaphore {
  private active = 0;
  private readonly waiting: Array<{ tryGrant: () => boolean }> = [];
  private readonly maxWaiters: number;
  constructor(private readonly limit: number, maxWaiters: number, private readonly admissionTimeoutMs: number) {
    this.maxWaiters = Number.isFinite(maxWaiters) ? Math.max(0, Math.floor(maxWaiters)) : 0;
  }
  async acquire(stage: AstraStage, signal?: AbortSignal): Promise<{ release: () => void; remainingMs: number }> {
    const deadline = performance.now() + Math.max(0, this.admissionTimeoutMs);
    if (signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", stage, "NOT_DISPATCHED");
    if (this.active < this.limit) {
      this.active += 1;
      return { release: this.makeRelease(), remainingMs: deadline - performance.now() };
    }
    if (this.waiting.length >= this.maxWaiters) throw new AstraClientError("ASTRA_CONCURRENCY_SATURATED", stage, "NOT_DISPATCHED");
    return new Promise((resolve, reject) => {
      let settled = false;
      let timer: NodeJS.Timeout | undefined;
      let waiter: { tryGrant: () => boolean };
      const remove = () => {
        const index = this.waiting.indexOf(waiter);
        if (index >= 0) this.waiting.splice(index, 1);
        if (timer) clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      const fail = (message: "ASTRA_REQUEST_TIMEOUT" | "ASTRA_REQUEST_ABORTED") => {
        if (settled) return;
        settled = true;
        remove();
        reject(new AstraClientError(message, stage, "NOT_DISPATCHED"));
      };
      const onAbort = () => fail("ASTRA_REQUEST_ABORTED");
      waiter = {
        tryGrant: () => {
          if (settled) return false;
          if (signal?.aborted) { fail("ASTRA_REQUEST_ABORTED"); return false; }
          if (performance.now() >= deadline) { fail("ASTRA_REQUEST_TIMEOUT"); return false; }
          settled = true;
          remove();
          this.active += 1;
          resolve({ release: this.makeRelease(), remainingMs: deadline - performance.now() });
          return true;
        },
      };
      this.waiting.push(waiter);
      if (signal) signal.addEventListener("abort", onAbort, { once: true });
      timer = setTimeout(() => fail("ASTRA_REQUEST_TIMEOUT"), Math.max(0, deadline - performance.now()));
    });
  }
  snapshot(): { active: number; waiting: number; maxWaiters: number; limit: number } {
    return { active: this.active, waiting: this.waiting.length, maxWaiters: this.maxWaiters, limit: this.limit };
  }
  private makeRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active = Math.max(0, this.active - 1);
      while (this.active < this.limit && this.waiting.length > 0) {
        const waiter = this.waiting.shift();
        if (waiter?.tryGrant()) break;
      }
    };
  }
}
