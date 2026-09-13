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
  type AstraStage,
  type AstraUsage,
} from "@pm/core";

const FAST_SYSTEM_PROMPT = "You are Astra Brain V1 Fast. Return only the requested structured economic decision. Use BUY_UP, BUY_DOWN, or NO_TRADE. Do not invent missing facts or expose chain-of-thought.";
const DEEP_SYSTEM_PROMPT = "You are Astra Brain V1 Deep Review. Return only the requested structured economic decision. Treat the exact Fast proposal as a proposal to audit: explicitly challenge it, look for reasons it is wrong, and then confirm it, veto it with NO_TRADE, or flip its direction. Do not invent missing facts or expose chain-of-thought.";
const BATCH_SYSTEM_PROMPT = "You are Astra Batch Learner V1. Return only a bounded list of concise lessons as structured output. Do not change configuration or risk policy. Do not invent missing facts.";

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
  // UTF-8 byte count is a conservative token upper bound for the bounded
  // request payload; the envelope margin covers SDK serialization overhead.
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
  /** Admission is fail-fast by default; any configured waiters are finite and deadline-bound. */
  maxWaiters?: Partial<Record<"FAST" | "DEEP" | "BATCH", number>>;
  pricing: { inputUsdPerM: number; cachedInputUsdPerM: number; cacheWriteUsdPerM?: number; outputUsdPerM: number };
  client?: OpenAI;
}

/** Dedicated Responses API client. No tools, web, files, raw history, or fallback provider. */
export class OpenAIAstraClient implements AstraClient {
  private readonly client: OpenAI;
  private readonly fast: BoundedSemaphore;
  private readonly deep: BoundedSemaphore;
  private readonly batch: BoundedSemaphore;
  constructor(private readonly options: AstraOpenAIOptions) {
    this.client = options.client ?? new OpenAI({ apiKey: options.apiKey });
    this.fast = new BoundedSemaphore(options.maxConcurrency.FAST, options.maxWaiters?.FAST ?? 0);
    this.deep = new BoundedSemaphore(options.maxConcurrency.DEEP, options.maxWaiters?.DEEP ?? 0);
    this.batch = new BoundedSemaphore(1, options.maxWaiters?.BATCH ?? 0);
  }

  async decide(request: AstraDecisionRequest): Promise<AstraModelResult> {
    const semaphore = request.stage === "DEEP" ? this.deep : this.fast;
    const started = performance.now();
    const lease = await semaphore.acquire(request.stage, this.options.timeoutMs[request.stage], request.signal);
    const controller = new AbortController();
    const timeout = lease.remainingMs > 0 ? setTimeout(() => controller.abort(), lease.remainingMs) : undefined;
    const abortRequest = () => controller.abort();
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    let dispatched = false;
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      if (lease.remainingMs <= 0) throw new AstraClientError("ASTRA_REQUEST_TIMEOUT", request.stage, "NOT_DISPATCHED");
      dispatched = true;
      const response = await this.client.responses.parse({
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
      }, { signal: controller.signal });
      if (!response.output_parsed) throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_MISSING", request.stage);
      const output = astraModelOutputSchema.parse(response.output_parsed);
      const usage = response.usage;
      const inputTokens = usage?.input_tokens ?? 0;
      const cachedInputTokens = usage?.input_tokens_details?.cached_tokens ?? 0;
      const outputTokens = usage?.output_tokens ?? 0;
      const reasoningTokens = usage?.output_tokens_details?.reasoning_tokens ?? 0;
      const cacheWriteTokens = usage?.input_tokens_details?.cache_write_tokens ?? 0;
      const costUsd = (Math.max(0, inputTokens - cachedInputTokens - cacheWriteTokens) / 1_000_000) * this.options.pricing.inputUsdPerM
        + (cachedInputTokens / 1_000_000) * this.options.pricing.cachedInputUsdPerM
        + (cacheWriteTokens / 1_000_000) * (this.options.pricing.cacheWriteUsdPerM ?? this.options.pricing.inputUsdPerM)
        + (outputTokens / 1_000_000) * this.options.pricing.outputUsdPerM;
      const safeUsage: AstraUsage = { inputTokens, cachedInputTokens, cacheWriteTokens, outputTokens, reasoningTokens, costUsd };
      return { output, usage: safeUsage, providerRequestId: response.id ?? null, latencyMs: performance.now() - started };
    } catch (error) {
      if (error instanceof AstraClientError) throw error;
      if (controller.signal.aborted) {
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
    } finally {
      if (timeout) clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      lease.release();
    }
  }

  async learn(request: AstraBatchRequest): Promise<AstraBatchResult> {
    const started = performance.now();
    const lease = await this.batch.acquire(request.stage, this.options.timeoutMs.BATCH, request.signal);
    const controller = new AbortController();
    const timeout = lease.remainingMs > 0 ? setTimeout(() => controller.abort(), lease.remainingMs) : undefined;
    const abortRequest = () => controller.abort();
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    let dispatched = false;
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage, "NOT_DISPATCHED");
      if (lease.remainingMs <= 0) throw new AstraClientError("ASTRA_REQUEST_TIMEOUT", request.stage, "NOT_DISPATCHED");
      dispatched = true;
      const response = await this.client.responses.parse({
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
        max_output_tokens: this.options.maxOutputTokens.BATCH,
        store: false,
      }, { signal: controller.signal });
      if (!response.output_parsed) throw new AstraClientError("ASTRA_STRUCTURED_OUTPUT_MISSING", request.stage);
      const output = astraBatchOutputSchema.parse(response.output_parsed);
      const usage = response.usage;
      const inputTokens = usage?.input_tokens ?? 0;
      const cachedInputTokens = usage?.input_tokens_details?.cached_tokens ?? 0;
      const outputTokens = usage?.output_tokens ?? 0;
      const reasoningTokens = usage?.output_tokens_details?.reasoning_tokens ?? 0;
      const cacheWriteTokens = usage?.input_tokens_details?.cache_write_tokens ?? 0;
      const costUsd = (Math.max(0, inputTokens - cachedInputTokens - cacheWriteTokens) / 1_000_000) * this.options.pricing.inputUsdPerM
        + (cachedInputTokens / 1_000_000) * this.options.pricing.cachedInputUsdPerM
        + (cacheWriteTokens / 1_000_000) * (this.options.pricing.cacheWriteUsdPerM ?? this.options.pricing.inputUsdPerM)
        + (outputTokens / 1_000_000) * this.options.pricing.outputUsdPerM;
      return {
        output,
        usage: { inputTokens, cachedInputTokens, cacheWriteTokens, outputTokens, reasoningTokens, costUsd },
        providerRequestId: response.id ?? null,
        latencyMs: performance.now() - started,
      };
    } catch (error) {
      if (error instanceof AstraClientError) throw error;
      if (controller.signal.aborted) {
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage, dispatched ? "DISPATCHED_UNKNOWN" : "NOT_DISPATCHED");
    } finally {
      if (timeout) clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      lease.release();
    }
  }

  concurrencySnapshot(stage: "FAST" | "DEEP" | "BATCH"): { active: number; waiting: number; maxWaiters: number; limit: number } {
    return (stage === "FAST" ? this.fast : stage === "DEEP" ? this.deep : this.batch).snapshot();
  }
}

export type AstraDispatchState = "NOT_DISPATCHED" | "DISPATCHED_UNKNOWN";

export class AstraClientError extends Error {
  constructor(message: string, readonly stage?: AstraStage, readonly dispatchState: AstraDispatchState = "DISPATCHED_UNKNOWN") { super(message); this.name = "AstraClientError"; }
}

class BoundedSemaphore {
  private active = 0;
  private readonly waiting: Array<{ tryGrant: () => boolean }> = [];
  private readonly maxWaiters: number;
  constructor(private readonly limit: number, maxWaiters: number) {
    this.maxWaiters = Number.isFinite(maxWaiters) ? Math.max(0, Math.floor(maxWaiters)) : 0;
  }
  async acquire(stage: AstraStage, timeoutMs: number, signal?: AbortSignal): Promise<{ release: () => void; remainingMs: number }> {
    const deadline = performance.now() + Math.max(0, timeoutMs);
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
