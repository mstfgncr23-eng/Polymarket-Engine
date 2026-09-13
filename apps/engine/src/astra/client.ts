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

export interface AstraOpenAIOptions {
  apiKey: string;
  model: string;
  reasoning: Record<AstraStage, "low" | "medium" | "high" | "xhigh" | "max">;
  timeoutMs: Record<AstraStage, number>;
  maxOutputTokens: Record<AstraStage, number>;
  maxConcurrency: Record<"FAST" | "DEEP", number>;
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
    this.fast = new BoundedSemaphore(options.maxConcurrency.FAST);
    this.deep = new BoundedSemaphore(options.maxConcurrency.DEEP);
    this.batch = new BoundedSemaphore(1);
  }

  async decide(request: AstraDecisionRequest): Promise<AstraModelResult> {
    if (request.stage === "BATCH") throw new AstraClientError("ASTRA_BATCH_REQUIRES_LEARN", request.stage);
    const semaphore = request.stage === "DEEP" ? this.deep : this.fast;
    const release = await semaphore.acquire();
    const started = performance.now();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.options.timeoutMs[request.stage]);
    const abortRequest = () => controller.abort();
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage);
      const response = await this.client.responses.parse({
        model: this.options.model,
        prompt_cache_key: `astra-brain-v1:${request.stage}`,
        reasoning: { effort: this.options.reasoning[request.stage] },
        input: [
          { role: "system", content: "You are Astra Brain V1. Return only the requested structured economic decision. Use BUY_UP, BUY_DOWN, or NO_TRADE. Do not invent missing facts." },
          { role: "user", content: JSON.stringify({
            task: "Evaluate this already-gated binary-market opportunity.",
            stage: request.stage,
            featurePacket: request.featurePacket,
            memory: request.memory.slice(-8),
            portfolio: request.portfolio,
          }) },
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
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage);
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage);
    } finally {
      clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      release();
    }
  }

  async learn(request: AstraBatchRequest): Promise<AstraBatchResult> {
    const release = await this.batch.acquire();
    const started = performance.now();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.options.timeoutMs.BATCH);
    const abortRequest = () => controller.abort();
    request.signal?.addEventListener("abort", abortRequest, { once: true });
    try {
      if (request.signal?.aborted) throw new AstraClientError("ASTRA_REQUEST_ABORTED", request.stage);
      const response = await this.client.responses.parse({
        model: this.options.model,
        prompt_cache_key: "astra-brain-v1:BATCH",
        reasoning: { effort: this.options.reasoning.BATCH },
        input: [
          { role: "system", content: "You are Astra Batch Learner V1. Return only a bounded list of concise lessons as structured output. Do not change configuration or risk policy. Do not invent missing facts." },
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
        throw new AstraClientError(request.signal?.aborted ? "ASTRA_REQUEST_ABORTED" : "ASTRA_REQUEST_TIMEOUT", request.stage);
      }
      throw new AstraClientError("ASTRA_REQUEST_FAILED", request.stage);
    } finally {
      clearTimeout(timeout);
      request.signal?.removeEventListener("abort", abortRequest);
      release();
    }
  }
}

export class AstraClientError extends Error {
  constructor(message: string, readonly stage?: AstraStage) { super(message); this.name = "AstraClientError"; }
}

class BoundedSemaphore {
  private active = 0;
  private readonly waiting: Array<() => void> = [];
  constructor(private readonly limit: number) {}
  async acquire(): Promise<() => void> {
    if (this.active >= this.limit) await new Promise<void>((resolve) => this.waiting.push(resolve));
    this.active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active = Math.max(0, this.active - 1);
      this.waiting.shift()?.();
    };
  }
}
