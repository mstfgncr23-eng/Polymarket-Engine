import { redactSecrets, type ExternalAgentRoleId } from "@pm/core";

export interface XaiInferenceConfig {
  apiKey: string;
  baseUrl: string;
  model: string;
  timeoutMs?: number;
  maxOutputTokens?: number;
  maxResponseBytes?: number;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

export interface XaiInferenceRequest {
  role: ExternalAgentRoleId;
  systemPrompt: string;
  userPrompt: string;
  responseSchema: Record<string, unknown>;
  signal?: AbortSignal;
  deadlineAt?: number;
}

export interface XaiUsage {
  inputTokens: number | null;
  outputTokens: number | null;
}

export interface XaiInferenceResponse {
  output: unknown;
  usage: XaiUsage;
  model: string;
  latencyMs: number;
}

export type XaiErrorCode =
  | "XAI_AUTH"
  | "XAI_TIMEOUT"
  | "XAI_ABORTED"
  | "XAI_RATE_LIMITED"
  | "XAI_SERVER_ERROR"
  | "XAI_HTTP_ERROR"
  | "XAI_RESPONSE_TOO_LARGE"
  | "XAI_INVALID_JSON"
  | "XAI_MALFORMED_RESPONSE";

export class XaiInferenceError extends Error {
  constructor(
    readonly code: XaiErrorCode,
    message: string,
    readonly httpStatus: number | null = null,
    readonly retryAfterMs: number | null = null,
    readonly retryable = false,
  ) {
    super(message);
    this.name = "XaiInferenceError";
  }
}

/** Minimal chat-completions client. It never logs or includes provider bodies. */
export class XaiInferenceClient {
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => number;
  private readonly timeoutMs: number;
  private readonly maxOutputTokens: number;
  private readonly maxResponseBytes: number;

  constructor(private readonly config: XaiInferenceConfig) {
    if (!config.apiKey.trim()) throw new Error("XAI_API_KEY_REQUIRED");
    this.fetchImpl = config.fetchImpl ?? fetch;
    this.now = config.now ?? (() => Date.now());
    this.timeoutMs = Math.max(250, Math.min(config.timeoutMs ?? 20_000, 120_000));
    this.maxOutputTokens = Math.max(1, Math.min(config.maxOutputTokens ?? 1_024, 16_384));
    this.maxResponseBytes = Math.max(16_384, Math.min(config.maxResponseBytes ?? 1_000_000, 4_000_000));
  }

  async infer(request: XaiInferenceRequest): Promise<XaiInferenceResponse> {
    const started = this.now();
    const controller = new AbortController();
    let timedOut = false;
    const timeoutAt = Math.min(
      this.now() + this.timeoutMs,
      request.deadlineAt ?? Number.POSITIVE_INFINITY,
    );
    const timeoutDelay = Math.max(1, timeoutAt - this.now());
    const timeoutTimer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutDelay);
    timeoutTimer.unref?.();
    const onAbort = () => controller.abort(request.signal?.reason);
    request.signal?.addEventListener("abort", onAbort, { once: true });
    if (request.deadlineAt !== undefined && request.deadlineAt <= this.now()) {
      clearTimeout(timeoutTimer);
      request.signal?.removeEventListener("abort", onAbort);
      throw new XaiInferenceError("XAI_TIMEOUT", "xAI request expired", null, null, false);
    }

    const endpoint = `${this.config.baseUrl.replace(/\/$/, "")}/chat/completions`;
    const body = {
      model: this.config.model,
      temperature: 0,
      max_tokens: this.maxOutputTokens,
      messages: [
        { role: "system", content: request.systemPrompt },
        { role: "user", content: request.userPrompt },
      ],
      response_format: {
        type: "json_schema",
        json_schema: {
          name: `grok_${request.role}`,
          strict: true,
          schema: request.responseSchema,
        },
      },
    };

    try {
      const response = await this.fetchImpl(endpoint, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.config.apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (response.status === 401 || response.status === 403) {
        throw new XaiInferenceError("XAI_AUTH", `xAI authorization failed (${response.status})`, response.status, null, false);
      }
      if (response.status === 408) {
        throw new XaiInferenceError("XAI_TIMEOUT", "xAI request timed out", response.status, null, true);
      }
      if (response.status === 429) {
        throw new XaiInferenceError("XAI_RATE_LIMITED", "xAI rate limit", response.status, retryAfterMs(response.headers.get("retry-after")), true);
      }
      if (response.status >= 500) {
        throw new XaiInferenceError("XAI_SERVER_ERROR", `xAI server error (${response.status})`, response.status, null, true);
      }
      if (!response.ok) {
        throw new XaiInferenceError("XAI_HTTP_ERROR", `xAI request failed (${response.status})`, response.status, null, false);
      }

      const text = await readBoundedResponse(response, this.maxResponseBytes);
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        throw new XaiInferenceError("XAI_INVALID_JSON", "xAI returned invalid JSON", response.status, null, false);
      }
      const content = extractContent(parsed);
      let output: unknown;
      try {
        output = JSON.parse(content);
      } catch {
        throw new XaiInferenceError("XAI_INVALID_JSON", "xAI structured content was not JSON", response.status, null, false);
      }
      const usage = extractUsage(parsed);
      return { output, usage, model: this.config.model, latencyMs: Math.max(0, this.now() - started) };
    } catch (error) {
      if (error instanceof XaiInferenceError) throw error;
      if (controller.signal.aborted) {
        const externallyAborted = request.signal?.aborted === true && !timedOut;
        throw new XaiInferenceError(externallyAborted ? "XAI_ABORTED" : "XAI_TIMEOUT", externallyAborted ? "xAI request aborted" : "xAI request timed out", null, null, false);
      }
      const safeMessage = String(redactSecrets(error instanceof Error ? error.message : "xAI network error"))
        .replace(/https?:\/\/[^\s]+/gi, "[redacted-url]")
        .slice(0, 160);
      throw new XaiInferenceError("XAI_HTTP_ERROR", safeMessage || "xAI network error", null, null, true);
    } finally {
      clearTimeout(timeoutTimer);
      request.signal?.removeEventListener("abort", onAbort);
    }
  }
}

function extractContent(value: unknown): string {
  if (!value || typeof value !== "object") throw new XaiInferenceError("XAI_MALFORMED_RESPONSE", "xAI response shape invalid");
  const choices = (value as { choices?: unknown }).choices;
  if (!Array.isArray(choices) || choices.length === 0) throw new XaiInferenceError("XAI_MALFORMED_RESPONSE", "xAI response contained no choice");
  const message = choices[0] && typeof choices[0] === "object" ? (choices[0] as { message?: unknown }).message : null;
  const content = message && typeof message === "object" ? (message as { content?: unknown }).content : null;
  if (typeof content !== "string" || content.length === 0) throw new XaiInferenceError("XAI_MALFORMED_RESPONSE", "xAI response contained no structured content");
  return content;
}

function extractUsage(value: unknown): XaiUsage {
  const usage = value && typeof value === "object" ? (value as { usage?: unknown }).usage : null;
  if (!usage || typeof usage !== "object") return { inputTokens: null, outputTokens: null };
  const prompt = (usage as { prompt_tokens?: unknown }).prompt_tokens;
  const completion = (usage as { completion_tokens?: unknown }).completion_tokens;
  return {
    inputTokens: typeof prompt === "number" && Number.isFinite(prompt) ? prompt : null,
    outputTokens: typeof completion === "number" && Number.isFinite(completion) ? completion : null,
  };
}

function retryAfterMs(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return null;
  return Math.min(30_000, seconds * 1_000);
}

async function readBoundedResponse(response: Response, maxBytes: number): Promise<string> {
  if (!response.body) {
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > maxBytes) throw new XaiInferenceError("XAI_RESPONSE_TOO_LARGE", "xAI response exceeded bounded size");
    return text;
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > maxBytes) throw new XaiInferenceError("XAI_RESPONSE_TOO_LARGE", "xAI response exceeded bounded size");
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { merged.set(chunk, offset); offset += chunk.byteLength; }
  return new TextDecoder().decode(merged);
}
