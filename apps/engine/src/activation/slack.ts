import {
  activationIdempotencyKey,
  grokActivationRequestSchema,
  redactSecrets,
  type ExternalAgentRoleId,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokActivationResult,
} from "@pm/core";

export interface SlackGrokActivationConfig {
  token: string;
  channelId: string;
  apiUrl: string;
  timeoutMs?: number;
  maxAttempts?: number;
  fetchImpl?: typeof fetch;
  delayImpl?: (ms: number) => Promise<void>;
  now?: () => number;
}

interface SlackPostResponse {
  ok?: boolean;
  error?: string;
}

export class SlackGrokActivationBridge implements GrokActivationBridge {
  readonly provider = "slack";
  private readonly timeoutMs: number;
  private readonly maxAttempts: number;
  private readonly fetchImpl: typeof fetch;
  private readonly delayImpl: (ms: number) => Promise<void>;
  private readonly now: () => number;

  constructor(private readonly config: SlackGrokActivationConfig) {
    this.timeoutMs = config.timeoutMs ?? 5_000;
    this.maxAttempts = Math.max(1, Math.min(config.maxAttempts ?? 3, 4));
    this.fetchImpl = config.fetchImpl ?? fetch;
    this.delayImpl = config.delayImpl ?? delay;
    this.now = config.now ?? (() => Date.now());
  }

  async activate(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    const parsed = grokActivationRequestSchema.parse(request);
    if (this.expired(parsed.expiresAt)) {
      return parsed.targetRoles.map((targetRole) => this.expiredResult(parsed.triggerId, targetRole));
    }
    const results: GrokActivationResult[] = [];
    for (const targetRole of parsed.targetRoles) {
      if (this.expired(parsed.expiresAt)) {
        results.push(this.expiredResult(parsed.triggerId, targetRole));
        continue;
      }
      results.push(await this.postRole(parsed, targetRole));
    }
    return results;
  }

  private expired(expiresAt: string): boolean {
    return new Date(expiresAt).getTime() <= this.now();
  }

  private expiredResult(triggerId: string, targetRole: ExternalAgentRoleId): GrokActivationResult {
    return {
      provider: this.provider, ok: false, skipped: true, targetRole,
      requestedAt: new Date(this.now()).toISOString(), sentAt: null, errorCode: "TRIGGER_EXPIRED",
      httpStatus: null, grokWakeObserved: false, idempotencyKey: activationIdempotencyKey(triggerId, targetRole),
    };
  }

  private async postRole(request: GrokActivationRequest, targetRole: ExternalAgentRoleId): Promise<GrokActivationResult> {
    const requestedAt = new Date(this.now()).toISOString();
    const payload = {
      eventType: request.eventType,
      triggerId: request.triggerId,
      traceId: request.traceId,
      marketId: request.marketId,
      asset: request.asset,
      severity: request.severity,
      triggerKind: request.triggerKind,
      createdAt: request.createdAt,
      expiresAt: request.expiresAt,
      targetRoles: [targetRole],
    };
    const body = {
      channel: this.config.channelId,
      text: `GROK_RESEARCH_REQUEST ${targetRole} ${request.triggerKind} ${request.triggerId}`,
      unfurl_links: false,
      unfurl_media: false,
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text: `\`\`\`${JSON.stringify(payload)}\`\`\`` },
        },
      ],
    };
    let lastError = "SLACK_REQUEST_FAILED";
    let httpStatus: number | null = null;
    for (let attempt = 0; attempt < this.maxAttempts; attempt += 1) {
      if (this.expired(request.expiresAt)) {
        return this.expiredResult(request.triggerId, targetRole);
      }
      try {
        const response = await this.fetchImpl(this.config.apiUrl, {
          method: "POST",
          headers: {
            authorization: `Bearer ${this.config.token}`,
            "content-type": "application/json; charset=utf-8",
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.timeoutMs),
        });
        httpStatus = response.status;
        const retryAfter = Number(response.headers.get("retry-after"));
        const retryable = Number.isFinite(retryAfter) && retryAfter > 0 && (response.status === 429 || response.status >= 500);
        if (retryable) {
          await this.delayImpl(Math.min(30_000, retryAfter * 1000));
          if (this.expired(request.expiresAt)) {
            return this.expiredResult(request.triggerId, targetRole);
          }
          lastError = slackErrorCode(`HTTP_${response.status}`);
          continue;
        }
        const json = await response.json().catch(() => ({})) as SlackPostResponse;
        if (response.ok && json.ok === true) {
          return {
            provider: this.provider, ok: true, skipped: false, targetRole, requestedAt,
            sentAt: new Date(this.now()).toISOString(), errorCode: null, httpStatus,
            grokWakeObserved: false, idempotencyKey: activationIdempotencyKey(request.triggerId, targetRole),
          };
        }
        lastError = slackErrorCode(json.error ?? `HTTP_${response.status}`);
        if (response.status >= 400 && response.status < 500 && response.status !== 429) break;
      } catch (error) {
        lastError = error instanceof Error && error.name === "TimeoutError" ? "SLACK_TIMEOUT" : "SLACK_NETWORK_ERROR";
      }
      if (attempt < this.maxAttempts - 1) {
        await this.delayImpl(200 * 2 ** attempt);
        if (this.expired(request.expiresAt)) {
          return this.expiredResult(request.triggerId, targetRole);
        }
      }
    }
    return {
      provider: this.provider, ok: false, skipped: false, targetRole, requestedAt,
      sentAt: null, errorCode: lastError, httpStatus,
      grokWakeObserved: false, idempotencyKey: activationIdempotencyKey(request.triggerId, targetRole),
    };
  }
}

function slackErrorCode(error: string): string {
  const sanitized = String(redactSecrets(error)).replace(/[^A-Z0-9_]/gi, "_").toUpperCase().slice(0, 80);
  return sanitized.length > 0 ? `SLACK_${sanitized}` : "SLACK_REQUEST_FAILED";
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
