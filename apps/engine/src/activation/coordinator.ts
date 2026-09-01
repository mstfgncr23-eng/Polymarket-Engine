import {
  activationIdempotencyKey,
  activationRequestFromTrigger,
  type ActivationLatency,
  type ExternalAgentRoleId,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokActivationResult,
  type TriggerEvent,
} from "@pm/core";

export interface ActivationDecision {
  activate: boolean;
  reason: "SEND" | "DUPLICATE" | "COOLDOWN" | "RATE_LIMITED" | "EXPIRED" | "SEVERITY_SUPPRESSED" | "NO_ROLES";
  request: GrokActivationRequest | null;
}

export class GrokActivationCoordinator {
  private readonly sent = new Map<string, number>();
  private readonly lastByMarketAsset = new Map<string, { at: number; severity: number }>();
  private readonly recent: number[] = [];
  readonly latency = new Map<string, ActivationLatency>();
  private readonly unattributedMcp: Array<{ at: string; tool: string | null }> = [];
  private readonly retryAfterUntil = new Map<string, number>();

  constructor(
    private readonly bridge: GrokActivationBridge,
    private readonly limits: { cooldownMs: number; maxPerMinute: number; suppressAtOrBelow: number; memoryLimit?: number; fanout?: number },
    private readonly now: () => Date = () => new Date(),
  ) {}

  decide(trigger: TriggerEvent): ActivationDecision {
    this.prune();
    const request = activationRequestFromTrigger(trigger);
    const now = this.now().getTime();
    if (new Date(trigger.expiresAt).getTime() <= now) return { activate: false, reason: "EXPIRED", request };
    if (request.targetRoles.length === 0) return { activate: false, reason: "NO_ROLES", request };
    const scope = `${trigger.marketId ?? "none"}:${trigger.asset ?? "none"}:${trigger.kind}`;
    const previous = this.lastByMarketAsset.get(scope);
    if (previous && now - previous.at < this.limits.cooldownMs && trigger.severity <= previous.severity && trigger.severity <= this.limits.suppressAtOrBelow) {
      return { activate: false, reason: "SEVERITY_SUPPRESSED", request };
    }
    const recent = this.recent.filter((at) => now - at < 60_000);
    this.recent.length = 0;
    this.recent.push(...recent);
    if (recent.length >= this.limits.maxPerMinute) return { activate: false, reason: "RATE_LIMITED", request };
    const duplicate = request.targetRoles.every((role) => this.sent.has(activationIdempotencyKey(trigger.id, role)));
    if (duplicate) return { activate: false, reason: "DUPLICATE", request };
    return { activate: true, reason: "SEND", request };
  }

  markRequested(trigger: TriggerEvent, request: GrokActivationRequest): void {
    const now = this.now();
    this.ensureLatency(trigger, now.toISOString());
    this.lastByMarketAsset.set(`${trigger.marketId ?? "none"}:${trigger.asset ?? "none"}:${trigger.kind}`, { at: now.getTime(), severity: trigger.severity });
    this.recent.push(now.getTime());
    for (const role of request.targetRoles) this.sent.set(activationIdempotencyKey(trigger.id, role), now.getTime());
    this.prune();
  }

  /** Record a trigger for MCP attribution without requesting a Grok wake. */
  ensureLatency(trigger: TriggerEvent, activationRequestedAt: string | null = null): void {
    if (this.latency.has(trigger.id)) return;
    this.latency.set(trigger.id, {
      triggerId: trigger.id,
      triggerCreatedAt: trigger.createdAt,
      activationRequestedAt,
      activationSentAt: null,
      firstMcpRequestAt: null,
      firstAcceptedSignalAt: null,
    });
    this.prune();
  }

  async dispatch(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    const now = this.now().getTime();
    if (new Date(request.expiresAt).getTime() <= now) {
      return request.targetRoles.map((targetRole) => ({
        provider: this.bridge.provider, ok: false, skipped: true, targetRole,
        requestedAt: new Date(now).toISOString(), sentAt: null, errorCode: "TRIGGER_EXPIRED",
        httpStatus: null, grokWakeObserved: false as const,
        idempotencyKey: activationIdempotencyKey(request.triggerId, targetRole),
      }));
    }
    const fanout = Math.max(1, this.limits.fanout ?? 3);
    const results: GrokActivationResult[] = [];
    const roles = [...request.targetRoles];
    for (let i = 0; i < roles.length; i += fanout) {
      const slice = roles.slice(i, i + fanout);
      const batch = await Promise.all(slice.map(async (role) => {
        if (new Date(request.expiresAt).getTime() <= this.now().getTime()) {
          return {
            provider: this.bridge.provider, ok: false, skipped: true, targetRole: role,
            requestedAt: this.now().toISOString(), sentAt: null, errorCode: "TRIGGER_EXPIRED",
            httpStatus: null, grokWakeObserved: false as const,
            idempotencyKey: activationIdempotencyKey(request.triggerId, role),
          } satisfies GrokActivationResult;
        }
        const until = this.retryAfterUntil.get(role) ?? 0;
        if (until > this.now().getTime()) {
          return {
            provider: this.bridge.provider, ok: false, skipped: true, targetRole: role,
            requestedAt: this.now().toISOString(), sentAt: null, errorCode: "RETRY_AFTER",
            httpStatus: 429, grokWakeObserved: false as const,
            idempotencyKey: activationIdempotencyKey(request.triggerId, role),
          } satisfies GrokActivationResult;
        }
        const single: GrokActivationRequest = { ...request, targetRoles: [role] };
        try {
          const [result] = await this.bridge.activate(single);
          if (result?.httpStatus === 429) {
            this.retryAfterUntil.set(role, this.now().getTime() + 5_000);
          }
          return result ?? {
            provider: this.bridge.provider, ok: false, skipped: false, targetRole: role,
            requestedAt: this.now().toISOString(), sentAt: null, errorCode: "ACTIVATION_EMPTY",
            httpStatus: null, grokWakeObserved: false as const,
            idempotencyKey: activationIdempotencyKey(request.triggerId, role),
          };
        } catch (error) {
          return {
            provider: this.bridge.provider, ok: false, skipped: false, targetRole: role,
            requestedAt: this.now().toISOString(), sentAt: null,
            errorCode: error instanceof Error ? error.name.slice(0, 80) : "ACTIVATION_FAILED",
            httpStatus: null, grokWakeObserved: false as const,
            idempotencyKey: activationIdempotencyKey(request.triggerId, role),
          } satisfies GrokActivationResult;
        }
      }));
      results.push(...batch);
    }
    const sentAt = results.find((item) => item.sentAt)?.sentAt ?? null;
    const current = this.latency.get(request.triggerId);
    if (current && sentAt && !current.activationSentAt) this.latency.set(request.triggerId, { ...current, activationSentAt: sentAt });
    return results;
  }

  restoreLatency(items: readonly ActivationLatency[]): void {
    this.latency.clear();
    for (const item of items) this.latency.set(item.triggerId, structuredClone(item));
    this.prune();
  }

  noteMcp(triggerIds: readonly string[], at = new Date()): void {
    for (const triggerId of triggerIds) {
      if (!triggerId) continue;
      const current = this.latency.get(triggerId);
      if (current && !current.firstMcpRequestAt) this.latency.set(triggerId, { ...current, firstMcpRequestAt: at.toISOString() });
    }
  }

  noteUnattributedMcp(tool: string | null, at = new Date()): void {
    this.unattributedMcp.push({ at: at.toISOString(), tool });
    if (this.unattributedMcp.length > 200) this.unattributedMcp.splice(0, this.unattributedMcp.length - 200);
  }

  unattributedSnapshot(): { lastAt: string | null; count: number; lastTool: string | null } {
    const last = this.unattributedMcp.at(-1);
    return { lastAt: last?.at ?? null, count: this.unattributedMcp.length, lastTool: last?.tool ?? null };
  }

  noteSignal(triggerId: string, at = new Date()): void {
    const current = this.latency.get(triggerId);
    if (current && !current.firstAcceptedSignalAt) this.latency.set(triggerId, { ...current, firstAcceptedSignalAt: at.toISOString() });
  }

  snapshot(): ActivationLatency[] {
    this.prune();
    return [...this.latency.values()];
  }

  footprint(): { sent: number; lastByMarketAsset: number; latency: number; recent: number } {
    this.prune();
    return { sent: this.sent.size, lastByMarketAsset: this.lastByMarketAsset.size, latency: this.latency.size, recent: this.recent.length };
  }

  private prune(): void {
    const now = this.now().getTime();
    const limit = this.limits.memoryLimit ?? 500;
    for (const [key, at] of this.sent) if (now - at > 3_600_000) this.sent.delete(key);
    for (const [key, item] of this.lastByMarketAsset) if (now - item.at > 3_600_000) this.lastByMarketAsset.delete(key);
    for (const [role, until] of this.retryAfterUntil) if (until <= now) this.retryAfterUntil.delete(role);
    const latencies = [...this.latency.entries()].sort((a, b) => new Date(a[1].triggerCreatedAt).getTime() - new Date(b[1].triggerCreatedAt).getTime());
    while (latencies.length > limit) {
      const oldest = latencies.shift();
      if (oldest) this.latency.delete(oldest[0]);
    }
  }
}

export function persistableActivation(result: GrokActivationResult, triggerId: string, reason: string): Record<string, unknown> {
  return {
    triggerId, provider: result.provider, ok: result.ok, skipped: result.skipped, targetRole: result.targetRole,
    requestedAt: result.requestedAt, sentAt: result.sentAt, errorCode: result.errorCode, httpStatus: result.httpStatus,
    grokWakeObserved: false, idempotencyKey: result.idempotencyKey, reason,
  };
}
