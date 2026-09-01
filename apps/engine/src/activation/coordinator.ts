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

  constructor(
    private readonly bridge: GrokActivationBridge,
    private readonly limits: { cooldownMs: number; maxPerMinute: number; suppressAtOrBelow: number },
    private readonly now: () => Date = () => new Date(),
  ) {}

  decide(trigger: TriggerEvent): ActivationDecision {
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
    this.latency.set(trigger.id, {
      triggerId: trigger.id,
      triggerCreatedAt: trigger.createdAt,
      activationRequestedAt: now.toISOString(),
      activationSentAt: null,
      firstMcpRequestAt: null,
      firstAcceptedSignalAt: null,
    });
    this.lastByMarketAsset.set(`${trigger.marketId ?? "none"}:${trigger.asset ?? "none"}:${trigger.kind}`, { at: now.getTime(), severity: trigger.severity });
    this.recent.push(now.getTime());
    for (const role of request.targetRoles) this.sent.set(activationIdempotencyKey(trigger.id, role), now.getTime());
  }

  async dispatch(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    try {
      const results = await this.bridge.activate(request);
      const sentAt = results.find((item) => item.sentAt)?.sentAt ?? null;
      const current = this.latency.get(request.triggerId);
      if (current && sentAt && !current.activationSentAt) this.latency.set(request.triggerId, { ...current, activationSentAt: sentAt });
      return results;
    } catch (error) {
      const requestedAt = new Date().toISOString();
      return request.targetRoles.map((targetRole) => ({
        provider: this.bridge.provider, ok: false, skipped: false, targetRole, requestedAt, sentAt: null,
        errorCode: error instanceof Error ? error.name.slice(0, 80) : "ACTIVATION_FAILED",
        httpStatus: null, grokWakeObserved: false as const,
        idempotencyKey: activationIdempotencyKey(request.triggerId, targetRole),
      }));
    }
  }

  noteMcp(triggerIds: readonly string[], at = new Date()): void {
    for (const triggerId of triggerIds) {
      const current = this.latency.get(triggerId);
      if (current && !current.firstMcpRequestAt) this.latency.set(triggerId, { ...current, firstMcpRequestAt: at.toISOString() });
    }
    if (triggerIds.length === 0) {
      for (const [id, current] of this.latency) {
        if (!current.firstMcpRequestAt) this.latency.set(id, { ...current, firstMcpRequestAt: at.toISOString() });
      }
    }
  }

  noteSignal(triggerId: string, at = new Date()): void {
    const current = this.latency.get(triggerId);
    if (current && !current.firstAcceptedSignalAt) this.latency.set(triggerId, { ...current, firstAcceptedSignalAt: at.toISOString() });
  }

  snapshot(): ActivationLatency[] {
    return [...this.latency.values()];
  }
}

export function persistableActivation(result: GrokActivationResult, triggerId: string, reason: string): Record<string, unknown> {
  return {
    triggerId, provider: result.provider, ok: result.ok, skipped: result.skipped, targetRole: result.targetRole,
    requestedAt: result.requestedAt, sentAt: result.sentAt, errorCode: result.errorCode, httpStatus: result.httpStatus,
    grokWakeObserved: false, idempotencyKey: result.idempotencyKey, reason,
  };
}
