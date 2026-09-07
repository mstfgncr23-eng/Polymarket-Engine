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

const SENT_TTL_MS = 3_600_000;
export const ACTIVATION_SCOPE_TTL_MS = 3_600_000;
const SCOPE_TTL_MS = ACTIVATION_SCOPE_TTL_MS;

export class GrokActivationCoordinator {
  private readonly sent = new Map<string, number>();
  private readonly sentOrder: Array<{ key: string; at: number } | undefined> = [];
  private sentHead = 0;
  private readonly lastByMarketAsset = new Map<string, { at: number; severity: number }>();
  private readonly lastOrder: Array<{ key: string; at: number } | undefined> = [];
  private lastHead = 0;
  private readonly recent: number[] = [];
  private recentHead = 0;
  readonly latency = new Map<string, ActivationLatency>();
  private readonly latencyOrder: string[] = [];
  private latencyHead = 0;
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
    this.trimRecent(now);
    if (this.recentCount() >= this.limits.maxPerMinute) return { activate: false, reason: "RATE_LIMITED", request };
    const duplicate = request.targetRoles.every((role) => this.sent.has(activationIdempotencyKey(trigger.id, role)));
    if (duplicate) return { activate: false, reason: "DUPLICATE", request };
    return { activate: true, reason: "SEND", request };
  }

  markRequested(trigger: TriggerEvent, request: GrokActivationRequest): void {
    const now = this.now();
    const nowMs = now.getTime();
    this.ensureLatency(trigger, now.toISOString());
    const scope = `${trigger.marketId ?? "none"}:${trigger.asset ?? "none"}:${trigger.kind}`;
    this.lastByMarketAsset.set(scope, { at: nowMs, severity: trigger.severity });
    this.lastOrder.push({ key: scope, at: nowMs });
    this.recent.push(nowMs);
    for (const role of request.targetRoles) {
      const key = activationIdempotencyKey(trigger.id, role);
      this.sent.set(key, nowMs);
      this.sentOrder.push({ key, at: nowMs });
    }
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
    this.latencyOrder.push(trigger.id);
    this.evictLatencyIfNeeded();
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
    // Scouts warm the process-local research cache before the single Alpha
    // synthesis call. Auxiliary risk/supervisor roles remain available for
    // audit, but are kept out of the Alpha critical section.
    const scouts = request.targetRoles.filter((role) => ["market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner"].includes(role as string));
    const alpha = request.targetRoles.filter((role) => role === "fair_probability_analyst");
    const auxiliary = request.targetRoles.filter((role) => !scouts.some((scout) => scout === role) && !alpha.some((item) => item === role));
    const roles = [...scouts, ...alpha, ...auxiliary];
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
    this.latencyOrder.length = 0;
    this.latencyHead = 0;
    const ordered = [...items].sort((a, b) => new Date(a.triggerCreatedAt).getTime() - new Date(b.triggerCreatedAt).getTime());
    for (const item of ordered) {
      this.latency.set(item.triggerId, structuredClone(item));
      this.latencyOrder.push(item.triggerId);
    }
    this.evictLatencyIfNeeded();
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

  touchScopeForTest(marketId: string, asset: string, kind: string, severity = 2): void {
    const nowMs = this.now().getTime();
    const scope = `${marketId}:${asset}:${kind}`;
    this.lastByMarketAsset.set(scope, { at: nowMs, severity });
    this.lastOrder.push({ key: scope, at: nowMs });
    this.prune();
  }

  footprint(): { sent: number; lastByMarketAsset: number; latency: number; recent: number } {
    this.prune();
    return { sent: this.sent.size, lastByMarketAsset: this.lastByMarketAsset.size, latency: this.latency.size, recent: this.recentCount() };
  }

  runtimeMetricsSnapshot(): unknown[] {
    const bridge = this.bridge as GrokActivationBridge & { metricsSnapshot?: () => unknown[] };
    return bridge.metricsSnapshot?.() ?? [];
  }

  private evictLatencyIfNeeded(): void {
    const limit = this.limits.memoryLimit ?? 500;
    while (this.latency.size > limit && this.latencyHead < this.latencyOrder.length) {
      const oldest = this.latencyOrder[this.latencyHead];
      this.latencyOrder[this.latencyHead] = undefined as unknown as string;
      this.latencyHead += 1;
      if (oldest) this.latency.delete(oldest);
    }
    this.compact(this.latencyOrder, this.latencyHead, (head) => { this.latencyHead = head; });
  }

  private prune(): void {
    const now = this.now().getTime();
    this.trimRecent(now);
    this.expireOrdered(this.sent, this.sentOrder, () => this.sentHead, (head) => { this.sentHead = head; }, now, SENT_TTL_MS);
    this.expireLastByMarket(now);
    for (const [role, until] of this.retryAfterUntil) if (until <= now) this.retryAfterUntil.delete(role);
    this.evictLatencyIfNeeded();
  }

  private trimRecent(now: number): void {
    while (this.recentHead < this.recent.length && now - (this.recent[this.recentHead] ?? 0) >= 60_000) {
      this.recentHead += 1;
    }
    if (this.recentHead >= 1_024) {
      this.recent.splice(0, this.recentHead);
      this.recentHead = 0;
    }
  }

  private recentCount(): number {
    return this.recent.length - this.recentHead;
  }

  private expireOrdered(
    map: Map<string, number>,
    order: Array<{ key: string; at: number } | undefined>,
    getHead: () => number,
    setHead: (head: number) => void,
    now: number,
    ttl: number,
  ): void {
    let head = getHead();
    while (head < order.length) {
      const record = order[head];
      if (!record) { head += 1; continue; }
      const current = map.get(record.key);
      if (current === record.at && now - record.at <= ttl) break;
      if (current === record.at && now - record.at > ttl) map.delete(record.key);
      order[head] = undefined;
      head += 1;
    }
    setHead(head);
    this.compact(order, head, setHead);
  }

  private expireLastByMarket(now: number): void {
    while (this.lastHead < this.lastOrder.length) {
      const record = this.lastOrder[this.lastHead];
      if (!record) { this.lastHead += 1; continue; }
      const item = this.lastByMarketAsset.get(record.key);
      if (item && item.at === record.at && now - record.at <= SCOPE_TTL_MS) break;
      if (item && item.at === record.at && now - record.at > SCOPE_TTL_MS) this.lastByMarketAsset.delete(record.key);
      this.lastOrder[this.lastHead] = undefined;
      this.lastHead += 1;
    }
    this.compact(this.lastOrder, this.lastHead, (head) => { this.lastHead = head; });
  }

  private compact(
    order: Array<unknown>,
    head: number,
    setHead: (head: number) => void,
  ): void {
    if (head < 1_024) return;
    order.splice(0, head);
    setHead(0);
  }
}

export function persistableActivation(result: GrokActivationResult, triggerId: string, reason: string): Record<string, unknown> {
  return {
    triggerId, provider: result.provider, ok: result.ok, skipped: result.skipped, targetRole: result.targetRole,
    requestedAt: result.requestedAt, sentAt: result.sentAt, errorCode: result.errorCode, httpStatus: result.httpStatus,
    grokWakeObserved: false, idempotencyKey: result.idempotencyKey, reason,
  };
}
