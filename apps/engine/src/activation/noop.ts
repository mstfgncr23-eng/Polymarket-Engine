import {
  grokActivationRequestSchema,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokActivationResult,
} from "@pm/core";

export class NoopGrokActivationBridge implements GrokActivationBridge {
  readonly provider = "noop";

  async activate(request: GrokActivationRequest): Promise<GrokActivationResult[]> {
    const parsed = grokActivationRequestSchema.parse(request);
    const requestedAt = new Date().toISOString();
    return parsed.targetRoles.map((targetRole) => ({
      provider: this.provider,
      ok: true,
      skipped: true,
      targetRole,
      requestedAt,
      sentAt: null,
      errorCode: "GROK_ACTIVATION_NOOP",
      httpStatus: null,
      grokWakeObserved: false as const,
      idempotencyKey: `${parsed.triggerId}:${targetRole}`,
    }));
  }
}
