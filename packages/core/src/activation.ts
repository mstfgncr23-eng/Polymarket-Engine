import { z } from "zod";
import { externalAgentRoleIdSchema, idSchema, timestampSchema, type ExternalAgentRoleId, type TriggerEvent } from "./domain.js";
import { rolesForTriggerKind } from "./role-routing.js";

export const grokActivationEventType = "GROK_RESEARCH_REQUEST" as const;

export const grokActivationRequestSchema = z.object({
  eventType: z.literal(grokActivationEventType),
  triggerId: idSchema,
  traceId: idSchema,
  marketId: idSchema.nullable(),
  asset: z.string().min(1).nullable(),
  severity: z.union([z.literal(0), z.literal(1), z.literal(2), z.literal(3)]),
  triggerKind: z.string().min(1),
  createdAt: timestampSchema,
  expiresAt: timestampSchema,
  targetRoles: z.array(externalAgentRoleIdSchema).min(1).max(6),
  /** Internal bounded scout warming; it never becomes an economic trigger. */
  researchRefresh: z.boolean().default(false),
}).strict();
export type GrokActivationRequest = z.infer<typeof grokActivationRequestSchema>;

export const grokActivationResultSchema = z.object({
  provider: z.string().min(1),
  ok: booleanLike(),
  skipped: z.boolean().default(false),
  targetRole: externalAgentRoleIdSchema,
  requestedAt: timestampSchema,
  sentAt: timestampSchema.nullable(),
  errorCode: z.string().nullable(),
  httpStatus: z.number().int().nullable(),
  grokWakeObserved: z.literal(false),
  idempotencyKey: z.string().min(1),
}).strict();
export type GrokActivationResult = z.infer<typeof grokActivationResultSchema>;

function booleanLike() {
  return z.boolean();
}

export interface GrokActivationBridge {
  readonly provider: string;
  activate(request: GrokActivationRequest): Promise<GrokActivationResult[]>;
}

export interface ActivationLatency {
  triggerId: string;
  triggerCreatedAt: string;
  activationRequestedAt: string | null;
  activationSentAt: string | null;
  firstMcpRequestAt: string | null;
  firstAcceptedSignalAt: string | null;
}

export function activationRequestFromTrigger(trigger: TriggerEvent): GrokActivationRequest {
  const kind = trigger.kind;
  const roles = rolesForTriggerKind(kind, trigger.severity);
  return grokActivationRequestSchema.parse({
    eventType: grokActivationEventType,
    triggerId: trigger.id,
    traceId: trigger.traceId,
    marketId: trigger.marketId,
    asset: trigger.asset ?? null,
    severity: trigger.severity,
    triggerKind: trigger.kind,
    createdAt: trigger.createdAt,
    expiresAt: trigger.expiresAt,
    targetRoles: roles.length > 0 ? roles : ["strategy_supervisor"],
    researchRefresh: false,
  });
}

export function activationIdempotencyKey(triggerId: string, role: ExternalAgentRoleId): string {
  return `${triggerId}:${role}`;
}

const SECRET_KEY = /(token|secret|authorization|bearer|api[_-]?key|private[_-]?key|wallet|signer)/i;

export function redactSecrets(value: unknown, seen = new WeakSet<object>()): unknown {
  if (typeof value === "string") return SECRET_KEY.test(value) ? "[redacted]" : value;
  if (!value || typeof value !== "object") return value;
  if (seen.has(value)) return "[cycle]";
  seen.add(value);
  if (Array.isArray(value)) return value.map((item) => redactSecrets(item, seen));
  const result: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    result[key] = SECRET_KEY.test(key) ? "[redacted]" : redactSecrets(item, seen);
  }
  return result;
}

export function containsSecretLeak(value: unknown, secrets: readonly string[]): boolean {
  const serialized = JSON.stringify(value);
  return secrets.some((secret) => secret.length > 0 && serialized.includes(secret));
}
