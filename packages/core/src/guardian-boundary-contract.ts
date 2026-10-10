import { z } from "zod";
import { d } from "./calculations.js";
import { sameReversalEpisode, type ReversalEpisode } from "./reversal-protection.js";

/**
 * G1 ONLY: pure transport models and defensive prechecks.
 * This module does NOT admit, schedule, authorize, or execute an economic order.
 * There is no runtime subscription or shadow Guardian.
 */
const identity = z.string().min(1).max(160).regex(/^[^\s]+$/);
const requestIdentity = z.string().min(8).max(160).regex(/^[A-Za-z0-9][A-Za-z0-9:_.-]*$/);
const timestamp = z.string().datetime({ offset: true });
const decimalSyntax = /^(?:0|[1-9]\d*)(?:\.\d{1,18})?$/;
function validDecimal(value: string, positive: boolean): boolean {
  if (value.length > 48 || !decimalSyntax.test(value)) return false;
  try { return d(value).isFinite() && (positive ? d(value).gt(0) : d(value).gte(0)); }
  catch { return false; }
}
const amount = z.string().refine((value) => validDecimal(value, true));
const nonnegativeAmount = z.string().refine((value) => validDecimal(value, false));
const probability = nonnegativeAmount.refine((value) => {
  if (!validDecimal(value, false)) return false;
  try { return d(value).lte(1); } catch { return false; }
});
const bookSide = z.strictObject({
  tokenId: identity,
  eventId: identity,
  receivedAt: timestamp,
  bestBid: probability,
  bestAsk: probability,
  bidSize: nonnegativeAmount,
  askSize: nonnegativeAmount,
}).superRefine((side, ctx) => {
  if (validDecimal(side.bestAsk, false) && validDecimal(side.bestBid, false)
    && d(side.bestAsk).lt(side.bestBid)) {
    ctx.addIssue({ code: "custom", message: "crossed book", path: ["bestAsk"] });
  }
});

/** Snapshot payload contract, NOT evidence of fresh/synchronized executable depth. */
export const guardianMarketObservationSchema = z.strictObject({
  version: z.literal(1),
  source: z.literal("CLOB"),
  marketId: identity,
  syncEpoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
  observedAt: timestamp,
  up: bookSide,
  down: bookSide,
}).superRefine((value, ctx) => {
  const at = Date.parse(value.observedAt);
  if (value.up.tokenId === value.down.tokenId || value.up.eventId === value.down.eventId) {
    ctx.addIssue({ code: "custom", message: "UP and DOWN must be distinct", path: ["down"] });
  }
  if (Date.parse(value.up.receivedAt) > at || Date.parse(value.down.receivedAt) > at) {
    ctx.addIssue({ code: "custom", message: "future child snapshot", path: ["observedAt"] });
  }
});
export type GuardianMarketObservation = z.infer<typeof guardianMarketObservationSchema>;

const episode = z.strictObject({
  marketId: identity,
  positionId: identity,
  unhedgedStartedAt: timestamp,
  heldOutcome: z.enum(["UP", "DOWN"]),
});

/**
 * A request is an expression of risk reduction intent, never a signed order.
 * The executor independently chooses the executable option using current books.
 * No buy/sell quantity, token, price or side from Guardian is authoritative.
 */
export const guardianProtectionRequestSchema = z.strictObject({
  version: z.literal(1),
  mode: z.literal("PAPER"),
  policyGeneration: z.literal(3),
  requestId: requestIdentity,
  experimentId: identity,
  episode,
  expectedResidualQty: amount,
  /** Observed global cursor watermark, NOT an exact per-position ownership fence. */
  sourceDurableCursor: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
  requestedKind: z.enum(["PROTECTIVE_SELL", "HEDGE_SECOND_LEG"]),
  decisionReason: z.enum(["EARLY_EMERGENCY", "EARLY_TIMEOUT", "LATE_EMERGENCY", "LATE_CONFIRMED"]),
  evidence: z.strictObject({
    upEventId: identity,
    downEventId: identity,
    syncEpoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    observedAt: timestamp,
  }),
  confirmedAt: timestamp,
  requestedAt: timestamp,
  expiresAt: timestamp,
}).superRefine((req, ctx) => {
  const started = Date.parse(req.episode.unhedgedStartedAt);
  const observed = Date.parse(req.evidence.observedAt);
  const confirmed = Date.parse(req.confirmedAt);
  const requested = Date.parse(req.requestedAt);
  const expires = Date.parse(req.expiresAt);
  if (req.evidence.upEventId === req.evidence.downEventId
    || !(started <= observed && observed <= confirmed && confirmed <= requested && requested < expires)
    || expires - requested > 30_000) {
    ctx.addIssue({ code: "custom", message: "invalid source or temporal ordering" });
  }
});
export type GuardianProtectionRequest = z.infer<typeof guardianProtectionRequestSchema>;

export type GuardianRequestPrecheckRejection =
  | "INVALID_REQUEST" | "MODE_MISMATCH" | "EPISODE_MISMATCH"
  | "RESIDUAL_CHANGED" | "SOURCE_CURSOR_AHEAD" | "STALE_OBSERVATION"
  | "FUTURE_OBSERVATION" | "FUTURE_REQUEST" | "EXPIRED_REQUEST"
  | "IDEMPOTENCY_KEY_CONFLICT";

export type GuardianRequestPrecheck =
  | { status: "REJECTED"; reason: GuardianRequestPrecheckRejection }
  | { status: "REPLAY_REQUIRES_DURABLE_RESULT_LOOKUP" }
  | { status: "CANDIDATE_ONLY"; request: GuardianProtectionRequest };

export interface GuardianRequestPrecheckContext {
  /** Read from authoritative durable state at the validation instant. */
  mode: "PAPER" | "LIVE";
  currentEpisode: ReversalEpisode | null;
  currentResidualQty: string;
  currentDurableCursor: number;
  /** Retrieved from an authoritative, persisted request-key index, if any. */
  existingRequest?: unknown;
  now: Date;
  maxObservationAgeMs: number;
  maxClockSkewMs: number;
}

/**
 * Pure reject-only filter. CANDIDATE_ONLY is explicitly NOT admission.
 * Executor must acquire durable ownership and revalidate current books,
 * position residual, idempotency, DB outcome and expiry at commit time.
 */
export function precheckGuardianProtectionRequest(
  input: unknown, context: GuardianRequestPrecheckContext,
): GuardianRequestPrecheck {
  const parsed = guardianProtectionRequestSchema.safeParse(input);
  if (!parsed.success) return { status: "REJECTED", reason: "INVALID_REQUEST" };
  const req = parsed.data;
  if (req.mode !== context.mode) return { status: "REJECTED", reason: "MODE_MISMATCH" };
  if (!context.currentEpisode || !sameReversalEpisode(context.currentEpisode, req.episode)) {
    return { status: "REJECTED", reason: "EPISODE_MISMATCH" };
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d{1,18})?$/.test(context.currentResidualQty)
    || !d(context.currentResidualQty).eq(req.expectedResidualQty)) {
    return { status: "REJECTED", reason: "RESIDUAL_CHANGED" };
  }
  // Unrelated committed events can legitimately advance the global cursor.
  // Episode, residual and eventual durable ownership are checked separately.
  if (!Number.isSafeInteger(context.currentDurableCursor) || context.currentDurableCursor < req.sourceDurableCursor) {
    return { status: "REJECTED", reason: "SOURCE_CURSOR_AHEAD" };
  }
  const now = context.now.getTime();
  if (!Number.isFinite(now) || !Number.isFinite(context.maxObservationAgeMs)
    || context.maxObservationAgeMs < 0 || !Number.isFinite(context.maxClockSkewMs) || context.maxClockSkewMs < 0) {
    return { status: "REJECTED", reason: "INVALID_REQUEST" };
  }
  const observed = Date.parse(req.evidence.observedAt);
  if (observed - now > context.maxClockSkewMs) return { status: "REJECTED", reason: "FUTURE_OBSERVATION" };
  if (now - observed > context.maxObservationAgeMs) return { status: "REJECTED", reason: "STALE_OBSERVATION" };
  if (Date.parse(req.requestedAt) - now > context.maxClockSkewMs) {
    return { status: "REJECTED", reason: "FUTURE_REQUEST" };
  }
  if (now >= Date.parse(req.expiresAt)) return { status: "REJECTED", reason: "EXPIRED_REQUEST" };
  if (context.existingRequest !== undefined) {
    const stored = guardianProtectionRequestSchema.safeParse(context.existingRequest);
    if (!stored.success || stored.data.requestId !== req.requestId
      || JSON.stringify(stored.data) !== JSON.stringify(req)) {
      return { status: "REJECTED", reason: "IDEMPOTENCY_KEY_CONFLICT" };
    }
    return { status: "REPLAY_REQUIRES_DURABLE_RESULT_LOOKUP" };
  }
  return { status: "CANDIDATE_ONLY", request: req };
}
