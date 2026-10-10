import { z } from "zod";
import { d } from "./calculations.js";
import { sameReversalEpisode } from "./reversal-protection.js";
import { guardianProtectionRequestSchema } from "./guardian-boundary-contract.js";

/**
 * G2E: PURE, OFFLINE contract for a read-only Guardian position projection.
 * These frames are NOT existing journal records and MUST NOT be invented by a
 * consumer. Future trusted producer must derive them from verified, committed
 * durable transitions + the single authoritative ledger, with exact cursors.
 *
 * No process, subscription, persistence, action, or economic authority here.
 */
const identity = z.string().min(1).max(160).regex(/^[^\s]+$/);
const timestamp = z.string().datetime({ offset: true });
const decimalSyntax = /^(?:0|[1-9]\d*)(?:\.\d{1,18})?$/;
const positiveDecimal = z.string().max(48).refine(v => {
  if (!decimalSyntax.test(v)) return false;
  try { return d(v).isFinite() && d(v).gt(0); } catch { return false; }
});
const price = positiveDecimal.refine(v => d(v).lte(1));
const cursorSchema = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const sha256 = z.string().regex(/^[0-9a-f]{64}$/);
const episodeSchema = guardianProtectionRequestSchema.shape.episode;

export const guardianOpenResidualSchema = z.strictObject({
  episode: episodeSchema,
  residualQty: positiveDecimal,
  entryPrice: price,
});
export type GuardianOpenResidual = z.infer<typeof guardianOpenResidualSchema>;

/** Bootstrap envelope; validation here DOES NOT verify the checkpoint or authenticate its producer. */
export const guardianPositionBootstrapSchema = z.strictObject({
  version: z.literal(1),
  mode: z.literal("PAPER"),
  authorityId: identity,
  experimentId: identity,
  /** Structural hash only; a trusted caller MUST verify it against the real checkpoint. */
  checkpointContentHash: sha256,
  cursor: cursorSchema,
  open: z.array(guardianOpenResidualSchema).max(4096),
  settledMarkets: z.array(identity).max(8192),
}).superRefine((value, ctx) => {
  const markets = new Set<string>();
  for (const item of value.open) {
    if (markets.has(item.episode.marketId)) ctx.addIssue({
      code: "custom", path: ["open"], message: "duplicate market" });
    markets.add(item.episode.marketId);
  }
  const settled = new Set<string>();
  for (const market of value.settledMarkets) {
    if (settled.has(market) || markets.has(market)) ctx.addIssue({
      code: "custom", path: ["settledMarkets"], message: "settled/open overlap or duplicate" });
    settled.add(market);
  }
});
export type GuardianPositionBootstrap = z.infer<typeof guardianPositionBootstrapSchema>;

const base = {
  version: z.literal(1),
  mode: z.literal("PAPER"),
  authorityId: identity,
  experimentId: identity,
  /** Source MUST be the single-writer durable sequence, not websocket ordering. */
  previousCursor: cursorSchema,
  cursor: cursorSchema,
  sourceOperationKey: identity,
  occurredAt: timestamp,
};

/** One frame for EVERY durable sequence; no filtered cursor jumps are allowed. */
export const guardianPositionFrameSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    ...base, kind: z.literal("FILL_POSITION"),
    sourceType: z.literal("PAPER_FILL"),
    marketId: identity,
    previous: episodeSchema.nullable(),
    next: guardianOpenResidualSchema.nullable(),
  }),
  z.strictObject({
    ...base, kind: z.literal("SETTLED"),
    sourceType: z.literal("POSITION_SETTLED"),
    marketId: identity,
  }),
  z.strictObject({
    ...base, kind: z.literal("NO_POSITION_CHANGE"),
    sourceType: z.enum([
      "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
      "RISK_REDUCTION_ADMITTED", "RISK_REDUCTION_FAILED",
      "NAKED_DIRECTIONAL_OBSERVATION", "PROFIT_TRAILING_PAIR_STATE_UPDATED",
      "REVERSAL_PROTECTION_STATE_UPDATED", "LEDGER_MARK_TO_MARKET",
      "INFRASTRUCTURE_COST_ACCRUED",
    ]),
  }),
]).superRefine((frame, ctx) => {
  if ((frame.kind === "FILL_POSITION" && !frame.sourceOperationKey.startsWith("fill:"))
    || (frame.kind === "SETTLED" && !frame.sourceOperationKey.startsWith("settlement:"))) {
    ctx.addIssue({ code: "custom", message: "source operation key/type mismatch", path: ["sourceOperationKey"] });
  }
});
export type GuardianPositionFrame = z.infer<typeof guardianPositionFrameSchema>;

export type GuardianPositionProjection = Readonly<{
  authorityId: string;
  experimentId: string;
  checkpointHash: string;
  cursor: number;
  /** Implementation-owned; reducer never mutates passed-in maps/sets. */
  open: ReadonlyMap<string, GuardianOpenResidual>;
  settledMarkets: ReadonlySet<string>;
  lastFrameJson: string | null;
}>;
export type GuardianPositionRejectReason =
  | "INVALID_BOOTSTRAP" | "INVALID_FRAME" | "AUTHORITY_MISMATCH" | "EXPERIMENT_MISMATCH"
  | "DURABLE_CURSOR_GAP" | "DURABLE_CURSOR_CONFLICT" | "DUPLICATE_CURSOR_CONFLICT"
  | "EPISODE_PRECONDITION_FAILED" | "ALREADY_SETTLED" | "INVALID_EPISODE_TRANSITION";
export type GuardianPositionResult =
  | { status: "REJECTED"; reason: GuardianPositionRejectReason }
  | { status: "DUPLICATE"; projection: GuardianPositionProjection }
  | { status: "APPLIED"; projection: GuardianPositionProjection };

export function bootstrapGuardianPositionProjection(input: unknown): GuardianPositionResult {
  const parsed = guardianPositionBootstrapSchema.safeParse(input);
  if (!parsed.success) return { status: "REJECTED", reason: "INVALID_BOOTSTRAP" };
  const boot = parsed.data;
  return {
    status: "APPLIED",
    projection: {
      authorityId: boot.authorityId,
      experimentId: boot.experimentId,
      checkpointHash: boot.checkpointContentHash,
      cursor: boot.cursor,
      open: new Map(boot.open.map(item => [item.episode.marketId, item])),
      settledMarkets: new Set(boot.settledMarkets),
      lastFrameJson: null,
    },
  };
}

/**
 * Fail closed on every gap, owner mismatch or stale episode. Any REJECTED
 * response MUST halt a future live consumer and require durable reconciliation.
 * APPLIED is a read-model update, NEVER an execution-admission decision.
 */
export function applyGuardianPositionFrame(
  current: GuardianPositionProjection, input: unknown,
): GuardianPositionResult {
  const parsed = guardianPositionFrameSchema.safeParse(input);
  if (!parsed.success) return { status: "REJECTED", reason: "INVALID_FRAME" };
  const frame = parsed.data;
  if (frame.authorityId !== current.authorityId) return { status: "REJECTED", reason: "AUTHORITY_MISMATCH" };
  if (frame.experimentId !== current.experimentId) return { status: "REJECTED", reason: "EXPERIMENT_MISMATCH" };
  const json = JSON.stringify(frame);
  if (frame.cursor === current.cursor) {
    return frame.previousCursor === frame.cursor - 1 && json === current.lastFrameJson
      ? { status: "DUPLICATE", projection: current }
      : { status: "REJECTED", reason: "DUPLICATE_CURSOR_CONFLICT" };
  }
  if (frame.cursor !== current.cursor + 1 || frame.previousCursor !== current.cursor) {
    return { status: "REJECTED", reason: "DURABLE_CURSOR_GAP" };
  }
  const open = new Map(current.open);
  const settled = new Set(current.settledMarkets);
  if (frame.kind === "FILL_POSITION") {
    const marketId = frame.marketId;
    if (frame.previous?.marketId !== undefined && frame.previous.marketId !== marketId
      || frame.next?.episode.marketId !== undefined && frame.next.episode.marketId !== marketId) {
      return { status: "REJECTED", reason: "INVALID_EPISODE_TRANSITION" };
    }
    if (settled.has(marketId)) return { status: "REJECTED", reason: "ALREADY_SETTLED" };
    const before = open.get(marketId);
    if ((before === undefined) !== (frame.previous === null)
      || (before && frame.previous && !sameReversalEpisode(before.episode, frame.previous))) {
      return { status: "REJECTED", reason: "EPISODE_PRECONDITION_FAILED" };
    }
    if (frame.next) {
      if (frame.previous && !sameReversalEpisode(frame.previous, frame.next.episode)) {
        if (Date.parse(frame.next.episode.unhedgedStartedAt) <= Date.parse(frame.previous.unhedgedStartedAt)) {
          return { status: "REJECTED", reason: "INVALID_EPISODE_TRANSITION" };
        }
      }
      open.set(marketId, frame.next);
    } else open.delete(marketId);
  } else if (frame.kind === "SETTLED") {
    if (settled.has(frame.marketId)) return { status: "REJECTED", reason: "ALREADY_SETTLED" };
    open.delete(frame.marketId);
    settled.add(frame.marketId);
  }
  return { status: "APPLIED", projection: {
    authorityId: current.authorityId, experimentId: current.experimentId,
    checkpointHash: current.checkpointHash, cursor: frame.cursor,
    open, settledMarkets: settled, lastFrameJson: json,
  } };
}
