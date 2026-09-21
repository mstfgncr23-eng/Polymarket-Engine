import {
  d,
  experimentManifestSchema,
  ledgerStateSchema,
  paperFillSchema,
  PaperLedger,
  positionSchema,
  restingPaperOrderSchema,
  type ExperimentManifest,
  type LedgerState,
  type PaperFill,
  type Position,
  type RestingPaperOrder,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION,
  CheckpointFenceError,
  hashDurablePayload,
  IdempotencyConflictError,
  isDurableTransitionType,
  type DurableTransition,
} from "@pm/db";
import { z, ZodError } from "zod";

/**
 * MUTATION INVENTORY (Step 2 — replay-authoritative vs derived)
 *
 * A  CRITICAL / replay-authoritative (this log is the recovery source):
 *    EXPERIMENT_STARTED, EXPERIMENT_START_VALIDATED, EXPERIMENT_START_ABORTED,
 *    EXPERIMENT_ENTRY_CLOSED, EXPERIMENT_COMPLETED
 *    PAPER_MAKER_RESERVED, PAPER_MAKER_RELEASED, PAPER_ORDER_CANCELLED
 *    PAPER_FILL (taker + maker partial/full)
 *    POSITION_SETTLED (includes resolution payoffs; no provider call on replay)
 *    LEDGER_MARK_TO_MARKET, INFRASTRUCTURE_COST_ACCRUED
 *
 * B  DERIVED AUDIT (journal_events, deterministic ids, not competing authorities):
 *    PAPER_ORDER_CREATED, FIRST_LEG_FILLED, HEDGE_COMPLETE, POSITION_UPDATED,
 *    PNL_SNAPSHOT, PAIR_REALIZED, POSITION_WIN/LOSS/BREAKEVEN,
 *    MARKET_RESOLUTION_OBSERVED, EXPERIMENT_PREFLIGHT, EXPERIMENT_REPORT_GENERATED,
 *    PAPER_MAKER_RESERVED/RELEASED/FILL/SETTLED mirrored copies, STATE_RECOVERED
 *
 * C  NONCRITICAL research/telemetry (not replayed):
 *    TRIGGER_EVENT, GROK_*, CONSENSUS, RISK_EVALUATION, activation,
 *    SYSTEM_HEALTH, MCP, STRATEGY_CHANGE_PROPOSED. Ordinary canonical
 *    research is journaled only inside an experiment window; raw
 *    MARKET_SNAPSHOT and REFERENCE_TICK inputs stay live and in-memory.
 *
 * D  EPHEMERAL / reconstructed live (not durable, not replayed):
 *    trigger windows, recentTrades cooldown, signal inbox, book snapshots,
 *    maker queue-ahead without a fill, settlement backoff, discovery maps
 *
 * Why a dedicated durable_transitions table (not journal_events columns):
 *    journal_events mixes telemetry + audit with random ids and no cursor.
 *    Replaying that log would require guessing which events are authoritative.
 *    A dedicated table is the single recovery source, sequenced only through
 *    the durable-mutation gate / PG advisory lock, leaving PR #12 admission
 *    concurrent for ordinary traffic.
 */

export const RECOVERY_FAILURE_REASONS = [
  "legacy_unsafe",
  "malformed",
  "unsupported",
  "conflict",
  "invariant",
  "unknown_type",
  "cursor_fence",
] as const;
export type RecoveryFailureReason = (typeof RECOVERY_FAILURE_REASONS)[number];

export class RecoveryFailClosedError extends Error {
  readonly code = "RECOVERY_FAIL_CLOSED";
  constructor(readonly reason: RecoveryFailureReason, message: string) {
    super(`RECOVERY_FAIL_CLOSED:${reason}: ${message}`);
    this.name = "RecoveryFailClosedError";
  }
}

export function normalizeRecoveryError(error: unknown): RecoveryFailClosedError {
  if (error instanceof RecoveryFailClosedError) return error;
  if (error instanceof IdempotencyConflictError) {
    return new RecoveryFailClosedError("conflict", error.message);
  }
  if (error instanceof CheckpointFenceError) {
    return new RecoveryFailClosedError("cursor_fence", error.message);
  }
  if (error instanceof ZodError) {
    return new RecoveryFailClosedError("malformed", error.issues.map((issue) => issue.message).join(";") || "schema");
  }
  return new RecoveryFailClosedError("malformed", error instanceof Error ? error.message : String(error));
}

export interface DerivedAuditRecord {
  id: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  traceId: string;
  payload: Record<string, unknown>;
  sourceEventId?: string | null;
}

const derivedAuditRecordSchema = z.object({
  id: z.string().min(1).max(160),
  type: z.string().min(1).max(80),
  aggregateType: z.string().min(1).max(80),
  aggregateId: z.string().min(1).max(160),
  traceId: z.string().min(1).max(160),
  payload: z.record(z.string(), z.unknown()),
  sourceEventId: z.string().max(160).nullable().optional(),
});

export function parseDerivedAudit(payload: Record<string, unknown>): DerivedAuditRecord[] {
  if (!Object.prototype.hasOwnProperty.call(payload, "derivedAudit") || payload.derivedAudit == null) {
    throw new RecoveryFailClosedError("malformed", "durable payload missing derivedAudit");
  }
  const parsed = z.array(derivedAuditRecordSchema).safeParse(payload.derivedAudit);
  if (!parsed.success) {
    throw new RecoveryFailClosedError("malformed", `derivedAudit schema: ${parsed.error.message}`);
  }
  return parsed.data;
}

export function cloneRecoverableState(state: RecoverableEngineState): RecoverableEngineState {
  const snapshot = state.ledger.snapshot();
  const ledger = new PaperLedger(snapshot.startingBalance, new Date(snapshot.updatedAt));
  ledger.restore(snapshot, [...state.ledger.positions.values()]);
  return {
    ledger,
    experiment: state.experiment ? structuredClone(state.experiment) : null,
    restingOrders: new Map([...state.restingOrders.entries()].map(([id, order]) => [id, structuredClone(order)])),
    makerReservations: new Map(state.makerReservations),
    settledMarkets: new Set(state.settledMarkets),
    lastInfrastructureAccrualAt: state.lastInfrastructureAccrualAt,
  };
}

export function plannedDurableTransition(
  input: {
    operationKey: string;
    type: string;
    aggregateType: string;
    aggregateId: string;
    payload: Record<string, unknown>;
  },
  occurredAt: Date,
  sequence: number,
): DurableTransition {
  return {
    sequence,
    operationKey: input.operationKey,
    type: input.type,
    aggregateType: input.aggregateType,
    aggregateId: input.aggregateId,
    payloadHash: "",
    payload: input.payload,
    occurredAt: occurredAt.toISOString(),
    recordedAt: occurredAt.toISOString(),
  };
}

export function durableIdentityConflicts(
  type: string,
  existing: Record<string, unknown>,
  incoming: Record<string, unknown>,
): boolean {
  const pick = (row: Record<string, unknown>, keys: readonly string[]): Record<string, unknown> => (
    Object.fromEntries(keys.map((key) => [key, row[key] ?? null]))
  );
  switch (type) {
    case "PAPER_FILL": {
      const fillConflict = hashDurablePayload({ fill: existing.fill ?? null })
        !== hashDurablePayload({ fill: incoming.fill ?? null });
      if (fillConflict) return true;
      // Intent was added after legacy fills existed. Once persisted, a retry
      // cannot reinterpret the same economic fill as another lifecycle action.
      if (Object.prototype.hasOwnProperty.call(existing, "intent")) {
        return existing.intent !== incoming.intent;
      }
      return false;
    }
    case "PAPER_MAKER_RESERVED": {
      const baseConflict = hashDurablePayload(pick(existing, ["orderId", "reserved", "resting"]))
        !== hashDurablePayload(pick(incoming, ["orderId", "reserved", "resting"]));
      if (baseConflict) return true;
      // intent became replay-authoritative for lifecycle correctness. Existing
      // pre-intent rows remain readable; rows that already carry intent must
      // reject a retry that changes its economic meaning.
      if (Object.prototype.hasOwnProperty.call(existing, "intent")) {
        return existing.intent !== incoming.intent;
      }
      return false;
    }
    case "PAPER_MAKER_RELEASED":
    case "PAPER_ORDER_CANCELLED":
      return hashDurablePayload(pick(existing, ["orderId", "cancel", "reason"]))
        !== hashDurablePayload(pick(incoming, ["orderId", "cancel", "reason"]));
    case "POSITION_SETTLED":
      return hashDurablePayload(pick(existing, ["marketId", "upPayoff", "downPayoff", "winner"]))
        !== hashDurablePayload(pick(incoming, ["marketId", "upPayoff", "downPayoff", "winner"]));
    case "INFRASTRUCTURE_COST_ACCRUED":
      return hashDurablePayload(pick(existing, ["aiCost", "infrastructureCost", "accrualKey"]))
        !== hashDurablePayload(pick(incoming, ["aiCost", "infrastructureCost", "accrualKey"]));
    case "EXPERIMENT_STARTED":
    case "EXPERIMENT_START_VALIDATED":
    case "EXPERIMENT_ENTRY_CLOSED":
    case "EXPERIMENT_COMPLETED":
    case "LEDGER_MARK_TO_MARKET":
    case "REPORT_SOURCE_INTEGRITY_FAULT":
    case "REPORT_SOURCE_INTEGRITY_REPAIRED":
      return false;
    default:
      return hashDurablePayload(existing) !== hashDurablePayload(incoming);
  }
}

export interface RecoverableEngineState {
  ledger: PaperLedger;
  experiment: ExperimentManifest | null;
  restingOrders: Map<string, RestingPaperOrder>;
  makerReservations: Map<string, string>;
  settledMarkets: Set<string>;
  lastInfrastructureAccrualAt: number;
}

const fillPayloadSchema = z.object({
  fill: paperFillSchema,
  positionId: z.string().min(1),
  reservationAfter: z.string().nullable(),
  restingAfter: restingPaperOrderSchema.nullable().optional(),
  intent: z.enum(["OPEN_FIRST_LEG", "HEDGE_SECOND_LEG", "PROTECTIVE_DE_RISK"]).optional(),
  deRisk: z.record(z.string(), z.unknown()).optional(),
});

const reservePayloadSchema = z.object({
  orderId: z.string().min(1),
  reserved: z.string(),
  resting: restingPaperOrderSchema,
  intent: z.enum(["OPEN_FIRST_LEG", "HEDGE_SECOND_LEG"]).optional(),
});

const releasePayloadSchema = z.object({
  orderId: z.string().min(1),
  // Released amount is an execution-time projection; the authoritative
  // recovery effect is derived from the reservation map at this sequence.
  released: z.string().optional(),
  cancel: z.boolean().optional(),
  reason: z.string().optional(),
});

const settlementPayloadSchema = z.object({
  marketId: z.string().min(1),
  upPayoff: z.enum(["0", "1"]),
  downPayoff: z.enum(["0", "1"]),
  settledAt: z.string().datetime({ offset: true }),
});

const mtmPayloadSchema = z.object({
  prices: z.record(z.string(), z.object({ up: z.string(), down: z.string() })),
  markedAt: z.string().datetime({ offset: true }),
});

const infraPayloadSchema = z.object({
  aiCost: z.string().default("0"),
  infrastructureCost: z.string(),
  accruedAt: z.string().datetime({ offset: true }),
  accrualKey: z.number().int().nonnegative(),
});

export function stablePositionId(marketId: string): string {
  return `pos:${marketId}`;
}

export function applyDurableTransition(state: RecoverableEngineState, transition: DurableTransition): void {
  if (!isDurableTransitionType(transition.type)) {
    throw new RecoveryFailClosedError("unknown_type", `unsupported authoritative type ${transition.type}`);
  }
  try {
    switch (transition.type) {
    case "EXPERIMENT_STARTED":
    case "EXPERIMENT_START_VALIDATED":
    case "EXPERIMENT_START_ABORTED":
    case "EXPERIMENT_ENTRY_CLOSED":
    case "EXPERIMENT_COMPLETED":
        state.experiment = experimentManifestSchema.parse(transition.payload.experiment ?? transition.payload);
        if (transition.type === "EXPERIMENT_STARTED") {
          const at = state.experiment.startedAt ? new Date(state.experiment.startedAt) : new Date(transition.occurredAt);
          const ledger = state.ledger.snapshot();
          const openPosition = [...state.ledger.positions.values()].some(
            (position) => d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0),
          );
          const restingOrder = [...state.restingOrders.values()].some(
            (order) => d(order.remaining).gt(0),
          );
          const makerReservation = [...state.makerReservations.values()].some(
            (amount) => d(amount).gt(0),
          );
          if (openPosition || restingOrder || makerReservation || d(ledger.reservedCash).gt(0)) {
            throw new RecoveryFailClosedError(
              "invariant",
              "experiment start attempted with economic residue",
            );
          }

          const fresh = new PaperLedger(state.experiment.startingBalance, at);
          state.ledger.restore(fresh.snapshot(), []);
          state.restingOrders.clear();
          state.makerReservations.clear();
          state.lastInfrastructureAccrualAt = at.getTime();
        }
        return;
      case "PAPER_MAKER_RESERVED": {
        const payload = reservePayloadSchema.parse(transition.payload);
        state.ledger.reserve(payload.reserved, new Date(transition.occurredAt));
        state.makerReservations.set(payload.orderId, payload.reserved);
        const resting = payload.intent && !payload.resting.intent
          ? { ...payload.resting, intent: payload.intent }
          : payload.resting;
        state.restingOrders.set(payload.orderId, resting);
        return;
      }
      case "PAPER_MAKER_RELEASED":
      case "PAPER_ORDER_CANCELLED": {
        const payload = releasePayloadSchema.parse(transition.payload);
        const reserved = state.makerReservations.get(payload.orderId);
        if (reserved) {
          const remaining = d(reserved);
          if (remaining.gt(0)) state.ledger.release(remaining, new Date(transition.occurredAt));
          state.makerReservations.delete(payload.orderId);
        }
        if (transition.type === "PAPER_ORDER_CANCELLED" || payload.cancel) {
          state.restingOrders.delete(payload.orderId);
        }
        return;
      }
      case "PAPER_FILL": {
        const payload = fillPayloadSchema.parse(transition.payload);
        applyFillReservation(state, payload.fill, payload.reservationAfter, new Date(transition.occurredAt));
        state.ledger.applyFill(payload.fill, { positionId: payload.positionId });
        if (payload.restingAfter) {
          if (!d(payload.restingAfter.remaining).gt(0)) state.restingOrders.delete(payload.fill.orderId);
          else state.restingOrders.set(payload.fill.orderId, payload.restingAfter);
        } else if (payload.fill.liquidityRole === "MAKER" && !d(state.restingOrders.get(payload.fill.orderId)?.remaining ?? 1).gt(0)) {
          state.restingOrders.delete(payload.fill.orderId);
        }
        return;
      }
      case "POSITION_SETTLED": {
        const payload = settlementPayloadSchema.parse(transition.payload);
        state.ledger.settlePosition(payload.marketId, payload.upPayoff, payload.downPayoff, new Date(payload.settledAt));
        return;
      }
      case "LEDGER_MARK_TO_MARKET": {
        const payload = mtmPayloadSchema.parse(transition.payload);
        state.ledger.revalue(payload.prices, payload.markedAt);
        return;
      }
      case "INFRASTRUCTURE_COST_ACCRUED": {
        const payload = infraPayloadSchema.parse(transition.payload);
        state.ledger.addOperatingCosts(payload.aiCost, payload.infrastructureCost, new Date(payload.accruedAt));
        state.lastInfrastructureAccrualAt = payload.accrualKey;
        return;
      }
      case "REPORT_SOURCE_INTEGRITY_FAULT":
      case "REPORT_SOURCE_INTEGRITY_REPAIRED":
        return;
      default:
        throw new RecoveryFailClosedError("unsupported", `unhandled authoritative type ${transition.type}`);
    }
  } catch (error) {
    if (error instanceof RecoveryFailClosedError) throw error;
    throw new RecoveryFailClosedError("malformed", `malformed ${transition.type} seq=${transition.sequence}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function applyFillReservation(
  state: RecoverableEngineState,
  fill: PaperFill,
  reservationAfter: string | null,
  at: Date,
): void {
  const reserved = state.makerReservations.get(fill.orderId);
  if (!reserved || fill.liquidityRole !== "MAKER") return;
  const fillCost = d(fill.quantity).mul(fill.fillPrice).plus(fill.feeUsd);
  const reservedAmount = d(reserved);
  state.ledger.release(reservedAmount.lt(fillCost) ? reservedAmount : fillCost, at);
  if (reservationAfter && d(reservationAfter).gt(0)) state.makerReservations.set(fill.orderId, reservationAfter);
  else state.makerReservations.delete(fill.orderId);
}

export interface ReplayInvariantReport {
  ok: boolean;
  reasons: string[];
}

export function validatePostReplayInvariants(
  state: RecoverableEngineState,
  lastApplied: number,
  expectedCursor: number,
): ReplayInvariantReport {
  const reasons: string[] = [];
  let ledger: LedgerState;
  try {
    ledger = ledgerStateSchema.parse(state.ledger.snapshot());
  } catch (error) {
    reasons.push(`ledger_parse:${error instanceof Error ? error.message : String(error)}`);
    return { ok: false, reasons };
  }
  const cash = d(ledger.cash);
  const reserved = d(ledger.reservedCash);
  if (!cash.isFinite() || cash.lt(0)) reasons.push("cash_invalid");
  if (!reserved.isFinite() || reserved.lt(0)) reasons.push("reserved_invalid");
  const seenPositions = new Set<string>();
  for (const position of state.ledger.positions.values()) {
    try { positionSchema.parse(position); }
    catch (error) { reasons.push(`position_parse:${error instanceof Error ? error.message : String(error)}`); }
    if (seenPositions.has(position.id)) reasons.push("duplicate_position_id");
    seenPositions.add(position.id);
    if (state.settledMarkets.has(position.marketId) && (d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0))) {
      reasons.push("settled_and_open");
    }
  }
  let reservationSum = d(0);
  for (const [orderId, amount] of state.makerReservations) {
    const reservedAmount = d(amount);
    if (!reservedAmount.isFinite() || !reservedAmount.gt(0)) reasons.push(`reservation_nonpositive:${orderId}`);
    if (!state.restingOrders.has(orderId)) reasons.push(`reservation_without_resting:${orderId}`);
    reservationSum = reservationSum.plus(reservedAmount);
  }
  for (const [orderId, resting] of state.restingOrders) {
    if (!d(resting.remaining).gt(0)) reasons.push(`resting_nonpositive:${orderId}`);
    const reservedAmount = state.makerReservations.get(orderId);
    if (reservedAmount == null || !d(reservedAmount).gt(0)) reasons.push(`resting_without_reservation:${orderId}`);
  }
  if (!reservationSum.eq(reserved)) {
    reasons.push(`reserved_cash_mismatch:${reserved.toFixed()}!=${reservationSum.toFixed()}`);
  }
  const status = state.experiment?.status;
  if (status && !["IDLE", "CREATED", "RUNNING", "ENTRY_CLOSED", "COMPLETED", "FAILED", "ABORTED_START"].includes(status)) {
    reasons.push("experiment_status_invalid");
  }
  if (lastApplied !== expectedCursor) reasons.push(`cursor_mismatch:${lastApplied}!=${expectedCursor}`);
  return { ok: reasons.length === 0, reasons };
}

export interface LegacySafety {
  safe: boolean;
  reason?: RecoveryFailureReason;
  detail: string;
}

export function assessLegacyCheckpointSafety(input: {
  experiment: ExperimentManifest | null;
  positions: readonly Position[];
  restingOrders: readonly RestingPaperOrder[];
  makerReservations: Record<string, string>;
  settledMarkets?: readonly string[];
}): LegacySafety {
  const status = input.experiment?.status ?? "IDLE";
  const open = input.positions.some((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0);
  const resting = input.restingOrders.length > 0;
  const reserved = Object.keys(input.makerReservations).length > 0;
  const unresolvedSettlement = open;
  if (status === "IDLE" && !open && !resting && !reserved && !unresolvedSettlement) {
    return { safe: true, detail: "legacy idle empty baseline" };
  }
  return {
    safe: false,
    reason: "legacy_unsafe",
    detail: `legacy checkpoint has no durable cursor and active obligations status=${status} open=${open} resting=${resting} reserved=${reserved}`,
  };
}

export function isLegacyCheckpointPayload(payload: Record<string, unknown>): boolean {
  return payload.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || typeof payload.durableCursor !== "number";
}

export function parseCheckpointDurableCursor(payload: Record<string, unknown>): number | null {
  return typeof payload.durableCursor === "number" && Number.isInteger(payload.durableCursor) && payload.durableCursor >= 0
    ? payload.durableCursor
    : null;
}

export function parseCheckpointDerivedAuditCursor(payload: Record<string, unknown>): number {
  return typeof payload.derivedAuditCursor === "number"
    && Number.isInteger(payload.derivedAuditCursor)
    && payload.derivedAuditCursor >= 0
    ? payload.derivedAuditCursor
    : 0;
}

export const CRITICAL_AUDIT_TYPES = [
  "PAPER_FILL",
  "PAPER_MAKER_RESERVED",
  "PAPER_MAKER_RELEASED",
  "PAPER_ORDER_CANCELLED",
  "PAPER_ORDER_CREATED",
  "FIRST_LEG_FILLED",
  "HEDGE_COMPLETE",
  "POSITION_UPDATED",
  "POSITION_SETTLED",
  "POSITION_WIN",
  "POSITION_LOSS",
  "POSITION_BREAKEVEN",
  "PAIR_REALIZED",
  "MARKET_RESOLUTION_OBSERVED",
  "EXPERIMENT_STARTED",
  "EXPERIMENT_START_VALIDATED",
  "EXPERIMENT_START_ABORTED",
  "EXPERIMENT_ENTRY_CLOSED",
  "EXPERIMENT_COMPLETED",
  "LEDGER_MARK_TO_MARKET",
  "INFRASTRUCTURE_COST_ACCRUED",
  "PNL_SNAPSHOT",
] as const;
