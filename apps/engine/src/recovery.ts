import { createNakedDirectionalDiagnostic, nakedDirectionalDiagnosticSchema, mergeNakedDirectionalObservation,
  profitTrailingPairStateSchema, type NakedDirectionalDiagnostic, type ProfitTrailingPairState } from "@pm/core";
import {
  d,
  experimentManifestSchema,
  ledgerStateSchema,
  paperFillSchema,
  paperOrderSchema,
  PaperLedger,
  positionSchema,
  restingPaperOrderSchema,
  type ExperimentManifest,
  type LedgerState,
  type PaperFill,
  type Position,
  type RestingPaperOrder,
  type BinarySettlementLookup,
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
import { accountResultFill, completeResult, emptyResultAccounting, resultAccountingSchema, type ResultAccounting } from "./trading-results.js";

/**
 * MUTATION INVENTORY (Step 2 — replay-authoritative vs derived)
 *
 * A  CRITICAL / replay-authoritative (this log is the recovery source):
 *    EXPERIMENT_STARTED, EXPERIMENT_START_VALIDATED, EXPERIMENT_START_ABORTED,
 *    EXPERIMENT_ENTRY_CLOSED, EXPERIMENT_COMPLETED
 *    PAPER_MAKER_RESERVED, PAPER_MAKER_RELEASED, PAPER_ORDER_CANCELLED
 *    RISK_REDUCTION_ADMITTED, RISK_REDUCTION_FAILED (shared taker ownership)
 *    PAPER_FILL (taker + maker partial/full; releases matching taker ownership)
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
    resultAccounting: structuredClone(state.resultAccounting ?? emptyResultAccounting()),
    experiment: state.experiment ? structuredClone(state.experiment) : null,
    restingOrders: new Map([...state.restingOrders.entries()].map(([id, order]) => [id, structuredClone(order)])),
    makerReservations: new Map(state.makerReservations),
    settledMarkets: new Set(state.settledMarkets),
    settlementLookups: new Map([...(state.settlementLookups ?? [])].map(([id, lookup]) => [id, structuredClone(lookup)])),
    riskReductionOwners: new Map([...(state.riskReductionOwners ?? [])].map(([market, owner]) => [market, structuredClone(owner)])),
    riskReductionFailures: new Map([...(state.riskReductionFailures ?? [])].map(([market, failure]) => [market, structuredClone(failure)])),
    lastInfrastructureAccrualAt: state.lastInfrastructureAccrualAt,
    nakedDirectionalDiagnostics: new Map([...(state.nakedDirectionalDiagnostics ?? [])].map(([market, diagnostic]) => [market, structuredClone(diagnostic)])),
    profitTrailingPairStates: new Map([...(state.profitTrailingPairStates ?? [])].map(([market, trailing]) => [market, structuredClone(trailing)])),
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
  resultAccounting?: ResultAccounting;
  nakedDirectionalDiagnostics?: Map<string, NakedDirectionalDiagnostic>;
  profitTrailingPairStates?: Map<string, ProfitTrailingPairState>;
  ledger: PaperLedger;
  experiment: ExperimentManifest | null;
  restingOrders: Map<string, RestingPaperOrder>;
  makerReservations: Map<string, string>;
  settledMarkets: Set<string>;
  /** Fill-time identity, retained only while economic inventory remains. */
  settlementLookups?: Map<string, BinarySettlementLookup>;
  lastInfrastructureAccrualAt: number;
  /** Optional only for checkpoints/tests written before durable taker admission. */
  riskReductionOwners?: Map<string, RiskReductionOwnership>;
  /** Latest actual execution failure per open episode, shared by all takers. */
  riskReductionFailures?: Map<string, RiskReductionExecutionFailure>;
}

export const riskReductionOwnershipSchema = z.object({
  marketId: z.string().min(1),
  unhedgedStartedAt: z.string().datetime({ offset: true }),
  intent: z.enum(["HEDGE_SECOND_LEG", "PROTECTIVE_DE_RISK", "RESCUE_SECOND_LEG", "GUARDIAN_PROTECTIVE_SELL"]),
  order: paperOrderSchema,
  admittedResidualQuantity: z.string().refine((v) => d(v).isFinite() && d(v).gt(0)),
  decisionReceivedAt: z.string().datetime({ offset: true }),
  state: z.literal("ADMITTED"),
  deRisk: z.record(z.string(), z.unknown()).optional(),
}).superRefine((owner, ctx) => {
  if (owner.order.marketId !== owner.marketId || owner.order.orderType !== "TAKER"
    || owner.order.status !== "PENDING"
    || (owner.intent === "GUARDIAN_PROTECTIVE_SELL" && !d(owner.order.quantity).eq(owner.admittedResidualQuantity))
    || !d(owner.order.quantity).gt(0) || d(owner.order.quantity).gt(owner.admittedResidualQuantity)
    || owner.order.side !== ((owner.intent === "PROTECTIVE_DE_RISK" || owner.intent === "GUARDIAN_PROTECTIVE_SELL") ? "SELL" : "BUY")) {
    ctx.addIssue({ code: "custom", message: "invalid risk-reduction taker identity" });
  }
});
export type RiskReductionOwnership = z.infer<typeof riskReductionOwnershipSchema>;
export const riskReductionExecutionFailureSchema = z.object({
  ownership: riskReductionOwnershipSchema,
  reason: z.string().min(1),
});
export type RiskReductionExecutionFailure = z.infer<typeof riskReductionExecutionFailureSchema>;

export function unhedgedEpisode(position: Position): string {
  return position.unhedgedStartedAt ?? position.openedAt;
}

export const settlementLookupSchema = z.object({
  marketId: z.string().min(1),
  slug: z.string().nullable(),
  endDate: z.string().datetime({ offset: true }),
  yesMapsTo: z.enum(["UP", "DOWN"]),
  noMapsTo: z.enum(["UP", "DOWN"]),
}).refine((lookup) => lookup.yesMapsTo !== lookup.noMapsTo);

const fillPayloadSchema = z.object({
  nakedDirectionalSoak: z.boolean().optional(),
  fill: paperFillSchema,
  positionId: z.string().min(1),
  reservationAfter: z.string().nullable(),
  restingAfter: restingPaperOrderSchema.nullable().optional(),
  intent: z.enum(["OPEN_FIRST_LEG", "HEDGE_SECOND_LEG", "PROTECTIVE_DE_RISK", "RESCUE_SECOND_LEG", "GUARDIAN_PROTECTIVE_SELL"]).optional(),
  deRisk: z.record(z.string(), z.unknown()).optional(),
  settlementLookup: settlementLookupSchema.optional(),
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
      case "RISK_REDUCTION_ADMITTED": {
        const owner = riskReductionOwnershipSchema.parse(transition.payload.ownership);
        const owners = state.riskReductionOwners ??= new Map();
        if (owners.has(owner.marketId)) throw new RecoveryFailClosedError("invariant", "overlapping taker ownership");
        const position = state.ledger.positions.get(owner.marketId);
        if (!position?.unpairedOutcome || state.settledMarkets.has(owner.marketId)
          || unhedgedEpisode(position) !== owner.unhedgedStartedAt
          || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)) {
          throw new RecoveryFailClosedError("invariant", "admission does not match durable residual");
        }
        const failure = state.riskReductionFailures?.get(owner.marketId);
        if (failure && failure.ownership.unhedgedStartedAt !== owner.unhedgedStartedAt) state.riskReductionFailures!.delete(owner.marketId);
        owners.set(owner.marketId, owner);
        return;
      }
      case "RISK_REDUCTION_FAILED": {
        const owner = riskReductionOwnershipSchema.parse(transition.payload.ownership);
        if (transition.payload.executionFailure !== undefined && typeof transition.payload.executionFailure !== "boolean") {
          throw new RecoveryFailClosedError("malformed", "invalid execution failure marker");
        }
        if (typeof transition.payload.reason !== "string" || !transition.payload.reason) {
          throw new RecoveryFailClosedError("malformed", "failure missing reason");
        }
        const current = state.riskReductionOwners?.get(owner.marketId);
        if (current?.order.id === owner.order.id && current.unhedgedStartedAt === owner.unhedgedStartedAt) {
          state.riskReductionOwners!.delete(owner.marketId);
          const position = state.ledger.positions.get(owner.marketId);
          if (transition.payload.executionFailure === true && position?.unpairedOutcome
            && unhedgedEpisode(position) === owner.unhedgedStartedAt
            && d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)) {
            (state.riskReductionFailures ??= new Map()).set(owner.marketId,
              { ownership: owner, reason: transition.payload.reason });
          }
        }
        return;
      }
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
          state.resultAccounting = emptyResultAccounting();
          state.restingOrders.clear();
          state.makerReservations.clear();
          state.profitTrailingPairStates?.clear();
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
      case "PROFIT_TRAILING_PAIR_STATE_UPDATED": {
        const trailing = profitTrailingPairStateSchema.parse(transition.payload.state);
        const position = state.ledger.positions.get(trailing.marketId);
        if (!position?.unpairedOutcome || position.id !== trailing.positionId
          || unhedgedEpisode(position) !== trailing.unhedgedStartedAt
          || d(position.unpairedQuantity).lte(0)) {
          throw new RecoveryFailClosedError("invariant", "profit trailing state does not match durable unpaired episode");
        }
        const map = state.profitTrailingPairStates ??= new Map();
        const previous = map.get(trailing.marketId);
        if (previous && previous.positionId === trailing.positionId
          && previous.unhedgedStartedAt === trailing.unhedgedStartedAt
          && trailing.revision <= previous.revision) {
          throw new RecoveryFailClosedError("invariant", "profit trailing revision did not advance");
        }
        map.set(trailing.marketId, trailing);
        return;
      }
      case "PAPER_FILL": {
        const payload = fillPayloadSchema.parse(transition.payload);
        if (payload.settlementLookup && payload.settlementLookup.marketId !== payload.fill.marketId) {
          throw new RecoveryFailClosedError("invariant", "fill settlement lookup market mismatch");
        }
        const owner = state.riskReductionOwners?.get(payload.fill.marketId);
        if (owner?.order.id === payload.fill.orderId) {
          const position = state.ledger.positions.get(owner.marketId);
          if (!position?.unpairedOutcome || unhedgedEpisode(position) !== owner.unhedgedStartedAt
            || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)
            || payload.fill.tokenId !== owner.order.tokenId || payload.fill.outcome !== owner.order.outcome
            || payload.fill.side !== owner.order.side || !d(payload.fill.quantity).eq(owner.order.quantity)) {
            throw new RecoveryFailClosedError("invariant", "fill does not match admitted taker ownership");
          }
        }
        applyFillReservation(state, payload.fill, payload.reservationAfter, new Date(transition.occurredAt));
        const previous = state.ledger.positions.get(payload.fill.marketId);
        state.ledger.applyFill(payload.fill, { positionId: payload.positionId });
        const remaining = state.ledger.positions.get(payload.fill.marketId);
        accountResultFill(state.resultAccounting ??= emptyResultAccounting(), payload.fill, previous, remaining);
        const trailing = state.profitTrailingPairStates?.get(payload.fill.marketId);
        if (trailing && (!remaining?.unpairedOutcome || remaining.id !== trailing.positionId
          || unhedgedEpisode(remaining) !== trailing.unhedgedStartedAt || d(remaining.unpairedQuantity).lte(0))) {
          state.profitTrailingPairStates!.delete(payload.fill.marketId);
        }
        if (payload.nakedDirectionalSoak === true && remaining) {
          const diagnostics = state.nakedDirectionalDiagnostics ??= new Map();
          if (!diagnostics.has(payload.fill.marketId)) {
            diagnostics.set(payload.fill.marketId, createNakedDirectionalDiagnostic(remaining, payload.fill));
          }
          const diagnostic = diagnostics.get(payload.fill.marketId)!;
          diagnostic.entryPrice = payload.fill.outcome === "UP" ? remaining.averageUpCost : remaining.averageDownCost;
          diagnostic.entryQuantity = remaining.unpairedQuantity;
          diagnostic.entryFee = remaining.totalFees;
        }
        const lookups = state.settlementLookups ??= new Map();
        if (remaining && (d(remaining.upQuantity).gt(0) || d(remaining.downQuantity).gt(0))) {
          // Preserve the original economic mapping across hedges and rediscovery.
          if (payload.settlementLookup && !lookups.has(payload.fill.marketId)) {
            lookups.set(payload.fill.marketId, structuredClone(payload.settlementLookup));
          }
        } else lookups.delete(payload.fill.marketId);
        const failure = state.riskReductionFailures?.get(payload.fill.marketId);
        if (failure && (!remaining?.unpairedOutcome || !d(remaining.unpairedQuantity).gt(0)
          || unhedgedEpisode(remaining) !== failure.ownership.unhedgedStartedAt
          || !d(remaining.unpairedQuantity).eq(failure.ownership.admittedResidualQuantity))) {
          state.riskReductionFailures!.delete(payload.fill.marketId);
        }
        // Completion is atomic with the economic fill, never a later release.
        if (state.riskReductionOwners?.get(payload.fill.marketId)?.order.id === payload.fill.orderId) {
          state.riskReductionOwners.delete(payload.fill.marketId);
        }
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
        const settled = state.ledger.settlePosition(payload.marketId, payload.upPayoff, payload.downPayoff, new Date(payload.settledAt));
        if (settled) completeResult(state.resultAccounting ??= emptyResultAccounting(), payload.marketId, settled.realizedDelta, settled.position.totalFees);
        state.settlementLookups?.delete(payload.marketId);
        state.riskReductionOwners?.delete(payload.marketId);
        state.riskReductionFailures?.delete(payload.marketId);
        state.nakedDirectionalDiagnostics?.delete(payload.marketId);
        state.profitTrailingPairStates?.delete(payload.marketId);
        return;
      }
      case "NAKED_DIRECTIONAL_OBSERVATION": {
        const diagnostics = z.array(nakedDirectionalDiagnosticSchema).parse(transition.payload.diagnostics);
        for (const diagnostic of diagnostics) {
          const position = state.ledger.positions.get(diagnostic.marketId);
          if (!position || position.id !== diagnostic.positionId) throw new RecoveryFailClosedError("invariant", "soak observation without real position");
          const map = state.nakedDirectionalDiagnostics ??= new Map();
          const previous = map.get(diagnostic.marketId);
          // Live books may advance while this compact snapshot waits on the
          // durable gate. Never overwrite newer in-memory extrema.
          if (!previous || previous.revision < diagnostic.revision) map.set(diagnostic.marketId, mergeNakedDirectionalObservation(previous, diagnostic));
        }
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
  if (state.resultAccounting && !resultAccountingSchema.safeParse(state.resultAccounting).success) reasons.push("result_accounting_invalid");
  for (const marketId of Object.keys(state.resultAccounting?.episodes ?? {})) {
    if (!state.ledger.positions.has(marketId)) reasons.push("result_episode_without_position");
  }
  if (state.resultAccounting?.available) {
    for (const position of state.ledger.positions.values()) {
      const episode = state.resultAccounting.episodes[position.marketId];
      if (!episode || !d(episode.fees).eq(position.totalFees)) reasons.push("result_episode_fees_mismatch");
    }
  }
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
  for (const [marketId, diagnostic] of state.nakedDirectionalDiagnostics ?? []) {
    if (!nakedDirectionalDiagnosticSchema.safeParse(diagnostic).success
      || diagnostic.marketId !== marketId || state.ledger.positions.get(marketId)?.id !== diagnostic.positionId) {
      reasons.push("naked_directional_diagnostic_invalid");
    }
  }
  if (state.experiment?.riskConfiguration.profitTrailingPairSoakEnabled === true) {
    const risk = state.experiment.riskConfiguration;
    const target = Number(risk.profitTrailingPairTargetEdge ?? risk.normalHedgeMinLockedPairEdge);
    const arm = Number(risk.profitTrailingPairArmEdge);
    const retrace = Number(risk.profitTrailingPairRetraceEdge);
    if (!Number.isFinite(target) || !Number.isFinite(arm) || !Number.isFinite(retrace)
      || target <= 0 || arm < 0 || retrace <= 0 || arm >= target || retrace >= target
      || risk.nakedDirectionalSoakEnabled === true) {
      reasons.push("profit_trailing_pair_config_invalid");
    }
  }
  for (const [marketId, trailing] of state.profitTrailingPairStates ?? []) {
    const position = state.ledger.positions.get(marketId);
    if (!profitTrailingPairStateSchema.safeParse(trailing).success || trailing.marketId !== marketId
      || !position?.unpairedOutcome || position.id !== trailing.positionId
      || unhedgedEpisode(position) !== trailing.unhedgedStartedAt || d(position.unpairedQuantity).lte(0)) {
      reasons.push("profit_trailing_pair_state_invalid");
    }
  }
  for (const [marketId, owner] of state.riskReductionOwners ?? []) {
    if (!riskReductionOwnershipSchema.safeParse(owner).success || marketId !== owner.marketId) reasons.push("risk_reduction_owner_invalid");
    if (state.settledMarkets.has(marketId)) reasons.push("settled_and_pending_taker");
  }
  for (const [marketId, failure] of state.riskReductionFailures ?? []) {
    if (!riskReductionExecutionFailureSchema.safeParse(failure).success || marketId !== failure.ownership.marketId) {
      reasons.push("risk_reduction_failure_invalid");
    }
  }
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
  "NAKED_DIRECTIONAL_SUMMARY",
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
