import { createHash } from "node:crypto";
import { z } from "zod";
import { d, experimentManifestSchema, ledgerStateSchema, positionSchema, restingPaperOrderSchema, nakedDirectionalDiagnosticSchema, profitTrailingPairStateSchema, reversalProtectionStateSchema } from "@pm/core";

/** Closed set of replay-authoritative paper-state transitions. */
export const DURABLE_TRANSITION_TYPES = [
  "EXPERIMENT_STARTED",
  "EXPERIMENT_START_VALIDATED",
  "EXPERIMENT_START_ABORTED",
  "EXPERIMENT_ENTRY_CLOSED",
  "EXPERIMENT_COMPLETED",
  "EXPERIMENT_WS_EVENT",
  "EXPERIMENT_NON_FINAL_QUARANTINED",
  "PAPER_MAKER_RESERVED",
  "PAPER_MAKER_RELEASED",
  "PAPER_ORDER_CANCELLED",
  "RISK_REDUCTION_ADMITTED",
  "RISK_REDUCTION_FAILED",
  "PAPER_FILL",
  "NAKED_DIRECTIONAL_OBSERVATION",
  "PROFIT_TRAILING_PAIR_STATE_UPDATED",
  "REVERSAL_PROTECTION_STATE_UPDATED",
  "POSITION_SETTLED",
  "LEDGER_MARK_TO_MARKET",
  "INFRASTRUCTURE_COST_ACCRUED",
  "REPORT_SOURCE_INTEGRITY_FAULT",
  "REPORT_SOURCE_INTEGRITY_REPAIRED",
] as const;

export type DurableTransitionType = (typeof DURABLE_TRANSITION_TYPES)[number];

export const DURABLE_METRIC_TYPES = [...DURABLE_TRANSITION_TYPES, "other"] as const;
export type DurableMetricType = (typeof DURABLE_METRIC_TYPES)[number];

export function durableMetricType(type: string): DurableMetricType {
  return (DURABLE_TRANSITION_TYPES as readonly string[]).includes(type) ? type as DurableMetricType : "other";
}

export const CHECKPOINT_SCHEMA_VERSION = 2;

/** Version for the verifiable recovery-snapshot envelope used by compaction. */
export const RECOVERY_SNAPSHOT_SCHEMA_VERSION = 1;

/** Postgres advisory lock key for the durable-mutation + checkpoint fence. */
export const DURABLE_MUTATION_LOCK_ID = 872_314_001;
/** Separate advisory lock for the journal event admission fence. */
export const JOURNAL_COMMIT_LOCK_ID = 872_314_002;

export const durableTransitionInputSchema = z.object({
  operationKey: z.string().min(1).max(160),
  type: z.string().min(1).max(80),
  aggregateType: z.string().min(1).max(80),
  aggregateId: z.string().min(1).max(160),
  payload: z.record(z.string(), z.unknown()),
  occurredAt: z.date().optional(),
});
export type DurableTransitionInput = z.input<typeof durableTransitionInputSchema>;

export interface DurableTransition {
  sequence: number;
  operationKey: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  payloadHash: string;
  payload: Record<string, unknown>;
  occurredAt: string;
  recordedAt: string;
}

export interface DurableCommitResult {
  sequence: number;
  operationKey: string;
  inserted: boolean;
  existing: boolean;
  transition: DurableTransition;
}

/** Bounded live retries while catching up to a foreign durable tail. */
export const MAX_DURABLE_CURSOR_CONVERGENCE_ATTEMPTS = 8;

export class IdempotencyConflictError extends Error {
  readonly code = "IDEMPOTENCY_CONFLICT";
  constructor(
    readonly operationKey: string,
    readonly existingType: string,
    readonly incomingType: string,
  ) {
    super(`IDEMPOTENCY_CONFLICT: key=${operationKey} existingType=${existingType} incomingType=${incomingType}`);
    this.name = "IdempotencyConflictError";
  }
}

export class DurableCursorBehindError extends Error {
  readonly code = "DURABLE_CURSOR_BEHIND";
  constructor(
    readonly expectedPreviousSequence: number,
    readonly latestCommitted: number,
  ) {
    super(`DURABLE_CURSOR_BEHIND: expected previous ${expectedPreviousSequence} but latest committed is ${latestCommitted}`);
    this.name = "DurableCursorBehindError";
  }
}

export class CheckpointFenceError extends Error {
  readonly code = "CHECKPOINT_FENCE";
  constructor(
    readonly claimedCursor: number,
    readonly latestCommitted: number,
  ) {
    super(`CHECKPOINT_FENCE: claimed cursor ${claimedCursor} skips or exceeds latest committed ${latestCommitted}`);
    this.name = "CheckpointFenceError";
  }
}

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
}

export function authoritativeDurablePayload(payload: Record<string, unknown>): Record<string, unknown> {
  const { derivedAudit: _derivedAudit, ...authoritative } = payload;
  return authoritative;
}

export function hashDurablePayload(payload: Record<string, unknown>): string {
  return createHash("sha256").update(canonicalJson(authoritativeDurablePayload(payload))).digest("hex");
}

/** Deterministic integrity proof for a checkpoint payload. */
export function hashCheckpointPayload(payload: Record<string, unknown>): string {
  const { contentHash: _contentHash, checkpointContentHash: _checkpointContentHash, ...content } = payload;
  return createHash("sha256").update(canonicalJson(content)).digest("hex");
}

/** Recovery authority requires structural state invariants in addition to a hash. */
export function isSemanticallyValidCheckpointPayload(payload: Record<string, unknown>): boolean {
  if (!payload || typeof payload !== "object") return false;
  if (payload.nakedDirectionalDiagnostics !== undefined) {
    if (!Array.isArray(payload.nakedDirectionalDiagnostics)) return false;
    if (!payload.nakedDirectionalDiagnostics.every((item) => nakedDirectionalDiagnosticSchema.safeParse(item).success)) return false;
  }
  if (payload.reversalProtectionStates !== undefined) {
    if (!Array.isArray(payload.reversalProtectionStates)) return false;
    if (!payload.reversalProtectionStates.every((item) => reversalProtectionStateSchema.safeParse(item).success)) return false;
  }
  if (payload.profitTrailingPairStates !== undefined) {
    if (!Array.isArray(payload.profitTrailingPairStates)) return false;
    if (!payload.profitTrailingPairStates.every((item) => profitTrailingPairStateSchema.safeParse(item).success)) return false;
  }
  const cursor = payload.durableCursor;
  if (cursor != null && (typeof cursor !== "number" || !Number.isInteger(cursor) || cursor < 0)) return false;
  const ledger = payload.ledger;
  if (ledger == null) return true; // legacy checkpoints had no projected ledger
  if (typeof ledger !== "object") return false;
  const l = ledger as Record<string, unknown>;
  // Full schema-v2 snapshots use the same parsers as recovery. Older compact
  // fixtures are accepted only when they do not claim a full recoverable state.
  const fullLedger = Object.prototype.hasOwnProperty.call(l, "cash") && Object.prototype.hasOwnProperty.call(l, "reservedCash");
  if (fullLedger) {
    try { ledgerStateSchema.parse(l); } catch { return false; }
  } else {
    for (const keys of [["cash", "cashUsd"], ["reservedCash", "reservedUsd"], ["startingBalance", "startingBalanceUsd"]]) {
      const raw = l[keys[0]!] ?? l[keys[1]!];
      if (raw == null) continue;
      const value = Number(raw);
      if (!Number.isFinite(value) || value < 0) return false;
    }
  }
  if (fullLedger && payload.schemaVersion === CHECKPOINT_SCHEMA_VERSION) {
    for (const key of ["positions", "restingOrders", "activeTriggers", "grokSignals", "settledMarkets"]) {
      if (!Array.isArray(payload[key])) return false;
    }
  }
  for (const key of ["positions", "restingOrders", "activeTriggers", "grokSignals"]) {
    if (payload[key] != null && !Array.isArray(payload[key])) return false;
  }
  if (Array.isArray(payload.positions) && payload.positions.length > 0 && fullLedger) {
    try { payload.positions.forEach((row) => positionSchema.parse(row)); } catch { return false; }
  }
  const reservations = payload.makerReservations;
  const resting = payload.restingOrders;
  if (fullLedger && Array.isArray(resting)) {
    try { resting.forEach((row) => restingPaperOrderSchema.parse(row)); } catch { return false; }
    if (reservations == null || typeof reservations !== "object" || Array.isArray(reservations)) return false;
    const reservationMap = reservations as Record<string, unknown>;
    let sum = d(0);
    for (const [orderId, amount] of Object.entries(reservationMap)) {
      try { const n = d(String(amount)); if (!n.gt(0)) return false; sum = sum.plus(n); } catch { return false; }
      if (!resting.some((row) => row && typeof row === "object" && (row as { order?: { id?: unknown } }).order?.id === orderId)) return false;
    }
    for (const row of resting) {
      const orderId = row && typeof row === "object" ? (row as { order?: { id?: unknown } }).order?.id : null;
      const remaining = row && typeof row === "object" ? (row as { remaining?: unknown }).remaining : null;
      if (typeof orderId !== "string" || !(orderId in reservationMap)) return false;
      try { if (!d(String(remaining)).gt(0)) return false; } catch { return false; }
    }
    try {
      const reserved = d(String(l.reservedCash));
      if (!sum.eq(reserved)) return false;
    } catch { return false; }
  }
  if (payload.experiment != null && fullLedger) {
    try { experimentManifestSchema.parse(payload.experiment); } catch { return false; }
  }
  if (Array.isArray(payload.positions) && Array.isArray(payload.settledMarkets)) {
    const settled = new Set(payload.settledMarkets.filter((id): id is string => typeof id === "string"));
    for (const row of payload.positions) {
      if (!row || typeof row !== "object") continue;
      const position = row as { marketId?: unknown; upQuantity?: unknown; downQuantity?: unknown };
      try {
        if (typeof position.marketId === "string" && settled.has(position.marketId)
          && (d(String(position.upQuantity ?? 0)).gt(0) || d(String(position.downQuantity ?? 0)).gt(0))) return false;
      } catch { return false; }
    }
  }
  return true;
}

/** Minimal replay identity retained after durable transition compaction. */
export interface CompactedOperationKey {
  operationKey: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  /** Small lifecycle scope used to close related identities in bulk. */
  scopeId?: string | null;
  payloadHash: string;
  sequence: number;
  occurredAt: string;
  recordedAt: string;
  expiresAt: string;
  /** OPEN identities are pinned independently of TTL/pressure. */
  lifecycleState?: "open" | "closed" | "historical";
}

export interface DurableCompactionResult {
  deleted: number;
  cursor: number;
  highWater: number;
  compactedOperationKeys: number;
}

export function derivedAuditEventId(kind: string, authorityId: string): string {
  const id = `derived:${kind}:${authorityId}`;
  return id.length <= 160 ? id : id.slice(0, 160);
}

export function isDurableTransitionType(type: string): type is DurableTransitionType {
  return (DURABLE_TRANSITION_TYPES as readonly string[]).includes(type);
}
