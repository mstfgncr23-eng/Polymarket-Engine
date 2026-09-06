import { createHash } from "node:crypto";
import { z } from "zod";

/** Closed set of replay-authoritative paper-state transitions. */
export const DURABLE_TRANSITION_TYPES = [
  "EXPERIMENT_STARTED",
  "EXPERIMENT_START_VALIDATED",
  "EXPERIMENT_START_ABORTED",
  "EXPERIMENT_ENTRY_CLOSED",
  "EXPERIMENT_COMPLETED",
  "PAPER_MAKER_RESERVED",
  "PAPER_MAKER_RELEASED",
  "PAPER_ORDER_CANCELLED",
  "PAPER_FILL",
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
  const cursor = payload.durableCursor;
  if (cursor != null && (typeof cursor !== "number" || !Number.isInteger(cursor) || cursor < 0)) return false;
  const ledger = payload.ledger;
  if (ledger == null) return true; // legacy checkpoints had no projected ledger
  if (typeof ledger !== "object") return false;
  const l = ledger as Record<string, unknown>;
  for (const keys of [["cash", "cashUsd"], ["reservedCash", "reservedUsd"], ["startingBalance", "startingBalanceUsd"]]) {
    const raw = l[keys[0]!] ?? l[keys[1]!];
    if (raw == null) continue;
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) return false;
  }
  for (const key of ["positions", "restingOrders", "activeTriggers", "grokSignals"]) {
    if (payload[key] != null && !Array.isArray(payload[key])) return false;
  }
  return true;
}

/** Minimal replay identity retained after durable transition compaction. */
export interface CompactedOperationKey {
  operationKey: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
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
