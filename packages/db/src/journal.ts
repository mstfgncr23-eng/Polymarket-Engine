import { and, asc, desc, eq, gt, gte, inArray, lt, lte, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import {
  CANONICAL_REPORT_INPUT_EVENT_TYPES,
  compareJournalEventOrder, createId, EVENT_PAGE_SIZE, EVENT_SCAN_CEILING, isProtectedJournalType,
  journalEventSchema, sortJournalEventsChronologically,
  type JournalEvent, type JournalKeysetCursor, type JournalKeysetQuery, type JournalQuery,
} from "@pm/core";
import {
  CheckpointFenceError,
  DURABLE_MUTATION_LOCK_ID,
  DurableCursorBehindError,
  hashCheckpointPayload,
  isSemanticallyValidCheckpointPayload,
  durableTransitionInputSchema,
  hashDurablePayload,
  IdempotencyConflictError,
  type DurableCommitResult,
  type CompactedOperationKey,
  type DurableCompactionResult,
  type DurableTransition,
  type DurableTransitionInput,
} from "./durable.js";
import {
  ExperimentReportConflictError,
  hashExperimentReport,
  persistReportConflictSafe,
  type DurableExperimentReport,
  type DurableExperimentReportInput,
  type PersistReportResult,
} from "./reports.js";
import { durableReplayKeys, durableSequenceMetadata, durableTransitions, engineCheckpoints, experimentReportRollups, experimentReports, journalCommitMetadata, journalEvents, mcpIdempotencyKeys } from "./schema.js";
import {
  journalMayMaintain,
  journalMayMigrate,
  journalMayPersistReport,
  journalMayRuntimeWrite,
  journalMustVerifySchema,
  JournalMigrateForbiddenError,
  JournalMutateForbiddenError,
  LARGE_JOURNAL_KEYSET_INDEXES,
  LARGE_JOURNAL_SYNC_INDEX_MAX_ROWS,
  type JournalAccessMode,
} from "./access.js";
import { SchemaVerificationError, verifyRequiredSchema } from "./schema-verify.js";
import type { EventJournalKeyset } from "./scan.js";

export interface EngineCheckpoint {
  id: string;
  experimentId: string | null;
  sequence: number;
  payload: Record<string, unknown>;
  createdAt: string;
  /** Present for PR18 verified snapshots; absent on legacy checkpoints. */
  contentHash?: string;
  verified?: boolean;
}

export interface AnalyticsRollup {
  experimentId: string;
  rollupKind: string;
  schemaVersion: number;
  sourceCursor: number;
  sourceFenceId: string | null;
  sourceFenceOccurredAt: string | null;
  sourceEventCount?: number;
  sourceCommitSeq?: number;
  sourceRecordedAt?: string | null;
  stableRead?: boolean;
  previousContentHash?: string | null;
  coverageHash?: string;
  accumulator: Record<string, unknown>;
  contentHash: string;
  verified: boolean;
  createdAt: string;
}

export interface JournalAppendOptions {
  signal?: AbortSignal;
  deadlineAt?: number;
}

export class JournalAppendAbortedError extends Error {
  readonly code = "JOURNAL_APPEND_ABORTED";
  constructor(readonly reason: "aborted" | "deadline" = "aborted") {
    super(reason === "deadline" ? "JOURNAL_APPEND_DEADLINE" : "JOURNAL_APPEND_ABORTED");
    this.name = "JournalAppendAbortedError";
  }
}

/** Duck-typed so bundled/src copies of this class still classify abort/deadline. */
export function isJournalAppendAbortedError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  if (error instanceof JournalAppendAbortedError) return true;
  const name = "name" in error ? String(error.name) : "";
  const code = "code" in error ? String(error.code) : "";
  const message = error instanceof Error ? error.message : "";
  return name === "JournalAppendAbortedError"
    || code === "JOURNAL_APPEND_ABORTED"
    || message === "JOURNAL_APPEND_DEADLINE"
    || message === "JOURNAL_APPEND_ABORTED";
}

export function isJournalAppendDeadlineError(error: unknown): boolean {
  if (!isJournalAppendAbortedError(error) || !error || typeof error !== "object") return false;
  if (error instanceof JournalAppendAbortedError) return error.reason === "deadline";
  const reason = "reason" in error ? String((error as { reason?: unknown }).reason) : "";
  const message = error instanceof Error ? error.message : "";
  return reason === "deadline" || message === "JOURNAL_APPEND_DEADLINE";
}

/** Backend terminate / pool poison after transaction_timeout. Safe to retry once. */
export function isTransientJournalError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  if (isJournalAppendAbortedError(error)) return false;
  const code = "code" in error ? String(error.code) : "";
  const message = error instanceof Error ? error.message : String(error);
  return code === "CONNECTION_CLOSED"
    || code === "CONNECTION_DESTROYED"
    || code === "ECONNRESET"
    || code === "57P01"
    || code === "57P02"
    || code === "57P03"
    || code === "25P04"
    || code === "08006"
    || code === "08003"
    || /connection (closed|terminated|ended|destroyed)/i.test(message)
    || /terminating connection/i.test(message)
    || /Cannot read properties of null/.test(message);
}

/** Memory's holds precede mutation, so cancellation proves that no commit can follow. */
async function waitForAppendHold(hold: Promise<void>, options: JournalAppendOptions): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  try {
    await Promise.race([hold, new Promise<never>((_, reject) => {
      abort = () => reject(new JournalAppendAbortedError("aborted"));
      options.signal?.addEventListener("abort", abort, { once: true });
      if (options.signal?.aborted) abort();
      if (options.deadlineAt != null) {
        timer = setTimeout(() => reject(new JournalAppendAbortedError("deadline")), Math.max(0, options.deadlineAt - Date.now()));
      }
    })]);
  } finally {
    if (timer) clearTimeout(timer);
    if (abort) options.signal?.removeEventListener("abort", abort);
  }
}

export interface EventJournal {
  initialize(): Promise<void>;
  healthCheck(): Promise<boolean>;
  append(event: JournalEvent, options?: JournalAppendOptions): Promise<void>;
  appendMany(events: readonly JournalEvent[]): Promise<void>;
  readTrace(traceId: string): Promise<JournalEvent[]>;
  readTracePage(traceId: string, input: { after?: JournalKeysetCursor; limit: number }): Promise<{ events: JournalEvent[]; nextCursor: JournalKeysetCursor | null }>;
  readAll(): Promise<JournalEvent[]>;
  queryEvents(query: JournalQuery): Promise<JournalEvent[]>;
  queryEventsKeyset(query: JournalKeysetQuery): Promise<JournalEvent[]>;
  peekLatestKeyset(query: Omit<JournalKeysetQuery, "limit" | "after" | "through">): Promise<JournalKeysetCursor | null>;
  countByType(types?: readonly string[]): Promise<Record<string, number>>;
  claimIdempotency(scope: string, key: string): Promise<boolean>;
  pruneIdempotencyKeys(olderThan: Date, limit: number): Promise<{ deleted: number }>;
  idempotencyCreatedAt(scope: string, key: string): Promise<string | null>;
  persistExperimentReport(input: DurableExperimentReportInput): Promise<PersistReportResult>;
  loadExperimentReport(experimentId: string, reportKind?: "final"): Promise<DurableExperimentReport | null>;
  deleteJournalEventsBounded(input: {
    types: readonly string[];
    occurredBefore: Date;
    limit: number;
  }): Promise<{ deleted: number }>;
  pruneOldCheckpoints(keepLatest: number, limit?: number): Promise<{ deleted: number; latestSequence: number | null }>;
  listCheckpoints(): Promise<EngineCheckpoint[]>;
  commitDurableTransition(input: DurableTransitionInput, expectedPreviousSequence: number): Promise<DurableCommitResult>;
  readDurableTransitionsAfter(sequence: number, limit: number): Promise<DurableTransition[]>;
  queryDurableTransitions(query: { types?: readonly string[]; afterSequence?: number; limit: number }): Promise<DurableTransition[]>;
  readDurableByOperationKey(operationKey: string): Promise<DurableTransition | null>;
  readJournalEventById(id: string): Promise<JournalEvent | null>;
  latestDurableSequence(): Promise<number>;
  saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint>;
  loadLatestCheckpoint(): Promise<EngineCheckpoint | null>;
  /** Snapshot/compaction APIs are optional so legacy journal adapters remain valid. */
  compactDurableTransitionsThrough?(cursor: number, options?: { maxReplayKeys?: number; ttlMs?: number; maxRows?: number; maxBatches?: number; batchSize?: number; budgetMs?: number }): Promise<DurableCompactionResult>;
  listVerifiedCheckpoints?(): Promise<EngineCheckpoint[]>;
  persistAnalyticsRollup?(rollup: AnalyticsRollup): Promise<AnalyticsRollup>;
  loadAnalyticsRollup?(experimentId: string, rollupKind?: string): Promise<AnalyticsRollup | null>;
  deleteCanonicalEventsThrough?(input: { experimentId: string; occurredBefore?: Date; limit: number; sourceCursor: number; sourceCommitSeq?: number; sourceRecordedAt?: Date }): Promise<{ deleted: number }>;
  countEligible(types: readonly string[], occurredBefore: Date): Promise<{ count: number; oldestOccurredAt: string | null }>;
  withReadSnapshot<T>(work: (journal: EventJournalKeyset) => Promise<T>): Promise<{ value: T; stable: boolean }>;
  latestCommitSeq(): Promise<number>;
  eventCommitSeq?(id: string): Promise<number | null>;
  connectWithoutMigrate?(mode: JournalAccessMode): Promise<void>;
  close(): Promise<void>;
}

/**
 * Optional bounded in-memory observability mode.  It is used by local
 * capacity probes so their journal bookkeeping cannot masquerade as engine
 * retained state.  The default remains unbounded for the deterministic
 * memory journal used by correctness tests.
 */
export interface MemoryEventJournalOptions {
  maxEvents?: number;
  maxMutations?: number;
}

export class MemoryEventJournal implements EventJournal {
  private readonly events: JournalEvent[] = [];
  private readonly eventIds = new Set<string>();
  private readonly staged = new Map<string, JournalEvent>();
  private readonly commitSeqById = new Map<string, number>();
  private nextCommitSeq = 0;
  private readonly checkpoints: EngineCheckpoint[] = [];
  private readonly idempotencyKeys = new Map<string, string>();
  private readonly reports = new Map<string, DurableExperimentReport>();
  private readonly rollups = new Map<string, AnalyticsRollup>();
  private readonly durableRows: DurableTransition[] = [];
  private readonly durableByKey = new Map<string, DurableTransition>();
  private readonly compactedByKey = new Map<string, CompactedOperationKey>();
  private durableHighWater = 0;
  private readonly maxEvents: number;
  private readonly maxMutations: number;
  private durableChain = Promise.resolve();
  private persistChain = Promise.resolve();
  available = true;
  failNextDurableInsert = false;
  migrateInvocations = 0;
  readonly mutations: string[] = [];
  readonly failAppendTypes = new Set<string>();
  access: JournalAccessMode = "boot-migrate";
  appendInFlightHold: Promise<void> | null = null;
  appendInFlightHoldTypes: string[] | null = null;
  appendAcquireHold: Promise<void> | null = null;
  appendAcquireHoldTypes: string[] | null = null;
  inFlightAppends = 0;

  constructor(options: MemoryEventJournalOptions = {}) {
    this.maxEvents = options.maxEvents == null ? Number.POSITIVE_INFINITY : Math.max(1, Math.floor(options.maxEvents));
    this.maxMutations = options.maxMutations == null ? Number.POSITIVE_INFINITY : Math.max(1, Math.floor(options.maxMutations));
  }

  /** Counts for local capacity evidence; this does not expose production state. */
  memoryFootprintForTest(): { events: number; eventIds: number; commitSeqById: number; mutations: number } {
    return {
      events: this.events.length,
      eventIds: this.eventIds.size,
      commitSeqById: this.commitSeqById.size,
      mutations: this.mutations.length,
    };
  }

  /** Read-only hot-table approximation for deterministic plateau evidence. */
  storageFootprintForTest(): Record<string, { rows: number; bytes: number }> {
    const size = (value: unknown): number => Buffer.byteLength(JSON.stringify(value) ?? "null");
    return {
      journal_events: { rows: this.events.length, bytes: this.events.reduce((n, row) => n + size(row), 0) },
      durable_transitions: { rows: this.durableRows.length, bytes: this.durableRows.reduce((n, row) => n + size(row), 0) },
      durable_replay_keys: { rows: this.compactedByKey.size, bytes: [...this.compactedByKey.values()].reduce((n, row) => n + size(row), 0) },
      engine_checkpoints: { rows: this.checkpoints.length, bytes: this.checkpoints.reduce((n, row) => n + size(row), 0) },
      mcp_idempotency: { rows: this.idempotencyKeys.size, bytes: [...this.idempotencyKeys].reduce((n, row) => n + size(row), 0) },
      experiment_report_rollups: { rows: this.rollups.size, bytes: [...this.rollups.values()].reduce((n, row) => n + size(row), 0) },
      experiment_reports: { rows: this.reports.size, bytes: [...this.reports.values()].reduce((n, row) => n + size(row), 0) },
    };
  }

  private recordMutation(value: string): void {
    this.mutations.push(value);
    if (this.mutations.length > this.maxMutations) this.mutations.splice(0, this.mutations.length - this.maxMutations);
  }

  private retainEvent(event: JournalEvent): void {
    this.eventIds.add(event.id);
    this.events.push(event);
    this.nextCommitSeq += 1;
    this.commitSeqById.set(event.id, this.nextCommitSeq);
    if (this.events.length <= this.maxEvents) return;
    const evicted = this.events.splice(0, this.events.length - this.maxEvents);
    for (const old of evicted) {
      this.eventIds.delete(old.id);
      this.commitSeqById.delete(old.id);
    }
  }

  setAppendInFlightHoldForTest(hold: Promise<void> | null, types?: string[]): void {
    this.appendInFlightHold = hold;
    this.appendInFlightHoldTypes = types ?? null;
  }

  /** Stall after append has begun but before the in-transaction deadline is recomputed (pool-acquire analogue). */
  setAppendAcquireHoldForTest(hold: Promise<void> | null, types?: string[]): void {
    this.appendAcquireHold = hold;
    this.appendAcquireHoldTypes = types ?? null;
  }

  async initialize(): Promise<void> {
    if (!journalMayMigrate(this.access)) throw new JournalMigrateForbiddenError(this.access);
    this.migrateInvocations += 1;
  }
  async connectWithoutMigrate(mode: JournalAccessMode): Promise<void> {
    if (journalMayMigrate(mode)) throw new JournalMigrateForbiddenError(mode);
    this.access = mode;
  }
  async healthCheck(): Promise<boolean> { return this.available; }
  async append(event: JournalEvent, options: JournalAppendOptions = {}): Promise<void> {
    assertJournalCapability(this.access, "runtime-write", "append");
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const parsed = journalEventSchema.parse(event);
    if (this.failAppendTypes.has(parsed.type)) throw new Error("JOURNAL_APPEND_FAILED");
    if (this.eventIds.has(parsed.id) || this.staged.has(parsed.id)) return;
    this.inFlightAppends += 1;
    try {
      const acquireApplies = !this.appendAcquireHoldTypes || this.appendAcquireHoldTypes.includes(parsed.type);
      if (this.appendAcquireHold && acquireApplies) await waitForAppendHold(this.appendAcquireHold, options);
      if (this.failAppendTypes.has(parsed.type)) throw new Error("JOURNAL_APPEND_FAILED");
      if (options.signal?.aborted) throw new JournalAppendAbortedError("aborted");
      if (options.deadlineAt != null && Date.now() >= options.deadlineAt) throw new JournalAppendAbortedError("deadline");
      const holdApplies = !this.appendInFlightHoldTypes || this.appendInFlightHoldTypes.includes(parsed.type);
      if (this.appendInFlightHold && holdApplies) await waitForAppendHold(this.appendInFlightHold, options);
      if (this.failAppendTypes.has(parsed.type)) throw new Error("JOURNAL_APPEND_FAILED");
      if (options.signal?.aborted) throw new JournalAppendAbortedError("aborted");
      if (options.deadlineAt != null && Date.now() >= options.deadlineAt) throw new JournalAppendAbortedError("deadline");
      if (this.eventIds.has(parsed.id) || this.staged.has(parsed.id)) return;
      this.retainEvent(structuredClone(parsed));
      this.recordMutation(`append:${parsed.type}`);
    } finally {
      this.inFlightAppends = Math.max(0, this.inFlightAppends - 1);
    }
  }
  /** Assign recordedAt now but keep the row invisible until commitStaged (commit-stability tests). */
  stageUncommitted(event: JournalEvent): void {
    const parsed = journalEventSchema.parse(event);
    this.staged.set(parsed.id, structuredClone(parsed));
  }
  commitStaged(id: string): void {
    const event = this.staged.get(id);
    if (!event) return;
    this.staged.delete(id);
    if (this.eventIds.has(id)) return;
    this.retainEvent(event);
    this.recordMutation(`commit:${event.type}`);
  }
  async appendMany(events: readonly JournalEvent[]): Promise<void> {
    assertJournalCapability(this.access, "runtime-write", "appendMany");
    for (const event of events) await this.append(event);
  }
  async readTrace(traceId: string): Promise<JournalEvent[]> { return structuredClone(this.events.filter((event) => event.traceId === traceId)); }
  async readTracePage(traceId: string, input: { after?: JournalKeysetCursor; limit: number }): Promise<{ events: JournalEvent[]; nextCursor: JournalKeysetCursor | null }> {
    const limit = Math.max(1, input.limit);
    const rows = this.events.filter((event) => event.traceId === traceId).sort(compareJournalEventOrder);
    const start = input.after
      ? rows.findIndex((event) => compareJournalEventOrder(event, input.after!) > 0)
      : 0;
    const slice = start < 0 ? [] : rows.slice(start, start + limit);
    const last = slice[slice.length - 1];
    return {
      events: structuredClone(slice),
      nextCursor: last && slice.length === limit ? { occurredAt: last.occurredAt, id: last.id } : null,
    };
  }
  async readAll(): Promise<JournalEvent[]> { return structuredClone(this.events); }
  async queryEvents(query: JournalQuery): Promise<JournalEvent[]> {
    let rows = this.events;
    if (query.types && query.types.length > 0) rows = rows.filter((event) => query.types!.includes(event.type));
    if (query.experimentId) {
      rows = rows.filter((event) => event.aggregateId === query.experimentId
        || (event.payload as { experimentId?: unknown }).experimentId === query.experimentId);
    }
    if (query.from) rows = rows.filter((event) => new Date(event.occurredAt).getTime() >= query.from!.getTime());
    if (query.to) rows = rows.filter((event) => new Date(event.occurredAt).getTime() <= query.to!.getTime());
    if (query.order === "asc") rows = [...rows].sort(compareJournalEventOrder);
    if (query.order === "desc") rows = [...rows].sort(compareJournalEventOrder).reverse();
    const offset = query.offset ?? 0;
    const limit = query.limit ?? rows.length;
    const page = structuredClone(rows.slice(offset, offset + limit));
    return query.order === "desc" ? page.reverse() : page;
  }
  async countByType(types?: readonly string[]): Promise<Record<string, number>> {
    const counts: Record<string, number> = {};
    for (const event of this.events) {
      if (types && !types.includes(event.type)) continue;
      counts[event.type] = (counts[event.type] ?? 0) + 1;
    }
    return counts;
  }
  async queryEventsKeyset(query: JournalKeysetQuery): Promise<JournalEvent[]> {
    const rows = this.filterKeyset(query).sort(compareJournalEventOrder).slice(0, Math.max(1, query.limit));
    return structuredClone(rows);
  }
  async peekLatestKeyset(query: Omit<JournalKeysetQuery, "limit" | "after" | "through">): Promise<JournalKeysetCursor | null> {
    const rows = this.filterKeyset({ ...query, limit: 1 }).sort(compareJournalEventOrder);
    const last = rows[rows.length - 1];
    return last ? { occurredAt: last.occurredAt, id: last.id } : null;
  }
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    assertJournalCapability(this.access, "runtime-write", "claimIdempotency");
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const composite = `${scope}\u0000${key}`;
    if (this.idempotencyKeys.has(composite)) return false;
    this.idempotencyKeys.set(composite, new Date().toISOString());
    return true;
  }
  async pruneIdempotencyKeys(olderThan: Date, limit: number): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "pruneIdempotencyKeys");
    const cutoff = olderThan.toISOString();
    const cap = Math.max(0, Math.floor(limit));
    if (cap <= 0) return { deleted: 0 };
    const victims = [...this.idempotencyKeys.entries()]
      .filter(([, createdAt]) => createdAt < cutoff)
      .sort((a, b) => a[1].localeCompare(b[1]))
      .slice(0, cap);
    for (const [key] of victims) this.idempotencyKeys.delete(key);
    return { deleted: victims.length };
  }
  async idempotencyCreatedAt(scope: string, key: string): Promise<string | null> {
    return this.idempotencyKeys.get(`${scope}\u0000${key}`) ?? null;
  }
  async persistExperimentReport(input: DurableExperimentReportInput): Promise<PersistReportResult> {
    assertJournalCapability(this.access, "report-write", "persistExperimentReport");
    return this.withPersistLock(() => persistReportConflictSafe(input, {
      insert: async (stored) => {
        const key = `${stored.experimentId}:${stored.reportKind}`;
        if (this.reports.has(key)) return "conflict";
        this.reports.set(key, structuredClone(stored));
        this.recordMutation("persistExperimentReport");
        return structuredClone(stored);
      },
      load: async () => {
        const existing = this.reports.get(`${input.experimentId}:${input.reportKind}`);
        return existing ? structuredClone(existing) : null;
      },
    }));
  }
  async loadExperimentReport(experimentId: string, reportKind: "final" = "final"): Promise<DurableExperimentReport | null> {
    const existing = this.reports.get(`${experimentId}:${reportKind}`);
    return existing ? structuredClone(existing) : null;
  }
  async deleteJournalEventsBounded(input: { types: readonly string[]; occurredBefore: Date; limit: number }): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "deleteJournalEventsBounded");
    if (input.types.some((type) => isProtectedJournalType(type))) {
      throw new Error("RETENTION_REFUSED_PROTECTED_TYPES");
    }
    const cap = Math.max(0, Math.floor(input.limit));
    if (cap <= 0) return { deleted: 0 };
    const cutoff = input.occurredBefore.toISOString();
    const victims = this.events
      .filter((event) => input.types.includes(event.type) && event.occurredAt < cutoff)
      .sort(compareJournalEventOrder)
      .slice(0, cap);
    const ids = new Set(victims.map((event) => event.id));
    for (let index = this.events.length - 1; index >= 0; index -= 1) {
      const id = this.events[index]!.id;
      if (ids.has(id)) {
        this.eventIds.delete(id);
        this.commitSeqById.delete(id);
        this.events.splice(index, 1);
      }
    }
    this.recordMutation(`deleteJournalEventsBounded:${victims.length}`);
    return { deleted: victims.length };
  }
  async countEligible(types: readonly string[], occurredBefore: Date): Promise<{ count: number; oldestOccurredAt: string | null }> {
    const cutoff = occurredBefore.toISOString();
    const rows = this.events
      .filter((event) => types.includes(event.type) && event.occurredAt < cutoff)
      .sort(compareJournalEventOrder);
    return { count: rows.length, oldestOccurredAt: rows[0]?.occurredAt ?? null };
  }
  async latestCommitSeq(): Promise<number> {
    return this.nextCommitSeq;
  }
  async eventCommitSeq(id: string): Promise<number | null> { return this.commitSeqById.get(id) ?? null; }
  async withReadSnapshot<T>(work: (journal: EventJournalKeyset) => Promise<T>): Promise<{ value: T; stable: boolean }> {
    const frozen = this.events.map((event) => structuredClone(event));
    const seq = new Map(this.commitSeqById);
    const maxSeq = this.nextCommitSeq;
    const snap: EventJournalKeyset = {
      queryEventsKeyset: async (query) => {
        const rows = filterKeysetRows(frozen, seq, query).sort(compareJournalEventOrder).slice(0, Math.max(1, query.limit));
        return structuredClone(rows);
      },
      peekLatestKeyset: async (query) => {
        const rows = filterKeysetRows(frozen, seq, { ...query, limit: 1 }).sort(compareJournalEventOrder);
        const last = rows[rows.length - 1];
        return last ? { occurredAt: last.occurredAt, id: last.id } : null;
      },
      latestCommitSeq: async () => maxSeq,
    };
    return { value: await work(snap), stable: true };
  }
  async pruneOldCheckpoints(keepLatest: number, limit?: number): Promise<{ deleted: number; latestSequence: number | null }> {
    assertJournalCapability(this.access, "maintenance", "pruneOldCheckpoints");
    const keep = Math.max(1, keepLatest);
    const ordered = [...this.checkpoints].sort((a, b) => b.sequence - a.sequence);
    const latest = ordered[0]?.sequence ?? null;
    if (limit != null && limit <= 0) return { deleted: 0, latestSequence: latest };
    const retained = new Set(ordered.slice(0, keep).map((row) => row.id));
    const victims = this.checkpoints
      .filter((row) => !retained.has(row.id) && row.sequence !== latest)
      .sort((a, b) => a.sequence - b.sequence);
    const capped = limit == null ? victims : victims.slice(0, Math.floor(limit));
    const remove = new Set(capped.map((row) => row.id));
    if (remove.size > 0) {
      const kept = this.checkpoints.filter((row) => !remove.has(row.id));
      this.checkpoints.splice(0, this.checkpoints.length, ...kept);
    }
    return { deleted: remove.size, latestSequence: latest };
  }
  async listCheckpoints(): Promise<EngineCheckpoint[]> {
    return structuredClone(this.checkpoints);
  }

  private filterKeyset(query: Omit<JournalKeysetQuery, "limit"> & { limit?: number }): JournalEvent[] {
    return filterKeysetRows(this.events, this.commitSeqById, query);
  }

  async commitDurableTransition(input: DurableTransitionInput, expectedPreviousSequence: number): Promise<DurableCommitResult> {
    assertJournalCapability(this.access, "runtime-write", "commitDurableTransition");
    return this.withDurableLock(() => this.commitDurableTransitionUnlocked(input, expectedPreviousSequence));
  }
  async readDurableTransitionsAfter(sequence: number, limit: number): Promise<DurableTransition[]> {
    const cap = Math.max(1, Math.min(limit, 10_000));
    return structuredClone(this.durableRows.filter((row) => row.sequence > sequence).slice(0, cap));
  }
  async readDurableByOperationKey(operationKey: string): Promise<DurableTransition | null> {
    const existing = this.durableByKey.get(operationKey);
    if (existing) return structuredClone(existing);
    const compacted = this.compactedByKey.get(operationKey);
    if (!compacted || (compacted.lifecycleState !== "open" && Date.parse(compacted.expiresAt) <= Date.now())) {
      if (compacted) this.compactedByKey.delete(operationKey);
      return null;
    }
    return { operationKey: compacted.operationKey, type: compacted.type, aggregateType: compacted.aggregateType, aggregateId: compacted.aggregateId, payloadHash: compacted.payloadHash, sequence: compacted.sequence, occurredAt: compacted.occurredAt, recordedAt: compacted.recordedAt, payload: {} };
  }
  async queryDurableTransitions(query: { types?: readonly string[]; afterSequence?: number; limit: number }): Promise<DurableTransition[]> {
    const after = query.afterSequence ?? 0;
    const cap = Math.max(1, Math.min(query.limit, 10_000));
    return structuredClone(this.durableRows
      .filter((row) => row.sequence > after && (!query.types || query.types.includes(row.type)))
      .slice(0, cap));
  }
  async readJournalEventById(id: string): Promise<JournalEvent | null> {
    const event = this.events.find((row) => row.id === id);
    return event ? structuredClone(event) : null;
  }
  async latestDurableSequence(): Promise<number> {
    return this.durableHighWater;
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    assertJournalCapability(this.access, "runtime-write", "saveCheckpoint");
    return this.withDurableLock(() => {
      const cursor = typeof checkpoint.payload.durableCursor === "number" ? checkpoint.payload.durableCursor : null;
      if (cursor != null) {
        const latest = this.durableHighWater;
        if (cursor !== latest) throw new CheckpointFenceError(cursor, latest);
      }
      const payload = structuredClone(checkpoint.payload);
      const contentHash = hashCheckpointPayload(payload);
      payload.checkpointContentHash = contentHash;
      const stored = { ...structuredClone(checkpoint), payload, contentHash, verified: true, id: createId("checkpoint"), createdAt: new Date().toISOString() };
      this.checkpoints.push(stored);
      return structuredClone(stored);
    });
  }
  async loadLatestCheckpoint(): Promise<EngineCheckpoint | null> {
    const latest = this.checkpoints.toSorted((a, b) => b.sequence - a.sequence)[0];
    return latest ? structuredClone(latest) : null;
  }
  async listVerifiedCheckpoints(): Promise<EngineCheckpoint[]> {
    const rows = this.checkpoints.filter((row) => row.verified && row.contentHash && hashCheckpointPayload(row.payload) === row.contentHash && isSemanticallyValidCheckpointPayload(row.payload)
      && (typeof row.payload.durableCursor !== "number" || row.payload.durableCursor <= this.durableHighWater));
    return structuredClone(rows.sort((a, b) => b.sequence - a.sequence));
  }
  async compactDurableTransitionsThrough(cursor: number, options: { maxReplayKeys?: number; ttlMs?: number; maxRows?: number; maxBatches?: number; batchSize?: number; budgetMs?: number } = {}): Promise<DurableCompactionResult> {
    assertJournalCapability(this.access, "maintenance", "compactDurableTransitionsThrough");
    const target = Math.max(0, Math.floor(cursor));
    if (target <= 0) return { deleted: 0, cursor: target, highWater: this.durableHighWater, compactedOperationKeys: 0 };
    if (target > this.durableHighWater) throw new CheckpointFenceError(target, this.durableHighWater);
    const ttl = Math.max(60_000, options.ttlMs ?? 7 * 24 * 60 * 60_000);
    const maxKeys = Math.max(0, Math.floor(options.maxReplayKeys ?? 4_096));
    if ((options.maxBatches ?? 1) <= 0 || (options.budgetMs ?? 1) <= 0) return { deleted: 0, cursor: target, highWater: this.durableHighWater, compactedOperationKeys: this.compactedByKey.size };
    const rowCap = Math.max(0, Math.min(options.maxRows ?? Number.MAX_SAFE_INTEGER, options.batchSize ?? Number.MAX_SAFE_INTEGER));
    if (rowCap <= 0) return { deleted: 0, cursor: target, highWater: this.durableHighWater, compactedOperationKeys: this.compactedByKey.size };
    const victims = this.durableRows.filter((row) => row.sequence <= target).slice(0, rowCap);
    const now = Date.now();
    for (const row of victims) {
      if (maxKeys > 0) this.compactedByKey.set(row.operationKey, { operationKey: row.operationKey, type: row.type, aggregateType: row.aggregateType, aggregateId: row.aggregateId, scopeId: replayScopeId(row), payloadHash: row.payloadHash, sequence: row.sequence, occurredAt: row.occurredAt, recordedAt: row.recordedAt, expiresAt: new Date(now + ttl).toISOString(), lifecycleState: replayLifecycleState(row) });
      this.durableByKey.delete(row.operationKey);
    }
    const closedOrders = new Set(victims.filter((row) => row.type === "PAPER_MAKER_RELEASED" || row.type === "PAPER_ORDER_CANCELLED").map((row) => row.aggregateId));
    for (const orderId of closedOrders) {
      const reserve = this.compactedByKey.get(`maker:${orderId}:reserve`);
      if (reserve) reserve.lifecycleState = "closed";
    }
    const closedExperiments = new Set(victims.filter((row) => row.type === "EXPERIMENT_COMPLETED" || row.type === "EXPERIMENT_START_ABORTED").map((row) => row.aggregateId));
    for (const experimentId of closedExperiments) {
      for (const row of this.compactedByKey.values()) {
        if (row.type === "EXPERIMENT_STARTED" && row.aggregateId === experimentId) row.lifecycleState = "closed";
        if (row.type === "EXPERIMENT_PREPARED" && row.scopeId === experimentId) row.lifecycleState = "closed";
      }
    }
    const startedExperiments = new Set(victims.filter((row) => row.type === "EXPERIMENT_STARTED").map((row) => row.aggregateId));
    for (const experimentId of startedExperiments) {
      for (const row of this.compactedByKey.values()) {
        if (row.type === "EXPERIMENT_PREPARED" && row.scopeId === experimentId) row.lifecycleState = "closed";
      }
    }
    const repairedExperiments = new Set(victims.filter((row) => row.type === "REPORT_SOURCE_INTEGRITY_REPAIRED")
      .map((row) => typeof row.payload.experimentId === "string" ? row.payload.experimentId : row.aggregateId));
    for (const experimentId of repairedExperiments) {
      for (const row of this.compactedByKey.values()) {
        if (row.type === "REPORT_SOURCE_INTEGRITY_FAULT" && row.aggregateId === experimentId) row.lifecycleState = "closed";
      }
    }
    const settledScopes = new Set(victims.filter((row) => row.type === "POSITION_SETTLED")
      .map((row) => replayScopeId(row)).filter((scope): scope is string => Boolean(scope)));
    for (const row of this.compactedByKey.values()) {
      if (row.type === "PAPER_FILL" && row.lifecycleState === "open" && row.scopeId && settledScopes.has(row.scopeId)) row.lifecycleState = "closed";
    }
    while (this.compactedByKey.size > maxKeys) {
      const oldest = [...this.compactedByKey.values()].filter((row) => row.lifecycleState !== "open").sort((a, b) => a.sequence - b.sequence)[0];
      if (!oldest) break;
      this.compactedByKey.delete(oldest.operationKey);
    }
    for (const [key, row] of [...this.compactedByKey.entries()]) {
      if (row.lifecycleState !== "open" && Date.parse(row.expiresAt) <= now) this.compactedByKey.delete(key);
    }
    if (victims.length > 0) {
      this.durableRows.splice(0, victims.length);
      this.recordMutation(`compactDurableTransitionsThrough:${victims.length}`);
    }
    return { deleted: victims.length, cursor: target, highWater: this.durableHighWater, compactedOperationKeys: this.compactedByKey.size };
  }
  async persistAnalyticsRollup(rollup: AnalyticsRollup): Promise<AnalyticsRollup> {
    assertJournalCapability(this.access, "report-write", "persistAnalyticsRollup");
    const stored = structuredClone(rollup);
    // Memory journal keeps one versioned row per experiment/kind.
    const map = this.rollups;
    const key = `${stored.experimentId}:${stored.rollupKind}`;
    const existing = map.get(key);
    if (existing) {
      const oldCoverage = existing.sourceEventCount ?? 0;
      const newCoverage = stored.sourceEventCount ?? 0;
      const oldSeq = existing.sourceCommitSeq ?? 0;
      const newSeq = stored.sourceCommitSeq ?? 0;
      if (newCoverage < oldCoverage || newSeq < oldSeq) return structuredClone(existing);
      if (newCoverage === oldCoverage && newSeq === oldSeq) {
        if (existing.contentHash !== stored.contentHash) throw new Error("ANALYTICS_ROLLUP_CONFLICT");
        return structuredClone(existing);
      }
      if ((stored.previousContentHash ?? null) !== existing.contentHash) throw new Error("ANALYTICS_ROLLUP_PREFIX_MISMATCH");
    }
    map.set(key, stored);
    return structuredClone(stored);
  }
  async loadAnalyticsRollup(experimentId: string, rollupKind = "canonical"): Promise<AnalyticsRollup | null> {
    const row = this.rollups.get(`${experimentId}:${rollupKind}`);
    return row ? structuredClone(row) : null;
  }
  async deleteCanonicalEventsThrough(input: { experimentId: string; occurredBefore?: Date; limit: number; sourceCursor: number; sourceCommitSeq?: number; sourceRecordedAt?: Date }): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "deleteCanonicalEventsThrough");
    const cap = Math.max(0, Math.floor(input.limit));
    const before = input.occurredBefore?.getTime() ?? Number.POSITIVE_INFINITY;
    const victims = this.events.filter((event) => CANONICAL_REPORT_INPUT_EVENT_TYPES.includes(event.type as (typeof CANONICAL_REPORT_INPUT_EVENT_TYPES)[number]))
      .filter((event) => (event.payload as { experimentId?: unknown }).experimentId === input.experimentId && Date.parse(event.occurredAt) < before)
      .filter((event) => input.sourceCommitSeq == null || (this.commitSeqById.get(event.id) ?? Number.POSITIVE_INFINITY) <= input.sourceCommitSeq)
      .filter((event) => input.sourceRecordedAt == null || Date.parse(event.recordedAt) <= input.sourceRecordedAt.getTime())
      .slice(0, cap);
    const ids = new Set(victims.map((event) => event.id));
    for (let i = this.events.length - 1; i >= 0; i -= 1) if (ids.has(this.events[i]!.id)) {
      this.eventIds.delete(this.events[i]!.id);
      this.commitSeqById.delete(this.events[i]!.id);
      this.events.splice(i, 1);
    }
    this.recordMutation(`deleteCanonicalEventsThrough:${victims.length}`);
    return { deleted: victims.length };
  }
  async close(): Promise<void> {}

  private async withPersistLock<T>(work: () => T | Promise<T>): Promise<T> {
    const previous = this.persistChain;
    let release: () => void = () => undefined;
    this.persistChain = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try { return await work(); } finally { release(); }
  }

  private async withDurableLock<T>(work: () => T | Promise<T>): Promise<T> {
    const previous = this.durableChain;
    let release: () => void = () => undefined;
    this.durableChain = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
      return await work();
    } finally {
      release();
    }
  }

  private commitDurableTransitionUnlocked(input: DurableTransitionInput, expectedPreviousSequence: number): DurableCommitResult {
    const parsed = durableTransitionInputSchema.parse(input);
    const payloadHash = hashDurablePayload(parsed.payload);
    const existing = this.durableByKey.get(parsed.operationKey);
    const compacted = existing ? undefined : this.compactedByKey.get(parsed.operationKey);
    const replay = existing ?? (compacted ? { ...compacted, payload: {} } as DurableTransition : undefined);
    if (replay) {
      if (
        replay.type !== parsed.type
        || replay.aggregateType !== parsed.aggregateType
        || replay.aggregateId !== parsed.aggregateId
        || replay.payloadHash !== payloadHash
      ) {
        throw new IdempotencyConflictError(parsed.operationKey, replay.type, parsed.type);
      }
      return {
        sequence: replay.sequence,
        operationKey: parsed.operationKey,
        inserted: false,
        existing: true,
        transition: structuredClone(replay),
      };
    }
    const latest = this.durableHighWater;
    if (latest !== expectedPreviousSequence) {
      throw new DurableCursorBehindError(expectedPreviousSequence, latest);
    }
    if (this.failNextDurableInsert) {
      this.failNextDurableInsert = false;
      throw new Error("DURABLE_INSERT_FAILED");
    }
    const now = new Date();
    const occurredAt = parsed.occurredAt ?? now;
    const row: DurableTransition = {
      sequence: latest + 1,
      operationKey: parsed.operationKey,
      type: parsed.type,
      aggregateType: parsed.aggregateType,
      aggregateId: parsed.aggregateId,
      payloadHash,
      payload: structuredClone(parsed.payload),
      occurredAt: occurredAt.toISOString(),
      recordedAt: now.toISOString(),
    };
    this.durableRows.push(row);
    this.durableByKey.set(parsed.operationKey, row);
    this.durableHighWater = row.sequence;
    return {
      sequence: row.sequence,
      operationKey: parsed.operationKey,
      inserted: true,
      existing: false,
      transition: structuredClone(row),
    };
  }
}

function filterKeysetRows(
  events: readonly JournalEvent[],
  commitSeqById: ReadonlyMap<string, number>,
  query: Omit<JournalKeysetQuery, "limit"> & { limit?: number },
): JournalEvent[] {
  let rows = [...events];
  if (query.types && query.types.length > 0) rows = rows.filter((event) => query.types!.includes(event.type));
  if (query.experimentId) {
    rows = rows.filter((event) => event.aggregateId === query.experimentId
      || (event.payload as { experimentId?: unknown }).experimentId === query.experimentId);
  }
  if (query.from) rows = rows.filter((event) => new Date(event.occurredAt).getTime() >= query.from!.getTime());
  if (query.to) rows = rows.filter((event) => new Date(event.occurredAt).getTime() <= query.to!.getTime());
  if (query.recordedAtTo) rows = rows.filter((event) => new Date(event.recordedAt).getTime() <= query.recordedAtTo!.getTime());
  if (query.commitSeqTo != null) {
    rows = rows.filter((event) => (commitSeqById.get(event.id) ?? Number.POSITIVE_INFINITY) <= query.commitSeqTo!);
  }
  if (query.commitSeqAfter != null) {
    rows = rows.filter((event) => (commitSeqById.get(event.id) ?? Number.NEGATIVE_INFINITY) > query.commitSeqAfter!);
  }
  if (query.after) rows = rows.filter((event) => compareJournalEventOrder(event, query.after!) > 0);
  if (query.through) rows = rows.filter((event) => compareJournalEventOrder(event, query.through!) <= 0);
  return rows;
}

export class PostgresEventJournal implements EventJournal {
  private readonly client: ReturnType<typeof postgres>;
  private readonly db: ReturnType<typeof drizzle>;
  readonly access: JournalAccessMode;
  migrateInvocations = 0;

  constructor(databaseUrl: string, private readonly migrationsFolder: string, options: { access?: JournalAccessMode } = {}) {
    this.access = options.access ?? "boot-migrate";
    this.client = postgres(databaseUrl, {
      max: 10,
      idle_timeout: 20,
      connect_timeout: 10,
      onclose: () => undefined,
      onnotice: () => undefined,
    });
    this.db = drizzle(this.client);
  }

  async initialize(): Promise<void> {
    if (!journalMayMigrate(this.access)) throw new JournalMigrateForbiddenError(this.access);
    this.migrateInvocations += 1;
    await migrate(this.db, { migrationsFolder: this.migrationsFolder });
    await this.ensureKeysetIndexesIfSafe();
    await this.verifySchema();
  }
  async connectWithoutMigrate(mode: JournalAccessMode = this.access): Promise<void> {
    if (journalMayMigrate(mode)) throw new JournalMigrateForbiddenError(mode);
    if (journalMustVerifySchema(mode)) await this.verifySchema();
  }
  async verifySchema(): Promise<void> {
    await verifyRequiredSchema(async <T extends Record<string, unknown>>(sql: string) => {
      const rows = await this.client.unsafe(sql);
      return rows as unknown as T[];
    });
  }
  async healthCheck(): Promise<boolean> {
    try { await this.client`select 1`; return true; } catch { return false; }
  }
  async append(event: JournalEvent, options: JournalAppendOptions = {}): Promise<void> {
    assertJournalCapability(this.access, "runtime-write", "append");
    const value = journalEventSchema.parse(event);
    if (options.signal?.aborted) throw new JournalAppendAbortedError("aborted");
    if (options.deadlineAt != null && Date.now() >= options.deadlineAt) throw new JournalAppendAbortedError("deadline");
    let callbackFailed = false;
    let transactionId: string | undefined;
    try {
      await this.db.transaction(async (tx) => {
        try {
          // Recompute inside the acquired transaction immediately before mutation.
          // Pool acquisition can consume the outer deadline; a pre-txn remaining is stale.
          if (options.deadlineAt != null) {
            const remaining = options.deadlineAt - Date.now();
            if (remaining <= 0) throw new JournalAppendAbortedError("deadline");
            await tx.execute(sql`SELECT set_config('statement_timeout', ${String(remaining)}, true)`);
            // PG17 bounds the open transaction, including idle time after INSERT.
            // It disables this timer before irreversible COMMIT; callers must still
            // await the transaction outcome rather than infer rollback from a timer.
            const transactionRemaining = options.deadlineAt - Date.now();
            if (transactionRemaining <= 0) throw new JournalAppendAbortedError("deadline");
            const rows = await tx.execute(sql`SELECT set_config('transaction_timeout', ${String(transactionRemaining)}, true), pg_current_xact_id()::text AS xid`);
            transactionId = rows[0]?.xid as string | undefined;
          }
          if (options.signal?.aborted) throw new JournalAppendAbortedError("aborted");
          if (options.deadlineAt != null && Date.now() >= options.deadlineAt) throw new JournalAppendAbortedError("deadline");
          await tx.insert(journalEvents).values({
            ...value, occurredAt: new Date(value.occurredAt), recordedAt: new Date(value.recordedAt),
          }).onConflictDoNothing({ target: journalEvents.id });
        } catch (error) {
          callbackFailed = true;
          throw error;
        }
      });
    } catch (error) {
      // postgres.js can expose an idle transaction_timeout as CONNECTION_CLOSED.
      // Resolve that ambiguity using this exact transaction's PostgreSQL status,
      // not an absent-row read (an in-progress transaction also hides its row).
      let status: unknown;
      if (!callbackFailed && transactionId) {
        try {
          const rows = await this.db.execute(sql`SELECT pg_xact_status(${transactionId}::xid8) AS status`);
          status = rows[0]?.status;
        } catch { /* Unknown outcome remains an ordinary persist failure. */ }
        if (status === "committed") return;
      }
      // A failed callback never sends COMMIT (postgres.js begin). Otherwise only
      // an explicit aborted transaction status can prove no late commit is possible.
      if (options.deadlineAt != null && Date.now() >= options.deadlineAt && (callbackFailed || status === "aborted")) {
        throw new JournalAppendAbortedError("deadline");
      }
      throw error;
    }
  }
  async appendMany(events: readonly JournalEvent[]): Promise<void> {
    assertJournalCapability(this.access, "runtime-write", "appendMany");
    if (events.length === 0) return;
    const values = events.map((raw) => journalEventSchema.parse(raw));
    await this.db.insert(journalEvents).values(values.map((value) => ({
      ...value, occurredAt: new Date(value.occurredAt), recordedAt: new Date(value.recordedAt),
    }))).onConflictDoNothing({ target: journalEvents.id });
  }
  async readTrace(traceId: string): Promise<JournalEvent[]> {
    const rows = await this.db.select().from(journalEvents)
      .where(eq(journalEvents.traceId, traceId))
      .orderBy(asc(journalEvents.occurredAt), asc(journalEvents.id))
      .limit(10_000);
    return rows.map(toJournalEvent);
  }
  async readTracePage(traceId: string, input: { after?: JournalKeysetCursor; limit: number }): Promise<{ events: JournalEvent[]; nextCursor: JournalKeysetCursor | null }> {
    const limit = Math.max(1, input.limit);
    const filters = [eq(journalEvents.traceId, traceId)];
    if (input.after) {
      filters.push(sql`(${journalEvents.occurredAt}, ${journalEvents.id}) > (${input.after.occurredAt}::timestamptz, ${input.after.id})`);
    }
    const rows = await this.db.select().from(journalEvents)
      .where(and(...filters))
      .orderBy(asc(journalEvents.occurredAt), asc(journalEvents.id))
      .limit(limit);
    const events = rows.map(toJournalEvent);
    const last = events[events.length - 1];
    return {
      events,
      nextCursor: last && events.length === limit ? { occurredAt: last.occurredAt, id: last.id } : null,
    };
  }
  async readAll(): Promise<JournalEvent[]> {
    const rows = await this.db.select().from(journalEvents).limit(10_000);
    return rows.map(toJournalEvent);
  }
  async queryEvents(query: JournalQuery): Promise<JournalEvent[]> {
    const filters = [];
    if (query.types && query.types.length > 0) filters.push(inArray(journalEvents.type, [...query.types]));
    if (query.experimentId) {
      filters.push(sql`(${journalEvents.aggregateId} = ${query.experimentId} OR ${journalEvents.payload}->>'experimentId' = ${query.experimentId})`);
    }
    if (query.from) filters.push(gte(journalEvents.occurredAt, query.from));
    if (query.to) filters.push(lte(journalEvents.occurredAt, query.to));
    const orderDesc = query.order !== "asc";
    const rows = await this.db.select().from(journalEvents)
      .where(filters.length > 0 ? and(...filters) : undefined)
      .orderBy(
        orderDesc ? desc(journalEvents.occurredAt) : asc(journalEvents.occurredAt),
        orderDesc ? desc(journalEvents.id) : asc(journalEvents.id),
      )
      .limit(Math.min(query.limit ?? 5_000, 20_000))
      .offset(query.offset ?? 0);
    const events = rows.map(toJournalEvent);
    return orderDesc ? events.reverse() : events;
  }
  async countByType(types?: readonly string[]): Promise<Record<string, number>> {
    const filters = types && types.length > 0 ? [inArray(journalEvents.type, [...types])] : [];
    const rows = await this.db.select({
      type: journalEvents.type,
      count: sql<number>`count(*)::int`,
    }).from(journalEvents).where(filters.length > 0 ? and(...filters) : undefined).groupBy(journalEvents.type);
    return Object.fromEntries(rows.map((row) => [row.type, Number(row.count)]));
  }
  /** Read-only physical footprint for storage soak diagnostics. */
  async storageFootprintForTest(): Promise<Record<string, { rows: number; bytes: number }>> {
    const names = ["journal_events", "durable_transitions", "durable_replay_keys", "engine_checkpoints", "mcp_idempotency_keys", "experiment_report_rollups", "experiment_reports"] as const;
    const result: Record<string, { rows: number; bytes: number }> = {};
    for (const name of names) {
      const rows = await this.client.unsafe(`SELECT count(*)::bigint AS rows, pg_total_relation_size('public.${name}')::bigint AS bytes`);
      result[name === "mcp_idempotency_keys" ? "mcp_idempotency" : name] = { rows: Number(rows[0]?.rows ?? 0), bytes: Number(rows[0]?.bytes ?? 0) };
    }
    return result;
  }
  async queryEventsKeyset(query: JournalKeysetQuery): Promise<JournalEvent[]> {
    const filters = journalKeysetFilters(query);
    const rows = await this.db.select().from(journalEvents)
      .where(filters.length > 0 ? and(...filters) : undefined)
      .orderBy(asc(journalEvents.occurredAt), asc(journalEvents.id))
      .limit(Math.max(1, Math.min(query.limit, 20_000)));
    return rows.map(toJournalEvent);
  }
  async peekLatestKeyset(query: Omit<JournalKeysetQuery, "limit" | "after" | "through">): Promise<JournalKeysetCursor | null> {
    const filters = journalKeysetFilters(query);
    const rows = await this.db.select({
      occurredAt: journalEvents.occurredAt,
      id: journalEvents.id,
    }).from(journalEvents)
      .where(filters.length > 0 ? and(...filters) : undefined)
      .orderBy(desc(journalEvents.occurredAt), desc(journalEvents.id))
      .limit(1);
    const row = rows[0];
    return row ? { occurredAt: row.occurredAt.toISOString(), id: row.id } : null;
  }
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    assertJournalCapability(this.access, "runtime-write", "claimIdempotency");
    const rows = await this.db.insert(mcpIdempotencyKeys).values({ scope, key })
      .onConflictDoNothing({ target: [mcpIdempotencyKeys.scope, mcpIdempotencyKeys.key] }).returning({ key: mcpIdempotencyKeys.key });
    return rows.length === 1;
  }
  async pruneIdempotencyKeys(olderThan: Date, limit: number): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "pruneIdempotencyKeys");
    const requested = Math.max(0, Math.floor(limit));
    if (requested <= 0) return { deleted: 0 };
    const cap = Math.min(requested, 500);
    const result = await this.client`
      DELETE FROM mcp_idempotency_keys
      WHERE (scope, key) IN (
        SELECT scope, key FROM mcp_idempotency_keys
        WHERE created_at < ${olderThan.toISOString()}
        ORDER BY created_at ASC, scope ASC, key ASC
        LIMIT ${cap}
      )
    `;
    return { deleted: result.count ?? 0 };
  }
  async idempotencyCreatedAt(scope: string, key: string): Promise<string | null> {
    const rows = await this.db.select({ createdAt: mcpIdempotencyKeys.createdAt })
      .from(mcpIdempotencyKeys)
      .where(and(eq(mcpIdempotencyKeys.scope, scope), eq(mcpIdempotencyKeys.key, key)))
      .limit(1);
    return rows[0]?.createdAt.toISOString() ?? null;
  }
  async persistExperimentReport(input: DurableExperimentReportInput): Promise<PersistReportResult> {
    assertJournalCapability(this.access, "report-write", "persistExperimentReport");
    const hash = input.contentHash || hashExperimentReport(input);
    const values = {
      experimentId: input.experimentId,
      reportKind: input.reportKind,
      schemaVersion: input.schemaVersion,
      generatedAt: new Date(input.generatedAt),
      complete: input.complete,
      truncated: input.truncated,
      sourceEventCount: input.sourceEventCount,
      sourceWindowStart: input.sourceWindowStart ? new Date(input.sourceWindowStart) : null,
      sourceWindowEnd: input.sourceWindowEnd ? new Date(input.sourceWindowEnd) : null,
      sourceFenceOccurredAt: input.sourceFenceOccurredAt ? new Date(input.sourceFenceOccurredAt) : null,
      sourceFenceId: input.sourceFenceId,
      earliestIncludedAt: input.earliestIncludedAt ? new Date(input.earliestIncludedAt) : null,
      latestIncludedAt: input.latestIncludedAt ? new Date(input.latestIncludedAt) : null,
      canonicalSummary: input.canonicalSummary,
      renderedMarkdown: input.renderedMarkdown,
      contentHash: hash,
    };
    const persistOnce = () => persistReportConflictSafe({ ...input, contentHash: hash }, {
      insert: async (stored) => {
        const inserted = await this.db.insert(experimentReports).values({
          ...values,
          persistedAt: new Date(stored.persistedAt),
        }).onConflictDoNothing({
          target: [experimentReports.experimentId, experimentReports.reportKind],
        }).returning();
        const row = inserted[0];
        return row ? toExperimentReport(row) : "conflict";
      },
      load: async () => {
        const rows = await this.db.select().from(experimentReports)
          .where(and(eq(experimentReports.experimentId, input.experimentId), eq(experimentReports.reportKind, input.reportKind)))
          .limit(1);
        return rows[0] ? toExperimentReport(rows[0]) : null;
      },
    });
    try {
      return await persistOnce();
    } catch (error) {
      if (!isTransientJournalError(error)) throw error;
      return persistOnce();
    }
  }
  async loadExperimentReport(experimentId: string, reportKind: "final" = "final"): Promise<DurableExperimentReport | null> {
    const rows = await this.db.select().from(experimentReports)
      .where(and(eq(experimentReports.experimentId, experimentId), eq(experimentReports.reportKind, reportKind)))
      .limit(1);
    return rows[0] ? toExperimentReport(rows[0]) : null;
  }
  async deleteJournalEventsBounded(input: { types: readonly string[]; occurredBefore: Date; limit: number }): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "deleteJournalEventsBounded");
    if (input.types.length === 0) return { deleted: 0 };
    if (input.types.some((type) => isProtectedJournalType(type))) {
      throw new Error("RETENTION_REFUSED_PROTECTED_TYPES");
    }
    const requested = Math.max(0, Math.floor(input.limit));
    if (requested <= 0) return { deleted: 0 };
    const cap = Math.min(requested, 500);
    const result = await this.client`
      DELETE FROM journal_events
      WHERE (occurred_at, id) IN (
        SELECT occurred_at, id FROM journal_events
        WHERE type = ANY(${input.types as string[]})
          AND occurred_at < ${input.occurredBefore.toISOString()}
        ORDER BY occurred_at ASC, id ASC
        LIMIT ${cap}
      )
    `;
    return { deleted: result.count ?? 0 };
  }
  async pruneOldCheckpoints(keepLatest: number, limit?: number): Promise<{ deleted: number; latestSequence: number | null }> {
    assertJournalCapability(this.access, "maintenance", "pruneOldCheckpoints");
    const keep = Math.max(1, keepLatest);
    const latestRows = await this.db.select({
      max: sql<number>`coalesce(max(${engineCheckpoints.sequence}), 0)`,
    }).from(engineCheckpoints);
    const latest = Number(latestRows[0]?.max ?? 0);
    if (latest === 0) return { deleted: 0, latestSequence: null };
    const cap = limit == null ? Number.MAX_SAFE_INTEGER : Math.max(0, Math.floor(limit));
    if (cap <= 0) return { deleted: 0, latestSequence: latest };
    const result = await this.client`
      DELETE FROM engine_checkpoints
      WHERE id IN (
        SELECT id FROM engine_checkpoints
        WHERE sequence < ${latest}
          AND sequence NOT IN (
            SELECT sequence FROM engine_checkpoints
            ORDER BY sequence DESC
            LIMIT ${keep}
          )
        ORDER BY sequence ASC
        LIMIT ${cap}
      )
    `;
    return { deleted: result.count ?? 0, latestSequence: latest };
  }
  async listCheckpoints(): Promise<EngineCheckpoint[]> {
    const rows = await this.db.select().from(engineCheckpoints).orderBy(desc(engineCheckpoints.sequence)).limit(64);
    return rows.map((row) => {
      const payload = row.payload as Record<string, unknown>;
      const contentHash = typeof payload.checkpointContentHash === "string" ? payload.checkpointContentHash : undefined;
      return {
        id: row.id, experimentId: row.experimentId, sequence: row.sequence, payload,
        createdAt: row.createdAt.toISOString(),
        ...(contentHash ? { contentHash } : {}),
        verified: Boolean(contentHash && hashCheckpointPayload(payload) === contentHash),
      };
    });
  }
  async listVerifiedCheckpoints(): Promise<EngineCheckpoint[]> {
    const rows = await this.listCheckpoints();
    const highWater = await this.latestDurableSequence();
    return rows.filter((row) => row.verified && row.contentHash && isSemanticallyValidCheckpointPayload(row.payload)
      && (typeof row.payload.durableCursor !== "number" || row.payload.durableCursor <= highWater)).sort((a, b) => b.sequence - a.sequence);
  }
  async commitDurableTransition(input: DurableTransitionInput, expectedPreviousSequence: number): Promise<DurableCommitResult> {
    assertJournalCapability(this.access, "runtime-write", "commitDurableTransition");
    const parsed = durableTransitionInputSchema.parse(input);
    const payloadHash = hashDurablePayload(parsed.payload);
    const occurredAt = parsed.occurredAt ?? new Date();
    return this.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`);
      const existingRows = await tx.select().from(durableTransitions)
        .where(eq(durableTransitions.operationKey, parsed.operationKey)).limit(1);
      const existing = existingRows[0];
      const compactedRows = existing ? [] : await tx.select().from(durableReplayKeys)
        .where(eq(durableReplayKeys.operationKey, parsed.operationKey)).limit(1);
      const compactedExisting = compactedRows[0];
      if (!existing && compactedExisting && (compactedExisting.lifecycleState === "open" || compactedExisting.expiresAt.getTime() > Date.now())) {
        if (compactedExisting.type !== parsed.type || compactedExisting.aggregateType !== parsed.aggregateType || compactedExisting.aggregateId !== parsed.aggregateId || compactedExisting.payloadHash !== payloadHash) {
          throw new IdempotencyConflictError(parsed.operationKey, compactedExisting.type, parsed.type);
        }
        return { sequence: Number(compactedExisting.sequence), operationKey: parsed.operationKey, inserted: false, existing: true, transition: { sequence: Number(compactedExisting.sequence), operationKey: compactedExisting.operationKey, type: compactedExisting.type, aggregateType: compactedExisting.aggregateType, aggregateId: compactedExisting.aggregateId, payloadHash: compactedExisting.payloadHash, payload: {}, occurredAt: compactedExisting.occurredAt.toISOString(), recordedAt: compactedExisting.recordedAt.toISOString() } };
      }
      if (existing) {
        if (
          existing.type !== parsed.type
          || existing.aggregateType !== parsed.aggregateType
          || existing.aggregateId !== parsed.aggregateId
          || existing.payloadHash !== payloadHash
        ) {
          throw new IdempotencyConflictError(parsed.operationKey, existing.type, parsed.type);
        }
        return {
          sequence: Number(existing.sequence),
          operationKey: parsed.operationKey,
          inserted: false,
          existing: true,
          transition: toDurableTransition(existing),
        };
      }
      await tx.insert(durableSequenceMetadata).values({ id: 1, highWater: 0 }).onConflictDoNothing();
      const latestRows = await tx.select({ highWater: durableSequenceMetadata.highWater }).from(durableSequenceMetadata).where(eq(durableSequenceMetadata.id, 1)).limit(1);
      const latest = Number(latestRows[0]?.highWater ?? 0);
      if (latest !== expectedPreviousSequence) {
        throw new DurableCursorBehindError(expectedPreviousSequence, latest);
      }
      const inserted = await tx.insert(durableTransitions).values({
        sequence: latest + 1,
        operationKey: parsed.operationKey,
        type: parsed.type,
        aggregateType: parsed.aggregateType,
        aggregateId: parsed.aggregateId,
        payloadHash,
        payload: parsed.payload,
        occurredAt,
      }).returning();
      const row = inserted[0];
      if (!row) throw new Error("DURABLE_TRANSITION_INSERT_FAILED");
      const transition = toDurableTransition(row);
      await tx.update(durableSequenceMetadata).set({ highWater: latest + 1, updatedAt: new Date() }).where(eq(durableSequenceMetadata.id, 1));
      return {
        sequence: transition.sequence,
        operationKey: parsed.operationKey,
        inserted: true,
        existing: false,
        transition,
      };
    });
  }
  async readDurableTransitionsAfter(sequence: number, limit: number): Promise<DurableTransition[]> {
    const cap = Math.max(1, Math.min(limit, 10_000));
    const rows = await this.db.select().from(durableTransitions)
      .where(gt(durableTransitions.sequence, sequence))
      .orderBy(asc(durableTransitions.sequence))
      .limit(cap);
    return rows.map(toDurableTransition);
  }
  async readDurableByOperationKey(operationKey: string): Promise<DurableTransition | null> {
    const rows = await this.db.select().from(durableTransitions)
      .where(eq(durableTransitions.operationKey, operationKey)).limit(1);
    const row = rows[0];
    if (row) return toDurableTransition(row);
    const replayRows = await this.db.select().from(durableReplayKeys).where(eq(durableReplayKeys.operationKey, operationKey)).limit(1);
    const compacted = replayRows[0];
    if (!compacted || (compacted.lifecycleState !== "open" && compacted.expiresAt.getTime() <= Date.now())) return null;
    return { sequence: Number(compacted.sequence), operationKey: compacted.operationKey, type: compacted.type, aggregateType: compacted.aggregateType, aggregateId: compacted.aggregateId, payloadHash: compacted.payloadHash, payload: {}, occurredAt: compacted.occurredAt.toISOString(), recordedAt: compacted.recordedAt.toISOString() };
  }
  async queryDurableTransitions(query: { types?: readonly string[]; afterSequence?: number; limit: number }): Promise<DurableTransition[]> {
    const after = query.afterSequence ?? 0;
    const cap = Math.max(1, Math.min(query.limit, 10_000));
    const filters = [gt(durableTransitions.sequence, after)];
    if (query.types && query.types.length > 0) filters.push(inArray(durableTransitions.type, [...query.types]));
    const rows = await this.db.select().from(durableTransitions)
      .where(and(...filters))
      .orderBy(asc(durableTransitions.sequence))
      .limit(cap);
    return rows.map(toDurableTransition);
  }
  async readJournalEventById(id: string): Promise<JournalEvent | null> {
    const rows = await this.db.select().from(journalEvents).where(eq(journalEvents.id, id)).limit(1);
    const row = rows[0];
    return row ? toJournalEvent(row) : null;
  }
  async latestDurableSequence(): Promise<number> {
    const rows = await this.db.select({ highWater: durableSequenceMetadata.highWater }).from(durableSequenceMetadata).where(eq(durableSequenceMetadata.id, 1)).limit(1);
    if (rows[0]) return Number(rows[0].highWater);
    const fallback = await this.db.select({ max: sql<number>`coalesce(max(${durableTransitions.sequence}), 0)` }).from(durableTransitions);
    return Number(fallback[0]?.max ?? 0);
  }
  async compactDurableTransitionsThrough(cursor: number, options: { maxReplayKeys?: number; ttlMs?: number; maxRows?: number; maxBatches?: number; batchSize?: number; budgetMs?: number } = {}): Promise<DurableCompactionResult> {
    assertJournalCapability(this.access, "maintenance", "compactDurableTransitionsThrough");
    const target = Math.max(0, Math.floor(cursor));
    const ttl = Math.max(60_000, options.ttlMs ?? 7 * 24 * 60 * 60_000);
    const maxKeys = Math.max(0, Math.floor(options.maxReplayKeys ?? 4_096));
    if ((options.maxBatches ?? 1) <= 0 || (options.budgetMs ?? 1) <= 0) return { deleted: 0, cursor: target, highWater: await this.latestDurableSequence(), compactedOperationKeys: 0 };
    const deadline = Date.now() + Math.max(0, options.budgetMs ?? 1);
    return this.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`);
      const remaining = Math.max(1, deadline - Date.now());
      await tx.execute(sql`SELECT set_config('statement_timeout', ${String(remaining)}, true)`);
      await tx.insert(durableSequenceMetadata).values({ id: 1, highWater: 0 }).onConflictDoNothing();
      const current = await tx.select({ highWater: durableSequenceMetadata.highWater }).from(durableSequenceMetadata).where(eq(durableSequenceMetadata.id, 1)).limit(1);
      const highWater = Number(current[0]?.highWater ?? 0);
      if (target > highWater) throw new CheckpointFenceError(target, highWater);
      const rowCap = Math.max(0, Math.min(options.maxRows ?? Number.MAX_SAFE_INTEGER, options.batchSize ?? Number.MAX_SAFE_INTEGER));
      if (rowCap <= 0) return { deleted: 0, cursor: target, highWater, compactedOperationKeys: 0 };
      const rows = await tx.select().from(durableTransitions).where(lte(durableTransitions.sequence, target)).orderBy(asc(durableTransitions.sequence)).limit(Math.min(rowCap, 10_000));
      const expiresAt = new Date(Date.now() + ttl).toISOString();
      if (Date.now() >= deadline) throw new Error("RETENTION_CYCLE_BUDGET_EXCEEDED");
      if (rows.length > 0) {
        // One bounded multi-row statement replaces the old N+1 insert loop.
        await tx.insert(durableReplayKeys).values(rows.map((row) => {
          const transition = toDurableTransition(row);
          return {
            operationKey: row.operationKey, type: row.type, aggregateType: row.aggregateType,
            aggregateId: row.aggregateId, scopeId: replayScopeId(transition), payloadHash: row.payloadHash,
            sequence: row.sequence, occurredAt: row.occurredAt, recordedAt: row.recordedAt,
            expiresAt: new Date(expiresAt), lifecycleState: replayLifecycleState(transition),
          };
        })).onConflictDoUpdate({ target: durableReplayKeys.operationKey, set: { expiresAt: new Date(expiresAt) } });
      }
      const closedReserveKeys = rows.filter((item) => item.type === "PAPER_MAKER_RELEASED" || item.type === "PAPER_ORDER_CANCELLED").map((row) => `maker:${row.aggregateId}:reserve`);
      if (closedReserveKeys.length > 0) {
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(inArray(durableReplayKeys.operationKey, closedReserveKeys));
      }
      const closedExperimentIds = rows.filter((item) => item.type === "EXPERIMENT_COMPLETED" || item.type === "EXPERIMENT_START_ABORTED").map((row) => row.aggregateId);
      if (closedExperimentIds.length > 0) {
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(and(eq(durableReplayKeys.type, "EXPERIMENT_STARTED"), inArray(durableReplayKeys.aggregateId, closedExperimentIds)));
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(and(eq(durableReplayKeys.type, "EXPERIMENT_PREPARED"), inArray(durableReplayKeys.scopeId, closedExperimentIds)));
      }
      const startedExperimentIds = rows.filter((item) => item.type === "EXPERIMENT_STARTED").map((row) => row.aggregateId);
      if (startedExperimentIds.length > 0) {
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(and(eq(durableReplayKeys.type, "EXPERIMENT_PREPARED"), inArray(durableReplayKeys.scopeId, startedExperimentIds)));
      }
      const repairedExperimentIds = rows.filter((item) => item.type === "REPORT_SOURCE_INTEGRITY_REPAIRED").map((row) => {
        const payload = row.payload as Record<string, unknown>;
        return typeof payload.experimentId === "string" ? payload.experimentId : row.aggregateId;
      });
      if (repairedExperimentIds.length > 0) {
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(and(eq(durableReplayKeys.type, "REPORT_SOURCE_INTEGRITY_FAULT"), inArray(durableReplayKeys.aggregateId, repairedExperimentIds)));
      }
      const settledScopes = rows.filter((item) => item.type === "POSITION_SETTLED").map((row) => replayScopeId(toDurableTransition(row))).filter((scope): scope is string => Boolean(scope));
      if (settledScopes.length > 0) {
        await tx.update(durableReplayKeys).set({ lifecycleState: "closed" }).where(and(eq(durableReplayKeys.type, "PAPER_FILL"), eq(durableReplayKeys.lifecycleState, "open"), inArray(durableReplayKeys.scopeId, settledScopes)));
      }
      if (maxKeys > 0) {
        await tx.execute(sql`DELETE FROM durable_replay_keys WHERE operation_key IN (SELECT operation_key FROM durable_replay_keys WHERE lifecycle_state <> 'open' ORDER BY (expires_at <= now()) DESC, sequence ASC LIMIT GREATEST(0, (SELECT count(*) FROM durable_replay_keys WHERE lifecycle_state <> 'open') - ${maxKeys}))`);
      } else {
        await tx.delete(durableReplayKeys).where(sql`expires_at <= now()`);
      }
      if (rows.length > 0) {
        await tx.delete(durableTransitions).where(inArray(durableTransitions.operationKey, rows.map((row) => row.operationKey)));
      }
      const countRows = await tx.select({ count: sql<number>`count(*)::int` }).from(durableReplayKeys);
      return { deleted: rows.length, cursor: target, highWater, compactedOperationKeys: Number(countRows[0]?.count ?? 0) };
    });
  }
  async persistAnalyticsRollup(rollup: AnalyticsRollup): Promise<AnalyticsRollup> {
    assertJournalCapability(this.access, "report-write", "persistAnalyticsRollup");
    const row = await this.db.transaction(async (tx) => {
      const existingRows = await tx.select().from(experimentReportRollups).where(and(eq(experimentReportRollups.experimentId, rollup.experimentId), eq(experimentReportRollups.rollupKind, rollup.rollupKind))).limit(1);
      const existing = existingRows[0];
      const newCount = rollup.sourceEventCount ?? 0;
      const newSeq = rollup.sourceCommitSeq ?? 0;
      if (existing) {
        const oldCount = Number(existing.sourceEventCount ?? 0); const oldSeq = Number(existing.sourceCommitSeq ?? 0);
        if (newCount < oldCount || newSeq < oldSeq) return existing;
        if (newCount === oldCount && newSeq === oldSeq) {
          if (existing.contentHash !== rollup.contentHash) throw new Error("ANALYTICS_ROLLUP_CONFLICT");
          return existing;
        }
        if ((rollup.previousContentHash ?? null) !== existing.contentHash) throw new Error("ANALYTICS_ROLLUP_PREFIX_MISMATCH");
        const updated = await tx.update(experimentReportRollups).set({
          schemaVersion: rollup.schemaVersion, sourceCursor: rollup.sourceCursor, sourceFenceId: rollup.sourceFenceId,
          sourceFenceOccurredAt: rollup.sourceFenceOccurredAt ? new Date(rollup.sourceFenceOccurredAt) : null,
          sourceEventCount: newCount, sourceCommitSeq: newSeq, sourceRecordedAt: rollup.sourceRecordedAt ? new Date(rollup.sourceRecordedAt) : null,
          stableRead: rollup.stableRead ?? false, previousContentHash: rollup.previousContentHash ?? null,
          coverageHash: rollup.coverageHash ?? null,
          accumulator: rollup.accumulator, contentHash: rollup.contentHash, verified: rollup.verified, createdAt: new Date(rollup.createdAt),
        }).where(and(eq(experimentReportRollups.experimentId, rollup.experimentId), eq(experimentReportRollups.rollupKind, rollup.rollupKind), eq(experimentReportRollups.contentHash, existing.contentHash))).returning();
        return updated[0] ?? existing;
      }
      const inserted = await tx.insert(experimentReportRollups).values({
        experimentId: rollup.experimentId, rollupKind: rollup.rollupKind, schemaVersion: rollup.schemaVersion,
        sourceCursor: rollup.sourceCursor, sourceFenceId: rollup.sourceFenceId,
        sourceFenceOccurredAt: rollup.sourceFenceOccurredAt ? new Date(rollup.sourceFenceOccurredAt) : null,
        sourceEventCount: newCount, sourceCommitSeq: newSeq, sourceRecordedAt: rollup.sourceRecordedAt ? new Date(rollup.sourceRecordedAt) : null,
        stableRead: rollup.stableRead ?? false, previousContentHash: rollup.previousContentHash ?? null, coverageHash: rollup.coverageHash ?? null,
        accumulator: rollup.accumulator, contentHash: rollup.contentHash, verified: rollup.verified, createdAt: new Date(rollup.createdAt),
      }).onConflictDoNothing({ target: [experimentReportRollups.experimentId, experimentReportRollups.rollupKind] }).returning();
      return inserted[0] ?? (await tx.select().from(experimentReportRollups).where(and(eq(experimentReportRollups.experimentId, rollup.experimentId), eq(experimentReportRollups.rollupKind, rollup.rollupKind))).limit(1))[0];
    });
    if (!row) throw new Error("ANALYTICS_ROLLUP_WRITE_FAILED");
    const rowCount = Number(row.sourceEventCount ?? 0);
    const rowSeq = Number(row.sourceCommitSeq ?? 0);
    const requestedCount = rollup.sourceEventCount ?? 0;
    const requestedSeq = rollup.sourceCommitSeq ?? 0;
    const rowIsStrictlyNewer = rowCount > requestedCount || (rowCount === requestedCount && rowSeq > requestedSeq);
    if (row.contentHash !== rollup.contentHash && !rowIsStrictlyNewer) throw new Error("ANALYTICS_ROLLUP_CONFLICT");
    return {
      experimentId: row.experimentId, rollupKind: row.rollupKind, schemaVersion: row.schemaVersion, sourceCursor: Number(row.sourceCursor),
      sourceFenceId: row.sourceFenceId, sourceFenceOccurredAt: row.sourceFenceOccurredAt?.toISOString() ?? null, sourceEventCount: Number(row.sourceEventCount ?? 0), sourceCommitSeq: Number(row.sourceCommitSeq ?? 0), sourceRecordedAt: row.sourceRecordedAt?.toISOString() ?? null, stableRead: row.stableRead, previousContentHash: row.previousContentHash, ...(row.coverageHash ? { coverageHash: row.coverageHash } : {}),
      accumulator: row.accumulator as Record<string, unknown>, contentHash: row.contentHash, verified: row.verified, createdAt: row.createdAt.toISOString(),
    };
  }
  async loadAnalyticsRollup(experimentId: string, rollupKind = "canonical"): Promise<AnalyticsRollup | null> {
    const rows = await this.db.select().from(experimentReportRollups).where(and(eq(experimentReportRollups.experimentId, experimentId), eq(experimentReportRollups.rollupKind, rollupKind))).limit(1);
    const row = rows[0];
    return row ? { experimentId: row.experimentId, rollupKind: row.rollupKind, schemaVersion: row.schemaVersion, sourceCursor: Number(row.sourceCursor), sourceFenceId: row.sourceFenceId, sourceFenceOccurredAt: row.sourceFenceOccurredAt?.toISOString() ?? null, sourceEventCount: Number(row.sourceEventCount ?? 0), sourceCommitSeq: Number(row.sourceCommitSeq ?? 0), sourceRecordedAt: row.sourceRecordedAt?.toISOString() ?? null, stableRead: row.stableRead, previousContentHash: row.previousContentHash, ...(row.coverageHash ? { coverageHash: row.coverageHash } : {}), accumulator: row.accumulator as Record<string, unknown>, contentHash: row.contentHash, verified: row.verified, createdAt: row.createdAt.toISOString() } : null;
  }
  async deleteCanonicalEventsThrough(input: { experimentId: string; occurredBefore?: Date; limit: number; sourceCursor: number; sourceCommitSeq?: number; sourceRecordedAt?: Date }): Promise<{ deleted: number }> {
    assertJournalCapability(this.access, "maintenance", "deleteCanonicalEventsThrough");
    const cap = Math.max(0, Math.min(500, Math.floor(input.limit)));
    if (cap <= 0) return { deleted: 0 };
    // A cleanup-authoritative rollup must prove both the durable commit fence
    // and the recorded-at fence. recorded_at alone is not commit authority.
    if (input.sourceCommitSeq == null) return { deleted: 0 };
    const before = input.occurredBefore ?? new Date();
    const types = [...CANONICAL_REPORT_INPUT_EVENT_TYPES] as string[];
    const result = await this.client`
      DELETE FROM journal_events
      WHERE id IN (
        SELECT id FROM journal_events
        WHERE type = ANY(${types}) AND (payload->>'experimentId') = ${input.experimentId}
          AND occurred_at < ${before.toISOString()}
          AND commit_seq <= ${input.sourceCommitSeq}
          AND (${input.sourceRecordedAt == null} OR recorded_at <= ${(input.sourceRecordedAt ?? new Date()).toISOString()})
        ORDER BY occurred_at ASC, id ASC LIMIT ${cap}
      )`;
    return { deleted: result.count ?? 0 };
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    assertJournalCapability(this.access, "runtime-write", "saveCheckpoint");
    const id = createId("checkpoint");
    const payload = structuredClone(checkpoint.payload);
    const contentHash = hashCheckpointPayload(payload);
    payload.checkpointContentHash = contentHash;
    const cursor = typeof payload.durableCursor === "number" ? payload.durableCursor : null;
    const toCheckpoint = (row: typeof engineCheckpoints.$inferSelect): EngineCheckpoint => ({
      id: row.id, experimentId: row.experimentId, sequence: row.sequence,
      payload: row.payload as Record<string, unknown>, createdAt: row.createdAt.toISOString(), contentHash, verified: true,
    });
    if (cursor == null) {
      const rows = await this.db.insert(engineCheckpoints).values({ id, ...checkpoint, payload }).returning();
      const row = rows[0];
      if (!row) throw new Error("CHECKPOINT_WRITE_FAILED");
      return toCheckpoint(row);
    }
    return this.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`);
      await tx.insert(durableSequenceMetadata).values({ id: 1, highWater: 0 }).onConflictDoNothing();
      const latestRows = await tx.select({ highWater: durableSequenceMetadata.highWater }).from(durableSequenceMetadata).where(eq(durableSequenceMetadata.id, 1)).limit(1);
      const latest = Number(latestRows[0]?.highWater ?? 0);
      if (cursor !== latest) throw new CheckpointFenceError(cursor, latest);
      const rows = await tx.insert(engineCheckpoints).values({ id, ...checkpoint, payload }).returning();
      const row = rows[0];
      if (!row) throw new Error("CHECKPOINT_WRITE_FAILED");
      return toCheckpoint(row);
    });
  }
  async loadLatestCheckpoint(): Promise<EngineCheckpoint | null> {
    const rows = await this.db.select().from(engineCheckpoints).orderBy(desc(engineCheckpoints.sequence)).limit(1);
    const row = rows[0];
    if (!row) return null;
    const payload = row.payload as Record<string, unknown>;
    const contentHash = typeof payload.checkpointContentHash === "string" ? payload.checkpointContentHash : undefined;
    return {
      id: row.id, experimentId: row.experimentId, sequence: row.sequence, payload,
      createdAt: row.createdAt.toISOString(),
      ...(contentHash ? { contentHash } : {}),
      verified: Boolean(contentHash && hashCheckpointPayload(payload) === contentHash),
    };
  }
  async countEligible(types: readonly string[], occurredBefore: Date): Promise<{ count: number; oldestOccurredAt: string | null }> {
    if (types.length === 0) return { count: 0, oldestOccurredAt: null };
    const rows = await this.db.select({
      count: sql<number>`count(*)::int`,
      oldest: sql<Date | null>`min(${journalEvents.occurredAt})`,
    }).from(journalEvents)
      .where(and(inArray(journalEvents.type, [...types]), lt(journalEvents.occurredAt, occurredBefore)));
    const row = rows[0];
    return {
      count: Number(row?.count ?? 0),
      oldestOccurredAt: row?.oldest ? new Date(row.oldest).toISOString() : null,
    };
  }
  async latestCommitSeq(): Promise<number> {
    const rows = await this.db.select({ highWater: journalCommitMetadata.highWater })
      .from(journalCommitMetadata).where(eq(journalCommitMetadata.id, 1)).limit(1);
    return Number(rows[0]?.highWater ?? 0);
  }
  async eventCommitSeq(id: string): Promise<number | null> {
    const rows = await this.db.select({ commitSeq: journalEvents.commitSeq }).from(journalEvents)
      .where(eq(journalEvents.id, id)).limit(1);
    const value = rows[0]?.commitSeq;
    return value == null ? null : Number(value);
  }
  async withReadSnapshot<T>(work: (journal: EventJournalKeyset) => Promise<T>): Promise<{ value: T; stable: boolean }> {
    // The caller owns the accumulator. Retrying a partially executed callback
    // here would fold its earlier pages twice; retry must start a fresh report.
    let value!: T;
    await this.client.begin(async (sql) => {
      await sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`;
      const snap: EventJournalKeyset = {
        queryEventsKeyset: async (query) => {
          const built = buildJournalKeysetSql(query);
          const rows = await sql.unsafe(built.text, built.values as never[]) as Array<Record<string, unknown>>;
          return rows.map((row) => toJournalEvent({
            id: String(row.id),
            traceId: String(row.trace_id),
            correlationId: String(row.correlation_id),
            type: String(row.type),
            aggregateType: String(row.aggregate_type),
            aggregateId: String(row.aggregate_id),
            payload: row.payload as Record<string, unknown>,
            occurredAt: new Date(row.occurred_at as string | Date),
            recordedAt: new Date(row.recorded_at as string | Date),
            sourceEventId: row.source_event_id == null ? null : String(row.source_event_id),
          } as typeof journalEvents.$inferSelect));
        },
        peekLatestKeyset: async (query) => {
          const built = buildJournalKeysetSql({ ...query, limit: 1, orderDesc: true });
          const rows = await sql.unsafe(built.text, built.values as never[]) as Array<{ occurred_at: Date | string; id: string }>;
          const row = rows[0];
          return row ? { occurredAt: new Date(row.occurred_at).toISOString(), id: row.id } : null;
        },
        latestCommitSeq: async () => {
          const rows = await sql`SELECT high_water FROM journal_commit_metadata WHERE id = 1`;
          return Number(rows[0]?.high_water ?? 0);
        },
      };
      value = await work(snap);
    });
    return { value, stable: true };
  }
  private async ensureKeysetIndexesIfSafe(): Promise<void> {
    const missing: string[] = [];
    for (const name of LARGE_JOURNAL_KEYSET_INDEXES) {
      const rows = await this.client`SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = ${name}`;
      if (rows.length === 0) missing.push(name);
    }
    if (missing.length === 0) return;
    const countRows = await this.client`SELECT coalesce(reltuples, 0)::bigint AS estimate FROM pg_class WHERE relname = 'journal_events'`;
    const estimate = Number(countRows[0]?.estimate ?? 0);
    if (estimate > LARGE_JOURNAL_SYNC_INDEX_MAX_ROWS) return;
    await this.client.unsafe(`CREATE INDEX IF NOT EXISTS journal_occurred_id_idx ON journal_events (occurred_at, id)`);
    await this.client.unsafe(`CREATE INDEX IF NOT EXISTS journal_type_occurred_id_idx ON journal_events (type, occurred_at, id)`);
  }
  async close(): Promise<void> { await this.client.end({ timeout: 5 }); }
}

/** postgres.js `unsafe` binds text; Date objects throw at parameter bind. */
function sqlBindValue(value: unknown): unknown {
  return value instanceof Date ? value.toISOString() : value;
}

export function buildJournalKeysetSql(query: JournalKeysetQuery & { orderDesc?: boolean }): { text: string; values: unknown[] } {
  const clauses: string[] = [];
  const values: unknown[] = [];
  const push = (fragment: string, value: unknown) => {
    values.push(sqlBindValue(value));
    clauses.push(fragment.replace("?", `$${values.length}`));
  };
  if (query.types && query.types.length > 0) push(`type = ANY(?)`, query.types);
  if (query.experimentId) {
    values.push(query.experimentId, query.experimentId);
    clauses.push(`(aggregate_id = $${values.length - 1} OR payload->>'experimentId' = $${values.length})`);
  }
  // Drizzle installs pass-through timestamp serializers on this shared client.
  // Raw postgres.js snapshot queries must bind ISO strings, not Date objects.
  if (query.from) push(`occurred_at >= ?`, query.from);
  if (query.to) push(`occurred_at <= ?`, query.to);
  if (query.recordedAtTo) push(`recorded_at <= ?`, query.recordedAtTo);
  if (query.commitSeqTo != null) push(`commit_seq <= ?`, query.commitSeqTo);
  if (query.commitSeqAfter != null) push(`commit_seq > ?`, query.commitSeqAfter);
  if (query.after) {
    values.push(sqlBindValue(query.after.occurredAt), query.after.id);
    clauses.push(`(occurred_at, id) > ($${values.length - 1}::timestamptz, $${values.length})`);
  }
  if (query.through) {
    values.push(sqlBindValue(query.through.occurredAt), query.through.id);
    clauses.push(`(occurred_at, id) <= ($${values.length - 1}::timestamptz, $${values.length})`);
  }
  values.push(Math.max(1, Math.min(query.limit, 20_000)));
  const where = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
  const order = query.orderDesc ? "occurred_at DESC, id DESC" : "occurred_at ASC, id ASC";
  return {
    text: `SELECT id, trace_id, correlation_id, type, aggregate_type, aggregate_id, payload, occurred_at, recorded_at, commit_seq, source_event_id FROM journal_events ${where} ORDER BY ${order} LIMIT $${values.length}`,
    values,
  };
}

function journalKeysetFilters(query: Omit<JournalKeysetQuery, "limit">): ReturnType<typeof sql>[] {
  const filters = [];
  if (query.types && query.types.length > 0) filters.push(inArray(journalEvents.type, [...query.types]));
  if (query.experimentId) {
    filters.push(sql`(${journalEvents.aggregateId} = ${query.experimentId} OR ${journalEvents.payload}->>'experimentId' = ${query.experimentId})`);
  }
  if (query.from) filters.push(gte(journalEvents.occurredAt, query.from));
  if (query.to) filters.push(lte(journalEvents.occurredAt, query.to));
  if (query.recordedAtTo) filters.push(lte(journalEvents.recordedAt, query.recordedAtTo));
  if (query.commitSeqTo != null) filters.push(lte(journalEvents.commitSeq, query.commitSeqTo));
  if (query.commitSeqAfter != null) filters.push(gt(journalEvents.commitSeq, query.commitSeqAfter));
  if (query.after) {
    filters.push(sql`(${journalEvents.occurredAt}, ${journalEvents.id}) > (${query.after.occurredAt}::timestamptz, ${query.after.id})`);
  }
  if (query.through) {
    filters.push(sql`(${journalEvents.occurredAt}, ${journalEvents.id}) <= (${query.through.occurredAt}::timestamptz, ${query.through.id})`);
  }
  return filters;
}

function toExperimentReport(row: typeof experimentReports.$inferSelect): DurableExperimentReport {
  return {
    experimentId: row.experimentId,
    reportKind: "final",
    schemaVersion: row.schemaVersion,
    generatedAt: row.generatedAt.toISOString(),
    complete: row.complete,
    truncated: row.truncated,
    sourceEventCount: row.sourceEventCount,
    sourceWindowStart: row.sourceWindowStart?.toISOString() ?? null,
    sourceWindowEnd: row.sourceWindowEnd?.toISOString() ?? null,
    sourceFenceOccurredAt: row.sourceFenceOccurredAt?.toISOString() ?? null,
    sourceFenceId: row.sourceFenceId,
    earliestIncludedAt: row.earliestIncludedAt?.toISOString() ?? null,
    latestIncludedAt: row.latestIncludedAt?.toISOString() ?? null,
    canonicalSummary: row.canonicalSummary as Record<string, unknown>,
    renderedMarkdown: row.renderedMarkdown,
    contentHash: row.contentHash,
    persistedAt: row.persistedAt.toISOString(),
  };
}

function assertJournalCapability(mode: JournalAccessMode, capability: "runtime-write" | "report-write" | "maintenance", operation: string): void {
  const allowed = capability === "runtime-write" ? journalMayRuntimeWrite(mode)
    : capability === "report-write" ? journalMayPersistReport(mode)
    : journalMayMaintain(mode);
  if (!allowed) throw new JournalMutateForbiddenError(mode, operation);
}

function toJournalEvent(row: typeof journalEvents.$inferSelect): JournalEvent {
  return journalEventSchema.parse({ ...row, occurredAt: row.occurredAt.toISOString(), recordedAt: row.recordedAt.toISOString() });
}

function toDurableTransition(row: typeof durableTransitions.$inferSelect): DurableTransition {
  return {
    sequence: Number(row.sequence),
    operationKey: row.operationKey,
    type: row.type,
    aggregateType: row.aggregateType,
    aggregateId: row.aggregateId,
    payloadHash: row.payloadHash,
    payload: row.payload as Record<string, unknown>,
    occurredAt: row.occurredAt.toISOString(),
    recordedAt: row.recordedAt.toISOString(),
  };
}

function replayLifecycleState(row: Pick<DurableTransition, "type" | "payload">): "open" | "closed" | "historical" {
  // A full engine fill is pinned while its market/position obligation remains
  // live. A later POSITION_SETTLED scope update closes it, after which normal
  // bounded pressure/TTL eviction is safe. Minimal fixtures without a fill
  // payload retain the historical closed behavior.
  if (row.type === "PAPER_FILL" && row.payload && typeof row.payload === "object" && "fill" in row.payload) return "open";
  if (row.type === "PAPER_MAKER_RESERVED" || row.type === "EXPERIMENT_STARTED" || row.type === "EXPERIMENT_PREPARED") return "open";
  if (row.type === "REPORT_SOURCE_INTEGRITY_FAULT") return "open";
  return "closed";
}

function replayScopeId(row: Pick<DurableTransition, "type" | "payload" | "aggregateId">): string | null {
  const payload = row.payload as Record<string, unknown>;
  if (row.type === "PAPER_FILL" && payload.fill && typeof payload.fill === "object") {
    const marketId = (payload.fill as Record<string, unknown>).marketId;
    return typeof marketId === "string" ? marketId : null;
  }
  if (row.type === "POSITION_SETTLED" && typeof payload.marketId === "string") return payload.marketId;
  if (row.type === "PAPER_MAKER_RESERVED" || row.type === "PAPER_MAKER_RELEASED" || row.type === "PAPER_ORDER_CANCELLED") {
    return typeof payload.orderId === "string" ? payload.orderId : row.aggregateId;
  }
  if (row.type === "EXPERIMENT_PREPARED") {
    const experiment = payload.experiment;
    if (experiment && typeof experiment === "object" && typeof (experiment as Record<string, unknown>).id === "string") {
      return (experiment as Record<string, unknown>).id as string;
    }
  }
  if (row.type.startsWith("EXPERIMENT_")) return typeof payload.experimentId === "string" ? payload.experimentId : row.aggregateId;
  return null;
}

export { EVENT_PAGE_SIZE, EVENT_SCAN_CEILING };

export interface PagedEventScan {
  events: JournalEvent[];
  complete: boolean;
  truncated: boolean;
  scanned: number;
}

export async function queryEventsPaged(
  journal: EventJournal,
  query: Omit<JournalQuery, "limit" | "offset">,
  pageSize = EVENT_PAGE_SIZE,
  ceiling = EVENT_SCAN_CEILING,
): Promise<PagedEventScan> {
  const size = Math.max(1, Math.min(pageSize, 20_000));
  const cap = Math.max(1, ceiling);
  const events: JournalEvent[] = [];
  let offset = 0;
  let truncated = false;
  for (;;) {
    if (offset >= cap) {
      truncated = true;
      break;
    }
    const page = await journal.queryEvents({ ...query, limit: size, offset, order: query.order ?? "asc" });
    if (page.length === 0) break;
    events.push(...page);
    if (page.length < size) break;
    offset += page.length;
    if (offset >= cap) {
      truncated = true;
      break;
    }
  }
  return {
    events: sortJournalEventsChronologically(events),
    complete: !truncated,
    truncated,
    scanned: events.length,
  };
}

export function createJournalEvent(input: {
  id?: string;
  traceId: string; correlationId?: string; type: string; aggregateType: string; aggregateId: string;
  payload: Record<string, unknown>; occurredAt?: Date; sourceEventId?: string | null;
}): JournalEvent {
  const now = new Date();
  return {
    id: input.id ?? createId("event"), traceId: input.traceId, correlationId: input.correlationId ?? input.traceId,
    type: input.type, aggregateType: input.aggregateType, aggregateId: input.aggregateId,
    payload: input.payload, occurredAt: (input.occurredAt ?? now).toISOString(), recordedAt: now.toISOString(),
    sourceEventId: input.sourceEventId ?? null,
  };
}

export { CheckpointFenceError, DurableCursorBehindError, IdempotencyConflictError, ExperimentReportConflictError, SchemaVerificationError, JournalMutateForbiddenError };
