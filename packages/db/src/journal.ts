import { and, asc, desc, eq, gt, gte, inArray, lte, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import {
  compareJournalEventOrder, createId, EVENT_PAGE_SIZE, EVENT_SCAN_CEILING, journalEventSchema,
  sortJournalEventsChronologically, type JournalEvent, type JournalQuery,
} from "@pm/core";
import {
  CheckpointFenceError,
  DURABLE_MUTATION_LOCK_ID,
  durableTransitionInputSchema,
  hashDurablePayload,
  IdempotencyConflictError,
  type DurableCommitResult,
  type DurableTransition,
  type DurableTransitionInput,
} from "./durable.js";
import { durableTransitions, engineCheckpoints, journalEvents, mcpIdempotencyKeys } from "./schema.js";

export interface EngineCheckpoint {
  id: string;
  experimentId: string | null;
  sequence: number;
  payload: Record<string, unknown>;
  createdAt: string;
}

export interface EventJournal {
  initialize(): Promise<void>;
  healthCheck(): Promise<boolean>;
  append(event: JournalEvent): Promise<void>;
  appendMany(events: readonly JournalEvent[]): Promise<void>;
  readTrace(traceId: string): Promise<JournalEvent[]>;
  readAll(): Promise<JournalEvent[]>;
  queryEvents(query: JournalQuery): Promise<JournalEvent[]>;
  countByType(types?: readonly string[]): Promise<Record<string, number>>;
  claimIdempotency(scope: string, key: string): Promise<boolean>;
  commitDurableTransition(input: DurableTransitionInput): Promise<DurableCommitResult>;
  readDurableTransitionsAfter(sequence: number, limit: number): Promise<DurableTransition[]>;
  latestDurableSequence(): Promise<number>;
  saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint>;
  loadLatestCheckpoint(): Promise<EngineCheckpoint | null>;
  close(): Promise<void>;
}

export class MemoryEventJournal implements EventJournal {
  private readonly events: JournalEvent[] = [];
  private readonly checkpoints: EngineCheckpoint[] = [];
  private readonly idempotencyKeys = new Set<string>();
  private readonly durableRows: DurableTransition[] = [];
  private readonly durableByKey = new Map<string, DurableTransition>();
  private nextDurableSequence = 1;
  private durableChain = Promise.resolve();
  available = true;

  async initialize(): Promise<void> {}
  async healthCheck(): Promise<boolean> { return this.available; }
  async append(event: JournalEvent): Promise<void> {
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const parsed = journalEventSchema.parse(event);
    if (this.events.some((item) => item.id === parsed.id)) return;
    this.events.push(structuredClone(parsed));
  }
  async appendMany(events: readonly JournalEvent[]): Promise<void> {
    for (const event of events) await this.append(event);
  }
  async readTrace(traceId: string): Promise<JournalEvent[]> { return structuredClone(this.events.filter((event) => event.traceId === traceId)); }
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
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const composite = `${scope}\u0000${key}`;
    if (this.idempotencyKeys.has(composite)) return false;
    this.idempotencyKeys.add(composite);
    return true;
  }
  async commitDurableTransition(input: DurableTransitionInput): Promise<DurableCommitResult> {
    return this.withDurableLock(() => this.commitDurableTransitionUnlocked(input));
  }
  async readDurableTransitionsAfter(sequence: number, limit: number): Promise<DurableTransition[]> {
    const cap = Math.max(1, Math.min(limit, 10_000));
    return structuredClone(this.durableRows.filter((row) => row.sequence > sequence).slice(0, cap));
  }
  async latestDurableSequence(): Promise<number> {
    return this.durableRows.length === 0 ? 0 : this.durableRows[this.durableRows.length - 1]!.sequence;
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    return this.withDurableLock(() => {
      const cursor = typeof checkpoint.payload.durableCursor === "number" ? checkpoint.payload.durableCursor : null;
      if (cursor != null) {
        const latest = this.durableRows.length === 0 ? 0 : this.durableRows[this.durableRows.length - 1]!.sequence;
        if (cursor !== latest) throw new CheckpointFenceError(cursor, latest);
      }
      const stored = { ...structuredClone(checkpoint), id: createId("checkpoint"), createdAt: new Date().toISOString() };
      this.checkpoints.push(stored);
      return structuredClone(stored);
    });
  }
  async loadLatestCheckpoint(): Promise<EngineCheckpoint | null> {
    const latest = this.checkpoints.toSorted((a, b) => b.sequence - a.sequence)[0];
    return latest ? structuredClone(latest) : null;
  }
  async close(): Promise<void> {}

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

  private commitDurableTransitionUnlocked(input: DurableTransitionInput): DurableCommitResult {
    const parsed = durableTransitionInputSchema.parse(input);
    const payloadHash = hashDurablePayload(parsed.payload);
    const existing = this.durableByKey.get(parsed.operationKey);
    if (existing) {
      if (
        existing.type !== parsed.type
        || existing.aggregateType !== parsed.aggregateType
        || existing.aggregateId !== parsed.aggregateId
        || existing.payloadHash !== payloadHash
      ) {
        throw new IdempotencyConflictError(parsed.operationKey, existing.type, parsed.type);
      }
      return { sequence: existing.sequence, operationKey: parsed.operationKey, inserted: false, existing: true };
    }
    const now = new Date();
    const row: DurableTransition = {
      sequence: this.nextDurableSequence,
      operationKey: parsed.operationKey,
      type: parsed.type,
      aggregateType: parsed.aggregateType,
      aggregateId: parsed.aggregateId,
      payloadHash,
      payload: structuredClone(parsed.payload),
      occurredAt: (parsed.occurredAt ?? now).toISOString(),
      recordedAt: now.toISOString(),
    };
    this.nextDurableSequence += 1;
    this.durableRows.push(row);
    this.durableByKey.set(parsed.operationKey, row);
    return { sequence: row.sequence, operationKey: parsed.operationKey, inserted: true, existing: false };
  }
}

export class PostgresEventJournal implements EventJournal {
  private readonly client: ReturnType<typeof postgres>;
  private readonly db: ReturnType<typeof drizzle>;

  constructor(databaseUrl: string, private readonly migrationsFolder: string) {
    this.client = postgres(databaseUrl, { max: 10, idle_timeout: 20, connect_timeout: 10 });
    this.db = drizzle(this.client);
  }

  async initialize(): Promise<void> { await migrate(this.db, { migrationsFolder: this.migrationsFolder }); }
  async healthCheck(): Promise<boolean> {
    try { await this.client`select 1`; return true; } catch { return false; }
  }
  async append(event: JournalEvent): Promise<void> {
    const value = journalEventSchema.parse(event);
    await this.db.insert(journalEvents).values({
      ...value, occurredAt: new Date(value.occurredAt), recordedAt: new Date(value.recordedAt),
    }).onConflictDoNothing({ target: journalEvents.id });
  }
  async appendMany(events: readonly JournalEvent[]): Promise<void> {
    if (events.length === 0) return;
    const values = events.map((raw) => {
      const value = journalEventSchema.parse(raw);
      return { ...value, occurredAt: new Date(value.occurredAt), recordedAt: new Date(value.recordedAt) };
    });
    await this.db.insert(journalEvents).values(values).onConflictDoNothing({ target: journalEvents.id });
  }
  async readTrace(traceId: string): Promise<JournalEvent[]> {
    const rows = await this.db.select().from(journalEvents).where(eq(journalEvents.traceId, traceId));
    return rows.map(toJournalEvent);
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
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    const rows = await this.db.insert(mcpIdempotencyKeys).values({ scope, key })
      .onConflictDoNothing({ target: [mcpIdempotencyKeys.scope, mcpIdempotencyKeys.key] }).returning({ key: mcpIdempotencyKeys.key });
    return rows.length === 1;
  }
  async commitDurableTransition(input: DurableTransitionInput): Promise<DurableCommitResult> {
    const parsed = durableTransitionInputSchema.parse(input);
    const payloadHash = hashDurablePayload(parsed.payload);
    const occurredAt = parsed.occurredAt ?? new Date();
    return this.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`);
      const existingRows = await tx.select().from(durableTransitions)
        .where(eq(durableTransitions.operationKey, parsed.operationKey)).limit(1);
      const existing = existingRows[0];
      if (existing) {
        if (
          existing.type !== parsed.type
          || existing.aggregateType !== parsed.aggregateType
          || existing.aggregateId !== parsed.aggregateId
          || existing.payloadHash !== payloadHash
        ) {
          throw new IdempotencyConflictError(parsed.operationKey, existing.type, parsed.type);
        }
        return { sequence: Number(existing.sequence), operationKey: parsed.operationKey, inserted: false, existing: true };
      }
      const inserted = await tx.insert(durableTransitions).values({
        operationKey: parsed.operationKey,
        type: parsed.type,
        aggregateType: parsed.aggregateType,
        aggregateId: parsed.aggregateId,
        payloadHash,
        payload: parsed.payload,
        occurredAt,
      }).returning({ sequence: durableTransitions.sequence });
      const sequence = inserted[0]?.sequence;
      if (sequence == null) throw new Error("DURABLE_TRANSITION_INSERT_FAILED");
      return { sequence: Number(sequence), operationKey: parsed.operationKey, inserted: true, existing: false };
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
  async latestDurableSequence(): Promise<number> {
    const rows = await this.db.select({
      max: sql<number>`coalesce(max(${durableTransitions.sequence}), 0)`,
    }).from(durableTransitions);
    return Number(rows[0]?.max ?? 0);
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    const id = createId("checkpoint");
    const cursor = typeof checkpoint.payload.durableCursor === "number" ? checkpoint.payload.durableCursor : null;
    const toCheckpoint = (row: typeof engineCheckpoints.$inferSelect): EngineCheckpoint => ({
      id: row.id, experimentId: row.experimentId, sequence: row.sequence,
      payload: row.payload as Record<string, unknown>, createdAt: row.createdAt.toISOString(),
    });
    if (cursor == null) {
      const rows = await this.db.insert(engineCheckpoints).values({ id, ...checkpoint }).returning();
      const row = rows[0];
      if (!row) throw new Error("CHECKPOINT_WRITE_FAILED");
      return toCheckpoint(row);
    }
    return this.db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`);
      const latestRows = await tx.select({
        max: sql<number>`coalesce(max(${durableTransitions.sequence}), 0)`,
      }).from(durableTransitions);
      const latest = Number(latestRows[0]?.max ?? 0);
      if (cursor !== latest) throw new CheckpointFenceError(cursor, latest);
      const rows = await tx.insert(engineCheckpoints).values({ id, ...checkpoint }).returning();
      const row = rows[0];
      if (!row) throw new Error("CHECKPOINT_WRITE_FAILED");
      return toCheckpoint(row);
    });
  }
  async loadLatestCheckpoint(): Promise<EngineCheckpoint | null> {
    const rows = await this.db.select().from(engineCheckpoints).orderBy(desc(engineCheckpoints.sequence)).limit(1);
    const row = rows[0];
    return row ? { id: row.id, experimentId: row.experimentId, sequence: row.sequence, payload: row.payload as Record<string, unknown>, createdAt: row.createdAt.toISOString() } : null;
  }
  async close(): Promise<void> { await this.client.end({ timeout: 5 }); }
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

export { CheckpointFenceError, IdempotencyConflictError };
