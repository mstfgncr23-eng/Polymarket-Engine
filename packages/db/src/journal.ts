import { desc, eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import { createId, journalEventSchema, type JournalEvent } from "@pm/core";
import { engineCheckpoints, journalEvents, mcpIdempotencyKeys } from "./schema.js";

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
  claimIdempotency(scope: string, key: string): Promise<boolean>;
  saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint>;
  loadLatestCheckpoint(): Promise<EngineCheckpoint | null>;
  close(): Promise<void>;
}

export class MemoryEventJournal implements EventJournal {
  private readonly events: JournalEvent[] = [];
  private readonly checkpoints: EngineCheckpoint[] = [];
  private readonly idempotencyKeys = new Set<string>();
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
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const composite = `${scope}\u0000${key}`;
    if (this.idempotencyKeys.has(composite)) return false;
    this.idempotencyKeys.add(composite);
    return true;
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    if (!this.available) throw new Error("DATABASE_UNAVAILABLE");
    const stored = { ...structuredClone(checkpoint), id: createId("checkpoint"), createdAt: new Date().toISOString() };
    this.checkpoints.push(stored);
    return structuredClone(stored);
  }
  async loadLatestCheckpoint(): Promise<EngineCheckpoint | null> {
    const latest = this.checkpoints.toSorted((a, b) => b.sequence - a.sequence)[0];
    return latest ? structuredClone(latest) : null;
  }
  async close(): Promise<void> {}
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
    const rows = await this.db.select().from(journalEvents);
    return rows.map(toJournalEvent);
  }
  async claimIdempotency(scope: string, key: string): Promise<boolean> {
    const rows = await this.db.insert(mcpIdempotencyKeys).values({ scope, key })
      .onConflictDoNothing({ target: [mcpIdempotencyKeys.scope, mcpIdempotencyKeys.key] }).returning({ key: mcpIdempotencyKeys.key });
    return rows.length === 1;
  }
  async saveCheckpoint(checkpoint: Omit<EngineCheckpoint, "id" | "createdAt">): Promise<EngineCheckpoint> {
    const id = createId("checkpoint");
    const rows = await this.db.insert(engineCheckpoints).values({ id, ...checkpoint }).returning();
    const row = rows[0];
    if (!row) throw new Error("CHECKPOINT_WRITE_FAILED");
    return { id: row.id, experimentId: row.experimentId, sequence: row.sequence, payload: row.payload as Record<string, unknown>, createdAt: row.createdAt.toISOString() };
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

export function createJournalEvent(input: {
  traceId: string; correlationId?: string; type: string; aggregateType: string; aggregateId: string;
  payload: Record<string, unknown>; occurredAt?: Date; sourceEventId?: string | null;
}): JournalEvent {
  const now = new Date();
  return {
    id: createId("event"), traceId: input.traceId, correlationId: input.correlationId ?? input.traceId,
    type: input.type, aggregateType: input.aggregateType, aggregateId: input.aggregateId,
    payload: input.payload, occurredAt: (input.occurredAt ?? now).toISOString(), recordedAt: now.toISOString(),
    sourceEventId: input.sourceEventId ?? null,
  };
}
