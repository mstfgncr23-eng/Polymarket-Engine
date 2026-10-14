import { createRequire } from "node:module";
import { describe, expect, it, vi } from "vitest";
import type { JournalEvent } from "@pm/core";
import { MemoryEventJournal, PostgresEventJournal, buildJournalKeysetSql, isCanonicalFinalReport, isJournalAppendAbortedError, isJournalAppendDeadlineError, isTransientJournalError, JournalAppendAbortedError, type JournalAppendOptions, type EventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { createJournalEvent } from "@pm/db";
import { isolatedPostgres } from "./simple-postgres-fixture.js";
import { KeyedResultFlight } from "../apps/engine/src/runtime-supervisor.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";

// Migration/deprecation rationale and all seven original cases are recorded in
// SOURCE_SEAL_TEST_MIGRATION.md. No source-seal runtime contract is reconstructed.

// Resolve the adapter's pinned Drizzle dependency, without adding a test dependency.
const { PgDialect } = createRequire(new URL("../packages/db/package.json", import.meta.url))("drizzle-orm/pg-core");
const dialect = new PgDialect();
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

type Tx = {
  execute(query: unknown): Promise<unknown>;
  insert(table: unknown): { values(value: unknown): { onConflictDoNothing(input: unknown): { returning(input: unknown): Promise<{ id: string }[]> } } };
};

/**
 * The existing db.transaction boundary is the seam: run the real Postgres
 * adapter, including its SQL settings and error mapping. Model PG17's two
 * phases: abortable open transaction, then irreversible COMMIT with its timeout
 * disabled. A timer may terminate the former independently of the client hold.
 * This deliberately does not model COMMIT as an abortable INSERT.
 */
class CommitModelJournal extends MemoryEventJournal {
  readonly pg = new PostgresEventJournal("postgres://test:test@127.0.0.1:1/unused", "/unused");
  readonly queries: { sql: string; params: unknown[] }[] = [];
  readonly commitHold = deferred();
  insertDelayMs = 90;
  mode: "open" | "irreversible" | "transport" | "healthy" = "open";
  inserts = 0;
  transactions = 0;
  commits = 0;
  aborted = false;

  override async append(event: JournalEvent, options: JournalAppendOptions = {}): Promise<void> {
    if (event.id !== "commit-outcome-input") return super.append(event, options);
    Object.defineProperty(this.pg, "db", { configurable: true, value: {
      execute: async (query: unknown) => {
        const compiled = dialect.sqlToQuery(query);
        expect(compiled.sql).toBe("SELECT pg_xact_status($1::xid8) AS status");
        expect(compiled.params).toEqual(["42"]);
        return [{ status: this.aborted ? "aborted" : this.commits ? "committed" : "in progress" }];
      },
      transaction: async (work: (tx: Tx) => Promise<void>) => {
        this.transactions += 1;
        const mode = this.mode;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let aborted = false;
        let rejectOutcome!: (error: Error) => void;
        const serverOutcome = new Promise<never>((_, reject) => { rejectOutcome = reject; });
        const scope = async () => {
          await work({
            execute: async (query) => {
              const compiled = dialect.sqlToQuery(query);
              this.queries.push(compiled);
              if (compiled.sql.includes("'transaction_timeout'")) {
                timer = setTimeout(() => {
                  aborted = this.aborted = true;
                  rejectOutcome(Object.assign(new Error("terminating connection due to transaction timeout"), { code: "25P04" }));
                }, Number(compiled.params[0]));
              }
              return [{ xid: "42" }];
            },
            insert: () => ({ values: () => ({ onConflictDoNothing: () => ({ returning: async () => {
              if (this.insertDelayMs) await new Promise((resolve) => setTimeout(resolve, this.insertDelayMs));
              if (aborted) throw new Error("transaction aborted");
              this.inserts += 1; // INSERT succeeded, row still uncommitted.
              return [{ id: event.id }];
            } }) }) }),
          });
          if (mode === "irreversible" || mode === "transport") {
            clearTimeout(timer); // PG17 CommitTransaction, before RecordTransactionCommit.
          }
          if (mode === "transport") {
            // The transport failure arrives after the global deadline, while
            // the irreversible transaction can still finish on the server.
            await new Promise((resolve) => setTimeout(resolve, 20));
            rejectOutcome(Object.assign(new Error("connection closed during commit"), { code: "CONNECTION_CLOSED" }));
          }
          if (mode !== "healthy") await this.commitHold.promise;
          if (aborted) throw new Error("transaction already terminated; COMMIT impossible");
          await super.append(event);
          this.commits += 1;
        };
        try { await Promise.race([scope(), serverOutcome]); }
        finally { clearTimeout(timer); }
      },
    } });
    await this.pg.append(event, options);
  }
}

function paperEngine(journal: EventJournal) {
  const adapter = new ScriptedMarketAdapter();
  adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
  return new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
    market: adapter as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
  });
}

// Exercise the current journal contract with a real canonical input. No seal
// module, test-only seal authority, or seal persistence API is reconstructed.
function inputEvent(experimentId = "commit-exp") {
  return createJournalEvent({ id: "commit-outcome-input", traceId: experimentId,
    type: "TRIGGER_EVENT", aggregateType: "experiment", aggregateId: experimentId,
    payload: { experimentId }, occurredAt: new Date() });
}

describe("Current Engine journal transaction/commit outcome (migrated Step-4 controls)", () => {
  it("preserves confirmed adapter deadline abort even when the client wall clock has time remaining", async () => {
    const journal = new MemoryEventJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const event = inputEvent(engine.experimentManifest()!.id);
      const append = journal.append.bind(journal);
      journal.append = async (item, options) => {
        if (item.id === event.id) throw new JournalAppendAbortedError("deadline");
        return append(item, options);
      };
      await expect(engine.persistReportInput(event, undefined, { deadlineAt: Date.now() + 5_000 }))
        .rejects.toMatchObject({ reason: "deadline", code: "JOURNAL_APPEND_ABORTED" });
      expect(await journal.readJournalEventById(event.id)).toBeNull();
      expect(engine.journalAdmissionSnapshot().active).toBe(0);
    } finally { await engine.stop(); }
  });
  it("classifies abort/deadline and transient journal errors without relying on instanceof identity", () => {
    expect(isJournalAppendAbortedError(new JournalAppendAbortedError("deadline"))).toBe(true);
    expect(isJournalAppendDeadlineError(new JournalAppendAbortedError("deadline"))).toBe(true);
    expect(isJournalAppendDeadlineError(new JournalAppendAbortedError("aborted"))).toBe(false);
    expect(isJournalAppendDeadlineError(Object.assign(new Error("JOURNAL_APPEND_DEADLINE"), { name: "JournalAppendAbortedError", code: "JOURNAL_APPEND_ABORTED", reason: "deadline" }))).toBe(true);
    expect(isJournalAppendAbortedError(Object.assign(new Error("JOURNAL_APPEND_DEADLINE"), { name: "JournalAppendAbortedError", code: "JOURNAL_APPEND_ABORTED" }))).toBe(true);
    expect(isJournalAppendAbortedError(new Error("JOURNAL_APPEND_FAILED"))).toBe(false);
    expect(isTransientJournalError(Object.assign(new Error("terminating connection due to transaction timeout"), { code: "25P04" }))).toBe(true);
    expect(isTransientJournalError(Object.assign(new Error("Cannot read properties of null (reading 'write')"), { code: "CONNECTION_CLOSED" }))).toBe(true);
    expect(isTransientJournalError(new JournalAppendAbortedError("deadline"))).toBe(false);
  });

  it("REPEATABLE READ keyset SQL binds Date fences as ISO strings for postgres.js unsafe", () => {
    const recordedAtTo = new Date("2026-09-05T18:00:00.000Z");
    const built = buildJournalKeysetSql({
      types: ["EXPERIMENT_STARTED"],
      from: new Date("2026-09-05T17:00:00.000Z"),
      to: new Date("2026-09-05T18:00:00.000Z"),
      recordedAtTo,
      after: { occurredAt: "2026-09-05T17:30:00.000Z", id: "evt-1" },
      limit: 8,
    });
    expect(built.values.every((value) => !(value instanceof Date))).toBe(true);
    expect(built.values).toEqual([
      ["EXPERIMENT_STARTED"],
      "2026-09-05T17:00:00.000Z",
      "2026-09-05T18:00:00.000Z",
      "2026-09-05T18:00:00.000Z",
      "2026-09-05T17:30:00.000Z",
      "evt-1",
      8,
    ]);
  });


  it.runIf(process.env.SIMPLE_RETENTION_POSTGRES_TEST === "1")("real PG17: INSERT followed by held open transaction is server-aborted; retry/restart preserves one input/report", { timeout: 30_000 }, async () => {
    const fixture = await isolatedPostgres();
    const pg = fixture.journal;
    const engine = paperEngine(pg);
    const db = Reflect.get(pg, "db");
    const transaction = db.transaction.bind(db);
    const inserted = deferred();
    let holdNext = true;
    try {
      expect(Number((await fixture.sql`SHOW server_version_num`)[0].server_version_num)).toBeGreaterThanOrEqual(170000);
      await engine.start();
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      const event = inputEvent(engine.experimentManifest()!.id);
      db.transaction = (work: (tx: Tx) => Promise<void>, ...args: unknown[]) => transaction(async (tx: Tx) => {
        const hold = holdNext; holdNext = false;
        const result = await work(tx); // Real INSERT succeeded, still inside abortable transaction.
        if (hold) {
          inserted.resolve();
          // Keep the server transaction open after INSERT. Do not pretend an
          // irreversible COMMIT can be cancelled by a client Promise timeout.
          await new Promise((resolve) => setTimeout(resolve, 1_200));
        }
        return result;
      }, ...args);
      const attempt = pg.append(event, { deadlineAt: Date.now() + 600 })
        .then(() => null, (error: unknown) => error);
      await inserted.promise;
      expect(await pg.readJournalEventById(event.id)).toBeNull();
      expect(isJournalAppendDeadlineError(await attempt)).toBe(true);
      db.transaction = transaction;
      expect(await pg.readJournalEventById(event.id)).toBeNull();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(await pg.readJournalEventById(event.id)).toBeNull();
      await pg.append(event, { deadlineAt: Date.now() + 5_000 });
      await pg.append(event);
      expect(await pg.queryEvents({ types: [event.type] })).toHaveLength(1);
      await engine.completeExperimentForTest();
      await engine.writeFinalReportForTest();
      const report = await pg.loadExperimentReport(engine.experimentManifest()!.id);
      expect(report && isCanonicalFinalReport(report)).toBe(true);
      const snapshot = await pg.withReadSnapshot((snap) => snap.queryEventsKeyset({ experimentId: event.aggregateId, limit: 100 }));
      expect(snapshot.value).toEqual(await pg.queryEventsKeyset({ experimentId: event.aggregateId, limit: 100 }));
      await engine.stop();
      const restarted = paperEngine(pg);
      await restarted.start();
      try {
        await restarted.writeFinalReportForTest();
        expect((await pg.loadExperimentReport(event.aggregateId))?.contentHash).toBe(report!.contentHash);
        expect(await pg.queryEvents({ types: [event.type] })).toHaveLength(1);
      } finally { await restarted.stop(); }
    } finally {
      db.transaction = transaction;
      if (!engine.state.draining) await engine.stop();
      await fixture.close();
    }
  });

  it("successful INSERT near deadline, held open transaction: confirmed abort prevents late commit; retry is idempotent", async () => {
    const journal = new CommitModelJournal();
    const event = inputEvent();
    try {
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      const attempt = journal.append(event, { deadlineAt: Date.now() + 100 }).then(() => null, (error: unknown) => error);
      await vi.advanceTimersByTimeAsync(90);
      expect(journal.inserts).toBe(1);
      expect(journal.commits).toBe(0);
      expect(await journal.readJournalEventById(event.id)).toBeNull();
      await vi.advanceTimersByTimeAsync(20);
      expect(isJournalAppendDeadlineError(await attempt)).toBe(true);
      expect(journal.aborted).toBe(true);
      expect(journal.queries).toEqual([
        expect.objectContaining({ sql: "SELECT set_config('statement_timeout', $1, true)", params: ["100"] }),
        expect.objectContaining({ sql: "SELECT set_config('transaction_timeout', $1, true), pg_current_xact_id()::text AS xid", params: ["100"] }),
      ]);
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(journal.commits).toBe(0);
      expect(await journal.readJournalEventById(event.id)).toBeNull();
      vi.useRealTimers();
      journal.mode = "healthy"; journal.insertDelayMs = 0;
      await journal.append(event, { deadlineAt: Date.now() + 5_000 });
      await journal.append(event);
      expect(await journal.queryEvents({ types: [event.type] })).toHaveLength(1);
    } finally { vi.useRealTimers(); journal.commitHold.resolve(); await journal.pg.close(); }
  });

  it("irreversible COMMIT crossing deadline stays pending; the current keyed-flight primitive shares eventual success", async () => {
    const journal = new CommitModelJournal(); journal.mode = "irreversible";
    const event = inputEvent();
    const flight = new KeyedResultFlight<void>(); // Current complete-metrics single-flight primitive, not a seal API.
    try {
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      let returned = false;
      const work = () => journal.append(event, { deadlineAt: Date.now() + 100 });
      const attempt = flight.run(event.id, work).then(() => { returned = true; });
      await vi.advanceTimersByTimeAsync(110);
      expect(journal.inserts).toBe(1);
      expect(returned).toBe(false);
      const retry = flight.run(event.id, work);
      expect(journal.transactions).toBe(1);
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      await Promise.all([attempt, retry]);
      expect(journal.commits).toBe(1);
      expect(await journal.readJournalEventById(event.id)).toEqual(event);
    } finally { vi.useRealTimers(); journal.commitHold.resolve(); await journal.pg.close(); }
  });

  it("unknown transport outcome during irreversible COMMIT is not proof of deadline rollback", async () => {
    const journal = new CommitModelJournal(); journal.mode = "transport";
    const event = inputEvent();
    try {
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      const attempt = journal.append(event, { deadlineAt: Date.now() + 100 }).then(() => null, (error: unknown) => error);
      await vi.advanceTimersByTimeAsync(110);
      const error = await attempt;
      expect(error).toMatchObject({ code: "CONNECTION_CLOSED" });
      expect(isJournalAppendDeadlineError(error)).toBe(false);
      expect(journal.aborted).toBe(false);
      expect(await journal.readJournalEventById(event.id)).toBeNull();
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(journal.commits).toBe(1);
      expect(await journal.readJournalEventById(event.id)).toEqual(event);
    } finally { vi.useRealTimers(); journal.commitHold.resolve(); await journal.pg.close(); }
  });
});
