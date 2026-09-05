import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import type { JournalEvent } from "@pm/core";
import { MemoryEventJournal, PostgresEventJournal, buildJournalKeysetSql, isCanonicalFinalReport, isJournalAppendAbortedError, isJournalAppendDeadlineError, isTransientJournalError, JournalAppendAbortedError, type JournalAppendOptions, type EventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { streamExperimentSummary } from "../apps/engine/src/final-report.js";
import { loadSourceSeal } from "../apps/engine/src/source-seal.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";

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
  insert(table: unknown): { values(value: unknown): { onConflictDoNothing(input: unknown): Promise<void> } };
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
    if (event.type !== "EXPERIMENT_SOURCE_SEALED") return super.append(event, options);
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
            insert: () => ({ values: () => ({ onConflictDoNothing: async () => {
              if (this.insertDelayMs) await new Promise((resolve) => setTimeout(resolve, this.insertDelayMs));
              if (aborted) throw new Error("transaction aborted");
              this.inserts += 1; // INSERT succeeded, row still uncommitted.
            } }) }),
          });
          if (mode === "irreversible" || mode === "transport") {
            clearTimeout(timer); // PG17 CommitTransaction, before RecordTransactionCommit.
          }
          if (mode === "transport") {
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

const ADVISORY_COMMIT_WAITER_SQL = `SELECT EXISTS (
  SELECT 1 FROM pg_stat_activity
  WHERE wait_event_type = 'Lock'
    AND wait_event = 'advisory'
    AND lower(btrim(query)) IN ('commit', 'commit;')
) AS waiting`;

async function waitForAdvisoryCommitWaiter(
  client: { unsafe: (sql: string) => Promise<Array<{ waiting: boolean }>> },
  timeoutMs: number,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const rows = await client.unsafe(ADVISORY_COMMIT_WAITER_SQL);
    if (rows[0]?.waiting) return true;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return false;
}

async function waitUntilNoAdvisoryCommitWaiter(
  client: { unsafe: (sql: string) => Promise<Array<{ waiting: boolean }>> },
  timeoutMs: number,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const rows = await client.unsafe(ADVISORY_COMMIT_WAITER_SQL);
    if (!rows[0]?.waiting) return true;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  return false;
}

async function dropSealCommitHold(client: { unsafe: (sql: string) => Promise<unknown> }): Promise<void> {
  await client.unsafe("DROP TRIGGER IF EXISTS step4_hold_seal_commit ON journal_events").catch(() => undefined);
  await client.unsafe("DROP FUNCTION IF EXISTS step4_hold_seal_commit()").catch(() => undefined);
}

async function evictDeadPoolConnections(client: { unsafe: (sql: string) => Promise<unknown> }): Promise<void> {
  for (let i = 0; i < 4; i += 1) {
    await client.unsafe("SELECT 1").catch(() => undefined);
  }
}

describe("Step-4 Postgres transaction/commit outcome", () => {
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

  it.skipIf(!process.env.STEP4_TEST_DATABASE_URL)("real PG17: held transaction after INSERT aborts before timeout result, then retry/restart produce one canonical seal/report", { timeout: 30_000 }, async () => {
    const url = process.env.STEP4_TEST_DATABASE_URL!;
    const target = new URL(url);
    // This is a disposable CI service, never an operator/production DATABASE_URL.
    expect(["localhost", "127.0.0.1"]).toContain(target.hostname);
    expect(target.pathname).toBe("/step4_test");
    const migrations = fileURLToPath(new URL("../packages/db/migrations", import.meta.url));
    const pg = new PostgresEventJournal(url, migrations);
    const engine = paperEngine(pg);
    const inserted = deferred();
    await engine.start();
    const db = Reflect.get(pg, "db");
    const client = Reflect.get(pg, "client");
    const blocker = await client.reserve();
    let blockerReleased = false;
    const transaction = db.transaction.bind(db);
    let holdNext = true;
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.completeExperimentForTest();
      const experimentId = engine.experimentManifest()!.id;
      // Hold actual server-side COMMIT, after INSERT, in a deferred constraint
      // trigger. This avoids replacing postgres.js's transaction completion.
      await client.unsafe(`CREATE FUNCTION step4_hold_seal_commit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_advisory_xact_lock(431517); RETURN NEW; END $$`);
      await client.unsafe(`CREATE CONSTRAINT TRIGGER step4_hold_seal_commit
        AFTER INSERT ON journal_events DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW WHEN (NEW.type = 'EXPERIMENT_SOURCE_SEALED')
        EXECUTE FUNCTION step4_hold_seal_commit()`);
      await blocker.unsafe("SELECT pg_advisory_lock(431517)");
      db.transaction = (work: (tx: Tx) => Promise<void>, ...args: unknown[]) => transaction(async (tx: Tx) => {
        if (!holdNext) return work(tx);
        holdNext = false;
        const execute = tx.execute.bind(tx);
        tx.execute = async (query) => {
          const result = await execute(query);
          if (dialect.sqlToQuery(query).sql.includes("'transaction_timeout'")) {
            await new Promise((resolve) => setTimeout(resolve, 1_500));
          }
          return result;
        };
        const result = await work(tx); // Real INSERT succeeded; COMMIT runs the deferred trigger.
        inserted.resolve();
        return result;
      }, ...args);
      engine.setSourceSealTimeoutForTest(2_000);
      const attempt = engine.ensureSourceSealForTest();
      await inserted.promise;
      expect(await loadSourceSeal(pg, experimentId)).toBeNull();
      expect(await waitForAdvisoryCommitWaiter(client, 400)).toBe(true);
      expect(await attempt).toEqual({ sealed: false, reason: "SOURCE_SEAL_TIMEOUT" });
      // transaction_timeout must abort the waiter before unlock, or a late COMMIT can appear.
      expect(await waitUntilNoAdvisoryCommitWaiter(client, 3_000)).toBe(true);
      await dropSealCommitHold(client);
      db.transaction = transaction;
      await blocker.unsafe("SELECT pg_advisory_unlock(431517)").catch(() => undefined);
      blocker.release();
      blockerReleased = true;
      await evictDeadPoolConnections(client);
      expect(await loadSourceSeal(pg, experimentId)).toBeNull();
      engine.setSourceSealTimeoutForTest(5_000);
      expect(await engine.ensureSourceSealForTest()).toEqual({ sealed: true, reason: null });
      expect(await pg.queryEvents({ types: ["EXPERIMENT_SOURCE_SEALED"], experimentId })).toHaveLength(1);
      const streamed = await streamExperimentSummary(pg, engine.experimentManifest()!, {
        status: "COMPLETED", reportKind: "final", metricsScope: "complete", complete: true, truncated: false,
      });
      expect(streamed.scan.complete).toBe(true);
      expect(streamed.scan.scanned).toBeGreaterThanOrEqual(2);
      expect(streamed.summary.complete).toBe(true);
      await engine.writeFinalReportForTest();
      const report = await pg.loadExperimentReport(experimentId);
      expect(report && isCanonicalFinalReport(report)).toBe(true);
      await engine.stop();
      const restartJournal = new PostgresEventJournal(url, migrations);
      const restarted = paperEngine(restartJournal);
      await restarted.start();
      try {
        expect(await restarted.ensureSourceSealForTest()).toEqual({ sealed: true, reason: null });
        await restarted.writeFinalReportForTest();
        expect(await restartJournal.queryEvents({ types: ["EXPERIMENT_SOURCE_SEALED"], experimentId })).toHaveLength(1);
        expect((await restartJournal.loadExperimentReport(experimentId))?.contentHash).toBe(report!.contentHash);
      } finally {
        await restarted.stop();
        await restartJournal.close();
      }
    } finally {
      await dropSealCommitHold(client);
      db.transaction = transaction;
      if (!blockerReleased) {
        await blocker.unsafe("SELECT pg_advisory_unlock(431517)").catch(() => undefined);
        blocker.release();
      }
      if (!engine.state.draining) await engine.stop();
      await pg.close();
    }
  });

  it("successful INSERT near deadline, held COMMIT: timeout aborts; release cannot commit; retry/restart finality is idempotent", async () => {
    const journal = new CommitModelJournal();
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.completeExperimentForTest();
      const experimentId = engine.experimentManifest()!.id;
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      engine.setSourceSealTimeoutForTest(100);
      const attempt = engine.ensureSourceSealForTest();
      await vi.advanceTimersByTimeAsync(90);
      expect(journal.inserts).toBe(1);
      expect(journal.commits).toBe(0);
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      await vi.advanceTimersByTimeAsync(20);
      expect(await attempt).toEqual({ sealed: false, reason: "SOURCE_SEAL_TIMEOUT" });
      expect(journal.aborted).toBe(true);
      expect(journal.queries).toEqual([
        expect.objectContaining({ sql: "SELECT set_config('statement_timeout', $1, true)", params: ["100"] }),
        expect.objectContaining({ sql: "SELECT set_config('transaction_timeout', $1, true), pg_current_xact_id()::text AS xid", params: ["100"] }),
      ]);
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(journal.commits).toBe(0);
      expect(await loadSourceSeal(journal, experimentId)).toBeNull();
      vi.useRealTimers();
      journal.mode = "healthy";
      journal.insertDelayMs = 0;
      engine.setSourceSealTimeoutForTest(5_000);
      expect(await engine.ensureSourceSealForTest()).toEqual({ sealed: true, reason: null });
      expect(journal.commits).toBe(1);
      expect(await journal.queryEvents({ types: ["EXPERIMENT_SOURCE_SEALED"] })).toHaveLength(1);
      await engine.writeFinalReportForTest();
      const report = await journal.loadExperimentReport(experimentId);
      expect(report && isCanonicalFinalReport(report)).toBe(true);
      await engine.stop();
      const restarted = paperEngine(journal);
      await restarted.start();
      try {
        expect(await restarted.ensureSourceSealForTest()).toEqual({ sealed: true, reason: null });
        await restarted.writeFinalReportForTest();
        expect((await journal.loadExperimentReport(experimentId))?.contentHash).toBe(report!.contentHash);
        expect(await journal.queryEvents({ types: ["EXPERIMENT_SOURCE_SEALED"] })).toHaveLength(1);
        expect(journal.commits).toBe(1);
      } finally { await restarted.stop(); }
    } finally {
      vi.useRealTimers();
      journal.commitHold.resolve();
      if (!engine.state.draining) await engine.stop();
      await journal.pg.close();
    }
  });

  it("irreversible COMMIT crossing deadline cannot be called a timeout; concurrent retry shares its eventual success", async () => {
    const journal = new CommitModelJournal();
    journal.mode = "irreversible";
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.completeExperimentForTest();
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      engine.setSourceSealTimeoutForTest(100);
      let returned = false;
      const attempt = engine.ensureSourceSealForTest().then((result) => { returned = true; return result; });
      await vi.advanceTimersByTimeAsync(110);
      expect(journal.inserts).toBe(1);
      expect(returned).toBe(false);
      const retry = engine.ensureSourceSealForTest();
      expect(journal.transactions).toBe(1);
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(await attempt).toEqual({ sealed: true, reason: null });
      expect(await retry).toEqual({ sealed: true, reason: null });
      expect(journal.commits).toBe(1);
    } finally {
      vi.useRealTimers(); journal.commitHold.resolve(); await engine.stop(); await journal.pg.close();
    }
  });

  it("unknown transport outcome at COMMIT is a persist failure, never proof of timeout/rollback", async () => {
    const journal = new CommitModelJournal();
    journal.mode = "transport";
    const engine = paperEngine(journal);
    await engine.start();
    try {
      expect(await engine.beginExperiment()).toMatchObject({ ok: true });
      await engine.completeExperimentForTest();
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      engine.setSourceSealTimeoutForTest(100);
      const attempt = engine.ensureSourceSealForTest();
      await vi.advanceTimersByTimeAsync(110);
      expect(await attempt).toEqual({ sealed: false, reason: "SOURCE_SEAL_PERSIST_FAILED" });
      journal.commitHold.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(journal.commits).toBe(1); // The transport error was correctly NOT a rollback assertion.
    } finally {
      vi.useRealTimers(); journal.commitHold.resolve(); await engine.stop(); await journal.pg.close();
    }
  });
});
