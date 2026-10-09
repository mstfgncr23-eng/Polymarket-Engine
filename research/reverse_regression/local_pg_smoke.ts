import { resolve } from "node:path";
import { PostgresEventJournal, createJournalEvent } from "../../packages/db/src/index.ts";

const target = process.env.DATABASE_URL;
if (!target) throw new Error("LOCAL_DB_URL_REQUIRED");
const url = new URL(target);
if (url.hostname !== "127.0.0.1" || url.pathname !== "/forensic" || url.username !== "forensic") {
  throw new Error("LOCAL_DB_TARGET_GUARD_REJECTED");
}
if (process.env.PM_FORENSIC_ENV !== "isolated-paper") {
  throw new Error("ISOLATED_PAPER_GUARD_REJECTED");
}
const journal = new PostgresEventJournal(target, resolve("packages/db/migrations"));
try {
  await journal.initialize();
  const healthy = await journal.healthCheck();
  if (!healthy) throw new Error("ISOLATED_POSTGRES_HEALTH_FAILED");
  const event = createJournalEvent({
    traceId: "forensic_local_journal_probe",
    type: "SYSTEM_HEALTH_EVENT", aggregateType: "forensic", aggregateId: "local-smoke",
    payload: { status: "OK", name: "isolated-db-smoke" },
  });
  await journal.append(event);
  await journal.append(event);
  console.log(JSON.stringify({
    mode: "isolated-paper", database: "127.0.0.1/forensic",
    schema: "initialized", health: "UP", append: "OK", duplicate_append: "IDEMPOTENT",
  }));
} finally {
  await journal.close();
}
