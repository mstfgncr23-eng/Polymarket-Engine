import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { PostgresEventJournal } from "@pm/db";
const postgres = createRequire(new URL("../packages/db/package.json", import.meta.url))("postgres");

/** Dedicated test container only. Each case owns a whole disposable database,
 * so real production migrations/triggers/locks run without schema substitutions. */
export async function p0PostgresFixture(raw: string) {
  const url = new URL(raw);
  if (url.hostname !== "paper-coordinated-test-pg" || url.pathname !== "/paper_test" || url.username !== "paper_test")
    throw new Error("P0_DISPOSABLE_POSTGRES_REQUIRED");
  const admin = postgres(url.toString(), { max: 1, onnotice: () => undefined });
  const database = "p0_execution_" + randomUUID().replaceAll("-", "");
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  url.pathname = "/" + database;
  const journal = new PostgresEventJournal(url.toString(), resolve("packages/db/migrations"));
  const close = journal.close.bind(journal);
  journal.close = async () => undefined; // Restart retains the same isolated authority.
  return { journal, async close() {
    await close();
    if (!/^p0_execution_[a-f0-9]{32}$/.test(database)) throw Error("UNSAFE_TEST_DATABASE_NAME");
    await admin.unsafe(`DROP DATABASE "${database}" WITH (FORCE)`);
    await admin.end();
  } };
}
