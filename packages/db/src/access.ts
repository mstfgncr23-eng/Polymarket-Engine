/** How a journal connection is allowed to touch Postgres. */

export const JOURNAL_ACCESS_MODES = ["boot-migrate", "verify-only", "readonly", "maintenance"] as const;
export type JournalAccessMode = (typeof JOURNAL_ACCESS_MODES)[number];

export function journalMayMigrate(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate";
}

export function journalMayMutateData(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate" || mode === "maintenance";
}

export function journalMustVerifySchema(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate" || mode === "verify-only" || mode === "maintenance";
}

export class JournalMigrateForbiddenError extends Error {
  readonly code = "JOURNAL_MIGRATE_FORBIDDEN";
  constructor(readonly mode: JournalAccessMode) {
    super(`JOURNAL_MIGRATE_FORBIDDEN: access=${mode} cannot run migrate/DDL`);
    this.name = "JournalMigrateForbiddenError";
  }
}

export const LARGE_JOURNAL_KEYSET_INDEXES = [
  "journal_occurred_id_idx",
  "journal_type_occurred_id_idx",
] as const;

/** Sync CREATE INDEX is safe only on empty/small local journals. Production uses CONCURRENTLY. */
export const LARGE_JOURNAL_SYNC_INDEX_MAX_ROWS = 10_000;
