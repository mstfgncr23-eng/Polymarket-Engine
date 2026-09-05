/** How a journal connection is allowed to touch Postgres. */

export const JOURNAL_ACCESS_MODES = ["boot-migrate", "verify-only", "readonly", "maintenance", "report-write"] as const;
export type JournalAccessMode = (typeof JOURNAL_ACCESS_MODES)[number];

export type JournalMutationCapability = "migrate" | "runtime-write" | "report-write" | "maintenance";

export function journalMayMigrate(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate";
}

export function journalMayRuntimeWrite(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate";
}

export function journalMayPersistReport(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate" || mode === "report-write";
}

export function journalMayMaintain(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate" || mode === "maintenance";
}

export function journalMayMutateData(mode: JournalAccessMode): boolean {
  return journalMayRuntimeWrite(mode) || journalMayPersistReport(mode) || journalMayMaintain(mode);
}

export function journalMustVerifySchema(mode: JournalAccessMode): boolean {
  return mode === "boot-migrate" || mode === "verify-only" || mode === "maintenance" || mode === "report-write";
}

export class JournalMigrateForbiddenError extends Error {
  readonly code = "JOURNAL_MIGRATE_FORBIDDEN";
  constructor(readonly mode: JournalAccessMode) {
    super(`JOURNAL_MIGRATE_FORBIDDEN: access=${mode} cannot run migrate/DDL`);
    this.name = "JournalMigrateForbiddenError";
  }
}

export class JournalMutateForbiddenError extends Error {
  readonly code = "JOURNAL_MUTATE_FORBIDDEN";
  constructor(readonly mode: JournalAccessMode, readonly operation: string) {
    super(`JOURNAL_MUTATE_FORBIDDEN: ${operation} access=${mode}`);
    this.name = "JournalMutateForbiddenError";
  }
}

export const LARGE_JOURNAL_KEYSET_INDEXES = [
  "journal_occurred_id_idx",
  "journal_type_occurred_id_idx",
] as const;

/** Sync CREATE INDEX is safe only on empty/small local journals. Production uses CONCURRENTLY. */
export const LARGE_JOURNAL_SYNC_INDEX_MAX_ROWS = 10_000;
