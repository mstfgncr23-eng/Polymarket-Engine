export const REQUIRED_PUBLIC_TABLES = [
  "journal_events",
  "engine_checkpoints",
  "mcp_idempotency_keys",
  "durable_transitions",
  "experiment_reports",
] as const;

export const REQUIRED_INDEXES = [
  "journal_trace_idx",
  "journal_aggregate_idx",
  "journal_type_time_idx",
  "journal_experiment_idx",
  "journal_occurred_id_idx",
  "journal_type_occurred_id_idx",
  "checkpoint_sequence_idx",
  "mcp_idempotency_scope_key_idx",
  "durable_transitions_operation_key_uidx",
  "durable_transitions_sequence_idx",
  "experiment_reports_pkey",
  "experiment_reports_hash_idx",
  "experiment_reports_complete_idx",
] as const;

export const REQUIRED_RLS_TABLES = [
  "journal_events",
  "engine_checkpoints",
  "mcp_idempotency_keys",
  "durable_transitions",
  "experiment_reports",
] as const;

/** PR18 additions are verified separately so older read-only tooling remains compatible. */
export const COMPACTION_TABLES = ["durable_sequence_metadata", "experiment_report_rollups", "durable_replay_keys"] as const;
export const COMPACTION_INDEXES = ["durable_sequence_metadata_pkey", "experiment_report_rollups_pkey", "experiment_report_rollups_cursor_idx", "durable_replay_keys_sequence_idx", "durable_replay_keys_expiry_idx"] as const;

export function evaluateCompactionSchemaInspection(found: SchemaInspection): void {
  const missing: string[] = [];
  for (const table of COMPACTION_TABLES) if (!found.tables.includes(table)) missing.push(`missing relation public.${table}`);
  for (const index of COMPACTION_INDEXES) if (!found.indexes.includes(index)) missing.push(`missing index ${index}`);
  for (const table of COMPACTION_TABLES) {
    if (found.tables.includes(table) && !found.rlsEnabled.includes(table)) missing.push(`rls disabled on public.${table}`);
    if (found.rlsForced.includes(table)) missing.push(`rls forced on public.${table}`);
  }
  if (missing.length > 0) throw new SchemaVerificationError(missing);
}

export class SchemaVerificationError extends Error {
  readonly code = "SCHEMA_VERIFICATION_FAILED";
  constructor(readonly missing: readonly string[]) {
    super(`SCHEMA_VERIFICATION_FAILED: ${missing.join("; ")}`);
    this.name = "SchemaVerificationError";
  }
}

export interface SchemaInspection {
  tables: readonly string[];
  indexes: readonly string[];
  rlsEnabled: readonly string[];
  rlsForced: readonly string[];
  invalidIndexes?: readonly string[];
}

export function evaluateSchemaInspection(found: SchemaInspection): void {
  const missing: string[] = [];
  for (const table of REQUIRED_PUBLIC_TABLES) {
    if (!found.tables.includes(table)) missing.push(`missing relation public.${table}`);
  }
  for (const index of REQUIRED_INDEXES) {
    if (!found.indexes.includes(index)) missing.push(`missing index ${index}`);
  }
  if (found.invalidIndexes == null) {
    missing.push("index validity not inspected");
  }
  for (const index of found.invalidIndexes ?? []) {
    missing.push(`invalid index ${index}`);
  }
  for (const table of REQUIRED_RLS_TABLES) {
    if (found.tables.includes(table) && !found.rlsEnabled.includes(table)) {
      missing.push(`rls disabled on public.${table}`);
    }
    if (found.rlsForced.includes(table)) missing.push(`rls forced on public.${table}`);
  }
  if (missing.length > 0) throw new SchemaVerificationError(missing);
}

export async function inspectPublicSchema(
  query: <T extends Record<string, unknown>>(sql: string) => Promise<T[]>,
): Promise<SchemaInspection> {
  const tables = await query<{ tablename: string }>(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
  );
  const indexes = await query<{ indexname: string }>(
    `SELECT indexname FROM pg_indexes WHERE schemaname = 'public'`,
  );
  const rls = await query<{ relname: string; relrowsecurity: boolean; relforcerowsecurity: boolean }>(
    `SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relkind = 'r'`,
  );
  const invalid = await query<{ indexname: string }>(
    `SELECT c.relname AS indexname
     FROM pg_index i
     JOIN pg_class c ON c.oid = i.indexrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND NOT i.indisvalid`,
  );
  return {
    tables: tables.map((row) => row.tablename),
    indexes: indexes.map((row) => row.indexname),
    rlsEnabled: rls.filter((row) => row.relrowsecurity).map((row) => row.relname),
    rlsForced: rls.filter((row) => row.relforcerowsecurity).map((row) => row.relname),
    invalidIndexes: invalid.map((row) => row.indexname),
  };
}

export async function verifyRequiredSchema(
  query: <T extends Record<string, unknown>>(sql: string) => Promise<T[]>,
): Promise<void> {
  const inspection = await inspectPublicSchema(query);
  evaluateSchemaInspection(inspection);
  evaluateCompactionSchemaInspection(inspection);
}
