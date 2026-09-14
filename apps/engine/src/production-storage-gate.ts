const REQUIRED_PRODUCTION_STORAGE_FLAGS = [
  "RETENTION_ENABLED",
  "RETENTION_COMPACTION_ENABLED",
  "RETENTION_ANALYTICS_ROLLUP_ENABLED",
] as const;

const MAX_PRODUCTION_COMPLETED_GRACE_MS = 48 * 60 * 60_000;

function flagEnabled(value: string | undefined): boolean {
  return value === "true" || value === "1";
}

/**
 * Production must never run an experiment with the bounded-storage lifecycle
 * silently disabled. The engine can otherwise accumulate canonical journal
 * detail until the database quota is exhausted even though retention code is
 * present in the binary.
 */
export function assertProductionStorageLifecycle(env: NodeJS.ProcessEnv = process.env): void {
  if (env.NODE_ENV !== "production") return;

  const disabled = REQUIRED_PRODUCTION_STORAGE_FLAGS.filter((name) => !flagEnabled(env[name]));
  if (disabled.length > 0) {
    throw new Error(`PRODUCTION_STORAGE_LIFECYCLE_REQUIRED:${disabled.join(",")}`);
  }

  const rawGrace = env.RETENTION_COMPLETED_GRACE_MS;
  if (rawGrace == null || rawGrace.trim() === "") {
    throw new Error("PRODUCTION_RETENTION_COMPLETED_GRACE_REQUIRED");
  }
  const completedGraceMs = Number(rawGrace);
  if (!Number.isFinite(completedGraceMs) || completedGraceMs <= 0 || completedGraceMs > MAX_PRODUCTION_COMPLETED_GRACE_MS) {
    throw new Error(`PRODUCTION_RETENTION_COMPLETED_GRACE_UNBOUNDED:${rawGrace}`);
  }
}

export { REQUIRED_PRODUCTION_STORAGE_FLAGS, MAX_PRODUCTION_COMPLETED_GRACE_MS };
