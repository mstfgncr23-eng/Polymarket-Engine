import { describe, expect, it } from "vitest";
import { assertProductionStorageLifecycle } from "../apps/engine/src/production-storage-gate.js";

const safeProduction = {
  NODE_ENV: "production",
  RETENTION_ENABLED: "true",
  RETENTION_COMPACTION_ENABLED: "true",
  RETENTION_ANALYTICS_ROLLUP_ENABLED: "true",
  RETENTION_COMPLETED_GRACE_MS: String(24 * 60 * 60_000),
} as NodeJS.ProcessEnv;

describe("production storage lifecycle gate", () => {
  it("accepts production only when retention, compaction and analytics rollup are enabled", () => {
    expect(() => assertProductionStorageLifecycle(safeProduction)).not.toThrow();
  });

  it.each([
    "RETENTION_ENABLED",
    "RETENTION_COMPACTION_ENABLED",
    "RETENTION_ANALYTICS_ROLLUP_ENABLED",
  ] as const)("fails closed when %s is missing or disabled", (flag) => {
    const missing = { ...safeProduction };
    delete missing[flag];
    expect(() => assertProductionStorageLifecycle(missing)).toThrow(/PRODUCTION_STORAGE_LIFECYCLE_REQUIRED/);

    expect(() => assertProductionStorageLifecycle({ ...safeProduction, [flag]: "false" })).toThrow(/PRODUCTION_STORAGE_LIFECYCLE_REQUIRED/);
  });

  it("requires an explicit completed-experiment grace no larger than 48 hours", () => {
    const missing = { ...safeProduction };
    delete missing.RETENTION_COMPLETED_GRACE_MS;
    expect(() => assertProductionStorageLifecycle(missing)).toThrow(/PRODUCTION_RETENTION_COMPLETED_GRACE_REQUIRED/);

    expect(() => assertProductionStorageLifecycle({
      ...safeProduction,
      RETENTION_COMPLETED_GRACE_MS: String(7 * 24 * 60 * 60_000),
    })).toThrow(/PRODUCTION_RETENTION_COMPLETED_GRACE_UNBOUNDED/);
  });

  it("does not constrain non-production environments", () => {
    expect(() => assertProductionStorageLifecycle({ NODE_ENV: "test" })).not.toThrow();
    expect(() => assertProductionStorageLifecycle({ NODE_ENV: "development" })).not.toThrow();
  });
});
