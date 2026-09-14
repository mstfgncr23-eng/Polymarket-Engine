import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PostgresEventJournal } from "@pm/db";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import {
  loadVerifiedAnalyticsRollup,
  persistVerifiedAnalyticsRollup,
} from "../apps/engine/src/final-report.js";

const databaseUrl = process.env.STEP4_TEST_DATABASE_URL;
const migrations = fileURLToPath(new URL("../packages/db/migrations", import.meta.url));

function localPgUrl(): string {
  if (!databaseUrl) throw new Error("STEP4_TEST_DATABASE_URL_REQUIRED");
  const parsed = new URL(databaseUrl);
  // Never let this integration test target production Supabase.
  expect(["localhost", "127.0.0.1"]).toContain(parsed.hostname);
  return databaseUrl;
}

describe("analytics rollup Postgres JSONB restart round-trip", () => {
  it.skipIf(!databaseUrl)("persists and reloads a verified accumulator with undefined optional fields", { timeout: 30_000 }, async () => {
    const url = localPgUrl();
    const experimentId = `rollup-pg-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const accumulator = new ExperimentSummaryAccumulator(experimentId, {
      reportKind: "interim",
      metricsScope: "rolling",
      status: "RUNNING",
      complete: false,
    }).serialize() as unknown as Record<string, unknown>;

    // This is the exact shape that broke production: serialize() contains
    // lastLedger: undefined, while Postgres jsonb omits that property.
    expect(Object.prototype.hasOwnProperty.call(accumulator, "lastLedger")).toBe(true);
    expect(accumulator.lastLedger).toBeUndefined();

    const writer = new PostgresEventJournal(url, migrations);
    await writer.initialize();
    let storedHash: string;
    try {
      const persisted = await persistVerifiedAnalyticsRollup(writer, {
        experimentId,
        sourceCursor: 1,
        sourceEventCount: 0,
        sourceCommitSeq: 1,
        stableRead: true,
        sourceCoverageComplete: true,
        accumulator,
      });
      expect(persisted).not.toBeNull();
      storedHash = persisted!.contentHash;
    } finally {
      await writer.close();
    }

    const reader = new PostgresEventJournal(url, migrations, { access: "readonly" });
    await reader.connectWithoutMigrate("readonly");
    try {
      const raw = await reader.loadAnalyticsRollup!(experimentId, "canonical");
      expect(raw).not.toBeNull();
      expect(Object.prototype.hasOwnProperty.call(raw!.accumulator, "lastLedger")).toBe(false);
      expect(raw!.contentHash).toBe(storedHash!);

      const verifiedAfterRestart = await loadVerifiedAnalyticsRollup(reader, experimentId);
      expect(verifiedAfterRestart).not.toBeNull();
      expect(verifiedAfterRestart!.contentHash).toBe(storedHash!);
    } finally {
      await reader.close();
    }
  });
});
