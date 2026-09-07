import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MONITOR_CONTROL_EVENT_TYPES } from "@pm/core";
import { createJournalEvent, PostgresEventJournal } from "@pm/db";
import { dryRunJournalCleanup } from "../apps/engine/src/journal-cleanup.js";
import { defaultRetentionConfig } from "../apps/engine/src/retention-manager.js";

const databaseUrl = process.env.STEP4_TEST_DATABASE_URL;
const migrations = fileURLToPath(new URL("../packages/db/migrations", import.meta.url));

function localPgUrl(): string {
  if (!databaseUrl) throw new Error("STEP4_TEST_DATABASE_URL_REQUIRED");
  const parsed = new URL(databaseUrl);
  // Integration tests must never receive a production Supabase URL.
  expect(["localhost", "127.0.0.1"]).toContain(parsed.hostname);
  return databaseUrl;
}

function event(type: string, id: string, occurredAt: Date) {
  return createJournalEvent({
    id,
    traceId: id,
    correlationId: id,
    type,
    aggregateType: "pr19-test",
    aggregateId: id,
    payload: { pr19DateBindingTest: true, id },
    occurredAt,
  });
}

describe("PR19 Postgres retention timestamp bindings", () => {
  it.skipIf(!databaseUrl)("PG17 readonly official dry-run returns mutation-free eligibility", { timeout: 30_000 }, async () => {
    const url = localPgUrl();
    const prefix = `pr19-dry-${Date.now()}`;
    const now = new Date("2040-01-03T00:00:00.000Z");
    const cutoff = new Date("2040-01-02T00:00:00.000Z");
    const writer = new PostgresEventJournal(url, migrations);
    await writer.initialize();
    await writer.append(event("TRIGGER_EVENT", `${prefix}-old`, new Date("2040-01-01T00:00:00.000Z")));
    await writer.append(event("TRIGGER_EVENT", `${prefix}-recent`, new Date("2040-01-02T12:00:00.000Z")));
    await writer.close();

    const reader = new PostgresEventJournal(url, migrations, { access: "readonly" });
    await reader.connectWithoutMigrate("readonly");
    try {
      const before = await reader.countByType();
      const expectedEligible = (await reader.queryEventsKeyset({ types: [...MONITOR_CONTROL_EVENT_TYPES], limit: 20_000 }))
        .filter((row) => new Date(row.occurredAt).getTime() < cutoff.getTime()).length;
      const dry = await dryRunJournalCleanup(reader, now, null, defaultRetentionConfig({
        enabled: true,
        telemetryTtlMs: 24 * 60 * 60_000,
        monitorTtlMs: 24 * 60 * 60_000,
        batchSize: 50,
      }));
      expect(dry.mode).toBe("dry-run");
      expect(dry.mutated).toBe(false);
      expect(dry.monitor.eligible).toBe(expectedEligible);
      expect(dry.monitor.cutoff).toBe(cutoff.toISOString());
      expect(await reader.countByType()).toEqual(before);
    } finally {
      await reader.close();
    }
  });

  it.skipIf(!databaseUrl)("PG17 bounded maintenance delete removes only eligible monitor rows", { timeout: 30_000 }, async () => {
    const url = localPgUrl();
    const prefix = `pr19-delete-${Date.now()}`;
    const oldId = `${prefix}-old`;
    const recentId = `${prefix}-recent`;
    const writer = new PostgresEventJournal(url, migrations);
    await writer.initialize();
    await writer.append(event("REFERENCE_PROBE_TRANSIENT", oldId, new Date("2040-02-01T00:00:00.000Z")));
    await writer.append(event("REFERENCE_PROBE_TRANSIENT", recentId, new Date("2040-02-02T12:00:00.000Z")));
    await writer.close();

    const maintenance = new PostgresEventJournal(url, migrations, { access: "maintenance" });
    await maintenance.connectWithoutMigrate("maintenance");
    try {
      const result = await maintenance.deleteJournalEventsBounded({
        types: ["REFERENCE_PROBE_TRANSIENT"],
        occurredBefore: new Date("2040-02-02T00:00:00.000Z"),
        limit: 1,
      });
      expect(result.deleted).toBe(1);
      const remaining = await maintenance.queryEventsKeyset({ types: ["REFERENCE_PROBE_TRANSIENT"], limit: 20_000 });
      expect(remaining.some((row) => row.id === oldId)).toBe(false);
      expect(remaining.some((row) => row.id === recentId)).toBe(true);
    } finally {
      await maintenance.close();
    }
  });

  it.skipIf(!databaseUrl)("PG17 MCP idempotency pruning binds Date cutoffs safely", { timeout: 30_000 }, async () => {
    const url = localPgUrl();
    const prefix = `pr19-idempotency-${Date.now()}`;
    const writer = new PostgresEventJournal(url, migrations);
    await writer.initialize();
    try {
      const client = (writer as unknown as { client: { unsafe: (query: string, values?: unknown[]) => Promise<unknown> } }).client;
      await client.unsafe(
        "INSERT INTO mcp_idempotency_keys (scope, key, created_at) VALUES ($1, $2, $3::timestamptz), ($1, $4, $5::timestamptz) ON CONFLICT (scope, key) DO NOTHING",
        [prefix, "old", "2040-03-01T00:00:00.000Z", "recent", "2040-03-02T12:00:00.000Z"],
      );
      const maintenance = new PostgresEventJournal(url, migrations, { access: "maintenance" });
      await maintenance.connectWithoutMigrate("maintenance");
      try {
        const result = await maintenance.pruneIdempotencyKeys(new Date("2040-03-02T00:00:00.000Z"), 10);
        expect(result.deleted).toBe(1);
        expect(await maintenance.idempotencyCreatedAt(prefix, "old")).toBeNull();
        expect(await maintenance.idempotencyCreatedAt(prefix, "recent")).toBe("2040-03-02T12:00:00.000Z");
      } finally {
        await maintenance.close();
      }
    } finally {
      await writer.close();
    }
  });
});
