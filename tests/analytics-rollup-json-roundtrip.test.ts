import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { canonicalJson, MemoryEventJournal } from "@pm/db";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";
import {
  hashAnalyticsRollup,
  loadVerifiedAnalyticsRollup,
  persistVerifiedAnalyticsRollup,
} from "../apps/engine/src/final-report.js";

function emptySnapshot(experimentId = "exp-rollup"): Record<string, unknown> {
  return new ExperimentSummaryAccumulator(experimentId, {
    reportKind: "interim",
    metricsScope: "rolling",
    status: "RUNNING",
    complete: false,
  }).serialize() as unknown as Record<string, unknown>;
}

function legacyV1Hash(storedAccumulator: Record<string, unknown>): string {
  // Exact historical shape: serialize() emitted lastLedger: undefined before
  // Postgres jsonb dropped the property on write.
  return createHash("sha256")
    .update(canonicalJson({ ...storedAccumulator, lastLedger: undefined }))
    .digest("hex");
}

describe("analytics rollup JSONB hash compatibility", () => {
  it("hashes the JSON storage representation so restart round-trips are stable", () => {
    const inMemory = emptySnapshot();
    expect(Object.prototype.hasOwnProperty.call(inMemory, "lastLedger")).toBe(true);
    expect(inMemory.lastLedger).toBeUndefined();

    const stored = JSON.parse(JSON.stringify(inMemory)) as Record<string, unknown>;
    expect(Object.prototype.hasOwnProperty.call(stored, "lastLedger")).toBe(false);

    expect(hashAnalyticsRollup(inMemory)).toBe(hashAnalyticsRollup(stored));
  });

  it("accepts only the known schema-v1 missing-lastLedger legacy hash", async () => {
    const journal = new MemoryEventJournal();
    const stored = JSON.parse(JSON.stringify(emptySnapshot())) as Record<string, unknown>;
    const contentHash = legacyV1Hash(stored);

    await journal.persistAnalyticsRollup!({
      experimentId: "exp-rollup",
      rollupKind: "canonical",
      schemaVersion: 1,
      sourceCursor: 1,
      sourceFenceId: null,
      sourceFenceOccurredAt: null,
      sourceEventCount: 0,
      sourceCommitSeq: 1,
      sourceRecordedAt: null,
      stableRead: true,
      previousContentHash: null,
      accumulator: stored,
      contentHash,
      verified: true,
      createdAt: "2026-09-14T00:00:00.000Z",
    });

    const loaded = await loadVerifiedAnalyticsRollup(journal, "exp-rollup");
    expect(loaded?.contentHash).toBe(contentHash);

    const otherStored = JSON.parse(JSON.stringify(emptySnapshot("exp-other"))) as Record<string, unknown>;
    const unrelatedLegacyHash = createHash("sha256")
      .update(canonicalJson({ ...otherStored, someUnknownLegacyField: undefined }))
      .digest("hex");
    const other = new MemoryEventJournal();
    await other.persistAnalyticsRollup!({
      experimentId: "exp-other",
      rollupKind: "canonical",
      schemaVersion: 1,
      sourceCursor: 1,
      sourceFenceId: null,
      sourceFenceOccurredAt: null,
      sourceEventCount: 0,
      sourceCommitSeq: 1,
      sourceRecordedAt: null,
      stableRead: true,
      previousContentHash: null,
      accumulator: otherStored,
      contentHash: unrelatedLegacyHash,
      verified: true,
      createdAt: "2026-09-14T00:00:00.000Z",
    });
    expect(await loadVerifiedAnalyticsRollup(other, "exp-other")).toBeNull();
  });

  it("advances a recovered legacy prefix using the old hash as the prefix fence", async () => {
    const journal = new MemoryEventJournal();
    const stored = JSON.parse(JSON.stringify(emptySnapshot())) as Record<string, unknown>;
    const legacyHash = legacyV1Hash(stored);

    await journal.persistAnalyticsRollup!({
      experimentId: "exp-rollup",
      rollupKind: "canonical",
      schemaVersion: 1,
      sourceCursor: 1,
      sourceFenceId: null,
      sourceFenceOccurredAt: null,
      sourceEventCount: 0,
      sourceCommitSeq: 1,
      sourceRecordedAt: null,
      stableRead: true,
      previousContentHash: null,
      accumulator: stored,
      contentHash: legacyHash,
      verified: true,
      createdAt: "2026-09-14T00:00:00.000Z",
    });

    const loaded = await loadVerifiedAnalyticsRollup(journal, "exp-rollup");
    expect(loaded?.contentHash).toBe(legacyHash);

    const nextAccumulator = { ...stored, eventCount: 1 };
    const advanced = await persistVerifiedAnalyticsRollup(journal, {
      experimentId: "exp-rollup",
      sourceCursor: 2,
      sourceEventCount: 0,
      sourceCommitSeq: 2,
      stableRead: true,
      sourceCoverageComplete: true,
      baseContentHash: loaded!.contentHash,
      accumulator: nextAccumulator,
    });

    expect(advanced?.previousContentHash).toBe(legacyHash);
    expect(advanced?.contentHash).toBe(hashAnalyticsRollup(nextAccumulator));
    expect(advanced?.contentHash).not.toBe(legacyHash);
    expect((await loadVerifiedAnalyticsRollup(journal, "exp-rollup"))?.contentHash).toBe(advanced?.contentHash);
  });
});
