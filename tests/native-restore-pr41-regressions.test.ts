import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { experimentAllowsFirstLeg } from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { exportExperimentArchive } from "../apps/engine/src/archive.js";

describe("native restore PR41 independent regressions", () => {
  it("uses a strict, time-aware first-leg deadline and rejects missing deadlines", () => {
    const deadline = new Date("2026-09-13T01:00:00.000Z");
    const manifest = {
      status: "RUNNING" as const,
      startValidationPending: false,
      entryClosesAt: deadline.toISOString(),
    } as never;

    expect(experimentAllowsFirstLeg(manifest, new Date(deadline.getTime() - 1))).toBe(true);
    expect(experimentAllowsFirstLeg(manifest, deadline)).toBe(false);
    expect(experimentAllowsFirstLeg(manifest, new Date(deadline.getTime() + 1))).toBe(false);
    expect(experimentAllowsFirstLeg({ status: "RUNNING", startValidationPending: false } as never, new Date(deadline.getTime() - 1))).toBe(false);
    expect(experimentAllowsFirstLeg({ ...manifest, startValidationPending: true } as never, new Date(deadline.getTime() - 1))).toBe(false);
  });

  it("exports a running experiment as a non-empty interim archive using the durable start time", async () => {
    const journal = new MemoryEventJournal();
    const experimentId = "exp-native-restore";
    const startedAt = "2026-01-01T00:00:00.000Z";

    await journal.commitDurableTransition({
      operationKey: `experiment:${experimentId}:started`,
      type: "EXPERIMENT_STARTED",
      aggregateType: "experiment",
      aggregateId: experimentId,
      payload: {
        experiment: {
          id: experimentId,
          status: "RUNNING",
          startedAt,
          completedAt: null,
          strategyVersion: "temporal-pair-hedge-v1",
          riskConfiguration: {},
          modelConfiguration: { authority: "NATIVE_DETERMINISTIC_V1" },
          systemVersion: "test",
          gitSha: "test",
        },
      },
      occurredAt: new Date(startedAt),
    }, 0);

    await journal.append(createJournalEvent({
      traceId: "running-archive-trace",
      type: "RISK_EVALUATION",
      aggregateType: "risk",
      aggregateId: "running-archive-risk",
      occurredAt: new Date("2026-01-01T00:00:01.000Z"),
      payload: { experimentId, accepted: false, marketId: "m1" },
    }));

    const directory = await mkdtemp(join(tmpdir(), "native-restore-running-archive-"));
    const result = await exportExperimentArchive(journal, { experimentId, directory });
    const manifest = JSON.parse(await readFile(result.manifestPath, "utf8")) as Record<string, unknown>;
    const summary = JSON.parse(await readFile(result.summaryPath, "utf8")) as Record<string, unknown>;

    expect(manifest.experimentStart).toBe(startedAt);
    expect(manifest.experimentStatus).toBe("RUNNING");
    expect(Number(manifest.sourceEventCount)).toBeGreaterThan(0);
    expect(summary.reportKind).toBe("interim");
    expect(summary.metricsScope).toBe("rolling");
    expect(summary.complete).toBe(false);
    expect(summary.truncated).toBe(true);
  });
});
