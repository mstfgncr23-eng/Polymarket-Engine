import { describe, expect, it } from "vitest";
import { paperFill, startQualifiedEngine } from "./paper-engine-testkit.js";

function installCompletedExperiment(engine: Awaited<ReturnType<typeof startQualifiedEngine>>["engine"], id: string) {
  const current = engine.experimentManifest();
  if (!current) throw new Error("missing experiment manifest");
  const at = new Date().toISOString();
  const completed = {
    ...current,
    id,
    status: "COMPLETED" as const,
    startedAt: at,
    entryClosesAt: at,
    completedAt: at,
    startValidationPending: false,
  };

  (engine as unknown as { experiment: typeof completed }).experiment = completed;
  engine.state.experimentId = id;
  engine.state.experimentStatus = "COMPLETED";

  return completed;
}

describe("repeat experiment lifecycle", () => {
  it("keeps a completed experiment closed until its canonical final report exists", async () => {
    const { engine, journal } = await startQualifiedEngine();

    try {
      const previous = installCompletedExperiment(engine, "experiment-previous");

      (journal as unknown as {
        loadExperimentReport: (id: string, kind?: "final") => Promise<unknown>;
      }).loadExperimentReport = async () => null;

      const result = await engine.beginExperiment();

      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(["PREVIOUS_EXPERIMENT_REPORT_NOT_FINALIZED"]);
      expect(engine.experimentManifest()?.id).toBe(previous.id);
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
    } finally {
      await engine.stop();
    }
  });

  it("starts a new clean experiment after a finalized completed experiment", async () => {
    const { engine, journal } = await startQualifiedEngine();

    try {
      const previous = installCompletedExperiment(engine, "experiment-previous");

      engine.ledger.applyFill(paperFill("btc-mkt", "UP", "0.40"));
      engine.ledger.settlePosition("btc-mkt", "0", "1");

      const before = engine.ledger.snapshot();
      expect(engine.ledger.positions.size).toBe(0);
      expect(before.cash).not.toBe(before.startingBalance);
      expect(before.realizedPnl).not.toBe("0");

      (journal as unknown as {
        loadExperimentReport: (id: string, kind?: "final") => Promise<unknown>;
      }).loadExperimentReport = async (id: string) => (
        id === previous.id
          ? {
              experimentId: previous.id,
              reportKind: "final",
              complete: true,
              truncated: false,
            }
          : null
      );

      const result = await engine.beginExperiment();

      expect(result.ok).toBe(true);

      const next = engine.experimentManifest();
      expect(next?.status).toBe("RUNNING");
      expect(next?.id).not.toBe(previous.id);

      const after = engine.ledger.snapshot();
      expect(engine.ledger.positions.size).toBe(0);
      expect(after.cash).toBe(after.startingBalance);
      expect(after.reservedCash).toBe("0");
      expect(after.realizedPnl).toBe("0");
      expect(after.unrealizedPnl).toBe("0");
      expect(after.grossPnl).toBe("0");
      expect(after.fees).toBe("0");
      expect(after.slippage).toBe("0");
      expect(after.drawdown).toBe("0");
    } finally {
      await engine.stop();
    }
  });
});
