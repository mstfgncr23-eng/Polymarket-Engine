import { describe, expect, it } from "vitest";
import { paperFill, startQualifiedEngine } from "./paper-engine-testkit.js";

describe("beginExperiment economic residue gate", () => {
  it("refuses to start when leftover paper positions remain from an IDLE ledger", async () => {
    const { engine } = await startQualifiedEngine();
    try {
      expect(engine.experimentManifest()?.status === "RUNNING").toBe(false);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
      expect(engine.ledger.positions.size).toBe(1);

      const result = await engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(expect.arrayContaining([
        "OPEN_POSITIONS_REMAIN",
        "UNRESOLVED_ECONOMIC_OBLIGATIONS",
      ]));
      expect(engine.experimentManifest()?.status).not.toBe("RUNNING");
    } finally {
      await engine.stop();
    }
  });

  it("refuses to start when the journal is degraded", async () => {
    const { engine } = await startQualifiedEngine();
    try {
      engine.state.journalDegraded = true;

      const result = await engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(expect.arrayContaining(["JOURNAL_NOT_HEALTHY"]));
      expect(engine.experimentManifest()?.status).not.toBe("RUNNING");
    } finally {
      await engine.stop();
    }
  });

  it("still starts a clean IDLE engine with no leftover exposure", async () => {
    const { engine } = await startQualifiedEngine();
    try {
      expect(engine.ledger.positions.size).toBe(0);
      const result = await engine.beginExperiment();
      expect(result.ok).toBe(true);
      expect(engine.experimentManifest()?.status).toBe("RUNNING");
    } finally {
      await engine.stop();
    }
  });
});
