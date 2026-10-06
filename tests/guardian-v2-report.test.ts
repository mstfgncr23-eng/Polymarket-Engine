import { expect, it } from "vitest";
import { createJournalEvent } from "@pm/db";
import { ExperimentSummaryAccumulator } from "../apps/engine/src/report.js";

it("reports EARLY/LATE SELL or BUY exits separately from paired/naked settlement through JSON rollup", () => {
  const acc = new ExperimentSummaryAccumulator("guardian");
  for (const [type, payload] of [
    ["GUARDIAN_EXECUTION_COMPLETED", { sensor: "EARLY", selection: "PROTECTIVE_DE_RISK" }],
    ["GUARDIAN_EXECUTION_COMPLETED", { sensor: "LATE", selection: "RESCUE_SECOND_LEG" }],
    ["POSITION_SETTLED", { upQuantity: "5", downQuantity: "5" }],
    ["POSITION_SETTLED", { upQuantity: "5", downQuantity: "0" }],
  ] as const) acc.fold(createJournalEvent({ type, traceId: "test", aggregateType: "position", aggregateId: "p", payload }));
  expect(acc.finalize().guardianOutcomes).toEqual({ earlyExits: 1, lateExits: 1, pairedSettlements: 1, nakedSettlements: 1 });
  const restored = ExperimentSummaryAccumulator.fromSnapshot(JSON.parse(JSON.stringify(acc.serialize())));
  expect(restored.finalize().guardianOutcomes).toEqual(acc.finalize().guardianOutcomes);
});
