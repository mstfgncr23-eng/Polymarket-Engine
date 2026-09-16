import { readFile, writeFile } from "node:fs/promises";

const path = new URL("../apps/engine/src/engine.ts", import.meta.url);
let source = await readFile(path, "utf8");

function replaceOnce(label, needle, replacement) {
  const count = source.split(needle).length - 1;
  if (count !== 1) throw new Error(`${label}: expected exactly 1 match, found ${count}`);
  source = source.replace(needle, replacement);
}

replaceOnce(
  "beginExperiment residue gate",
  `      if (!this.state.newPaperEntryAllowed() || this.overload.state === "FAIL_CLOSED") {\n        const preflight = this.preflight("48H_NATIVE_PAPER_EXPERIMENT");\n        return { ...preflight, ok: false, blockers: [...new Set([...preflight.blockers, "OVERLOAD_FAIL_CLOSED"])] };\n      }`,
  `      const startBlockers = [\n        ...(this.state.journalDegraded || this.overload.hasIntegrityFault ? ["JOURNAL_NOT_HEALTHY"] : []),\n        ...this.experimentEconomicResidueBlockers(),\n      ];\n      const entryBlocked = !this.state.newPaperEntryAllowed() || this.overload.state === "FAIL_CLOSED";\n      if (entryBlocked || startBlockers.length > 0) {\n        const preflight = this.preflight("48H_NATIVE_PAPER_EXPERIMENT");\n        return {\n          ...preflight,\n          ok: false,\n          blockers: [...new Set([\n            ...preflight.blockers,\n            ...startBlockers,\n            ...(entryBlocked ? ["OVERLOAD_FAIL_CLOSED"] : []),\n          ])],\n        };\n      }`,
);

replaceOnce(
  "economic residue helper",
  `  private obligationMarketIds(): Set<string> {\n    const ids = this.openPositionMarketIds();\n    for (const resting of this.restingOrders.values()) {\n      if (d(resting.remaining).gt(0)) ids.add(resting.order.marketId);\n    }\n    return ids;\n  }`,
  `  private obligationMarketIds(): Set<string> {\n    const ids = this.openPositionMarketIds();\n    for (const resting of this.restingOrders.values()) {\n      if (d(resting.remaining).gt(0)) ids.add(resting.order.marketId);\n    }\n    return ids;\n  }\n\n  private experimentEconomicResidueBlockers(): string[] {\n    const ledger = this.ledger.snapshot();\n    const openPositions = [...this.ledger.positions.values()].filter((position) => d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0));\n    const restingOrders = [...this.restingOrders.values()].filter((order) => d(order.remaining).gt(0));\n    const makerReservations = [...this.makerReservations.entries()].filter(([, amount]) => d(amount).gt(0));\n    const blockers: string[] = [];\n    if (openPositions.length > 0 || this.ledger.positions.size > 0) blockers.push("OPEN_POSITIONS_REMAIN");\n    if (restingOrders.length > 0 || this.restingOrders.size > 0) blockers.push("RESTING_ORDERS_REMAIN");\n    if (makerReservations.length > 0 || this.makerReservations.size > 0) blockers.push("MAKER_RESERVATIONS_REMAIN");\n    if (!d(ledger.reservedCash).eq(0)) blockers.push("RESERVED_CASH_REMAIN");\n    if (this.obligationMarketIds().size > 0) blockers.push("UNRESOLVED_ECONOMIC_OBLIGATIONS");\n    return blockers;\n  }`,
);

await writeFile(path, source, "utf8");
console.log("backported beginExperiment economic-residue and journal-health gate");
