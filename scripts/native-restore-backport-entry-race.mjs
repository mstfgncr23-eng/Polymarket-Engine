import { readFile, writeFile } from "node:fs/promises";

const path = new URL("../apps/engine/src/engine.ts", import.meta.url);
let source = await readFile(path, "utf8");

function replaceOnce(label, needle, replacement) {
  const count = source.split(needle).length - 1;
  if (count !== 1) throw new Error(`${label}: expected exactly 1 match, found ${count}`);
  source = source.replace(needle, replacement);
}

replaceOnce(
  "native trigger boundary gate",
  `    const nativeBookkeepingStarted = performance.now();\n    this.activation.ensureLatency(trigger);\n    this.state.setActivationLatency(this.activation.snapshot());\n    await this.persistReportInput(createJournalEvent({\n      traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,\n      payload: {\n        triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null,\n        reason: "NATIVE_DETERMINISTIC_ONLY", activate: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,\n      },\n    }));\n    this.state.addActivation({\n      triggerId: trigger.id, reason: "NATIVE_DETERMINISTIC_ONLY", at: this.nowFn().toISOString(),\n      provider: "native_deterministic", grokWakeObserved: false,\n    });\n    this.metrics.idleActivationBookkeepingDuration.observe((performance.now() - nativeBookkeepingStarted) / 1000);\n    this.record("NATIVE_DECISION_EVALUATION", trigger.traceId, trigger.marketId, {\n      authority: NATIVE_DETERMINISTIC_AUTHORITY, triggerId: trigger.id, expiresAt: trigger.expiresAt,\n    }, trigger.asset ?? null);\n    await this.evaluateNativeDecisionForTrigger(trigger, provenance);`,
  `    await this.experimentBoundaryGate.run(async () => {\n      const nativeBookkeepingStarted = performance.now();\n      this.activation.ensureLatency(trigger);\n      this.state.setActivationLatency(this.activation.snapshot());\n      await this.persistReportInput(createJournalEvent({\n        traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,\n        payload: {\n          triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null,\n          reason: "NATIVE_DETERMINISTIC_ONLY", activate: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,\n        },\n      }));\n      this.state.addActivation({\n        triggerId: trigger.id, reason: "NATIVE_DETERMINISTIC_ONLY", at: this.nowFn().toISOString(),\n        provider: "native_deterministic", grokWakeObserved: false,\n      });\n      this.metrics.idleActivationBookkeepingDuration.observe((performance.now() - nativeBookkeepingStarted) / 1000);\n      this.record("NATIVE_DECISION_EVALUATION", trigger.traceId, trigger.marketId, {\n        authority: NATIVE_DETERMINISTIC_AUTHORITY, triggerId: trigger.id, expiresAt: trigger.expiresAt,\n      }, trigger.asset ?? null);\n      await this.evaluateNativeDecisionForTrigger(trigger, provenance);\n    });`,
);

replaceOnce(
  "entry helper insertion",
  `  /**\n   * Evaluate one trigger from the current trusted pair. The Engine owns the\n   * synchronization, epoch, provenance, lifecycle, and persistence gates;\n   * the core model only receives the already-admitted inputs.\n   */`,
  `  /**\n   * The entry deadline is an economic admission fence, not merely a\n   * maintenance hint. Callers use this while holding experimentBoundaryGate.\n   */\n  private async ensureFirstLegEntryOpen(): Promise<boolean> {\n    if (experimentAllowsFirstLeg(this.experiment, this.nowFn())) return true;\n    const current = this.experiment;\n    const now = this.nowFn();\n    if (current?.status === "RUNNING" && current.entryClosesAt && now.getTime() >= new Date(current.entryClosesAt).getTime()) {\n      await this.commitExperimentStatus({ ...current, status: "ENTRY_CLOSED", completedAt: null, markToMarketAt: null });\n    }\n    return false;\n  }\n\n  /**\n   * Evaluate one trigger from the current trusted pair. The Engine owns the\n   * synchronization, epoch, provenance, lifecycle, and persistence gates;\n   * the core model only receives the already-admitted inputs.\n   */`,
);

replaceOnce(
  "last economic admission fence",
  `  private async submitPaperOrder(order: PaperOrder, decisionSnapshot: MarketSnapshot, duplicateKey: string, intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG"): Promise<void> {\n    if (intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return;\n    this.metrics.paperOrders.inc({ type: order.orderType });`,
  `  private async submitPaperOrder(order: PaperOrder, decisionSnapshot: MarketSnapshot, duplicateKey: string, intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG"): Promise<void> {\n    if (intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return;\n    // Last economic admission fence immediately before PAPER_ORDER_CREATED.\n    // close-entries and the native decision path are serialized by the same\n    // experimentBoundaryGate, while deadline expiry closes durably here.\n    if (intent === "OPEN_FIRST_LEG" && !(await this.ensureFirstLegEntryOpen())) return;\n    this.metrics.paperOrders.inc({ type: order.orderType });`,
);

replaceOnce(
  "maintenance close serialization",
  `    if (previous === "RUNNING" && (next.status === "ENTRY_CLOSED" || next.status === "COMPLETED")) {\n      const closed: ExperimentManifest = {\n        ...next, status: "ENTRY_CLOSED", completedAt: null, markToMarketAt: null,\n      };\n      if (this.experiment.status !== "ENTRY_CLOSED") await this.commitExperimentStatus(closed);\n    }\n    if (next.status === "COMPLETED" && this.experiment?.status !== "COMPLETED") {\n      await this.commitExperimentStatus(next);\n    } else if (next.status === "ENTRY_CLOSED" && this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") {\n      await this.commitExperimentStatus(next);\n    }`,
  `    if (previous === "RUNNING" && (next.status === "ENTRY_CLOSED" || next.status === "COMPLETED")) {\n      const closed: ExperimentManifest = {\n        ...next, status: "ENTRY_CLOSED", completedAt: null, markToMarketAt: null,\n      };\n      await this.experimentBoundaryGate.run(async () => {\n        if (this.experiment?.status === "RUNNING") await this.commitExperimentStatus(closed);\n      });\n    }\n    if (next.status === "COMPLETED" && this.experiment?.status !== "COMPLETED") {\n      await this.experimentBoundaryGate.run(async () => {\n        if (this.experiment?.status !== "COMPLETED") await this.commitExperimentStatus(next);\n      });\n    } else if (next.status === "ENTRY_CLOSED" && this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") {\n      await this.experimentBoundaryGate.run(async () => {\n        if (this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") await this.commitExperimentStatus(next);\n      });\n    }`,
);

replaceOnce(
  "test completion serialization",
  `  async completeExperimentForTest(): Promise<void> {\n    if (!this.experiment) throw new Error("NO_EXPERIMENT");\n    await this.commitExperimentStatus({\n      ...this.experiment,\n      status: "COMPLETED",\n      completedAt: this.nowFn().toISOString(),\n    });\n  }`,
  `  async completeExperimentForTest(): Promise<void> {\n    await this.experimentBoundaryGate.run(async () => {\n      if (!this.experiment) throw new Error("NO_EXPERIMENT");\n      await this.commitExperimentStatus({\n        ...this.experiment,\n        status: "COMPLETED",\n        completedAt: this.nowFn().toISOString(),\n      });\n    });\n  }`,
);

await writeFile(path, source, "utf8");
console.log("backported native close-vs-entry economic admission fence");
