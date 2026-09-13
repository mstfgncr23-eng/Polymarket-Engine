from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


engine_path = Path("apps/engine/src/engine.ts")
engine = engine_path.read_text(encoding="utf-8")

engine = replace_once(
    engine,
    '''  /** Keeps ingress out of the transient post-commit/start-validation window. */\n  private experimentStartPending = false;\n  private checkpointSequence = 0;''',
    '''  /** Keeps ingress out of the transient post-commit/start-validation window. */\n  private experimentStartPending = false;\n  /** Freezes new trigger admission while prepare-next proves a stable drained boundary. */\n  private experimentPreparePending = false;\n  private checkpointSequence = 0;''',
    "prepare-pending field",
)

prepare_start = engine.index("  async prepareNextExperiment(): Promise<Record<string, unknown>> {")
prefix, prepare_tail = engine[:prepare_start], engine[prepare_start:]
prepare_tail = replace_once(
    prepare_tail,
    '''      if (this.state.journalDegraded || this.overload.state === "FAIL_CLOSED" || this.overload.hasIntegrityFault) {\n        return { ok: false, blockers: ["JOURNAL_NOT_HEALTHY"], experiment: current };\n      }\n      if (this.supervisor.snapshot().unresolved > 0) {\n        return { ok: false, blockers: ["RUNTIME_WORK_UNRESOLVED"], experiment: current };\n      }''',
    '''      if (this.state.journalDegraded || this.overload.state === "FAIL_CLOSED" || this.overload.hasIntegrityFault) {\n        return { ok: false, blockers: ["JOURNAL_NOT_HEALTHY"], experiment: current };\n      }\n      this.experimentPreparePending = true;\n      try {\n        // Freeze new trigger admission before taking the drain snapshot. Once\n        // unresolved reaches zero it cannot increase again until this prepare\n        // attempt exits, so no old-run trigger can cross the durable boundary.\n        if (this.supervisor.snapshot().unresolved > 0) {\n          return { ok: false, blockers: ["RUNTIME_WORK_UNRESOLVED"], experiment: current };\n        }''',
    "stable prepare drain fence",
)
prepare_tail = replace_once(
    prepare_tail,
    '''      return { ok: true, experiment: this.experiment, checkpoint: "ok" };\n    });\n  }''',
    '''        return { ok: true, experiment: this.experiment, checkpoint: "ok" };\n      } finally {\n        this.experimentPreparePending = false;\n      }\n    });\n  }''',
    "prepare fence release",
)
engine = prefix + prepare_tail

engine = replace_once(
    engine,
    '''  private admitTrigger(trigger: TriggerEvent, provenance?: TriggerProvenance): AdmitResult {\n    if (this.started && !this.state.recoveryComplete) {\n      return { admitted: false, reason: "fail_closed", done: Promise.resolve() };\n    }\n    if (this.sourceSealBlocksAdmission()) {\n      return { admitted: false, reason: "source_sealed", done: Promise.resolve() };\n    }\n    const resolved = provenance ?? this.captureProvenance(trigger);\n    const lane = triggerWorkLane(trigger.kind);''',
    '''  private admitTrigger(trigger: TriggerEvent, provenance?: TriggerProvenance): AdmitResult {\n    if (this.started && !this.state.recoveryComplete) {\n      return { admitted: false, reason: "fail_closed", done: Promise.resolve() };\n    }\n    const lane = triggerWorkLane(trigger.kind);\n    if (this.experimentPreparePending) {\n      this.metrics.runtimeWorkRejected.inc({ lane, reason: "admission_closed" });\n      return { admitted: false, reason: "admission_closed", done: Promise.resolve() };\n    }\n    if (this.sourceSealBlocksAdmission()) {\n      return { admitted: false, reason: "source_sealed", done: Promise.resolve() };\n    }\n    const resolved = provenance ?? this.captureProvenance(trigger);''',
    "prepare trigger admission fence",
)

engine = replace_once(
    engine,
    '''    if (this.processTriggerHold) await this.processTriggerHold;\n    if (signal?.aborted) return;\n    // A trigger emitted while a start is pending cannot begin an active''',
    '''    if (this.processTriggerHold) await this.processTriggerHold;\n    if (signal?.aborted) return;\n    if (runGeneration !== this.experimentRunGeneration) return;\n    // A trigger emitted while a start is pending cannot begin an active''',
    "post-hold generation fence",
)

engine = replace_once(
    engine,
    '''    await this.persistReportInput(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }), signal);\n    this.rememberProvenance(trigger.id, provenance);''',
    '''    await this.persistReportInput(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }), signal);\n    if (signal?.aborted) return;\n    if (runGeneration !== this.experimentRunGeneration) return;\n    this.rememberProvenance(trigger.id, provenance);''',
    "post-persist generation fence",
)
engine_path.write_text(engine, encoding="utf-8")

tests_path = Path("tests/operator-prepare-next.test.ts")
tests = tests_path.read_text(encoding="utf-8")
marker = '  it("unconditionally fences an old-generation P0 trigger across preparation while preserving same-run hedging", async () => {'
start = tests.index(marker)
closing = tests.rfind("\n  });\n});")
if closing < start:
    raise SystemExit("could not locate final prepare-next test boundary")
closing += len("\n  });")

replacement = r'''  it("freezes new P0 admission after the prepare drain point and preserves same-run hedging", async () => {
    const test = await setup();
    let releaseReportLoad!: () => void;
    let reportLoadStarted!: () => void;
    const reportLoadGate = new Promise<void>((resolve) => { releaseReportLoad = resolve; });
    const reportLoadStartedPromise = new Promise<void>((resolve) => { reportLoadStarted = resolve; });
    const originalLoadReport = test.journal.loadExperimentReport.bind(test.journal);
    let heldReportLoad = false;
    let stalePending: Promise<void> | undefined;
    const stale = p0Trigger("stale-after-drain");
    const sameRun = p0Trigger("same-run");

    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({
        marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP", asset: "BTC",
        receivedAt: new Date(START_MS).toISOString(), sourceTimestamp: new Date(START_MS).toISOString(),
        asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }],
      });
      const down = snapshot({
        marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", asset: "BTC",
        receivedAt: new Date(START_MS).toISOString(), sourceTimestamp: new Date(START_MS).toISOString(),
        asks: [{ price: "0.51", quantity: "300" }], bids: [{ price: "0.50", quantity: "300" }],
      });
      await test.market.emitSnapshot(up);
      await test.market.emitSnapshot(down);
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().unresolved === 0);
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();
      const previous = test.engine.experimentManifest()!;

      test.journal.loadExperimentReport = async (experimentId, reportKind = "final") => {
        if (!heldReportLoad) {
          heldReportLoad = true;
          reportLoadStarted();
          await reportLoadGate;
        }
        return originalLoadReport(experimentId, reportKind);
      };

      const preparePromise = test.engine.prepareNextExperiment();
      await reportLoadStartedPromise;

      // prepare-next has frozen trigger admission and already observed a stable
      // zero-unresolved supervisor snapshot. A P0 arriving now must never enter
      // the supervisor, so there is no await window that can cross the boundary.
      stalePending = test.engine.registerTriggerForResearch(stale);
      await stalePending;
      expect(test.engine.runtimeSupervisorSnapshot().unresolved).toBe(0);
      expect(await test.journal.readTrace(stale.traceId)).toEqual([]);

      releaseReportLoad();
      expect((await preparePromise).ok).toBe(true);
      const next = test.engine.experimentManifest()!;
      expect(next.id).not.toBe(previous.id);
      expect(next.status).toBe("IDLE");
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      expect(test.engine.experimentManifest()?.status).toBe("RUNNING");

      await test.engine.applyObservedFill({
        ...fill("UP", "0.40", "10"), id: "fresh-run-first-leg", orderId: "fresh-run-first-leg-order",
        marketId: "btc-mkt", tokenId: "btc-up", timestamp: new Date(START_MS).toISOString(),
      });
      const beforeSameRun = test.engine.ledger.snapshot();
      expect(await test.journal.readTrace(stale.traceId)).toEqual([]);
      expect(test.engine.ledger.snapshot()).toEqual(beforeSameRun);

      // The fence is scoped only to prepare-next. Once the boundary has
      // completed, current-generation deterministic hedging remains immediate
      // and does not wait for Astra.
      await test.engine.registerTriggerForResearch(sameRun);
      const sameRunEvents = await test.journal.readTrace(sameRun.traceId);
      expect(sameRunEvents.some((event) => event.type === "ASTRA_DECISION" && event.payload.bypass === "DETERMINISTIC_HEDGE")).toBe(true);
      expect(sameRunEvents.some((event) => event.type === "RISK_EVALUATION")).toBe(true);
    } finally {
      releaseReportLoad();
      await stalePending?.catch(() => undefined);
      test.journal.loadExperimentReport = originalLoadReport;
      await test.engine.stop();
    }
  });

  it("blocks prepare on trigger work admitted before the freeze, then succeeds after that work drains", async () => {
    const test = await setup();
    let releaseTrigger!: () => void;
    const triggerHold = new Promise<void>((resolve) => { releaseTrigger = resolve; });
    let pending: Promise<void> | undefined;

    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();

      test.engine.setProcessTriggerHoldForTest(triggerHold);
      pending = test.engine.registerTriggerForResearch(p0Trigger("pre-freeze-inflight"));
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().lanes.p0.inFlight === 1);

      const blocked = await test.engine.prepareNextExperiment();
      expect(blocked).toMatchObject({ ok: false, blockers: ["RUNTIME_WORK_UNRESOLVED"] });
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(0);

      test.engine.setProcessTriggerHoldForTest(null);
      releaseTrigger();
      await pending;
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().unresolved === 0);

      const prepared = await test.engine.prepareNextExperiment();
      expect(prepared).toMatchObject({ ok: true, experiment: { status: "IDLE" } });
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(1);
    } finally {
      test.engine.setProcessTriggerHoldForTest(null);
      releaseTrigger();
      await pending?.catch(() => undefined);
      await test.engine.stop();
    }
  });'''

tests = tests[:start] + replacement + tests[closing:]
tests_path.write_text(tests, encoding="utf-8")
