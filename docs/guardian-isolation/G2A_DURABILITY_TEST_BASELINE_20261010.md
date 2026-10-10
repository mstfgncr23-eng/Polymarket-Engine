# G2A: PAPER Test Baseline Durability and Restart Forensics

Date: 2026-10-10
Branch: refactor/guardian-isolation-atomic
Scope: TEST HARNESS ONLY. No runtime implementation, no process launch, no shadow mode.

## Authority, safeguards and real project precedents

- Engine runtime examined: `apps/engine/src/engine.ts` on verified forensic image source lineage. Snapshot path calls `observeGuardianSnapshot` and schedules **asynchronous** Guardian durable mutations. `guardianRuntimeStates` is advanced before `reversalProtectionStates` receives the committed projection.
- Reference architecture: Hummingbot ControllerBase's `control_task` emits `ExecutorAction` batches to an `actions_queue`, while NautilusTrader differentiates RiskEngine and ExecutionEngine command paths. This is evidence for separating intent and execution responsibility, NOT proof they provide per-process isolation or that copying either fits this product.
- Operator explicitly forbids SHADOW or any simultaneous second live Guardian. All tests here are scripted local PAPER fixtures and a single local `EngineService` per test. No VPS / real market connectivity, wallet, LIVE, deploy, or Git push.

## Baseline reproduction (unchanged G1 runtime)

Before modifications, the two existing integration test files yielded **27 failed / 12 passed (39 tests)**. G1 source export rollback also produced 27/12, proving G1 did not introduce those failures.

### Root cause A: asynchronous Guardian projection not awaited (test harness)

- In an EARLY test, after the sequence t=0s ratio .75, t=5s .70 and t=10s .70:
  - Private in-memory `guardianRuntimeStates`: **EARLY_RECOVERY_WATCH**, revision **3**, armedAt t=10s.
  - Authoritative in-memory durable projection `reversalProtectionStates`: **OBSERVING**, revision **1**.
  - This was observed directly at the failed assertion, with position active and no engine errors.
- `ScriptedMarketAdapter.emitSnapshot` awaited the market callback but the callback intentionally does **not** await Guardian persistence. The test was asserting the durable projection too early.
- Only the `publishSensor` test helper was changed to await `awaitGuardianPersistence(MARKET)` before examining the durable projection. No production callback ordering or storage implementation was changed.
- Result after synchronization alone: **34 passed / 2 failed** Guardian PAPER tests, compared with **10 passed / 26 failed** before. The remaining two failures were incorrect test fixtures.

### Root cause B: idempotency fixture sent same ID but altered event identity

- `event(s, "late-arm")` creates a new row with changed transport metadata; merely reusing an event ID does not make it identical.
- Re-send the exact event returned by the durable journal instead, verifying legitimate exact retries are idempotent and inconsistent duplicate IDs remain conflicts.
- For commit-before-apply crash replay, the test now reuses the same `JournalEvent` object before and after restart.

### Root cause C: crash injection used a legacy Guardian operation key

- The frozen Guardian format generation 3 uses `reversal:<positionId>:<unhedgedStartedAt>:v2g3:<revision>`, not the old `...:v2:<revision>` key.
- The incorrect key never injected a crash. The test now uses `v2g3:1`, and the crash/replay assertion is active.
- Result after B + C: **36/36 Guardian V2 PAPER tests PASS** (single-file run).

## Separate unresolved genuine economic behavior: Profit-trailing/legacy deadline

One unrelated integration test in `tests/profit-trailing-pair-engine.test.ts` still fails. Its assertion is kept unchanged.

Reproduced and inspected locally with disposable diagnostic prints (then restored the file to HEAD):
1. The first UP leg remains open with qty 5 and profit-trailing high-water edge 0.1 after checkpoint compaction.
2. Restart restores both the exact open position and high-water edge 0.1. Cash remains 996, position remains UP qty 5.
3. At virtual unhedged age **303000ms**, publish an executable retrace book.
4. Actual second `PAPER_FILL`: **SELL** with intent `PROTECTIVE_DE_RISK`, reason `MAX_UNHEDGED_DURATION`, retryCount 1, executable price 0.8, qty 5. Not the expected `HEDGE_SECOND_LEG`.
5. The ledger position is then absent because the protective sale closed the residual.
6. Underlying A/B evaluation chose A_ONLY; opposite ask depth was reported unavailable in that protective execution context.

**Interpretation:** This is a real collision between legacy deadline and the resumed profit-trailing episode in this test. The failure must NOT be hidden by treating a missing position as success or changing the expected action. Determine separately whether the deadline should defer to an armed trailing PAIR and why its book selection returned NO_EXECUTABLE_ASK_DEPTH; do not remove the expiry safety policy by accident.

**Unproven:** Whether this specific restart defect explains all real soak losses or all reference disconnections. No such causal claim.

## Gate status

- G2A fixes ONLY existing automated test fixture synchronization, exact idempotent replay and matching crash injection key.
- Does not implement G2 independent Guardian library/process, IPC, durable position stream, execution gate or any risk behavior change.
- G2 main implementation is **blocked** pending targeted design review of deadline/trailing post-restart arbitration and direct source-of-truth/persistence consistency.
- Operator approval required before any process cutover, VPS changes or deployment.
