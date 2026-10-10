# G2G-1 | Atomic Maker/Taker Ownership Recovery

Date: 2026-10-10
Branch: refactor/guardian-isolation-atomic
Parent: 79b9639 (G0–G5 roadmap)
Status: LOCAL, OFFLINE, READ-ONLY. No shadow, runtime change or deployment.

## Root cause and objective

The G2F offline adapter rejected every checkpoint containing reserved maker cash, resting maker orders or pending taker risk ownership. Real EngineService stores those states in checkpoint payload, and economic recovery cannot truthfully discard them. G2G-1 now reconstructs **those authorities** using the actual core/recovery schemas and enforces checkpoint invariants.

Source inspected: apps/engine/src/recovery.ts (riskReductionOwnershipSchema, riskReductionExecutionFailureSchema, applyDurableTransition, validatePostReplayInvariants); packages/core/src/execution.ts restingPaperOrderSchema; packages/core/src/accounting.ts PaperLedger.reserve/release; packages/db/src/durable.ts semantic checkpoint verifier.

## Atomic code delta

File: apps/engine/src/guardian-offline-durable-source.ts

- Parse and restore maker resting orders, reservation amounts, pending risk-reduction owners and prior execution failures into the **offline RecoverableEngineState**. Preserve their durable identities, market, episode, residual quantity and relevant economic order metadata.
- Reject duplicate resting order IDs, duplicate taker ownership market keys, duplicate failure keys, mismatched episode timestamps, changed residual amounts, settled owners and simultaneous pending owner/failure for the same market.
- Call existing `validatePostReplayInvariants` on hydrated checkpoints **and after every durable transition**, including reserved cash vs summed maker reservation. This intentionally refuses internally inconsistent economic inputs.
- Previously valid partial-fill, settlement, global-cursor MTM, hash and gap semantics remain unchanged.
- A verified maker cancellation releases reserved cash and removes the order. Committed `RISK_REDUCTION_FAILED` removes pending ownership and records the actual execution-failure marker. Both are reconstructed through the existing `applyDurableTransition`, never through fabricated journal events.

## Verification

- G2G-1 added five deterministic tests to tests/guardian-offline-durable-source.test.ts, including valid pending taker, pending->execution failure, same-episode fencing, valid maker/reserved cash->cancel, duplicate maker and old execution failure.
- Existing 9 G2F tests retained; the fixture was corrected to represent **real valid ResultAccounting** rather than a synthetic invalid checkpoint that could pass prior weaker validation.
- Before change: G2F 9/9 PASS. After first invariant enforcement: tests exposed invalid fixture accounting; corrected input, not production invariants.
- **14/14 G2F+G2G-1 targeted tests PASS**; includes real maker cancellation and reserved-cash release.
- **212/212 broad tests PASS** across 12 test files, including existing durable-risk-reduction ownership, Guardian PAPER, 12/5/2 and replay tests (PM_FORENSIC_STAGE=08_current).
- Core TypeScript: PASS; forensic engine TypeScript: PASS; git diff --check: PASS.

## Not yet supported: explicit fail-closed

- Nonempty `reversalProtectionStates` (Guardian), `profitTrailingPairStates`, naked diagnostics, settlement lookups and unresolved report-source integrity/quarantine still block bootstrap.
- No live producer; no DB read/subscribe; no IPC; no process; no Guardian autonomous action. Guardian receives **zero** new execution authority.
- ExternallyVerifiedContentHash is a trusted-caller input, not authentication; future source must independently verify it against real persisted checkpoint and producer epoch.
- The adapter replays a cloned RecoverableEngineState each row, so current O(state) memory/CPU complexity has NOT been measured for long streams. This is deterministic proof tooling, NOT deployment-ready transport.
- Historic operation keys before the checkpoint are still not in an active dedup index; cross-process once-only guarantees belong to later gates.
- No G3 order-execution extraction or G4 cutover is authorized here.

## Next

G2G-2: replay Guardian V2 and Profit-trailing state + settlement lookup, protect their episode fencing, handle unresolved DB/report integrity only with explicit halt/reconciliation. Then G2H transport/cursor planning and bounded CPU profile. No hidden shadow service.
