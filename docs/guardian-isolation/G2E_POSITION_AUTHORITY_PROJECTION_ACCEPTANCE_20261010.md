# G2E | Guardian Read-Only Position Authority Projection

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent HEAD: `a0caab948f86e18f2edf3160e5c797c34e2c3a0d`
Status: Atomic local pure-contract implementation complete. NO runtime wiring, NO SHADOW.

## Root cause and verified source facts

- Real economic PAPER_FILL is a **durable transition** with operation key `fill:<fill.id>` and authority payload containing `fill`, `positionId`, reservation/resting status and optional intent. This record is NOT a complete Guardian-open-residual record: the authoritative outcome/qty and episode derive from `PaperLedger.applyFill` after serialized replay.
- `POSITION_SETTLED` is a durable terminal authority with operation key `settlement:<marketId>:<experimentId>`; an event in a research journal is not itself durable permission to trade.
- Real `PaperLedger` reuses `pos:<marketId>` as stable ID; same market/position ID may have another residual exposure episode with a **different `unhedgedStartedAt`**. Ledger recalculates `unpairedOutcome`, `unpairedQuantity` and episode start from fill inventory.
- `readDurableTransitionsAfter` returns globally sequenced transition rows; skipping unrelated transitions in a filtered cursor stream can silently hide missing economic mutations. `sourceDurableCursor` is global and not a per-position fence.
- Existing durable transitions may be compacted; a newly attached Guardian cannot assume an unbounded replay from cursor zero exists. It will require a trusted, externally hash-verified checkpoint/bootstrap.
- Source code reviewed: `apps/engine/src/engine.ts:applyFill`, `buildFillDerivedAudit`, `settleMarket`, `apps/engine/src/recovery.ts:applyDurableTransition`, `packages/core/src/accounting.ts:PaperLedger.applyFill`, `packages/db/src/durable.ts` and `packages/db/src/journal.ts`.

## Changes in this atom

1. `packages/core/src/guardian-position-projection.ts` (new, pure):
   - Strict PAPER-only versioned `GuardianPositionBootstrap`: caller-supplied checkpoint hash, global sequence cursor, one authority ID, one experiment ID, all active residual episodes and settled markets.
   - Strict `GuardianPositionFrame` for **every sequential durable mutation**, including position-changing PAPER_FILL, terminal POSITION_SETTLED and explicit whitelisted NO_POSITION_CHANGE transitions. Reject all unmodeled types rather than silently skip them.
   - Immutable reducer `bootstrapGuardianPositionProjection` and `applyGuardianPositionFrame`, returning APPLIED / DUPLICATE / REJECTED. A REJECTED result is a future consumer **fail-closed stop condition**; no code here invokes an order or updates a real ledger.
   - Episode fencing uses all four fields (market, position, unhedgedStartedAt, heldOutcome); a changed timestamp can replace an episode only with strictly newer time, and a formerly closed market may be reopened only before settlement.
   - The settled market set is terminal within this experiment. An old episode cannot regain ownership after a new episode or settlement.
   - A duplicate is accepted only when the **last** normalized frame is identical; changed payload with the same cursor, gap, reorder or cursor/source mismatch is rejected.
   - Structural source-operation key checks for `fill:` and `settlement:` prefixes.
2. `packages/core/src/index.ts`: export the pure module.
3. `tests/guardian-position-projection.test.ts`: 14 new deterministic tests including actual PaperLedger partial SELL, close and same-market reopen episode semantics.
4. This acceptance report.

## Invariant details

- **IMPORTANT: The schema does not authenticate a sender or verify a checkpoint hash.** `checkpointContentHash` is a 64-digit hex FORMAT check only. A future trusted owner must verify against the authoritative `hashCheckpointPayload` before invoking bootstrap.
- `authorityId` is an identity/fence field, not a cryptographic signature. Future transport authentication, single-source guarantees and fencing remain mandatory.
- Frames here are a **proposed trusted projection** from committed state, not existing rows and not a live service. The future producer MUST prove cursor completeness, per-transition provenance, and exact ledger state under the durable mutation fence. A best-effort `afterApply` emit cannot by itself guarantee lossless delivery after a crash.
- The consumer cannot forge a missing or repaired row into `NO_POSITION_CHANGE`; critical integrity-fault transitions, experiment starts and unmodeled types are not on its whitelist. Their handling requires a separately verified resnapshot/recovery protocol.
- The read-model contains only directional residual, held outcome and entry price, NOT economic ownership, resting makers, cash, settlement truth, executable price or admission permission. The single executor must still verify all economic fences.
- No stored request-id dedup or journal compaction replay is implemented here. No cross-process delivery ACK or authentication exists.
- Bounded snapshot limits are deliberate fail-closed bounds, not proof that all actual experiment sizes fit them.
- Same-millisecond unhedged episode collisions and trusted publisher throughput need explicit fault/scale analysis in a future gate.

## Tests and proof

- `pnpm exec vitest run tests/guardian-position-projection.test.ts`: **14/14 PASS**.
- `PM_FORENSIC_STAGE=08_current pnpm exec vitest run tests/guardian-position-projection.test.ts tests/guardian-boundary-contract.test.ts tests/guardian-v2-state-equivalence.test.ts tests/guardian-v2.test.ts tests/reversal-protection-paper.test.ts tests/profit-trailing-pair-engine.test.ts tests/engine-guardian-paper.test.ts tests/profit-trailing-pair.test.ts tests/guardian-book-signal.test.ts tests/reversal-protection.test.ts`: **182/182 PASS** across 10 test files.
- `pnpm --filter @pm/core typecheck`: PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `git diff --check`: PASS.

## Next atom / non-authorizations

G2F should create a **read-only, verified offline adapter test** that converts existing recovery checkpoint + contiguous `DurableTransition` source facts into these frames while independently projecting true `PaperLedger` state, and fault-injects cursor gaps, compaction, duplicate identity and commit-unknown outcomes. Stop if checkpoint authority or a fully ordered source is missing.

It must NOT launch a Guardian daemon, connect to live WebSockets, introduce a second live engine, change DB schemas, deploy containers, alter the live VPS, or touch LIVE/wallet. No shadow mode.

Do not claim G2 process separation or G3 single executor is complete. This atom establishes only a tested offline contract.
