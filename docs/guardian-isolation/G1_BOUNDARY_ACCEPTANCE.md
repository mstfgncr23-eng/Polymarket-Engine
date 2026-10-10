# G1 Atomic Acceptance: Pure Guardian Message Contracts

Date: 2026-10-10
Status: G1 implementation under test; NO runtime integration, NO SHADOW.

## Change scope
- New `packages/core/src/guardian-boundary-contract.ts`: strict versioned CLOB observation model; versioned PAPER-only protection request; pure reject-only precheck.
- Export through `packages/core/src/index.ts`.
- New `tests/guardian-boundary-contract.test.ts`.
- This document; no `apps/engine/src` modifications, no migration, no process launch, no production/vps changes.
- The 12/5/2, 300-second and 80-second risk mechanisms are untouched.

## Core contract

- Request protocol version **1**; locked Guardian policy generation **3**; trading mode **PAPER** only.
- May request only `PROTECTIVE_SELL` or `HEDGE_SECOND_LEG` and one of `EARLY_EMERGENCY`, `EARLY_TIMEOUT`, `LATE_EMERGENCY`, `LATE_CONFIRMED`.
- Includes stable `requestId`, `experimentId`, precise open `episode` (`marketId`, `positionId`, `unhedgedStartedAt`, `heldOutcome`), expected remaining qty, source durable cursor watermark, UP/DOWN book evidence IDs and sync epoch, confirmed/request/expiry times.
- Rejects malformed decimals, zero, negative, exponent syntax, unknown fields, impossible chronology, duplicated evidence IDs, expired request, stale/future observation, mismatched episode, changed residual, and sources ahead of the authoritative cursor.
- The global `sourceDurableCursor` is **not** an exact position-ownership fence: unrelated market events can advance it. A downstream admission/commit path must independently establish durable per-episode ownership and idempotency.
- A matching `requestId` replay returns `REPLAY_REQUIRES_DURABLE_RESULT_LOOKUP`, never a new permission to submit.
- `CANDIDATE_ONLY` explicitly means **no economic authority**. Validity of current books, token identity, cancel/fence, maker/adoption states, database health, settle/expiry, and fill idempotency MUST be rechecked inside the future single authoritative executor.
- No new order executor is provided by this change. No Guardian process, IPC, timer, subscriptions, emissions, external actions or shadow mode added.

## Targeted tests and deterministic checks

- `pnpm exec vitest run tests/guardian-boundary-contract.test.ts` must pass all G1 tests.
- `pnpm exec vitest run tests/guardian-boundary-contract.test.ts tests/reversal-protection.test.ts tests/profit-trailing-pair.test.ts` must pass.
- `pnpm --filter @pm/core typecheck` must pass.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit` must pass.
- `git diff --check` must pass.

## Unrelated pre-existing baseline failures, independently reproduced

- `pnpm typecheck` on the entire stage-08 forensic source fails with `apps/engine/src/operator-report.ts(2,32): Cannot find module './source-seal.js'`. `source-seal.ts` is absent from HEAD; stage-08 source-map verification remains intact. Do **not** fabricate an unrelated source-seal file just to make G1 green.
- The existing `tests/reversal-protection-paper.test.ts` and `tests/profit-trailing-pair-engine.test.ts` had **27 failed / 12 passed** tests when executed together with G1 changes present.
- To verify provenance, G1's `packages/core/src/index.ts` export was temporarily restored to **exact HEAD** (no G1 module reachable from these tests). The **same 27 failed / 12 passed** baseline was reproduced. Then the G1 export was restored. A temporary log was written only under Windows `%TEMP%`, not added to the repo.
- These failures are NOT marked fixed. G2/G3 planning cannot assume the old PAPER integration tests are reliable until the exact root cause is isolated and they pass or are explicitly justified.

## Constraints for next atomic gate

- G2 is **NOT** authorized by this document. No shadow/second live Guardian.
- Before G2 process design, resolve availability/ordering tradeoffs: global cursor watermark vs per-position fence, durable replay after closure, processing backpressure, identity collisions, fail-closed DB ambiguity, stale source clocks, and independent feed synchronization.
- Persisted deduplication records must be authoritative; G1's pure `existingRequest` fixture is not a real transactional dedup store.
- All economic mutations, fill idempotency, ownership fencing and crash recovery remain future work.

## Provenance
- Branch: `refactor/guardian-isolation-atomic` from `4fd4ae56ea9ab27d81c57711dc533cf6d0387f4f`, G0 commit `17de09d264ed05e8c355beedc2b4d2fdd54869d1`.
- Parent forensic stage-08 source-map verification: 76 files, 0 different.
- Install used `pnpm install --offline --frozen-lockfile --ignore-scripts`; no network access or dependency upgrade.
