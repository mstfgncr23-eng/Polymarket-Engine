# G2D | Guardian V2 History Work Reduction: Atomic Acceptance

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent: `e6446ef5267be7b8af9a07c66fad9a8cab91bc48`
Scope: independent pure Guardian V2 reducer efficiency, deterministic trace regression, local PAPER tests ONLY. **NO SHADOW**.

## Root cause and hypothesis

- Local fixed-rate instrumentation attributed the majority of synchronous hot-path cost to `EngineService.observeGuardianSnapshot`, not ordinary Profit-trailing.
- `packages/core/src/guardian-v2.ts:evaluateGuardianV2` previously did `structuredClone(previous)` on EVERY Guardian state transition, even when `history` contained hundreds of EARLY samples.
- The 10s anchor logic did two full linear `history.filter` traversals before copying the retained history. Additionally, the intact JSON equality and Zod state validation still inspect history. These are distinct costs.
- The reducer is already pure/transport independent. This atom does **not** create a separate Guardian process, executor, queue, risk authority, or feed.

## Changed files

1. `packages/core/src/guardian-v2.ts`
   - `structuredClone(previous)` replaced with `{ ...previous }`; the only nested state is `history`, and existing reducer changes to it use fresh array assignments, never in-place mutation.
   - Replaced two `history.filter` traversals with one backward scan to locate latest anchor at/before 10s cutoff and a single `slice` of the strictly newer chronological tail. Retains the SAME anchor and subsequent samples.
   - All thresholds, locked generations, timing windows, phases, event IDs, durable revision increments, `guardianV2StateSchema.parse`, JSON-noop comparison, and risk decisions untouched.
2. `tests/guardian-v2-state-equivalence.test.ts`
   - Added deterministic 720-observation Golden SHA-256 trace, generated **BEFORE** modifying the reducer.
   - Frozen expected digest: `420a79582087070595d5965c29e5b6d3ece2606ea471db26d82f10aa02c3ef17`.
   - Each iteration also checks the evaluator did NOT mutate the original input state.
3. This acceptance note.

## Proof / test results

- Golden full-output SHA-256 after optimization: **identical to frozen pre-change digest**.
- `PM_FORENSIC_STAGE=08_current pnpm exec vitest run tests/guardian-v2-state-equivalence.test.ts tests/guardian-v2.test.ts tests/reversal-protection-paper.test.ts tests/profit-trailing-pair-engine.test.ts tests/engine-guardian-paper.test.ts tests/profit-trailing-pair.test.ts tests/guardian-boundary-contract.test.ts tests/guardian-book-signal.test.ts tests/reversal-protection.test.ts`: **168/168 PASS**, 9 test files.
- `pnpm --filter @pm/core typecheck`: PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `git diff --check`: PASS.

## Fixed-rate local measurement

Script: `tests/forensic-branch-overhead.test.ts`, single local EngineService and scripted snapshots (NOT a live/simulated second Guardian).
Matching conditions: `AUDIT_RATES=100`, `AUDIT_DURATION_MS=2500`, `AUDIT_WARMUP=650`, `PM_FORENSIC_STAGE=08_current`, 250 measured ticks after warm-up; 668 history entries, 899 final Guardian revisions, 250 observed snapshots, no timeout. Measurements saved to local Windows `%TEMP%\guardian_g2d_before_20261010.txt`, `guardian_g2d_after_20261010.txt`, `guardian_g2d_after_repeat_20261010.txt`.

| Item | Before | After #1 | After #2 |
| --- | ---: | ---: | ---: |
| CPU ms/snapshot | 7.252 | 5.376 | 3.372 |
| `observeGuardianSnapshot` synchronous accumulated ms | 1380.5 | 999.7 | 997.6 |
| Callback p95 ms | 7.861 | 6.994 | 7.215 |
| Event-loop p99 ms | 35.00 | 23.86 | 26.77 |
| Completed ticks | 250 | 250 | 250 |

- The observed Guardian synchronous branch time decreased **~27.6%** (1380.5 -> 999.7ms), replicated at 997.6ms. This is a LOCAL directional measurement, NOT proof of a 27.6% global engine/VPS improvement.
- CPU process counters vary by run / machine conditions; no single CPU percentage or service-level claim is justified by 1 pre + 2 post runs, some executed alongside other tests. Dedicated repeated before/after A/B performance isolation remains future work, without any live shadow service.
- `guardianV2StateSchema.parse` still validates all entries and JSON-noop comparison still serializes state; heavy processing remains and must be measured before the next change.
- Fixed-rate `100/s` is a synthetic generator rate and MUST NOT be mislabeled 100 market/seconds end-to-end production capacity.

## Safety / rollback

- No change to `apps/engine/src/engine.ts` economic behavior, no order logic, no 12/5/2 values, no restart, no DB SQL, no durable migration, no VPS, no production Docker deployment, no LIVE or wallet.
- No shadow, second Guardian, mirrored real trading, or dual ledger writer.
- Golden trace covers one extended EARLY path. Existing Guardian V2 and PAPER lifecycle tests also cover early/late emergency, deadline, persistence, restart and exact idempotency. It does not constitute exhaustive proof for all market patterns.
- Rollback is the isolated commit, not runtime switch; original forensic worktree remains untouched.
- Follow-on G2E should identify safe position-event/observation ownership and authenticated/fenced transport contract, **offline only** before any separate process can exist. The bottleneck fix here is NOT equivalent to actual process isolation.
