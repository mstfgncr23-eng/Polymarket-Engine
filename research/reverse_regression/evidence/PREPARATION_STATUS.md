# Preparation verification | 2026-10-09

## Executed
- **Local only**: new worktree branch forensic/reverse-regression-isolation from 21a195a6809e6d7eaf8b691dba65cf8ce3edda6f.
- Retrieved nine running/previously deployed Docker source maps with docker cp to stdout over SSH (VPS READ ONLY); they are untracked and kept on local disk.
- Current image stage 08_current applied and checked via source-file SHA/byte-for-byte stage verification.
- Stage 07_cpu_v3 applied, source snapshot verified, typechecked, then restored to 08_current; stage verification returned 0 mismatches.
- pnpm install --offline --frozen-lockfile --ignore-scripts completed locally (291 packages from local cache, zero downloads).
- Reconstructed main-entry TS typecheck: 08_current PASS, 07_cpu_v3 PASS.
- Bounded Python observer: 7/7 unit tests PASS. Synthetic incident sample: REF_AND_CLOB detected, exit=10; evidence JSON generated correctly.
- Baseline relevant Vitest: 45 PASS, 2 FAIL, 47 total. Existing Git-base tests assert OLD contracts: Guardian recovery window 60,000ms vs compiled current 65,000ms; single global shard callback vs compiled current 4 independent lanes. These are stale test assertions relative to deployed source, NOT proof that current runtime is healthy or faulty.
- Actual network failure-trigger regression has NOT been run. No real soak, no rollback, no production modification.
- Full workspace typecheck has an additional incompatible stale CLI/operator-report source file that imports source-seal deleted from the compiled engine. Dedicated engine-main forensic TS config excludes the standalone CLI, and @pm/db index export was reconstructed from bundler requirements. This must be reconciled before a comprehensive PR or release.

## Next gated work (not yet executed)
1. Update/add stage-specific behavioral tests for current/shard vs safe expectations; don't change trading policy.
2. Build a test environment with independent local PostgreSQL, isolated directories and PAPER only, with absolutely no production state.
3. Produce a reproducible failure-trigger baseline under equivalent market input. Without raw feeds, failure non-reproduction is INCONCLUSIVE.
4. Run one backward stage at a time; confirm removal and re-introduction causally (A-B-A), preserve journal correctness and order.
5. User approves any production/rollback action separately.
